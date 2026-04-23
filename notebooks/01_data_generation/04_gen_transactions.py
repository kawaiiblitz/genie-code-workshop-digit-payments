# Databricks notebook source
# MAGIC %md
# MAGIC # 04 — Generador de `transactions` (streaming-friendly)
# MAGIC
# MAGIC ~5M transacciones a lo largo de 30 días, distribuidas en 10 archivos
# MAGIC Parquet. Append-only — no hay Op column, solo nuevos registros.
# MAGIC
# MAGIC Cada archivo representa un "bloque" que DMS + Kinesis escribirían al
# MAGIC landing. En Silver, el equipo los leerá con Autoloader en modo streaming.
# MAGIC
# MAGIC **Patrón de fraude embebido** (para que la demo de Gold tenga signal):
# MAGIC - ~0.8% de transacciones son fraude
# MAGIC - Fraude tiene más probabilidad de noche (2am-5am)
# MAGIC - Fraude tiene más probabilidad en BINs con `risk_flag = HIGH`
# MAGIC - Fraude tiene montos sesgados (muy altos o inusualmente específicos)

# COMMAND ----------

# MAGIC %run ../config

# COMMAND ----------

from pyspark.sql import functions as F
from datetime import datetime, timedelta

LANDING = f"{LANDING_ROOT}/transactions_raw"
N_TRANSACTIONS = 5_000_000
N_BATCHES = 10
N_MERCHANTS_SAMPLE = 1000
N_BINS_SAMPLE = 500

# COMMAND ----------

# MAGIC %md
# MAGIC ## Leer dim tables y crearles índice posicional

# COMMAND ----------

# Leer merchants y bins, tomar una muestra y asignarles índice 0..N-1
merchants_sample = (
    spark.read.parquet(f"{LANDING_ROOT}/merchants_cdc/initial_load_20260323.parquet")
    .select("merchant_id")
    .limit(N_MERCHANTS_SAMPLE)
    .collect()
)
bins_sample = (
    spark.read.parquet(f"{LANDING_ROOT}/bins_cdc/initial_load_20260323.parquet")
    .select("bin")
    .limit(N_BINS_SAMPLE)
    .collect()
)

merchants_idx_df = spark.createDataFrame(
    [(i, r.merchant_id) for i, r in enumerate(merchants_sample)],
    ["m_pos", "merchant_id"],
)

bins_idx_df = spark.createDataFrame(
    [(i, r.bin) for i, r in enumerate(bins_sample)],
    ["b_pos", "bin"],
)

n_merchants = merchants_idx_df.count()
n_bins = bins_idx_df.count()
print(f"Dims listas: {n_merchants} merchants, {n_bins} bins")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generar cada batch

# COMMAND ----------

base_date = datetime(2026, 3, 24)
rows_per_batch = N_TRANSACTIONS // N_BATCHES

for batch_idx in range(N_BATCHES):
    batch_start = base_date + timedelta(days=batch_idx * 3)
    batch_end = batch_start + timedelta(days=3)

    # Generar columnas base con posiciones aleatorias para merchant y bin
    base = (
        spark.range(rows_per_batch)
        .withColumn("seq", F.col("id") + F.lit(batch_idx * rows_per_batch))
        .withColumn("transaction_id", F.concat(F.lit("TXN-"), F.lpad(F.col("seq").cast("string"), 10, "0")))
        .withColumn("m_pos", (F.rand(seed=batch_idx * 7) * F.lit(n_merchants)).cast("int"))
        .withColumn("b_pos", (F.rand(seed=batch_idx * 13) * F.lit(n_bins)).cast("int"))
        .withColumn("customer_seq", (F.rand(seed=batch_idx * 11) * F.lit(120_000) + 1).cast("int"))
        .withColumn("customer_id", F.concat(F.lit("CUS-"), F.lpad(F.col("customer_seq").cast("string"), 8, "0")))
        .withColumn("card_last4", F.lpad((F.rand(seed=batch_idx * 17) * F.lit(9999)).cast("int").cast("string"), 4, "0"))
        # Amount: lognormal — mayormente 100-2000 MXN, cola larga hasta 50K
        .withColumn("amount_mxn", F.round(F.exp(F.lit(5.5) + F.randn(seed=batch_idx * 19) * F.lit(1.2)), 2))
        .withColumn("currency", F.lit("MXN"))
        .withColumn("payment_method",
                    F.when(F.rand(seed=batch_idx * 23) < 0.78, "CARD")
                     .when(F.rand(seed=batch_idx * 23) < 0.90, "SPEI")
                     .when(F.rand(seed=batch_idx * 23) < 0.97, "OXXO")
                     .otherwise("PAYNET"))
        .withColumn("status",
                    F.when(F.rand(seed=batch_idx * 29) < 0.92, "APPROVED")
                     .when(F.rand(seed=batch_idx * 29) < 0.98, "DECLINED")
                     .otherwise("PENDING"))
        .withColumn("hour_of_day", (F.rand(seed=batch_idx * 31) * F.lit(24)).cast("int"))
        .withColumn("day_offset", (F.rand(seed=batch_idx * 37) * F.lit(3)).cast("int"))
        .withColumn("transaction_ts",
                    F.expr(f"timestamp '{batch_start.strftime('%Y-%m-%d %H:%M:%S')}' + make_interval(0, 0, 0, day_offset, hour_of_day, cast(rand() * 60 as int), 0)"))
        # Fraud flag — probabilidad base 0.5%, +2% si hora 2-5am
        .withColumn("is_fraud",
                    F.when((F.col("hour_of_day") >= 2) & (F.col("hour_of_day") <= 5) & (F.rand(seed=batch_idx * 41) < 0.025), 1)
                     .when(F.rand(seed=batch_idx * 41) < 0.005, 1)
                     .otherwise(0))
        .withColumn("fraud_score",
                    F.when(F.col("is_fraud") == 1, F.round(F.lit(0.7) + F.rand(seed=batch_idx * 43) * F.lit(0.3), 3))
                     .otherwise(F.round(F.rand(seed=batch_idx * 43) * F.lit(0.4), 3)))
        .withColumn("ip_country",
                    F.when(F.rand(seed=batch_idx * 47) < 0.85, "MX")
                     .when(F.rand(seed=batch_idx * 47) < 0.95, "US")
                     .otherwise("XX"))
        .withColumn("device_fingerprint", F.sha2(F.concat(F.col("customer_id"), F.col("transaction_id")), 256).substr(1, 20))
    )

    # Join con merchant y bin usando el índice posicional (no usa F.array con miles de literals)
    df = (
        base
        .join(F.broadcast(merchants_idx_df), "m_pos")
        .join(F.broadcast(bins_idx_df), "b_pos")
        .drop("id", "seq", "m_pos", "b_pos", "customer_seq", "hour_of_day", "day_offset")
    )

    filename = f"batch_{batch_start.strftime('%Y%m%d')}.parquet"
    df.write.mode("overwrite").parquet(f"{LANDING}/{filename}")
    print(f"  {filename}: {rows_per_batch:,} transacciones ({batch_start.date()} → {batch_end.date()})")

print(f"\n✓ Transactions listas: {N_TRANSACTIONS:,} total en {N_BATCHES} archivos")
