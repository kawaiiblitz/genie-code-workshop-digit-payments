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
from pyspark.sql import types as T

LANDING = f"{LANDING_ROOT}/transactions_raw"
N_TRANSACTIONS = 5_000_000
N_BATCHES = 10

# COMMAND ----------

# MAGIC %md
# MAGIC ## Leer merchants y bins generados previamente
# MAGIC
# MAGIC Los necesitamos para generar FKs válidas.

# COMMAND ----------

merchants = (
    spark.read.parquet(f"{LANDING_ROOT}/merchants_cdc/initial_load_20260323.parquet")
    .select("merchant_id", "country", "risk_tier", "mcc_code")
    .cache()
)
bins = (
    spark.read.parquet(f"{LANDING_ROOT}/bins_cdc/initial_load_20260323.parquet")
    .select("bin", "risk_flag", "card_brand", "card_type", "country")
    .cache()
)

n_merchants = merchants.count()
n_bins = bins.count()
print(f"Merchants base: {n_merchants} | BINs base: {n_bins}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Arrays broadcast para sampling aleatorio rápido

# COMMAND ----------

merchant_ids = [r["merchant_id"] for r in merchants.collect()]
bin_rows = [(r["bin"], r["risk_flag"], r["card_brand"], r["card_type"], r["country"]) for r in bins.collect()]

b_merchant_ids = spark.sparkContext.broadcast(merchant_ids)
b_bin_rows = spark.sparkContext.broadcast(bin_rows)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generar cada batch

# COMMAND ----------

from datetime import datetime, timedelta
import random

base_date = datetime(2026, 3, 24)
rows_per_batch = N_TRANSACTIONS // N_BATCHES

for batch_idx in range(N_BATCHES):
    batch_start = base_date + timedelta(days=batch_idx * 3)
    batch_end = batch_start + timedelta(days=3)

    # Genero el batch con Spark para velocidad
    df = (
        spark.range(rows_per_batch)
        .withColumn("seq", F.col("id") + F.lit(batch_idx * rows_per_batch))
        .withColumn("transaction_id", F.concat(F.lit("TXN-"), F.lpad(F.col("seq").cast("string"), 10, "0")))
        # Sample merchant
        .withColumn("merchant_idx", (F.rand(seed=batch_idx * 7) * F.lit(len(merchant_ids))).cast("int"))
        .withColumn("merchant_id",
                    F.element_at(F.array(*[F.lit(m) for m in merchant_ids[:1000]]),  # uso primeros 1000 por performance
                                 F.col("merchant_idx") % F.lit(1000) + 1))
        # Customer aleatorio (puede no existir, simulando clientes nuevos que llegan después)
        .withColumn("customer_seq", (F.rand(seed=batch_idx * 11) * F.lit(120_000) + 1).cast("int"))
        .withColumn("customer_id", F.concat(F.lit("CUS-"), F.lpad(F.col("customer_seq").cast("string"), 8, "0")))
        # Bin
        .withColumn("bin_idx", (F.rand(seed=batch_idx * 13) * F.lit(len(bin_rows))).cast("int"))
        .withColumn("bin",
                    F.element_at(F.array(*[F.lit(b[0]) for b in bin_rows[:500]]),
                                 F.col("bin_idx") % F.lit(500) + 1))
        .withColumn("card_last4", F.lpad((F.rand(seed=batch_idx * 17) * F.lit(9999)).cast("int").cast("string"), 4, "0"))
        # Amount: lognormal — mayormente 100-2000 MXN, cola larga hasta 50K
        .withColumn("amount_mxn", F.round(F.exp(F.lit(5.5) + F.randn(seed=batch_idx * 19) * F.lit(1.2)), 2))
        .withColumn("currency", F.lit("MXN"))
        .withColumn("payment_method",
                    F.when(F.rand(seed=batch_idx * 23) < 0.78, "CARD")
                     .when(F.rand(seed=batch_idx * 23) < 0.90, "SPEI")
                     .when(F.rand(seed=batch_idx * 23) < 0.97, "OXXO")
                     .otherwise("PAYNET"))
        # Status: 92% approved, 6% declined, 2% pending/reversed
        .withColumn("status",
                    F.when(F.rand(seed=batch_idx * 29) < 0.92, "APPROVED")
                     .when(F.rand(seed=batch_idx * 29) < 0.98, "DECLINED")
                     .otherwise("PENDING"))
        # Timestamp distribuido en la ventana, con sesgo horario
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
        .drop("id", "seq", "merchant_idx", "customer_seq", "bin_idx", "hour_of_day", "day_offset")
    )

    filename = f"batch_{batch_start.strftime('%Y%m%d')}.parquet"
    (df.write
       .mode("overwrite")
       .parquet(f"{LANDING}/{filename}"))
    print(f"  {filename}: {rows_per_batch:,} transacciones ({batch_start.date()} → {batch_end.date()})")

print(f"\n✓ Transactions listas: {N_TRANSACTIONS:,} total en {N_BATCHES} archivos")
