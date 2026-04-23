# Databricks notebook source
# MAGIC %md
# MAGIC # 05 — Generador de `fraud_signals`
# MAGIC
# MAGIC Eventos del motor antifraude: velocity rules, geo mismatch, device risk,
# MAGIC etc. Correlacionados con las transacciones marcadas `is_fraud = 1` más
# MAGIC una porción de falsos positivos.
# MAGIC
# MAGIC Append-only, streaming-friendly.

# COMMAND ----------

from pyspark.sql import functions as F
from datetime import datetime, timedelta

CATALOG = "digit_payments"
LANDING = f"/Volumes/{CATALOG}/raw/landing/fraud_signals_raw"

SIGNAL_TYPES = [
    "VELOCITY_HIGH",
    "GEO_MISMATCH",
    "DEVICE_FINGERPRINT_NEW",
    "HIGH_RISK_BIN",
    "UNUSUAL_AMOUNT",
    "NIGHT_TRANSACTION",
    "MULTIPLE_DECLINED",
]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Por cada batch de transacciones, generar señales

# COMMAND ----------

base_date = datetime(2026, 3, 24)

for batch_idx in range(10):
    batch_start = base_date + timedelta(days=batch_idx * 3)
    txn_path = f"/Volumes/{CATALOG}/raw/landing/transactions_raw/batch_{batch_start.strftime('%Y%m%d')}.parquet"

    txns = spark.read.parquet(txn_path)

    # Tomar todas las transacciones fraudulentas + muestreo de legítimas para FP
    fraud_txns = txns.filter(F.col("is_fraud") == 1)
    legit_sample = txns.filter(F.col("is_fraud") == 0).sample(fraction=0.008, seed=batch_idx * 53)

    base = fraud_txns.select("transaction_id", "transaction_ts", "merchant_id", "customer_id", "fraud_score", F.lit(True).alias("is_real_fraud"))
    base = base.union(legit_sample.select("transaction_id", "transaction_ts", "merchant_id", "customer_id", "fraud_score", F.lit(False).alias("is_real_fraud")))

    # Cada transacción sospechosa puede generar 1-3 señales
    signals = (
        base
        .withColumn("n_signals", (F.rand(seed=batch_idx * 59) * F.lit(3)).cast("int") + F.lit(1))
        .withColumn("signal_idx", F.explode(F.sequence(F.lit(1), F.col("n_signals"))))
        .withColumn("signal_type",
                    F.element_at(F.array(*[F.lit(s) for s in SIGNAL_TYPES]),
                                 (F.rand(seed=batch_idx * 61) * F.lit(len(SIGNAL_TYPES))).cast("int") + 1))
        .withColumn("signal_value", F.round(F.rand(seed=batch_idx * 67), 4))
        .withColumn("signal_ts", F.col("transaction_ts") + F.expr("make_interval(0,0,0,0,0,0,cast(rand()*5 as int))"))
        .withColumn("event_id", F.concat(F.lit("SIG-"), F.col("transaction_id"), F.lit("-"), F.col("signal_idx").cast("string")))
        .select("event_id", "transaction_id", "signal_type", "signal_value", "signal_ts", "is_real_fraud")
    )

    filename = f"batch_{batch_start.strftime('%Y%m%d')}.parquet"
    signals.write.mode("overwrite").parquet(f"{LANDING}/{filename}")
    cnt = signals.count()
    print(f"  {filename}: {cnt:,} señales")

print("\n✓ Fraud signals listas")
