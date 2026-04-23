# Databricks notebook source
# MAGIC %md
# MAGIC # 03 — Generador de `customers` con CDC
# MAGIC
# MAGIC Clientes finales que hacen pagos. Datos ya enmascarados por DMS
# MAGIC (email hasheado, etc.) tal como OpenPay los recibe.

# COMMAND ----------

import random
import hashlib
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

random.seed(44)

CATALOG = "digit_payments"
LANDING = f"/Volumes/{CATALOG}/raw/landing/customers_cdc"
N_CUSTOMERS = 100_000

# COMMAND ----------

COUNTRIES = [("MX", 0.75), ("CO", 0.10), ("PE", 0.08), ("AR", 0.07)]
TIERS = [("BRONZE", 0.60), ("SILVER", 0.30), ("GOLD", 0.08), ("PLATINUM", 0.02)]

def weighted(options):
    r = random.random()
    cum = 0
    for opt, w in options:
        cum += w
        if r < cum:
            return opt
    return options[-1][0]

def hash_email(cid: int) -> str:
    return hashlib.sha256(f"customer{cid}@digitpay.mx".encode()).hexdigest()[:16]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Carga inicial (100K)
# MAGIC
# MAGIC Usamos Spark `range()` para velocidad — a este volumen hacerlo en Python puro sería lento.

# COMMAND ----------

load_date = datetime(2026, 3, 23)

df_initial = (
    spark.range(1, N_CUSTOMERS + 1)
    .withColumnRenamed("id", "customer_seq")
    .withColumn("customer_id", F.concat(F.lit("CUS-"), F.lpad(F.col("customer_seq").cast("string"), 8, "0")))
    .withColumn("email_hash", F.sha2(F.concat(F.lit("customer"), F.col("customer_seq"), F.lit("@digitpay.mx")), 256).substr(1, 16))
    .withColumn("country", F.element_at(
        F.array(*[F.lit(c) for c, _ in COUNTRIES]),
        (F.rand(seed=44) * F.lit(len(COUNTRIES)) + 1).cast("int")
    ))
    .withColumn("tier", F.when(F.rand(seed=45) < 0.60, "BRONZE")
                        .when(F.rand(seed=45) < 0.90, "SILVER")
                        .when(F.rand(seed=45) < 0.98, "GOLD")
                        .otherwise("PLATINUM"))
    .withColumn("created_at", F.expr("timestamp '2024-01-01 00:00:00' + make_interval(0, 0, 0, cast(rand() * 800 as int), 0, 0, 0)"))
    .withColumn("status", F.lit("ACTIVE"))
    .withColumn("Op", F.lit("I"))
    .withColumn("ts", F.lit(load_date))
    .drop("customer_seq")
)

df_initial.write.mode("overwrite").parquet(f"{LANDING}/initial_load_20260323.parquet")
print(f"Carga inicial: {N_CUSTOMERS} clientes")

# COMMAND ----------

# MAGIC %md
# MAGIC ## CDC incremental (30 días)
# MAGIC
# MAGIC - ~500 clientes nuevos por batch
# MAGIC - ~200 updates por batch (cambios de tier, country raro)
# MAGIC - ~10 deletes por batch

# COMMAND ----------

from pyspark.sql import Row

base_date = datetime(2026, 3, 24)
next_id = N_CUSTOMERS + 1

for batch_idx in range(10):
    batch_date = base_date + timedelta(days=batch_idx * 3)
    rows = []

    # Nuevos
    for _ in range(random.randint(400, 600)):
        rows.append({
            "customer_id": f"CUS-{next_id:08d}",
            "email_hash": hash_email(next_id),
            "country": weighted(COUNTRIES),
            "tier": weighted(TIERS),
            "created_at": batch_date,
            "status": "ACTIVE",
            "Op": "I",
            "ts": batch_date + timedelta(hours=random.randint(0, 23)),
        })
        next_id += 1

    # Updates
    for _ in range(random.randint(150, 250)):
        cid = random.randint(1, N_CUSTOMERS)
        rows.append({
            "customer_id": f"CUS-{cid:08d}",
            "email_hash": hash_email(cid),
            "country": weighted(COUNTRIES),
            "tier": weighted(TIERS),
            "created_at": datetime(2024, 1, 1) + timedelta(days=random.randint(0, 800)),
            "status": "ACTIVE",
            "Op": "U",
            "ts": batch_date + timedelta(hours=random.randint(0, 23)),
        })

    # Deletes
    for _ in range(random.randint(5, 15)):
        cid = random.randint(1, N_CUSTOMERS)
        rows.append({
            "customer_id": f"CUS-{cid:08d}",
            "email_hash": hash_email(cid),
            "country": "MX",
            "tier": "BRONZE",
            "created_at": datetime(2024, 1, 1),
            "status": "DEACTIVATED",
            "Op": "D",
            "ts": batch_date + timedelta(hours=random.randint(0, 23)),
        })

    df = spark.createDataFrame([Row(**r) for r in rows])
    filename = f"cdc_{batch_date.strftime('%Y%m%d')}.parquet"
    df.write.mode("overwrite").parquet(f"{LANDING}/{filename}")
    print(f"  {filename}: {len(rows)} cambios")

print(f"\n✓ Customers CDC listo. Siguiente customer_id: CUS-{next_id:08d}")
