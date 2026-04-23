# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Generador de `merchants` con CDC
# MAGIC
# MAGIC Simula el output de AWS DMS:
# MAGIC - 1 archivo de **carga inicial** (~5,000 comercios)
# MAGIC - 10 archivos de **CDC incremental** distribuidos a lo largo de 30 días
# MAGIC   con columnas `Op` (I/U/D) y `ts`
# MAGIC
# MAGIC Formato: Parquet en `/Volumes/digit_payments/raw/landing/merchants_cdc/`

# COMMAND ----------

# MAGIC %run ../config

# COMMAND ----------

import random
from datetime import datetime, timedelta
from pyspark.sql import Row

random.seed(42)

LANDING = f"{LANDING_ROOT}/merchants_cdc"
N_MERCHANTS = 5_000
N_CDC_BATCHES = 10

# COMMAND ----------

# MAGIC %md
# MAGIC ## Catálogos de negocio

# COMMAND ----------

MCC_CATEGORIES = [
    ("5411", "Supermercados"),
    ("5812", "Restaurantes"),
    ("5541", "Gasolineras"),
    ("4900", "Servicios públicos"),
    ("4814", "Telecomunicaciones"),
    ("7299", "Servicios profesionales"),
    ("5999", "Retail misceláneo"),
    ("5310", "Tiendas de descuento"),
    ("5912", "Farmacias"),
    ("4722", "Agencias de viaje"),
]

COUNTRIES = ["MX", "MX", "MX", "MX", "CO", "PE", "AR"]  # MX dominante
RISK_TIERS = ["A", "A", "B", "B", "B", "C"]
BUSINESS_TYPES = ["B2C", "B2C", "B2C", "B2B"]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Carga inicial

# COMMAND ----------

def make_merchant(merchant_id: int, created_at: datetime) -> dict:
    mcc, category = random.choice(MCC_CATEGORIES)
    return {
        "merchant_id": f"MER-{merchant_id:06d}",
        "merchant_name": f"{category.split()[0]} {random.choice(['Plus','Mx','Pro','Max','Express','Premium'])} {merchant_id:04d}",
        "mcc_code": mcc,
        "mcc_description": category,
        "country": random.choice(COUNTRIES),
        "business_type": random.choice(BUSINESS_TYPES),
        "risk_tier": random.choice(RISK_TIERS),
        "monthly_volume_estimate": round(random.lognormvariate(10, 1.5), 2),
        "created_at": created_at,
        "status": "ACTIVE",
    }

load_date = datetime(2026, 3, 23)
initial_rows = []
for i in range(1, N_MERCHANTS + 1):
    created = load_date - timedelta(days=random.randint(30, 1800))
    m = make_merchant(i, created)
    m["Op"] = "I"
    m["ts"] = load_date
    initial_rows.append(m)

df_initial = spark.createDataFrame([Row(**r) for r in initial_rows])
df_initial.write.mode("overwrite").parquet(f"{LANDING}/initial_load_20260323.parquet")
print(f"Carga inicial: {df_initial.count()} comercios escritos")

# COMMAND ----------

# MAGIC %md
# MAGIC ## CDC incremental (30 días, 10 archivos)

# COMMAND ----------

base_date = datetime(2026, 3, 24)
next_merchant_id = N_MERCHANTS + 1

for batch_idx in range(N_CDC_BATCHES):
    batch_date = base_date + timedelta(days=batch_idx * 3)
    cdc_rows = []

    # ~5 nuevos comercios por batch
    for _ in range(random.randint(3, 8)):
        m = make_merchant(next_merchant_id, batch_date)
        m["Op"] = "I"
        m["ts"] = batch_date + timedelta(hours=random.randint(0, 23))
        cdc_rows.append(m)
        next_merchant_id += 1

    # ~50 updates por batch (cambio de risk_tier o monthly_volume)
    update_ids = random.sample(range(1, N_MERCHANTS + 1), random.randint(40, 60))
    for mid in update_ids:
        m = make_merchant(mid, load_date)
        m["risk_tier"] = random.choice(RISK_TIERS)
        m["monthly_volume_estimate"] = round(random.lognormvariate(10, 1.5), 2)
        m["Op"] = "U"
        m["ts"] = batch_date + timedelta(hours=random.randint(0, 23))
        cdc_rows.append(m)

    # ~2 deletes por batch
    for _ in range(random.randint(1, 3)):
        mid = random.randint(1, N_MERCHANTS)
        m = make_merchant(mid, load_date)
        m["status"] = "INACTIVE"
        m["Op"] = "D"
        m["ts"] = batch_date + timedelta(hours=random.randint(0, 23))
        cdc_rows.append(m)

    df_cdc = spark.createDataFrame([Row(**r) for r in cdc_rows])
    filename = f"cdc_{batch_date.strftime('%Y%m%d')}.parquet"
    df_cdc.write.mode("overwrite").parquet(f"{LANDING}/{filename}")
    print(f"  {filename}: {len(cdc_rows)} cambios (I/U/D)")

print(f"\n✓ Merchants CDC listo. Siguiente merchant_id: MER-{next_merchant_id:06d}")
