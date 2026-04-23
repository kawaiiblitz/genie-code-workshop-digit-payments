# Databricks notebook source
# MAGIC %md
# MAGIC # 02 — Generador de `bins` con CDC
# MAGIC
# MAGIC BIN = primeros 6 dígitos del número de tarjeta. Identifica banco emisor,
# MAGIC marca (Visa/Mastercard/Amex), tipo (crédito/débito) y país.
# MAGIC
# MAGIC Los BINs cambian poco, pero cuando cambian (ej: se marca como riesgoso)
# MAGIC es crítico capturarlo con SCD Type 2 para auditar fraude en el pasado.

# COMMAND ----------

# MAGIC %run ../config

# COMMAND ----------

import random
from datetime import datetime, timedelta
from pyspark.sql import Row

random.seed(43)

LANDING = f"{LANDING_ROOT}/bins_cdc"
N_BINS = 1_000

# COMMAND ----------

CARD_BRANDS = [("VISA", 0.55), ("MASTERCARD", 0.35), ("AMEX", 0.07), ("DISCOVER", 0.03)]
CARD_TYPES = [("CREDIT", 0.45), ("DEBIT", 0.50), ("PREPAID", 0.05)]
ISSUER_BANKS = [
    "BBVA México", "Santander México", "Banorte", "Citibanamex",
    "HSBC México", "Scotiabank", "Banco Azteca", "Inbursa",
    "BBVA Colombia", "Bancolombia", "BCP Perú", "Galicia Argentina",
]
COUNTRIES_BY_BANK = {
    "BBVA México": "MX", "Santander México": "MX", "Banorte": "MX",
    "Citibanamex": "MX", "HSBC México": "MX", "Scotiabank": "MX",
    "Banco Azteca": "MX", "Inbursa": "MX",
    "BBVA Colombia": "CO", "Bancolombia": "CO",
    "BCP Perú": "PE", "Galicia Argentina": "AR",
}

def weighted_choice(options):
    r = random.random()
    cumulative = 0
    for opt, weight in options:
        cumulative += weight
        if r < cumulative:
            return opt
    return options[-1][0]

def make_bin(bin_seed: int) -> dict:
    bank = random.choice(ISSUER_BANKS)
    return {
        "bin": f"{400000 + bin_seed:06d}",
        "issuer_bank": bank,
        "card_brand": weighted_choice(CARD_BRANDS),
        "card_type": weighted_choice(CARD_TYPES),
        "country": COUNTRIES_BY_BANK[bank],
        "is_prepaid": False,
        "risk_flag": random.choices(["LOW", "MEDIUM", "HIGH"], weights=[85, 12, 3])[0],
    }

# COMMAND ----------

load_date = datetime(2026, 3, 23)
initial = []
used_seeds = random.sample(range(1, 100_000), N_BINS)
for seed in used_seeds:
    b = make_bin(seed)
    b["Op"] = "I"
    b["ts"] = load_date
    initial.append(b)

df_initial = spark.createDataFrame([Row(**r) for r in initial])
df_initial.write.mode("overwrite").parquet(f"{LANDING}/initial_load_20260323.parquet")
print(f"Carga inicial: {len(initial)} BINs")

# COMMAND ----------

# MAGIC %md
# MAGIC ## CDC: pocos cambios, pero los que hay importan (risk_flag)

# COMMAND ----------

base_date = datetime(2026, 3, 24)
existing_bins = [r["bin"] for r in initial]

for batch_idx in range(10):
    batch_date = base_date + timedelta(days=batch_idx * 3)
    rows = []

    # 0-3 BINs nuevos (bancos emiten rangos nuevos ocasionalmente)
    for _ in range(random.randint(0, 3)):
        b = make_bin(random.randint(100_000, 999_999))
        b["Op"] = "I"
        b["ts"] = batch_date + timedelta(hours=random.randint(0, 23))
        rows.append(b)

    # 5-15 updates (la mayoría son cambios de risk_flag — esto es el ORO para SCD2)
    for bin_num in random.sample(existing_bins, random.randint(5, 15)):
        b = make_bin(0)
        b["bin"] = bin_num
        b["risk_flag"] = random.choices(["LOW", "MEDIUM", "HIGH"], weights=[70, 20, 10])[0]
        b["Op"] = "U"
        b["ts"] = batch_date + timedelta(hours=random.randint(0, 23))
        rows.append(b)

    df = spark.createDataFrame([Row(**r) for r in rows])
    filename = f"cdc_{batch_date.strftime('%Y%m%d')}.parquet"
    df.write.mode("overwrite").parquet(f"{LANDING}/{filename}")
    print(f"  {filename}: {len(rows)} cambios")

print("\n✓ Bins CDC listo")
