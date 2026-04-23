# Databricks notebook source
# MAGIC %md
# MAGIC # 99 — Orquestador: correr todos los generadores en orden
# MAGIC
# MAGIC Ejecuta en secuencia:
# MAGIC 1. `01_gen_merchants` — 5K comercios + CDC
# MAGIC 2. `02_gen_bins` — 1K BINs + CDC
# MAGIC 3. `03_gen_customers` — 100K clientes + CDC
# MAGIC 4. `04_gen_transactions` — 5M transacciones (depende de merchants + bins)
# MAGIC 5. `05_gen_fraud_signals` — ~500K señales (depende de transactions)
# MAGIC
# MAGIC Correr esto **una sola vez** antes del workshop.

# COMMAND ----------

import time

t0 = time.time()

notebooks = [
    "./01_gen_merchants",
    "./02_gen_bins",
    "./03_gen_customers",
    "./04_gen_transactions",
    "./05_gen_fraud_signals",
]

for nb in notebooks:
    print(f"\n{'='*60}\n▶ Corriendo {nb}\n{'='*60}")
    start = time.time()
    dbutils.notebook.run(nb, timeout_seconds=1800)
    print(f"  ✓ {nb} completado en {time.time() - start:.1f}s")

print(f"\n{'='*60}")
print(f"✓ TODO LISTO — tiempo total: {time.time() - t0:.1f}s")
print(f"{'='*60}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verificación rápida

# COMMAND ----------

CATALOG = "digit_payments"
landing_root = f"/Volumes/{CATALOG}/raw/landing"

for entity in ["merchants_cdc", "bins_cdc", "customers_cdc", "transactions_raw", "fraud_signals_raw"]:
    files = dbutils.fs.ls(f"{landing_root}/{entity}")
    n_files = len([f for f in files if not f.name.startswith("_")])
    print(f"  {entity}: {n_files} archivos")
