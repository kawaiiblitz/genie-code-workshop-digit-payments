# Databricks notebook source
# MAGIC %md
# MAGIC # 00 — Setup del workshop
# MAGIC
# MAGIC Crea catálogo, esquemas y volumen UC para simular el landing de AWS DMS.
# MAGIC
# MAGIC **Importante:** solo crea `raw` como esquema + el volumen.
# MAGIC Los esquemas `bronze`, `silver`, `gold` los crea Genie Code en vivo.

# COMMAND ----------

CATALOG = "digit_payments"
VOLUME_NAME = "landing"

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.raw")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.raw.{VOLUME_NAME}")

print(f"Catálogo y volumen listos en /Volumes/{CATALOG}/raw/{VOLUME_NAME}/")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Estructura esperada del volumen
# MAGIC ```
# MAGIC /Volumes/digit_payments/raw/landing/
# MAGIC     merchants_cdc/
# MAGIC     bins_cdc/
# MAGIC     customers_cdc/
# MAGIC     transactions_raw/
# MAGIC     fraud_signals_raw/
# MAGIC ```
