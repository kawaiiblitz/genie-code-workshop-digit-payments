# Databricks notebook source
# MAGIC %md
# MAGIC # 00 — Setup del workshop
# MAGIC
# MAGIC Crea catálogo, esquemas y volumen UC para simular el landing de AWS DMS.
# MAGIC
# MAGIC **Importante:** solo crea `raw` como esquema + el volumen.
# MAGIC Los esquemas `bronze`, `silver`, `gold` los crea Genie Code en vivo.
# MAGIC
# MAGIC ## ⚠️ Antes de correr
# MAGIC Revisá `../config` y ajustá `CATALOG`, `RAW_SCHEMA`, `VOLUME_NAME` si
# MAGIC no querés usar los defaults (`digit_payments.raw.landing`).

# COMMAND ----------

# MAGIC %run ../config

# COMMAND ----------

# Verificar que el catálogo existe (sin intentar crearlo — evita requerir permiso sobre la metastore)
catalogs = [r.catalog for r in spark.sql("SHOW CATALOGS").collect()]
if CATALOG not in catalogs:
    raise RuntimeError(
        f"El catálogo '{CATALOG}' no existe en este workspace. "
        f"Crealo primero desde la UI o ajustá CATALOG en config.py a uno existente."
    )

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{RAW_SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{RAW_SCHEMA}.{VOLUME_NAME}")

print(f"Schema y volumen listos en {LANDING_ROOT}/")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Estructura esperada del volumen
# MAGIC ```
# MAGIC {LANDING_ROOT}/
# MAGIC     merchants_cdc/
# MAGIC     bins_cdc/
# MAGIC     customers_cdc/
# MAGIC     transactions_raw/
# MAGIC     fraud_signals_raw/
# MAGIC ```
