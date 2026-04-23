# Databricks notebook source
# MAGIC %md
# MAGIC # Config compartida del workshop
# MAGIC
# MAGIC Este notebook centraliza las variables que usan todos los demás
# MAGIC notebooks del workshop. Cambiá acá una sola vez y todo el repo queda
# MAGIC apuntando al catálogo/esquema que quieras.
# MAGIC
# MAGIC Cada notebook lo invoca con:
# MAGIC ```python
# MAGIC %run ../config
# MAGIC ```

# COMMAND ----------

# ┌────────────────────────────────────────────────────────────────┐
# │ ★ AJUSTÁ ESTAS VARIABLES ANTES DE CORRER CUALQUIER NOTEBOOK ★  │
# └────────────────────────────────────────────────────────────────┘

CATALOG = "digit_payments"       # ← cambia si quieres usar otro catálogo
RAW_SCHEMA = "raw"               # ← esquema para el landing
VOLUME_NAME = "landing"          # ← volumen UC dentro de raw

# ┌────────────────────────────────────────────────────────────────┐

LANDING_ROOT = f"/Volumes/{CATALOG}/{RAW_SCHEMA}/{VOLUME_NAME}"

print(f"★ Config activa: {CATALOG}.{RAW_SCHEMA}.{VOLUME_NAME}")
print(f"★ Landing root: {LANDING_ROOT}")
