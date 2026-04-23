# Workshop Genie Code — `digit_payments`

Workshop guiado sobre **Genie Code** en Databricks.
---

## ¿Qué es Genie Code?

Agente de IA dentro del workspace de Databricks. Le describes qué quieres
(en español o inglés) y escribe, ejecuta y depura el código: notebooks,
Lakeflow Declarative Pipelines, jobs, modelos, Genie Spaces.

El workshop lo demuestra construyendo una plataforma de medallón end-to-end
**sin escribir código a mano**, guiado por ~7 prompts.

---

## Arquitectura objetivo

```
s3://landing/                         ◄── simulación de AWS DMS
    merchants_cdc/      (I/U/D + ts)
    bins_cdc/
    customers_cdc/
    transactions_raw/   (append-only, streaming-friendly)
    fraud_signals_raw/
         │
         ▼
catalog.digit_payments.bronze         ◄── ingesta con expectations
         │
         ▼
catalog.digit_payments.silver        
    merchants       (SCD Type 2 con APPLY CHANGES)
    bins            (SCD Type 2)
    customers       (SCD Type 2)
    transactions    (streaming, enriquecida con merchant + bin)
    fraud_signals
         │
         ▼
catalog.digit_payments.gold
    merchant_daily_risk      (reemplaza window functions caras)
    bin_risk_profile
         │
         ▼
Metric Views + Genie Space + Row Filter por país
```

---


## Estructura del repo

```
genie-code-workshop-digit-payments/
│
├── README.md                          (este archivo)
├── docs/
│   ├── arquitectura.md                Diagrama + explicación de la arquitectura
│   └── genie_code_prompts.md          ★ Los 6 prompts para ejecutar en Genie Code
│
└── notebooks/
    ├── config.py                      Variables centrales (CATALOG, SCHEMA, VOLUME)
    │
    ├── 00_setup/
    │   └── 00_setup.py                Crea catalog + esquema + volumen UC
    │
    └── 01_data_generation/            Scaffold: genera la zona landing
        ├── 01_gen_merchants.py        Full load + 30 días de CDC
        ├── 02_gen_bins.py
        ├── 03_gen_customers.py
        ├── 04_gen_transactions.py     ~5M registros, append-only
        ├── 05_gen_fraud_signals.py
        └── 99_run_all.py              Orquestador
```

**Todo lo que está en `bronze/`, `silver/` y `gold/` se construye con
Genie Code durante la demo**.

---

## Cómo correr

### Prerrequisitos

- Workspace Databricks con Unity Catalog y Genie habilitado
- Databricks CLI autenticado (`databricks auth login`)
- Python 3.10+
- Permisos para crear catálogo + volumen UC en el workspace

### Paso 0 — Ajustar catálogo y esquema *(hazlo primero)*

**Antes de correr cualquier cosa**, abre `notebooks/config.py` y revisa:

```python
CATALOG = "digit_payments"    # ← cambia si ya tienes un catálogo sandbox propio
RAW_SCHEMA = "raw"
VOLUME_NAME = "landing"
```

Por default crea el catálogo `digit_payments` (si no existe) con el esquema
`raw` y el volumen `landing`. Si en tu workspace no puedes crear catálogos, o
prefieres reutilizar uno existente, cambia estas 3 variables. El resto de los
notebooks las levantan automáticamente vía `%run ../config`.

### Paso 1 — Preparar el scaffold en el workspace

1. Subir la carpeta `notebooks/` al workspace (vía `databricks workspace import-dir` o la UI)
2. Correr `00_setup/00_setup` (crea catálogo + esquema + volumen — ~10s)
3. Correr `01_data_generation/99_run_all` (genera ~5.6M registros de raw — ~3-5 min)

### Paso 2 — Ejecutar el workshop

Abre Genie Code en el workspace y sigue
[`docs/genie_code_prompts.md`](docs/genie_code_prompts.md) en orden.

---
