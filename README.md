# Workshop Genie Code — `digit_payments`

Workshop guiado de **1 hora** sobre **Genie Code** en Databricks, diseñado para
un equipo de ingeniería que hoy construye su capa Silver a mano desde archivos
CDC (estilo AWS DMS) y consume directo de Silver para dashboards de fraude,
lo que le pega al performance.

Cliente destino: **OpenPay** (alias en la demo: **`digit_payments`**).

---

## La historia que cuenta esta demo

> *"Tienen DMS escribiendo CDC a S3 todos los días. El equipo está completando
> Silver manualmente. Mientras tanto, los dashboards de fraude consumen directo
> de transaccional y cargan en 15-20 minutos. Vamos a ver cómo Genie Code
> cierra Silver con sus estándares, habilita una Gold ligera para fraude, y
> pone todo detrás de una capa semántica con Genie Space — en una hora."*

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
catalog.digit_payments.silver         ◄── ★ EL NÚCLEO DE LA DEMO
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

## Agenda (1 hora — demo guiada + 2 momentos hands-on)

| Tiempo | Módulo | Qué ve / hace la audiencia |
|---|---|---|
| 0:00–0:05 | Contexto y arquitectura | Diagrama del antes/después. El dolor real. |
| 0:05–0:20 | **Silver con Genie Code** (Prompts 1 + 2) | Bronze + Silver con `APPLY CHANGES`. SCD2 sobre CDC. |
| **0:20–0:27** | **★ Hands-on #1** | La audiencia le pide a Genie Code que convierta UNO de sus MERGEs manuales actuales a `APPLY CHANGES`, en su propio workspace. |
| 0:27–0:41 | Silver hechos + Gold (Prompts 3 + 4) | Streaming enriquecido + agregado incremental que reemplaza window functions. |
| 0:41–0:48 | Metric View + Genie Space (Prompt 5) | Capa semántica + NL queries. |
| **0:48–0:54** | **★ Hands-on #2** | La audiencia abre su propio Genie Space y le hace una pregunta de su negocio real. |
| 0:54–0:57 | Gobierno (Prompt 6) | Row filter + column masking por país. |
| 0:57–1:00 | Cierre y recursos | Repo, próximos pasos. |

---

## Estructura del repo

```
openpay-workshop/
│
├── README.md                          (este archivo)
├── docs/
│   ├── arquitectura.md                Diagrama + explicación larga
│   ├── genie_code_prompts.md          ★ Los ~7 prompts para copiar/pegar en vivo
│   └── workshop_guion.md              Guion minuto a minuto para Raquel
│
├── notebooks/
│   ├── 00_setup/
│   │   └── 00_setup.py                Crea catalog + esquemas + volumen UC
│   │
│   └── 01_data_generation/            Scaffold: genera la zona landing
│       ├── 01_gen_merchants.py        Full load + 30 días de CDC
│       ├── 02_gen_bins.py
│       ├── 03_gen_customers.py
│       ├── 04_gen_transactions.py     ~5M registros, append-only
│       ├── 05_gen_fraud_signals.py
│       └── 99_run_all.py              Orquestador
│
└── scripts/
    └── deploy_to_workspace.sh         (Opcional) Sube al workspace
```

**Todo lo que está en `bronze/`, `silver/` y `gold/` se construye en vivo con
Genie Code durante la demo** — por diseño.

---

## Cómo correr

### Prerrequisitos

- Workspace Databricks con Unity Catalog y Genie habilitado
- Databricks CLI autenticado (`databricks auth login`)
- Python 3.10+
- Permisos para crear catálogo + volumen UC en el workspace

### ★ Paso 0 — Ajustar catálogo y esquema *(todos lo hacen primero)*

**Antes de correr cualquier cosa**, abrí `notebooks/config.py` y revisá:

```python
CATALOG = "digit_payments"    # ← cambiá si ya tenés un catálogo sandbox propio
RAW_SCHEMA = "raw"
VOLUME_NAME = "landing"
```

Por default crea el catálogo `digit_payments` (si no existe) con el esquema
`raw` y el volumen `landing`. Si en tu workspace no podés crear catálogos, o
preferís reutilizar uno existente, cambia estas 3 variables. El resto de los
notebooks lo levantan automáticamente vía `%run ../config`.

### Paso 1 — Preparar el scaffold en el workspace

1. Subir la carpeta `notebooks/` al workspace (vía `databricks workspace import-dir` o la UI)
2. Correr `00_setup/00_setup` (crea catálogo + esquema + volumen — ~10s)
3. Correr `01_data_generation/99_run_all` (genera ~5.6M registros de raw — ~3-5 min)

### Paso 2 — Ejecutar el workshop

Abrir Genie Code en el workspace y seguir
[`docs/genie_code_prompts.md`](docs/genie_code_prompts.md) en orden.

### Paso 3 — Guion para quien presenta

Revisar [`docs/workshop_guion.md`](docs/workshop_guion.md) antes de arrancar.
Contiene el minuto a minuto + los **2 momentos hands-on** donde la audiencia
prueba Genie Code sobre sus propios datos reales.

---

## Por qué esta demo (y no otra)

| Enfoque típico | Esta demo |
|---|---|
| Tablas raw estáticas | **CDC incremental estilo AWS DMS** (refleja el flujo real del cliente) |
| "Ver todas las features de Genie Code" | **Resolver el dolor específico del cliente**: cerrar Silver |
| Hands-on que se cae si el wifi falla | **Demo guiada**: el presentador controla el ritmo |
| Silver aspiracional, Gold como estrella | **Silver es el protagonista**, Gold es el cierre |
