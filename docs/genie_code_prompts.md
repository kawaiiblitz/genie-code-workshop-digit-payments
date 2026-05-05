# Prompts de Genie Code — `digit_payments`

Estos son los **7 prompts** para ejecutar en Genie Code durante la demo,
en orden. Cada uno asume que el anterior ya se ejecutó. Al final hay un
apéndice de bolsillo con prompts extra para Q&A.

---

## Contexto para Genie Code

Antes del primer prompt, pega este **contexto inicial** para orientar al agente:

> Este paso define a qué se está enfrentando Genie Code. Los archivos de
> landing, el esquema del CDC, los nombres de las tablas. Con esto, el
> agente no tiene que adivinar nada del dominio.

```
Estoy construyendo una plataforma de datos para un procesador de pagos
(alias: digit_payments). Tengo archivos CDC estilo AWS DMS en
/Volumes/digit_payments/raw/landing/ con estas rutas:

- merchants_cdc/        (columnas: merchant_id, merchant_name, mcc_code,
                         mcc_description, country, business_type, risk_tier,
                         monthly_volume_estimate, created_at, status, Op, ts)
- bins_cdc/             (bin, issuer_bank, card_brand, card_type, country,
                         is_prepaid, risk_flag, Op, ts)
- customers_cdc/        (customer_id, email_hash, country, tier, created_at,
                         status, Op, ts)
- transactions_raw/     (append-only: transaction_id, merchant_id, customer_id,
                         bin, card_last4, amount_mxn, currency, payment_method,
                         status, transaction_ts, is_fraud, fraud_score,
                         ip_country, device_fingerprint)
- fraud_signals_raw/    (append-only: event_id, transaction_id, signal_type,
                         signal_value, signal_ts, is_real_fraud)

Los archivos CDC tienen `Op` ∈ {I, U, D} y timestamp `ts`. Los archivos _raw
son append-only (nuevas transacciones/señales, nunca se actualizan).

Cuando tu Silver procesa ese archivo con APPLY CHANGES INTO, lee la columna Op y decide:
  - I → inserta la fila nueva                               
  - U → actualiza la existente
  - D → borra la fila (o la marca como inactiva)

Vamos a construir un medallón: bronze → silver → gold, usando Lakeflow
Declarative Pipelines. Todo bajo el catálogo `digit_payments`, en esquemas
`bronze`, `silver`, `gold` que crearemos sobre la marcha.
```

---

## Prompt maestro de estándares (opcional, recomendado)

Inmediatamente después del contexto, pega este prompt. Son las "reglas del
juego" que Genie Code va a respetar durante **toda** la sesión, así no
tienes que repetir los estándares en cada prompt.

```
Antes de empezar, sigue estas reglas en TODO lo que generes:

NAMING
- snake_case en todos los identificadores
- prefijos: dim_ para dimensiones, fact_ para hechos, stg_ para staging
- nombres de columnas descriptivos (evitar abreviaciones obscuras)

GOBIERNO (Unity Catalog)
- Toda tabla con COMMENT descriptivo (qué contiene, quién la produce)
- Columnas sensibles con COMMENT indicando sensibilidad
- Tags UC obligatorios por tabla: 'layer', 'domain', 'pii_level'

PERFORMANCE
- Liquid clustering sobre las claves de join más frecuentes
- Predictive Optimization ENABLE en esquemas gold
- Z-order en tablas que no usan liquid clustering

CALIDAD DE DATOS
- Expectations con nivel apropiado: warn para monitoreo, drop para datos
  inválidos que no deben propagarse
- Comments en cada expectativa explicando su propósito

STREAMING
- Watermark de 10 min en streams
- Deduplicación con dropDuplicatesWithinWatermark cuando aplique
- Triggers fijos (processingTime='1 minute') para control de costo

Respeta estas reglas sin que te las repita en cada prompt.
```

---

## Prompt 1 — Bronze: ingesta con Autoloader

```
Crea un Lakeflow Declarative Pipeline llamado `digit_payments_bronze` que
ingiera los 5 orígenes desde /Volumes/digit_payments/raw/landing/ hacia el
esquema digit_payments.bronze.

Usa Autoloader con cloudFiles en formato parquet. Una tabla de streaming
por origen:

- bronze.merchants_cdc_raw
- bronze.bins_cdc_raw
- bronze.customers_cdc_raw
- bronze.transactions_raw
- bronze.fraud_signals_raw

Para cada tabla:
- Añade columnas de auditoría: _ingestion_ts (current_timestamp),
  _source_file (_metadata.file_path)
- Agrega COMMENT describiendo la tabla
- Inferencia de schema automática
- Guarda el checkpoint dentro del pipeline

Agrega expectations básicas:
- En transacciones: amount_mxn > 0 (warn, no drop)
- En merchants CDC: Op debe ser 'I', 'U' o 'D' (drop si no)
```

**Qué debería generar Genie Code:** un pipeline DLT/Lakeflow con 5 tablas
streaming, decorator `@dlt.table`, `@dlt.expect_or_drop` para el `Op` válido,
Autoloader configurado.

---

## Prompt 2 — Silver dimensiones

```
En el mismo pipeline, agrega el esquema silver con las 3 dimensiones
usando APPLY CHANGES INTO:

1. silver.merchants
   - Fuente: bronze.merchants_cdc_raw
   - Claves: merchant_id
   - Secuencia: ts
   - APPLY AS DELETE WHEN Op = 'D'
   - Exceptúa las columnas Op y ts del target

2. silver.bins
   - Fuente: bronze.bins_cdc_raw
   - Claves: bin
   - Secuencia: ts
   - APPLY AS DELETE WHEN Op = 'D'

3. silver.customers
   - Fuente: bronze.customers_cdc_raw
   - Claves: customer_id
   - Secuencia: ts
   - APPLY AS DELETE WHEN Op = 'D'

Agrega expectations:
- silver.merchants: risk_tier IN ('A','B','C')
- silver.bins: risk_flag IN ('LOW','MEDIUM','HIGH')

Agrega COMMENTs descriptivos en cada tabla.
```

---

## Prompt 3 — Silver hechos: streaming enriquecido

```
Agrega al pipeline dos tablas silver más:

4. silver.transactions
   - Fuente streaming: bronze.transactions_raw
   - Streaming-static join contra silver.merchants y silver.bins (las
     versiones vigentes que APPLY CHANGES mantiene actualizadas)
   - Enriquece con: merchant_country, merchant_risk_tier, merchant_mcc,
     bin_issuer_bank, bin_card_brand, bin_card_type, bin_risk_flag
   - Genera una columna derivada transaction_hour = hour(transaction_ts)
   - Genera is_night_transaction = transaction_hour BETWEEN 2 AND 5
   - Es append-only (no CDC)

5. silver.fraud_signals
   - Fuente streaming: bronze.fraud_signals_raw
   - Sin enriquecimiento, solo limpia y tipa

Expectations en silver.transactions:
- amount_mxn BETWEEN 1 AND 1000000 (warn)
- currency = 'MXN' (warn)
- merchant_id debe existir en silver.merchants (drop) — usa la primitiva
  de referential integrity de DLT si está disponible, si no, un expect con
  subquery

Agrega tags de Unity Catalog a las tablas silver:
- 'layer' = 'silver'
- 'domain' = 'payments'
- 'pii_level' = 'low' (ya viene enmascarado desde DMS)

Agrega COMMENT a cada columna importante de silver.transactions.
```

**Genie Code** agregó:
- El streaming-static join (patrón no trivial)
- Las tags de UC (consistencia de nomenclatura)
- Comments a nivel columna (sin que lo pidieras explícitamente — Genie Code
  aprende el estándar del contexto)

---

## Prompt 4 — Gold: reemplazar window functions por agregados incrementales

```
Crea el esquema gold y dos tablas materialized view en el pipeline:

1. gold.merchant_daily_risk
   - Una fila por (merchant_id, transaction_date)
   - Métricas:
     * total_transactions
     * total_amount_mxn (sum)
     * approved_transactions, declined_transactions
     * fraud_transaction_count
     * fraud_rate (fraud_transactions / total_transactions)
     * night_transaction_pct (% de transacciones entre 2-5am)
     * unique_customers
     * avg_transaction_amount
     * max_transaction_amount
   - Calculado incremental desde silver.transactions
   - Incluir merchant_country, merchant_risk_tier para filtros

2. gold.bin_risk_profile
   - Una fila por (bin, week_start_date)
   - Métricas: total_transactions, fraud_rate, avg_amount, unique_merchants,
     signal_count_total (join con silver.fraud_signals)
   - Incluir bin_issuer_bank, bin_card_brand, bin_risk_flag vigente
```

---

## Prompt 5 — Capa semántica: Metric View + Genie Space

```
Sobre gold.merchant_daily_risk, genera una Metric View de Unity Catalog
llamada digit_payments.gold.merchant_kpis con estas métricas:

- total_volume: sum(total_amount_mxn)
- fraud_rate: sum(fraud_transaction_count) / sum(total_transactions)
- approval_rate: sum(approved_transactions) / sum(total_transactions)
- unique_merchants_active: count(distinct merchant_id)
- avg_ticket: sum(total_amount_mxn) / sum(total_transactions)

Dimensiones:
- transaction_date (con drilldown por día/semana/mes)
- merchant_country
- merchant_risk_tier

Luego, crea una Genie Space llamada "Prevención de Fraude — digit_payments"
que use como fuente:
- gold.merchant_kpis (la metric view)
- gold.bin_risk_profile

Agrega instrucciones en español al Genie Space:
- Los analistas de fraude preguntan en español
- Cuando se pregunte por 'tasa de fraude', usar la métrica fraud_rate
- Filtro default: últimos 30 días salvo que se indique otro rango
- Cuando se compare tiers de merchant, usar merchant_risk_tier

Agrega 3 preguntas ejemplo en el Genie Space:
- "¿Qué comercios tienen la tasa de fraude más alta esta semana?"
- "Muéstrame el volumen total por país los últimos 30 días"
- "¿Cuáles BINs con risk_flag HIGH generaron más fraude la semana pasada?"
```

**Punto de pausa — DEMO EN VIVO:** abre el Genie Space generado y hazle
una pregunta en lenguaje natural que NO esté en los ejemplos. Por ejemplo:
*"¿Hubo picos de fraude nocturno en merchants tier C el fin de semana?"*
Que la audiencia vea cómo Genie responde con SQL + gráfica.

---

## Prompt 6 — Gobierno: row filter por país

```
Agrega un row filter en silver.transactions y gold.merchant_daily_risk que:
- Permita ver TODAS las filas si el usuario pertenece al grupo
  `fraude_admin_global`
- Si no, solo vea filas cuyo merchant_country coincida con el country
  asociado a su grupo (`fraude_mx`, `fraude_co`, `fraude_pe`, `fraude_ar`)

Implementa como una función SQL en unity catalog + ALTER TABLE ... SET ROW
FILTER.

Agrega también column masking sobre silver.transactions.device_fingerprint:
- Usuarios del grupo `fraude_admin_global` ven el valor completo
- El resto ve una versión hasheada (sha2 de 8 chars)
```

---

## Setup de Skills (antes de las Sesiones 2 y 3)

Las Skills de Genie Code son archivos markdown que viven en
`/Workspace/.assistant/skills/`. Una vez creadas, Genie las respeta
automáticamente sin necesidad de repetir las reglas en cada prompt.

Para los prompts del dashboard (Prompt 7) y del pipeline de ML
(Prompts 8 a 10) usaremos una sola Skill que define la paleta de marca
OpenPay y reglas de estilo visual.

### Cómo crear la Skill (clickable, una vez por workspace)

1. Abre **Genie Code** en tu workspace.
2. Click el ícono ⚙️ de **Settings** del panel de Genie Code.
3. Scroll hasta la sección **Skills** y click **Create skills folder**.
   Eso genera `/Workspace/.assistant/skills/`.
4. Dentro de esa carpeta, crea un archivo nuevo
   `estilo_visual_digit_payments.md`.
5. Pega el body de abajo y guarda.

A partir de ese momento Genie aplica la paleta a cualquier dashboard
o visualización que genere para `digit_payments`.

### Body de la Skill (`estilo_visual_digit_payments.md`)

```markdown
# Estilo visual digit_payments

Cuando generes dashboards o visualizaciones para digit_payments, respeta
esta paleta de marca OpenPay y estas reglas de uso.

## Paleta de marca

- Navy primario (titulos, texto destacado):   `#1B3D6F`
- Turquesa primario (acento, volumen sano):   `#3FCBC0`

## Paleta semántica de fraude

- Rojo (fraude, peligro, alertas):     `#D62728`
- Verde (aprobaciones, sano):          `#2CA02C`
- Naranja (atención intermedia):       `#FF7F0E`
- Fondo claro del dashboard:           `#F5F5F5`
- Texto en gris carbón:                `#2A2A2A`

## Reglas

- Counters KPI: usa turquesa `#3FCBC0` para volumen y métricas neutras.
  Para fraud_rate usa rojo `#D62728`.
- Cuando un visual codifique fraud_rate por intensidad (gradiente,
  conditional formatting, heatmap), usa la rampa
  `#2CA02C` → `#FFD700` → `#D62728` (verde a rojo).
- Top N de riesgos (BINs HIGH, merchants en alerta): barras todas en
  rojo `#D62728`, son riesgo por definición.
- Mantén el mismo color por país a lo largo de todos los visuales del
  mismo dashboard.
- Títulos de widget en navy `#1B3D6F` bold. Texto en `#2A2A2A`.
```

> **Nota de Tag Policy**: si tu workspace tiene una Tag Policy que
> restringe valores del tag `domain` (común en setups corporativos),
> ajusta el valor a uno permitido. Por ejemplo en lugar de
> `domain=fraud_detection` puede que tu workspace solo permita `finance`,
> `sales`, etc. Esto aplica a todas las Feature Tables, modelos y
> tablas registradas que metan tags.

---

## Prompt 7 — Dashboard AI/BI con marca OpenPay (Sesión 2)

Este prompt asume que la Sesión 1 ya pasó: Bronze, Silver, Gold, metric
view y Genie Space ya están construidos. El Prompt 7 vive en una **Sesión
2 corta** dedicada al dashboard, no como cierre del workshop original.

**Pre-requisito**: tener creada la Skill `estilo_visual_digit_payments`
(ver sección "Setup de Skills" arriba). Esa Skill es la que hace que los
colores salgan en navy y turquesa de OpenPay sin pedirlo en el prompt.

```
Sobre las tablas Gold ya construidas (digit_payments.gold), genera un
dashboard AI/BI llamado "Prevencion de Fraude · digit_payments" con
estos 5 visuales. Todos consumen de Gold, no caigas a Silver.

1. KPI counters (4, ultimos 30 dias) sobre merchant_kpis (metric view):
   volumen total, fraud_rate, merchants activos, ticket promedio.

2. Linea: fraud_rate diaria por merchant_country (ultimos 90 dias) sobre
   merchant_kpis.

3. Tabla: top 20 merchants con mayor fraud_rate esta semana sobre
   merchant_daily_risk. Columnas: merchant_id, merchant_country,
   merchant_risk_tier, total_transactions, fraud_rate, total_amount_mxn.

4. Bar chart: fraud_rate por merchant_risk_tier (ultimos 30 dias) sobre
   merchant_kpis. Color por valor con gradiente verde-rojo.

5. Bar horizontal: top 10 BINs HIGH risk con mayor fraud_rate (esta semana)
   sobre bin_risk_profile.

Filtros globales: rango de fechas (default 30 dias), merchant_country,
merchant_risk_tier.

Aplica la Skill "Estilo visual digit_payments" para los colores. Cada
query queda como dataset nombrado y reusable.
```

**Output esperado**: 5 datasets nombrados (uno por visual), todos
consultando Gold. Las queries deben usar `MEASURE()` sobre
`gold.merchant_kpis` cuando sea posible, para que las métricas del
dashboard y del Genie Space del Prompt 5 sigan compartiendo definición.

---

# Sesión 3 — Pipeline de ML para detección de fraude

Esta sesión asume que las Sesiones 1 y 2 ya pasaron (medallón completo
en Gold, metric view, Genie Space, dashboard). Aquí construimos el
pipeline de ML completo: Feature Engineering en Unity Catalog Feature
Store, training de 3 modelos comparables con MLflow Experiments, y
registro del Champion en UC Model Registry.

**Pre-requisitos:**
- Sesiones 1 y 2 completadas (Gold + dashboard).
- Cluster all-purpose o serverless compute para notebooks (Python
  requerido). Un SQL Warehouse no alcanza para `databricks-feature-engineering`.
- Skill `estilo_visual_digit_payments` activa (opcional, aplica si los
  notebooks generan plots).

**Por qué importa esta sesión:** convierte los activos del medallón en
un activo de ML gobernado. La Feature Table es reusable por otros
equipos, el modelo se versiona en UC, todo el lineage se mantiene
end-to-end desde el CDC de DMS hasta la inferencia.

---

## Prompt 8 — Feature Engineering con arquitectura medallón

```
Construye el primer paso de un pipeline de ML para deteccion de
fraude transaccional, usando las capas Gold ya construidas y Silver
solo para features transaccionales puras.

PASO 1 - FEATURE ENGINEERING
Crea una Feature Table en
digit_payments.gold.transaction_features
con primary key compuesta (transaction_id, transaction_ts) y
timestamp_keys=[transaction_ts]. La PK compuesta es requisito de
databricks-feature-engineering cuando hay timeseries.

Parte de silver.transactions y haz point-in-time joins con las Gold
(clave: el join debe ser as-of la fecha anterior a la transaccion
para evitar leakage del propio dia).

Features transaccionales (de silver.transactions):
- transaction_hour (hour de transaction_ts)
- is_night (hour entre 2 y 5)
- amount_log (log1p de amount_mxn)
- ip_country_mismatch (1 si ip_country != merchant_country)

Features historicas del merchant (de gold.merchant_daily_risk, joineadas
as-of transaction_date - 1 dia):
- merchant_avg_amount_30d
- merchant_fraud_rate_30d
- merchant_total_transactions_30d

Features historicas del BIN (de gold.bin_risk_profile, as-of la
semana anterior):
- bin_fraud_rate_prev_week
- bin_total_transactions_prev_week

Features estaticas del customer (de silver.customers):
- customer_tenure_days

Features categoricas one-hot:
- merchant_risk_tier (tier_a, tier_b, tier_c)
- bin_risk_flag (low, medium, high)

Filtra a transaction_ts BETWEEN '2026-01-04' AND '2026-04-04'.
Usa databricks-feature-engineering con time_lookup_key = transaction_ts.
Registra la Feature Table con COMMENT explicando que las features
historicas son point-in-time correctas, y tags UC validos.
```

**Conceptos clave que se enseñan con este prompt:**

- **Feature Store como contrato**: la Feature Table es un activo
  gobernado en UC, con primary key declarada, timestamp key, lineage
  automático y permisos finos. Otros equipos la consumen con tres
  líneas (`FeatureLookup`) sin reescribir la lógica.

- **Point-in-time joins (anti-leakage)**: el offset de un día contra
  `merchant_daily_risk` y de una semana contra `bin_risk_profile` evita
  que la fraud_rate del agregado incluya la transacción que el modelo
  intenta predecir. Es la diferencia entre demo y producción.

- **Silver vs Gold para features**: las transaccionales puras nacen en
  Silver (granularidad transacción), las históricas vienen de Gold
  pre-agregadas. No recalcular lo que el medallón ya entrega.

**Errores comunes que vas a ver**:
- *"Timeseries column 'transaction_ts' is not in primary_keys"*: añade
  `transaction_ts` también a `primary_keys`. La PK compuesta es
  requisito cuando hay timeseries column.
- *"Tag value X is not allowed for tag policy key domain"*: tu
  workspace tiene Tag Policy. Cambia el valor a uno permitido (ej.
  `finance` en lugar de `fraud_detection`).
- *"Cast DATE to BIGINT failed"*: usa `unix_date()` para convertir.
  Genie suele auto-corregirlo.

---

## Prompt 9 — Training con MLflow Experiments

```
PASO 2 - ENTRENAMIENTO con MLflow Experiments

Crea un experiment en
/Users/<tu-email>/digit_payments_fraud_models y entrena 3 modelos
comparables como runs separados:

a) Logistic Regression (baseline)
b) Random Forest
c) XGBoost o sklearn GBT

Para cada run:
- Carga las features con FeatureLookup desde
  digit_payments.gold.transaction_features (timestamp_lookup_key
  transaction_ts) para que el point-in-time se respete automaticamente.
- Train/test 80/20 estratificado por is_fraud.
- class_weight='balanced' (la clase positiva esta desbalanceada,
  fraud_rate < 1%).
- mlflow.autolog activado.
- Metricas custom: F1, precision, recall, AUC-PR (NO usar AUC-ROC ni
  accuracy, son enganosas con desbalance extremo).
- Loguea matriz de confusion como artefacto.

Los 3 runs comparten experiment, dataset y split, asi son comparables
en la UI del Experiment.
```

**Conceptos clave que se enseñan con este prompt:**

- **MLflow Experiment como Excel automatizado**: cada run captura
  parámetros, métricas, código, environment y signature, sin que tú
  hagas logging manual. Comparas runs en la UI ordenando por la
  métrica que importa.

- **Métrica correcta con clases desbalanceadas**: con fraud_rate ~1%,
  un modelo que predice siempre "no fraude" tiene 99% accuracy y es
  inútil. AUC-ROC también engaña con desbalance extremo. **AUC-PR**
  (Area Under Precision-Recall Curve) es la métrica correcta.

- **`class_weight='balanced'`**: penaliza más el error en la clase
  minoritaria, evita que el modelo aprenda el truco de predecir
  siempre la mayoritaria.

- **3 modelos en escalera de complejidad**: empezar por Logistic
  Regression (baseline simple) permite medir cuánto vale la
  complejidad extra de Random Forest y XGBoost. Si baseline ya da
  AUC-PR aceptable, no metas un boosted tree solo por meterlo.

---

## Prompt 10 — Registro del Champion en Unity Catalog

```
PASO 3 - REGISTRO en Unity Catalog Model Registry

Selecciona el run con mejor AUC-PR del experiment
digit_payments_fraud_models y registralo en Unity Catalog en
digit_payments.gold.fraud_classifier con:

- alias "Champion"
- description que incluya el AUC-PR alcanzado y la fecha
- signature inferida automaticamente del input del training
- tags UC validos: layer=gold, pii_level=none, ml_task=classification

Los otros 2 modelos quedan en el experiment como referencia pero NO
se registran. Si despues quieren probar uno como Challenger, le ponen
ese alias en el siguiente registro.
```

**Conceptos clave que se enseñan con este prompt:**

- **Experiment vs Registry**: Experiments guardan TODO (incluyendo los
  malos). Registry guarda SOLO lo que se va a usar. El paso de "curar"
  los buenos es lo que separa MLOps maduro de "modelos en notebooks
  sueltos".

- **Patrón Champion-Challenger**: un alias `Champion` apunta a la
  versión que sirve producción. Cuando entrenan un Challenger, lo
  prueban en sombra. Si gana, swap de aliases sin downtime.

- **Modelo como activo de UC**: igual que una tabla, el modelo tiene
  permisos finos, lineage hacia las Feature Tables y datos de origen,
  audit trail. Cuando auditoría pregunta de dónde viene el modelo, la
  respuesta está en el lineage automático, no en un PowerPoint.

---

## Cómo otros equipos consumen y actualizan la Feature Table

Una vez registrada la Feature Table, otros equipos pueden:

### 1. Reusar features para entrenar SUS modelos (sin reescribir nada)

```python
from databricks.feature_engineering import FeatureEngineeringClient, FeatureLookup

fe = FeatureEngineeringClient()

# Su DataFrame con etiquetas (sus propias transacciones a entrenar)
labels_df = spark.table("...")

training_set = fe.create_training_set(
    df=labels_df,
    feature_lookups=[
        FeatureLookup(
            table_name="digit_payments.gold.transaction_features",
            lookup_key="transaction_id",
            timestamp_lookup_key="transaction_ts"  # respeta point-in-time
        )
    ],
    label="is_fraud"
)

training_df = training_set.load_df()
```

Cero reescribir SQL, cero recalcular features, cero riesgo de leakage.

### 2. Scorear transacciones nuevas con un modelo registrado

```python
predictions = fe.score_batch(
    model_uri="models:/digit_payments.gold.fraud_classifier@Champion",
    df=new_transactions  # solo necesita PK + timestamp
)
```

El modelo "se acuerda" de qué Feature Table necesita y resuelve el
join de features automáticamente.

### 3. Actualizar la Feature Table con datos nuevos (sin romper consumidores)

```python
# Append/merge de nuevas transacciones procesadas
fe.write_table(
    name="digit_payments.gold.transaction_features",
    df=nuevas_features,
    mode="merge"  # upsert por primary key
)
```

Como el contrato (PK + timestamp + columnas) se mantiene, los
consumidores existentes (modelos, training jobs, batch scoring) NO se
rompen.

### 4. Agregar una columna nueva (feature nueva)

```python
fe.write_table(
    name="digit_payments.gold.transaction_features",
    df=df_con_nueva_columna,  # incluye la columna nueva
    mode="merge"
)
```

Feature Store hace `ALTER TABLE ADD COLUMN` automáticamente. Los
consumidores que no piden la nueva columna en sus `FeatureLookup`
simplemente la ignoran. **Operación segura.**

### 5. Cambiar la definición de una feature existente · ⚠️ NO en sitio

Si la fórmula de `merchant_fraud_rate_30d` cambia (por ejemplo, ahora
excluye merchants nuevos), **NO sobrescribas la columna**. Crea una
versión nueva:

```python
fe.create_table(
    name="digit_payments.gold.transaction_features_v2",
    primary_keys=["transaction_id", "transaction_ts"],
    timestamp_keys=["transaction_ts"],
    df=df_con_definicion_nueva,
    description="V2 con merchant_fraud_rate_30d excluyendo merchants nuevos"
)
```

Migra a los consumidores gradualmente. Cuando todos estén en `v2`,
deprecan `v1`. Esta disciplina evita divergencias silenciosas entre
equipos.

### Regla mental para mantener compatibilidad

| Operación | Seguridad | Cómo |
|---|---|---|
| Agregar filas nuevas | ✅ Segura | `fe.write_table` mode merge |
| Agregar columna nueva | ✅ Segura | `fe.write_table` con DataFrame que la incluye |
| Renombrar columna | ⚠️ Rompe | Crear `_v2`, migrar consumidores |
| Cambiar tipo de columna | ⚠️ Rompe | Crear `_v2`, migrar consumidores |
| Cambiar definición de feature existente | ⚠️ Rompe | Crear `_v2`, migrar consumidores |
| Eliminar columna | ⚠️ Rompe | Avisar consumidores antes |

---

## Apéndice de bolsillo — Otros prompts si sobra tiempo

### Streaming inference

```
Sobre el modelo digit_payments.gold.fraud_classifier@Champion, monta
un Lakeflow Pipeline en streaming que score nuevas transacciones de
silver.transactions en tiempo real, usando fe.score_batch internamente.
Output: digit_payments.gold.transaction_scores con (transaction_id,
fraud_score, scored_at).
```

### Online Feature Store (latencia <50ms)

```
Sincroniza digit_payments.gold.transaction_features a una Online Table
para servir features con latencia <50ms desde un Model Serving endpoint.
Configura run_continuously=true para que las actualizaciones se
propaguen automaticamente.
```
