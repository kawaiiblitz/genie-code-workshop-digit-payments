# Prompts de Genie Code — `digit_payments`

Estos son los **6 prompts** que Raquel copia/pega en Genie Code durante la
demo, en orden. Cada uno asume que el anterior ya se ejecutó.

> **Tip para la demo:** abre Genie Code con el contexto del catálogo
> `digit_payments` ya cargado. Antes de pegar cada prompt, lee en voz alta
> **qué vas a pedirle** para que la audiencia sepa qué esperar ANTES de ver
> el código que Genie Code va a escribir.

> **Idioma:** Genie Code entiende español perfectamente. Los prompts están
> en español para que la audiencia los lea contigo. Si prefieres inglés,
> son intercambiables.

---

## Contexto para Genie Code

Antes del primer prompt, pega este **contexto inicial** para orientar al agente:

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

Vamos a construir un medallón: bronze → silver → gold, usando Lakeflow
Declarative Pipelines. Todo bajo el catálogo `digit_payments`, en esquemas
`bronze`, `silver`, `gold` que crearemos sobre la marcha.
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

## Prompt 2 — Silver dimensiones: SCD Type 2 con `APPLY CHANGES`

> **Este es el momento estrella del workshop.** Aquí es donde el equipo de
> OpenPay ve que Genie Code les resuelve en 2 minutos lo que les ha tomado
> semanas.

```
En el mismo pipeline, agrega el esquema silver con las 3 dimensiones como
SCD Type 2 usando APPLY CHANGES INTO:

1. silver.merchants
   - Fuente: bronze.merchants_cdc_raw
   - Claves: merchant_id
   - Secuencia: ts
   - Columnas a trackear (SCD2): risk_tier, monthly_volume_estimate, status
   - Ignora el resto para SCD2 (solo actualiza in-place)
   - APPLY AS DELETE WHEN Op = 'D'
   - Exceptúa las columnas Op y ts del target

2. silver.bins
   - Fuente: bronze.bins_cdc_raw
   - Claves: bin
   - Secuencia: ts
   - Trackea risk_flag como SCD2 (es lo único importante históricamente)
   - APPLY AS DELETE WHEN Op = 'D'

3. silver.customers
   - Fuente: bronze.customers_cdc_raw
   - Claves: customer_id
   - Secuencia: ts
   - Trackea tier, country como SCD2
   - APPLY AS DELETE WHEN Op = 'D'

Agrega expectations:
- silver.merchants: risk_tier IN ('A','B','C')
- silver.bins: risk_flag IN ('LOW','MEDIUM','HIGH')

Agrega COMMENTs explicando para qué sirve cada tabla y que el tracking
histórico es crítico para auditoría de fraude (poder responder "cuando
ocurrió esta transacción, ¿este merchant estaba en qué tier?").
```

**Punto de pausa en la demo:** cuando Genie Code termine, muestra el código
generado y señala literalmente la línea de `APPLY CHANGES INTO ... STORED AS
SCD TYPE 2`. Esa línea *reemplaza* ~50 líneas de MERGE manual con lógica de
válido-desde / válido-hasta que OpenPay escribiría a mano.

---

## Prompt 3 — Silver hechos: streaming enriquecido

```
Agrega al pipeline dos tablas silver más:

4. silver.transactions
   - Fuente streaming: bronze.transactions_raw
   - Join (con streaming-static join contra silver.merchants vigentes y
     silver.bins vigentes — es decir, con __END_AT IS NULL)
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

**Punto de pausa:** mostrá que Genie Code agregó:
- El streaming-static join (patrón no trivial)
- Las tags de UC (consistencia que OpenPay pidió)
- Comments a nivel columna (sin que lo pidieras explícitamente — Genie Code
  aprende el estándar del contexto)

---

## Prompt 4 — Gold: reemplazar window functions por agregados incrementales

> **Este prompt ataca el pain del 9-abr directamente: window functions
> sobre Silver que revientan el performance.**

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

Comentá en un bloque markdown (dlt.markdown si existe, o como COMMENT en SQL)
POR QUÉ estas Gold existen: reemplazan queries con WINDOW FUNCTION sobre
Silver que tomaban 15-20 minutos. Ahora el cálculo es incremental y los
dashboards consultan Gold directamente.
```

**Punto de pausa crítico:** abre un notebook lateral rápido y corre un
`SELECT * FROM gold.merchant_daily_risk ORDER BY fraud_rate DESC LIMIT 20`.
Tiempo de respuesta: sub-segundo. Compara verbalmente con los 15-20 min de
sus dashboards actuales.

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
tú una pregunta en lenguaje natural que NO esté en los ejemplos. Por ejemplo:
*"¿Hubo picos de fraude nocturno en merchants tier C el fin de semana?"*
Que la audiencia vea que Genie responde con SQL + gráfica.

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

**Cierre de la demo:** señala que este gobierno se escribió en ~30 segundos
con Genie Code, vs. horas de documentación y testing manual. Y que todo
quedó en el pipeline declarativo — reproducible, versionable en Git,
idéntico en dev/prod.

---

## Apéndice: prompts de "bolsillo" para Q&A

Si alguien de la audiencia pregunta algo específico, ten listos estos
prompts cortos para demostrar que Genie Code resuelve ad hoc:

**"¿Cómo agregaría alertas si la fraud_rate supera 2%?"**
```
Agrega un job de Databricks que corra diariamente una query contra
gold.merchant_daily_risk buscando merchants con fraud_rate > 0.02 en el
último día, y envíe un email al equipo de fraude con los resultados.
```

**"¿Puede hacer lo mismo pero streaming/real-time?"**
```
Modifica gold.merchant_daily_risk a una ventana de 1 hora en streaming
sobre silver.transactions (watermark 10 min), y crea una alerta que
dispare cuando fraud_rate horaria > 5%.
```

**"¿Cómo entrenamos un modelo de fraude sobre esto?"**
```
Sobre silver.transactions, genera un notebook que:
- Cree features: transaction_hour, is_night, amount_vs_merchant_avg,
  customer_tenure_days, bin_risk_flag one-hot
- Entrene un GBT classifier con MLflow para predecir is_fraud
- Registre el modelo en UC como digit_payments.ml.fraud_classifier
```
