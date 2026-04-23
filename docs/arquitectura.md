# Arquitectura de la demo

## El flujo real del cliente (OpenPay, hoy)

```
┌──────────────────┐    AWS DMS    ┌──────────────┐
│ OLTP (Mongo /    │  ───────────► │   S3         │
│ ElasticSearch)   │   CDC I/U/D   │   landing/   │
└──────────────────┘   incremental └──────┬───────┘
                                          │
                                          ▼
                                  ┌──────────────────┐
                                  │  Databricks      │
                                  │  Silver          │◄── ★ bloqueo actual
                                  │  (incompleta)    │
                                  └──────┬───────────┘
                                         │
                                         ▼
                         ┌───────────────────────────┐
                         │  Dashboards consumen      │
                         │  DIRECTO de Silver con    │
                         │  window functions         │
                         │  → 15-20 min de carga     │
                         └───────────────────────────┘
```

**Pain point real** (notas del 9-abr y 31-mar-2026):
- DMS ya entrega incrementales enmascarados a S3 landing
- El equipo está completando Silver *a mano*, tabla por tabla
- Consumen Silver directo para tableros de fraude → performance pésimo
- Deadline interno: cerrar Silver a finales de mayo 2026

## Lo que la demo espejera (digit_payments)

```
┌────────────────────────────────────────────────────────────────────┐
│  LANDING (volumen UC — simula S3 que escribe DMS)                  │
│                                                                    │
│   merchants_cdc/      ← insert/update/delete con Op + ts          │
│   bins_cdc/           ← idem                                       │
│   customers_cdc/      ← idem                                       │
│   transactions_raw/   ← append-only (puede leerse en streaming)    │
│   fraud_signals_raw/  ← append-only                                │
└────────────────────┬───────────────────────────────────────────────┘
                     │
                     ▼  [ingesta con Autoloader + expectations]
┌────────────────────────────────────────────────────────────────────┐
│  BRONZE                                                            │
│  Autoloader lee del volumen, agrega metadata de auditoría           │
└────────────────────┬───────────────────────────────────────────────┘
                     │
                     ▼  [APPLY CHANGES INTO — el momento decisivo]
┌────────────────────────────────────────────────────────────────────┐
│  SILVER  ← aquí se demuestra el cierre del dolor                   │
│                                                                    │
│   merchants        (SCD Type 2, tracking histórico)                │
│   bins             (SCD Type 2)                                    │
│   customers        (SCD Type 2)                                    │
│   transactions     (streaming, enriquecida con merchant + bin)     │
│   fraud_signals                                                    │
└────────────────────┬───────────────────────────────────────────────┘
                     │
                     ▼  [agregaciones incrementales]
┌────────────────────────────────────────────────────────────────────┐
│  GOLD                                                              │
│                                                                    │
│   merchant_daily_risk                                              │
│     - reemplaza WINDOW FUNCTION(...) OVER (...) sobre Silver       │
│     - se recalcula incremental, no de golpe                        │
│                                                                    │
│   bin_risk_profile                                                 │
└────────────────────┬───────────────────────────────────────────────┘
                     │
                     ▼
┌────────────────────────────────────────────────────────────────────┐
│  CAPA SEMÁNTICA + CONSUMO                                          │
│                                                                    │
│   Metric Views (UC)          ← definición única de KPIs            │
│   Genie Space                ← NL queries para negocio             │
│   Row filter por país        ← gobierno fino                       │
└────────────────────────────────────────────────────────────────────┘
```

## Volúmenes

| Entidad | Registros | Notas |
|---|---|---|
| merchants | ~5,000 | Full load + ~50 updates/día por 30 días |
| bins | ~1,000 | Cambian poco, ~5 updates/día |
| customers | ~100,000 | Full load + ~200 updates/día |
| transactions | ~5,000,000 | 30 días, distribución no uniforme (picos nocturnos para fraude) |
| fraud_signals | ~500,000 | Correlacionados con ~0.8% de transacciones |

Total raw: ~5.6M registros. Generación local: <5 min en cluster Serverless.

## Decisiones de diseño

### Por qué SCD Type 2 y no SCD 1

Los merchants y BINs tienen atributos que cambian y a fraude le importa el
*snapshot histórico* al momento de la transacción (ej: "cuando se hizo esa
transacción, ¿ese comercio estaba en tier A o B?"). SCD2 es la única forma
correcta de responder eso sin joins imposibles.

### Por qué `APPLY CHANGES INTO` y no MERGE manual

Lakeflow Declarative Pipelines maneja la complejidad de SCD2 con una
sentencia declarativa. En la demo, Genie Code la escribe sola — sin que el
equipo de OpenPay tenga que aprender la sintaxis. Ese es el "wow".

### Por qué Metric Views después de Gold

Ya se les vendió en la sesión del 26-feb-2026. El workshop cierra el ciclo:
Gold → Metric View → Genie Space. El equipo de BI ve que la capa semántica
no es un PowerPoint, es algo que corre y Genie responde.
