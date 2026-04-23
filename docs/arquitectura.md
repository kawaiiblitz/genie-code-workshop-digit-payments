# Arquitectura de la demo

## Flujo típico de un procesador de pagos con CDC

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

**Qué tipo de escenario representa esta demo:**
- DMS entrega incrementales enmascarados a S3 landing
- El equipo escribe transformaciones para poblar Silver
- Dashboards y modelos consumen de Silver (o de Gold, si la tienen)

La demo **no propone una arquitectura correcta** — muestra cómo Genie Code
puede acelerar cada capa de un escenario de medallón con CDC, que es un
patrón común en procesadores de pagos. Si un equipo ya tiene su propia
forma de estructurar esto, Genie Code se adapta a sus convenciones.

## Lo que la demo espejea (digit_payments)

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

