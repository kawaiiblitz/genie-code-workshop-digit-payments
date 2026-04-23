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

## Decisiones de la demo (no prescripciones arquitectónicas)

### Por qué aparece SCD Type 2

SCD Type 2 es útil cuando quieres reconstruir el estado histórico de una
dimensión (ej: "cuando pasó esta transacción, ¿qué tier tenía este
comercio?"). No es la única forma de manejar cambios — SCD1 (sobreescribir)
es válido si no necesitas historia. La demo muestra SCD2 porque es un
patrón que Genie Code escribe muy bien en pocas líneas; si un equipo
prefiere SCD1, también lo genera.

### Por qué aparece `APPLY CHANGES INTO`

Es la sentencia declarativa de Lakeflow Declarative Pipelines para manejar
SCD2 sin escribir MERGE a mano. Es poderosa como demo porque en ~8 líneas
reemplaza lo que en MERGE toma 50. Pero si el equipo tiene MERGEs que
funcionan, no hay razón de reemplazarlos mañana — Genie Code los escribe
igual de rápido si se los pides.

### Por qué aparece Metric Views + Genie Space

Cerrar el ciclo end-to-end: desde el landing hasta una pregunta en
lenguaje natural respondida por Genie. La capa semántica y el Genie
Space son útiles si el equipo tiene analistas que preguntan mucho de lo
mismo — pero no son obligatorios para usar el resto de lo que se mostró.
