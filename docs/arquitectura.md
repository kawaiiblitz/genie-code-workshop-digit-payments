# Arquitectura de la demo

## Flujo típico de un procesador de pagos con CDC

```
┌──────────────────┐    AWS DMS    ┌──────────────┐
│ OLTP (           │  ───────────► │   S3         │
│ ElasticSearch)   │   CDC I/U/D   │   landing/   │
└──────────────────┘               └──────┬───────┘
                                          │
                                          ▼
                                  ┌──────────────────┐
                                  │  Databricks      │
                                  │  Silver          │
                                  │                  │
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

## DB Sintética

| Entidad | Registros | Notas |
|---|---|---|
| merchants | ~5,000 | Full load + ~50 updates/día por 30 días |
| bins | ~1,000 | Cambian poco, ~5 updates/día |
| customers | ~100,000 | Full load + ~200 updates/día |
| transactions | ~5,000,000 | 30 días, distribución no uniforme (picos nocturnos para fraude) |
| fraud_signals | ~500,000 |  
