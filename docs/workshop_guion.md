# Guion del workshop — minuto a minuto

**Audiencia:** desarrolladores del equipo de datos de OpenPay
(Víctor Quevedo, Enrique Garduño, José Carlos Hernández y su equipo).

**Duración:** 1 hora, demo guiada.

**Tu herramienta principal:** Genie Code en el workspace
`fevm-serverless-stable-rtpa` con el catálogo `digit_payments` ya preparado.

**Lo que ellos esperan:** vieron el repo de Miguel para Bitso. Esperan
algo similar. Vas a darles algo **mejor para su caso**: en vez de una demo
de marketing/ML, vas a resolver su dolor real — cerrar Silver desde CDC de
DMS.

---

## Antes del workshop (checklist)

- [ ] **Paso 0 de `config.py`:** verificá que `CATALOG = "digit_payments"`
      (o el que vayas a usar) esté correcto **antes de correr el scaffold**.
- [ ] El día anterior: correr `00_setup` y luego `99_run_all` en el
      workspace. Verificar que los 5 folders del landing tengan archivos.
- [ ] Abrir Genie Code y **probar el Prompt 1 y 2** en una sesión
      desechable. Si algo falla, debuggealo antes de la demo.
- [ ] **Avisá al cliente que lleguen con Genie Code abierto** en su
      workspace dev y una tabla CDC a la mano para el Hands-on #1.
      Sugerencia de email 24h antes:
      > *"Mañana vamos a tener 2 momentos donde ustedes van a usar Genie
      > Code con sus propios datos. Para aprovecharlo al máximo, lleguen
      > con: (1) Genie Code abierto en su workspace dev, (2) un MERGE que
      > hayan escrito a mano esta semana sobre algún CDC, y (3) una
      > pregunta de negocio que quieran probar en Genie."*
- [ ] Tener un browser con 3 tabs abiertas:
  1. Genie Code
  2. El Unity Catalog Explorer del catálogo `digit_payments`
  3. Este guion
- [ ] Zoom con pantalla compartida probado
- [ ] **Plan B:** si Genie Code tarda mucho o falla en vivo, el repo tiene
      los notebooks de referencia — *pero evita caer ahí*, el valor de la
      demo es verlo escribirse solo.
- [ ] **Plan B para los hands-on:** si nadie del equipo trajo un MERGE real,
      tené un ejemplo sintético guardado que les puedas mostrar y pedirles
      que lo adapten mentalmente a su caso.

---

## 0:00 – 0:05 — Apertura y contexto

**Tu enganche (di esto literal o algo parecido):**

> *"Antes de arrancar, una pregunta rápida: ¿cuántos de ustedes están hoy
> escribiendo MERGEs a mano sobre los CDC que aterrizan desde DMS en S3?
> ... OK, vamos a resolver eso en la próxima hora.*
>
> *La sesión del 9 de abril fue clara: Silver incompleta bloquea todo.
> Los dashboards tardan 15-20 minutos porque están leyendo directo de
> Silver con window functions caras. El plan que acordamos con Asiel y
> Francisco fue habilitar una Gold ligera, pero para llegar a Gold
> necesitan Silver terminada primero.*
>
> *Hoy les quiero mostrar cómo cerrar esa Silver usando Genie Code —
> el agente de IA que viene dentro del workspace. Vamos a construir un
> pipeline de medallón completo, con CDC, SCD Type 2, agregados Gold,
> Metric Views, Genie Space y row filters — en una hora, sin escribir
> una sola línea de código a mano."*

**Muestra el diagrama de arquitectura** (`docs/arquitectura.md`). Señala:
- La zona landing (≈ su S3 con DMS)
- Bronze como ingesta sin magia
- **Silver como el foco** del día
- Gold como el cierre

**Pregunta a la audiencia para engagement:** *"¿Hoy cómo están manejando
los deletes que viene del CDC? Porque ese suele ser el caso borde que
tuerce el brazo."* Deja que respondan — ahí ya enganchaste.

---

---

> **Formato del workshop:** demo guiada + 2 momentos hands-on.
> **Vos** corrés los Prompts 1-6 en tu pantalla. La audiencia **te sigue con
> la vista**, y en dos momentos específicos (0:20 y 0:48) pausás para que
> **ellos le pidan algo a Genie Code en su propio workspace, sobre sus
> datos reales**. Esto cumple la política STS (ellos al teclado) y les da
> el "aha moment" con data propia.

---

## 0:05 – 0:08 — Prompt 1 (Bronze)

**Qué vas a decir mientras Genie Code trabaja:**

> *"Le estoy pidiendo que monte la ingesta de los 5 orígenes con
> Autoloader. Lo importante aquí no es el código que escriba — es que
> no voy a escribir nada manual."*

Pega el **Prompt 1** de `genie_code_prompts.md`.

Mientras corre (30-60s), señala a la audiencia:
- Están viendo un agente leer su contexto, escoger Autoloader,
  configurar checkpoints y agregar auditoría sin que se lo indicaras
  explícitamente
- Las expectations declarativas son la base de la calidad

**Cuando termine:** muestra el código resultante. No lo leas entero —
señala solo estas 2-3 cosas:
- El decorator `@dlt.table` con comment
- Las expectations sobre `Op`
- Las columnas de auditoría `_ingestion_ts` y `_source_file`

---

## 0:08 – 0:20 — Prompt 2 (Silver SCD2) — **★ ESTRELLA ★**

> *"Este es el prompt que vale el workshop. Voy a pedirle Silver con SCD
> Type 2 sobre los tres CDCs — merchants, bins, customers. Esto es lo que
> el equipo de Víctor está haciendo a mano hoy."*

Pega el **Prompt 2**.

**Mientras Genie Code escribe**, explica:
- Qué es SCD Type 2 en términos simples: *"Cada cambio al risk_tier de un
  comercio genera una nueva fila con `__START_AT` y `__END_AT`. La fila
  vigente tiene `__END_AT IS NULL`. Así, cuando analicen fraude de hace 3
  meses, pueden responder con qué tier estaba el comercio ese día — no el
  tier de hoy."*
- Por qué importa para ellos: **el motor de fraude tiene que poder
  reconstruir el contexto histórico**. Sin SCD2, si un comercio pasó de
  tier A a C, todas sus transacciones pasadas "parecen" haber sido de un
  comercio tier C. Falso.

**Cuando Genie Code termine**, señala literalmente:

```python
dlt.apply_changes(
    target="silver.merchants",
    source="bronze.merchants_cdc_raw",
    keys=["merchant_id"],
    sequence_by="ts",
    apply_as_deletes=expr("Op = 'D'"),
    stored_as_scd_type=2,
    track_history_column_list=["risk_tier", "monthly_volume_estimate", "status"],
)
```

Decí en voz alta: *"Esta llamada reemplaza ~60 líneas de MERGE manual
con lógica de versionado. Y lo que acaban de ver es que Genie Code ya
sabe el dialecto de Lakeflow Declarative Pipelines; no tienen que
aprenderlo."*

**Pausa breve para preguntas** (1 min). Si hay muchas dudas, diles que las
retomás al final — el momento hands-on viene YA.

---

## 0:20 – 0:27 — ★ HANDS-ON #1 — "Convertí tu MERGE manual"

> **Este es el primer punto donde el teclado pasa a ellos.** Vos sólo guías.

**Qué les decís (literal):**

> *"Paren. Ahora van ustedes. Cada uno abra Genie Code en su workspace dev.
> Tomen UNO de los MERGEs que hayan escrito a mano esta semana contra sus
> CDCs — no importa cuál, el más chiquito que tengan — y pídanle a Genie
> Code que lo reescriba como `APPLY CHANGES INTO` en un Lakeflow Declarative
> Pipeline. Les doy 5 minutos. Luego me cuentan qué les generó."*

**Prompt sugerido para que les dictés** (si alguien no sabe cómo empezar):

```
Tengo esta tabla CDC que viene de AWS DMS con columnas Op (I/U/D) y ts:
[que ellos peguen el schema o el DDL de su tabla real]

Y este MERGE que escribí a mano:
[que peguen su MERGE]

Convertilo a APPLY CHANGES INTO sobre un Lakeflow Declarative Pipeline,
guardándolo como SCD Type 2 si la tabla lo amerita. Agregá expectations
básicas y comments.
```

**Tu rol durante estos 7 min:**
- Mirá que nadie quede trabado. Si ves a alguien sin escribir, acercate virtualmente
- NO escribas vos. Si sugieren, suger en voz alta: *"probá agregarle X al prompt"*
- Si a Víctor le salió algo raro, hacelo hablar: *"Víctor, ¿qué generó? Compartí pantalla 30 seg"*

**Los últimos 2 minutos del módulo:** dejá que 1-2 compartan qué obtuvieron.
Esto genera conversación orgánica y marca el tono de "sí, funciona con
nuestros datos".

**Si nadie quiere compartir:** no fuerces. Decí *"OK, seguimos — cuando
quieran me cuentan qué les salió"* y arrancá el Prompt 3. Van a estar
pensando en lo suyo mientras vos hablás.

---

## 0:27 – 0:34 — Prompt 3 (Silver hechos con streaming enriquecido)

> *"Ahora las transacciones. Append-only, streaming, enriquecidas con
> merchants y BINs vigentes. Y agrego tags de UC porque Víctor mencionó
> el tema de nomenclatura consistente."*

Pega el **Prompt 3**.

**Mientras corre**, explica el stream-static join:
*"Genie Code va a hacer un join entre el stream de transacciones y la
versión estática vigente de merchants/bins. Es un patrón no trivial —
si lo escribieran a mano tendrían que acordarse de filtrar por
`__END_AT IS NULL`. Miren qué hace."*

**Cuando termine**, muestra:
- El filtro `__END_AT IS NULL` (demuestra que Genie Code entiende SCD2)
- Las UC tags (`'pii_level' = 'low'` — cumplimiento)
- Los comments a nivel columna

---

## 0:34 – 0:41 — Prompt 4 (Gold — el ataque directo al pain)

> *"Ahora sí, ataquemos el pain de los 15-20 minutos. ¿Qué tan caros son
> sus window functions sobre Silver? Vamos a reemplazarlos con un
> agregado incremental en Gold."*

Pega el **Prompt 4**.

**Cuando termine**, abre un notebook lateral y ejecuta:

```sql
SELECT merchant_id, merchant_country, fraud_rate, total_transactions
FROM digit_payments.gold.merchant_daily_risk
WHERE transaction_date >= current_date() - 7
ORDER BY fraud_rate DESC
LIMIT 20;
```

Tiempo de respuesta: <1s. **Contraste verbal:**

> *"Lo que acaban de ver — 20 comercios con más fraude en la última
> semana — en sub-segundo. Ustedes esa query la corren sobre Silver con
> WINDOW y tarda 15 minutos. Esa es la diferencia de tener Gold ligera y
> calculada incremental."*

---

## 0:41 – 0:48 — Prompt 5 (Metric View + Genie Space)

> *"Lo que sigue es la capa semántica — esto ya se los había mencionado
> el 26 de febrero, Metric Views. Hoy lo vamos a crear, y encima le
> pegamos un Genie Space para que Andrea, Enrique y el equipo de negocio
> pregunten en lenguaje natural."*

Pega el **Prompt 5**.

**Cuando termine**, abre el Genie Space generado.

**DEMO EN VIVO — improvisá una pregunta que NO esté en los ejemplos.**
Sugerencias:
- *"¿Cuáles son los 5 comercios con mayor volumen de fraude nocturno la
  semana pasada?"*
- *"¿La tasa de fraude de BBVA México es más alta que la de Santander
  México?"*
- *"¿En qué país la fraud_rate subió más en los últimos 15 días?"*

Deja que la audiencia proponga una pregunta. Si alguien del equipo pregunta
algo específico de OpenPay, mejor — más real.

---

## 0:48 – 0:54 — ★ HANDS-ON #2 — "Pregúntale algo real a Genie"

> **Segundo momento donde ellos conducen.**

**Qué les decís:**

> *"Ahora ustedes. Abran Genie en su workspace — si ya tienen un Genie Space
> configurado, perfecto; si no, pueden hacer la pregunta directo sobre una
> tabla Silver suya. Háganle UNA pregunta de negocio real: algo que Enrique
> o Andrea les pidió esta semana y todavía no respondieron. 5 minutos.
> Después me cuentan si Genie lo resolvió, lo resolvió a medias, o falló."*

**Prompt sugerido si no se les ocurre qué preguntar:**

```
[En su Genie Space o sobre su tabla Silver]:
"¿Cuáles fueron los 10 comercios con más transacciones declinadas la semana
pasada, agrupados por país?"
```

O algo del estilo de sus propios tableros de fraude.

**Tu rol:**
- Si alguien dice *"Genie no entendió"*, pregúntale qué palabras usó. Ayúdalo
  a reformular SIN dictarle el prompt entero. *"Probá darle el nombre exacto
  de la columna"* o *"decile qué period es 'semana pasada'"*
- Si a alguien le sale perfecto, pedíle que lo comparta — ejemplo real > ejemplo tuyo

**Objetivo del módulo:** que vean que Genie no es magia, es una herramienta
con la que iteran. La primera pregunta quizás sale mediocre; la tercera ya
sale bien. **Ese aprendizaje es el workshop.**

---

## 0:54 – 0:57 — Prompt 6 (Row filter + column mask)

> *"El último prompt — gobierno. Víctor ya planteó en la sesión del 9
> de abril que quieren segmentar por país para el training de Genie y
> para el acceso a datos. Aquí va."*

Pega el **Prompt 6**.

**Mientras corre**, menciona:
- Row filter ejecutado por UC, no por la query → no se puede saltar
- Column masking del `device_fingerprint` para proteger info sensible
- Todo declarativo, versionable en Git

---

## 0:57 – 1:00 — Cierre y siguientes pasos

**Cerrá con estos 3 puntos:**

1. **Lo que acabamos de hacer en 1 hora:** CDC bronze → SCD2 silver →
   Gold agregada → Metric View → Genie Space → Row filter. Todo
   declarativo, todo productivo.

2. **Lo que ustedes pueden hacer el lunes:** apuntar Genie Code a sus
   archivos reales de DMS y empezar a cerrar Silver con APPLY CHANGES.
   No necesitan re-escribir sus MERGEs — pídanle a Genie Code que los
   convierta a APPLY CHANGES.

3. **El compromiso del 9 de abril sigue en pie:** STS acompaña pero no
   ejecuta. Lo que vieron hoy es exactamente el tipo de handoff — les
   muestro cómo se hace, el equipo de Víctor lo aplica.

**Compartir el repo:**

> *"Les dejo el repo con los datos mock y los prompts que usé — pueden
> replicar esta demo en su workspace y usarla como template."*

URL del repo: (subilo a GitHub antes de la sesión) o comparte vía
Databricks workspace.

**Preguntas abiertas.** Si sobra tiempo, apéndice de `genie_code_prompts.md`
tiene los prompts de "bolsillo" para ML, alertas y streaming real-time.

---

## Plan B — si algo falla en vivo

**Si Genie Code no responde / da error:**
- No te trabes. Di: *"Esto pasa a veces con cualquier LLM. Voy al plan B."*
- Abre el notebook de referencia del repo correspondiente al módulo
- Corre el código ya escrito, explícalo como si Genie Code lo hubiera
  generado (la audiencia no nota la diferencia si no titubeás)

**Si una tabla Silver/Gold no se puebla:**
- El pipeline puede tardar 2-3 min la primera vez en arrancar
- Usá el tiempo para hacer preguntas a la audiencia o explicar más del
  contexto arquitectónico

**Si alguien pregunta algo muy técnico que no sabes:**
- *"Buena pregunta — déjame agendarlo para la próxima sesión hands-on,
  que esa sí va a ser en su workspace."* Liga con el compromiso de la
  sesión hands-on pendiente.

---

## Notas para vos (Raquel)

- Tu audiencia son devs técnicos. No los trates con guantes — si algo es
  complejo, decilo. Respetan cuando se habla con precisión.
- El `alias` `digit_payments` es intencional para no mencionar OpenPay en
  pantalla. Si alguien pregunta por qué, decí que es un ejemplo neutro.
- **No compares con otros procesadores** (ni Conekta ni MercadoPago). La
  demo es sobre Genie Code, no sobre competencia.
- El repo de Miguel está linkeado — si alguien quiere ver un caso de ML
  más pesado, referilo a eso. No dupliques.
- **Tu punch line** para el final: *"Lo que les acabo de mostrar se
  replica en su workspace el lunes. El costo de entrada es cero —
  Genie Code ya está incluido."*
