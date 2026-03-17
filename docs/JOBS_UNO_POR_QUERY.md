# Jobs NPS: uno por query (evitar timeout)

El job único **NPS_AI_QUERIES_SELLERS** hacía timeout porque las queries son pesadas. La solución es **un job por query**: cada uno corre solo, escribe su tabla y puede ejecutarse en paralelo.

---

## Cómo correrlos

- **Dataset destino (todos):** `meli-bi-data.SBOX_NPS_ANALYTICS`
- Crear en Data Flow (Fury) **un job por fila** de la tabla de abajo.
- Cada job tiene **un solo step** tipo "BigQuery - Execute" que ejecuta la query y escribe en la tabla indicada.
- Podés correr todos los jobs en paralelo; no hay dependencias entre ellos.
- El modelo NPS Sellers (Checkpoint 2) lee estas tablas cuando `usar_tablas_dataflow: true` en `config.yaml`.

---

## Listado: 1 job = 1 query = 1 tabla

| # | Nombre del job (sugerido) | Tabla a escribir | Query (archivo en el repo) |
|---|---------------------------|------------------|----------------------------|
| 1 | **NPS_AI_QUERY_SEGMENTACION** | `SBOX_NPS_ANALYTICS.SEGMENTATION_SELLERS` | `src/nps_model/sql/enrichment_segmentacion.sql` |
| 2 | **NPS_AI_QUERY_TRANSACCIONES** | `SBOX_NPS_ANALYTICS.TRANSACCIONES_SELLERS` | `src/nps_model/sql/enrichment_transacciones.sql` |
| 3 | **NPS_AI_QUERY_CREDITS** | `SBOX_NPS_ANALYTICS.CREDITS_SELLERS` | `src/nps_model/sql/enrichment_credits.sql` |
| 4 | **NPS_AI_QUERY_REMUNERADA** | `SBOX_NPS_ANALYTICS.REMUNERADA_SELLERS` | `src/nps_model/sql/enrichment_inversiones.sql` |

Opcionales (si los usás más adelante):

| # | Nombre del job | Tabla | Query |
|---|----------------|-------|--------|
| 5 | NPS_AI_QUERY_CREDITOS_FUTURO | `SBOX_NPS_ANALYTICS.CREDITOS_FUTURO` | `src/nps_model/sql/creditos_futuro.sql` |
| 6 | NPS_AI_QUERY_POTS_POR_MES | `SBOX_NPS_ANALYTICS.POTS_POR_MES` | `src/nps_model/sql/pots_por_mes_futuro.sql` (ajustar: es un CTE, hay que armar SELECT final) |
| 7 | NPS_AI_QUERY_SEGMENTACION_FUTURO | `SBOX_NPS_ANALYTICS.SEGMENTACION_SELLERS_FUTURO` | `src/nps_model/sql/segmentacion_sellers_futuro.sql` |

---

## Parámetros en las queries

Algunas queries usan placeholders que hay que reemplazar o parametrizar en el job:

- **enrichment_segmentacion.sql:** `{sites}`, `{fecha_minima}`, `{fecha_maxima}`
- **enrichment_transacciones.sql:** `{sites}`, `{fecha_minima}`, `{fecha_maxima}`
- **enrichment_credits.sql:** `{sites}`, `{fecha_minima_month}`, `{fecha_maxima_month}`
- **enrichment_inversiones.sql:** `{sites}`, `{fecha_minima}`, `{fecha_maxima}`

Valores típicos:

- `sites`: `'MLA','MLB','MLM','MLC','MLU','MCO','MPE'`
- `fecha_minima` / `fecha_maxima`: ventana de ~14 meses atrás desde el mes actual (como en el job anterior).

En Data Flow podés definir variables de job y usarlas en la query, o reemplazar a mano antes de pegar la SQL en el step.

---

## Crear cada job en Data Flow (resumen)

1. Entrar a **data-flow.adminml.com** (rol NPS-Commiter o el que corresponda).
2. **Create new job**.
3. Nombre: ej. `NPS_AI_QUERY_SEGMENTACION`.
4. Un solo nodo: **BigQuery - Execute**.
   - Query: contenido del `.sql` correspondiente (con parámetros reemplazados o configurados).
   - Destino: escribir resultado en `meli-bi-data.SBOX_NPS_ANALYTICS.[NOMBRE_TABLA]` (según la tabla de arriba). Si el step no permite “write to table” directo, usar en la query algo tipo `CREATE OR REPLACE TABLE \`meli-bi-data.SBOX_NPS_ANALYTICS.SEGMENTATION_SELLERS\` AS ( ... )` con la query actual dentro del paréntesis.
5. Guardar y ejecutar.
6. Repetir para los otros 3 jobs (y los opcionales si los usás).

Cuando los 4 jobs principales terminen, las tablas estarán actualizadas y el modelo (con `usar_tablas_dataflow: true`) usará esos datos en el Checkpoint 2.

---

## SQL listos para pegar (generados)

En el repo podés generar los 4 SQL con parámetros ya reemplazados y envueltos en `CREATE OR REPLACE TABLE`:

```bash
python scripts/generar_sql_dataflow_jobs.py
```

Se crean en **`scripts/dataflow_ready/`**:

- `NPS_AI_QUERY_SEGMENTACION.sql`
- `NPS_AI_QUERY_TRANSACCIONES.sql`
- `NPS_AI_QUERY_CREDITS.sql`
- `NPS_AI_QUERY_REMUNERADA.sql`

Abrí cada archivo, copiá todo el contenido y pegalo en el step "BigQuery - Execute" del job correspondiente en Data Flow. No hace falta cambiar nada; la ventana de fechas es 14 meses hasta el inicio del mes actual.
