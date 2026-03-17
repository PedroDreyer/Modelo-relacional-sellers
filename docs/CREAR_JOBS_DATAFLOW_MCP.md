# Crear los 4 jobs en Data Flow vía MCP

El modelo **ya está configurado** para consumir las tablas que escriben estos jobs:
- `config.yaml` → `enriquecimiento.usar_tablas_dataflow: true`
- `config.yaml` → `enriquecimiento.dataset_dataflow: "SBOX_NPS_ANALYTICS"`

Cuando el **MCP Dataflow** esté conectado en Cursor, pedí al agente:

> "Creá los 4 jobs de Data Flow usando el manifiesto `scripts/dataflow_ready/dataflow_jobs_manifest.json`: un job por entrada, nombre = job_name, un step BigQuery Execute con el contenido del sql_file correspondiente. Cada query ya tiene CREATE OR REPLACE TABLE y escribe en meli-bi-data.SBOX_NPS_ANALYTICS.[table_name]."

## Listado (1 job = 1 query = 1 tabla)

| Job name | Tabla destino |
|----------|----------------|
| NPS_AI_QUERY_SEGMENTACION | SBOX_NPS_ANALYTICS.SEGMENTATION_SELLERS |
| NPS_AI_QUERY_TRANSACCIONES | SBOX_NPS_ANALYTICS.TRANSACCIONES_SELLERS |
| NPS_AI_QUERY_CREDITS | SBOX_NPS_ANALYTICS.CREDITS_SELLERS |
| NPS_AI_QUERY_REMUNERADA | SBOX_NPS_ANALYTICS.REMUNERADA_SELLERS |

Los SQL listos están en **`scripts/dataflow_ready/`** (mismo nombre que `job_name` + `.sql`). Cada archivo incluye el `CREATE OR REPLACE TABLE ... AS (...)` completo; el step del job solo debe ejecutar ese SQL.

## Si creás los jobs a mano en Fury

1. Entrá a Data Flow (Fury).
2. Para cada job: Create new job → nombre según tabla de arriba → un step **BigQuery - Execute** → pegá el contenido del `.sql` correspondiente en `scripts/dataflow_ready/`.
3. Guardar y (opcional) programar ejecución diaria.

Cuando los 4 jobs corran y escriban en `SBOX_NPS_ANALYTICS`, el modelo con `usar_tablas_dataflow: true` leerá esas tablas en el Checkpoint 2.
