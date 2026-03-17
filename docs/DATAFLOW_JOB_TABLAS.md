# Jobs Dataflow NPS – Tablas

Los datos de enriquecimiento se escriben en el dataset **SBOX_NPS_ANALYTICS**. Para evitar timeout, se usan **varios jobs: uno por query** (ver **docs/JOBS_UNO_POR_QUERY.md**). Cada job escribe una tabla.

## Mapeo Step → Tabla

| Step (nombre) | Tabla en BigQuery | Uso en el modelo |
|---------------|-------------------|------------------|
| SEGMENTATION_SELLERS | `SBOX_NPS_ANALYTICS.SEGMENTATION_SELLERS` | ✅ Sí (`usar_tablas_dataflow: true`) |
| TRANSACCIONES_SELLERS | `SBOX_NPS_ANALYTICS.TRANSACCIONES_SELLERS` | ✅ Sí |
| CREDITS_SELLERS | `SBOX_NPS_ANALYTICS.CREDITS_SELLERS` | ✅ Sí |
| REMUNERADA_SELLERS | `SBOX_NPS_ANALYTICS.REMUNERADA_SELLERS` | ✅ Sí |
| CREDITOS_FUTURO | `SBOX_NPS_ANALYTICS.CREDITOS_FUTURO` | Opcional (columnas USO_MC, USO_SL, etc.) |
| POTS_POR_MES | `SBOX_NPS_ANALYTICS.POTS_POR_MES` | Opcional (sellers con POTS activo por mes) |
| SEGMENTACION_SELLERS_FUTURO | `SBOX_NPS_ANALYTICS.SEGMENTACION_SELLERS_FUTURO` | Opcional (ventana 31 días) |

## Configuración del modelo

En `config/config.yaml`, sección `enriquecimiento`:

- **usar_tablas_dataflow: true** → el modelo lee de las 4 tablas principales (segmentación, transacciones, credits, remunerada) en lugar de ejecutar las queries.
- **dataset_dataflow: "SBOX_NPS_ANALYTICS"** → dataset donde el job escribe.

Las tablas CREDITOS_FUTURO, POTS_POR_MES y SEGMENTACION_SELLERS_FUTURO quedan disponibles para ampliar el modelo cuando haga falta.
