# Qué hace el modelo – NPS Relacional Sellers

Documento de referencia: **todo lo que hace el modelo**, de punta a punta.

---

## 1. Objetivo del modelo

- **Qué es:** Análisis de NPS Relacional para **Sellers** de Mercado Pago (productos Point, QR, Online Payments).
- **Para qué:** Ver variaciones de NPS entre períodos (MoM o QvsQ, YoY), identificar motivos de queja, tendencias, alertas y causas raíz a partir de comentarios.
- **Entrega:** Un reporte ejecutivo en **HTML** con pestañas (Resumen, Drivers NPS, Cualitativo, Anexos).

---

## 2. De dónde salen los datos

| Origen | Qué aporta |
|--------|------------|
| **BigQuery – tabla principal** | Encuestas NPS de sellers. Tabla: `meli-bi-data.SBOX_CX_BI_ADS_CORE.BT_NPS_TX_SELLERS_MP_DETAIL`. El modelo las lee en el **Checkpoint 0**. |
| **Job NPS_AI_QUERIES_SELLERS (Data Flow)** | Opcional. Cada paso del job escribe en su tabla (segmentación, transacciones, remuneración, créditos, etc.). Si en `config.yaml` usás **enriquecimiento** con `usar_tablas_dataflow: true`, el modelo lee esas tablas en el **Checkpoint 2** y las cruza con las encuestas por `CUST_ID` + `END_DATE_MONTH`. Si el job no corrió o está deshabilitado el enriquecimiento, el modelo trabaja solo con la tabla principal. |

---

## 3. Configuración principal (`config/config.yaml`)

- **sites:** País a analizar (ej. `MLB`). Un site por corrida.
- **fecha_final:** Mes de cierre del análisis en `YYYYMM` (ej. `202601`).
- **comparacion.tipo:** `MoM` (mes vs mes anterior) o `QvsQ` (trimestre vs trimestre anterior).
- **enriquecimiento:**  
  - `cargar_credits`, `cargar_transacciones`, `cargar_inversiones`, `cargar_segmentacion`: activar/desactivar cada fuente.  
  - `usar_tablas_dataflow`: si es `true`, lee las tablas que escribe el job; si es `false`, corre las queries de enriquecimiento definidas en el repo.

---

## 4. Flujo completo: qué hace cada paso

El script **`ejecutar_modelo_completo.py`** corre todo en este orden:

| Paso | Nombre | Qué hace | Input | Output |
|------|--------|----------|--------|--------|
| **0** | Carga de datos | Lee encuestas NPS desde BigQuery (13 meses). Usa cache si ya existe parquet para ese site/mes. | Config, BigQuery | `data/datos_nps_{SITE}_{MES}.parquet`, `data/checkpoint0_{SITE}_{MES}_metadatos.json` |
| **2** | Enriquecimiento | Opcional. Cruza encuestas con créditos, transacciones, inversiones y/o segmentación (por CUST_ID + mes). Puede leer tablas del Data Flow o ejecutar queries del repo. | Parquet del CP0, config de enriquecimiento, BigQuery (o tablas Data Flow) | `data/datos_nps_enriquecido_{SITE}_{MES}.parquet` |
| **1** | Drivers NPS | Calcula shares de motivos de queja, NPS por dimensiones (producto, PF/PJ, crédito, etc.), variaciones MoM, efecto NPS vs efecto mix. Normaliza motivos (ej. Seguridad + Falta de seguridad → “Seguridad”). | Parquet (enriquecido si existe, si no el base) | `data/checkpoint1_consolidado_{SITE}_{MES}.json` |
| **3** | Tendencias y anomalías | Tendencias en motivos (últimos 12 meses) y anomalías en quejas por motivo (baseline adaptativo). | Checkpoint 1, datos de quejas por mes | `data/checkpoint3_tendencias_anomalias_{SITE}_{MES}.json` |
| **4** | Alertas emergentes | Detecta cambios significativos en motivos de queja (subidas/bajadas fuertes). No incluye “Otros” en las alertas. | Checkpoint 1 | `data/checkpoint4_alertas_emergentes_{SITE}_{MES}.json` |
| **5** | Análisis cualitativo | Causas raíz a partir de comentarios (con Claude si no hay cache). Comentarios por variación, retagueo de “Otros” (opcional), hipótesis (opcional). Si no existe análisis para (site, mes), genera prompt y **pausa** hasta que se complete y se guarde el JSON. | Checkpoint 1, parquet con comentarios | `data/checkpoint5_causas_raiz_{SITE}_{MES}.json`, comments, retagueo, hipótesis (si aplica) |
| **HTML** | Generación del reporte | Arma el HTML ejecutivo: NPS, comparación (MoM/QvsQ), YoY, gráficos de evolución, tabla de quejas por motivo, tendencias, alertas, aperturas por producto/dimensiones, análisis cualitativo, anexos. | Todos los checkpoints + parquet | `outputs/NPSRelSellers_{SITE}_{MES}_{timestamp}.html` |

---

## 5. Contenido del HTML (pestañas)

1. **Resumen (Tab 1)**  
   KPIs NPS, comparación (vs trim. ant. o vs mes ant.), YoY, evolución NPS, evolución de quejas por motivo, detalle de quejas por motivo (MoM), tendencias, alertas emergentes, apertura por producto y cortes (si hay enriquecimiento).

2. **Problemas de funcionamiento (Tab 2)**  
   Point: NPS y mix por dispositivo; anomalías; placeholders TTP/PTM si aplica.

3. **Análisis cualitativo (Tab 3)**  
   Causas raíz por motivo, comentarios, retagueo de “Otros” (si existe), hipótesis (si existe).

4. **Anexos (Tab 4)**  
   Tablas completas por dimensión, shares de motivos, datos crudos para auditoría.

---

## 6. Dónde se guarda todo

- **data/**  
  Parquets de encuestas (y enriquecido si aplica), JSON de checkpoints 0, 1, 3, 4 y 5, tiempos de ejecución. Es la “cache” del modelo.
- **outputs/**  
  Solo los HTML finales (y, si el script los deja ahí, algún parquet/JSON intermedio).
- **.cache/**  
  Cache de BigQuery (queries del Checkpoint 0 y, si aplica, del enriquecimiento).

---

## 7. Comandos útiles

```powershell
# Validar entorno y BigQuery
python validar_setup.py

# Ejecutar modelo completo (usa cache de checkpoints si existen)
python ejecutar_modelo_completo.py

# Forzar recarga desde BigQuery (borra parquet y metadatos del CP0)
python ejecutar_modelo_completo.py --recargar-datos

# Solo generar HTML (si ya tenés todos los checkpoints y parquet)
python scripts/generar_html_final.py
```

---

## 8. Relación con el job (NPS_AI_QUERIES_SELLERS)

- **El job** ejecuta las queries que **llenan** las tablas de enriquecimiento (segmentación, transacciones, remuneración, créditos, etc.). Cada paso escribe en **su** tabla.
- **El modelo** (este repo) **lee** esas tablas en el Checkpoint 2 si el enriquecimiento está habilitado y `usar_tablas_dataflow: true`. Si el job no corrió o no terminó, el modelo usa la última versión disponible de esas tablas (o solo la tabla principal de NPS si el enriquecimiento está desactivado).

---

## 9. Resumen en una frase

El modelo **carga encuestas NPS de sellers desde BigQuery**, opcionalmente las **enriquece** con datos del job, **calcula drivers, tendencias, alertas y análisis cualitativo**, y **genera un HTML ejecutivo** con todo eso. Los “análisis de queries correctos” por dimensión se aplican cuando **corre el job**; el modelo consume lo que ya está en las tablas.
