# Mapeo: Queries Dataflow → Estructura de Análisis y Output

Cada query del job **NPS_AI_QUERIES_SELLERS** alimenta cortes y secciones concretas del modelo. Este doc alinea tablas con la estructura de output y la lógica de razonamiento.

---

## 1. Para qué sirve cada query (tabla)

| Tabla | Sirve para | Responsable / Origen |
|-------|------------|----------------------|
| **SEGMENTATION_SELLERS** | PF/PJ, Segmento (LT/SMBs), Rangos TPV/TPN, Newbies/Legacy, Región, **Producto principal** (Point, QR, OP, Transferencias), flags por producto | Segmentación - Tomas Jose Sanz |
| **TRANSACCIONES_SELLERS** | TPV/TPN por producto, rangos de volumen, TIPO_PERSONA_KYC, RANGO_TPN, RANGO_TPV | Complementa segmentación para rangos y producto |
| **CREDITS_SELLERS** | **Credits FRED**: TC & MC, calidad de la oferta, upsells (FLAG_USA_CREDITO, ESTADO_OFERTA_CREDITO, FLAG_TARJETA_CREDITO, CREDIT_GROUP) | Credits - Tomas Jose Sanz |
| **REMUNERADA_SELLERS** | **Inversiones**: uso POTS, fondeado, FLAG_USA_INVERSIONES, POTS_CANTIDAD (Cofrinhos / cuenta remunerada) | Inversiones - Tomas Jose Sanz |
| **CREDITOS_FUTURO** | Vista alternativa créditos: USO_MC, USO_SL, USO_CC, USO_PL, USO_TC, flags de oferta | Para apertura Fred completo por producto (Merchant, TC) si se necesita |
| **POTS_POR_MES** | Sellers con POTS activo por mes (lista CUS_CUST_ID + SIT_SITE_ID + TIM_MONTH) | Útil para cruces “Winner” / uso inversiones |
| **SEGMENTACION_SELLERS_FUTURO** | Misma lógica que SEGMENTATION_SELLERS pero ventana **31 días** (snapshot reciente) | Opcional para análisis de ventana corta |

---

## 2. Dónde entra cada uno en el output

### Tab Principal NPS
- **Resumen por Update** (Point, SMBs, OP): filtro por **producto principal** y **segmento** → **SEGMENTATION_SELLERS** (+ update config).
- **Párrafo quejas con causa raíz + párrafo productos con causa raíz**: motivos de queja (encuesta) + causas raíz (cualitativo); productos desde **SEGMENTATION_SELLERS** (Point/QR/OP/Transferencias).
- **Quejas – gráfico barras + deep dive por motivo**:
  - **Tasas** → Pricing por escalas (datos encuesta + rangos si aplica).
  - **Credits** → FRED Credits → **CREDITS_SELLERS** (y opcional **CREDITOS_FUTURO**).
  - **Inversiones** → Cofrinhos / cuenta remunerada (mix fondeo) → **REMUNERADA_SELLERS** (y **POTS_POR_MES** si se usa).
  - **Atención al cliente** → Top Off (encuesta + cualitativo).
  - **Calidad** → suele no moverse; igual se muestra.
- **Apertura por producto principal** (herramienta de cobro), NPS y Mix:
  - **Point** → problemas de funcionamiento + mix de **devices** (encuesta + segmentación).
  - **OP** → problemas OP + apertura Link vs API Cow → **SEGMENTATION_SELLERS** (LINK_FLAG, API_FLAG).
  - **QR**, **Transferencias** → **SEGMENTATION_SELLERS**.
- **Cortes específicos**: Segmento (LT/SMBs), PF/PJ, Antigüedad (Newbie/Legacy) → **SEGMENTATION_SELLERS**.
- **Producto Fintech Services**:
  - **Credits** → apertura Fred completo por producto (Merchant, TC) → **CREDITS_SELLERS** / **CREDITOS_FUTURO**.
  - **Investments** → Cofrinhos, cuenta remunerada, investments → **REMUNERADA_SELLERS** (+ **POTS_POR_MES** si se incorpora).

### Tab Problemas de funcionamiento
- Evolución por site y por **device** (Smart, MPOS, Tap to phone, POS): encuesta + segmentación/dispositivo.
- Motivos de problema de funcionamiento, deep dive; drivers: actualización launcher, pantallas de error, PTM (encuesta + cualitativo).

### Resumen Updates (filtros)
- **SMBs**: todos los productos (Point, QR, OP) para todos los sites → **SEGMENTATION_SELLERS** (segmento + producto).
- **Point**: todos los segmentos, solo producto Point.
- **OP**: todos los segmentos, solo producto API-Cow y Link.

### Cruces con encuestas (especificados en tu doc)
- Credits (FRED, oferta, upsells) → **CREDITS_SELLERS**.
- Inversiones (Uso/Fondeado/Winner) → **REMUNERADA_SELLERS** (+ **POTS_POR_MES**).
- Top Off, Pricing, Método de pago, Aprobación (solo OP), Restricciones (solo OP): encuesta + cualitativo; las tablas de enriquecimiento dan el **corte** (PF/PJ, producto, crédito, inversiones) para cruzar.

---

## 3. Lógica de razonamiento (por dónde viene la variación)

1. **Quejas**  
   Si algún motivo de queja explica la variación → se queda ahí (deep dive por motivo; Tasas/Credits/Inversiones/Atención con las tablas arriba).  
   Si no se mueve ningún motivo → pasar a **Producto**.

2. **Producto**  
   Point / OP / QR / Transferencias → **SEGMENTATION_SELLERS** (y NPS por producto).  
   Si la caída es en **Point** → ir a **Problemas de funcionamiento**.

3. **Problemas de funcionamiento**  
   Evolución por device, motivos (launcher, pantallas de error, PTM), deep dive por motivo.

4. **Credits e Inversiones con data dura**  
   Revisar con **CREDITS_SELLERS** y **REMUNERADA_SELLERS** (y opcional **CREDITOS_FUTURO**, **POTS_POR_MES**) si la variación se asocia a subida/caída y mix de encuestados (ej. más/menos usuarios con crédito o con inversiones).

---

## 4. Resumen: tabla → uso en el modelo

| Tabla | Cortes / Secciones que alimenta |
|-------|----------------------------------|
| SEGMENTATION_SELLERS | Update (Point/SMBs/OP), producto principal, segmento LT/SMBs, PF/PJ, Newbie/Legacy, dispositivos (para Point), Link vs API Cow (OP), rangos TPV/TPN |
| TRANSACCIONES_SELLERS | Rangos TPV/TPN, volumen por producto (refuerzo de segmentación) |
| CREDITS_SELLERS | Deep dive Credits (FRED), Producto Fintech – Credits, cruce “subida/caída + mix encuestado” |
| REMUNERADA_SELLERS | Deep dive Inversiones (Cofrinhos, cuenta remunerada), Producto Fintech – Investments, cruce con data dura |
| CREDITOS_FUTURO | Apertura Fred detallada (Merchant, TC) si se activa |
| POTS_POR_MES | Lista uso POTS por mes para cruces “Winner”/inversiones |
| SEGMENTACION_SELLERS_FUTURO | Snapshot 31 días (opcional) |

Con esto, cada query tiene un rol claro en la estructura de output y en la lógica de razonamiento (Quejas → Producto → Problemas de funcionamiento + Credits/Inversiones con data dura).
