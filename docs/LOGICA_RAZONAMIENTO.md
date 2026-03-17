# Logica de Razonamiento — Modelo NPS Relacional Sellers

Flujo completo del modelo: que datos entran, que calculos hace, que decisiones toma, y que sale en cada paso.

---

## Flujo general

```
Checkpoint 0 (Carga)
       |
       v
Checkpoint 2 (Enriquecimiento — opcional)
       |
       v
Checkpoint 1 (Drivers NPS)
       |
       v
Checkpoint 3 (Tendencias y anomalias)
       |
       v
Checkpoint 4 (Alertas emergentes)
       |
       v
Checkpoint 5 (Analisis cualitativo — puede pausar)
       |
       v
HTML Final (Motor de razonamiento + reporte)
```

---

## 1. Checkpoint 0 — Carga de datos

**Input:** config.yaml (site, quarters), BigQuery  
**Output:** `data/datos_nps_{SITE}_{MES}.parquet`

| Paso | Que hace |
|------|----------|
| Query | Lee `BT_NPS_TX_SELLERS_MP_DETAIL` filtrando por site + rango de fechas de ambos quarters |
| NPS | Clasifica cada encuesta: 9-10 = Promotor (+1), 7-8 = Neutro (0), 0-6 = Detractor (-1) |
| Motivo | Unifica en un solo campo segun categoria: promotor usa `MPROM`, neutro usa `MNEUTRO`, detractor usa `MDET` |
| Cache | Si el parquet ya existe, no recarga |

**Campos clave que salen:**
- `NPS` (-1, 0, +1), `NOTA_NPS` (0-10), `MOTIVO`, `COMMENTS`
- `SEGMENTO_TAMANO_SELLER`, `SEGMENTO_CROSSMP`, `E_CODE`
- `POINT_USER`, `QR_USER`, `OP_USER`, `MODELO_DEVICE`
- `PROBLEMA_FUNCIONAMIENTO`, `TIPO_PROBLEMA`
- 10 valoraciones de device (bluetooth, chip, wifi, etc.)

---

## 2. Checkpoint 2 — Enriquecimiento (opcional)

**Input:** parquet del CP0 + tablas Dataflow en BigQuery  
**Output:** `data/datos_nps_enriquecido_{SITE}_{MES}.parquet`

Hace LEFT JOIN por `CUST_ID + END_DATE_MONTH` con hasta 4 fuentes:

| Fuente | Tabla Dataflow | Columnas que agrega | Config |
|--------|---------------|---------------------|--------|
| Credits | `CREDITS_SELLERS` | CREDIT_GROUP, FLAG_USA_CREDITO, FLAG_TARJETA_CREDITO, ESTADO_OFERTA_CREDITO | `cargar_credits` |
| Transacciones | `TRANSACCIONES_SELLERS` | TPV_TOTAL, TPN_TOTAL, RANGO_TPV, RANGO_TPN | `cargar_transacciones` |
| Inversiones | `REMUNERADA_SELLERS` | FLAG_POTS_ACTIVO, FLAG_USA_INVERSIONES | `cargar_inversiones` |
| Segmentacion | `SEGMENTATION_SELLERS` | PRODUCTO_PRINCIPAL, NEWBIE_LEGACY, SEGMENTO, PF/PJ, flags de producto | `cargar_segmentacion` |

**Decisiones:**
- Si una fuente falla o la tabla no existe, continua sin ella
- El modelo funciona con o sin enriquecimiento (pero pierde dimensiones)

---

## 3. Checkpoint 1 — Drivers NPS

**Input:** parquet (enriquecido si existe, base si no)  
**Output:** `data/checkpoint1_consolidado_{SITE}_{MES}.json`

### 3.1 Filtro de producto (update)

Si `update.tipo != "all"`, filtra por producto. Orden de prioridad:
1. `PRODUCTO_PRINCIPAL` (de segmentacion enriquecida)
2. Flags: `POINT_FLAG`, `OP_FLAG`, `LINK_FLAG`, `API_FLAG`
3. `SEGMENTO_CROSSMP` (de la encuesta base)
4. `POINT_USER`, `QR_USER`, `OP_USER` (flags base — cobertura baja)

### 3.2 Shares de motivos

Para cada mes y cada motivo:

```
Share = (encuestas_con_motivo / total_encuestas) × 100
```

### 3.3 Quejas

```
Quejas = %neutros + 2 × %detractores
```

Donde %neutros y %detractores son sobre el total de encuestas (no solo las del motivo).
La formula pondera doble a los detractores porque su impacto en NPS es mayor.

### 3.4 Variaciones MoM

```
var_share_mom  = share_mes_actual  - share_mes_anterior
var_quejas_mom = quejas_mes_actual - quejas_mes_anterior
```

### 3.5 NPS por dimensiones

Para cada dimension habilitada (PRODUCTO_PRINCIPAL, SEGMENTO_TAMANO_SELLER, E_CODE, etc.):

```
NPS_valor = mean(NPS) × 100     (agrupado por dimension + mes)
Share_valor = (count / total) × 100
```

### 3.6 Descomposicion de efectos

Para cada valor de cada dimension:

```
Efecto_NPS  = (NPS_actual - NPS_anterior) × (Share_anterior / 100)
Efecto_MIX  = ((Share_actual - Share_anterior) / 100) × NPS_actual
Efecto_NETO = Efecto_NPS + Efecto_MIX
```

- **Efecto NPS**: cuanto cambio el NPS del grupo (manteniendo su tamaño constante)
- **Efecto MIX**: cuanto cambio la composicion del grupo (manteniendo su NPS constante)
- **Validacion**: la suma de todos los Efecto_NETO debe ≈ variacion total de NPS (tolerancia: 0.1pp)

---

## 4. Checkpoint 3 — Tendencias y anomalias

**Input:** checkpoint1 (drivers) + parquet (para quejas mensuales)  
**Output:** `data/checkpoint3_tendencias_anomalias_{SITE}_{MES}.json`

### 4.1 Deteccion de tendencias

Analiza los ultimos 12 meses de quejas de cada motivo:

| Parametro | Valor |
|-----------|-------|
| Tolerancia neutro | ±0.03pp (variacion tan chica se ignora) |
| Meses minimos para tendencia | 3 consecutivos en misma direccion |
| Si hay 6+ meses consecutivos | Tolera hasta 2 meses neutros intercalados |

**Clasificacion de intensidad:**

| Intensidad | Variacion acumulada |
|------------|---------------------|
| Estable | < 0.1pp |
| Leve | 0.1 – 0.5pp |
| Moderada | 0.5 – 1.0pp |
| Fuerte | > 1.0pp |

**Direccion:** creciente, decreciente, variable, estable

### 4.2 Deteccion de anomalias

**Calculo del baseline adaptativo:**
1. Mediana de los ultimos 12 meses (ancla robusta contra outliers)
2. Identifica meses "normales": dentro de ±0.7pp de la mediana
3. Baseline = promedio de los meses normales (minimo 3; si no, usa la mediana)

**Clasificacion de patrones:**

| Patron | Condicion |
|--------|-----------|
| Normal | Diferencia vs baseline ≤ ±0.5pp |
| Pico aislado | > 1.5pp sobre baseline y ≤ 2 meses elevados |
| Deterioro sostenido | > 0.7pp sobre baseline durante 3+ meses consecutivos |
| Elevado normalizando | > 0.5pp sobre baseline pero bajando desde el pico |
| Normalizado | ≤ ±0.5pp despues de haber tenido un pico |
| Mejora destacada | < -0.7pp bajo baseline |

---

## 5. Checkpoint 4 — Alertas emergentes

**Input:** checkpoint1 (drivers con variaciones)  
**Output:** `data/checkpoint4_alertas_emergentes_{SITE}_{MES}.json`

**Logica:**

```
Para cada motivo (excluyendo "Otros" y "Sin informacion"):
   Si |var_quejas_mom| >= 0.9pp:
      Si var > 0 → tipo = "alerta" (aumento de quejas)
      Si var < 0 → tipo = "mejora" (baja de quejas)
```

| Parametro | Valor |
|-----------|-------|
| Threshold variacion | ≥ 0.9pp MoM |
| Quejas alto | ≥ 5.0% |
| Excluye | "Otros motivos", "Sin informacion" |

---

## 6. Checkpoint 5 — Analisis cualitativo

**Input:** parquet + checkpoint1  
**Output:** JSONs de causas raiz, comentarios, retagueo, hipotesis

### 6.1 Causas raiz (LLM — con cache)

- Si `checkpoint5_causas_raiz_{SITE}_{MES}.json` existe → usa cache
- Si no existe:
  1. Prepara hasta 100 comentarios por motivo (prioriza detractores, luego neutros)
  2. Genera prompt para LLM (Claude/Gemini)
  3. Guarda prompt en `data/temp_prompt_claude_{SITE}_{MES}.txt`
  4. **PAUSA** (exit code 1) — espera que el agente ejecute el analisis
  5. Minimo 3 comentarios por motivo para incluir en analisis

### 6.2 Comentarios sobre variaciones (automatico, sin LLM)

- Extrae comentarios para motivos con |var_mom| >= 0.5pp
- Hasta 10 comentarios por motivo (prioriza detractores)

### 6.3 Retagueo de "Otros" (LLM, opcional)

- Se activa si el share de "Otros" o "Sin informacion" >= 10%
- Toma hasta 200 comentarios de "Otros"
- LLM los reclasifica en motivos especificos

---

## 7. HTML Final — Motor de razonamiento

**Input:** todos los checkpoints + parquet  
**Output:** `outputs/NPSRelSellers_{SITE}_{MES}_{TIMESTAMP}.html`

### 7.1 Calculo de NPS global

```
NPS por mes = mean(NPS) × 100     donde NPS ∈ {-1, 0, +1}
NPS por quarter = promedio de los 3 meses del quarter
Variacion QvsQ = NPS_quarter_actual - NPS_quarter_anterior
Variacion YoY  = NPS_mes_actual - NPS_mismo_mes_anio_anterior
```

### 7.2 Recuadro gris — Resumen ejecutivo

Parrafo narrativo que explica la variacion de NPS del periodo, identificando que motivos cambiaron y que dimensiones de enriquecimiento los respaldan. Se compone de 4 bloques en secuencia:

---

#### BLOQUE 1: Variacion de NPS

Muestra el NPS actual con sus variaciones MoM/QvsQ y YoY.

```
Formato: "En [PERIODO], el NPS de [SITE] alcanzo X% (+/-Xpp MoM / +/-Xpp YoY)."
```

Colores:
- Verde si variacion >= +1pp
- Rojo si variacion <= -1pp
- Negro si entre -1 y +1pp

---

#### BLOQUE 2: Explicacion por motivos de quejas

Identifica que motivos variaron significativamente, con logica asimetrica segun la direccion del NPS:

**Si NPS BAJA (<= -1pp):**
- Principales: motivos que SUBEN >= 0.5pp (deterioros que explican la caida)
- Compensaciones: motivos que BAJAN >= 0.9pp (mejoras ocultas que frenaron la caida)

**Si NPS SUBE (>= +1pp):**
- Principales: motivos que BAJAN >= 0.5pp (mejoras que explican la suba)
- Compensaciones: motivos que SUBEN >= 0.9pp (deterioros ocultos que frenaron la suba)

**Si NPS ESTABLE (-1 < var < +1):**
- Principales: todos los motivos que varian >= 0.5pp (en cualquier direccion)

Ejemplo:
```
"Este resultado se explica principalmente por un aumento de quejas de
Atendimento ao cliente (+1.2pp MoM) [drivers]..."
```

---

#### BLOQUE 3: Asociacion con dimensiones de enriquecimiento

Para cada motivo principal, vincula con dimensiones del enriquecimiento que lo explican.

**Mapeo motivo → dimensiones (Sellers):**

| Motivo | Dimension de enriquecimiento |
|--------|------------------------------|
| Taxas / Comissoes / Pricing | CREDIT_GROUP, RANGO_TPV |
| Emprestimo / Credito / Cartao | FLAG_USA_CREDITO, ESTADO_OFERTA_CREDITO |
| Investimentos / Retornos | FLAG_USA_INVERSIONES, FLAG_POTS_ACTIVO |
| Problemas funcionamento | MODELO_DEVICE, TIPO_PROBLEMA |
| Atendimento ao cliente | (sin dimension mapeada — solo cualitativo) |
| Pagamentos recusados | (sin dimension mapeada — solo cualitativo) |

**Clasificacion de la asociacion (4 categorias):**

| Categoria | Cuando aplica | Wording |
|-----------|---------------|---------|
| EXPLICA_OK (prioridad 1) | La dimension de enriquecimiento muestra movimiento en la misma direccion que el motivo | "relacionado con [cambio] en [dimension]" |
| EXPLICA_MIX (prioridad 2) | Solo el motivo varia, la dimension esta neutra o no disponible | "relacionado con [cambio] en encuestas, aunque [dimension] se mantiene estable" |
| NO_EXPLICA (prioridad 3) | Ni el motivo ni la dimension muestran movimiento significativo | "que no se explica por variacion de [dimension]" |
| CONTRADICTORIO (prioridad 4) | La dimension se mueve en direccion opuesta al motivo | "a pesar de que [dimension] [mejoro/empeoro]" |

Ejemplo:
```
"Este resultado se explica principalmente por un aumento de quejas de
Taxas e custos (+1.2pp MoM) relacionado con cambio en CREDIT_GROUP
(aumento de sellers sin credito activo +3pp)."
```

**Nota Sellers vs Buyers:** El modelo Sellers no tiene "datos reales" (ordenes, delay, PNR). La validacion cruzada se hace contra las dimensiones de enriquecimiento (credits, transacciones, segmentacion). Si el enriquecimiento no esta disponible, el bloque 3 se omite y se usa solo el analisis cualitativo del checkpoint 5.

---

#### BLOQUE 4: Analisis por productos (equivalente a "logisticas" en Buyers)

Si hay PRODUCTO_PRINCIPAL disponible (del enriquecimiento), analiza que producto aporto mas a la variacion.

Logica:
1. Calcular efecto MIX: cambio en share de cada producto × su NPS fijo
2. Calcular efecto NPS: cambio en NPS de cada producto × su share fijo
3. Sumar = efecto NETO total por producto
4. Ordenar por impacto (mayor absoluto primero)
5. Si el producto principal es Point → drill en POINT_DEVICE_TYPE

Ejemplo:
```
"El producto que mas aporto a esta caida fue Point (-6.5pp):
-5.5pp por efecto mix (su share bajo -2pp) y -0.9pp por efecto NPS
(se observa aumento de problemas de funcionamiento +0.7pp)."
```

---

### 7.3 Umbrales del recuadro gris

```
UMBRAL_NPS_ESTABLE   = 1.0pp   # ±1pp para considerar NPS "sin cambio"
UMBRAL_PRINCIPAL     = 0.5pp   # Motivos en direccion del NPS
UMBRAL_COMPENSACION  = 0.9pp   # Motivos opuestos (compensaciones)
UMBRAL_DRIVER        = 0.5pp   # Para efecto neto de producto
UMBRAL_DEVICE        = 1.0pp   # Para variacion de device
```

### 7.4 Reglas del recuadro gris

1. Priorizar por direccion del NPS (deterioros si baja, mejoras si sube)
2. Usar umbrales asimetricos (0.5pp principales, 0.9pp compensaciones)
3. Asociar motivos con dimensiones de enriquecimiento (mapeo predefinido)
4. Clasificar asociaciones en 4 categorias (EXPLICA_OK tiene prioridad)
5. Evitar duplicados de dimensiones (si una dimension ya se uso, no repetirla)
6. Mostrar compensaciones significativas (resaltan efectos ocultos)
7. Incluir analisis por productos si hay enriquecimiento disponible
8. Nunca usar "Otros" o "Sin informacion" como explicacion principal
9. Si no hay enriquecimiento → saltar bloque 3, usar solo cualitativo (CP5)

### 7.5 Output del recuadro gris

```html
<div style="background-color: #FAFAFA; padding: 20px; border-radius: 8px;">
  <p>
    En Q4 2025, el NPS de MLB alcanzo X% (-Xpp QvsQ / -Xpp YoY).
    Este resultado se explica principalmente por un aumento de quejas de
    [Motivo1] (+Xpp MoM) relacionado con [dimension/cualitativo].
    Tambien se observa un aumento de quejas de [Motivo2] (+Xpp MoM)
    [asociacion]. Sin embargo, se observan mejoras en [Motivo3] (-Xpp MoM)
    [asociacion], que compensan parcialmente el impacto total.

    El producto que mas aporto a esta variacion fue [Producto] (-Xpp)...
  </p>
</div>
```

### 7.6 Estructura del HTML (4 tabs)

| Tab | Contenido |
|-----|-----------|
| Resumen | Recuadro gris ejecutivo, KPIs NPS, variaciones QvsQ/YoY, evolucion NPS (13 meses), evolucion quejas (7 meses, top 8 motivos), tabla de quejas con acordeon deep-dive, alertas, aperturas por dimension |
| Problemas funcionamiento | NPS y mix por device, anomalias, valoraciones promedio por device |
| Analisis cualitativo | Causas raiz por motivo, retagueo, comentarios sobre variaciones, hipotesis |
| Anexos | Tablas completas por dimension, shares de motivos, datos para auditoria |

---

## Resumen de thresholds

| Concepto | Threshold | Donde se usa |
|----------|-----------|-------------|
| NPS estable (recuadro gris) | ±1pp | Bloque 1 |
| Motivos principales (recuadro gris) | >= 0.5pp | Bloque 2 |
| Motivos compensacion (recuadro gris) | >= 0.9pp | Bloque 2 |
| Variacion significativa (alertas) | >= 0.9pp | Checkpoint 4 |
| Tolerancia neutro (tendencias) | ±0.03pp | Checkpoint 3 |
| Meses minimos para tendencia | 3 consecutivos | Checkpoint 3 |
| Baseline normales (anomalias) | ±0.7pp de mediana | Checkpoint 3 |
| Pico aislado | > 1.5pp sobre baseline | Checkpoint 3 |
| Deterioro sostenido | > 0.7pp por 3+ meses | Checkpoint 3 |
| Elevado | > 0.5pp sobre baseline | Checkpoint 3 |
| Mejora destacada | < -0.7pp bajo baseline | Checkpoint 3 |
| Retagueo de Otros | Share >= 10% | Checkpoint 5 |
| Efecto neto producto driver | > 0.5pp | Bloque 4 |
| Variacion device driver | > 1pp | Bloque 4 (drill Point) |
| Validacion efectos | diferencia <= 0.1pp | Checkpoint 1 |
| Intensidad leve | < 0.5pp | Checkpoint 3 |
| Intensidad moderada | 0.5 – 1.0pp | Checkpoint 3 |
| Intensidad fuerte | > 1.0pp | Checkpoint 3 |
