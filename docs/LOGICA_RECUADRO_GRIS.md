# LOGICA RESUMEN EJECUTIVO SELLERS MP

## Objetivo

Generar un párrafo narrativo que explique la variación de NPS del período, identificando qué motivos de queja cambiaron, qué dimensiones de enriquecimiento los respaldan, y qué producto aportó más al movimiento.

## FLUJO GENERAL (4 BLOQUES)

### BLOQUE 1: Variación de NPS

Muestra el NPS actual con sus variaciones Q vs Q.

**Formato:**
```
En [QUARTER], el NPS de [SITE] [PRODUCTO/OS] y [SEGMENTO/OS] alcanzó X% (+/-Xpp QvsQ / +/-Xpp YoY).
```

**Lógica:**
- Verde si variación >= +1pp
- Rojo si variación <= -1pp
- Negro si entre -1 y +1pp

**Nota Importante:**
- Siempre el analista va a elegir un solo SITE (MLA, MLB, MLC, MLM, MCO, MLU, MPE)
- Para el site que elija, puede analizar múltiples productos o filtrar uno solo (POINT, QR, OP_COW_API, OP_LINK, TRANSFERENCIAS)
- Lo mismo para segmentos, puede seleccionar múltiples o mirar uno solo (LONGTAIL, SMBs)

### BLOQUE 2: Explicación por Motivos de Quejas

Identifica qué motivos de queja variaron significativamente.

**Lógica de filtrado:**
- **Si NPS BAJA (<= -1pp):**
  - Principales: Motivos que SUBEN >= 0.5pp (deterioros)
  - Compensaciones: Motivos que BAJAN >= 0.9pp (mejoras ocultas)
- **Si NPS SUBE (>= +1pp):**
  - Principales: Motivos que BAJAN >= 0.5pp (mejoras)
  - Compensaciones: Motivos que SUBEN >= 0.5pp (deterioros ocultos)
- **Si NPS ESTABLE (-1 < var < +1):**
  - Principales: Todos los motivos que varían >= 0.5pp (en cualquier dirección)

**Ejemplo:**
```
Este resultado se explica principalmente por un aumento de quejas de
créditos (+1.2pp QvsQ) [drivers]...
```

### BLOQUE 3: Asociación con Drivers

Para cada motivo, vincular con driver que lo explican.

La validación se hace cruzando con drivers (credits, transacciones,
segmentación, inversiones). Si el enriquecimiento no está disponible, se usa la causa raíz
del análisis cualitativo (Checkpoint 5) como fallback.

**Mapeo Motivo → Dimensiones:**
```
Tasas y comisiones                        → Pricing por escalas (WIP)
Financiamiento                            → oferta/uso de créditos y TC
Inversiones                               → Usa/no usa Pots, Turbinado/no Turbinado (en PF)
Calidad y funcionamiento del device       → Problemas de funcionamiento
Atención al cliente                       → FLAG_TOPOFF (BT_CX_SELLERS_MP_TOP_OFF)
Pagamentos recusados                      → (sin dimensión — usar causa raíz CP5)
Cobros en cuotas                          → (sin dimensión — usar causa raíz CP5)
Plazo                                     → (sin dimensión — usar causa raíz CP5)
```

**Clasificación de la asociación (4 categorías):**
- **EXPLICA_OK (prioridad 1):**
  - El driver muestra movimiento en la misma dirección que el motivo
  - Wording: "relacionado con [cambio] en [dimensión] (+Xpp)"
- **EXPLICA_MIX (prioridad 2):**
  - Solo el motivo varía, la dimensión está neutra o no disponible
  - Wording: "relacionado con [cambio] en encuestas, aunque [dimensión] se mantiene estable"
- **NO_EXPLICA (prioridad 3):**
  - Ninguno de los dos indicadores muestra movimiento significativo
  - Wording: "que no se explica por variación de [dimensión]"
- **CONTRADICTORIO (prioridad 4):**
  - La dimensión se mueve en dirección opuesta al motivo
  - Wording: "a pesar de que [dimensión] [mejoró/empeoró]"

**Fallback sin enriquecimiento:**
- Si no hay dimensión mapeada o el enriquecimiento no cargó:
  - Usar causa raíz del Checkpoint 5 (análisis cualitativo LLM)
  - Wording: "donde los sellers reportan [causa_raiz_principal del CP5]"

**Ejemplo completo:**
```
Este resultado se explica principalmente por un aumento de quejas de
financiamiento (+1.2pp QvsQ) relacionado con aumento de sellers sin oferta de crédito
(+3pp en share).

También se observa un aumento de quejas de Atendimento ao cliente (+0.8pp QvsQ)
donde los sellers reportan imposibilidad de acceder a atención humana.
```

### BLOQUE 4: Análisis por cambio de MIX

(3 tablas)
1. PRODUCTO PRINCIPAL (Point/QR/OP_COW_API/OP_LINK/TRANSFERENCIAS) — Si filtra por 1 solo producto, no mostrar
2. SEGMENTO (LONGTAIL/SMBs) — Si filtra por 1 solo segmento, no mostrar
3. PERSONA (PJ/PF)

Analizar qué productos principales, segmentos y personas aportaron más a la variación.

**Lógica:**
1. Calcular efecto MIX: cambio en share de cada producto × su NPS fijo
2. Calcular efecto NPS: cambio en NPS de cada producto × su share fijo
3. Sumar = efecto NETO total por variable
4. Ordenar por impacto (mayor absoluto primero)
5. Si el producto principal es Point → drill en POINT_DEVICE_TYPE (tab aparte)

**Tab POINT** → Solo para Producto Principal = Point:
Calcular variación MIX de device principal (mPOS/POS/SMART)

**Ejemplo:**
```
El producto que más aportó a esta caída fue Point (-6.5pp):
-5.5pp por efecto mix (su share bajó -2pp) y -0.9pp por efecto NPS
(se observa aumento de problemas de funcionamiento +0.7pp en encuestas
de sellers Point, principalmente en dispositivos Smart).
```

## UMBRALES CONFIGURABLES

```
UMBRAL_NPS_ESTABLE   = 1.0    # ±1pp para considerar NPS "sin cambio"
UMBRAL_PRINCIPAL     = 0.5    # Motivos en dirección del NPS (pp)
UMBRAL_COMPENSACION  = 0.9    # Motivos opuestos / compensaciones (pp)
UMBRAL_DRIVER_DIM    = 0.5    # Para dimensión de enriquecimiento (pp en share o NPS)
UMBRAL_PRODUCTO      = 0.5    # Para efecto neto de producto (pp)
UMBRAL_DEVICE        = 1.0    # Para variación de device Point (pp)
```

## OUTPUT FINAL

```html
<div style="background-color: #FAFAFA; padding: 20px; border-radius: 8px;">
  <p>
    En Q4 2025, el NPS de MLB alcanzó 42p.p. (-2pp QvsQ / -1pp YoY).
    Esta caída se explica principalmente por un aumento de quejas de
    financiamiento (+1.2pp QvsQ) relacionado con aumento de sellers
    sin oferta de crédito (+10pp, 25% share). También se observa un
    aumento de quejas de Atención al cliente (+0.8pp QvsQ) donde los
    sellers reportan imposibilidad de acceder a atención humana. Sin embargo,
    se observan mejoras en quejas por tasas (-1.0pp QvsQ) donde los
    sellers reportan menos quejas por tasas elevadas, que compensan parcialmente
    el impacto total.

    El producto que más aportó a esta caída fue Point (-4.2pp):
    -3.1pp por efecto mix (su share bajó -1.5pp) y -1.1pp por efecto NPS
    (se observa aumento de quejas de funcionamiento en dispositivos Smart).
  </p>
</div>
```

## ESTRUCTURA HTML (4 TABS)

### Hoja 1: Resumen
- Explicación NPS (variación QvsQ/YoY)
- Quejas (principales + compensaciones)
- MIX (Producto, Segmento, Persona)
- TBD Promotores (motivos de promoción)
- Mini detalle en cada motivo

### Hoja 2: Drivers (Penetración Real)
- Triangulación encuesta vs datos reales de enriquecimiento
- Por cada motivo que se mueve: qué pasa en la realidad
- Clasificación EXPLICA_OK / MIX / NO_EXPLICA / CONTRADICTORIO

### Hoja 3: Point (Problemas de Funcionamiento)
- Problemas por tipo (Batería, WiFi, Congelamento, Chip, Recusa)
- Por modelo de device (Smart, mPOS, POS)
- Valoraciones por atributo
- MIX decomposition de device type

### Hoja 4: Cualitativo
- Comentarios por motivo de queja
- Causas raíz (CP5) con frecuencias y ejemplos

## PUNTOS CLAVE PARA REPLICAR

1. Priorizar por dirección del NPS (deterioros si baja, mejoras si sube)
2. Usar umbrales asimétricos (0.5pp principales, 0.9pp compensaciones)
3. Asociar motivos con dimensiones de enriquecimiento (mapeo predefinido)
4. Clasificar asociaciones en 4 categorías (EXPLICA_OK tiene prioridad)
5. Fallback a causas raíz del CP5 cuando no hay dimensión mapeada o enriquecimiento no cargó
6. Evitar duplicados de dimensiones (si una dimensión ya se usó, no repetirla)
7. Mostrar compensaciones significativas (resaltan efectos ocultos)
8. Incluir análisis por productos si hay enriquecimiento disponible
9. Si producto driver es Point → drill en POINT_DEVICE_TYPE
10. Nunca usar "Outros motivos" o "Sin información" como explicación principal
