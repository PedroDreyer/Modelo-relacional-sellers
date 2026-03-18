# Lógica de Razonamiento por Driver

Cada motivo de queja tiene una dimensión de enriquecimiento asociada que permite
validar con datos reales si el movimiento en quejas tiene correlato operacional.

## Reglas por Driver

### 1. Crédito (Empréstimo ou cartão de crédito)

**Dimensión primaria:** CREDIT_GROUP (FRED - 5 grupos)
**Drill-down:** × SEGMENTO (SMB/Longtail) o × PRODUCTO_PRINCIPAL según update

**Lógica:**
- relacion_inversa = true
- MENOS sellers con oferta/uso de crédito → MÁS quejas por crédito
- El sub-grupo relevante es el "positivo" (grupo 5: Uso de CC e TC)
- Si NPS del grupo 5 cae → confirma que la insatisfacción crediticia impacta NPS
- Si share del grupo 5 cae → menos sellers acceden al producto, más quejas

**Qué buscar:**
1. NPS QvsQ del grupo 5 (CC+TC) — es el grupo con mejor perfil crediticio
2. Share del grupo 5 vs Q anterior — ¿más o menos sellers tienen acceso?
3. Drill-down: ¿en qué segmento/producto se concentra la variación?
4. Voz del seller (CP5): ¿mencionan falta de TC, juros altos, límites reducidos?

**Ejemplo output:**
> "aumento de quejas de Crédito (+7pp): NPS de 5. Uso de CC e TC pasó de 63 a 54 (-9pp),
> principalmente en SMB (-12pp NPS, 14% del segmento); sellers reportan: falta de acceso a TC"

---

### 2. Comisiones y cargos (Taxas e comissões)

**Dimensión primaria:** RANGO_TPV
**Drill-down:** × SEGMENTO o × PRODUCTO_PRINCIPAL

**Lógica:**
- relacion_inversa = false (default)
- Mayor TPV = menores quejas por comisiones (sellers grandes tienen mejores tasas)
- Si sellers de TPV bajo crecen en share → más quejas por comisiones
- Nota: no tenemos query de pricing real todavía, este es un proxy

**Qué buscar:**
1. NPS por rango de TPV — ¿los de TPV bajo tienen NPS más bajo?
2. Share por rango — ¿cambió la composición?
3. Voz del seller: ¿comparan con competencia (Ton, InfinitePay, PagBank)?
4. ¿Mencionan simulador engañoso, tasas variables, cobro de PIX?

---

### 3. Inversiones (Investimentos e retornos)

**Dimensión primaria:** FLAG_USA_INVERSIONES
**Drill-down:** × SEGMENTO o × PRODUCTO_PRINCIPAL

**Lógica:**
- relacion_inversa = true
- MENOS sellers usando inversiones → MÁS quejas por inversiones
- El sub-grupo positivo es "Usa inversiones"
- Si NPS de "Usa inversiones" cae → el producto no satisface

**Qué buscar:**
1. NPS de "Usa inversiones" vs "No usa" — gap y variación
2. Share — ¿más o menos sellers usan inversiones?
3. FLAG_WINNER (Rendimiento PLUS) como dimensión complementaria
4. Voz: ¿mencionan rendimiento bajo, pérdida de dinero?

---

### 4. Atención al cliente (Atendimento ao cliente)

**Dimensión primaria:** FLAG_TOPOFF
**Tipo:** share_primario = true

**Lógica:**
- relacion_inversa = true
- share_primario = true → el SHARE es la señal, no el NPS
- +share "Con Top Off" = mejor cobertura de atención personalizada = MENOS quejas
- -share "Con Top Off" = menos cobertura = MÁS quejas
- Top Off es un servicio de atención exclusiva para sellers calificados

**Qué buscar:**
1. Share de "Con Top Off" QvsQ — ¿creció o cayó la cobertura?
2. NPS de "Con Top Off" vs "Sin Top Off" — gap de experiencia
3. Drill-down: ¿en qué segmento/producto se concentra?
4. Voz: ¿mencionan IA frustrante, falta de teléfono, bloqueos sin resolver?

**Ejemplo output:**
> "mejora de quejas de Atención al cliente (-2.1pp): share de Con Top Off creció
> de 63% a 65% (+2pp), NPS -6pp"

---

### 5. Cobros rechazados / Pagamentos recusados (solo OP)

**Dimensión primaria:** RANGO_APROBACION (solo LINK/APICOW)
**Drill-down:** × SEGMENTO

**Lógica:**
- relacion_inversa = true
- MENOR tasa de aprobación → MÁS quejas por cobros rechazados
- Rangos: Alta (≥95%), Media (85-95%), Baja (<85%)
- Si share de "Alta" crece y NPS de "Alta" sube → menos quejas

**Qué buscar:**
1. Distribución Alta/Media/Baja vs Q anterior
2. NPS por rango — ¿los de baja aprobación tienen NPS mucho menor?
3. Comparar encuesta vs universo total (sesgo de muestreo)
4. Voz: ¿mencionan rechazos de extranjeros, QR no funciona, crédito alto rechazado?

**Nota:** Solo aplica para updates LINK y APICOW. Para Point/SMBs no hay esta dimensión.

---

### 6. Calidad y funcionamiento del dispositivo (Point-specific)

**Dimensión primaria:** PROBLEMA_FUNCIONAMIENTO (Sí/No)
**Drill-down:** × SEGMENTO

**Lógica:**
- relacion_inversa = false
- MÁS sellers con problemas de funcionamiento → MÁS quejas
- Sub-dimensión: TIPO_PROBLEMA (Bluetooth, Chip, WiFi, Congelamiento, Batería, etc.)
- MODELO_DEVICE para cruzar con tipo de hardware

**Qué buscar:**
1. % PdF (Problemas de Funcionamiento) QvsQ por device type (mPOS, Smart, POS, Tap)
2. Motivos top de PdF — ¿Bluetooth? ¿Congelamiento? ¿Rechazos?
3. NPS de sellers con PdF vs sin PdF — gap de experiencia
4. Voz: ¿mencionan travamento, reposición negada, chip que no funciona?

**Nota:** Solo aplica para update Point. Tab 3 (PdF) tiene los charts dedicados.

---

## Reglas Transversales

### Drill-down jerárquico (Nivel 2)
- **Point/LINK/APICOW:** Nivel 1 (dimensión primaria) → Nivel 2 × SEGMENTO (SMB/Longtail)
- **SMBs:** Nivel 1 (dimensión primaria) → Nivel 2 × PRODUCTO_PRINCIPAL (Point/QR/OP/Transferencias)
- Solo se muestra si hay ≥10 registros y variación NPS ≥1pp

### Clasificación del driver
| Clasificación | Condición |
|---|---|
| EXPLICA_OK | Quejas se mueven Y dimensión se mueve en misma dirección |
| EXPLICA_MIX | Quejas se mueven pero dimensión estable |
| CONTRADICTORIO | Quejas se mueven Y dimensión en dirección opuesta |
| NO_EXPLICA | Sin movimiento significativo |
| FALLBACK_CP5 | Sin dimensión mapeada, usa voz del seller |

### Umbrales (configurables en config.yaml)
- `umbral_principal`: ±0.5pp para considerar motivo como driver
- `umbral_compensacion`: ±0.9pp para compensaciones
- `umbral_driver_dim`: ±0.5pp para movimiento de dimensión
- `umbral_nps_estable`: ±1.0pp para considerar NPS "sin cambio"

### Voz del seller (CP5)
Siempre se incluye como complemento del dato cuantitativo.
Formato: "dato + voz" en un solo wording.
