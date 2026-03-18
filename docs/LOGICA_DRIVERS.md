# Lógica de Razonamiento por Driver

Cada motivo de queja tiene una dimensión de enriquecimiento asociada que permite
validar con datos reales si el movimiento en quejas tiene correlato operacional.

## Reglas por Driver

### 1. Crédito (Empréstimo ou cartão de crédito)

**Dimensiones:** CREDIT_GROUP (FRED), FLAG_USA_CREDITO, FLAG_TARJETA_CREDITO
**Drill-down:** × SEGMENTO (SMB/Longtail) o × PRODUCTO_PRINCIPAL según update

#### Grupos FRED (CREDIT_GROUP)
| Grupo | Significado | Nivel de uso |
|-------|------------|-------------|
| 1. Sem uso e sem linha | Sin crédito, sin línea disponible | Ninguno |
| 2. Sem uso e com alguma linha | Tiene línea pero no la usa | Bajo |
| 3. Apenas uso de CC | Usa Crédito Corriente (préstamo) | Medio |
| 4. Apenas uso de TC | Usa Tarjeta de Crédito MP | Medio |
| 5. Uso de CC e TC | Usa ambos productos | Alto |

**Principio fundamental:** A mayor grupo FRED (1→5), mayor uso del ecosistema de
créditos de MP. Sellers que usan más productos crediticios tienden a tener mejor NPS
porque MP les resuelve necesidades financieras de su negocio.

#### Lógica de razonamiento

**Paso 1 — Chequear CREDIT_GROUP (dimensión primaria):**
- relacion_inversa = true
- Si share de grupos 3-5 (usuarios activos) SUBE → debería mejorar NPS
- Si share de grupos 3-5 BAJA → menos acceso/uso → posible aumento de quejas
- Mirar NPS QvsQ de CADA grupo (no solo el "mejor"):
  - Grupo 5 (CC+TC): ¿sube o baja? Es el más relevante por volumen de uso
  - Grupo 3 (solo CC): ¿cómo viene el préstamo?
  - Grupo 4 (solo TC): ¿cómo viene la tarjeta?
  - Grupos 1-2 (sin uso): NPS bajo es esperado, no es señal de alarma

**Paso 2 — Chequear FLAGS binarios (complemento):**
- FLAG_USA_CREDITO: "Usa crédito" vs "No usa crédito"
  - Si NPS de "Usa crédito" cae → el producto crediticio no satisface
  - Si share de "Usa crédito" cae → menos sellers acceden a crédito
- FLAG_TARJETA_CREDITO: "Tiene TC MP" vs "Sin TC MP"
  - Misma lógica: NPS y share del grupo que tiene TC

**Paso 3 — Drill-down (Nivel 2):**
- Dentro del grupo FRED que más varía, cruzar con SEGMENTO o PRODUCTO_PRINCIPAL
- Ejemplo: grupo 5 cayó -9pp NPS → ¿en SMBs o Longtail? → SMBs (-12pp, 14% del seg)

**Paso 4 — Voz del seller (CP5):**
- ¿Mencionan falta de acceso a TC pese a años de lealtad?
- ¿Juros extorsivos en préstamos?
- ¿Reducción arbitraria de límites post-pago?
- ¿Migración declarada a competidores (PagBank, InfinitePay, Banco do Brasil)?

#### Cuándo se activa
- Siempre que quejas por "Crédito" varíen ≥ umbral_principal (±0.5pp)
- Aplica a TODOS los updates (Point, SMBs, LINK, APICOW)

#### Interpretación por escenario
| Quejas crédito | NPS grupo 5 | Share grupos 3-5 | Interpretación |
|---|---|---|---|
| Suben ↑ | Baja ↓ | Estable | Producto crediticio deteriora experiencia |
| Suben ↑ | Estable | Baja ↓ | Menos sellers acceden → frustración por falta de acceso |
| Suben ↑ | Baja ↓ | Baja ↓ | Doble efecto: peor producto + menos acceso |
| Bajan ↓ | Sube ↑ | Sube ↑ | Mejora del ecosistema crediticio |
| Bajan ↓ | Estable | Sube ↑ | Más acceso reduce frustración |

#### Ejemplo output
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
