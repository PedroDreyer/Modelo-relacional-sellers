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

**Dimensiones:** FLAG_USA_INVERSIONES, FLAG_WINNER, FLAG_ASSET, FLAG_POTS_ACTIVO
**Drill-down:** × SEGMENTO (SMB/Longtail) o × PRODUCTO_PRINCIPAL según update

#### Productos de inversión MP

| Flag | Producto | Qué es |
|------|---------|--------|
| FLAG_USA_INVERSIONES | Cualquier inversión | Flag master: usa al menos 1 producto de inversión |
| FLAG_ASSET | Cuenta Remunerada | El saldo en cuenta rinde automáticamente (sin acción del seller) |
| FLAG_POTS_ACTIVO | Pots / Cofrinhos / Apartados / Reservas | Dinero apartado en una reserva que rinde. Mismo rendimiento que Asset pero separado del saldo disponible. Nombres por país: BR=Cofrinhos, MX=Apartados, AR=Reservas |
| FLAG_WINNER | Winner (Cuenta Pro / Turbinada) | Seller que ganó el challenge de Cuenta Pro. Tiene rendimiento EXTRA en Asset y Pots (mejor tasa que el seller normal) |

**Jerarquía:** WINNER > ASSET/POTS > No usa
- WINNER implica que tiene Asset y/o Pots con rendimiento premium
- ASSET y POTS son el mismo rendimiento, la diferencia es si el dinero está en saldo directo (Asset) o en una reserva separada (Pots)
- FLAG_USA_INVERSIONES = 1 si tiene Asset O Pots O ambos

**Principio fundamental:** Más sellers usando productos de inversión → mejor NPS,
porque MP les genera valor adicional con su dinero. El rendimiento es un diferencial
vs competidores que no ofrecen rendimiento automático del saldo.

#### Lógica de razonamiento

**Paso 1 — Chequear FLAG_USA_INVERSIONES (dimensión primaria):**
- relacion_inversa = true
- Si share de "Usa inversiones" SUBE → más sellers obtienen valor → debería mejorar NPS
- Si share de "Usa inversiones" BAJA → menos sellers obtienen valor → posible aumento de quejas
- Mirar NPS QvsQ de "Usa inversiones":
  - Si NPS sube → el producto satisface
  - Si NPS baja → el producto no satisface (puede ser por baja de tasa, experiencia pobre, etc.)

**Paso 2 — Chequear FLAG_WINNER (dimensión clave):**
- Winners tienen rendimiento premium → deberían tener NPS más alto
- Si NPS de Winners cae → algo pasó con Cuenta Pro/Turbinada (baja de tasa, cambio de condiciones)
- Si share de Winners cae → menos sellers acceden al programa premium
- **FLAG_WINNER es la dimensión más sensible** porque estos sellers eligieron activamente participar en el challenge

**Paso 3 — Chequear ASSET y POTS (complemento):**
- FLAG_ASSET: ¿más o menos sellers tienen cuenta remunerada?
- FLAG_POTS_ACTIVO: ¿más o menos sellers usan reservas/cofrinhos?
- Si ambos caen → caída generalizada del producto de inversiones
- Si uno sube y otro baja → migración entre productos (no necesariamente negativo)

**Paso 4 — Drill-down (Nivel 2):**
- Dentro de "Usa inversiones" o "Winner", cruzar con SEGMENTO o PRODUCTO_PRINCIPAL
- Ejemplo: NPS de Winners cayó → ¿en SMBs o Longtail?

**Paso 5 — Voz del seller (CP5):**
- ¿Mencionan rendimiento bajo o pérdida de dinero?
- ¿Comparan rendimiento con otros bancos?
- ¿Motivos mal catalogados? (sellers que se quejan de tasas pero eligieron "inversiones")

#### Cuándo se activa
- Siempre que quejas por "Inversiones y rendimiento" varíen ≥ umbral_principal (±0.5pp)
- Aplica a TODOS los updates (Point, SMBs, LINK, APICOW)

#### Interpretación por escenario
| Quejas inversiones | NPS "Usa inversiones" | Share "Usa inversiones" | NPS Winners | Interpretación |
|---|---|---|---|---|
| Suben ↑ | Baja ↓ | Estable | Baja ↓ | Producto deteriora: posible baja de tasa o cambio de condiciones |
| Suben ↑ | Estable | Baja ↓ | Estable | Menos sellers acceden → frustración por exclusión |
| Suben ↑ | Baja ↓ | Baja ↓ | Baja ↓ | Crisis del producto: peor experiencia + menos acceso |
| Bajan ↓ | Sube ↑ | Sube ↑ | Sube ↑ | Mejora integral del ecosistema de inversiones |
| Estable | Estable | Estable | Baja ↓ | Señal temprana: Winners insatisfechos, puede escalar |

#### Ejemplo output
> "aumento de quejas de Inversiones (+1.2pp): NPS de Winners pasó de 58 a 45 (-13pp,
> 16% del total), principalmente en Longtail (-15pp NPS, 84% del segmento);
> sellers reportan: rendimiento bajo comparado con otros bancos"

---

### 4. Atención al cliente (Atendimento ao cliente)

**Dimensión primaria:** FLAG_TOPOFF
**Drill-down:** × SEGMENTO (SMB/Longtail) o × PRODUCTO_PRINCIPAL según update

#### Qué es Top Off

Top Off es un programa de atención al cliente **especializada/personalizada** para sellers
que califican (generalmente por volumen de ventas o antigüedad). Los sellers Con Top Off
reciben atención prioritaria, agentes humanos dedicados, y resolución más rápida.

| Valor | Significado |
|-------|------------|
| Con Top Off | Seller con atención personalizada/prioritaria |
| Sin Top Off | Seller con atención estándar (chatbot, IA, cola general) |

**Principio fundamental:** Más sellers con Top Off = mejor cobertura de atención
personalizada = menos quejas por atención. Aplica a todos los sites y updates.

#### Lógica de razonamiento

**Paso 1 — Chequear SHARE de "Con Top Off" (señal primaria):**
- share_primario = true → el SHARE es la métrica principal, no el NPS
- relacion_inversa = true
- Si share de "Con Top Off" SUBE → más sellers reciben atención personalizada → menos quejas
- Si share de "Con Top Off" BAJA → menos cobertura → posible aumento de quejas
- **El share refleja cuántos sellers tienen acceso al servicio premium**

**Paso 2 — Chequear NPS de "Con Top Off" (señal complementaria):**
- Aunque share sea la señal primaria, el NPS también importa:
  - Si NPS de "Con Top Off" BAJA → la atención personalizada está fallando
  - Si NPS de "Con Top Off" SUBE → la atención personalizada mejora
- **Si suben quejas por atención Y el NPS de Con Top Off baja → algo está pasando
  con la calidad del servicio premium, no solo con la cobertura**

**Paso 3 — Comparar gap "Con Top Off" vs "Sin Top Off":**
- Gap esperado: NPS "Con Top Off" > NPS "Sin Top Off" (Top Off agrega valor)
- Si el gap se cierra → Top Off pierde diferencial
- Si el gap se amplía → Top Off funciona pero los de atención estándar sufren más

**Paso 4 — Drill-down (Nivel 2):**
- Dentro de "Con Top Off", cruzar con SEGMENTO o PRODUCTO_PRINCIPAL
- Ejemplo: NPS de Con Top Off cayó → ¿en SMBs o Longtail?

**Paso 5 — Voz del seller (CP5):**
- ¿Mencionan IA frustrante, imposibilidad de hablar con humano?
- ¿Bloqueos de cuenta sin resolución?
- ¿Retención de dinero sin respuesta?
- ¿Comparación con Amazon/otros que sí tienen teléfono?

#### Cuándo se activa
- Siempre que quejas por "Atención al cliente" varíen ≥ umbral_principal (±0.5pp)
- Aplica a TODOS los updates (Point, SMBs, LINK, APICOW)

#### Interpretación por escenario
| Quejas atención | Share "Con Top Off" | NPS "Con Top Off" | NPS "Sin Top Off" | Interpretación |
|---|---|---|---|---|
| Suben ↑ | Baja ↓ | Estable | Estable | Menos cobertura premium → más quejas |
| Suben ↑ | Estable | Baja ↓ | Estable | Calidad de Top Off deteriora → el servicio premium falla |
| Suben ↑ | Baja ↓ | Baja ↓ | Baja ↓ | Crisis: menos cobertura + peor calidad |
| Bajan ↓ | Sube ↑ | Estable | Estable | Más cobertura reduce quejas |
| Bajan ↓ | Estable | Sube ↑ | Estable | Mejora de calidad del servicio premium |
| Suben ↑ | Estable | Estable | Baja ↓ | Atención estándar empeora (no es Top Off, es el canal general) |

#### Ejemplo output
> "mejora de quejas de Atención al cliente (-2.1pp): share de Con Top Off creció
> de 63% a 65% (+2pp), NPS -6pp, principalmente en Longtail (-8pp NPS, 85% del segmento);
> sellers reportan: IA frustrante, falta de teléfono para atención"

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
