"""
Módulo de análisis cualitativo de comentarios para NPS Sellers.

Funcionalidades:
1. Preparar comentarios por motivo para análisis de causas raíz (LLM)
2. Retagueo de "Otros": reclasificar motivos genéricos usando comentarios (LLM)
3. Comments sobre variaciones: asociar comentarios representativos a motivos con variación significativa
4. Hipótesis + validación: validar hipótesis del usuario usando comentarios y datos (LLM)
"""

import pandas as pd
from typing import Dict, List, Tuple
import json


def preparar_comentarios_para_analisis(
    df_nps: pd.DataFrame,
    mes_actual: str,
    motivos_excluir: List[str] = None,
    max_comentarios: int = 100,
    motivo_col: str = "MOTIVO",
    comment_col: str = "COMMENTS",
) -> Dict[str, Dict]:
    """
    Prepara comentarios por motivo para análisis de causas raíz.
    
    Args:
        df_nps: DataFrame con datos de encuestas NPS Sellers
        mes_actual: Mes a analizar en formato YYYYMM
        motivos_excluir: Lista de motivos a excluir
        max_comentarios: Máximo de comentarios a analizar por motivo
        motivo_col: Columna de motivos
        comment_col: Columna de comentarios
    
    Returns:
        Dict con comentarios preparados por motivo
    """
    if motivos_excluir is None:
        motivos_excluir = ['Sin información']
    
    # Verificar si hay columna de comentarios
    tiene_comentarios = comment_col in df_nps.columns
    
    if not tiene_comentarios:
        print(f"   ⚠️  Columna '{comment_col}' no encontrada en los datos.")
        print(f"   ℹ️  Los comentarios están deshabilitados en la query actual.")
        print(f"   💡  Para habilitar, descomentar NPS_TX_COMMENT en main_query.sql")
        return {
            'metadata': {
                'mes_actual': mes_actual,
                'max_comentarios_por_motivo': max_comentarios,
                'motivos_excluidos': motivos_excluir,
                'total_motivos_analizados': 0,
                'nota': 'Comentarios no disponibles en la query actual'
            },
            'comentarios_por_motivo': {}
        }
    
    # Filtrar mes actual
    df_mes = df_nps[df_nps['END_DATE_MONTH'] == mes_actual].copy()
    
    # Obtener motivos disponibles
    motivos_disponibles = df_mes[motivo_col].dropna().unique()
    motivos_analizar = [m for m in motivos_disponibles if m not in motivos_excluir]
    
    comentarios_por_motivo = {}
    
    for motivo in motivos_analizar:
        df_motivo = df_mes[df_mes[motivo_col] == motivo].copy()
        
        if len(df_motivo) == 0:
            continue
        
        # Priorizar detractores (NPS=-1), luego neutros (NPS=0)
        detractores = df_motivo[df_motivo['NPS'] == -1].copy()
        neutros = df_motivo[df_motivo['NPS'] == 0].copy()
        
        comentarios = []
        comentarios_detractores = 0
        comentarios_neutros = 0
        
        # Buscar en detractores
        for _, row in detractores.iterrows():
            if len(comentarios) >= max_comentarios:
                break
            
            comentario = row.get(comment_col, '')
            if pd.notna(comentario) and str(comentario).strip() != '':
                cust_id = row.get('CUST_ID', 'N/A')
                cust_id = str(cust_id) if pd.notna(cust_id) else 'N/A'
                
                comentarios.append({
                    'comentario': str(comentario).strip(),
                    'cust_id': cust_id,
                    'nps': int(row.get('NPS', 0))
                })
                comentarios_detractores += 1
        
        # Completar con neutros si hace falta
        if len(comentarios) < max_comentarios:
            for _, row in neutros.iterrows():
                if len(comentarios) >= max_comentarios:
                    break
                
                comentario = row.get(comment_col, '')
                if pd.notna(comentario) and str(comentario).strip() != '':
                    cust_id = row.get('CUST_ID', 'N/A')
                    cust_id = str(cust_id) if pd.notna(cust_id) else 'N/A'
                    
                    comentarios.append({
                        'comentario': str(comentario).strip(),
                        'cust_id': cust_id,
                        'nps': int(row.get('NPS', 0))
                    })
                    comentarios_neutros += 1
        
        composicion = {
            "detractores": comentarios_detractores,
            "neutros": comentarios_neutros
        }
        
        if len(comentarios) >= 1:
            comentarios_por_motivo[motivo] = {
                'total_disponibles': len(df_motivo),
                'muestra_seleccionada': len(comentarios),
                'composicion': composicion,
                'comentarios': comentarios
            }
    
    return {
        'metadata': {
            'mes_actual': mes_actual,
            'max_comentarios_por_motivo': max_comentarios,
            'motivos_excluidos': motivos_excluir,
            'total_motivos_analizados': len(comentarios_por_motivo)
        },
        'comentarios_por_motivo': comentarios_por_motivo
    }


def generar_prompt_para_claude(datos_preparados: Dict, site: str = 'MLA') -> str:
    """Genera un prompt estructurado para que Claude analice los comentarios."""
    metadata = datos_preparados['metadata']
    comentarios_por_motivo = datos_preparados['comentarios_por_motivo']
    mes_actual = metadata['mes_actual']
    
    if not comentarios_por_motivo:
        return "No hay comentarios disponibles para analizar."
    
    prompt = f"""
# 🔍 ANÁLISIS DE CAUSAS RAÍZ - COMENTARIOS NPS Relacional Sellers

## Contexto
Necesito que analices comentarios de sellers (Mercado Pago - Point, QR, OP) para identificar las **causas raíz** de cada motivo.

**Mes analizado:** {mes_actual}
**Motivos a analizar:** {metadata['total_motivos_analizados']}

## Instrucciones

Para cada motivo, debes:
1. **Leer TODOS los comentarios** del motivo sin excepción
2. **Identificar patrones semánticos profundos** — no solo palabras repetidas, sino problemas subyacentes, comparaciones con competencia, impacto en el negocio del seller, y drivers de churn
3. **Agrupar por causa raíz** (mínimo 1, máximo 4 causas por motivo)
4. **Generar título descriptivo y específico** — NO genérico. Mal: "Taxas altas". Bien: "Taxas de Link de Pagamento significativamente mas altas que competencia directa (C6, EFI, Nubank)". NUNCA usar "Motivo mal catalogado" como título — siempre describir lo que los sellers realmente dicen.
5. **Generar descripción profunda** (3-5 oraciones) que explique: el patrón identificado, por qué afecta al seller, qué evidencia hay en los comentarios, y qué implicancia tiene para el negocio (churn, migración, etc.). Si los comentarios no se refieren al motivo de la encuesta, mencionarlo en la descripción pero el título debe reflejar lo que dicen los sellers.
6. **Calcular frecuencia** de cada causa (% y cantidad absoluta)
7. **Seleccionar 2-3 ejemplos** representativos — elegir los más articulados, no los más cortos
8. **Detectar motivos mal catalogados** — si sellers en "Otro" realmente se quejan de taxas, mencionarlo en la descripción (no en el título)

## Formato de salida REQUERIDO

Responde ÚNICAMENTE con un JSON válido:

```json
{{
  "metadata": {{
    "site": "{site}",
    "mes_actual": "{mes_actual}",
    "timestamp": "...",
    "metodo": "claude_assistant"
  }},
  "causas_por_motivo": {{
    "NombreMotivo": {{
      "total_comentarios_analizados": 60,
      "composicion": {{"detractores": 55, "neutros": 5}},
      "causas_raiz": {{
        "causa_1": {{
          "titulo": "Titulo especifico y accionable (ej: Divergencia entre simulador de taxas y cobro real genera sensacion de estafa)",
          "descripcion": "Descripcion profunda de 3-5 oraciones: patron identificado + por que afecta al seller + evidencia + implicancia para el negocio",
          "frecuencia_pct": 45.5,
          "frecuencia_abs": 27,
          "ejemplos": [
            {{"comentario": "...", "cust_id": "..."}}
          ]
        }}
      }}
    }}
  }}
}}
```

---

## 📋 COMENTARIOS POR MOTIVO

"""
    
    for motivo, datos in comentarios_por_motivo.items():
        total = datos['muestra_seleccionada']
        composicion = datos['composicion']
        comentarios = datos['comentarios']
        
        prompt += f"""
### 📌 MOTIVO: {motivo}

**Total comentarios:** {total} ({composicion['detractores']} detractores + {composicion['neutros']} neutros)

**Comentarios:**

"""
        for i, c in enumerate(comentarios, 1):
            prompt += f"{i}. \"{c['comentario']}\"\n   (Seller: {c['cust_id']}, NPS: {c['nps']})\n\n"
        
        prompt += "\n" + "="*80 + "\n\n"
    
    output_filename = f'checkpoint5_causas_raiz_{site}_{mes_actual}.json'
    
    prompt += f"""
## 📝 INSTRUCCIONES FINALES

1. **Guardar el archivo** usando Write tool:
   - **Path:** `data/{output_filename}`
2. **Confirmar** que se guardó correctamente
"""
    
    return prompt


def validar_formato_respuesta(respuesta_json: Dict) -> Tuple[bool, str]:
    """Valida que la respuesta de Claude tenga el formato correcto."""
    if 'causas_por_motivo' not in respuesta_json:
        return False, "Falta clave 'causas_por_motivo' en el JSON"
    
    causas_por_motivo = respuesta_json['causas_por_motivo']
    
    if not isinstance(causas_por_motivo, dict):
        return False, "'causas_por_motivo' debe ser un diccionario"
    
    for motivo, datos in causas_por_motivo.items():
        if 'total_comentarios_analizados' not in datos:
            return False, f"Falta 'total_comentarios_analizados' en motivo '{motivo}'"
        if 'causas_raiz' not in datos:
            return False, f"Falta 'causas_raiz' en motivo '{motivo}'"
    
    return True, ""


# ==========================================
# TIER 2: RETAGUEO DE "OTROS"
# ==========================================

# Motivos considerados genéricos que ameritan retagueo
MOTIVOS_GENERICOS = [
    "Otros", "Otros motivos", "Otros sin clasificar",
    "Sin información", "No especifica", "Otro",
]


def preparar_retagueo_otros(
    df_nps: pd.DataFrame,
    mes_actual: str,
    max_comentarios: int = 200,
    motivo_col: str = "MOTIVO",
    comment_col: str = "COMMENTS",
    umbral_share_otros: float = 10.0,
) -> Dict:
    """
    Prepara comentarios de motivos genéricos ("Otros", "Sin información", etc.)
    para reclasificación por LLM.

    Solo se activa si el share de motivos genéricos supera el umbral.

    Args:
        df_nps: DataFrame con datos de encuestas
        mes_actual: Mes a analizar (YYYYMM)
        max_comentarios: Máximo de comentarios a enviar al LLM
        motivo_col: Columna de motivos
        comment_col: Columna de comentarios
        umbral_share_otros: % mínimo de "Otros" para activar retagueo

    Returns:
        Dict con comentarios preparados para retagueo y metadata
    """
    resultado = {
        'activar_retagueo': False,
        'metadata': {
            'mes_actual': mes_actual,
            'motivos_genericos_encontrados': [],
            'share_otros_total': 0.0,
            'umbral_share': umbral_share_otros,
            'total_comentarios_otros': 0,
        },
        'comentarios_otros': []
    }

    if comment_col not in df_nps.columns:
        resultado['metadata']['nota'] = 'Columna de comentarios no disponible'
        return resultado

    df_mes = df_nps[df_nps['END_DATE_MONTH'] == mes_actual].copy()
    total_encuestas = len(df_mes)

    if total_encuestas == 0:
        return resultado

    # Identificar motivos genéricos presentes en los datos
    motivos_presentes = df_mes[motivo_col].dropna().unique()
    genericos_encontrados = [
        m for m in motivos_presentes
        if any(g.lower() in str(m).lower() for g in MOTIVOS_GENERICOS)
    ]

    if not genericos_encontrados:
        resultado['metadata']['nota'] = 'No se encontraron motivos genéricos'
        return resultado

    # Calcular share de genéricos
    df_otros = df_mes[df_mes[motivo_col].isin(genericos_encontrados)]
    share_otros = (len(df_otros) / total_encuestas) * 100

    resultado['metadata']['motivos_genericos_encontrados'] = genericos_encontrados
    resultado['metadata']['share_otros_total'] = round(share_otros, 1)

    if share_otros < umbral_share_otros:
        resultado['metadata']['nota'] = (
            f'Share de "Otros" ({share_otros:.1f}%) '
            f'por debajo del umbral ({umbral_share_otros}%)'
        )
        return resultado

    # Share supera umbral: preparar comentarios para retagueo
    resultado['activar_retagueo'] = True

    # Filtrar detractores y neutros con comentario no vacío
    df_otros_con_comment = df_otros[
        (df_otros[comment_col].notna()) &
        (df_otros[comment_col].str.strip() != '') &
        (df_otros['NPS'].isin([-1, 0]))
    ].copy()

    # Tomar muestra (priorizando detractores)
    df_det = df_otros_con_comment[df_otros_con_comment['NPS'] == -1]
    df_neu = df_otros_con_comment[df_otros_con_comment['NPS'] == 0]

    comentarios = []
    for _, row in df_det.head(max_comentarios).iterrows():
        comentarios.append({
            'comentario': str(row[comment_col]).strip(),
            'cust_id': str(row.get('CUST_ID', 'N/A')),
            'nps': int(row['NPS']),
            'motivo_original': str(row[motivo_col]),
        })

    restantes = max_comentarios - len(comentarios)
    if restantes > 0:
        for _, row in df_neu.head(restantes).iterrows():
            comentarios.append({
                'comentario': str(row[comment_col]).strip(),
                'cust_id': str(row.get('CUST_ID', 'N/A')),
                'nps': int(row['NPS']),
                'motivo_original': str(row[motivo_col]),
            })

    resultado['comentarios_otros'] = comentarios
    resultado['metadata']['total_comentarios_otros'] = len(comentarios)

    print(f"   📋 Retagueo: {len(genericos_encontrados)} motivos genéricos, "
          f"share={share_otros:.1f}%, {len(comentarios)} comentarios para reclasificar")

    return resultado


def generar_prompt_retagueo(datos_retagueo: Dict, site: str, mes_actual: str) -> str:
    """
    Genera prompt para que el LLM reclasifique comentarios de "Otros".

    Args:
        datos_retagueo: Output de preparar_retagueo_otros()
        site: Código del site
        mes_actual: Mes analizado

    Returns:
        Prompt string para el LLM
    """
    comentarios = datos_retagueo['comentarios_otros']
    metadata = datos_retagueo['metadata']

    prompt = f"""
# 🔄 RETAGUEO DE MOTIVOS GENÉRICOS - NPS Relacional Sellers {site}

## Contexto
En la encuesta NPS de sellers, un **{metadata['share_otros_total']:.1f}%** de las respuestas
cayeron en motivos genéricos ({', '.join(metadata['motivos_genericos_encontrados'])}).

Necesito que reclasifiques estos comentarios en motivos más específicos.

**Mes analizado:** {mes_actual}
**Total comentarios a reclasificar:** {len(comentarios)}

## Instrucciones

Para cada comentario:
1. **Leer** el comentario del seller
2. **Asignar** un motivo específico basado en el contenido
3. Los motivos posibles deben ser **concretos y accionables** (ej: "Demoras en acreditación",
   "Problemas con lector Point", "Comisiones altas", "Soporte técnico deficiente", etc.)
4. Si el comentario no permite identificar un motivo claro, asignar "Otros - Indeterminado"

## Formato de salida REQUERIDO

Responde ÚNICAMENTE con un JSON válido:

```json
{{
  "metadata": {{
    "site": "{site}",
    "mes_actual": "{mes_actual}",
    "total_reclasificados": {len(comentarios)},
    "metodo": "claude_retagueo"
  }},
  "resumen_retagueo": {{
    "motivo_reclasificado_1": {{
      "cantidad": 25,
      "porcentaje": 41.7,
      "descripcion": "Breve descripción del problema"
    }}
  }},
  "detalle": [
    {{
      "cust_id": "...",
      "comentario": "...",
      "motivo_original": "Otros",
      "motivo_reclasificado": "Motivo específico",
      "confianza": "alta/media/baja"
    }}
  ]
}}
```

---

## 📋 COMENTARIOS A RECLASIFICAR

"""
    for i, c in enumerate(comentarios, 1):
        prompt += (
            f'{i}. "{c["comentario"]}"\n'
            f'   (Seller: {c["cust_id"]}, NPS: {c["nps"]}, '
            f'Motivo original: {c["motivo_original"]})\n\n'
        )

    output_filename = f'checkpoint5_retagueo_{site}_{mes_actual}.json'

    prompt += f"""
## 📝 INSTRUCCIONES FINALES

1. **Guardar el archivo** usando Write tool:
   - **Path:** `data/{output_filename}`
2. **Confirmar** que se guardó correctamente
"""

    return prompt


# ==========================================
# TIER 2: COMMENTS SOBRE VARIACIONES
# ==========================================

def extraer_comentarios_por_variacion(
    df_nps: pd.DataFrame,
    variaciones_quejas: List[Dict],
    mes_actual: str,
    max_comentarios_por_motivo: int = 10,
    motivo_col: str = "MOTIVO",
    comment_col: str = "COMMENTS",
    umbral_variacion: float = 0.5,
) -> Dict[str, Dict]:
    """
    Para cada motivo con variación MoM significativa, extrae comentarios
    representativos del mes actual.

    Args:
        df_nps: DataFrame con datos de encuestas
        variaciones_quejas: Lista de dicts con variaciones por motivo
            (output de calcular_variaciones_quejas_detractores)
        mes_actual: Mes actual (YYYYMM)
        max_comentarios_por_motivo: Máximo de comentarios por motivo
        motivo_col: Columna de motivos
        comment_col: Columna de comentarios
        umbral_variacion: Variación mínima (en pp) para incluir comentarios

    Returns:
        Dict con comentarios por motivo que varió significativamente
    """
    resultado = {}

    if comment_col not in df_nps.columns:
        return resultado

    df_mes = df_nps[
        (df_nps['END_DATE_MONTH'] == mes_actual) &
        (df_nps['NPS'].isin([-1, 0]))
    ].copy()

    for var_info in variaciones_quejas:
        motivo = var_info.get('motivo')
        var_mom = var_info.get('var_mom', 0)

        if motivo is None or abs(var_mom) < umbral_variacion:
            continue

        df_motivo = df_mes[df_mes[motivo_col] == motivo]
        df_con_comment = df_motivo[
            (df_motivo[comment_col].notna()) &
            (df_motivo[comment_col].str.strip() != '')
        ]

        if len(df_con_comment) == 0:
            continue

        # Priorizar detractores
        df_det = df_con_comment[df_con_comment['NPS'] == -1].head(max_comentarios_por_motivo)
        restantes = max_comentarios_por_motivo - len(df_det)
        df_neu = df_con_comment[df_con_comment['NPS'] == 0].head(restantes) if restantes > 0 else pd.DataFrame()

        comentarios_muestra = []
        for _, row in pd.concat([df_det, df_neu]).iterrows():
            comentarios_muestra.append({
                'comentario': str(row[comment_col]).strip(),
                'cust_id': str(row.get('CUST_ID', 'N/A')),
                'nps': int(row['NPS']),
            })

        direccion = "empeoró" if var_mom > 0 else "mejoró"

        resultado[motivo] = {
            'motivo': motivo,
            'var_mom': round(var_mom, 2),
            'direccion': direccion,
            'total_comentarios_disponibles': len(df_con_comment),
            'muestra': len(comentarios_muestra),
            'comentarios': comentarios_muestra,
        }

    return resultado


# ==========================================
# TIER 4A: HIPÓTESIS + VALIDACIÓN
# ==========================================

def preparar_validacion_hipotesis(
    df_nps: pd.DataFrame,
    hipotesis: str,
    mes_actual: str,
    max_comentarios: int = 150,
    motivo_col: str = "MOTIVO",
    comment_col: str = "COMMENTS",
    dimensiones_relevantes: List[str] = None,
) -> Dict:
    """
    Prepara datos para que el LLM valide una hipótesis del usuario.

    Recopila:
    - Comentarios de detractores/neutros del mes actual
    - NPS por dimensiones relevantes (actual vs anterior)
    - Variaciones MoM por motivo
    - Contexto numérico para que el LLM pueda validar/refutar

    Args:
        df_nps: DataFrame con datos NPS
        hipotesis: Texto de la hipótesis a validar
        mes_actual: Mes actual (YYYYMM)
        max_comentarios: Máximo de comentarios a incluir
        motivo_col: Columna de motivos
        comment_col: Columna de comentarios
        dimensiones_relevantes: Lista de columnas de dimensiones para incluir contexto

    Returns:
        Dict con datos recopilados para validación
    """
    if dimensiones_relevantes is None:
        # Dimensiones por defecto que suelen existir
        dimensiones_relevantes = [
            'SEGMENTO_TAMANO_SELLER', 'PRODUCTO', 'TIPO_PERSONA',
            'TIPO_SELLER', 'ANTIGUEDAD', 'POINT_DEVICE_TYPE',
            'FLAG_USA_CREDITO', 'FLAG_USA_INVERSIONES',
            'RANGO_TPN', 'RANGO_TPV',
        ]

    resultado = {
        'hipotesis': hipotesis,
        'mes_actual': mes_actual,
        'tiene_comentarios': comment_col in df_nps.columns,
        'contexto_numerico': {},
        'comentarios_relevantes': [],
    }

    df_mes = df_nps[df_nps['END_DATE_MONTH'] == mes_actual].copy()

    # Calcular mes anterior
    año = int(mes_actual[:4])
    mes_num = int(mes_actual[4:])
    if mes_num == 1:
        mes_anterior = f"{año - 1}12"
    else:
        mes_anterior = f"{año}{mes_num - 1:02d}"

    df_mes_ant = df_nps[df_nps['END_DATE_MONTH'] == mes_anterior].copy()

    # NPS general
    nps_actual = df_mes['NPS'].mean() * 100 if len(df_mes) > 0 else None
    nps_anterior = df_mes_ant['NPS'].mean() * 100 if len(df_mes_ant) > 0 else None

    resultado['contexto_numerico']['nps_general'] = {
        'mes_actual': round(nps_actual, 1) if nps_actual is not None else None,
        'mes_anterior': round(nps_anterior, 1) if nps_anterior is not None else None,
        'variacion': round(nps_actual - nps_anterior, 1) if nps_actual and nps_anterior else None,
        'n_actual': len(df_mes),
        'n_anterior': len(df_mes_ant),
    }

    # Distribución de motivos (actual vs anterior)
    motivos_actual = {}
    motivos_anterior = {}

    if motivo_col in df_mes.columns:
        total_det_act = len(df_mes[df_mes['NPS'].isin([-1, 0])])
        for motivo, count in df_mes[df_mes['NPS'].isin([-1, 0])][motivo_col].value_counts().items():
            motivos_actual[motivo] = round(count / total_det_act * 100, 1) if total_det_act > 0 else 0

        total_det_ant = len(df_mes_ant[df_mes_ant['NPS'].isin([-1, 0])])
        for motivo, count in df_mes_ant[df_mes_ant['NPS'].isin([-1, 0])][motivo_col].value_counts().items():
            motivos_anterior[motivo] = round(count / total_det_ant * 100, 1) if total_det_ant > 0 else 0

    resultado['contexto_numerico']['motivos'] = {
        'actual': motivos_actual,
        'anterior': motivos_anterior,
    }

    # NPS por dimensiones relevantes
    resultado['contexto_numerico']['dimensiones'] = {}
    for dim in dimensiones_relevantes:
        if dim not in df_mes.columns:
            continue

        dim_data = {}
        for val in df_mes[dim].dropna().unique():
            nps_dim_act = df_mes[df_mes[dim] == val]['NPS'].mean() * 100
            nps_dim_ant_df = df_mes_ant[df_mes_ant[dim] == val] if dim in df_mes_ant.columns else pd.DataFrame()
            nps_dim_ant = nps_dim_ant_df['NPS'].mean() * 100 if len(nps_dim_ant_df) > 0 else None

            dim_data[str(val)] = {
                'nps_actual': round(nps_dim_act, 1),
                'nps_anterior': round(nps_dim_ant, 1) if nps_dim_ant is not None else None,
                'n': len(df_mes[df_mes[dim] == val]),
            }

        if dim_data:
            resultado['contexto_numerico']['dimensiones'][dim] = dim_data

    # Comentarios (si disponibles)
    if comment_col in df_nps.columns:
        df_con_comment = df_mes[
            (df_mes[comment_col].notna()) &
            (df_mes[comment_col].str.strip() != '') &
            (df_mes['NPS'].isin([-1, 0]))
        ]

        # Priorizar detractores
        df_det = df_con_comment[df_con_comment['NPS'] == -1]
        df_neu = df_con_comment[df_con_comment['NPS'] == 0]

        comentarios = []
        for _, row in df_det.head(max_comentarios).iterrows():
            comentarios.append({
                'comentario': str(row[comment_col]).strip(),
                'cust_id': str(row.get('CUST_ID', 'N/A')),
                'nps': int(row['NPS']),
                'motivo': str(row.get(motivo_col, 'N/A')),
            })

        restantes = max_comentarios - len(comentarios)
        if restantes > 0:
            for _, row in df_neu.head(restantes).iterrows():
                comentarios.append({
                    'comentario': str(row[comment_col]).strip(),
                    'cust_id': str(row.get('CUST_ID', 'N/A')),
                    'nps': int(row['NPS']),
                    'motivo': str(row.get(motivo_col, 'N/A')),
                })

        resultado['comentarios_relevantes'] = comentarios

    return resultado


def generar_prompt_hipotesis(datos_validacion: Dict, site: str) -> str:
    """
    Genera prompt para que el LLM valide/refute la hipótesis usando datos + comments.

    Args:
        datos_validacion: Output de preparar_validacion_hipotesis()
        site: Código de site

    Returns:
        Prompt string para el LLM
    """
    hipotesis = datos_validacion['hipotesis']
    mes = datos_validacion['mes_actual']
    ctx = datos_validacion['contexto_numerico']
    comentarios = datos_validacion['comentarios_relevantes']

    # Formatear contexto numérico
    nps_info = ctx.get('nps_general', {})
    nps_txt = (
        f"NPS actual: {nps_info.get('nps_actual', 'N/A')} "
        f"(anterior: {nps_info.get('nps_anterior', 'N/A')}, "
        f"variación: {nps_info.get('variacion', 'N/A')}pp)"
    )

    # Formatear motivos
    motivos_act = ctx.get('motivos', {}).get('actual', {})
    motivos_ant = ctx.get('motivos', {}).get('anterior', {})
    motivos_txt = ""
    for motivo, share in sorted(motivos_act.items(), key=lambda x: -x[1])[:10]:
        share_ant = motivos_ant.get(motivo, 0)
        var = share - share_ant
        motivos_txt += f"  - {motivo}: {share:.1f}% (var: {var:+.1f}pp)\n"

    # Formatear dimensiones
    dims_txt = ""
    for dim_name, dim_data in ctx.get('dimensiones', {}).items():
        dims_txt += f"\n  **{dim_name}:**\n"
        for val, info in sorted(dim_data.items(), key=lambda x: -x[1].get('n', 0))[:5]:
            nps_act = info.get('nps_actual', 'N/A')
            nps_ant = info.get('nps_anterior')
            n = info.get('n', 0)
            if nps_ant is not None:
                dims_txt += f"    - {val}: NPS={nps_act} (ant: {nps_ant}, n={n})\n"
            else:
                dims_txt += f"    - {val}: NPS={nps_act} (n={n})\n"

    prompt = f"""
# 🧪 VALIDACIÓN DE HIPÓTESIS - NPS Relacional Sellers {site}

## Hipótesis a validar

> "{hipotesis}"

## Contexto numérico

**NPS General:** {nps_txt}
**N encuestas:** actual={nps_info.get('n_actual', 0):,}, anterior={nps_info.get('n_anterior', 0):,}

### Distribución de motivos de quejas (detractores + neutros)
{motivos_txt}
### NPS por dimensiones
{dims_txt}

## Comentarios de detractores y neutros ({len(comentarios)} muestra)

"""

    for i, c in enumerate(comentarios, 1):
        prompt += f'{i}. "{c["comentario"]}"\n   (Seller: {c["cust_id"]}, NPS: {c["nps"]}, Motivo: {c["motivo"]})\n\n'

    output_filename = f'checkpoint5_hipotesis_{site}_{mes}.json'

    prompt += f"""
---

## Instrucciones de análisis

Con base en los datos numéricos y los comentarios arriba:

1. **EVALÚA** la hipótesis: ¿Los datos la soportan, la refutan, o es inconclusa?
2. **EVIDENCIA A FAVOR**: Lista las evidencias que soportan la hipótesis (datos + comentarios)
3. **EVIDENCIA EN CONTRA**: Lista las evidencias que la refutan o contradicen
4. **FACTORES ADICIONALES**: Identifica otros factores relevantes que la hipótesis no considera
5. **CONCLUSIÓN**: Veredicto (CONFIRMADA / PARCIALMENTE CONFIRMADA / REFUTADA / INCONCLUSA) con nivel de confianza
6. **RECOMENDACIÓN**: Qué acción tomar basado en la validación

## Formato de salida REQUERIDO

Responde ÚNICAMENTE con un JSON válido:

```json
{{
  "metadata": {{
    "site": "{site}",
    "mes_actual": "{mes}",
    "hipotesis": "{hipotesis}",
    "metodo": "claude_hipotesis"
  }},
  "validacion": {{
    "veredicto": "CONFIRMADA|PARCIALMENTE CONFIRMADA|REFUTADA|INCONCLUSA",
    "confianza": "alta|media|baja",
    "resumen": "Resumen ejecutivo de la validación en 2-3 oraciones",
    "evidencia_a_favor": [
      {{"tipo": "dato|comentario", "descripcion": "...", "detalle": "..."}}
    ],
    "evidencia_en_contra": [
      {{"tipo": "dato|comentario", "descripcion": "...", "detalle": "..."}}
    ],
    "factores_adicionales": [
      {{"factor": "...", "impacto": "...", "descripcion": "..."}}
    ],
    "recomendacion": "Acción recomendada basada en la validación"
  }}
}}
```

## 📝 INSTRUCCIONES FINALES

1. **Guardar el archivo** usando Write tool:
   - **Path:** `data/{output_filename}`
2. **Confirmar** que se guardó correctamente
"""

    return prompt
