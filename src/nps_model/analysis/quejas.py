"""
Módulo para análisis de motivos de quejas de sellers.

A diferencia del modelo Buyer, los motivos vienen directamente de la encuesta
(NPS_TX_MPROM, NPS_TX_MDET, NPS_TX_MNEUTRO) sin reclasificación.
"""

import pandas as pd
from typing import List, Dict, Optional
import logging

logger = logging.getLogger(__name__)


def calcular_variaciones_quejas_detractores(
    df: pd.DataFrame,
    mes_actual: str,
    mes_anterior: str,
    motivo_col: str = "MOTIVO",
    meses_actual: List[str] = None,
    meses_anterior: List[str] = None,
) -> List[Dict]:
    """
    Calcula variaciones de quejas por motivo entre dos períodos.
    Fórmula: Quejas = %neutros + 2 × %detractores (sobre total de encuestas)

    Si se pasan meses_actual/meses_anterior (listas de meses YYYYMM), se agregan
    todos los meses del quarter. Si no, se usa mes_actual/mes_anterior individual.

    Args:
        df: DataFrame con datos de encuestas sellers
        mes_actual: Mes actual en formato YYYYMM (fallback si no se pasan listas)
        mes_anterior: Mes anterior en formato YYYYMM (fallback si no se pasan listas)
        motivo_col: Nombre de la columna de motivos (default: 'MOTIVO')
        meses_actual: Lista de meses del quarter actual (ej: ['202601','202602','202603'])
        meses_anterior: Lista de meses del quarter anterior (ej: ['202510','202511','202512'])

    Returns:
        Lista de diccionarios con variaciones por motivo
    """
    if motivo_col not in df.columns:
        logger.warning(f"Columna {motivo_col} no encontrada en DataFrame")
        return []

    if 'NPS' not in df.columns:
        logger.warning("Columna NPS no encontrada en DataFrame")
        return []

    # Si se pasan listas de meses, agregar por quarter; si no, usar mes individual
    if meses_actual:
        df_actual = df[df['END_DATE_MONTH'].isin(meses_actual)].copy()
    else:
        df_actual = df[df['END_DATE_MONTH'] == mes_actual].copy()

    if meses_anterior:
        df_anterior = df[df['END_DATE_MONTH'].isin(meses_anterior)].copy()
    else:
        df_anterior = df[df['END_DATE_MONTH'] == mes_anterior].copy()

    total_enc_actual = len(df_actual)
    total_enc_anterior = len(df_anterior)

    if total_enc_actual == 0 and total_enc_anterior == 0:
        logger.warning(f"No hay encuestas en el período actual ni anterior")
        return []

    motivos_actual = df_actual[motivo_col].dropna().unique()
    motivos_anterior = df_anterior[motivo_col].dropna().unique()
    motivos_unicos = set(list(motivos_actual) + list(motivos_anterior))

    resultados = []

    for motivo in motivos_unicos:
        # Período actual
        df_mot_actual = df_actual[df_actual[motivo_col] == motivo]
        neutros_actual = len(df_mot_actual[df_mot_actual['NPS'] == 0])
        detractores_actual = len(df_mot_actual[df_mot_actual['NPS'] == -1])

        if total_enc_actual > 0:
            pct_neutros_actual = (neutros_actual / total_enc_actual) * 100
            pct_detractores_actual = (detractores_actual / total_enc_actual) * 100
            quejas_actual = pct_neutros_actual + (2 * pct_detractores_actual)
        else:
            quejas_actual = 0

        # Período anterior
        df_mot_anterior = df_anterior[df_anterior[motivo_col] == motivo]
        neutros_anterior = len(df_mot_anterior[df_mot_anterior['NPS'] == 0])
        detractores_anterior = len(df_mot_anterior[df_mot_anterior['NPS'] == -1])

        if total_enc_anterior > 0:
            pct_neutros_anterior = (neutros_anterior / total_enc_anterior) * 100
            pct_detractores_anterior = (detractores_anterior / total_enc_anterior) * 100
            quejas_anterior = pct_neutros_anterior + (2 * pct_detractores_anterior)
        else:
            quejas_anterior = 0

        var_mom = quejas_actual - quejas_anterior

        resultados.append({
            "motivo": motivo,
            "quejas_actual": quejas_actual,
            "quejas_anterior": quejas_anterior,
            "var_mom": var_mom,
            "neutros_actual": neutros_actual,
            "detractores_actual": detractores_actual,
            "neutros_anterior": neutros_anterior,
            "detractores_anterior": detractores_anterior,
        })

    resultados = sorted(resultados, key=lambda x: abs(x['var_mom']), reverse=True)
    logger.info(f"Calculadas variaciones para {len(resultados)} motivos")
    return resultados


def calcular_impacto_quejas_mensual(
    df: pd.DataFrame,
    meses: List[str],
    motivo_col: str = "MOTIVO"
) -> pd.DataFrame:
    """
    Calcula el impacto de quejas por motivo y mes.
    Fórmula: Quejas = %neutros + 2 × %detractores (sobre total encuestas del mes)

    Args:
        df: DataFrame con columnas ['END_DATE_MONTH', 'NPS', motivo_col]
        meses: Lista de meses a incluir (formato YYYYMM)
        motivo_col: Columna de motivos (default: MOTIVO)

    Returns:
        DataFrame con meses como índice y motivos como columnas
    """
    if motivo_col not in df.columns:
        logger.warning(f"Columna {motivo_col} no encontrada en DataFrame")
        return pd.DataFrame()

    impactos_por_mes = {}

    for mes in meses:
        df_mes = df[df['END_DATE_MONTH'] == mes].copy()
        total_enc_mes = len(df_mes)

        if total_enc_mes == 0:
            continue

        motivos_mes = df_mes[motivo_col].dropna().unique()
        impactos_mes = {}

        for motivo in motivos_mes:
            df_motivo = df_mes[df_mes[motivo_col] == motivo]

            neutros = len(df_motivo[df_motivo['NPS'] == 0])
            detractores = len(df_motivo[df_motivo['NPS'] == -1])

            pct_neutros = (neutros / total_enc_mes) * 100
            pct_detractores = (detractores / total_enc_mes) * 100
            impacto = pct_neutros + (2 * pct_detractores)

            impactos_mes[motivo] = impacto

        impactos_por_mes[mes] = impactos_mes

    impacto_df = pd.DataFrame(impactos_por_mes).fillna(0).T
    impacto_df = impacto_df.sort_index()

    logger.info(f"Calculado impacto de quejas: {impacto_df.shape[0]} meses × {impacto_df.shape[1]} motivos")
    return impacto_df


def calcular_impacto_quejas_por_quarter(
    df: pd.DataFrame,
    quarters: Dict[str, List[str]],
    motivo_col: str = "MOTIVO"
) -> pd.DataFrame:
    """
    Calcula el impacto de quejas por motivo agregado por quarter.
    Fórmula: Quejas = %neutros + 2 × %detractores (sobre total encuestas del quarter)

    Args:
        df: DataFrame con columnas ['END_DATE_MONTH', 'NPS', motivo_col]
        quarters: Dict {label_quarter: [lista_meses]} ej: {'25Q4': ['202510','202511','202512']}
        motivo_col: Columna de motivos (default: MOTIVO)

    Returns:
        DataFrame con quarters como índice y motivos como columnas
    """
    if motivo_col not in df.columns:
        logger.warning(f"Columna {motivo_col} no encontrada en DataFrame")
        return pd.DataFrame()

    impactos_por_q = {}

    for q_label, meses in quarters.items():
        df_q = df[df['END_DATE_MONTH'].isin(meses)].copy()
        total_enc = len(df_q)

        if total_enc == 0:
            continue

        motivos_q = df_q[motivo_col].dropna().unique()
        impactos_q = {}

        for motivo in motivos_q:
            df_motivo = df_q[df_q[motivo_col] == motivo]

            neutros = len(df_motivo[df_motivo['NPS'] == 0])
            detractores = len(df_motivo[df_motivo['NPS'] == -1])

            pct_neutros = (neutros / total_enc) * 100
            pct_detractores = (detractores / total_enc) * 100
            impacto = pct_neutros + (2 * pct_detractores)

            impactos_q[motivo] = impacto

        impactos_por_q[q_label] = impactos_q

    impacto_df = pd.DataFrame(impactos_por_q).fillna(0).T
    impacto_df = impacto_df.sort_index()

    logger.info(f"Calculado impacto de quejas por quarter: {impacto_df.shape[0]} quarters × {impacto_df.shape[1]} motivos")
    return impacto_df


def separar_mejoras_deterioros(
    variaciones_quejas: List[Dict],
    umbral_relevancia: float = 0.5
) -> tuple:
    """
    Separa variaciones de quejas en mejoras y deterioros.
    """
    mejoras = []
    deterioros = []
    
    for item in variaciones_quejas:
        var = item.get('var_mom', 0)
        if abs(var) < umbral_relevancia:
            continue
        if var < 0:
            mejoras.append(item)
        else:
            deterioros.append(item)
    
    mejoras = sorted(mejoras, key=lambda x: abs(x.get('var_mom', 0)), reverse=True)
    deterioros = sorted(deterioros, key=lambda x: abs(x.get('var_mom', 0)), reverse=True)
    
    return mejoras, deterioros
