"""
Análisis por dimensiones (VERTICAL, LOGISTIC_TYPE, etc.)
"""

import logging

import pandas as pd

from nps_model.metrics.nps import calcular_nps_por_dimension
from nps_model.metrics.drivers import calcular_shares_por_dimension
from nps_model.utils.dates import calcular_mes_anterior, calcular_mes_año_anterior

logger = logging.getLogger(__name__)


def analizar_por_dimension(
    df: pd.DataFrame,
    dimension: str,
    meses: list[str],
    mes_actual: str,
) -> dict:
    """
    Análisis completo de NPS y shares por una dimensión específica.
    
    Args:
        df: DataFrame con datos
        dimension: Nombre de la columna dimensión (ej: "VERTICAL", "SHP_PICKING_TYPE_CAT")
        meses: Lista de meses a analizar
        mes_actual: Mes actual en formato YYYYMM
    
    Returns:
        Diccionario con:
            - nps_por_dimension: DataFrame con NPS por valor de dimensión
            - variaciones_nps: DataFrame con variaciones MoM/YoY
            - shares: DataFrame con shares por valor
            - variaciones_shares: DataFrame con variaciones de shares
    """
    logger.info(f"Analizando dimensión: {dimension}")

    # Verificar que la dimensión existe
    if dimension not in df.columns:
        logger.error(f"Dimensión '{dimension}' no encontrada en el DataFrame")
        return {}

    mes_anterior = calcular_mes_anterior(mes_actual)
    mes_año_anterior = calcular_mes_año_anterior(mes_actual)

    # 1. NPS por dimensión
    nps_dim = calcular_nps_por_dimension(df, dimension, meses)

    # 2. Variaciones de NPS
    variaciones_nps = []
    for valor_dim in nps_dim.index:
        nps_actual = nps_dim.loc[valor_dim, mes_actual] if mes_actual in nps_dim.columns else None
        nps_ant = (
            nps_dim.loc[valor_dim, mes_anterior] if mes_anterior in nps_dim.columns else None
        )
        nps_yoy = (
            nps_dim.loc[valor_dim, mes_año_anterior]
            if mes_año_anterior in nps_dim.columns
            else None
        )

        var_mom = nps_actual - nps_ant if pd.notna(nps_actual) and pd.notna(nps_ant) else None
        var_yoy = nps_actual - nps_yoy if pd.notna(nps_actual) and pd.notna(nps_yoy) else None

        variaciones_nps.append(
            {
                dimension: valor_dim,
                "NPS_actual": nps_actual,
                "NPS_mes_anterior": nps_ant,
                "Variacion_MoM": var_mom,
                "NPS_año_anterior": nps_yoy,
                "Variacion_YoY": var_yoy,
            }
        )

    df_var_nps = pd.DataFrame(variaciones_nps)

    # 3. Shares de la dimensión
    shares_dim = calcular_shares_por_dimension(df, dimension, meses)

    # 4. Variaciones de shares
    variaciones_shares = []
    for valor_dim in shares_dim.index:
        share_actual = (
            shares_dim.loc[valor_dim, mes_actual] if mes_actual in shares_dim.columns else None
        )
        share_ant = (
            shares_dim.loc[valor_dim, mes_anterior] if mes_anterior in shares_dim.columns else None
        )
        share_yoy = (
            shares_dim.loc[valor_dim, mes_año_anterior]
            if mes_año_anterior in shares_dim.columns
            else None
        )

        var_mom = (
            share_actual - share_ant if pd.notna(share_actual) and pd.notna(share_ant) else None
        )
        var_yoy = (
            share_actual - share_yoy if pd.notna(share_actual) and pd.notna(share_yoy) else None
        )

        variaciones_shares.append(
            {
                dimension: valor_dim,
                "share_actual": share_actual,
                "share_mes_anterior": share_ant,
                "variacion_mom": var_mom,
                "share_año_anterior": share_yoy,
                "variacion_yoy": var_yoy,
            }
        )

    df_var_shares = pd.DataFrame(variaciones_shares)

    logger.info(
        f"✅ Análisis completado: {len(nps_dim)} valores únicos en {dimension}"
    )

    return {
        "nps_por_dimension": nps_dim,
        "variaciones_nps": df_var_nps,
        "shares": shares_dim,
        "variaciones_shares": df_var_shares,
        "dimension_name": dimension,
    }
