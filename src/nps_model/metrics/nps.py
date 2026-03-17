"""
Cálculo de métricas de NPS
"""

import logging
from typing import Optional

import pandas as pd

logger = logging.getLogger(__name__)


def calcular_nps_total(df: pd.DataFrame, group_by: Optional[list[str]] = None) -> pd.DataFrame:
    """
    Calcula el NPS score (promedio de NPS × 100).
    
    Args:
        df: DataFrame con columna NPS (-1, 0, 1)
        group_by: Lista de columnas para agrupar (ej: ["SITE", "END_DATE_MONTH"])
    
    Returns:
        DataFrame con NPS_mean y NPS_score
    """
    if group_by is None:
        group_by = ["SITE", "END_DATE_MONTH"]

    logger.debug(f"Calculando NPS agrupado por: {group_by}")

    # Agrupar y calcular promedio
    nps_df = df.groupby(group_by, as_index=False)["NPS"].mean()

    # NPS score = promedio × 100 (rango -100 a 100)
    nps_df["NPS_score"] = nps_df["NPS"] * 100

    # Renombrar para claridad
    nps_df.rename(columns={"NPS": "NPS_mean"}, inplace=True)

    return nps_df


def calcular_nps_por_dimension(
    df: pd.DataFrame,
    dimension: str,
    meses: list[str],
) -> pd.DataFrame:
    """
    Calcula NPS score por una dimensión específica (ej: VERTICAL, LOGISTIC_TYPE).
    
    Args:
        df: DataFrame con datos
        dimension: Nombre de la columna dimensión
        meses: Lista de meses a incluir (formato YYYYMM)
    
    Returns:
        DataFrame pivoteado con dimensión en filas y meses en columnas
    """
    logger.debug(f"Calculando NPS por dimensión: {dimension}")

    # Filtrar por meses
    df_filtered = df[df["END_DATE_MONTH"].isin(meses)].copy()

    # Calcular NPS por dimensión y mes
    nps_dim = (
        df_filtered.groupby([dimension, "END_DATE_MONTH"], as_index=False)["NPS"]
        .mean()
    )

    nps_dim["NPS_score"] = nps_dim["NPS"] * 100

    # Pivotear: dimensión en filas, meses en columnas
    nps_pivot = nps_dim.pivot(
        index=dimension,
        columns="END_DATE_MONTH",
        values="NPS_score",
    )

    # Asegurar que todos los meses estén presentes
    for mes in meses:
        if mes not in nps_pivot.columns:
            nps_pivot[mes] = None

    # Ordenar columnas por mes
    nps_pivot = nps_pivot[sorted(nps_pivot.columns)]

    return nps_pivot
