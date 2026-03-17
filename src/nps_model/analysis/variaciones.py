"""
Cálculo de variaciones MoM y YoY
"""

import logging
from typing import Optional

import pandas as pd

from nps_model.utils.dates import calcular_mes_anterior, calcular_mes_año_anterior

logger = logging.getLogger(__name__)


def calcular_variaciones_nps(
    df_nps: pd.DataFrame,
    mes_actual: str,
    group_by: Optional[list[str]] = None,
) -> pd.DataFrame:
    """
    Calcula variaciones MoM y YoY de NPS.
    
    Args:
        df_nps: DataFrame con NPS_score por período (columnas: group_by + NPS_score)
        mes_actual: Mes actual en formato YYYYMM
        group_by: Columnas de agrupación (ej: ["SITE"], ["VERTICAL"])
    
    Returns:
        DataFrame con variaciones:
            - NPS_actual
            - NPS_mes_anterior
            - Variacion_MoM
            - NPS_año_anterior
            - Variacion_YoY
    """
    if group_by is None:
        group_by = ["SITE"]

    logger.debug(f"Calculando variaciones NPS para mes: {mes_actual}")

    # Calcular meses de referencia
    mes_anterior = calcular_mes_anterior(mes_actual)
    mes_año_anterior = calcular_mes_año_anterior(mes_actual)

    logger.debug(f"Mes anterior (MoM): {mes_anterior}")
    logger.debug(f"Año anterior (YoY): {mes_año_anterior}")

    # Pivotar para tener meses como columnas
    df_pivot = df_nps.pivot_table(
        index=group_by,
        columns="END_DATE_MONTH",
        values="NPS_score",
        aggfunc="first",
    )

    resultados = []

    for idx in df_pivot.index:
        # Manejar tanto índices simples como multi-índices
        if isinstance(idx, tuple):
            group_vals = dict(zip(group_by, idx))
        else:
            group_vals = {group_by[0]: idx}

        # NPS actual
        nps_actual = df_pivot.loc[idx, mes_actual] if mes_actual in df_pivot.columns else None

        # NPS mes anterior (MoM)
        nps_mes_anterior = (
            df_pivot.loc[idx, mes_anterior] if mes_anterior in df_pivot.columns else None
        )

        # NPS año anterior (YoY)
        nps_año_anterior = (
            df_pivot.loc[idx, mes_año_anterior] if mes_año_anterior in df_pivot.columns else None
        )

        # Calcular variaciones
        var_mom = (nps_actual - nps_mes_anterior) if pd.notna(nps_actual) and pd.notna(nps_mes_anterior) else None
        var_yoy = (nps_actual - nps_año_anterior) if pd.notna(nps_actual) and pd.notna(nps_año_anterior) else None

        resultado = {
            **group_vals,
            "NPS_actual": nps_actual,
            "NPS_mes_anterior": nps_mes_anterior,
            "Variacion_MoM": var_mom,
            "NPS_año_anterior": nps_año_anterior,
            "Variacion_YoY": var_yoy,
        }

        resultados.append(resultado)

    df_resultado = pd.DataFrame(resultados)

    logger.info(
        f"✅ Variaciones calculadas para {len(df_resultado)} grupos "
        f"(agrupación: {', '.join(group_by)})"
    )

    return df_resultado


def calcular_variaciones_drivers(
    drivers_dict: dict[str, pd.DataFrame],
    mes_actual: str,
) -> dict[str, dict]:
    """
    Calcula variaciones MoM y YoY para drivers operacionales.
    
    Args:
        drivers_dict: Diccionario con shares de drivers por mes
                     {driver_name: DataFrame con shares por mes}
        mes_actual: Mes actual en formato YYYYMM
    
    Returns:
        Diccionario con variaciones por driver:
        {
            driver_name: {
                "share_actual": float,
                "share_mes_anterior": float,
                "variacion_mom": float,
                "share_año_anterior": float,
                "variacion_yoy": float,
            }
        }
    """
    logger.debug(f"Calculando variaciones de drivers para mes: {mes_actual}")

    mes_anterior = calcular_mes_anterior(mes_actual)
    mes_año_anterior = calcular_mes_año_anterior(mes_actual)

    resultados = {}

    for driver_name, df_driver in drivers_dict.items():
        if df_driver.empty:
            continue

        # Obtener shares
        share_actual = (
            df_driver[mes_actual].iloc[0] if mes_actual in df_driver.columns else None
        )
        share_mes_anterior = (
            df_driver[mes_anterior].iloc[0] if mes_anterior in df_driver.columns else None
        )
        share_año_anterior = (
            df_driver[mes_año_anterior].iloc[0] if mes_año_anterior in df_driver.columns else None
        )

        # Calcular variaciones
        var_mom = (
            share_actual - share_mes_anterior
            if pd.notna(share_actual) and pd.notna(share_mes_anterior)
            else None
        )
        var_yoy = (
            share_actual - share_año_anterior
            if pd.notna(share_actual) and pd.notna(share_año_anterior)
            else None
        )

        resultados[driver_name] = {
            "share_actual": share_actual,
            "share_mes_anterior": share_mes_anterior,
            "variacion_mom": var_mom,
            "share_año_anterior": share_año_anterior,
            "variacion_yoy": var_yoy,
        }

    logger.info(f"✅ Variaciones calculadas para {len(resultados)} drivers")

    return resultados


def calcular_variaciones_quejas(
    df_quejas: pd.DataFrame,
    mes_actual: str,
) -> pd.DataFrame:
    """
    Calcula variaciones MoM y YoY de quejas por motivo.
    
    Args:
        df_quejas: DataFrame con quejas por motivo y mes
                  (index: MOTIVO, columns: meses)
        mes_actual: Mes actual en formato YYYYMM
    
    Returns:
        DataFrame con variaciones de quejas
    """
    logger.debug(f"Calculando variaciones de quejas para mes: {mes_actual}")

    mes_anterior = calcular_mes_anterior(mes_actual)
    mes_año_anterior = calcular_mes_año_anterior(mes_actual)

    resultados = []

    for motivo in df_quejas.index:
        quejas_actual = (
            df_quejas.loc[motivo, mes_actual] if mes_actual in df_quejas.columns else None
        )
        quejas_mes_anterior = (
            df_quejas.loc[motivo, mes_anterior] if mes_anterior in df_quejas.columns else None
        )
        quejas_año_anterior = (
            df_quejas.loc[motivo, mes_año_anterior]
            if mes_año_anterior in df_quejas.columns
            else None
        )

        var_mom = (
            quejas_actual - quejas_mes_anterior
            if pd.notna(quejas_actual) and pd.notna(quejas_mes_anterior)
            else None
        )
        var_yoy = (
            quejas_actual - quejas_año_anterior
            if pd.notna(quejas_actual) and pd.notna(quejas_año_anterior)
            else None
        )

        resultados.append(
            {
                "MOTIVO": motivo,
                "quejas_actual": quejas_actual,
                "quejas_mes_anterior": quejas_mes_anterior,
                "variacion_mom": var_mom,
                "quejas_año_anterior": quejas_año_anterior,
                "variacion_yoy": var_yoy,
            }
        )

    df_resultado = pd.DataFrame(resultados)

    logger.info(f"✅ Variaciones de quejas calculadas para {len(df_resultado)} motivos")

    return df_resultado
