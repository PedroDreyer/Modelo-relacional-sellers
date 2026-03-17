"""
Calculo de shares de motivos de quejas en base de encuestas NPS Relacional Sellers.

IMPORTANTE: A diferencia del modelo Buyer que tiene drivers operacionales (Delay, PDD, etc.),
el modelo Sellers usa directamente los motivos de las encuestas (NPS_TX_MPROM, NPS_TX_MDET,
NPS_TX_MNEUTRO). Los "drivers" aqui son los motivos de queja reportados por los sellers.

Dimensiones de segmentacion de sellers:
- SEGMENTO_TAMANO_SELLER
- SEGMENTO_CROSSMP
- POINT_DEVICE_TYPE
- E_CODE
- PF_PJ
"""

import logging
from typing import List, Dict, Optional

import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)


from nps_model.utils.constants import DIMENSIONES_SELLERS


# ==========================================
# SHARES POR MOTIVO
# ==========================================

def calcular_shares_por_motivo(
    df: pd.DataFrame,
    meses: list,
    motivo_col: str = "MOTIVO",
) -> dict:
    """
    Calcula share (%) de cada motivo de queja por mes.

    Para cada mes, cuenta cuantos detractores+neutros tienen cada motivo
    y divide por total de detractores+neutros.

    Formula de quejas: %neutros + 2 x %detractores (sobre total encuestas).

    Args:
        df: DataFrame con datos de encuestas sellers
        meses: Lista de meses a analizar (formato YYYYMM)
        motivo_col: Columna de motivos (default: MOTIVO)

    Returns:
        Diccionario con estructura por motivo:
        {
            "MOTIVO_NAME": {
                "driver_name": "...",
                "meses": [...],
                "share": [...],
                "quejas": [...],
                "count_total": [...],
                "count_promotores": [...],
                "count_neutros": [...],
                "count_detractores": [...]
            }
        }
    """
    resultados = {}

    for mes in meses:
        df_mes = df[df["END_DATE_MONTH"] == mes].copy()
        total_encuestas = len(df_mes)

        if total_encuestas == 0:
            continue

        motivos = df_mes[motivo_col].dropna().unique()

        for motivo in motivos:
            df_motivo = df_mes[df_mes[motivo_col] == motivo]

            neutros = len(df_motivo[df_motivo["NPS"] == 0])
            detractores = len(df_motivo[df_motivo["NPS"] == -1])
            promotores = len(df_motivo[df_motivo["NPS"] == 1])

            # Formula de quejas
            pct_neutros = (neutros / total_encuestas) * 100
            pct_detractores = (detractores / total_encuestas) * 100
            quejas = pct_neutros + (2 * pct_detractores)

            # Share simple (% del total que menciona este motivo)
            count_total_motivo = len(df_motivo)
            share = (count_total_motivo / total_encuestas) * 100

            if motivo not in resultados:
                resultados[motivo] = {
                    "driver_name": motivo,
                    "meses": [],
                    "share": [],
                    "quejas": [],
                    "count_total": [],
                    "count_promotores": [],
                    "count_neutros": [],
                    "count_detractores": [],
                }

            resultados[motivo]["meses"].append(mes)
            resultados[motivo]["share"].append(share)
            resultados[motivo]["quejas"].append(quejas)
            resultados[motivo]["count_total"].append(int(count_total_motivo))
            resultados[motivo]["count_promotores"].append(int(promotores))
            resultados[motivo]["count_neutros"].append(int(neutros))
            resultados[motivo]["count_detractores"].append(int(detractores))

    logger.info(f"Calculados shares para {len(resultados)} motivos")
    return resultados


def calcular_variaciones_motivo_shares(
    resultado: dict,
    mes_actual: str,
    mes_anterior: str,
) -> dict:
    """
    Calcula variaciones MoM de shares de motivos.

    Args:
        resultado: Dict con shares por mes (un motivo individual)
        mes_actual: Mes actual (formato YYYYMM)
        mes_anterior: Mes anterior (formato YYYYMM)

    Returns:
        Diccionario actualizado con variaciones MoM
    """
    if not resultado or "meses" not in resultado:
        return resultado

    meses = resultado["meses"]
    shares = resultado.get("share", [])
    quejas = resultado.get("quejas", [])

    if not shares:
        return resultado

    try:
        idx_actual = meses.index(mes_actual)
        idx_anterior = meses.index(mes_anterior)

        resultado["mes_actual"] = mes_actual
        resultado["share_actual"] = shares[idx_actual]
        resultado["share_anterior"] = shares[idx_anterior]
        resultado["var_share_mom"] = shares[idx_actual] - shares[idx_anterior]

        if quejas:
            resultado["quejas_actual"] = quejas[idx_actual]
            resultado["quejas_anterior"] = quejas[idx_anterior]
            resultado["var_quejas_mom"] = quejas[idx_actual] - quejas[idx_anterior]

    except (ValueError, IndexError) as e:
        logger.warning(f"No se pudo calcular variacion MoM: {e}")

    return resultado


def calcular_todos_los_drivers_shares(
    df: pd.DataFrame,
    meses: list,
    motivo_col: str = "MOTIVO",
) -> dict:
    """
    Calcula shares de TODOS los motivos en la base de encuestas sellers.

    Entry point principal. Devuelve resultado en formato compatible con el pipeline.

    Args:
        df: DataFrame con datos de encuestas NPS Sellers
        meses: Lista de meses a analizar
        motivo_col: Columna de motivos

    Returns:
        Diccionario con todos los resultados por motivo
    """
    logger.info(f"Calculando shares de motivos para {len(meses)} meses")
    return calcular_shares_por_motivo(df, meses, motivo_col)


# Alias for backward compatibility
calcular_variaciones_driver_shares = calcular_variaciones_motivo_shares


# ==========================================
# ANALISIS POR DIMENSIONES DE SELLERS
# ==========================================

def calcular_nps_por_dimension(
    df: pd.DataFrame,
    dimension: str,
    meses: list,
) -> pd.DataFrame:
    """
    Calcula NPS score por una dimension de segmentacion de sellers.

    Args:
        df: DataFrame con datos de encuestas
        dimension: Nombre de la columna dimension
            (ej: SEGMENTO_TAMANO_SELLER, SEGMENTO_CROSSMP, POINT_DEVICE_TYPE, E_CODE, PF_PJ)
        meses: Lista de meses a incluir (formato YYYYMM)

    Returns:
        DataFrame pivoteado con dimension en filas y meses en columnas,
        valores son NPS score (-100 a 100)
    """
    if dimension not in df.columns:
        logger.warning(f"Dimension {dimension} no encontrada en DataFrame")
        return pd.DataFrame()

    df_filtered = df[df["END_DATE_MONTH"].isin(meses)].copy()

    if len(df_filtered) == 0:
        return pd.DataFrame()

    nps_dim = (
        df_filtered.groupby([dimension, "END_DATE_MONTH"], as_index=False)["NPS"]
        .mean()
    )
    nps_dim["NPS_score"] = nps_dim["NPS"] * 100

    nps_pivot = nps_dim.pivot(
        index=dimension,
        columns="END_DATE_MONTH",
        values="NPS_score",
    )

    for mes in meses:
        if mes not in nps_pivot.columns:
            nps_pivot[mes] = None

    nps_pivot = nps_pivot[sorted(nps_pivot.columns)]
    return nps_pivot


def calcular_shares_por_dimension(
    df: pd.DataFrame,
    dimension: str,
    meses: list,
) -> pd.DataFrame:
    """
    Calcula el share (%) de cada valor de una dimension de sellers.

    Ejemplo: Para SEGMENTO_TAMANO_SELLER, calcula % de Micro, Small, Medium, etc.

    Args:
        df: DataFrame con datos de encuestas
        dimension: Nombre de la columna dimension
        meses: Lista de meses a analizar

    Returns:
        DataFrame pivoteado con shares por valor de dimension y mes
    """
    if dimension not in df.columns:
        logger.warning(f"Dimension {dimension} no encontrada en DataFrame")
        return pd.DataFrame()

    resultados = []

    for mes in meses:
        df_mes = df[df["END_DATE_MONTH"] == mes].copy()
        total = len(df_mes)

        if total == 0:
            continue

        counts = df_mes[dimension].value_counts()

        for valor, count in counts.items():
            share = (count / total) * 100
            resultados.append({
                "END_DATE_MONTH": mes,
                dimension: valor,
                "share": share,
                "count": count,
                "total": total,
            })

    df_resultado = pd.DataFrame(resultados)

    if not df_resultado.empty:
        df_pivot = df_resultado.pivot(
            index=dimension, columns="END_DATE_MONTH", values="share"
        )
        for mes in meses:
            if mes not in df_pivot.columns:
                df_pivot[mes] = 0.0
        df_pivot = df_pivot[sorted(df_pivot.columns)]
        return df_pivot
    else:
        return pd.DataFrame()


def calcular_efectos_dimension(
    df: pd.DataFrame,
    dimension: str,
    mes_actual: str,
    mes_anterior: str,
    nps_total_actual: float,
    nps_total_anterior: float,
) -> pd.DataFrame:
    """
    Calcula efectos NPS, MIX y NETO para una dimension de sellers.

    Formulas:
    - Efecto_NPS = (NPS_actual - NPS_anterior) x (Share_anterior / 100)
    - Efecto_MIX = ((Share_actual - Share_anterior) / 100) x NPS_actual
    - Efecto_NETO = Efecto_NPS + Efecto_MIX

    Validacion: Suma de Efecto_NETO ~ Variacion total de NPS

    Args:
        df: DataFrame con datos de encuestas
        dimension: Nombre de la columna dimension
        mes_actual: Mes actual en formato YYYYMM
        mes_anterior: Mes anterior en formato YYYYMM
        nps_total_actual: NPS total del mes actual
        nps_total_anterior: NPS total del mes anterior

    Returns:
        DataFrame con efectos calculados por valor de dimension
    """
    if dimension not in df.columns:
        logger.warning(f"Dimension {dimension} no encontrada en DataFrame")
        return pd.DataFrame()

    nps_pivot = calcular_nps_por_dimension(df, dimension, [mes_actual, mes_anterior])
    shares_pivot = calcular_shares_por_dimension(df, dimension, [mes_actual, mes_anterior])

    if nps_pivot.empty or shares_pivot.empty:
        return pd.DataFrame()

    valores_dim = list(set(list(nps_pivot.index) + list(shares_pivot.index)))
    filas = []

    for valor in valores_dim:
        nps_act = nps_pivot.loc[valor, mes_actual] if (valor in nps_pivot.index and mes_actual in nps_pivot.columns) else None
        nps_ant = nps_pivot.loc[valor, mes_anterior] if (valor in nps_pivot.index and mes_anterior in nps_pivot.columns) else None
        share_act = shares_pivot.loc[valor, mes_actual] if (valor in shares_pivot.index and mes_actual in shares_pivot.columns) else None
        share_ant = shares_pivot.loc[valor, mes_anterior] if (valor in shares_pivot.index and mes_anterior in shares_pivot.columns) else None

        fila = {
            dimension: valor,
            "NPS_actual": nps_act,
            "NPS_anterior": nps_ant,
            "Share_actual": share_act,
            "Share_anterior": share_ant,
            "Efecto_NPS": None,
            "Efecto_MIX": None,
            "Efecto_NETO": None,
        }

        if (pd.notna(nps_act) and pd.notna(nps_ant) and
                pd.notna(share_act) and pd.notna(share_ant)):
            efecto_nps = (nps_act - nps_ant) * (share_ant / 100)
            efecto_mix = ((share_act - share_ant) / 100) * nps_act
            fila["Efecto_NPS"] = efecto_nps
            fila["Efecto_MIX"] = efecto_mix
            fila["Efecto_NETO"] = efecto_nps + efecto_mix

        filas.append(fila)

    df_resultado = pd.DataFrame(filas)

    # Validacion
    if not df_resultado.empty and df_resultado["Efecto_NETO"].notna().any():
        suma_efectos = df_resultado["Efecto_NETO"].sum()
        variacion_total = nps_total_actual - nps_total_anterior
        diferencia = abs(suma_efectos - variacion_total)
        if diferencia > 0.5:
            logger.warning(
                f"Diferencia en efectos para {dimension}: "
                f"suma={suma_efectos:.2f}pp vs variacion={variacion_total:.2f}pp "
                f"(dif={diferencia:.2f}pp)"
            )

    return df_resultado


def analizar_por_dimension(
    df: pd.DataFrame,
    dimension: str,
    meses: list,
    mes_actual: str,
) -> dict:
    """
    Analisis completo de NPS y shares por una dimension especifica de sellers.

    Args:
        df: DataFrame con datos
        dimension: Nombre de la columna dimension
        meses: Lista de meses a analizar
        mes_actual: Mes actual en formato YYYYMM

    Returns:
        Diccionario con nps_por_dimension, variaciones_nps, shares,
        variaciones_shares, dimension_name
    """
    logger.info(f"Analizando dimension: {dimension}")

    if dimension not in df.columns:
        logger.error(f"Dimension '{dimension}' no encontrada en el DataFrame")
        return {}

    # Calcular mes anterior y mes anio anterior
    year = int(mes_actual[:4])
    month = int(mes_actual[4:])
    m = month - 1
    y = year
    if m == 0:
        m = 12
        y -= 1
    mes_anterior = f"{y}{m:02d}"
    mes_anio_anterior = f"{year - 1}{month:02d}"

    # 1. NPS por dimension
    nps_dim = calcular_nps_por_dimension(df, dimension, meses)

    # 2. Variaciones de NPS
    variaciones_nps = []
    if not nps_dim.empty:
        for valor_dim in nps_dim.index:
            nps_act = nps_dim.loc[valor_dim, mes_actual] if mes_actual in nps_dim.columns else None
            nps_ant = nps_dim.loc[valor_dim, mes_anterior] if mes_anterior in nps_dim.columns else None
            nps_yoy = nps_dim.loc[valor_dim, mes_anio_anterior] if mes_anio_anterior in nps_dim.columns else None

            var_mom = nps_act - nps_ant if pd.notna(nps_act) and pd.notna(nps_ant) else None
            var_yoy = nps_act - nps_yoy if pd.notna(nps_act) and pd.notna(nps_yoy) else None

            variaciones_nps.append({
                dimension: valor_dim,
                "NPS_actual": nps_act,
                "NPS_mes_anterior": nps_ant,
                "Variacion_MoM": var_mom,
                "NPS_anio_anterior": nps_yoy,
                "Variacion_YoY": var_yoy,
            })

    df_var_nps = pd.DataFrame(variaciones_nps)

    # 3. Shares de la dimension
    shares_dim = calcular_shares_por_dimension(df, dimension, meses)

    # 4. Variaciones de shares
    variaciones_shares = []
    if not shares_dim.empty:
        for valor_dim in shares_dim.index:
            share_act = shares_dim.loc[valor_dim, mes_actual] if mes_actual in shares_dim.columns else None
            share_ant = shares_dim.loc[valor_dim, mes_anterior] if mes_anterior in shares_dim.columns else None
            share_yoy = shares_dim.loc[valor_dim, mes_anio_anterior] if mes_anio_anterior in shares_dim.columns else None

            var_mom = share_act - share_ant if pd.notna(share_act) and pd.notna(share_ant) else None
            var_yoy = share_act - share_yoy if pd.notna(share_act) and pd.notna(share_yoy) else None

            variaciones_shares.append({
                dimension: valor_dim,
                "share_actual": share_act,
                "share_mes_anterior": share_ant,
                "variacion_mom": var_mom,
                "share_anio_anterior": share_yoy,
                "variacion_yoy": var_yoy,
            })

    df_var_shares = pd.DataFrame(variaciones_shares)

    n_vals = len(nps_dim) if not nps_dim.empty else 0
    logger.info(f"Analisis completado: {n_vals} valores unicos en {dimension}")

    return {
        "nps_por_dimension": nps_dim,
        "variaciones_nps": df_var_nps,
        "shares": shares_dim,
        "variaciones_shares": df_var_shares,
        "dimension_name": dimension,
    }


def analizar_todas_dimensiones(
    df: pd.DataFrame,
    meses: list,
    mes_actual: str,
    dimensiones: list = None,
) -> dict:
    """
    Ejecuta analisis completo por todas las dimensiones de sellers.

    Args:
        df: DataFrame con datos de encuestas
        meses: Lista de meses
        mes_actual: Mes actual en formato YYYYMM
        dimensiones: Lista de dimensiones a analizar (default: DIMENSIONES_SELLERS)

    Returns:
        Diccionario con resultados por dimension
    """
    if dimensiones is None:
        dimensiones = DIMENSIONES_SELLERS

    resultados = {}
    for dim in dimensiones:
        if dim in df.columns:
            resultados[dim] = analizar_por_dimension(df, dim, meses, mes_actual)
        else:
            logger.warning(f"Dimension {dim} no encontrada en datos, saltando")

    logger.info(f"Analisis dimensional completado: {len(resultados)} dimensiones")
    return resultados
