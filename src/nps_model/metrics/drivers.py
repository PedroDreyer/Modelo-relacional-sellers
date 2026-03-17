"""
Cálculo de shares por dimensión para NPS Sellers.

En el modelo Buyer, este módulo calculaba shares de drivers operacionales.
En el modelo Sellers, calcula shares por dimensiones de segmentación.
"""

import logging

import pandas as pd

logger = logging.getLogger(__name__)


def calcular_shares_por_dimension(
    df: pd.DataFrame,
    dimension: str,
    meses: list[str],
) -> pd.DataFrame:
    """
    Calcula shares (% de encuestas) por valor de una dimensión.
    
    Args:
        df: DataFrame con datos de encuestas
        dimension: Columna de la dimensión (ej: "SEGMENTO_TAMANO_SELLER")
        meses: Lista de meses a analizar
    
    Returns:
        DataFrame pivoteado con valores de dimensión como índice y meses como columnas
    """
    if dimension not in df.columns:
        logger.warning(f"Dimensión '{dimension}' no encontrada")
        return pd.DataFrame()
    
    resultados = []
    
    for mes in meses:
        df_mes = df[df["END_DATE_MONTH"] == mes].copy()
        total_mes = len(df_mes)
        
        if total_mes == 0:
            continue
        
        conteos = df_mes[dimension].value_counts()
        
        for valor, count in conteos.items():
            share = (count / total_mes) * 100
            resultados.append({
                dimension: valor,
                "END_DATE_MONTH": mes,
                "share": share,
                "count": count,
                "total": total_mes,
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
