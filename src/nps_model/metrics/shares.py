"""
Módulo para calcular shares (participación %) por dimensión.

Los shares son el % que representa cada valor de una dimensión respecto al total.
Por ejemplo: FULL = 45%, XD = 30%, FLEX = 25% del total de órdenes.
"""

import pandas as pd
from typing import List, Optional


def calcular_shares_dimension(
    df: pd.DataFrame,
    dimension: str,
    meses: List[str],
    columna_conteo: str = "ORDER_ID",
) -> pd.DataFrame:
    """
    Calcula el share (%) de cada valor de una dimensión por mes.
    
    Args:
        df: DataFrame con los datos
        dimension: Nombre de la columna de dimensión (ej: 'SHP_PICKING_TYPE_CAT')
        meses: Lista de meses a analizar
        columna_conteo: Columna para contar (default: ORDER_ID)
    
    Returns:
        DataFrame con shares por mes
        Columnas: [dimension, END_DATE_MONTH, cantidad, share_%]
    """
    resultado = []
    
    for mes in meses:
        df_mes = df[df['END_DATE_MONTH'] == mes]
        
        if len(df_mes) == 0:
            continue
        
        # Contar por dimensión
        conteo = df_mes.groupby(dimension)[columna_conteo].nunique().reset_index()
        conteo.columns = [dimension, 'cantidad']
        
        # Calcular share
        total = conteo['cantidad'].sum()
        if total > 0:
            conteo['share_%'] = (conteo['cantidad'] / total) * 100
        else:
            conteo['share_%'] = 0
        
        conteo['END_DATE_MONTH'] = mes
        resultado.append(conteo)
    
    if resultado:
        df_resultado = pd.concat(resultado, ignore_index=True)
        return df_resultado[[dimension, 'END_DATE_MONTH', 'cantidad', 'share_%']]
    else:
        return pd.DataFrame(columns=[dimension, 'END_DATE_MONTH', 'cantidad', 'share_%'])


def calcular_variaciones_shares(
    df_shares: pd.DataFrame,
    dimension: str,
    mes_actual: str,
    mes_anterior: str,
    mes_año_anterior: Optional[str] = None,
) -> pd.DataFrame:
    """
    Calcula variaciones MoM y YoY de shares.
    
    Args:
        df_shares: DataFrame con shares por mes
        dimension: Nombre de la columna de dimensión
        mes_actual: Mes actual
        mes_anterior: Mes anterior
        mes_año_anterior: Mes del año anterior (opcional)
    
    Returns:
        DataFrame con variaciones
        Columnas: [dimension, share_actual, share_anterior, variacion_mom, ...]
    """
    # Shares del mes actual
    actual = df_shares[df_shares['END_DATE_MONTH'] == mes_actual].copy()
    actual = actual.rename(columns={'share_%': 'share_actual'})
    
    # Shares del mes anterior
    anterior = df_shares[df_shares['END_DATE_MONTH'] == mes_anterior].copy()
    anterior = anterior.rename(columns={'share_%': 'share_anterior'})
    
    # Merge
    resultado = actual[[dimension, 'share_actual']].merge(
        anterior[[dimension, 'share_anterior']],
        on=dimension,
        how='outer'
    )
    
    # Calcular variación MoM
    resultado['variacion_mom'] = resultado['share_actual'] - resultado['share_anterior']
    
    # YoY si está disponible
    if mes_año_anterior:
        año_ant = df_shares[df_shares['END_DATE_MONTH'] == mes_año_anterior].copy()
        año_ant = año_ant.rename(columns={'share_%': 'share_año_anterior'})
        
        resultado = resultado.merge(
            año_ant[[dimension, 'share_año_anterior']],
            on=dimension,
            how='left'
        )
        
        resultado['variacion_yoy'] = resultado['share_actual'] - resultado['share_año_anterior']
    
    return resultado


def calcular_share_driver_boolean(
    df: pd.DataFrame,
    columna_driver: str,
    condicion: str,
    meses: List[str],
    nombre_driver: str,
) -> pd.DataFrame:
    """
    Calcula share de un driver booleano (ej: Free Shipping, Delay).
    
    Args:
        df: DataFrame con los datos
        columna_driver: Columna del driver
        condicion: Condición para considerar True (ej: '>=1', '==True')
        meses: Lista de meses
        nombre_driver: Nombre descriptivo del driver
    
    Returns:
        DataFrame con shares del driver por mes
    """
    resultado = []
    
    for mes in meses:
        df_mes = df[df['END_DATE_MONTH'] == mes]
        
        if len(df_mes) == 0:
            continue
        
        total = len(df_mes)
        
        # Aplicar condición
        if condicion == '>=1':
            cantidad = len(df_mes[df_mes[columna_driver] >= 1])
        elif condicion == '==True' or condicion == True:
            cantidad = len(df_mes[df_mes[columna_driver] == True])
        elif condicion == '>0':
            cantidad = len(df_mes[df_mes[columna_driver] > 0])
        else:
            cantidad = len(df_mes[df_mes[columna_driver] == condicion])
        
        share = (cantidad / total) * 100 if total > 0 else 0
        
        resultado.append({
            'driver': nombre_driver,
            'END_DATE_MONTH': mes,
            'cantidad': cantidad,
            'total': total,
            'share_%': share
        })
    
    return pd.DataFrame(resultado)
