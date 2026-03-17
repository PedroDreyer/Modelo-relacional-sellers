"""
Módulo para calcular efectos NPS, MIX y NETO.

Este es el análisis más importante del modelo. Permite descomponer la variación
del NPS total en contribuciones de cada dimensión (ej: PICKING_TYPE, VERTICAL).

Fórmulas (del notebook original):
- Efecto_NPS = (NPS_actual - NPS_anterior) × (Share_anterior / 100)
- Efecto_MIX = ((Share_actual - Share_anterior) / 100) × NPS_actual
- Efecto_NETO = Efecto_NPS + Efecto_MIX

Validación crítica: 
  Suma de Efecto_NETO de todos los valores ≈ Variación total de NPS
"""

import pandas as pd
import numpy as np
from typing import Optional


def calcular_efectos_dimension(
    df_nps: pd.DataFrame,
    df_shares: pd.DataFrame,
    dimension: str,
    mes_actual: str,
    mes_anterior: str,
    nps_total_actual: float,
    nps_total_anterior: float,
) -> pd.DataFrame:
    """
    Calcula efectos NPS, MIX y NETO para una dimensión.
    
    Args:
        df_nps: DataFrame con NPS por dimensión y mes
                Columnas: [dimension, END_DATE_MONTH, NPS_score]
        df_shares: DataFrame con shares por dimensión y mes
                   Columnas: [dimension, END_DATE_MONTH, share_%, cantidad]
        dimension: Nombre de la columna de dimensión
        mes_actual: Mes actual en formato YYYYMM
        mes_anterior: Mes anterior en formato YYYYMM
        nps_total_actual: NPS total del mes actual (para validación)
        nps_total_anterior: NPS total del mes anterior (para validación)
    
    Returns:
        DataFrame con efectos calculados
        Columnas: [dimension_value, NPS_actual, NPS_anterior, Share_actual, 
                   Share_anterior, Efecto_NPS, Efecto_MIX, Efecto_NETO]
    """
    # Obtener datos del mes actual
    df_actual = df_nps[df_nps['END_DATE_MONTH'] == mes_actual].copy()
    df_actual = df_actual.rename(columns={'NPS_score': 'NPS_actual'})
    
    # Obtener datos del mes anterior
    df_anterior = df_nps[df_nps['END_DATE_MONTH'] == mes_anterior].copy()
    df_anterior = df_anterior.rename(columns={'NPS_score': 'NPS_anterior'})
    
    # Obtener shares del mes actual
    shares_actual = df_shares[df_shares['END_DATE_MONTH'] == mes_actual].copy()
    shares_actual = shares_actual.rename(columns={'share_%': 'Share_actual'})
    
    # Obtener shares del mes anterior
    shares_anterior = df_shares[df_shares['END_DATE_MONTH'] == mes_anterior].copy()
    shares_anterior = shares_anterior.rename(columns={'share_%': 'Share_anterior'})
    
    # Merge todos los datos
    df_resultado = df_actual[[dimension, 'NPS_actual']].merge(
        df_anterior[[dimension, 'NPS_anterior']],
        on=dimension,
        how='outer'
    )
    
    df_resultado = df_resultado.merge(
        shares_actual[[dimension, 'Share_actual']],
        on=dimension,
        how='outer'
    )
    
    df_resultado = df_resultado.merge(
        shares_anterior[[dimension, 'Share_anterior']],
        on=dimension,
        how='outer'
    )
    
    has_data = (
        df_resultado['NPS_actual'].notna() &
        df_resultado['NPS_anterior'].notna() &
        df_resultado['Share_actual'].notna() &
        df_resultado['Share_anterior'].notna()
    )
    
    df_resultado['Efecto_NPS'] = np.where(
        has_data,
        (df_resultado['NPS_actual'] - df_resultado['NPS_anterior']) * (df_resultado['Share_anterior'] / 100),
        None,
    )
    df_resultado['Efecto_MIX'] = np.where(
        has_data,
        ((df_resultado['Share_actual'] - df_resultado['Share_anterior']) / 100) * df_resultado['NPS_actual'],
        None,
    )
    df_resultado['Efecto_NPS'] = pd.to_numeric(df_resultado['Efecto_NPS'], errors='coerce')
    df_resultado['Efecto_MIX'] = pd.to_numeric(df_resultado['Efecto_MIX'], errors='coerce')
    df_resultado['Efecto_NETO'] = df_resultado['Efecto_NPS'] + df_resultado['Efecto_MIX']
    
    # Validación: La suma de efectos NETO debe aproximarse a la variación total
    suma_efectos = df_resultado['Efecto_NETO'].sum()
    variacion_total = nps_total_actual - nps_total_anterior
    diferencia = abs(suma_efectos - variacion_total)
    
    print(f"   📊 Efectos calculados para {dimension}:")
    print(f"      Suma de efectos NETO: {suma_efectos:.2f}pp")
    print(f"      Variación total NPS: {variacion_total:.2f}pp")
    print(f"      Diferencia: {diferencia:.2f}pp", end="")
    
    if diferencia <= 0.1:
        print(" ✅ (Validación OK)")
    else:
        print(" ⚠️ (Revisar: diferencia mayor a 0.1pp)")
    
    return df_resultado


def ordenar_por_impacto(
    df_efectos: pd.DataFrame,
    dimension: str,
    nps_sube: bool = True,
) -> pd.DataFrame:
    """
    Ordena los valores de una dimensión por su impacto (Efecto_NETO).
    
    Si el NPS sube, ordena por mayor contribución positiva primero.
    Si el NPS baja, ordena por mayor contribución negativa primero.
    
    Args:
        df_efectos: DataFrame con efectos calculados
        dimension: Nombre de la columna de dimensión
        nps_sube: Si el NPS total subió
    
    Returns:
        DataFrame ordenado
    """
    df_ordenado = df_efectos.copy()
    
    if nps_sube:
        # Si NPS sube, mostrar primero los que más aportaron (positivos)
        df_ordenado = df_ordenado.sort_values('Efecto_NETO', ascending=False)
    else:
        # Si NPS baja, mostrar primero los que más restaron (negativos)
        df_ordenado = df_ordenado.sort_values('Efecto_NETO', ascending=True)
    
    return df_ordenado


def clasificar_contribucion(efecto_neto: float, umbral: float = 0.3) -> str:
    """
    Clasifica la contribución de un valor según su efecto neto.
    
    Args:
        efecto_neto: Efecto neto calculado
        umbral: Umbral para considerar significativo
    
    Returns:
        "APORTA" si efecto > umbral
        "RESTA" si efecto < -umbral
        "NEUTRAL" si |efecto| < umbral
    """
    if pd.isna(efecto_neto):
        return "SIN_DATOS"
    
    if efecto_neto > umbral:
        return "APORTA"
    elif efecto_neto < -umbral:
        return "RESTA"
    else:
        return "NEUTRAL"


def validar_suma_efectos(
    df_efectos: pd.DataFrame,
    variacion_total: float,
    tolerancia: float = 0.1,
) -> bool:
    """
    Valida que la suma de efectos NETO sea consistente con la variación total.
    
    Args:
        df_efectos: DataFrame con efectos calculados
        variacion_total: Variación total del NPS
        tolerancia: Margen de error aceptable (en pp)
    
    Returns:
        True si la validación pasa, False si no
    """
    suma_efectos = df_efectos['Efecto_NETO'].sum()
    diferencia = abs(suma_efectos - variacion_total)
    
    return diferencia <= tolerancia
