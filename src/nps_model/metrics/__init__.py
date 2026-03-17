"""Módulo de métricas NPS"""

from .nps import calcular_nps_total, calcular_nps_por_dimension
from .shares import calcular_shares_dimension, calcular_variaciones_shares

__all__ = [
    "calcular_nps_total",
    "calcular_nps_por_dimension",
    "calcular_shares_dimension",
    "calcular_variaciones_shares",
]
