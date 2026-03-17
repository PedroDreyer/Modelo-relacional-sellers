"""
Módulo de análisis para NPS Relacional Sellers.

Adaptado del modelo Buyer:
- Sin análisis de drivers operacionales (no hay datos reales)
- Sin correlaciones de quejas con drivers reales
- Sin análisis de logísticas
- Dimensiones de sellers en vez de vertical/logística
"""

from .efectos import (
    calcular_efectos_dimension,
    ordenar_por_impacto,
)

__all__ = [
    "calcular_efectos_dimension",
    "ordenar_por_impacto",
]
