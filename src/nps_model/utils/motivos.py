"""
Normalización y consolidación de motivos de queja para consistencia en el análisis.

Dos niveles:
1. Consolidación: mapea wording viejo y nuevo a categorías estandarizadas (español).
   Resuelve el problema del cambio de wording de encuesta (~Sep-Oct 2025).
2. Seguridad legacy: agrupa variantes de "Seguridad" (mantenido por compatibilidad).
"""

import pandas as pd
from .constants import MOTIVO_NORMALIZACION_SEGURIDAD


# ==========================================
# MAPEO DE CONSOLIDACIÓN DE MOTIVOS
# ==========================================
# Cada tupla: (patrón a buscar en minúsculas, motivo consolidado)
# El orden importa: la primera coincidencia gana.
# Cubre portugués (BR), español (AR/MX/CL) y variantes de wording viejo/nuevo.

MOTIVO_CONSOLIDACION_PATRONES = [
    # --- Atención al cliente ---
    ("atendimento ao cliente", "Atención al cliente"),
    ("atención al cliente", "Atención al cliente"),
    ("atencion al cliente", "Atención al cliente"),
    # --- Cobro en cuotas / Parcelamento ---
    ("falta de parcelamento", "Cobro en cuotas"),
    ("parcelamento sem juros", "Cobro en cuotas"),
    ("vendas parceladas", "Cobro en cuotas"),
    ("cobro en cuotas", "Cobro en cuotas"),
    ("cobros en cuotas", "Cobro en cuotas"),
    ("cobros a meses", "Cobro en cuotas"),
    ("ventas en cuotas", "Cobro en cuotas"),
    ("promociones y cuotas", "Cobro en cuotas"),
    ("meses sin interés", "Cobro en cuotas"),
    ("meses sin interes", "Cobro en cuotas"),
    ("falta de cuotas", "Cobro en cuotas"),
    # --- Cobros rechazados / Pagamentos recusados ---
    ("pagamentos recusados", "Cobros rechazados"),
    ("cobros rechazados", "Cobros rechazados"),
    # --- Comisiones y cargos / Taxas ---
    ("taxas e comissões por venda", "Comisiones y cargos"),
    ("taxas e comissoes por venda", "Comisiones y cargos"),
    ("comissões por venda", "Comisiones y cargos"),
    ("taxas e custos", "Comisiones y cargos"),
    ("taxas e comissões", "Comisiones y cargos"),
    ("comisiones y cargos", "Comisiones y cargos"),
    ("comisiones por venta", "Comisiones y cargos"),
    ("comisiones y cargos de venta", "Comisiones y cargos"),
    # --- Contracargos ---
    ("operações contestadas", "Contracargos"),
    ("operaciones contestadas", "Contracargos"),
    ("contracargos", "Contracargos"),
    # --- Crédito ---
    ("empréstimo ou cartão de crédito", "Crédito"),
    ("emprestimo ou cartao de credito", "Crédito"),
    ("empréstimo o tarjeta de crédito", "Crédito"),
    ("crédito", "Crédito"),
    ("credito", "Crédito"),
    # --- Seguridad ---
    ("falta de segurança da conta", "Falta de seguridad en la cuenta"),
    ("falta de seguridad de la cuenta", "Falta de seguridad en la cuenta"),
    ("falta de seguridad en la cuenta", "Falta de seguridad en la cuenta"),
    ("segurança da conta", "Falta de seguridad en la cuenta"),
    ("seguridad de la cuenta", "Falta de seguridad en la cuenta"),
    # --- Inversiones ---
    ("investimentos e retornos baixos", "Inversiones y rendimiento de dinero en cuenta"),
    ("inversiones y rendimiento", "Inversiones y rendimiento de dinero en cuenta"),
    # --- Plazo de disponibilidad ---
    ("prazo para disponibilização", "Plazo de disponibilidad del dinero"),
    ("prazo para disponibilizacao", "Plazo de disponibilidad del dinero"),
    ("plazo de disponibilidad", "Plazo de disponibilidad del dinero"),
    # --- Problemas con funcionalidades ---
    ("problemas com as funcionalidades", "Problemas con las funcionalidades de la cuenta"),
    ("problemas con las funcionalidades", "Problemas con las funcionalidades de la cuenta"),
    ("problemas de funcionamiento", "Problemas con las funcionalidades de la cuenta"),
    # --- Calidad device (Point-specific) ---
    ("qualidade e funcionamento da maquininha", "Calidad y funcionamiento del dispositivo"),
    ("calidad y facilidad de uso del dispositivo", "Calidad y funcionamiento del dispositivo"),
    ("calidad y dificultad de uso del dispositivo", "Calidad y funcionamiento del dispositivo"),
    ("calidad del dispositivo", "Calidad y funcionamiento del dispositivo"),
    ("calidad y funcionamiento", "Calidad y funcionamiento del dispositivo"),
    # --- Servicios adicionales (MLC-specific) ---
    ("servicios adicionales", "Servicios adicionales"),
    ("serviços adicionais", "Servicios adicionales"),
    # --- Problemas funcionamiento herramienta ---
    ("problemas no funcionamento da ferramenta", "Problemas con las funcionalidades de la cuenta"),
    ("problemas en el funcionamiento de la herramienta", "Problemas con las funcionalidades de la cuenta"),
    # --- Integración ---
    ("dificuldade para integrar", "Otros"),
    ("dificultad para integrar", "Otros"),
    # --- Otros ---
    ("outro - por favor", "Otros"),
    ("otro - por favor", "Otros"),
    ("otro motivo", "Otros"),
    ("outros motivos", "Otros"),
    ("otros motivos", "Otros"),
]


def consolidar_motivo(valor: str) -> str:
    """Aplica el mapeo de consolidación a un valor de motivo."""
    if pd.isna(valor) or str(valor).strip() in ("", "nan", "None"):
        return "Sin información"
    lower = str(valor).strip().lower()
    for patron, consolidado in MOTIVO_CONSOLIDACION_PATRONES:
        if patron in lower:
            return consolidado
    return str(valor).strip()


def consolidar_motivos_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Consolida motivos en todas las columnas relevantes del DataFrame.
    Aplica el mapeo de wording viejo → nuevo para que los motivos sean
    comparables entre quarters (resuelve cambio de wording Sep-Oct 2025).

    Columnas afectadas: MOTIVO, DETRACTION_REASON_NPS, NEUTRAL_REASON_NPS, PROMOTION_REASON_NPS
    """
    df = df.copy()
    cols_motivo = ["MOTIVO", "DETRACTION_REASON_NPS", "NEUTRAL_REASON_NPS", "PROMOTION_REASON_NPS"]
    for col in cols_motivo:
        if col in df.columns:
            df[col] = df[col].apply(consolidar_motivo)
    return df


def normalizar_motivo_col(df: pd.DataFrame, motivo_col: str = "MOTIVO") -> pd.DataFrame:
    """
    Normaliza la columna de motivos en el DataFrame.
    Ahora usa consolidar_motivos_df() que aplica el mapeo completo.
    Mantiene compatibilidad con llamadas existentes.
    """
    return consolidar_motivos_df(df)
