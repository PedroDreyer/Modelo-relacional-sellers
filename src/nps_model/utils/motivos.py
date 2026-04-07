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

# Consolidation: merge old+new wording into consistent categories.
# This ensures evolution charts are comparable across quarters (wording changed ~Sep-Oct 2025).
# Top 12 ensures enough detail for all motivos to appear.
MOTIVO_CONSOLIDACION_PATRONES = [
    # --- Atención al cliente ---
    ("atendimento ao cliente", "Atendimento ao cliente"),
    ("atención al cliente", "Atendimento ao cliente"),
    ("atencion al cliente", "Atendimento ao cliente"),
    # --- Cobro en cuotas / Parcelamento ---
    ("falta de parcelamento", "Falta de parcelamento sem juros"),
    ("parcelamento sem juros", "Falta de parcelamento sem juros"),
    ("vendas parceladas", "Falta de parcelamento sem juros"),
    ("cobro en cuotas", "Cobro en cuotas"),
    ("cobros en cuotas", "Cobro en cuotas"),
    ("cobros a meses", "Cobro en cuotas"),
    ("ventas en cuotas", "Cobro en cuotas"),
    ("promociones y cuotas", "Cobro en cuotas"),
    ("meses sin interés", "Cobro en cuotas"),
    ("meses sin interes", "Cobro en cuotas"),
    ("falta de cuotas", "Cobro en cuotas"),
    ("cuotas sin interés", "Cobro en cuotas"),
    ("cuotas sin interes", "Cobro en cuotas"),
    # --- Cobros rechazados / Pagamentos recusados ---
    ("pagamentos recusados", "Pagamentos recusados"),
    ("cobros rechazados", "Pagamentos recusados"),
    # --- Comisiones y cargos / Taxas (merge new→old wording: "Taxas e custos") ---
    ("taxas e comissões por venda", "Taxas e custos"),
    ("taxas e comissoes por venda", "Taxas e custos"),
    ("comissões por venda", "Taxas e custos"),
    ("taxas e custos", "Taxas e custos"),
    ("taxas e comissões", "Taxas e custos"),
    ("comisiones y cargos", "Taxas e custos"),
    ("comisiones por venta", "Taxas e custos"),
    ("comisiones y cargos de venta", "Taxas e custos"),
    # --- Contracargos / Operações contestadas ---
    ("operações contestadas", "Operações contestadas"),
    ("operaciones contestadas", "Operações contestadas"),
    ("contracargos", "Operações contestadas"),
    # --- Crédito ---
    ("empréstimo ou cartão de crédito", "Empréstimo ou cartão de crédito"),
    ("emprestimo ou cartao de credito", "Empréstimo ou cartão de crédito"),
    ("empréstimo o tarjeta de crédito", "Empréstimo ou cartão de crédito"),
    ("préstamos y tarjeta de crédito", "Empréstimo ou cartão de crédito"),
    ("crédito", "Empréstimo ou cartão de crédito"),
    ("credito", "Empréstimo ou cartão de crédito"),
    # --- Seguridad ---
    ("falta de segurança da conta", "Falta de segurança da conta"),
    ("segurança da conta", "Falta de segurança da conta"),
    ("falta de seguridad de la cuenta", "Falta de segurança da conta"),
    ("falta de seguridad en la cuenta", "Falta de segurança da conta"),
    ("seguridad de la cuenta", "Falta de segurança da conta"),
    # --- Inversiones ---
    ("investimentos e retornos", "Investimentos e retornos baixos com dinheiro em conta"),
    ("inversiones y rendimiento", "Investimentos e retornos baixos com dinheiro em conta"),
    # --- Plazo de disponibilidad ---
    ("prazo para disponibilização", "Prazo para disponibilização do dinheiro"),
    ("prazo para disponibilizacao", "Prazo para disponibilização do dinheiro"),
    ("plazo de disponibilidad", "Prazo para disponibilização do dinheiro"),
    # --- Problemas con funcionalidades ---
    ("problemas com as funcionalidades", "Problemas com as funcionalidades da conta"),
    ("problemas con las funcionalidades", "Problemas com as funcionalidades da conta"),
    ("problemas de funcionamiento", "Problemas com as funcionalidades da conta"),
    ("problemas no funcionamento da ferramenta", "Problemas com as funcionalidades da conta"),
    ("problemas en el funcionamiento de la herramienta", "Problemas com as funcionalidades da conta"),
    # --- Calidad device (merge new→old: "Qualidade e funcionamento") ---
    ("qualidade e funcionamento da maquininha", "Qualidade e funcionamento da maquininha"),
    ("qualidade e dificuldade de uso da maquininha", "Qualidade e funcionamento da maquininha"),
    ("qualidade e facilidade de uso da maquininha", "Qualidade e funcionamento da maquininha"),
    ("calidad y facilidad de uso del dispositivo", "Qualidade e funcionamento da maquininha"),
    ("calidad y dificultad de uso del dispositivo", "Qualidade e funcionamento da maquininha"),
    ("calidad del dispositivo", "Qualidade e funcionamento da maquininha"),
    ("calidad y funcionamiento", "Qualidade e funcionamento da maquininha"),
    # --- Funcionalidades de la cuenta MP ---
    ("funcionalidades da conta", "Funcionalidades da conta MP"),
    ("funcionalidades de la cuenta", "Funcionalidades da conta MP"),
    # --- Servicios adicionales ---
    ("servicios adicionales", "Servicios adicionales"),
    ("serviços adicionais", "Servicios adicionales"),
    # --- Medios de pago ---
    ("meios de pagamento", "Meios de pagamento disponíveis"),
    ("medios de pagos disponibles", "Meios de pagamento disponíveis"),
    ("medios de pago disponibles", "Meios de pagamento disponíveis"),
    # --- Aprobación ---
    ("aprovação de pagamentos", "Aprovação de pagamentos"),
    ("aprobación de pagos", "Aprovação de pagamentos"),
    # --- Integración → Otros ---
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
