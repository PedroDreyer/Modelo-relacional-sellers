"""
Análisis de Alertas Emergentes para NPS Sellers
================================================

En el modelo Sellers MVP (sin datos reales operacionales), las alertas
se basan en cambios significativos en motivos de quejas que podrían
indicar problemas emergentes.

Detecta:
1. Motivos con variación significativa MoM
2. Motivos que están por encima de su baseline histórico
"""

from typing import Dict, List, Optional, Tuple


# Configuración
UMBRAL_VARIACION_SIGNIFICATIVA = 0.9  # pp de variación MoM para alerta
UMBRAL_QUEJAS_ALTO = 5.0  # % de quejas para considerar alto


def _es_motivo_otros(motivo: str) -> bool:
    """True si el motivo es genérico (Otros / Sin información). Nunca mostrar alertas para estos."""
    if not motivo or not isinstance(motivo, str):
        return True
    m = motivo.strip().lower()
    otros = ("otros", "outros", "sin información", "sem informação", "outros / sem informação", "otro")
    return m in otros or "otros" in m or "outros" in m or "otro motivo" in m or "outro motivo" in m


def analizar_alertas_emergentes(
    checkpoint_nps: Dict,
    mes_actual: str,
) -> Dict:
    """
    Analiza alertas emergentes basadas en variaciones de motivos de quejas.
    
    En el MVP de Sellers no hay datos reales, así que las alertas se basan
    exclusivamente en los cambios en motivos de quejas.
    
    Args:
        checkpoint_nps: Dict con datos del checkpoint1 (motivos como drivers)
        mes_actual: Mes actual en formato YYYYMM
    
    Returns:
        Dict con alertas detectadas
    """
    alertas_resultados = {}
    
    drivers = checkpoint_nps.get("drivers", {})
    
    for motivo_key, motivo_data in drivers.items():
        if not motivo_data:
            continue

        nombre = motivo_data.get("driver_name", motivo_key)
        if _es_motivo_otros(nombre) or _es_motivo_otros(motivo_key):
            continue
        var_mom = motivo_data.get("var_quejas_mom", motivo_data.get("var_share_mom", 0))
        quejas_actual = motivo_data.get("quejas_actual", motivo_data.get("share_actual", 0))
        
        if var_mom is None:
            continue
        
        alertas_motivo = []
        
        # Alerta por variación significativa
        if abs(var_mom) >= UMBRAL_VARIACION_SIGNIFICATIVA:
            if var_mom > 0:
                tipo = "alerta"
                mensaje = (
                    f"<span style='color:#d32f2f;'><b>🚨 Alerta:</b> "
                    f"El motivo <b>{nombre}</b> aumentó significativamente "
                    f"(<b>+{var_mom:.1f}pp</b>, actual <b>{quejas_actual:.1f}%</b>). "
                    f"Requiere investigación.</span>"
                )
            else:
                tipo = "mejora"
                mensaje = (
                    f"<span style='color:#388e3c;'><b>✅ Mejora:</b> "
                    f"El motivo <b>{nombre}</b> mejoró significativamente "
                    f"(<b>{var_mom:.1f}pp</b>, actual <b>{quejas_actual:.1f}%</b>).</span>"
                )
            
            alertas_motivo.append({
                "driver": nombre,
                "var_driver": round(var_mom, 2),
                "actual_driver": round(quejas_actual, 2) if quejas_actual else 0,
                "var_quejas": round(var_mom, 2),
                "tipo": tipo,
                "mensaje": mensaje
            })
        
        if alertas_motivo:
            alertas_resultados[motivo_key] = alertas_motivo
    
    resultado = {
        "metadata": {
            "mes_actual": mes_actual,
            "umbral_driver": UMBRAL_VARIACION_SIGNIFICATIVA,
            "umbral_quejas": UMBRAL_VARIACION_SIGNIFICATIVA,
            "total_motivos_analizados": len(drivers),
            "total_motivos_con_alertas": len(alertas_resultados),
            "total_alertas": sum(len(v) for v in alertas_resultados.values()),
            "nota": "MVP sin datos reales - alertas basadas solo en quejas"
        },
        "alertas": alertas_resultados
    }
    
    return resultado


def debe_mostrar_alerta_emergente(var_quejas: float, **kwargs) -> Tuple[bool, Optional[str]]:
    """Determina si debe mostrarse una alerta emergente."""
    if abs(var_quejas) >= UMBRAL_VARIACION_SIGNIFICATIVA:
        tipo = "mejora" if var_quejas < 0 else "alerta"
        return True, tipo
    return False, None


def generar_mensaje_alerta(tipo_alerta: str, driver_name: str, var_driver: float, actual_driver: float, var_quejas: float) -> str:
    """Genera el mensaje HTML de la alerta emergente."""
    signo = "+" if var_driver > 0 else ""
    
    if tipo_alerta == "mejora":
        return (
            f"<span style='color:#388e3c;'><b>✅ Mejora Destacada:</b> "
            f"El motivo <b>{driver_name}</b> muestra una mejora significativa "
            f"(<b>{signo}{var_driver:.1f}pp</b>, actual <b>{actual_driver:.1f}%</b>).</span>"
        )
    else:
        return (
            f"<span style='color:#d32f2f;'><b>🚨 Alerta:</b> "
            f"El motivo <b>{driver_name}</b> muestra un aumento significativo "
            f"(<b>{signo}{var_driver:.1f}pp</b>, actual <b>{actual_driver:.1f}%</b>). "
            f"Requiere seguimiento.</span>"
        )
