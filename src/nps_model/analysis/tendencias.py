"""
Módulo de análisis de tendencias para NPS Sellers.

Analiza tendencias en motivos de quejas (no hay drivers operacionales reales en el MVP).
Detecta tendencias usando análisis de rachas consecutivas y clasificación por intensidad.
"""

import logging
from typing import Dict, List, Optional
from datetime import datetime

logger = logging.getLogger(__name__)


# Constantes para análisis de tendencias
TOLERANCIA_NEUTRO = 0.03  # ±0.03pp se considera neutro
MESES_MINIMOS_PARA_TOLERANCIA = 6
MAX_NEUTROS_TOLERADOS = 2
MINIMO_MESES_TENDENCIA = 3  # 3+ meses consecutivos para confirmar tendencia


def generar_lista_meses(mes_final: str, cantidad_meses: int) -> List[str]:
    """Genera una lista de meses hacia atrás desde mes_final."""
    meses = []
    year = int(mes_final[:4])
    month = int(mes_final[4:])
    
    for _ in range(cantidad_meses):
        meses.insert(0, f"{year}{month:02d}")
        month -= 1
        if month == 0:
            month = 12
            year -= 1
    
    return meses


def convertir_mes_a_texto(mes: str) -> str:
    """Convierte YYYYMM a formato legible (ej: "Dec 2025")."""
    try:
        fecha = datetime.strptime(str(mes), '%Y%m')
        return fecha.strftime('%b %Y')
    except:
        return str(mes)


def contar_consecutivos_desde_actual(
    variaciones: List[Dict],
    direccion_objetivo: str,
    permitir_neutros: bool = False
) -> int:
    """Cuenta meses consecutivos en una dirección DESDE EL FINAL hacia atrás."""
    if not variaciones:
        return 0
    
    consecutivos = 0
    meses_neutros_tolerados = 0
    max_neutros = MAX_NEUTROS_TOLERADOS if permitir_neutros else 0
    
    for i in range(len(variaciones) - 1, -1, -1):
        v = variaciones[i]['variacion']
        
        if direccion_objetivo == 'positivo':
            if v > TOLERANCIA_NEUTRO:
                consecutivos += 1
                meses_neutros_tolerados = 0
            elif abs(v) <= TOLERANCIA_NEUTRO and consecutivos > 0 and max_neutros > 0:
                meses_neutros_tolerados += 1
                if meses_neutros_tolerados <= max_neutros:
                    consecutivos += 1
                else:
                    break
            else:
                break
        else:
            if v < -TOLERANCIA_NEUTRO:
                consecutivos += 1
                meses_neutros_tolerados = 0
            elif abs(v) <= TOLERANCIA_NEUTRO and consecutivos > 0 and max_neutros > 0:
                meses_neutros_tolerados += 1
                if meses_neutros_tolerados <= max_neutros:
                    consecutivos += 1
                else:
                    break
            else:
                break
    
    return consecutivos


def analizar_tendencia_driver(
    valores: List[float],
    meses: List[str],
    nombre_driver: str,
    periodo: str = "12 meses"
) -> Optional[Dict]:
    """Analiza tendencia de un motivo/driver basándose en rachas consecutivas."""
    if len(valores) < 3:
        return None
    
    resultado = {
        'driver': nombre_driver,
        'periodo': periodo,
        'meses_analizados': len(valores),
        'valor_inicial': valores[0],
        'valor_final': valores[-1],
        'variacion_total_pp': valores[-1] - valores[0],
        'mes_inicio': meses[0],
        'mes_fin': meses[-1]
    }
    
    variaciones_mensuales = []
    for i in range(1, len(valores)):
        variacion = valores[i] - valores[i-1]
        variaciones_mensuales.append({
            'mes': meses[i],
            'variacion': variacion,
            'valor': valores[i]
        })
    
    resultado['variaciones_mensuales'] = variaciones_mensuales
    
    if variaciones_mensuales:
        max_subida = max(variaciones_mensuales, key=lambda x: x['variacion'])
        max_bajada = min(variaciones_mensuales, key=lambda x: x['variacion'])
        resultado['mes_mayor_subida'] = max_subida if max_subida['variacion'] > 0.05 else None
        resultado['mes_mayor_bajada'] = max_bajada if max_bajada['variacion'] < -0.05 else None
    
    # Contar consecutivos
    meses_subiendo = contar_consecutivos_desde_actual(variaciones_mensuales, 'positivo', permitir_neutros=False)
    meses_bajando = contar_consecutivos_desde_actual(variaciones_mensuales, 'negativo', permitir_neutros=False)
    
    if meses_subiendo >= MESES_MINIMOS_PARA_TOLERANCIA:
        meses_consecutivos_subiendo = contar_consecutivos_desde_actual(variaciones_mensuales, 'positivo', permitir_neutros=True)
    else:
        meses_consecutivos_subiendo = meses_subiendo
    
    if meses_bajando >= MESES_MINIMOS_PARA_TOLERANCIA:
        meses_consecutivos_bajando = contar_consecutivos_desde_actual(variaciones_mensuales, 'negativo', permitir_neutros=True)
    else:
        meses_consecutivos_bajando = meses_bajando
    
    resultado['meses_consecutivos_subiendo'] = meses_consecutivos_subiendo
    resultado['meses_consecutivos_bajando'] = meses_consecutivos_bajando
    
    hay_tendencia_creciente = meses_consecutivos_subiendo >= MINIMO_MESES_TENDENCIA
    hay_tendencia_decreciente = meses_consecutivos_bajando >= MINIMO_MESES_TENDENCIA
    
    if hay_tendencia_creciente and hay_tendencia_decreciente:
        if meses_consecutivos_subiendo >= meses_consecutivos_bajando:
            direccion = 'creciente'
            meses_consecutivos = meses_consecutivos_subiendo
        else:
            direccion = 'decreciente'
            meses_consecutivos = meses_consecutivos_bajando
        hay_tendencia = True
    elif hay_tendencia_creciente:
        direccion = 'creciente'
        meses_consecutivos = meses_consecutivos_subiendo
        hay_tendencia = True
    elif hay_tendencia_decreciente:
        direccion = 'decreciente'
        meses_consecutivos = meses_consecutivos_bajando
        hay_tendencia = True
    else:
        direccion = 'variable'
        meses_consecutivos = max(meses_consecutivos_subiendo, meses_consecutivos_bajando)
        hay_tendencia = False
    
    resultado['meses_consecutivos'] = meses_consecutivos
    resultado['hay_tendencia'] = hay_tendencia
    
    if hay_tendencia and meses_consecutivos > 0 and len(valores) > meses_consecutivos:
        idx_inicio_tendencia = len(valores) - meses_consecutivos - 1
        idx_inicio_tendencia = max(0, idx_inicio_tendencia)
        resultado['mes_inicio_tendencia'] = meses[idx_inicio_tendencia]
        resultado['variacion_tendencia_pp'] = valores[-1] - valores[idx_inicio_tendencia]
    else:
        resultado['mes_inicio_tendencia'] = resultado['mes_inicio']
        resultado['variacion_tendencia_pp'] = resultado['variacion_total_pp']
    
    # Intensidad
    variacion_abs = abs(resultado['variacion_tendencia_pp'])
    if variacion_abs < 0.1:
        intensidad = 'estable'
    elif variacion_abs < 0.5:
        intensidad = 'leve'
    elif variacion_abs < 1.0:
        intensidad = 'moderada'
    else:
        intensidad = 'fuerte'
    
    if not hay_tendencia:
        clasificacion_final = 'variable'
        intensidad_final = 'variable'
    elif variacion_abs < 0.1:
        clasificacion_final = 'estable'
        intensidad_final = 'estable'
    else:
        clasificacion_final = direccion
        intensidad_final = intensidad
    
    resultado['clasificacion'] = {
        'direccion': clasificacion_final,
        'intensidad': intensidad_final,
        'hay_tendencia': hay_tendencia,
        'meses_consecutivos': meses_consecutivos
    }
    
    if variaciones_mensuales:
        resultado['max_variacion_mensual'] = max(abs(v['variacion']) for v in variaciones_mensuales)
    
    return resultado


def generar_parrafo_tendencia(resultado: Dict, es_driver_negativo: bool = True) -> Optional[str]:
    """Genera un párrafo descriptivo de la tendencia detectada."""
    if resultado is None:
        return None
    
    driver = resultado['driver']
    direccion = resultado['clasificacion']['direccion']
    intensidad = resultado['clasificacion']['intensidad']
    hay_tendencia = resultado['clasificacion'].get('hay_tendencia', False)
    meses_consecutivos = resultado['clasificacion'].get('meses_consecutivos', 0)
    variacion_tendencia = resultado.get('variacion_tendencia_pp', resultado['variacion_total_pp'])
    variacion_total = resultado['variacion_total_pp']
    mes_inicio_tendencia = convertir_mes_a_texto(resultado.get('mes_inicio_tendencia', resultado['mes_inicio']))
    mes_inicio_periodo = convertir_mes_a_texto(resultado['mes_inicio'])
    
    signo = "+" if variacion_tendencia > 0 else ""
    signo_total = "+" if variacion_total > 0 else ""
    
    if direccion == 'variable':
        max_var = resultado.get('max_variacion_mensual', 0)
        return f"📊 <b>{driver}</b>: Se mantiene <b>variable</b> sin tendencia definida, con variaciones de hasta ±{max_var:.1f}pp entre meses. Variación total: {signo_total}{variacion_total:.1f}pp."
    
    if intensidad == 'estable':
        return f"📊 <b>{driver}</b>: Se mantiene <b>estable</b> ({signo_total}{variacion_total:.1f}pp desde {mes_inicio_periodo})."
    
    # Tendencia clara
    emoji = "📈" if direccion == 'creciente' else "📉"
    color = "red" if direccion == 'creciente' else "green"
    
    parrafo = f"{emoji} <b>{driver}</b>: <b style='color:{color};'>Tendencia {direccion} {intensidad}</b> sostenida durante <b>{meses_consecutivos} meses</b>"
    parrafo += f" ({signo}{variacion_tendencia:.1f}pp desde {mes_inicio_tendencia})"
    parrafo += "."
    
    return parrafo


def analizar_todas_tendencias(
    drivers_data: Dict,
    mes_actual: str
) -> Dict[str, Dict]:
    """
    Analiza tendencias para todos los motivos de quejas.
    
    En el modelo Sellers MVP, las tendencias se analizan sobre los motivos
    de quejas (no hay drivers operacionales reales).
    
    Args:
        drivers_data: Dict con datos del checkpoint1_consolidado.json (motivos como drivers)
        mes_actual: Mes actual en formato YYYYMM
        
    Returns:
        Dict con tendencias por motivo
    """
    logger.info(f"Analizando tendencias de motivos para mes {mes_actual}")
    
    resultados_tendencias = {}
    drivers_dict = drivers_data.get('drivers', {})
    
    for driver_key, driver_data in drivers_dict.items():
        if not driver_data:
            continue
        
        nombre = driver_data.get('driver_name', driver_key)
        shares = driver_data.get('quejas', driver_data.get('share', []))
        meses_driver = driver_data.get('meses', [])
        
        if len(shares) < 3 or len(meses_driver) < 3:
            logger.warning(f"Datos insuficientes para {nombre}")
            continue
        
        resultado_tendencia = analizar_tendencia_driver(
            valores=shares,
            meses=meses_driver,
            nombre_driver=nombre,
            periodo="12 meses"
        )
        
        if resultado_tendencia:
            parrafo = generar_parrafo_tendencia(resultado_tendencia, es_driver_negativo=True)
            
            resultados_tendencias[driver_key] = {
                'nombre': nombre,
                'categoria': 'motivo',
                'es_negativo': True,
                'key': driver_key,
                'analisis': resultado_tendencia,
                'parrafo': parrafo
            }
    
    logger.info(f"Análisis completado: {len(resultados_tendencias)} motivos analizados")
    return resultados_tendencias
