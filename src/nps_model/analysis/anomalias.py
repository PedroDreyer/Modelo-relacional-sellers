"""
Módulo de análisis de anomalías en quejas usando baseline adaptativo.

Replica la lógica del notebook (líneas 8220-8650) para detectar patrones
atípicos en quejas por motivo usando baseline robusto (mediana + promedio de normales).
"""

import logging
from typing import Dict, List, Optional, Tuple
from datetime import datetime
import numpy as np

logger = logging.getLogger(__name__)


# ==========================================
# UMBRALES DE CONFIGURACIÓN
# ==========================================

UMBRAL_BASELINE_NORMALES = 0.7  # pp para considerar "mes normal" en cálculo de baseline
UMBRAL_PICO_AISLADO = 1.5  # pp sobre baseline para detectar pico aislado
UMBRAL_DETERIORO_SOSTENIDO = 0.7  # pp sobre baseline durante 3+ meses
UMBRAL_ELEVADO = 0.5  # pp sobre baseline (normalizando pero aún alto)
UMBRAL_MEJORA = -0.7  # pp bajo baseline (mejora destacada)


def convertir_mes_a_texto(mes: str) -> str:
    """
    Convierte YYYYMM a formato legible (ej: "Dec 2025").
    
    Args:
        mes: Mes en formato YYYYMM
        
    Returns:
        Mes en formato "MMM YYYY"
    """
    try:
        fecha = datetime.strptime(str(mes), '%Y%m')
        return fecha.strftime('%b %Y')
    except:
        return str(mes)


def calcular_baseline_adaptativo(serie_valores: List[float]) -> Tuple[Optional[float], Optional[float], int]:
    """
    Calcula baseline robusto en dos pasos:
    1. Mediana como referencia inicial (resistente a outliers)
    2. Promedio de meses "normales" (dentro de ±0.7pp de la mediana)
    
    Args:
        serie_valores: lista de valores históricos (últimos 12 meses)
        
    Returns:
        (baseline, mediana_ref, meses_normales_count)
    """
    if len(serie_valores) < 6:
        return None, None, 0
    
    # Paso 1: Calcular mediana como ancla robusta
    mediana_ref = np.median(serie_valores)
    
    # Paso 2: Identificar meses "normales" (dentro de ±0.7pp de la mediana)
    meses_normales = [v for v in serie_valores if abs(v - mediana_ref) <= UMBRAL_BASELINE_NORMALES]
    
    # Paso 3: Baseline = promedio de los meses normales
    if len(meses_normales) >= 3:  # Mínimo 3 meses para calcular baseline
        baseline = np.mean(meses_normales)
    else:
        # Fallback: si hay muy pocos normales, usar la mediana
        baseline = mediana_ref
    
    return baseline, mediana_ref, len(meses_normales)


def clasificar_patron_anomalia(serie_valores: List[float], baseline: float) -> Dict:
    """
    Clasifica el patrón de anomalía en la serie temporal.
    
    Tipos detectados:
    - 'normal': Dentro del baseline (±0.5pp)
    - 'pico_aislado': 1-2 meses >1.5pp sobre baseline
    - 'deterioro_sostenido': 3+ meses >0.7pp sobre baseline
    - 'elevado_normalizando': Bajó desde un pico pero aún >0.5pp sobre baseline
    - 'normalizado': Volvió al baseline después de un pico
    - 'mejora_destacada': <-0.7pp bajo baseline (después de un pico)
    
    Args:
        serie_valores: Lista de valores (del más antiguo al más reciente)
        baseline: Baseline calculado
        
    Returns:
        Dict con clasificación del patrón
    """
    resultado = {
        'tipo_anomalia': 'normal',
        'meses_anomalos': [],
        'valor_pico': None,
        'mes_pico_idx': None,
        'diferencia_pico': 0,
        'meses_consecutivos_elevados': 0,
        'normalizando': False,
        'mejora': False
    }
    
    if baseline is None or len(serie_valores) < 4:
        return resultado
    
    ultimos_6_meses = serie_valores[-6:]
    valor_actual = serie_valores[-1]
    diferencia_actual = valor_actual - baseline
    
    # 1. Identificar meses con valores elevados (>0.7pp sobre baseline)
    meses_elevados_info = []
    for i in range(max(0, len(serie_valores) - 6), len(serie_valores)):
        valor = serie_valores[i]
        dif = valor - baseline
        if dif > UMBRAL_DETERIORO_SOSTENIDO:
            meses_elevados_info.append({
                'idx': i,
                'valor': valor,
                'diferencia': dif
            })
    
    # 2. Buscar pico máximo
    if meses_elevados_info:
        pico_info = max(meses_elevados_info, key=lambda x: x['diferencia'])
        resultado['valor_pico'] = pico_info['valor']
        resultado['mes_pico_idx'] = pico_info['idx']
        resultado['diferencia_pico'] = pico_info['diferencia']
        resultado['meses_anomalos'] = [m['idx'] for m in meses_elevados_info]
    
    # 3. Contar meses consecutivos elevados (terminando en el mes actual o reciente)
    consecutivos = 0
    for i in range(len(serie_valores) - 1, -1, -1):
        if serie_valores[i] - baseline > UMBRAL_DETERIORO_SOSTENIDO:
            consecutivos += 1
        else:
            break
    resultado['meses_consecutivos_elevados'] = consecutivos
    
    # 4. Clasificar patrón
    if diferencia_actual <= UMBRAL_ELEVADO and diferencia_actual >= -UMBRAL_ELEVADO:
        # Dentro de rango normal
        if meses_elevados_info and pico_info['idx'] > len(serie_valores) - 4:
            # Hubo pico reciente pero ya normalizó
            resultado['tipo_anomalia'] = 'normalizado'
            resultado['normalizando'] = True
        else:
            resultado['tipo_anomalia'] = 'normal'
    
    elif diferencia_actual < UMBRAL_MEJORA:
        # Valor actual muy por debajo del baseline
        if meses_elevados_info:
            # Mejora después de un pico
            resultado['tipo_anomalia'] = 'mejora_destacada'
            resultado['mejora'] = True
        else:
            resultado['tipo_anomalia'] = 'normal'  # Bajo pero sin pico previo
    
    elif diferencia_actual > UMBRAL_PICO_AISLADO:
        # Valor actual muy elevado
        if len(meses_elevados_info) <= 2:
            resultado['tipo_anomalia'] = 'pico_aislado'
        else:
            resultado['tipo_anomalia'] = 'deterioro_sostenido'
    
    elif diferencia_actual > UMBRAL_ELEVADO:
        # Valor actual elevado pero no extremo
        if consecutivos >= 3:
            resultado['tipo_anomalia'] = 'deterioro_sostenido'
        elif meses_elevados_info and pico_info['diferencia'] > UMBRAL_PICO_AISLADO:
            # Bajó desde un pico pero aún elevado
            resultado['tipo_anomalia'] = 'elevado_normalizando'
            resultado['normalizando'] = True
        else:
            resultado['tipo_anomalia'] = 'normal'
    
    return resultado


def generar_mensaje_anomalia(resultado: Dict) -> Tuple[Optional[str], Optional[str]]:
    """
    Genera mensaje descriptivo de la anomalía detectada.
    
    Formatos según tipo (del notebook):
    - Pico aislado: "🔴 Motivo: Se detectó un pico aislado en MES de X.X% 
                     (+X.Xpp sobre baseline de X.X%)"
    - Deterioro sostenido: "🟠 Motivo: Se detectó deterioro sostenido durante N meses. 
                            En MES está en X.X% (+X.Xpp sobre baseline). Pico máximo: X.X%"
    - Elevado normalizando: "🟡 Motivo: Normalizando después de un pico. 
                             En MES bajó a X.X% (-X.Xpp desde el pico) pero aún elevado"
    - Normalizado: "🟢 Motivo: Normalizado en MES. Volvió al baseline en X.X% 
                    (dentro de ±0.5pp) después de un pico previo"
    - Mejora destacada: "🔵 Motivo: Mejora destacada en MES. 
                         Ahora está en X.X% (X.Xpp bajo baseline), mostrando mejora sostenida"
    
    Args:
        resultado: Dict con análisis de anomalía
        
    Returns:
        (mensaje_html, clase_css) o (None, None) si no hay anomalía
    """
    motivo = resultado['motivo']
    tipo = resultado['tipo_anomalia']
    valor_actual = resultado['valor_actual']
    baseline = resultado['baseline']
    diferencia = resultado['diferencia_vs_baseline']
    mes_actual = resultado['mes_actual']
    patron = resultado['detalles_patron']
    
    # Formatear mes
    mes_texto = convertir_mes_a_texto(mes_actual)
    
    signo = "+" if diferencia > 0 else ""
    
    if tipo == 'pico_aislado':
        mensaje = f"🔴 <b>{motivo}*</b>: Se detectó un <b>pico aislado</b> en <b>{mes_texto}</b> "
        mensaje += f"de <b>{valor_actual:.1f}%</b> ({signo}{diferencia:.1f}pp sobre baseline de {baseline:.1f}%)."
        return mensaje, 'anomalia-pico'
    
    elif tipo == 'deterioro_sostenido':
        consecutivos = patron['meses_consecutivos_elevados']
        mensaje = f"🟠 <b>{motivo}*</b>: Se detectó <b>deterioro sostenido</b> durante {consecutivos} meses. "
        mensaje += f"En <b>{mes_texto}</b> está en <b>{valor_actual:.1f}%</b> ({signo}{diferencia:.1f}pp sobre baseline de {baseline:.1f}%)."
        if patron['valor_pico']:
            pico_val = patron['valor_pico']
            pico_dif = patron['diferencia_pico']
            mensaje += f" Pico máximo: {pico_val:.1f}% (+{pico_dif:.1f}pp)."
        return mensaje, 'anomalia-deterioro'
    
    elif tipo == 'elevado_normalizando':
        pico_val = patron['valor_pico']
        bajada = pico_val - valor_actual
        mensaje = f"🟡 <b>{motivo}*</b>: <b>Normalizando</b> después de un pico. "
        mensaje += f"En <b>{mes_texto}</b> bajó a <b>{valor_actual:.1f}%</b> (-{bajada:.1f}pp desde el pico de {pico_val:.1f}%) "
        mensaje += f"pero aún permanece elevado ({signo}{diferencia:.1f}pp sobre baseline de {baseline:.1f}%)."
        return mensaje, 'anomalia-normalizando'
    
    elif tipo == 'normalizado':
        mensaje = f"🟢 <b>{motivo}*</b>: <b>Normalizado</b> en <b>{mes_texto}</b>. "
        mensaje += f"Volvió al baseline en <b>{valor_actual:.1f}%</b> (dentro de ±{UMBRAL_ELEVADO}pp del baseline de {baseline:.1f}%) "
        mensaje += f"después de un pico previo."
        return mensaje, 'anomalia-normalizado'
    
    elif tipo == 'mejora_destacada':
        pico_val = patron.get('valor_pico')
        mensaje = f"🔵 <b>{motivo}*</b>: <b>Mejora destacada</b> en <b>{mes_texto}</b>. "
        if pico_val:
            mensaje += f"Se detectó un pico previo de {pico_val:.1f}% que se normalizó completamente. "
        mensaje += f"Ahora está en <b>{valor_actual:.1f}%</b> ({diferencia:.1f}pp bajo baseline de {baseline:.1f}%), "
        mensaje += f"mostrando mejora sostenida."
        return mensaje, 'anomalia-mejora'
    
    else:  # normal
        return None, None


def analizar_anomalia_motivo(
    serie_valores: List[float],
    serie_meses: List[str],
    nombre_motivo: str
) -> Dict:
    """
    Analiza la serie temporal de quejas de un motivo usando baseline adaptativo.
    
    Args:
        serie_valores: lista de valores (del más antiguo al más reciente)
        serie_meses: lista de meses correspondientes
        nombre_motivo: nombre del motivo de queja
        
    Returns:
        Dict con resultados del análisis
    """
    resultado = {
        'motivo': nombre_motivo,
        'valor_actual': None,
        'mes_actual': None,
        'baseline': None,
        'mediana_ref': None,
        'meses_normales_count': 0,
        'tipo_anomalia': 'normal',
        'diferencia_vs_baseline': 0,
        'patron_detectado': False,
        'detalles_patron': {}
    }
    
    if len(serie_valores) < 6:
        return resultado
    
    # Valor actual
    valor_actual = serie_valores[-1]
    mes_actual = serie_meses[-1]
    resultado['valor_actual'] = valor_actual
    resultado['mes_actual'] = mes_actual
    
    # Calcular baseline adaptativo
    baseline, mediana_ref, meses_normales = calcular_baseline_adaptativo(serie_valores)
    resultado['baseline'] = baseline
    resultado['mediana_ref'] = mediana_ref
    resultado['meses_normales_count'] = meses_normales
    
    if baseline is None:
        return resultado
    
    resultado['diferencia_vs_baseline'] = valor_actual - baseline
    
    # Clasificar patrón de anomalía
    patron = clasificar_patron_anomalia(serie_valores, baseline)
    resultado['tipo_anomalia'] = patron['tipo_anomalia']
    resultado['detalles_patron'] = patron
    
    # Marcar si hay patrón detectado (no es 'normal')
    if patron['tipo_anomalia'] != 'normal':
        resultado['patron_detectado'] = True
    
    return resultado


def analizar_anomalias_quejas(
    impacto_df,
    mes_actual: str,
    motivos_analizar: Optional[List[str]] = None
) -> Dict[str, Dict]:
    """
    Analiza anomalías para todos los motivos de quejas usando baseline adaptativo.
    
    Excluye automáticamente:
    - "Sin información", "Otros motivos", "Otros sin clasificar"
    
    Analiza: últimos 12 meses
    
    Args:
        impacto_df: DataFrame con impacto de quejas por mes y motivo
        mes_actual: Mes actual en formato YYYYMM
        motivos_analizar: Lista opcional de motivos a analizar (si None, analiza todos menos excluidos)
        
    Returns:
        Dict con anomalías por motivo
    """
    logger.info(f"Analizando anomalías en quejas para mes {mes_actual}")
    
    # Motivos a excluir
    motivos_excluidos = ['Sin información', 'Otros motivos', 'Otros sin clasificar']
    
    # Si no se especifican motivos, usar todos menos los excluidos
    if motivos_analizar is None:
        motivos_analizar = [col for col in impacto_df.columns if col not in motivos_excluidos]
    else:
        motivos_analizar = [m for m in motivos_analizar if m not in motivos_excluidos]
    
    resultados_anomalias = {}
    
    for motivo in motivos_analizar:
        # Verificar que el motivo existe en el DataFrame
        if motivo not in impacto_df.columns:
            logger.warning(f"Motivo {motivo} no encontrado en impacto_df")
            continue
        
        # Obtener serie histórica de quejas para este motivo (últimos 12 meses)
        serie_meses = list(impacto_df.index[-12:])
        serie_valores = list(impacto_df[motivo].iloc[-12:].values)
        
        # Analizar anomalías con baseline adaptativo
        resultado_anomalia = analizar_anomalia_motivo(serie_valores, serie_meses, motivo)
        
        # Generar mensaje si hay anomalía
        if resultado_anomalia['patron_detectado']:
            mensaje, clase_css = generar_mensaje_anomalia(resultado_anomalia)
            resultado_anomalia['mensaje'] = mensaje
            resultado_anomalia['clase_css'] = clase_css
        
        # Guardar resultado
        resultados_anomalias[motivo] = resultado_anomalia
        
        logger.debug(f"Anomalía analizada para {motivo}: {resultado_anomalia['tipo_anomalia']}")
    
    anomalias_detectadas = sum(1 for r in resultados_anomalias.values() if r['patron_detectado'])
    logger.info(f"Análisis completado: {len(resultados_anomalias)} motivos analizados, {anomalias_detectadas} anomalías detectadas")
    
    return resultados_anomalias
