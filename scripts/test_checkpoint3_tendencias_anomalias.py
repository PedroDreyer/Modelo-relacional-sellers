"""
Checkpoint 3: Analisis de Tendencias y Anomalias - NPS Sellers

Calcula:
1. Tendencias en motivos de quejas (desde checkpoint1, ultimos 12 meses)
2. Anomalias en quejas por motivo (baseline adaptativo, ultimos 12 meses)

No requiere checkpoint_reales (no hay datos operacionales en Sellers).
Usa checkpoint1 (drivers NPS / motivos) como fuente de tendencias.

Output: checkpoint3_tendencias_anomalias_{SITE}_{MES}.json
"""

import json
import logging
import sys
from pathlib import Path

logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger(__name__)

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root / 'src'))

# Imports del modelo
from nps_model.analysis.tendencias import analizar_todas_tendencias
from nps_model.analysis.anomalias import analizar_anomalias_quejas
from nps_model.analysis.quejas import calcular_impacto_quejas_mensual

print("="*80)
print("\U0001f4ca CHECKPOINT 3: TENDENCIAS Y ANOMALIAS (SELLERS)")
print("="*80)

# ==========================================
# PASO 1: Cargar configuracion
# ==========================================

print("\n\U0001f4dd Paso 1: Cargando configuracion...")

import yaml
config_path = project_root / 'config' / 'config.yaml'

with open(config_path, 'r', encoding='utf-8') as f:
    config_data = yaml.safe_load(f)

sites_config = config_data.get('sites', ['MLA'])
site = sites_config[0] if sites_config else 'MLA'
quarter_actual = config_data.get('quarter_actual', '26Q1')
quarter_anterior = config_data.get('quarter_anterior', '25Q4')
from nps_model.utils.dates import quarter_fecha_final, quarter_label
mes_actual = quarter_fecha_final(quarter_actual)

print(f"   Site: {site}")
print(f"   Comparación: {quarter_label(quarter_anterior)} vs {quarter_label(quarter_actual)}")

# Early exit: si el output ya existe, no re-correr
update_tipo = config_data.get('update', {}).get('tipo', 'all')
output_path_check = project_root / 'data' / f'checkpoint3_tendencias_anomalias_{site}_{mes_actual}.json'
if output_path_check.exists():
    # Validate update_tipo matches
    cache_valid = True
    try:
        with open(output_path_check, 'r', encoding='utf-8') as f:
            cached_data = json.load(f)
        cached_update = cached_data.get('update_tipo', 'unknown')
        if cached_update != update_tipo:
            print(f"\n   ⚠️  Cache INVÁLIDO: update_tipo cambió ({cached_update} → {update_tipo})")
            print(f"   🗑️  Eliminando cache obsoleto...")
            output_path_check.unlink(missing_ok=True)
            cache_valid = False
    except (json.JSONDecodeError, OSError):
        cache_valid = False
        output_path_check.unlink(missing_ok=True)
    if cache_valid:
        print(f"\n✅ CHECKPOINT 3 YA EXISTE - USANDO CACHE")
        print(f"   {output_path_check.name}")
        print(f"   💡 Para regenerar, elimina el archivo y vuelve a ejecutar")
        sys.exit(0)

# ==========================================
# PASO 2: Cargar checkpoint1 (motivos NPS)
# ==========================================

print("\n\U0001f4ca Paso 2: Cargando checkpoint1...")

data_dir = project_root / 'data'
outputs_dir = project_root / 'outputs'

def encontrar_archivo(nombre_archivo):
    """Busca archivo en data/ primero, luego en outputs/"""
    if (data_dir / nombre_archivo).exists():
        return data_dir / nombre_archivo
    elif (outputs_dir / nombre_archivo).exists():
        return outputs_dir / nombre_archivo
    else:
        return None

checkpoint1_filename = f'checkpoint1_consolidado_{site}_{mes_actual}.json'
checkpoint1_path = encontrar_archivo(checkpoint1_filename)

if not checkpoint1_path:
    print(f"\u274c ERROR: No se encontro {checkpoint1_filename}")
    print("\U0001f4a1 Ejecuta primero: python scripts/test_checkpoint1_drivers_nps.py")
    sys.exit(1)

with open(checkpoint1_path, 'r', encoding='utf-8') as f:
    checkpoint1_data = json.load(f)

print(f"   \u2705 {checkpoint1_filename} cargado")

# ==========================================
# PASO 3: Analizar tendencias (motivos desde checkpoint1)
# ==========================================

print("\n\U0001f4c8 Paso 3: Analizando tendencias de motivos...")

tendencias_resultados = analizar_todas_tendencias(
    drivers_data=checkpoint1_data,
    mes_actual=mes_actual
)

print(f"   \u2705 Tendencias calculadas para {len(tendencias_resultados)} motivos")

# ==========================================
# PASO 4: Cargar datos de encuestas desde checkpoint0
# ==========================================

print("\n\U0001f4e6 Paso 4: Cargando datos de encuestas desde checkpoint0...")

import pandas as pd

datos_nps_path = project_root / 'data' / f'datos_nps_{site}_{mes_actual}.parquet'

if not datos_nps_path.exists():
    print(f"   \u274c ERROR: No se encontro {datos_nps_path.name}")
    print(f"   \U0001f4a1 Ejecuta primero: python scripts/test_checkpoint0_cargar_datos.py")
    sys.exit(1)

try:
    df_encuestas = pd.read_parquet(datos_nps_path)
    print(f"   \u2705 Datos cargados desde checkpoint0: {len(df_encuestas):,} registros")
except Exception as e:
    print(f"   \u274c Error cargando datos desde checkpoint0: {e}")
    sys.exit(1)

# ==========================================
# PASO 5: Calcular impacto de quejas mensual
# ==========================================

print("\n\U0001f4ac Paso 5: Calculando impacto de quejas...")

from nps_model.analysis.tendencias import generar_lista_meses
meses_para_analisis = generar_lista_meses(mes_actual, 12)

impacto_df = calcular_impacto_quejas_mensual(
    df=df_encuestas,
    meses=meses_para_analisis,
    motivo_col='MOTIVO'
)

print(f"   \u2705 Impacto calculado: {len(impacto_df)} meses x {len(impacto_df.columns)} motivos")

# ==========================================
# PASO 6: Analizar anomalias (quejas)
# ==========================================

print("\n\U0001f3af Paso 6: Analizando anomalias en quejas...")

anomalias_resultados = analizar_anomalias_quejas(
    impacto_df=impacto_df,
    mes_actual=mes_actual,
    motivos_analizar=None
)

anomalias_detectadas = sum(1 for r in anomalias_resultados.values() if r['patron_detectado'])
print(f"   \u2705 Anomalias calculadas para {len(anomalias_resultados)} motivos")
print(f"   \U0001f4ca Anomalias detectadas: {anomalias_detectadas}")

# ==========================================
# PASO 7: Guardar checkpoint consolidado
# ==========================================

print("\n\U0001f4be Paso 7: Guardando checkpoint...")

output_data = {
    "site": site,
    "mes_actual": mes_actual,
    "update_tipo": update_tipo,
    "meses_analizados": meses_para_analisis,
    "tendencias": tendencias_resultados,
    "anomalias": anomalias_resultados
}

output_path = project_root / 'data' / f'checkpoint3_tendencias_anomalias_{site}_{mes_actual}.json'
output_path.parent.mkdir(parents=True, exist_ok=True)

try:
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(output_data, f, indent=2, ensure_ascii=False)
    print(f"   \u2705 Guardado: {output_path}")
except PermissionError as e:
    print(f"   Error de permisos al guardar: {e}")
    from datetime import datetime
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    output_path_alt = project_root / 'outputs' / f'checkpoint3_tendencias_anomalias_{site}_{mes_actual}_{timestamp}.json'
    try:
        with open(output_path_alt, 'w', encoding='utf-8') as f:
            json.dump(output_data, f, indent=2, ensure_ascii=False)
        print(f"   \u2705 Guardado en alternativo: {output_path_alt}")
        output_path = output_path_alt
    except Exception as e2:
        print(f"   \u274c No se pudo guardar: {e2}")
        sys.exit(1)

print("\n" + "="*80)
print("\u2705 CHECKPOINT 3 COMPLETADO")
print("="*80)
print(f"\nArchivos generados:")
print(f"   - {output_path}")
