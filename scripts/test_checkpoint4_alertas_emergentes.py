"""
Checkpoint 4: Alertas Emergentes (Sellers)

Detecta senales tempranas de cambios significativos en motivos de quejas.
En Sellers no hay datos reales/operacionales, las alertas se basan
unicamente en variaciones de quejas desde checkpoint1.

Output: checkpoint4_alertas_emergentes_{SITE}_{MES}.json

Requiere:
- checkpoint1_consolidado_{SITE}_{MES}.json (Checkpoint 1)
"""

import json
import sys
from pathlib import Path
from datetime import datetime

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root / 'src'))

from nps_model.analysis.alertas_emergentes import analizar_alertas_emergentes
import yaml

print("="*80)
print("\U0001f6a8 CHECKPOINT 4: ALERTAS EMERGENTES (SELLERS)")
print("="*80)

# ==========================================
# PASO 1: Cargar configuracion
# ==========================================

print("\n\U0001f4dd Paso 1: Cargando configuracion...")

config_path = project_root / 'config' / 'config.yaml'
with open(config_path, 'r', encoding='utf-8') as f:
    config = yaml.safe_load(f)

sites = config.get('sites', ['MLA'])
site = sites[0] if sites else 'MLA'
quarter_actual = config.get('quarter_actual', '26Q1')
quarter_anterior = config.get('quarter_anterior', '25Q4')
from nps_model.utils.dates import quarter_fecha_final, quarter_label
mes_actual = quarter_fecha_final(quarter_actual)

update_tipo = config.get('update', {}).get('tipo', 'all')
current_fecha_corte = config.get('fecha_corte', None)

print(f"   Site: {site}")
print(f"   Comparación: {quarter_label(quarter_anterior)} vs {quarter_label(quarter_actual)}")

# ==========================================
# PASO 1B: Validate CP4 cache
# ==========================================
data_dir = project_root / 'data'
outputs_dir = project_root / 'outputs'

cp4_output = data_dir / f'checkpoint4_alertas_emergentes_{site}_{mes_actual}.json'
if cp4_output.exists():
    try:
        with open(cp4_output, 'r', encoding='utf-8') as f:
            cached = json.load(f)
        cached_ut = cached.get('metadata', {}).get('update_tipo', 'unknown')
        cached_fc = cached.get('metadata', {}).get('fecha_corte', None)
        if cached_ut == update_tipo and cached_fc == current_fecha_corte:
            print(f"\n✅ CHECKPOINT 4 YA EXISTE - USANDO CACHE (update: {update_tipo})")
            sys.exit(0)
        else:
            print(f"\n   ⚠️  Cache CP4 INVÁLIDO: update_tipo o fecha_corte cambió")
            cp4_output.unlink(missing_ok=True)
    except (json.JSONDecodeError, OSError):
        cp4_output.unlink(missing_ok=True)

# ==========================================
# PASO 2: Cargar checkpoint1 (unico checkpoint necesario)
# ==========================================

print("\n\U0001f4c2 Paso 2: Cargando checkpoint previo...")

def encontrar_archivo(nombre_archivo):
    """Busca archivo en data/ primero, luego en outputs/"""
    if (data_dir / nombre_archivo).exists():
        return data_dir / nombre_archivo
    elif (outputs_dir / nombre_archivo).exists():
        return outputs_dir / nombre_archivo
    else:
        return None

# Checkpoint 1: Drivers NPS (motivos)
checkpoint1_filename = f'checkpoint1_consolidado_{site}_{mes_actual}.json'
checkpoint1_path = encontrar_archivo(checkpoint1_filename)
if not checkpoint1_path:
    print(f"   \u274c Error: No se encontro {checkpoint1_filename}")
    print("   \U0001f4a1 Ejecuta primero: python scripts/test_checkpoint1_drivers_nps.py")
    sys.exit(1)

with open(checkpoint1_path, 'r', encoding='utf-8') as f:
    checkpoint_nps = json.load(f)
print(f"   \u2705 Checkpoint 1 cargado: {checkpoint1_filename}")

# ==========================================
# PASO 3: Ejecutar analisis de alertas
# ==========================================

print("\n\U0001f6a8 Paso 3: Analizando alertas emergentes...")

try:
    alertas_resultados = analizar_alertas_emergentes(
        checkpoint_nps=checkpoint_nps,
        mes_actual=mes_actual
    )
    
    metadata = alertas_resultados['metadata']
    alertas = alertas_resultados['alertas']
    
    print(f"   \u2705 Analisis completado")
    print(f"      - Motivos analizados: {metadata['total_motivos_analizados']}")
    print(f"      - Motivos con alertas: {metadata['total_motivos_con_alertas']}")
    print(f"      - Total de alertas: {metadata['total_alertas']}")
    
    if alertas:
        print(f"\n   Resumen de alertas detectadas:")
        for motivo, alertas_list in alertas.items():
            print(f"      - {motivo}: {len(alertas_list)} alerta(s)")
            for alerta in alertas_list:
                tipo_emoji = {
                    'alerta': '\u26a0\ufe0f',
                    'mejora': '\u2705',
                    'desalineacion': '\U0001f504'
                }.get(alerta['tipo'], '\u2753')
                print(f"        {tipo_emoji} {alerta['driver']}: {alerta['var_driver']:+.1f}pp")
    else:
        print(f"\n   No se detectaron alertas emergentes para este periodo")

except Exception as e:
    print(f"   \u274c Error en analisis: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

# ==========================================
# PASO 4: Guardar resultados en JSON
# ==========================================

print("\n\U0001f4be Paso 4: Guardando checkpoint...")

data_dir = project_root / 'data'
data_dir.mkdir(exist_ok=True)
output_path = data_dir / f'checkpoint4_alertas_emergentes_{site}_{mes_actual}.json'

try:
    # Inject update_tipo and fecha_corte into metadata for cache validation
    alertas_resultados.setdefault('metadata', {})['update_tipo'] = update_tipo
    alertas_resultados['metadata']['fecha_corte'] = current_fecha_corte
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(alertas_resultados, f, indent=2, ensure_ascii=False)
    print(f"   \u2705 Guardado: {output_path.name}")
except PermissionError:
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    output_path = data_dir / f'checkpoint4_alertas_emergentes_{site}_{mes_actual}_{timestamp}.json'
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(alertas_resultados, f, indent=2, ensure_ascii=False)
    print(f"   Archivo principal bloqueado, guardado como: {output_path.name}")

# ==========================================
# RESUMEN FINAL
# ==========================================

print("\n" + "="*80)
print("\u2705 CHECKPOINT 4 COMPLETADO")
print("="*80)
print(f"\nArchivo generado:")
print(f"   {output_path.name}")
print(f"\nResumen:")
print(f"   - Total alertas: {metadata['total_alertas']}")
print(f"   - Motivos con alertas: {metadata['total_motivos_con_alertas']}")
print(f"\nSiguiente paso:")
print(f"   python scripts/test_checkpoint5_analisis_cualitativo.py")
print("\n" + "="*80)
