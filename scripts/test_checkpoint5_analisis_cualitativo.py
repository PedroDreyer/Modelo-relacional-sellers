"""
Checkpoint 5: Analisis Cualitativo de Comentarios - NPS Sellers

Flujo completo:
1. Verifica si existe checkpoint5 (causas raiz) en cache
2. Si no existe: prepara datos, genera prompt para Claude, y pausa
3. Comments sobre variaciones (no LLM, auto-generated)
4. Retagueo de "Otros" (LLM, si share > umbral)
5. Hipotesis validation (optional, if configured)

Outputs:
- checkpoint5_causas_raiz_{SITE}_{MES}.json
- checkpoint5_comments_variaciones_{SITE}_{MES}.json
- checkpoint5_retagueo_{SITE}_{MES}.json (if activated)
- checkpoint5_hipotesis_{SITE}_{MES}.json (if configured)
"""

import json
import sys
from pathlib import Path
from datetime import datetime

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root / 'src'))

print("="*80)
print("\U0001f50d CHECKPOINT 5: ANALISIS CUALITATIVO SELLERS")
print("="*80)

# ==========================================
# PASO 1: Cargar configuracion
# ==========================================

print("\n\U0001f4dd Paso 1: Cargando configuracion...")

import yaml
config_path = project_root / 'config' / 'config.yaml'

with open(config_path, 'r', encoding='utf-8') as f:
    config = yaml.safe_load(f)

site = config['sites'][0]
quarter_actual = config.get('quarter_actual', '26Q1')
quarter_anterior = config.get('quarter_anterior', '25Q4')
from nps_model.utils.dates import quarter_fecha_final, quarter_label
fecha_final = quarter_fecha_final(quarter_actual)

print(f"   Site: {site}")
print(f"   Comparación: {quarter_label(quarter_anterior)} vs {quarter_label(quarter_actual)}")

# ==========================================
# PASO 2: Verificar checkpoint existente
# ==========================================

print("\n\U0001f50d Paso 2: Verificando checkpoint existente...")

output_dir = project_root / 'outputs'
data_dir = project_root / 'data'

checkpoint5_path_outputs = output_dir / f'checkpoint5_causas_raiz_{site}_{fecha_final}.json'
checkpoint5_path_data = data_dir / f'checkpoint5_causas_raiz_{site}_{fecha_final}.json'

if checkpoint5_path_outputs.exists():
    checkpoint5_path = checkpoint5_path_outputs
elif checkpoint5_path_data.exists():
    checkpoint5_path = checkpoint5_path_data
else:
    checkpoint5_path = None

import pandas as pd
from nps_model.analysis.comentarios import (
    preparar_comentarios_para_analisis,
    generar_prompt_para_claude,
    preparar_retagueo_otros,
    generar_prompt_retagueo,
    extraer_comentarios_por_variacion,
    preparar_validacion_hipotesis,
    generar_prompt_hipotesis,
)


# ==========================================
# Helper: load NPS data (prefer enriched)
# ==========================================
def _load_nps_data(data_dir, site, fecha_final):
    """Load NPS data, preferring enriched version."""
    enriquecido_path = data_dir / f'datos_nps_enriquecido_{site}_{fecha_final}.parquet'
    datos_nps_path = data_dir / f'datos_nps_{site}_{fecha_final}.parquet'

    path = enriquecido_path if enriquecido_path.exists() else datos_nps_path

    if not path.exists():
        print(f"   \u274c ERROR: No se encontro datos NPS")
        sys.exit(1)

    try:
        return pd.read_parquet(path, engine='pyarrow')
    except TypeError as e:
        if 'dbdate' in str(e):
            import pyarrow.parquet as pq
            table = pq.read_table(path)
            return table.to_pandas(ignore_metadata=True)
        raise


# ==========================================
# Check causas raiz cache
# ==========================================
update_tipo = config.get('update', {}).get('tipo', 'all')

if checkpoint5_path and checkpoint5_path.exists():
    # Validate update_tipo matches
    cache_valid = True
    try:
        with open(checkpoint5_path, 'r', encoding='utf-8') as f:
            cached_data = json.load(f)
        cached_update = cached_data.get('update_tipo', cached_data.get('metadata', {}).get('update_tipo', 'unknown'))
        if cached_update != update_tipo:
            print(f"   ⚠️  Cache INVÁLIDO: update_tipo cambió ({cached_update} → {update_tipo})")
            print(f"   🗑️  Eliminando cache obsoleto...")
            checkpoint5_path.unlink(missing_ok=True)
            checkpoint5_path = None
            cache_valid = False
    except (json.JSONDecodeError, OSError):
        cache_valid = False
        checkpoint5_path.unlink(missing_ok=True)
        checkpoint5_path = None

    if cache_valid:
        print(f"   ✅ Causas raiz ya existen para {site} - {fecha_final}")
        causas_raiz_exist = True
    else:
        causas_raiz_exist = False
        print("   No se encontro causas raiz, se generará prompt para Claude")
else:
    causas_raiz_exist = False
    print("   No se encontro causas raiz, se generará prompt para Claude")


# ==========================================
# STEP A: Generate comments on variations (NO LLM needed)
# ==========================================
print("\n\U0001f4ac Paso A: Generando comentarios sobre variaciones...")

comments_var_path = data_dir / f'checkpoint5_comments_variaciones_{site}_{fecha_final}.json'

if comments_var_path.exists():
    print(f"   \u2705 Ya existe: {comments_var_path.name}")
else:
    df_nps = _load_nps_data(data_dir, site, fecha_final)
    print(f"   \u2705 Datos NPS cargados: {len(df_nps):,} registros")

    # Load checkpoint1 to get driver variations
    checkpoint1_path = data_dir / f'checkpoint1_consolidado_{site}_{fecha_final}.json'
    variaciones_quejas = []

    if checkpoint1_path.exists():
        with open(checkpoint1_path, 'r', encoding='utf-8') as f:
            cp1_data = json.load(f)

        drivers = cp1_data.get('drivers', {})
        for driver_name, driver_data in drivers.items():
            var_mom = driver_data.get('var_quejas_mom', driver_data.get('var_share_mom', 0))
            if var_mom is not None:
                variaciones_quejas.append({
                    'motivo': driver_name,
                    'var_mom': var_mom,
                })

    if variaciones_quejas:
        resultado_comments = extraer_comentarios_por_variacion(
            df_nps=df_nps,
            variaciones_quejas=variaciones_quejas,
            mes_actual=fecha_final,
            max_comentarios_por_motivo=10,
            motivo_col='MOTIVO',
            comment_col='COMMENTS',
            umbral_variacion=0.5,
        )

        if resultado_comments:
            with open(comments_var_path, 'w', encoding='utf-8') as f:
                json.dump(resultado_comments, f, indent=2, ensure_ascii=False)
            print(f"   \u2705 Comments sobre variaciones guardados: {len(resultado_comments)} motivos")
        else:
            print("   \u2139\ufe0f  Sin variaciones significativas con comentarios")
    else:
        print("   \u2139\ufe0f  No hay datos de variaciones (checkpoint1 no encontrado)")


# ==========================================
# STEP B: Retagueo de "Otros" (LLM)
# ==========================================
print("\n\U0001f504 Paso B: Verificando retagueo de Otros...")

retagueo_path_data = data_dir / f'checkpoint5_retagueo_{site}_{fecha_final}.json'
retagueo_path_outputs = output_dir / f'checkpoint5_retagueo_{site}_{fecha_final}.json'

retagueo_exists = retagueo_path_data.exists() or retagueo_path_outputs.exists()

if retagueo_exists:
    print(f"   \u2705 Retagueo ya existe")
else:
    if 'df_nps' not in dir():
        df_nps = _load_nps_data(data_dir, site, fecha_final)

    datos_retagueo = preparar_retagueo_otros(
        df_nps=df_nps,
        mes_actual=fecha_final,
        max_comentarios=200,
        motivo_col='MOTIVO',
        comment_col='COMMENTS',
        umbral_share_otros=10.0,
    )

    if datos_retagueo['activar_retagueo']:
        # Save prompt for LLM to process
        prompt_retagueo = generar_prompt_retagueo(datos_retagueo, site, fecha_final)
        prompt_retagueo_path = data_dir / f'temp_prompt_retagueo_{site}_{fecha_final}.txt'
        with open(prompt_retagueo_path, 'w', encoding='utf-8') as f:
            f.write(prompt_retagueo)
        print(f"   \u2705 Prompt retagueo generado: {prompt_retagueo_path.name}")
        print(f"   \u2139\ufe0f  Share de 'Otros': {datos_retagueo['metadata']['share_otros_total']:.1f}%")
    else:
        print(f"   \u2139\ufe0f  Retagueo no activado: {datos_retagueo['metadata'].get('nota', 'share bajo')}")


# ==========================================
# STEP C: Hypothesis validation (optional)
# ==========================================
print("\n\U0001f9ea Paso C: Verificando hipótesis...")

hipotesis_texto = config.get('hipotesis', None)
hipotesis_path_data = data_dir / f'checkpoint5_hipotesis_{site}_{fecha_final}.json'
hipotesis_path_outputs = output_dir / f'checkpoint5_hipotesis_{site}_{fecha_final}.json'
hipotesis_exists = hipotesis_path_data.exists() or hipotesis_path_outputs.exists()

if hipotesis_texto and not hipotesis_exists:
    if 'df_nps' not in dir():
        df_nps = _load_nps_data(data_dir, site, fecha_final)

    datos_hipotesis = preparar_validacion_hipotesis(
        df_nps=df_nps,
        hipotesis=hipotesis_texto,
        mes_actual=fecha_final,
        max_comentarios=150,
        motivo_col='MOTIVO',
        comment_col='COMMENTS',
    )

    prompt_hipotesis = generar_prompt_hipotesis(datos_hipotesis, site)
    prompt_hipotesis_path = data_dir / f'temp_prompt_hipotesis_{site}_{fecha_final}.txt'
    with open(prompt_hipotesis_path, 'w', encoding='utf-8') as f:
        f.write(prompt_hipotesis)
    print(f"   \u2705 Prompt hipótesis generado: {prompt_hipotesis_path.name}")
elif hipotesis_exists:
    print(f"   \u2705 Hipótesis ya validada")
else:
    print(f"   \u2139\ufe0f  No hay hipótesis configurada")


# ==========================================
# STEP D: Causas Raiz (LLM - main checkpoint)
# ==========================================
if causas_raiz_exist:
    print("\n" + "="*80)
    print("\u2705 CHECKPOINT 5 COMPLETADO (causas raiz en cache, sub-checkpoints procesados)")
    print("="*80)
    sys.exit(0)

# If causas raiz don't exist, we need to generate the prompt and pause
print("\n\U0001f4e6 Paso D: Preparando causas raiz...")

if 'df_nps' not in dir():
    df_nps = _load_nps_data(data_dir, site, fecha_final)
    print(f"   \u2705 Datos NPS cargados: {len(df_nps):,} registros")

datos_preparados = preparar_comentarios_para_analisis(
    df_nps=df_nps,
    mes_actual=fecha_final,
    motivos_excluir=['Otros motivos', 'Sin informacion'],
    max_comentarios=100,
    motivo_col='MOTIVO'
)

metadata_prep = datos_preparados['metadata']
comentarios_por_motivo = datos_preparados['comentarios_por_motivo']

print(f"   \u2705 Comentarios preparados:")
print(f"      - Motivos a analizar: {metadata_prep['total_motivos_analizados']}")
for motivo, datos_motivo in comentarios_por_motivo.items():
    print(f"      - {motivo}: {datos_motivo['muestra_seleccionada']} comentarios")

prompt = generar_prompt_para_claude(datos_preparados, site=site)

data_dir.mkdir(exist_ok=True)

prompt_path = data_dir / f'temp_prompt_claude_{site}_{fecha_final}.txt'
with open(prompt_path, 'w', encoding='utf-8') as f:
    f.write(prompt)

print(f"   \u2705 Prompt generado: {prompt_path.name} (en data/)")

datos_prep_path = data_dir / f'temp_datos_preparados_{site}_{fecha_final}.json'
with open(datos_prep_path, 'w', encoding='utf-8') as f:
    json.dump(datos_preparados, f, indent=2, ensure_ascii=False)

print(f"   \u2705 Datos preparados guardados: {datos_prep_path.name} (en data/)")

print("\n" + "="*80)
print("\u23f8\ufe0f  ANALISIS CUALITATIVO - ACCION REQUERIDA")
print("="*80)
print(f"""

Claude Code leerá este archivo automáticamente y ejecutará el análisis:

   {prompt_path.name}

""")

# Exit with error to pause ejecutar_modelo_completo.py
sys.exit(1)
