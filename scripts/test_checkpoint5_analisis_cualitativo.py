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
from nps_model.utils.dates import quarter_fecha_final, quarter_label, quarter_to_months
fecha_final = quarter_fecha_final(quarter_actual)
meses_q_actual = quarter_to_months(quarter_actual)
meses_q_anterior = quarter_to_months(quarter_anterior)

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
    preparar_comentarios_promotores,
    generar_prompt_promotores,
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

current_fecha_corte = config.get('fecha_corte', None)

if checkpoint5_path and checkpoint5_path.exists():
    # Validate update_tipo and fecha_corte match (same as CP1)
    cache_valid = True
    try:
        with open(checkpoint5_path, 'r', encoding='utf-8') as f:
            cached_data = json.load(f)
        cached_update = cached_data.get('update_tipo', cached_data.get('metadata', {}).get('update_tipo', 'unknown'))
        cached_fecha_corte = cached_data.get('metadata', {}).get('fecha_corte', None)
        if cached_update != update_tipo:
            print(f"   ⚠️  Cache INVÁLIDO: update_tipo cambió ({cached_update} → {update_tipo})")
            print(f"   🗑️  Eliminando cache obsoleto...")
            checkpoint5_path.unlink(missing_ok=True)
            cache_valid = False
        elif cached_fecha_corte != current_fecha_corte:
            print(f"   ⚠️  Cache INVÁLIDO: fecha_corte cambió ({cached_fecha_corte} → {current_fecha_corte})")
            print(f"   🗑️  Eliminando cache obsoleto...")
            checkpoint5_path.unlink(missing_ok=True)
            cache_valid = False
    except (json.JSONDecodeError, OSError) as e:
        print(f"   ⚠️  Error leyendo CP5 cache: {e}")
        cache_valid = False

    if cache_valid:
        print(f"   ✅ Causas raiz ya existen para {site} - {fecha_final} (update: {update_tipo})")
        causas_raiz_exist = True
        # Enrich ejemplos with dims from enriched parquet (if not already done)
        try:
            import pandas as pd
            enriq_path = data_dir / f'datos_nps_enriquecido_{site}_{fecha_final}.parquet'
            if enriq_path.exists():
                _DIM_COLS_ENRICH = [
                    "PRODUCTO_PRINCIPAL", "SEGMENTO", "NEWBIE_LEGACY",
                    "FLAG_USA_CREDITO", "FLAG_TARJETA_CREDITO",
                    "FLAG_USA_INVERSIONES", "FLAG_TOPOFF", "FLAG_PRICING",
                    "MODELO_DEVICE", "OFERTA_TC",
                ]
                df_enriq = pd.read_parquet(enriq_path, engine='pyarrow')
                dims_lookup = {}
                id_col = 'CUST_ID' if 'CUST_ID' in df_enriq.columns else 'NPS_TX_CUS_CUST_ID'
                for _, row in df_enriq.drop_duplicates(subset=[id_col]).iterrows():
                    cid = str(row[id_col]).split('.')[0]  # strip decimal part
                    d = {}
                    for col in _DIM_COLS_ENRICH:
                        v = row.get(col)
                        if pd.notna(v) and str(v) not in ("", "Sin dato", "nan"):
                            d[col] = str(v)
                    if d:
                        dims_lookup[cid] = d

                # Inject dims into CP5 JSON ejemplos
                changed = False
                with open(checkpoint5_path, 'r', encoding='utf-8') as f:
                    cp5_data = json.load(f)
                for motivo, datos in (cp5_data.get('causas_por_motivo') or {}).items():
                    for causa_key, causa in (datos.get('causas_raiz') or {}).items():
                        new_ejs = []
                        for ej in (causa.get('ejemplos') or []):
                            if isinstance(ej, dict) and not ej.get('dims'):
                                cid = str(ej.get('cust_id', '')).split('.')[0]
                                if cid in dims_lookup:
                                    ej = {**ej, 'dims': dims_lookup[cid]}
                                    changed = True
                            new_ejs.append(ej)
                        causa['ejemplos'] = new_ejs
                if changed:
                    with open(checkpoint5_path, 'w', encoding='utf-8') as f:
                        json.dump(cp5_data, f, ensure_ascii=False, indent=2)
        except Exception as _e:
            pass  # dims enrichment is best-effort
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
    mes_actual=meses_q_actual,  # Full quarter, not just last month
    motivos_excluir=['Otros motivos', 'Otros', 'Sin informacion', 'Sin información', 'Outro - Por favor, especifique'],
    max_comentarios=300,
    motivo_col='MOTIVO'
)

metadata_prep = datos_preparados['metadata']
comentarios_por_motivo = datos_preparados['comentarios_por_motivo']

print(f"   \u2705 Comentarios preparados:")
print(f"      - Motivos a analizar: {metadata_prep['total_motivos_analizados']}")
for motivo, datos_motivo in comentarios_por_motivo.items():
    print(f"      - {motivo}: {datos_motivo['muestra_seleccionada']} comentarios")

prompt = generar_prompt_para_claude(datos_preparados, site=site)

# Append metadata so Claude Code includes it in the generated JSON
prompt += f"\n\n## ⚙️ METADATA PARA INCLUIR EN EL JSON\n"
prompt += f"- update_tipo: \"{update_tipo}\"\n"
prompt += f"- fecha_corte: \"{current_fecha_corte}\"\n"
prompt += f"\nIncluir estos valores en metadata del JSON generado.\n"

data_dir.mkdir(exist_ok=True)

prompt_path = data_dir / f'temp_prompt_claude_{site}_{fecha_final}.txt'
with open(prompt_path, 'w', encoding='utf-8') as f:
    f.write(prompt)

print(f"   \u2705 Prompt Q actual generado: {prompt_path.name} (en data/)")

# Generate Q anterior prompt too (for comparison)
fecha_final_ant = quarter_fecha_final(quarter_anterior)
cp5_ant_path = data_dir / f'checkpoint5_causas_raiz_{site}_{fecha_final}_q_anterior.json'
if not cp5_ant_path.exists():
    datos_prep_ant = preparar_comentarios_para_analisis(
        df_nps=df_nps,
        mes_actual=meses_q_anterior,
        motivos_excluir=['Otros motivos', 'Otros', 'Sin informacion', 'Sin información', 'Outro - Por favor, especifique'],
        max_comentarios=300,
        motivo_col='MOTIVO'
    )
    if datos_prep_ant['metadata']['total_motivos_analizados'] > 0:
        prompt_ant = generar_prompt_para_claude(datos_prep_ant, site=site)
        prompt_ant += f"\n\n## ⚙️ METADATA PARA INCLUIR EN EL JSON\n"
        prompt_ant += f"- update_tipo: \"{update_tipo}\"\n"
        prompt_ant += f"- fecha_corte: \"{current_fecha_corte}\"\n"
        prompt_ant += f"- quarter: \"{quarter_anterior}\"\n"
        prompt_ant += f"\nIncluir estos valores en metadata del JSON generado.\n"
        prompt_ant += f"\n**IMPORTANTE:** Guardar en: data/checkpoint5_causas_raiz_{site}_{fecha_final}_q_anterior.json\n"
        prompt_ant_path = data_dir / f'temp_prompt_claude_{site}_{fecha_final}_q_anterior.txt'
        with open(prompt_ant_path, 'w', encoding='utf-8') as f:
            f.write(prompt_ant)
        print(f"   \u2705 Prompt Q anterior generado: {prompt_ant_path.name}")
    else:
        print(f"   ⚠️  Q anterior sin comentarios suficientes")
else:
    print(f"   \u2705 CP5 Q anterior ya existe en cache")

datos_prep_path = data_dir / f'temp_datos_preparados_{site}_{fecha_final}.json'
with open(datos_prep_path, 'w', encoding='utf-8') as f:
    json.dump(datos_preparados, f, indent=2, ensure_ascii=False)

print(f"   \u2705 Datos preparados guardados: {datos_prep_path.name} (en data/)")

# Generate promoter analysis prompt
promotores_path = data_dir / f'checkpoint5_promotores_{site}_{fecha_final}.json'
if not promotores_path.exists():
    datos_prom = preparar_comentarios_promotores(
        df_nps=df_nps,
        mes_actual=meses_q_actual,
        max_comentarios=200,
    )
    if datos_prom['metadata']['total_motivos_analizados'] > 0:
        prompt_prom = generar_prompt_promotores(datos_prom, site=site)
        prompt_prom += f"\n\n## ⚙️ METADATA PARA INCLUIR EN EL JSON\n"
        prompt_prom += f"- update_tipo: \"{update_tipo}\"\n"
        prompt_prom += f"- fecha_corte: \"{current_fecha_corte}\"\n"
        prompt_prom += f"\nIncluir estos valores en metadata del JSON generado.\n"
        prompt_prom_path = data_dir / f'temp_prompt_promotores_{site}_{fecha_final}.txt'
        with open(prompt_prom_path, 'w', encoding='utf-8') as f:
            f.write(prompt_prom)
        print(f"   \u2705 Prompt promotores generado: {prompt_prom_path.name}")
    else:
        print(f"   ⚠️  Sin comentarios de promotores suficientes")
else:
    print(f"   \u2705 CP5 promotores ya existe en cache")

print("\n" + "="*80)
print("\u23f8\ufe0f  ANALISIS CUALITATIVO - ACCION REQUERIDA")
print("="*80)
print(f"""

Claude Code leerá estos archivos automáticamente y ejecutará el análisis:

   Q actual: {prompt_path.name}
   Q anterior: temp_prompt_claude_{site}_{fecha_final}_q_anterior.txt (si existe)
   Promotores: temp_prompt_promotores_{site}_{fecha_final}.txt (si existe)

""")

# Exit with error to pause ejecutar_modelo_completo.py
sys.exit(1)
