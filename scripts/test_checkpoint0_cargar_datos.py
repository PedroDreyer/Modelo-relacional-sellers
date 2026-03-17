"""
Checkpoint 0: Carga Unica de Datos de Encuestas NPS Sellers desde BigQuery

Carga los datos de encuestas sellers UNA SOLA VEZ.
Solo encuestas NPS. No hay datos reales, clasificaciones ni merge.

Output: datos_nps_{SITE}_{MES}.parquet, checkpoint0_{SITE}_{MES}_metadatos.json
"""

import json
import logging
import sys
from pathlib import Path
from datetime import datetime
import pandas as pd

logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger(__name__)

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root / 'src'))

from nps_model.io.bigquery_client import BigQueryClient
from nps_model.io.loaders import DataLoader
from nps_model.utils.dates import calcular_meses_atras

print("="*80)
print("\U0001f4e6 CHECKPOINT 0: CARGA DATOS ENCUESTAS SELLERS")
print("="*80)

# PASO 1: Cargar configuracion
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

filtros = config_data.get('filtros', {})
producto_list = filtros.get('producto', [])
producto = producto_list[0] if producto_list else None
update_tipo = config_data.get('update', {}).get('tipo', 'all')

print(f"   Site: {site}")
print(f"   Comparación: {quarter_label(quarter_anterior)} vs {quarter_label(quarter_actual)}")
print(f"   Update: {update_tipo}")
if producto:
    print(f"   Producto: {producto}")

# PASO 2: Verificar si ya existe checkpoint0
print("\n\U0001f50d Paso 2: Verificando checkpoint existente...")

data_dir = project_root / 'data'
data_dir.mkdir(exist_ok=True)

datos_nps_path = data_dir / f'datos_nps_{site}_{mes_actual}.parquet'
metadatos_path = data_dir / f'checkpoint0_{site}_{mes_actual}_metadatos.json'

if datos_nps_path.exists() and metadatos_path.exists():
    with open(metadatos_path, 'r', encoding='utf-8') as f:
        metadatos = json.load(f)

    cached_update = metadatos.get('update_tipo', 'unknown')
    if cached_update == update_tipo:
        print(f"   \u2705 Checkpoint ya existe para {site} - {mes_actual} (update: {update_tipo})")
        print(f"   Archivos: {datos_nps_path.name}, {metadatos_path.name}")
        print(f"   Registros NPS: {metadatos.get('registros_nps'):,}")
        print("\n" + "="*80)
        print("\u2705 CHECKPOINT 0 YA EXISTE - NO ES NECESARIO RECARGAR")
        print("="*80)
        sys.exit(0)
    else:
        print(f"   \u26a0\ufe0f  Cache INVÁLIDO: update_tipo cambió ({cached_update} → {update_tipo})")
        print(f"   \U0001f5d1\ufe0f  Eliminando cache obsoleto...")
        datos_nps_path.unlink(missing_ok=True)
        metadatos_path.unlink(missing_ok=True)
        # También eliminar downstream caches
        enriq_path = data_dir / f'datos_nps_enriquecido_{site}_{mes_actual}.parquet'
        cp1_path = data_dir / f'checkpoint1_consolidado_{site}_{mes_actual}.json'
        cp2_meta_path = data_dir / f'checkpoint2_{site}_{mes_actual}_metadatos.json'
        cp3_path = data_dir / f'checkpoint3_tendencias_anomalias_{site}_{mes_actual}.json'
        cp5_path_data = data_dir / f'checkpoint5_causas_raiz_{site}_{mes_actual}.json'
        cp5_path_outputs = project_root / 'outputs' / f'checkpoint5_causas_raiz_{site}_{mes_actual}.json'
        for p in [enriq_path, cp1_path, cp2_meta_path, cp3_path, cp5_path_data, cp5_path_outputs]:
            p.unlink(missing_ok=True)

print("   No se encontro checkpoint, cargando desde BigQuery...")

# PASO 3: Inicializar BigQuery
print("\n\U0001f50c Paso 3: Inicializando cliente BigQuery...")

bq_client = BigQueryClient(project_id=config_data.get('bigquery', {}).get('project_id', 'meli-bi-data'))
bq_client.initialize()
loader = DataLoader(bq_client=bq_client, use_cache=True, cache_dir=str(project_root / '.cache'))
print("   \u2705 Cliente BigQuery inicializado")

# PASO 4: Cargar datos NPS Sellers
print("\n\U0001f4ca Paso 4: Cargando datos de encuestas NPS Sellers...")

df_encuestas = loader.load_nps_data(
    sites=[site],
    fecha_final=mes_actual,
    quarter_anterior=quarter_anterior,
    quarter_actual=quarter_actual,
    producto=producto,
    update_tipo=update_tipo,
)

print(f"   \u2705 Datos NPS Sellers cargados: {len(df_encuestas):,} registros")

if df_encuestas.empty:
    print("   \u274c ERROR: No se obtuvieron datos.")
    sys.exit(1)

# PASO 5: Guardar datos en parquet
print("\n\U0001f4be Paso 5: Guardando datos en formato parquet...")

try:
    df_encuestas.to_parquet(datos_nps_path, compression='snappy', index=False)
    print(f"   \u2705 Guardado: {datos_nps_path.name} ({datos_nps_path.stat().st_size / 1024 / 1024:.1f} MB)")
except Exception as e:
    print(f"   \u274c Error guardando datos NPS: {e}")
    sys.exit(1)

# PASO 6: Guardar metadatos
print("\n\U0001f4cb Paso 6: Guardando metadatos del checkpoint...")

metadatos = {
    "site": site,
    "mes_actual": mes_actual,
    "update_tipo": update_tipo,
    "timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
    "registros_nps": len(df_encuestas),
    "columnas_nps": list(df_encuestas.columns),
    "archivos": {"datos_nps": datos_nps_path.name}
}

try:
    with open(metadatos_path, 'w', encoding='utf-8') as f:
        json.dump(metadatos, f, indent=2, ensure_ascii=False)
    print(f"   \u2705 Guardado: {metadatos_path.name}")
except Exception as e:
    print(f"   Error guardando metadatos: {e}")

# RESUMEN FINAL
print("\n" + "="*80)
print("\u2705 CHECKPOINT 0 COMPLETADO EXITOSAMENTE")
print("="*80)
print(f"   Site: {site}, Mes: {mes_actual}")
print(f"   Registros NPS: {len(df_encuestas):,}")
print(f"   Siguiente: python scripts/test_checkpoint1_drivers_nps.py")
print("="*80)
