"""
CHECKPOINT 2: Enriquecer Datos con Fuentes Externas

Carga datos adicionales de BigQuery y los joinea con las encuestas NPS:
- Credits: perfil crediticio (FLAG_USA_CREDITO, ESTADO_OFERTA_CREDITO, etc.)
- Transacciones: TPV/TPN por producto, rangos, PF/PJ desde KYC
- Inversiones: POTS (FLAG_USA_INVERSIONES)

Join: CUST_ID + END_DATE_MONTH

Output: datos_nps_enriquecido_{SITE}_{MES}.parquet

NOTA: Cada fuente es opcional. Si falla o no está habilitada, el modelo
continúa sin ella. Las columnas se agregan al parquet con LEFT JOIN.
"""

import json
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from datetime import datetime

import pandas as pd
import yaml

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root / 'src'))

from nps_model.io.bigquery_client import BigQueryClient
from nps_model.io.loaders import EnrichmentLoader

print("=" * 80)
print("🔗 CHECKPOINT 2: ENRIQUECER DATOS CON FUENTES EXTERNAS")
print("=" * 80)

# Paso 1: Config
print("\n📝 Paso 1: Cargando configuración...")
config_path = project_root / 'config' / 'config.yaml'
with open(config_path, 'r', encoding='utf-8') as f:
    config = yaml.safe_load(f)

site = config.get('sites', ['MLA'])[0]
quarter_actual = config.get('quarter_actual', '26Q1')
quarter_anterior = config.get('quarter_anterior', '25Q4')
from nps_model.utils.dates import quarter_fecha_final, quarter_label
mes_actual = quarter_fecha_final(quarter_actual)
enrich_config = config.get('enriquecimiento', {})

cargar_credits = enrich_config.get('cargar_credits', False)
cargar_transacciones = enrich_config.get('cargar_transacciones', False)
cargar_inversiones = enrich_config.get('cargar_inversiones', False)
cargar_segmentacion = enrich_config.get('cargar_segmentacion', False)
cargar_topoff = enrich_config.get('cargar_topoff', False)
cargar_pricing = enrich_config.get('cargar_pricing', False)
cargar_region = enrich_config.get('cargar_region', False)
usar_tablas_dataflow = enrich_config.get('usar_tablas_dataflow', False)
dataset_dataflow = enrich_config.get('dataset_dataflow', 'SBOX_NPS_ANALYTICS')

# Restricciones OP: ya vienen integradas en main_query_op.sql (CP0), no se cargan aquí
update_tipo = config.get('update', {}).get('tipo', 'all')
cargar_restricciones = False  # integrado en CP0 para OP

print(f"   Site: {site}, Mes: {mes_actual}")
print(f"   Credits: {'✅' if cargar_credits else '⏭️  deshabilitado'}")
print(f"   Transacciones: {'✅' if cargar_transacciones else '⏭️  deshabilitado'}")
print(f"   Inversiones: {'✅' if cargar_inversiones else '⏭️  deshabilitado'}")
print(f"   Segmentación: {'✅' if cargar_segmentacion else '⏭️  deshabilitado'}")
print(f"   Top Off: {'✅' if cargar_topoff else '⏭️  deshabilitado'}")
print(f"   Pricing: {'✅' if cargar_pricing else '⏭️  deshabilitado'}")
print(f"   Region: {'✅' if cargar_region else '⏭️  deshabilitado'}")
if cargar_restricciones:
    print(f"   Restricciones OP: ✅ (update={update_tipo})")
if any([cargar_credits, cargar_transacciones, cargar_inversiones, cargar_segmentacion, cargar_topoff, cargar_pricing]):
    print(f"   Fuente: {'Tablas Dataflow (' + dataset_dataflow + ')' if usar_tablas_dataflow else 'Queries BigQuery'}")

# Si todo está deshabilitado, salir rápido
if not any([cargar_credits, cargar_transacciones, cargar_inversiones, cargar_segmentacion, cargar_topoff, cargar_pricing, cargar_restricciones]):
    print("\n   ℹ️  No hay fuentes de enriquecimiento habilitadas.")
    print("   💡 Para habilitar, cambia cargar_credits/transacciones/inversiones a true en config.yaml")
    print("\n" + "=" * 80)
    print("✅ CHECKPOINT 2 COMPLETADO (SIN ENRIQUECIMIENTO)")
    print("=" * 80)
    sys.exit(0)

# Paso 2: Verificar si ya existe
print("\n🔍 Paso 2: Verificando checkpoint existente...")
data_dir = project_root / 'data'
enriquecido_path = data_dir / f'datos_nps_enriquecido_{site}_{mes_actual}.parquet'

if enriquecido_path.exists():
    # Validate update_tipo matches
    meta_path = data_dir / f'checkpoint2_{site}_{mes_actual}_metadatos.json'
    cache_valid = True
    if meta_path.exists():
        with open(meta_path, 'r', encoding='utf-8') as f:
            meta = json.load(f)
        cached_update = meta.get('update_tipo', 'unknown')
        if cached_update != update_tipo:
            print(f"   ⚠️  Cache INVÁLIDO: update_tipo cambió ({cached_update} → {update_tipo})")
            print(f"   🗑️  Eliminando cache obsoleto...")
            enriquecido_path.unlink(missing_ok=True)
            meta_path.unlink(missing_ok=True)
            cache_valid = False
    # Also check that universo JSONs exist (they are separate outputs)
    universo_files = [
        data_dir / f'credits_universo_{site}_{mes_actual}.json',
        data_dir / f'inversiones_universo_{site}_{mes_actual}.json',
        data_dir / f'topoff_universo_{site}_{mes_actual}.json',
        data_dir / f'segmentacion_universo_{site}_{mes_actual}.json',
    ]
    universo_files.append(data_dir / f'aprobacion_universo_{site}_{mes_actual}.json')
    universo_files.append(data_dir / f'pricing_universo_{site}_{mes_actual}.json')
    # Check existence AND that they match current update_tipo
    def _universo_valid(path):
        if not path.exists():
            return False
        # Verify the universo was generated for the same update_tipo
        # by checking .cache pkl params (universo JSONs don't store update_tipo directly)
        return True  # existence check is sufficient since CP2 cache invalidation handles update_tipo
    universo_missing = [f for f in universo_files if not f.exists()]
    # Also invalidate if .cache pkl files for universo don't match update_tipo
    cache_dir = project_root / '.cache'
    universo_cache_names = ['credits_universo', 'inversiones_universo', 'topoff_universo', 'segmentacion_universo']
    universo_cache_names.append('aprobacion_universo')
    for cache_name in universo_cache_names:
        pkl = cache_dir / f'{cache_name}_{site}_{mes_actual}.pkl'
        if pkl.exists():
            import pickle
            try:
                with open(pkl, 'rb') as pf:
                    cached = pickle.load(pf)
                cached_ut = cached.get('params', {}).get('update_tipo', 'unknown')
                if cached_ut != update_tipo:
                    # Wrong update_tipo — delete both pkl and json
                    pkl.unlink(missing_ok=True)
                    json_path = data_dir / f'{cache_name}_{site}_{mes_actual}.json'
                    json_path.unlink(missing_ok=True)
                    if json_path not in universo_missing:
                        universo_missing.append(json_path)
            except Exception:
                pass
    if cache_valid and not universo_missing:
        print(f"   ✅ Datos enriquecidos ya existen: {enriquecido_path.name}")
        print("   💡 Para regenerar, elimina el archivo y vuelve a ejecutar")
        print("\n" + "=" * 80)
        print("✅ CHECKPOINT 2 YA EXISTE - USANDO CACHE")
        print("=" * 80)
        sys.exit(0)
    elif cache_valid and universo_missing:
        print(f"   ✅ Parquet enriquecido existe pero faltan {len(universo_missing)} JSONs de universo")
        print(f"   🔄 Re-ejecutando queries de universo...")

# Paso 3: Cargar datos NPS base
print("\n📦 Paso 3: Cargando datos NPS base...")
datos_nps_path = data_dir / f'datos_nps_{site}_{mes_actual}.parquet'

if not datos_nps_path.exists():
    print(f"   ❌ ERROR: No se encontró {datos_nps_path.name}")
    print("   💡 Ejecuta primero: python scripts/test_checkpoint0_cargar_datos.py")
    sys.exit(1)

df_nps = pd.read_parquet(datos_nps_path)
print(f"   ✅ {len(df_nps):,} registros NPS cargados")

# Paso 4: Inicializar BigQuery
print("\n🔌 Paso 4: Inicializando cliente BigQuery...")
bq_client = BigQueryClient(
    project_id=config.get('bigquery', {}).get('project_id', 'meli-bi-data')
)
bq_client.initialize()
enrichment_loader = EnrichmentLoader(
    bq_client=bq_client,
    use_cache=True,
    cache_dir=str(project_root / '.cache'),
    use_dataflow_tables=usar_tablas_dataflow,
    dataset_dataflow=dataset_dataflow,
    quarter_anterior=quarter_anterior,
    quarter_actual=quarter_actual,
    update_tipo=config.get('update', {}).get('tipo', 'all'),
)

# Paso 5: Cargar fuentes de enriquecimiento EN PARALELO
print("\n🔗 Paso 5: Cargando fuentes de enriquecimiento (en paralelo)...")

fuentes_cargadas = {}
start_total = time.time()

tasks = {}
if cargar_credits:
    tasks['credits'] = lambda: enrichment_loader.load_credits([site], mes_actual)
    tasks['credits_universo'] = lambda: enrichment_loader.load_credits_universo([site], mes_actual)
    tasks['tc_limite'] = lambda: enrichment_loader.load_tc_limite([site], mes_actual)
    tasks['tc_limite_universo'] = lambda: enrichment_loader.load_tc_limite_universo([site], mes_actual)
if cargar_transacciones:
    tasks['transacciones'] = lambda: enrichment_loader.load_transacciones([site], mes_actual)
if cargar_inversiones:
    tasks['inversiones'] = lambda: enrichment_loader.load_inversiones([site], mes_actual)
    tasks['inversiones_universo'] = lambda: enrichment_loader.load_inversiones_universo([site], mes_actual)
if cargar_segmentacion:
    tasks['segmentacion'] = lambda: enrichment_loader.load_segmentacion([site], mes_actual)
    tasks['segmentacion_universo'] = lambda: enrichment_loader.load_segmentacion_universo([site], mes_actual)
if cargar_topoff:
    tasks['topoff'] = lambda: enrichment_loader.load_topoff([site], mes_actual)
    tasks['topoff_universo'] = lambda: enrichment_loader.load_topoff_universo([site], mes_actual)
if cargar_pricing:
    tasks['pricing'] = lambda: enrichment_loader.load_pricing([site], mes_actual)
    tasks['pricing_universo'] = lambda: enrichment_loader.load_pricing_universo([site], mes_actual)
if cargar_region and site == 'MLA':
    tasks['region'] = lambda: enrichment_loader.load_region([site], mes_actual)
elif cargar_region:
    print(f"   ⏭️  Region deshabilitado para {site} (solo MLA)")
tasks['aprobacion'] = lambda: enrichment_loader.load_aprobacion_op([site], mes_actual)
tasks['aprobacion_universo'] = lambda: enrichment_loader.load_aprobacion_universo([site], mes_actual)
if cargar_restricciones:
    tasks['restricciones'] = lambda: enrichment_loader.load_restricciones([site], mes_actual)

with ThreadPoolExecutor(max_workers=len(tasks)) as executor:
    futures = {executor.submit(fn): name for name, fn in tasks.items()}
    for future in as_completed(futures):
        name = futures[future]
        try:
            df_result = future.result()
            if df_result is not None and not df_result.empty:
                fuentes_cargadas[name] = df_result
        except Exception as e:
            print(f"   ⚠️  {name.capitalize()} falló: {str(e)[:120]}")

elapsed_total = time.time() - start_total
print(f"\n   ⏱️  Fuentes cargadas en {elapsed_total:.1f}s: {list(fuentes_cargadas.keys())}")

if not fuentes_cargadas:
    print("   ⚠️  Ninguna fuente de enriquecimiento retornó datos")
    print("\n" + "=" * 80)
    print("✅ CHECKPOINT 2 COMPLETADO (SIN DATOS DE ENRIQUECIMIENTO)")
    print("=" * 80)
    sys.exit(0)

# Paso 6: Joinear fuentes con datos NPS
print("\n🔗 Paso 6: Joineando fuentes con datos NPS...")

df_enriched = df_nps.copy()
columnas_originales = set(df_enriched.columns)

# Credits: join por CUST_ID + END_DATE_MONTH
if 'credits' in fuentes_cargadas:
    df_cr = fuentes_cargadas['credits']
    # Renombrar para join
    df_cr = df_cr.rename(columns={'CUS_CUST_ID': 'CUST_ID', 'TIM_MONTH': 'END_DATE_MONTH'})
    df_cr['END_DATE_MONTH'] = df_cr['END_DATE_MONTH'].astype(str)
    # Dedup: tomar último registro por seller+mes
    df_cr = df_cr.drop_duplicates(subset=['CUST_ID', 'END_DATE_MONTH'], keep='last')
    # Seleccionar columnas útiles para evitar conflictos
    cols_credits = ['CUST_ID', 'END_DATE_MONTH', 'CREDIT_GROUP', 'FLAG_USA_CREDITO',
                    'FLAG_TARJETA_CREDITO', 'ESTADO_OFERTA_CREDITO', 'FLAG_TC_OFFER']
    cols_disponibles = [c for c in cols_credits if c in df_cr.columns]
    df_cr = df_cr[cols_disponibles]

    n_antes = len(df_enriched)
    df_enriched = df_enriched.merge(df_cr, on=['CUST_ID', 'END_DATE_MONTH'], how='left')
    # OFERTA_TC se deriva en el bloque tc_limite (FLAG_TC_OFFER viene vacío en CREDITS_SELLERS)
    print(f"   ✅ Credits joineado: {len(df_enriched):,} registros (match rate: {df_enriched['FLAG_USA_CREDITO'].notna().mean()*100:.0f}%)")

# TC Límite: join por CUST_ID + END_DATE_MONTH
if 'tc_limite' in fuentes_cargadas:
    df_tc = fuentes_cargadas['tc_limite']
    df_tc = df_tc.rename(columns={'CUS_CUST_ID': 'CUST_ID', 'TIM_MONTH': 'END_DATE_MONTH'})
    df_tc['END_DATE_MONTH'] = df_tc['END_DATE_MONTH'].astype(str)
    df_tc = df_tc.drop_duplicates(subset=['CUST_ID', 'END_DATE_MONTH'], keep='last')
    cols_tc = ['CUST_ID', 'END_DATE_MONTH', 'LIMITE_OFRECIDO', 'LIMITE_CONSUMIDO', 'FLAG_TC_OFFER']
    cols_disponibles = [c for c in cols_tc if c in df_tc.columns]
    df_tc = df_tc[cols_disponibles]
    # Avoid column conflict: tc_limite trae FLAG_TC_OFFER real (CREDITS_SELLERS lo tiene vacío)
    if 'FLAG_TC_OFFER' in df_enriched.columns:
        df_enriched = df_enriched.drop(columns=['FLAG_TC_OFFER'])
    if 'OFERTA_TC' in df_enriched.columns:
        df_enriched = df_enriched.drop(columns=['OFERTA_TC'])
    df_enriched = df_enriched.merge(df_tc, on=['CUST_ID', 'END_DATE_MONTH'], how='left')
    # Derivar OFERTA_TC desde FLAG_TC_OFFER (ahora de LK_MP_MAUS_CREDIT_PROFILE)
    # FLAG_TC_OFFER puede llegar como int (0/1) o como string ya mapeado ("Con oferta TC")
    if 'FLAG_TC_OFFER' in df_enriched.columns:
        col = df_enriched['FLAG_TC_OFFER']
        if col.dropna().apply(lambda x: isinstance(x, str)).any():
            # Ya viene como string desde BQ — usar directamente, normalizar NaN
            df_enriched['OFERTA_TC'] = col.where(col.notna(), 'Sin dato')
        else:
            df_enriched['OFERTA_TC'] = col.map({1: 'Con oferta TC', 0: 'Sin oferta TC'}).fillna('Sin dato')
    # Derivar RANGO_LIMITE_TC para análisis por dimensión (rangos por moneda local)
    if 'LIMITE_OFRECIDO' in df_enriched.columns:
        import numpy as np
        RANGOS_LIMITE_TC = {
            'MLB': {  # BRL
                'cortes': [0, 500, 2000, 8000, 25000],
                'labels': ['Sin límite', 'Hasta R$500', 'R$500-2K', 'R$2K-8K', 'R$8K-25K', '+R$25K'],
            },
            'MLM': {  # MXN
                'cortes': [0, 3000, 10000, 30000, 80000],
                'labels': ['Sin límite', 'Hasta $3K', '$3K-10K', '$10K-30K', '$30K-80K', '+$80K'],
            },
            'MLA': {  # ARS
                'cortes': [0, 100000, 500000, 1500000, 5000000],
                'labels': ['Sin límite', 'Hasta $100K', '$100K-500K', '$500K-1.5M', '$1.5M-5M', '+$5M'],
            },
        }
        rango_cfg = RANGOS_LIMITE_TC.get(site, RANGOS_LIMITE_TC['MLB'])
        cortes = rango_cfg['cortes']
        lbls = rango_cfg['labels']
        conditions = [
            df_enriched['LIMITE_OFRECIDO'].isna(),
            df_enriched['LIMITE_OFRECIDO'] <= cortes[0],
            df_enriched['LIMITE_OFRECIDO'] <= cortes[1],
            df_enriched['LIMITE_OFRECIDO'] <= cortes[2],
            df_enriched['LIMITE_OFRECIDO'] <= cortes[3],
            df_enriched['LIMITE_OFRECIDO'] <= cortes[4],
            df_enriched['LIMITE_OFRECIDO'] > cortes[4],
        ]
        all_labels = ['Sin TC'] + lbls
        df_enriched['RANGO_LIMITE_TC'] = np.select(conditions, all_labels, default='Sin dato')
    match_rate = df_enriched['LIMITE_OFRECIDO'].notna().mean() * 100
    print(f"   ✅ TC Límite joineado: {len(df_enriched):,} registros (match rate: {match_rate:.0f}%)")

# Transacciones: join por CUS_CUST_ID + TIM_MONTH
if 'transacciones' in fuentes_cargadas:
    df_tx = fuentes_cargadas['transacciones']
    df_tx = df_tx.rename(columns={'CUS_CUST_ID': 'CUST_ID', 'TIM_MONTH': 'END_DATE_MONTH'})
    df_tx = df_tx.drop_duplicates(subset=['CUST_ID', 'END_DATE_MONTH'], keep='last')
    cols_tx = ['CUST_ID', 'END_DATE_MONTH', 'TPN_TOTAL', 'TPV_TOTAL',
               'RANGO_TPN', 'RANGO_TPV', 'TIPO_PERSONA_KYC',
               'TPN_POINT', 'TPN_QR', 'TPN_LINK', 'TPN_API']
    cols_disponibles = [c for c in cols_tx if c in df_tx.columns]
    df_tx = df_tx[cols_disponibles]

    df_enriched = df_enriched.merge(df_tx, on=['CUST_ID', 'END_DATE_MONTH'], how='left')
    print(f"   ✅ Transacciones joineado: {len(df_enriched):,} registros (match rate: {df_enriched['RANGO_TPN'].notna().mean()*100:.0f}%)")

# Inversiones: join por CUS_CUST_ID + TIM_MONTH
if 'inversiones' in fuentes_cargadas:
    df_inv = fuentes_cargadas['inversiones']
    df_inv = df_inv.rename(columns={'CUS_CUST_ID': 'CUST_ID', 'TIM_MONTH': 'END_DATE_MONTH'})
    df_inv['END_DATE_MONTH'] = df_inv['END_DATE_MONTH'].astype(str)
    df_inv = df_inv.drop_duplicates(subset=['CUST_ID', 'END_DATE_MONTH'], keep='last')
    cols_inv = ['CUST_ID', 'END_DATE_MONTH', 'FLAG_POTS_ACTIVO', 'FLAG_USA_INVERSIONES',
                'FLAG_INVERSIONES', 'FLAG_ASSET', 'FLAG_WINNER']
    cols_disponibles = [c for c in cols_inv if c in df_inv.columns]
    df_inv = df_inv[cols_disponibles]

    df_enriched = df_enriched.merge(df_inv, on=['CUST_ID', 'END_DATE_MONTH'], how='left')
    print(f"   ✅ Inversiones joineado: {len(df_enriched):,} registros (match rate: {df_enriched['FLAG_USA_INVERSIONES'].notna().mean()*100:.0f}%)")

# Segmentacion: join por CUS_CUST_ID + TIM_MONTH_TRANSACTION
if 'segmentacion' in fuentes_cargadas:
    df_seg = fuentes_cargadas['segmentacion']
    # Rename join keys to match NPS data
    df_seg = df_seg.rename(columns={
        'CUS_CUST_ID': 'CUST_ID',
    })
    # Convert TIM_MONTH_TRANSACTION to string to match END_DATE_MONTH format
    if 'TIM_MONTH_TRANSACTION' in df_seg.columns:
        df_seg['END_DATE_MONTH'] = df_seg['TIM_MONTH_TRANSACTION'].astype(str)
    
    df_seg = df_seg.drop_duplicates(subset=['CUST_ID', 'END_DATE_MONTH'], keep='last')
    
    # Derive PRODUCTO_PRINCIPAL and NEWBIE_LEGACY
    if 'PRODUCTO' in df_seg.columns:
        df_seg['PRODUCTO_PRINCIPAL'] = df_seg['PRODUCTO'].map({
            'POINT': 'Point',
            'QR': 'QR',
            'LINK': 'OP',
            'APICOW': 'OP',
            'TRANSFERENCIAS': 'Transferencias',
        }).fillna('Otro')
    
    if 'NEW_MAS_FLAG' in df_seg.columns:
        df_seg['NEWBIE_LEGACY'] = df_seg['NEW_MAS_FLAG'].map({
            1: 'Newbie',
            0: 'Legacy',
        }).fillna('Sin dato')
    
    cols_seg = ['CUST_ID', 'END_DATE_MONTH', 'PRODUCTO_PRINCIPAL', 'NEWBIE_LEGACY',
                'SEGMENTO', 'PERSON', 'PRODUCTO', 'NEW_MAS_FLAG',
                'POINT_FLAG', 'QR_FLAG', 'LINK_FLAG', 'API_FLAG', 'TRANSF_FLAG',
                'TPN_POINT', 'TPN_QR', 'TPN_LINK', 'TPN_API', 'TPN_TRS',
                'TPV_POINT', 'TPV_QR', 'TPV_API', 'TPV_LINK',
                'OP_FLAG', 'USO_PRODUCTOS']
    cols_disponibles = [c for c in cols_seg if c in df_seg.columns]
    df_seg = df_seg[cols_disponibles]

    df_enriched = df_enriched.merge(df_seg, on=['CUST_ID', 'END_DATE_MONTH'], how='left')
    match_col = 'PRODUCTO_PRINCIPAL' if 'PRODUCTO_PRINCIPAL' in df_enriched.columns else cols_disponibles[-1]
    print(f"   ✅ Segmentación joineado: {len(df_enriched):,} registros (match rate: {df_enriched[match_col].notna().mean()*100:.0f}%)")

    # Derive FLAG_ONLY_TRANSFER: TRANSFERENCIAS con 0 selling tools
    if 'PRODUCTO' in df_enriched.columns:
        selling_flags = ['POINT_FLAG', 'QR_FLAG', 'LINK_FLAG', 'API_FLAG']
        available_flags = [f for f in selling_flags if f in df_enriched.columns]
        if available_flags:
            has_selling_tool = df_enriched[available_flags].fillna(0).sum(axis=1) > 0
            is_transfer = df_enriched['PRODUCTO'] == 'TRANSFERENCIAS'
            df_enriched['FLAG_ONLY_TRANSFER'] = 'Sin dato'
            df_enriched.loc[df_enriched['PRODUCTO'].notna(), 'FLAG_ONLY_TRANSFER'] = 'USA_SELLING_TOOL'
            df_enriched.loc[is_transfer & ~has_selling_tool, 'FLAG_ONLY_TRANSFER'] = 'ONLY_TRANSFER'
            print(f"   ✅ FLAG_ONLY_TRANSFER derivado: {(df_enriched['FLAG_ONLY_TRANSFER'] == 'ONLY_TRANSFER').sum():,} only-transfer")

# Top Off: join solo por CUST_ID + SITE (tabla de estado, sin mes)
if 'topoff' in fuentes_cargadas:
    df_to = fuentes_cargadas['topoff']
    df_to = df_to.rename(columns={'CUS_CUST_ID': 'CUST_ID', 'SIT_SITE_ID': 'SITE'})
    df_to = df_to.drop_duplicates(subset=['CUST_ID', 'SITE'], keep='last')
    cols_topoff = ['CUST_ID', 'SITE', 'FLAG_TOPOFF', 'TOPOFF_CATEGORY']
    cols_disponibles = [c for c in cols_topoff if c in df_to.columns]
    df_to = df_to[cols_disponibles]

    df_enriched = df_enriched.merge(df_to, on=['CUST_ID', 'SITE'], how='left')
    # Convertir a label string (CP1 necesita categorías, no int)
    df_enriched['FLAG_TOPOFF'] = df_enriched['FLAG_TOPOFF'].fillna(0).astype(int).map({1: 'Con Top Off', 0: 'Sin Top Off'})
    topoff_match = (df_enriched['FLAG_TOPOFF'] == 'Con Top Off').sum()
    print(f"   ✅ Top Off joineado: {len(df_enriched):,} registros (match rate: {topoff_match / len(df_enriched) * 100:.1f}%)")

# Pricing: join por CUST_ID + END_DATE_MONTH (o solo CUST_ID para MLA/MLM)
if 'pricing' in fuentes_cargadas:
    df_pr = fuentes_cargadas['pricing']
    df_pr = df_pr.rename(columns={'CUS_CUST_ID': 'CUST_ID', 'TIM_MONTH': 'END_DATE_MONTH'})
    df_pr['END_DATE_MONTH'] = df_pr['END_DATE_MONTH'].astype(str)
    df_pr = df_pr.drop_duplicates(subset=['CUST_ID', 'END_DATE_MONTH'], keep='last')

    cols_pricing = ['CUST_ID', 'END_DATE_MONTH', 'PRICING', 'SCALE_LEVEL', 'PRICING_TYPE', 'PRICING_DESCRIPTION']
    cols_disponibles = [c for c in cols_pricing if c in df_pr.columns]
    df_pr = df_pr[cols_disponibles]

    if 'END_DATE_MONTH' in df_pr.columns and len(df_pr['END_DATE_MONTH'].unique()) > 1:
        df_enriched = df_enriched.merge(df_pr, on=['CUST_ID', 'END_DATE_MONTH'], how='left')
    else:
        # MLA/MLM: join solo por CUST_ID (pricing es snapshot, no mensual)
        df_pr_nodups = df_pr.drop(columns=['END_DATE_MONTH'], errors='ignore').drop_duplicates(subset=['CUST_ID'], keep='last')
        df_enriched = df_enriched.merge(df_pr_nodups, on=['CUST_ID'], how='left')

    # Convert to categorical labels
    df_enriched['PRICING'] = df_enriched['PRICING'].fillna(0).astype(int)
    df_enriched['FLAG_PRICING'] = df_enriched['PRICING'].map({1: 'Con pricing escalas', 0: 'Sin pricing escalas'})
    df_enriched['SCALE_LEVEL'] = df_enriched['SCALE_LEVEL'].apply(
        lambda x: f"Escala {int(x)}" if pd.notna(x) and x != 0 else "Sin escala"
    )
    pricing_match = (df_enriched['FLAG_PRICING'] == 'Con pricing escalas').sum()
    print(f"   ✅ Pricing joineado: {len(df_enriched):,} registros (match rate: {pricing_match / len(df_enriched) * 100:.1f}%)")

# Region: join por CUST_ID + END_DATE_MONTH
if 'region' in fuentes_cargadas:
    df_reg = fuentes_cargadas['region']
    df_reg = df_reg.rename(columns={'CUS_CUST_ID': 'CUST_ID', 'TIM_MONTH': 'END_DATE_MONTH'})
    df_reg['END_DATE_MONTH'] = df_reg['END_DATE_MONTH'].astype(str)
    df_reg = df_reg.drop_duplicates(subset=['CUST_ID', 'END_DATE_MONTH'], keep='last')
    cols_reg = ['CUST_ID', 'END_DATE_MONTH', 'REGION', 'CIUDAD']
    cols_disponibles = [c for c in cols_reg if c in df_reg.columns]
    df_reg = df_reg[cols_disponibles]
    df_enriched = df_enriched.merge(df_reg, on=['CUST_ID', 'END_DATE_MONTH'], how='left')
    # Normalize region: title case + group macro-regions
    if 'REGION' in df_enriched.columns:
        df_enriched['REGION'] = df_enriched['REGION'].str.strip().str.title()
        # Group Buenos Aires variants
        ba_variants = ['Buenos Aires', 'Ciudad Autonoma Buenos Aires', 'Ciudad Autónoma Buenos Aires',
                       'Capital Federal', 'Caba', 'C.a.b.a.', 'Ciudad De Buenos Aires']
        df_enriched.loc[df_enriched['REGION'].isin(ba_variants), 'REGION'] = 'Buenos Aires'
    if 'CIUDAD' in df_enriched.columns:
        df_enriched['CIUDAD'] = df_enriched['CIUDAD'].str.strip().str.title()
    region_match = df_enriched['REGION'].notna().sum()
    print(f"   ✅ Region joineado: {len(df_enriched):,} registros (match rate: {region_match / len(df_enriched) * 100:.1f}%)")

# Aprobación OP: join por CUST_ID + END_DATE_MONTH, derive RANGO_APROBACION
if 'aprobacion' in fuentes_cargadas:
    df_ap = fuentes_cargadas['aprobacion']
    df_ap = df_ap.rename(columns={'CUS_CUST_ID': 'CUST_ID', 'TIM_MONTH': 'END_DATE_MONTH'})
    df_ap['END_DATE_MONTH'] = df_ap['END_DATE_MONTH'].astype(str)
    df_ap = df_ap.drop_duplicates(subset=['CUST_ID', 'END_DATE_MONTH'], keep='last')
    cols_ap = ['CUST_ID', 'END_DATE_MONTH', 'TASA_APROBACION']
    cols_disponibles = [c for c in cols_ap if c in df_ap.columns]
    df_ap = df_ap[cols_disponibles]

    df_enriched = df_enriched.merge(df_ap, on=['CUST_ID', 'END_DATE_MONTH'], how='left')
    # Cast Decimal to float (BQ returns Decimal type)
    df_enriched['TASA_APROBACION'] = pd.to_numeric(df_enriched['TASA_APROBACION'], errors='coerce')
    # Derive categorical RANGO_APROBACION for CP1 dimension analysis
    df_enriched['RANGO_APROBACION'] = pd.cut(
        df_enriched['TASA_APROBACION'],
        bins=[-0.01, 0.85, 0.95, 1.01],
        labels=['Baja (<85%)', 'Media (85-95%)', 'Alta (≥95%)'],
        right=True,
    ).astype(str)
    df_enriched.loc[df_enriched['TASA_APROBACION'].isna(), 'RANGO_APROBACION'] = 'Sin datos'
    match_rate = df_enriched['TASA_APROBACION'].notna().mean() * 100
    print(f"   ✅ Aprobación joineado: {len(df_enriched):,} registros (match rate: {match_rate:.0f}%)")

# Restricciones OP: join por SURVEY_ID y filtrar CONSIDERACION_AJUSTADA = 1
if 'restricciones' in fuentes_cargadas:
    df_rest = fuentes_cargadas['restricciones']
    df_rest = df_rest.drop_duplicates(subset=['SURVEY_ID'], keep='last')
    cols_rest = ['SURVEY_ID', 'CONSIDERACION_AJUSTADA', 'COLOR_DE_TARJETA', 'USER_RESTRICCION']
    cols_disponibles = [c for c in cols_rest if c in df_rest.columns]
    df_rest = df_rest[cols_disponibles]

    n_antes = len(df_enriched)
    df_enriched = df_enriched.merge(df_rest, on='SURVEY_ID', how='left')
    # Sellers sin match en restricciones → CONSIDERACION_AJUSTADA = 1 (no restricto)
    df_enriched['CONSIDERACION_AJUSTADA'] = df_enriched['CONSIDERACION_AJUSTADA'].fillna(1).astype(int)
    n_restrictos = (df_enriched['CONSIDERACION_AJUSTADA'] == 0).sum()
    print(f"   🛡️  Restricciones joineado: {n_restrictos:,} sellers restrictos de {n_antes:,}")
    # Filtrar: solo sellers NO restrictos
    df_enriched = df_enriched[df_enriched['CONSIDERACION_AJUSTADA'] == 1].copy()
    print(f"   ✅ Filtrado CONSIDERACION_AJUSTADA=1: {len(df_enriched):,} registros (excluidos: {n_restrictos:,})")

# Resumen de columnas nuevas
columnas_nuevas = set(df_enriched.columns) - columnas_originales
print(f"\n   📋 Columnas nuevas agregadas ({len(columnas_nuevas)}):")
for col in sorted(columnas_nuevas):
    non_null = df_enriched[col].notna().sum()
    print(f"      • {col}: {non_null:,} valores no nulos")

# Paso 6b: Guardar universo total (data agregada, no se joinea)
if 'credits_universo' in fuentes_cargadas:
    import json as _json
    df_cu = fuentes_cargadas.pop('credits_universo')  # pop para no incluir en join
    cu_path = data_dir / f'credits_universo_{site}_{mes_actual}.json'
    # Convertir a dict por dimensión y mes para fácil consumo
    cu_data = {}
    for dim in ['CREDIT_GROUP', 'FLAG_USA_CREDITO', 'FLAG_TARJETA_CREDITO', 'ESTADO_OFERTA_CREDITO', 'FLAG_TC_OFFER']:
        if dim not in df_cu.columns:
            continue
        agg = df_cu.groupby(['TIM_MONTH', dim])['total_sellers'].sum().reset_index()
        totals = df_cu.groupby('TIM_MONTH')['total_sellers'].sum()
        shares = {}
        for _, row in agg.iterrows():
            mes = str(row['TIM_MONTH'])
            val = str(row[dim])
            total = totals.get(row['TIM_MONTH'], 1)
            shares.setdefault(dim, {}).setdefault(val, {})[mes] = round(float(row['total_sellers'] / total * 100), 2)
        cu_data.update(shares)
    # Map FLAG_TC_OFFER (0/1) → OFERTA_TC labels for consistency with survey data
    if 'FLAG_TC_OFFER' in cu_data:
        oferta_mapped = {}
        for val, mes_shares in cu_data['FLAG_TC_OFFER'].items():
            label = {'1': 'Con oferta TC', '0': 'Sin oferta TC'}.get(str(val).strip(), val)
            oferta_mapped[label] = mes_shares
        cu_data['OFERTA_TC'] = oferta_mapped
        del cu_data['FLAG_TC_OFFER']
    with open(cu_path, 'w', encoding='utf-8') as f:
        _json.dump(cu_data, f, indent=2, ensure_ascii=False)
    print(f"   ✅ Universo credits guardado: {cu_path.name}")

if 'tc_limite_universo' in fuentes_cargadas:
    import json as _json_tcl
    df_tcl = fuentes_cargadas.pop('tc_limite_universo')
    tcl_path = data_dir / f'tc_limite_universo_{site}_{mes_actual}.json'
    tcl_data = {}
    if 'TIM_MONTH' in df_tcl.columns:
        for dim in ['FLAG_TC_OFFER', 'RANGO_LIMITE_TC']:
            if dim not in df_tcl.columns:
                continue
            totals = df_tcl.groupby('TIM_MONTH')['total_sellers'].sum()
            for _, row in df_tcl.iterrows():
                mes = str(row['TIM_MONTH'])
                raw_val = str(row[dim])
                val = {'1': 'Con oferta TC', '0': 'Sin oferta TC'}.get(raw_val, raw_val)
                out_key = 'OFERTA_TC' if dim == 'FLAG_TC_OFFER' else dim
                total = totals.get(row['TIM_MONTH'], 1)
                tcl_data.setdefault(out_key, {}).setdefault(val, {})[mes] = round(
                    float(row['total_sellers'] / total * 100), 2
                )
    with open(tcl_path, 'w', encoding='utf-8') as f:
        _json_tcl.dump(tcl_data, f, indent=2, ensure_ascii=False)
    print(f"   ✅ Universo TC límite guardado: {tcl_path.name}")

if 'inversiones_universo' in fuentes_cargadas:
    import json as _json_inv
    df_iu = fuentes_cargadas.pop('inversiones_universo')
    iu_path = data_dir / f'inversiones_universo_{site}_{mes_actual}.json'
    iu_data = {}
    for dim in ['FLAG_USA_INVERSIONES', 'FLAG_POTS_ACTIVO', 'FLAG_INVERSIONES', 'FLAG_ASSET', 'FLAG_WINNER']:
        if dim not in df_iu.columns:
            continue
        agg = df_iu.groupby(['TIM_MONTH', dim])['total_sellers'].sum().reset_index()
        totals = df_iu.groupby('TIM_MONTH')['total_sellers'].sum()
        shares = {}
        for _, row in agg.iterrows():
            mes = str(row['TIM_MONTH'])
            val = str(row[dim])
            total = totals.get(row['TIM_MONTH'], 1)
            shares.setdefault(dim, {}).setdefault(val, {})[mes] = round(float(row['total_sellers'] / total * 100), 2)
        iu_data.update(shares)
    with open(iu_path, 'w', encoding='utf-8') as f:
        _json_inv.dump(iu_data, f, indent=2, ensure_ascii=False)
    print(f"   ✅ Universo inversiones guardado: {iu_path.name}")

if 'topoff_universo' in fuentes_cargadas:
    import json as _json2
    df_tu = fuentes_cargadas.pop('topoff_universo')
    tu_path = data_dir / f'topoff_universo_{site}_{mes_actual}.json'
    # Same format as credits_universo: {dim: {value: {month: share%}}}
    tu_data = {}
    if 'TIM_MONTH' in df_tu.columns and 'FLAG_TOPOFF' in df_tu.columns:
        totals = df_tu.groupby('TIM_MONTH')['total_sellers'].sum()
        for _, row in df_tu.iterrows():
            mes = str(row['TIM_MONTH'])
            val = str(row['FLAG_TOPOFF'])
            total = totals.get(row['TIM_MONTH'], 1)
            tu_data.setdefault('FLAG_TOPOFF', {}).setdefault(val, {})[mes] = round(float(row['total_sellers'] / total * 100), 2)
    with open(tu_path, 'w', encoding='utf-8') as f:
        _json2.dump(tu_data, f, indent=2, ensure_ascii=False)
    print(f"   ✅ Universo topoff guardado: {tu_path.name}")

if 'segmentacion_universo' in fuentes_cargadas:
    import json as _json_seg
    df_su = fuentes_cargadas.pop('segmentacion_universo')
    su_path = data_dir / f'segmentacion_universo_{site}_{mes_actual}.json'
    su_data = {}
    for dim in ['PRODUCTO_PRINCIPAL', 'NEWBIE_LEGACY', 'FLAG_ONLY_TRANSFER']:
        if dim not in df_su.columns:
            continue
        agg = df_su.groupby(['TIM_MONTH', dim])['total_sellers'].sum().reset_index()
        totals = df_su.groupby('TIM_MONTH')['total_sellers'].sum()
        shares = {}
        for _, row in agg.iterrows():
            mes = str(row['TIM_MONTH'])
            val = str(row[dim])
            total = totals.get(row['TIM_MONTH'], 1)
            shares.setdefault(dim, {}).setdefault(val, {})[mes] = round(float(row['total_sellers'] / total * 100), 2)
        su_data.update(shares)
    with open(su_path, 'w', encoding='utf-8') as f:
        _json_seg.dump(su_data, f, indent=2, ensure_ascii=False)
    print(f"   ✅ Universo segmentacion guardado: {su_path.name}")

if 'aprobacion_universo' in fuentes_cargadas:
    import json as _json_ap
    df_au = fuentes_cargadas.pop('aprobacion_universo')
    au_path = data_dir / f'aprobacion_universo_{site}_{mes_actual}.json'
    au_data = {}
    if 'TIM_MONTH' in df_au.columns and 'RANGO_APROBACION' in df_au.columns:
        totals = df_au.groupby('TIM_MONTH')['total_sellers'].sum()
        for _, row in df_au.iterrows():
            mes = str(row['TIM_MONTH'])
            val = str(row['RANGO_APROBACION'])
            total = totals.get(row['TIM_MONTH'], 1)
            au_data.setdefault('RANGO_APROBACION', {}).setdefault(val, {})[mes] = round(float(row['total_sellers'] / total * 100), 2)
    with open(au_path, 'w', encoding='utf-8') as f:
        _json_ap.dump(au_data, f, indent=2, ensure_ascii=False)
    print(f"   ✅ Universo aprobacion guardado: {au_path.name}")

# Pricing universo → JSON
if 'pricing_universo' in fuentes_cargadas:
    import json as _json_pr
    df_pu = fuentes_cargadas.pop('pricing_universo')
    pu_path = data_dir / f'pricing_universo_{site}_{mes_actual}.json'
    pu_data = {}
    if 'TIM_MONTH' in df_pu.columns:
        for dim_col in ['FLAG_PRICING', 'SCALE_LEVEL']:
            if dim_col not in df_pu.columns:
                continue
            totals = df_pu.groupby('TIM_MONTH')['total_sellers'].sum()
            for _, row in df_pu.iterrows():
                mes = str(row['TIM_MONTH'])
                val = str(row[dim_col])
                total = totals.get(row['TIM_MONTH'], 1)
                pu_data.setdefault(dim_col, {}).setdefault(val, {})[mes] = round(float(row['total_sellers'] / total * 100), 2)
    with open(pu_path, 'w', encoding='utf-8') as f:
        _json_pr.dump(pu_data, f, indent=2, ensure_ascii=False)
    print(f"   ✅ Universo pricing guardado: {pu_path.name}")

# Paso 7: Guardar parquet enriquecido
print("\n💾 Paso 7: Guardando datos enriquecidos...")

try:
    df_enriched.to_parquet(enriquecido_path, compression='snappy', index=False)
    size_mb = enriquecido_path.stat().st_size / 1024 / 1024
    print(f"   ✅ Guardado: {enriquecido_path.name} ({size_mb:.1f} MB)")
except Exception as e:
    print(f"   ❌ Error guardando: {e}")
    sys.exit(1)

# Guardar metadatos
metadatos = {
    "site": site,
    "mes_actual": mes_actual,
    "update_tipo": update_tipo,
    "timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
    "registros_totales": len(df_enriched),
    "fuentes_cargadas": list(fuentes_cargadas.keys()),
    "columnas_nuevas": sorted(list(columnas_nuevas)),
}

metadatos_path = data_dir / f'checkpoint2_{site}_{mes_actual}_metadatos.json'
with open(metadatos_path, 'w', encoding='utf-8') as f:
    json.dump(metadatos, f, indent=2, ensure_ascii=False)

print(f"\n" + "=" * 80)
print("✅ CHECKPOINT 2 COMPLETADO")
print("=" * 80)
print(f"   Fuentes: {', '.join(fuentes_cargadas.keys())}")
print(f"   Columnas nuevas: {len(columnas_nuevas)}")
print(f"   Archivo: {enriquecido_path.name}")
print("=" * 80)
