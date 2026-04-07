"""
Generador HTML Final — NPS Relacional Sellers v4 (4 tabs modulares).

Tabs:
  1. Resumen        — NPS headline, diagnóstico narrativo, quejas
  2. Cortes&Drivers — Dimensiones unificadas (encuesta + real) por motivo
  3. PdF            — Problemas de funcionamiento, devices, valoraciones
  4. Cuali          — Causas raíz CP5, retagueo, comentarios

Uso:
    python scripts/generar_html_final.py
"""

import sys
from pathlib import Path

script_dir = Path(__file__).parent.absolute()
project_root = script_dir.parent
sys.path.insert(0, str(project_root / "src"))
sys.path.insert(0, str(script_dir))

import json
import yaml
import pandas as pd
from datetime import datetime

from nps_model.metrics import calcular_nps_total
from nps_model.analysis.quejas import (
    calcular_variaciones_quejas_detractores,
    calcular_impacto_quejas_mensual,
    calcular_impacto_quejas_por_quarter,
)
from nps_model.analysis.razonamiento import ejecutar_razonamiento
from nps_model.analysis.updates import filtrar_por_update
from nps_model.utils.dates import (
    calcular_mes_anterior,
    calcular_mes_año_anterior,
    quarter_to_months,
    quarter_fecha_final,
    quarter_label,
)
from nps_model.utils.motivos import normalizar_motivo_col
from nps_model.analysis.tendencias import generar_lista_meses

from gen_tab1_resumen import generar_tab1
from gen_tab2_cortes import generar_tab2
from gen_tab3_pdf import generar_tab3
from gen_tab4_cuali import generar_tab4
from gen_tab5_interactivo import generar_tab5

print("\n" + "=" * 80)
print("📄 GENERADOR HTML FINAL — NPS Relacional Sellers (v4 · modular)")
print("=" * 80)

# ============================================================
# 1. Config
# ============================================================
print("\n📝 Paso 1: Cargando configuración...")
with open(project_root / "config" / "config.yaml", "r", encoding="utf-8") as f:
    config_data = yaml.safe_load(f)

site = config_data.get("sites", ["MLA"])[0]
quarter_actual = config_data.get("quarter_actual", "26Q1")
quarter_anterior = config_data.get("quarter_anterior", "25Q4")
fecha_final = quarter_fecha_final(quarter_actual)
update_tipo = config_data.get("update", {}).get("tipo", "all")

mes_anterior = calcular_mes_anterior(fecha_final)
mes_yoy = calcular_mes_año_anterior(fecha_final)

q_label_act = quarter_label(quarter_actual)
q_label_ant = quarter_label(quarter_anterior)

meses_q_actual = quarter_to_months(quarter_actual)
meses_q_anterior = quarter_to_months(quarter_anterior)

# YoY: same quarter, previous year (e.g. 26Q1 → 25Q1)
q_num = quarter_actual[-1]  # "1" from "26Q1"
q_year = int(quarter_actual[:2])  # 26 from "26Q1"
quarter_yoy = f"{q_year - 1}Q{q_num}"
meses_q_yoy = quarter_to_months(quarter_yoy)
q_label_yoy = quarter_label(quarter_yoy)

print(f"   Site: {site}, {q_label_ant} vs {q_label_act}, Update: {update_tipo}")

# ============================================================
# 2. Load checkpoints
# ============================================================
print("\n📊 Paso 2: Cargando datos de checkpoints...")

data_dir = project_root / "data"
outputs_dir = project_root / "outputs"
outputs_dir.mkdir(exist_ok=True)


def encontrar_archivo(nombre):
    if (data_dir / nombre).exists():
        return data_dir / nombre
    if (outputs_dir / nombre).exists():
        return outputs_dir / nombre
    raise FileNotFoundError(f"No se encontró {nombre}")


def cargar_json_seguro(nombre, label):
    try:
        with open(encontrar_archivo(nombre), "r", encoding="utf-8") as f:
            data = json.load(f)
        print(f"   ✅ {label} cargado")
        return data
    except FileNotFoundError:
        print(f"   ⚠️  {label} no encontrado, continuando...")
        return None


def _validate_cp_update(cp_data, cp_name):
    """Warn if checkpoint has mismatched update_tipo."""
    if not cp_data or not isinstance(cp_data, dict):
        return
    cp_ut = cp_data.get('update_tipo', cp_data.get('metadata', {}).get('update_tipo'))
    if cp_ut and cp_ut != update_tipo:
        print(f"   ⚠️  {cp_name} tiene update_tipo={cp_ut} pero corrida actual es {update_tipo} — datos pueden ser incorrectos")

checkpoint1 = cargar_json_seguro(f"checkpoint1_consolidado_{site}_{fecha_final}.json", "Checkpoint 1")
if not checkpoint1:
    print("   ❌ Checkpoint 1 es obligatorio")
    sys.exit(1)
_validate_cp_update(checkpoint1, "CP1")

checkpoint3 = cargar_json_seguro(f"checkpoint3_tendencias_anomalias_{site}_{fecha_final}.json", "Checkpoint 3")
_validate_cp_update(checkpoint3, "CP3")
checkpoint4 = cargar_json_seguro(f"checkpoint4_alertas_emergentes_{site}_{fecha_final}.json", "Checkpoint 4")
_validate_cp_update(checkpoint4, "CP4")
causas_raiz_data = cargar_json_seguro(f"checkpoint5_causas_raiz_{site}_{fecha_final}.json", "CP5 causas raíz")
if not causas_raiz_data:
    print("   ❌ CP5 (análisis cualitativo) es OBLIGATORIO. No se generará HTML incompleto.")
    print("   💡 Ejecutá el modelo completo para que Claude genere el CP5 primero.")
    sys.exit(1)
_validate_cp_update(causas_raiz_data, "CP5")
retagueo_data = cargar_json_seguro(f"checkpoint5_retagueo_{site}_{fecha_final}.json", "CP5 retagueo")
comments_variaciones_data = cargar_json_seguro(f"checkpoint5_comments_variaciones_{site}_{fecha_final}.json", "CP5 comments")
promotores_data = cargar_json_seguro(f"checkpoint5_promotores_{site}_{fecha_final}.json", "CP5 promotores")

# Load CP5 del Q anterior: try new format first (same run), then old format (separate run)
fecha_q_anterior = quarter_fecha_final(quarter_anterior)
causas_raiz_anterior = cargar_json_seguro(f"checkpoint5_causas_raiz_{site}_{fecha_final}_q_anterior.json", "CP5 causas raíz Q anterior")
if not causas_raiz_anterior:
    causas_raiz_anterior = cargar_json_seguro(f"checkpoint5_causas_raiz_{site}_{fecha_q_anterior}.json", "CP5 causas raíz Q anterior (legacy)")

tendencias_data = checkpoint3.get("tendencias", {}) if checkpoint3 else {}

# ============================================================
# 3. Load NPS data
# ============================================================
print("\n📦 Paso 3: Cargando datos de encuestas...")
try:
    df_main = pd.read_parquet(encontrar_archivo(f"datos_nps_enriquecido_{site}_{fecha_final}.parquet"))
    print("   ✅ Usando datos enriquecidos")
except FileNotFoundError:
    df_main = pd.read_parquet(encontrar_archivo(f"datos_nps_{site}_{fecha_final}.parquet"))
    print("   ✅ Usando datos base")

df_filtered = df_main[df_main["SITE"] == site].copy()

# Aplicar fecha de corte si está configurada
fecha_corte = config_data.get("fecha_corte")
if fecha_corte:
    if "END_DATE" in df_filtered.columns:
        df_filtered["END_DATE"] = pd.to_datetime(df_filtered["END_DATE"], utc=True)
        antes = len(df_filtered)
        df_filtered = df_filtered[df_filtered["END_DATE"] <= pd.Timestamp(fecha_corte, tz='UTC')]
        print(f"   📆 Fecha de corte: {fecha_corte} ({antes - len(df_filtered):,} registros excluidos)")

if update_tipo != "all":
    df_filtered = filtrar_por_update(df_filtered, update_tipo)
    print(f"   ✅ Filtro update '{update_tipo}': {len(df_filtered):,} registros")
else:
    print(f"   ✅ {len(df_filtered):,} registros")
df_filtered = normalizar_motivo_col(df_filtered)

# MODELO_DEVICE: intersección de BQ MODELO_DEVICE + DEVICE_DECLARADO (mapping analista).
# El analista filtra por MODELO_DEVICE de BQ en el pivot Y selecciona DEVICE_DECLARADO
# específicos. Solo sellers donde ambos coinciden entran al análisis PdF.
if "DEVICE_DECLARADO" in df_filtered.columns:
    _DEVICE_MAP_BY_SITE = {
        "MLB": {
            "point mini": "mPOS", "point mini com nfc": "mPOS",
            "point air": "POS", "point mini chip": "POS",
            "point pro": "POS", "point pro 2": "POS", "point 3": "POS",
            "point smart": "Smart", "point smart 2": "Smart",
            "point tap": "Tap",
        },
        "MLM": {
            "point blue": "mPOS",
            "point smart": "Smart", "point smart 2": "Smart",
        },
        "MLA": {
            "point smart": "Smart", "point smart 2": "Smart",
        },
        "MLC": {
            "point smart": "Smart", "point smart 2": "Smart",
            "smart redelcom": "Smart",
        },
    }
    _dev_map = _DEVICE_MAP_BY_SITE.get(site, {})
    import re as _re
    def _map_device_dd(val):
        if pd.isna(val):
            return None
        low = _re.sub(r'\s*\(.*', '', str(val).lower().strip()).strip()
        if low in _dev_map:
            return _dev_map[low]
        for pattern, device_type in sorted(_dev_map.items(), key=lambda x: -len(x[0])):
            if low.startswith(pattern):
                return device_type
        return None
    _dd_device = df_filtered["DEVICE_DECLARADO"].apply(_map_device_dd)
    _bq_device = df_filtered.get("MODELO_DEVICE")
    # Intersección: solo donde BQ y DD coinciden, o DD mapea y BQ es null
    if _bq_device is not None:
        df_filtered["MODELO_DEVICE"] = _dd_device.where(
            (_bq_device.isna()) | (_dd_device == _bq_device), other=None
        )
    else:
        df_filtered["MODELO_DEVICE"] = _dd_device
    _dev_count = df_filtered["MODELO_DEVICE"].notna().sum()
    print(f"   📱 MODELO_DEVICE (intersección BQ+DD): {_dev_count:,} valores")

# Tap: usa PDF_POINT_TAP en vez de PROBLEMA_FUNCIONAMIENTO.
if "PDF_POINT_TAP" in df_filtered.columns and "PROBLEMA_FUNCIONAMIENTO" in df_filtered.columns:
    _is_tap = df_filtered["MODELO_DEVICE"] == "Tap"
    _tap_count = _is_tap.sum()
    df_filtered.loc[_is_tap, "PROBLEMA_FUNCIONAMIENTO"] = df_filtered.loc[_is_tap, "PDF_POINT_TAP"]
    if "TIPO_PDF_POINT_TAP" in df_filtered.columns:
        df_filtered.loc[_is_tap, "TIPO_PROBLEMA"] = df_filtered.loc[_is_tap, "TIPO_PDF_POINT_TAP"]
    print(f"   📱 Tap PdF: usando PDF_POINT_TAP para {_tap_count:,} registros")

# Determine actual mes_actual from data (respects fecha_corte)
_meses_reales = sorted(df_filtered['END_DATE_MONTH'].unique())
_meses_reales = [m for m in _meses_reales if m <= fecha_final]
if _meses_reales:
    fecha_final_real = _meses_reales[-1]
else:
    fecha_final_real = fecha_final

# ============================================================
# 4. Calculate NPS
# ============================================================
print("\n📈 Paso 4: Calculando NPS...")
df_nps_simple = calcular_nps_total(df_filtered, group_by=["END_DATE_MONTH"])
nps_dict = df_nps_simple.set_index("END_DATE_MONTH")["NPS_score"].to_dict()

# NPS por quarter: promedio directo de todos los registros (no promedio de promedios mensuales)
# Esto coincide con cómo se reporta en la PPT
df_q_act = df_filtered[df_filtered["END_DATE_MONTH"].isin(meses_q_actual)]
df_q_ant = df_filtered[df_filtered["END_DATE_MONTH"].isin(meses_q_anterior)]
df_q_yoy = df_filtered[df_filtered["END_DATE_MONTH"].isin(meses_q_yoy)] if meses_q_yoy else pd.DataFrame()

nps_actual = df_q_act["NPS"].mean() * 100 if len(df_q_act) > 0 else 0
nps_anterior_val = df_q_ant["NPS"].mean() * 100 if len(df_q_ant) > 0 else 0
nps_yoy_val = df_q_yoy["NPS"].mean() * 100 if len(df_q_yoy) > 0 else 0
var_qvsq = nps_actual - nps_anterior_val
var_yoy = nps_actual - nps_yoy_val if len(df_q_yoy) > 0 else 0

n_encuestas = sum(len(df_filtered[df_filtered["END_DATE_MONTH"] == m]) for m in meses_q_actual)
print(f"   NPS: {nps_actual:.0f} | vs {q_label_ant}: {var_qvsq:+.1f}pp | YoY: {var_yoy:+.1f}pp")

# ============================================================
# 5. Quejas
# ============================================================
print("\n💬 Paso 5: Calculando variaciones de quejas...")
variaciones_quejas = calcular_variaciones_quejas_detractores(
    df=df_filtered, mes_actual=fecha_final_real, mes_anterior=mes_anterior, motivo_col="MOTIVO",
    meses_actual=meses_q_actual, meses_anterior=meses_q_anterior,
)

# ============================================================
# 6. Charts data
# ============================================================
print("\n📊 Paso 6: Preparando datos para gráficos...")

def fmt_mes(ms):
    nombres = {"01": "Ene", "02": "Feb", "03": "Mar", "04": "Abr", "05": "May", "06": "Jun",
               "07": "Jul", "08": "Ago", "09": "Sep", "10": "Oct", "11": "Nov", "12": "Dic"}
    return f"{nombres.get(ms[4:], ms[4:])} {ms[:4]}"

# NPS evolución por quarter (últimos 5 quarters)
from nps_model.utils.dates import parse_quarter
nps_evolucion = []
_y, _q = parse_quarter(quarter_actual)
_quarters_5 = []
for _ in range(5):
    _quarters_5.append((_y, _q))
    _q -= 1
    if _q == 0:
        _q = 4
        _y -= 1
_quarters_5.reverse()
for _yr, _qn in _quarters_5:
    _ql = f"Q{_qn} {_yr}"
    _qcode = f"{_yr % 100}Q{_qn}"
    _meses = quarter_to_months(_qcode)
    # Direct average of all records in the quarter (matches PPT methodology)
    _df_q = df_filtered[df_filtered["END_DATE_MONTH"].isin(_meses)]
    if len(_df_q) > 0:
        _nps_q = round(_df_q["NPS"].mean() * 100, 1)
        nps_evolucion.append({"mes": _ql, "nps": _nps_q})

# Quejas por quarter (últimos 5 quarters, igual que NPS)
quarters_quejas = {}
for _yr, _qn in _quarters_5:
    _ql = f"Q{_qn} {_yr}"
    _qcode = f"{_yr % 100}Q{_qn}"
    quarters_quejas[_ql] = quarter_to_months(_qcode)
impacto_q_df = calcular_impacto_quejas_por_quarter(df=df_filtered, quarters=quarters_quejas, motivo_col="MOTIVO")

# Usar quarters para motivos_top (stacked bar chart)
motivos_top = []
q_labels_ordered = [f"Q{_qn} {_yr}" for _yr, _qn in _quarters_5]
# Solo incluir quarters que tengan datos
q_labels_with_data = [ql for ql in q_labels_ordered if ql in impacto_q_df.index]
_MOTIVOS_EXCLUIR_CHART = {"Outro - Por favor, especifique", "Otro - Por favor, especifique",
                          "Sin información", "Otros motivos"}
if not impacto_q_df.empty and q_label_act in impacto_q_df.index:
    valores_actual = impacto_q_df.loc[q_label_act].sort_values(ascending=False)
    valores_actual = valores_actual[~valores_actual.index.isin(_MOTIVOS_EXCLUIR_CHART)]
    # Only include motivos with >= 0.5% share in ANY quarter (filter out tiny/promoter motivos)
    significant_motivos = set()
    for ql in q_labels_with_data:
        row = impacto_q_df.loc[ql]
        for mot, val in row.items():
            if val >= 0.5 and mot not in _MOTIVOS_EXCLUIR_CHART:
                significant_motivos.add(mot)
    # "Otros" goes last, all significant motivos included
    motivos_sin_otros = [m for m in valores_actual.index if m in significant_motivos and m != "Otros"]
    motivos_order = motivos_sin_otros + (["Otros"] if "Otros" in significant_motivos else [])
    for motivo in motivos_order:
        serie = []
        for ql in q_labels_with_data:
            serie.append({"mes": ql, "valor": round(impacto_q_df.loc[ql].get(motivo, 0), 2)})
        motivos_top.append({"motivo": motivo, "serie": serie})
meses_quejas = q_labels_with_data  # Override para que el chart use labels de quarters

# ============================================================
# 7. Reasoning engine
# ============================================================
print("\n🧠 Paso 7: Ejecutando motor de razonamiento...")
# Inyectar variaciones QvsQ en los drivers de CP1 (CP1 calcula MoM, el razonamiento necesita QvsQ)
if variaciones_quejas and "drivers" in checkpoint1:
    _var_qvsq_map = {v["motivo"]: v["var_mom"] for v in variaciones_quejas}
    for motivo, data in checkpoint1["drivers"].items():
        if motivo in _var_qvsq_map:
            data["var_quejas_qvsq"] = _var_qvsq_map[motivo]

try:
    razonamiento = ejecutar_razonamiento(
        checkpoint1_data=checkpoint1,
        checkpoint3_data=checkpoint3,
        checkpoint4_data=checkpoint4,
        checkpoint5_data=causas_raiz_data,
        df_nps=df_filtered,
        site=site,
        mes_actual=fecha_final_real,
        config=config_data,
        quarter_actual=quarter_actual,
        quarter_anterior=quarter_anterior,
    )
    print(f"   ✅ Razonamiento completado — var QvsQ: {razonamiento.get('variacion_nps_mom', 'N/A')}pp")
except Exception as e:
    print(f"   ⚠️  Error en razonamiento: {e}")
    import traceback; traceback.print_exc()
    razonamiento = {}

colores_config = config_data.get("colores_motivos", {})

# ============================================================
# 8. Generate tab sections
# ============================================================
print("\n📄 Paso 8: Generando secciones HTML por tab...")

# Tab 1
tab1_html, tab1_chart_js = generar_tab1(
    site=site,
    quarter_actual=quarter_actual,
    quarter_anterior=quarter_anterior,
    q_label_actual=q_label_act,
    q_label_anterior=q_label_ant,
    nps_actual=nps_actual,
    var_qvsq=var_qvsq,
    var_yoy=var_yoy,
    n_encuestas=n_encuestas,
    parrafo_resumen=razonamiento.get("parrafo_resumen", ""),
    nps_evolucion=nps_evolucion,
    motivos_top=motivos_top,
    meses_quejas=meses_quejas,
    variaciones_quejas=variaciones_quejas,
    tendencias_data=tendencias_data,
    causas_raiz_data=causas_raiz_data,
    razonamiento=razonamiento,
    colores_config=colores_config,
)
print("   ✅ Tab 1 (Resumen)")

# Tab 2
dimensiones_ch1 = checkpoint1.get("dimensiones", {})

# Inyectar shares del universo total en dimensiones
def _inject_universo(json_path, dim_keys, label):
    """Inyecta shares_real_por_mes desde JSON universo a dimensiones CP1."""
    if not json_path.exists():
        return
    with open(json_path, "r", encoding="utf-8") as f:
        univ = json.load(f)
    injected = 0
    for dim_key in dim_keys:
        if dim_key not in dimensiones_ch1 or dim_key not in univ:
            continue
        for item in dimensiones_ch1[dim_key]:
            val = str(item.get("dimension", ""))
            if val in univ[dim_key]:
                item["shares_real_por_mes"] = univ[dim_key][val]
                injected += 1

_inject_universo(
    data_dir / f"credits_universo_{site}_{fecha_final}.json",
    ["CREDIT_GROUP", "FLAG_USA_CREDITO", "FLAG_TARJETA_CREDITO", "ESTADO_OFERTA_CREDITO", "OFERTA_TC"],
    "Credits")
_inject_universo(
    data_dir / f"tc_limite_universo_{site}_{fecha_final}.json",
    ["OFERTA_TC", "RANGO_LIMITE_TC"],
    "TcLimite")
_inject_universo(
    data_dir / f"inversiones_universo_{site}_{fecha_final}.json",
    ["FLAG_USA_INVERSIONES", "FLAG_POTS_ACTIVO", "FLAG_INVERSIONES", "FLAG_ASSET", "FLAG_WINNER"],
    "Inversiones")
_inject_universo(
    data_dir / f"topoff_universo_{site}_{fecha_final}.json",
    ["FLAG_TOPOFF"],
    "TopOff")
_inject_universo(
    data_dir / f"segmentacion_universo_{site}_{fecha_final}.json",
    ["PRODUCTO_PRINCIPAL", "NEWBIE_LEGACY", "FLAG_ONLY_TRANSFER"],
    "Segmentacion")
_inject_universo(
    data_dir / f"aprobacion_universo_{site}_{fecha_final}.json",
    ["RANGO_APROBACION"],
    "Aprobacion")
_inject_universo(
    data_dir / f"pricing_universo_{site}_{fecha_final}.json",
    ["FLAG_PRICING", "SCALE_LEVEL"],
    "Pricing")

tab2_html = generar_tab2(
    dimensiones_ch1=dimensiones_ch1,
    razonamiento=razonamiento,
    q_label_ant=q_label_ant,
    q_label_act=q_label_act,
    q_label_yoy=q_label_yoy,
    meses_q_ant=meses_q_anterior,
    meses_q_act=meses_q_actual,
    meses_q_yoy=meses_q_yoy,
    variaciones_quejas=variaciones_quejas,
    update_tipo=update_tipo,
)
print("   ✅ Tab 2 (Cortes & Drivers)")

# Tab 3: build meses_por_q for all available quarters
all_meses = sorted(df_filtered["END_DATE_MONTH"].dropna().unique())
meses_por_q = {}
# Detect quarters from the data
for m in all_meses:
    y = int(m[:4])
    mo = int(m[4:])
    qn = (mo - 1) // 3 + 1
    qlabel = f"{y % 100}Q{qn}"
    meses_por_q.setdefault(qlabel, []).append(m)

# Shared filter dimensions for Tab 3 and Tab 4
FILTER_DIMS = [
    "MODELO_DEVICE", "PRODUCTO_PRINCIPAL", "NEWBIE_LEGACY", "FLAG_USA_CREDITO",
    "FLAG_TARJETA_CREDITO", "FLAG_USA_INVERSIONES", "FLAG_TOPOFF", "FLAG_WINNER",
    "FLAG_PRICING", "SCALE_LEVEL",
]
_available_dims = [d for d in FILTER_DIMS if d in df_filtered.columns]

tab3_html = generar_tab3(df=df_filtered, meses_por_q=meses_por_q, filter_dimensions=_available_dims)
print("   ✅ Tab 3 (PdF)")

# Tab 4: inyectar frecuencia_pct_anterior desde CP5 Q anterior
if causas_raiz_data and causas_raiz_anterior:
    from nps_model.utils.motivos import consolidar_motivo
    ant_lookup = {}
    for motivo_raw, datos_ant in (causas_raiz_anterior.get("causas_por_motivo") or {}).items():
        motivo_cons = consolidar_motivo(motivo_raw)
        for cid, causa in (datos_ant.get("causas_raiz") or {}).items():
            titulo = causa.get("titulo", "")
            freq = causa.get("frecuencia_pct", 0)
            ant_lookup.setdefault(motivo_cons, {})[titulo.lower().strip()] = freq

    for motivo, datos in (causas_raiz_data.get("causas_por_motivo") or {}).items():
        motivo_cons = consolidar_motivo(motivo)
        ant_motivo = ant_lookup.get(motivo_cons, {})
        for cid, causa in (datos.get("causas_raiz") or {}).items():
            titulo = causa.get("titulo", "").lower().strip()
            if titulo in ant_motivo:
                causa["frecuencia_pct_anterior"] = ant_motivo[titulo]

if causas_raiz_data and _available_dims:
    df_mes_cuali = df_filtered[df_filtered["END_DATE_MONTH"] == fecha_final]
    _cust_dims: dict = {}
    for _, row in df_mes_cuali.iterrows():
        cid = str(row.get("CUST_ID", ""))
        if cid and cid not in _cust_dims:
            _cust_dims[cid] = {d: str(row[d]) for d in _available_dims if pd.notna(row.get(d))}
    tagged = 0
    for _motivo, _datos in (causas_raiz_data.get("causas_por_motivo") or {}).items():
        for _cid_key, _causa in (_datos.get("causas_raiz") or {}).items():
            for _ej in _causa.get("ejemplos", []):
                _cust = str(_ej.get("cust_id", ""))
                _ej["dims"] = _cust_dims.get(_cust, {})
                if _ej["dims"]:
                    tagged += 1

tab4_html = generar_tab4(
    causas_raiz_data=causas_raiz_data,
    causas_raiz_anterior=causas_raiz_anterior,
    retagueo_data=retagueo_data,
    comments_variaciones_data=comments_variaciones_data,
    variaciones_quejas=variaciones_quejas,
    q_label_ant=q_label_ant,
    q_label_act=q_label_act,
    razonamiento=razonamiento,
    filter_dimensions=_available_dims,
    promotores_data=promotores_data,
)
print("   ✅ Tab 4 (Cualitativo)")

def _load_universo_json(path):
    if path.exists():
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}

universo_credits     = _load_universo_json(data_dir / f"credits_universo_{site}_{fecha_final}.json")
universo_inversiones = _load_universo_json(data_dir / f"inversiones_universo_{site}_{fecha_final}.json")
universo_pricing     = _load_universo_json(data_dir / f"pricing_universo_{site}_{fecha_final}.json")
universo_topoff      = _load_universo_json(data_dir / f"topoff_universo_{site}_{fecha_final}.json")
universo_tc_limite   = _load_universo_json(data_dir / f"tc_limite_universo_{site}_{fecha_final}.json")
# Merge tc_limite_universo into credits for unified access in Tab 5
universo_credits_merged = {**universo_credits, **universo_tc_limite}

tab5_html = generar_tab5(
    df=df_filtered,
    meses_por_q=meses_por_q,
    quarter_actual=quarter_actual,
    quarter_anterior=quarter_anterior,
    colores_motivos=config_data.get("colores_motivos", {}),
    universo_credits=universo_credits_merged,
    universo_inversiones=universo_inversiones,
    universo_pricing=universo_pricing,
    universo_topoff=universo_topoff,
)
print("   ✅ Tab 5 (Dashboard Interactivo)")

# ============================================================
# 9. Compose full HTML
# ============================================================
print("\n📄 Paso 9: Componiendo HTML final...")

update_badge = f" | {update_tipo}" if update_tipo != "all" else ""
mom_color = "#388e3c" if var_qvsq >= 0 else "#d32f2f"
yoy_color = "#388e3c" if var_yoy >= 0 else "#d32f2f"

html = f"""<!DOCTYPE html>
<html lang="es">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>NPS Relacional Sellers MP — {site} {q_label_act}</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.7/dist/chart.umd.min.js"></script>
<script src="https://cdn.jsdelivr.net/npm/chartjs-plugin-datalabels@2.2.0/dist/chartjs-plugin-datalabels.min.js"></script>
<script>Chart.register(ChartDataLabels); Chart.defaults.plugins.datalabels = false;</script>
<style>
:root {{
  --meli-yellow: #FFE600;
  --meli-dark: #333333;
  --bg-body: #f0f2f5;
  --bg-card: #ffffff;
  --bg-muted: #fafafa;
  --green: #28a745;
  --red: #dc3545;
  --blue: #2196f3;
  --orange: #ff9800;
  --purple: #9c27b0;
  --teal: #009688;
  --grey: #6c757d;
  --shadow: 0 2px 8px rgba(0,0,0,.08);
  --radius: 10px;
  --font: 'Segoe UI', system-ui, -apple-system, sans-serif;
}}
* {{ margin:0; padding:0; box-sizing:border-box; }}
body {{ font-family:var(--font); background:var(--bg-body); color:var(--meli-dark); line-height:1.6; }}
.container {{ max-width:1320px; margin:0 auto; padding:0 24px 48px; }}

header {{ background:var(--meli-yellow); padding:20px 0; margin-bottom:28px; box-shadow:0 2px 12px rgba(0,0,0,.1); }}
header .inner {{ max-width:1320px; margin:0 auto; padding:0 24px; display:flex; align-items:center; justify-content:space-between; }}
header h1 {{ font-size:22px; font-weight:700; }}
header .meta {{ font-size:14px; color:#555; display:flex; gap:20px; }}
header .meta span {{ font-weight:600; }}

.tabs {{ display:flex; gap:4px; border-bottom:2px solid #e0e0e0; margin-bottom:28px; overflow-x:auto; }}
.tab-btn {{ padding:12px 22px; border:none; background:transparent; font-family:var(--font); font-size:14px; font-weight:600; color:var(--grey); cursor:pointer; border-bottom:3px solid transparent; transition:all .25s; white-space:nowrap; }}
.tab-btn:hover {{ color:var(--meli-dark); }}
.tab-btn.active {{ color:var(--meli-dark); border-bottom-color:var(--meli-yellow); }}
.tab-content {{ display:none; }}
.tab-content.active {{ display:block; }}

.section {{ background:var(--bg-card); border-radius:var(--radius); box-shadow:var(--shadow); padding:28px; margin-bottom:24px; }}
.section-title {{ font-size:17px; font-weight:700; margin-bottom:18px; padding-bottom:10px; border-bottom:2px solid var(--meli-yellow); display:flex; align-items:center; gap:8px; }}

.narrative-box {{ background:var(--bg-muted); border-left:4px solid var(--meli-yellow); border-radius:6px; padding:24px 28px; font-size:15px; line-height:1.85; color:#444; text-align:justify; }}

.chart-row {{ display:grid; grid-template-columns:1fr 1fr; gap:24px; align-items:stretch; }}
@media (max-width:900px) {{ .chart-row {{ grid-template-columns:1fr; }} }}
.chart-col {{ display:flex; flex-direction:column; }}
.chart-wrap {{ position:relative; height:350px; }}

.cards-grid {{ display:grid; grid-template-columns:repeat(auto-fill,minmax(380px,1fr)); gap:18px; }}
.motivo-card {{ border-radius:var(--radius); padding:20px; border-left:5px solid; box-shadow:var(--shadow); background:var(--bg-card); }}
.motivo-card .card-header {{ display:flex; align-items:center; justify-content:space-between; margin-bottom:12px; }}
.motivo-card .card-header h4 {{ font-size:15px; font-weight:700; }}
.motivo-card .badge {{ font-size:12px; padding:3px 10px; border-radius:12px; font-weight:600; }}
.badge-up {{ background:#fce4ec; color:var(--red); }}
.badge-down {{ background:#e8f5e9; color:var(--green); }}
.badge-stable {{ background:#f5f5f5; color:var(--grey); }}
.motivo-card .driver-row {{ font-size:13px; padding:6px 0; border-bottom:1px solid #f0f0f0; display:flex; align-items:center; gap:6px; }}
.classify-tag {{ display:inline-block; font-size:11px; padding:2px 8px; border-radius:4px; font-weight:600; text-transform:uppercase; letter-spacing:.3px; }}
.classify-ok {{ background:#e8f5e9; color:#2e7d32; }}
.classify-mix {{ background:#fff3e0; color:#e65100; }}
.classify-no {{ background:#f5f5f5; color:#757575; }}
.classify-contra {{ background:#fce4ec; color:#c62828; }}
.classify-cp5 {{ background:#e3f2fd; color:#1565c0; }}
.causa-raiz {{ margin-top:10px; padding:10px 14px; background:#f8f9fa; border-radius:6px; font-size:13px; color:#555; }}
.causa-raiz strong {{ color:var(--meli-dark); }}
.card-header {{ display:flex; align-items:center; gap:8px; }}
.card-header h4 {{ margin:0; flex:1; }}
.card-detail {{ padding-top:8px; }}

.mix-section {{ margin-top:8px; }}
.mix-section h3 {{ font-size:15px; font-weight:700; margin-bottom:12px; color:var(--meli-dark); }}
table.mix-table {{ width:100%; border-collapse:collapse; font-size:13px; }}
table.mix-table th {{ background:var(--meli-yellow); font-weight:700; text-align:center; padding:10px 12px; border-bottom:2px solid #ddd; }}
table.mix-table th:first-child {{ text-align:left; }}
table.mix-table td {{ padding:10px 12px; text-align:center; border-bottom:1px solid #eee; }}
table.mix-table td:first-child {{ text-align:left; font-weight:600; }}
table.mix-table tr:hover {{ background:#fafafa; }}
.val-pos {{ color:var(--green); font-weight:600; }}
.val-neg {{ color:var(--red); font-weight:600; }}
.val-neutral {{ color:var(--grey); }}

.dim-chip {{ display:inline-block; font-size:11px; padding:3px 8px; border-radius:10px; background:#e3f2fd; color:#1565c0; font-weight:600; }}
.dim-chip.encuesta {{ background:#e8eaf6; color:#3949ab; }}
.dim-chip.real {{ background:#e8f5e9; color:#2e7d32; }}

.assoc-box {{ margin-top:14px; padding:14px 18px; border-radius:8px; border-left:4px solid; font-size:13px; }}
.assoc-box .label {{ display:flex; align-items:center; gap:8px; margin-bottom:4px; }}
.assoc-box p {{ margin:4px 0; color:#555; }}

.dim-group-header {{ font-size:15px; font-weight:700; margin-bottom:14px; display:flex; align-items:center; gap:10px; padding-bottom:8px; border-bottom:1px solid #eee; }}

.point-drill {{ margin-top:16px; padding:18px; background:#e3f2fd; border-radius:8px; border:1px solid #bbdefb; }}

.quali-card {{ border-radius:var(--radius); padding:18px 22px; margin-bottom:16px; background:var(--bg-card); box-shadow:var(--shadow); border-left:5px solid var(--blue); }}
.quali-card h4 {{ font-size:15px; margin-bottom:0; }}
.causa-detail {{ margin-bottom:2px; }}
.causa-detail summary {{ list-style:none; }}
.causa-detail summary::-webkit-details-marker {{ display:none; }}
.causa-detail[open] .comment-example {{ display:block; }}
.causa-bar-row {{ display:flex; align-items:center; gap:8px; padding:5px 0; cursor:pointer; }}
.causa-bar-row:hover {{ background:#f8f9fa; border-radius:4px; }}
.causa-bar-label {{ flex:0 0 280px; font-size:12.5px; color:#333; white-space:nowrap; overflow:hidden; text-overflow:ellipsis; }}
.causa-bar-track {{ flex:1; height:18px; background:#f0f0f0; border-radius:3px; overflow:hidden; }}
.causa-bar-fill {{ height:100%; border-radius:3px; transition:width 0.3s; }}
.causa-bar-value {{ flex:0 0 40px; text-align:right; font-size:12px; font-weight:600; color:#555; }}
.comment-example {{ margin-top:4px; padding:6px 10px; background:#f8f9fa; border-left:3px solid #ccc; font-size:11.5px; color:#666; font-style:italic; border-radius:0 4px 4px 0; }}

.text-sm {{ font-size:13px; }}
.text-muted {{ color:var(--grey); }}
.mt-16 {{ margin-top:16px; }}
.mt-24 {{ margin-top:24px; }}
</style>
</head>
<body>

<header>
  <div class="inner">
    <h1>NPS Relacional Sellers MP</h1>
    <div class="meta">
      <div>Site: <span>{site}</span></div>
      <div>Período: <span>{q_label_ant} vs {q_label_act}</span></div>
      <div>Producto: <span>{update_tipo if update_tipo != 'all' else 'Todos'}</span></div>
      <div>Encuestas: <span>{n_encuestas:,}</span></div>
    </div>
  </div>
</header>

<div class="container">

<div class="tabs">
  <button class="tab-btn active" onclick="openTab('resumen')">Resumen</button>
  <button class="tab-btn" onclick="openTab('cortes')">Cortes & Drivers</button>
  <button class="tab-btn" onclick="openTab('point')">PdF</button>
  <button class="tab-btn" onclick="openTab('cuali')">Cualitativo</button>
  <button class="tab-btn" onclick="openTab('dashboard')">Dashboard</button>
</div>

<!-- ══════════════ TAB 1 — RESUMEN ══════════════ -->
<div id="resumen" class="tab-content active">
{tab1_html}
</div>

<!-- ══════════════ TAB 2 — CORTES & DRIVERS ══════════════ -->
<div id="cortes" class="tab-content">
{tab2_html}
</div>

<!-- ══════════════ TAB 3 — PdF ══════════════ -->
<div id="point" class="tab-content">
{tab3_html}
</div>

<!-- ══════════════ TAB 4 — CUALITATIVO ══════════════ -->
<div id="cuali" class="tab-content">
{tab4_html}
</div>

<div id="dashboard" class="tab-content">
{tab5_html}
</div>

</div>

<script>
function openTab(id) {{
  document.querySelectorAll('.tab-content').forEach(t=>t.classList.remove('active'));
  document.querySelectorAll('.tab-btn').forEach(b=>b.classList.remove('active'));
  document.getElementById(id).classList.add('active');
  event.currentTarget.classList.add('active');
  // Trigger resize so Chart.js recalculates hidden charts
  setTimeout(()=>window.dispatchEvent(new Event('resize')),50);
}}

{tab1_chart_js}
</script>

<div style="text-align:center;padding:20px;color:#bbb;font-size:11px;">
  Generado: {datetime.now().strftime('%Y-%m-%d %H:%M')} | NPS Relacional Sellers v4.0{update_badge}
</div>
</body>
</html>"""

# ============================================================
# 10. Save — organized by site/update_tipo
# ============================================================
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
html_filename = f"NPSRelSellers_{site}_{fecha_final}_{timestamp}.html"

# Organize: outputs/{SITE}/{UPDATE_TYPE}/ for updates, outputs/{SITE}/cortes_especificos/ for custom
if update_tipo != "all":
    sub_dir = outputs_dir / site / update_tipo
else:
    sub_dir = outputs_dir / site / "cortes_especificos"
sub_dir.mkdir(parents=True, exist_ok=True)

html_path = sub_dir / html_filename

with open(html_path, "w", encoding="utf-8") as f:
    f.write(html)

print(f"\n✅ HTML generado: {html_filename}")
print(f"📂 Ubicación: {html_path}")

import webbrowser
import os
import subprocess

try:
    chrome = r"C:\Program Files\Google\Chrome\Application\chrome.exe"
    if os.path.exists(chrome):
        subprocess.Popen([chrome, str(html_path.resolve())])
        print("🌐 Abriendo en Chrome...")
    else:
        webbrowser.open(str(html_path.resolve()))
        print("🌐 Abriendo en navegador...")
except Exception:
    pass

print("\n" + "=" * 80)
print("✅ HTML FINAL GENERADO EXITOSAMENTE")
print("=" * 80)
