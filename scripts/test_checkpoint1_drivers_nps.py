"""
CHECKPOINT 1: Shares de Motivos y NPS por Dimensiones - NPS Sellers

Calcula:
- Shares de motivos de quejas (basados en columna MOTIVO directamente)
- NPS por ALL dimensions (base + enrichment: Producto, Newbie/Legacy, TPV/TPN, Credits, Inversiones)
- Variaciones MoM
- Descomposicion de variacion: Efecto NPS + Efecto Mix
- Update-based filtering (Point/SMBs/OP/all)

Input: datos_nps_enriquecido_{SITE}_{MES}.parquet (preferred) or datos_nps_{SITE}_{MES}.parquet
Output: checkpoint1_consolidado_{SITE}_{MES}.json
"""

import sys
from pathlib import Path

# Agregar src al path usando la ubicacion del script como referencia
script_dir = Path(__file__).parent.absolute()
project_root = script_dir.parent
sys.path.insert(0, str(project_root / 'src'))

from nps_model.analysis.drivers_nps import calcular_todos_los_drivers_shares, calcular_variaciones_motivo_shares
from nps_model.analysis.updates import filtrar_por_update, get_dimensiones_por_update
from nps_model.utils.dates import calcular_meses_atras, generar_rango_meses
from nps_model.utils.constants import DIMENSION_CONFIG_MAP
from nps_model.utils.motivos import normalizar_motivo_col
import pandas as pd
import json
import yaml

def main():
    print("="*80)
    print("CHECKPOINT 1: Shares de Motivos y Dimensiones NPS Sellers")
    print("="*80)
    
    # Leer configuracion desde config.yaml
    config_path = project_root / "config" / "config.yaml"
    if not config_path.exists():
        print("\u274c Error: No se encontro config/config.yaml")
        return
    
    with open(config_path, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)
    
    # Obtener parametros del config
    sites_config = config.get("sites", ["MLA"])
    site = sites_config[0] if sites_config else "MLA"
    quarter_actual = config.get("quarter_actual", "26Q1")
    quarter_anterior = config.get("quarter_anterior", "25Q4")
    from nps_model.utils.dates import quarter_fecha_final, quarter_label
    fecha_final = quarter_fecha_final(quarter_actual)
    
    print(f"\n\u2699\ufe0f  Configuracion leida de config.yaml:")
    print(f"   Site: {site}")
    print(f"   Comparación: {quarter_label(quarter_anterior)} vs {quarter_label(quarter_actual)}")

    # Early exit: si el output ya existe, no re-correr
    update_tipo = config.get('update', {}).get('tipo', 'all')
    output_path_check = project_root / "data" / f"checkpoint1_consolidado_{site}_{fecha_final}.json"
    if output_path_check.exists():
        # Validate update_tipo matches
        cache_valid = True
        try:
            with open(output_path_check, 'r', encoding='utf-8') as f:
                cached_data = json.load(f)
            cached_update = cached_data.get('update_tipo', 'unknown')
            if cached_update != update_tipo:
                print(f"\n   \u26a0\ufe0f  Cache INVÁLIDO: update_tipo cambió ({cached_update} → {update_tipo})")
                print(f"   \U0001f5d1\ufe0f  Eliminando cache obsoleto...")
                output_path_check.unlink(missing_ok=True)
                cache_valid = False
        except (json.JSONDecodeError, OSError):
            cache_valid = False
            output_path_check.unlink(missing_ok=True)
        if cache_valid:
            print(f"\n\u2705 CHECKPOINT 1 YA EXISTE - USANDO CACHE")
            print(f"   {output_path_check.name}")
            print(f"   \U0001f4a1 Para regenerar, elimina el archivo y vuelve a ejecutar")
            return

    # 1. Cargar datos (preferir enriquecidos si existen)
    print("\n\U0001f4ca Paso 1: Cargando datos...")
    
    enriquecido_path = project_root / 'data' / f'datos_nps_enriquecido_{site}_{fecha_final}.parquet'
    datos_nps_path = project_root / 'data' / f'datos_nps_{site}_{fecha_final}.parquet'
    
    if enriquecido_path.exists():
        datos_path = enriquecido_path
        print(f"   \u2705 Usando datos enriquecidos (checkpoint 2)")
    elif datos_nps_path.exists():
        datos_path = datos_nps_path
        print(f"   \u2705 Usando datos base (sin enriquecimiento)")
    else:
        print(f"   \u274c ERROR: No se encontraron datos NPS")
        print(f"   Ejecuta primero: python scripts/test_checkpoint0_cargar_datos.py")
        return
    
    try:
        df_encuestas = pd.read_parquet(datos_path)
        print(f"   \u2705 Datos cargados: {len(df_encuestas):,} registros")
        print(f"   Meses disponibles: {sorted(df_encuestas['END_DATE_MONTH'].unique())}")
        print(f"   Columnas disponibles: {len(df_encuestas.columns)}")
    except Exception as e:
        print(f"   \u274c Error cargando datos: {e}")
        return
    
    # 2. Filtrar por site
    df_filtered = df_encuestas[df_encuestas['SITE'] == site].copy()
    print(f"\n   \u2705 Datos filtrados para {site}: {len(df_filtered):,} registros")
    
    # Apply update-based filtering
    update_tipo = config.get('update', {}).get('tipo', 'all')
    if update_tipo != 'all':
        df_filtered = filtrar_por_update(df_filtered, update_tipo)
        print(f"   \u2705 Update filter '{update_tipo}' aplicado: {len(df_filtered):,} registros")
    
    # Override dimensiones según update tipo (ocultar dimensiones redundantes)
    dim_override = get_dimensiones_por_update(update_tipo)
    if 'dimensiones' not in config:
        config['dimensiones'] = {}
    config['dimensiones'].update(dim_override)

    # Normalizar motivos (agrupar Seguridad y Falta de seguridad en "Seguridad")
    df_filtered = normalizar_motivo_col(df_filtered)
    
    # 3. Determinar meses a analizar (ultimos 7 meses hasta fecha_final + mes YoY)
    meses_disponibles = sorted(df_filtered['END_DATE_MONTH'].unique())
    meses_disponibles = [m for m in meses_disponibles if m <= fecha_final]
    
    anno_actual = int(fecha_final[:4])
    mes_num = fecha_final[4:]
    mes_yoy = f"{anno_actual - 1}{mes_num}"
    
    meses_para_analisis = meses_disponibles[-7:] if len(meses_disponibles) >= 7 else meses_disponibles

    # Agregar todos los meses del Q YoY (no solo el último mes)
    q_yoy_num = int(mes_num) // 3  # 0-based quarter index
    if q_yoy_num == 0:
        q_yoy_num = 4
        yoy_year = anno_actual - 2
    else:
        yoy_year = anno_actual - 1
    q_yoy_start = (q_yoy_num - 1) * 3 + 1 if q_yoy_num <= 4 else 10
    meses_q_yoy = [f"{yoy_year}{q_yoy_start + i:02d}" for i in range(3)]

    for m_yoy in meses_q_yoy:
        if m_yoy in meses_disponibles and m_yoy not in meses_para_analisis:
            meses_para_analisis = [m_yoy] + meses_para_analisis
    meses_para_analisis = sorted(meses_para_analisis)
    
    mes_actual = fecha_final
    mes_anterior = meses_para_analisis[-2] if len(meses_para_analisis) >= 2 else mes_actual
    
    print(f"\n   Meses a analizar: {meses_para_analisis}")
    print(f"   Mes actual: {mes_actual}, Mes anterior: {mes_anterior}, Mes YoY: {mes_yoy}")
    
    # 4. Calcular shares de motivos (usa columna MOTIVO directamente)
    print("\n\U0001f4ca Paso 2: Calculando shares de motivos...")
    
    resultado = calcular_todos_los_drivers_shares(
        df_filtered,
        meses_para_analisis,
        motivo_col="MOTIVO"
    )
    
    # 5. Calcular variaciones MoM
    print("\n\U0001f4ca Paso 3: Calculando variaciones MoM...")
    
    for driver_name, driver_data in resultado.items():
        if driver_data and "meses" in driver_data:
            resultado[driver_name] = calcular_variaciones_motivo_shares(
                driver_data, mes_actual, mes_anterior
            )
    
    # 6. Calcular NPS por dimensiones
    print("\n\U0001f4ca Paso 4: Calculando NPS por dimensiones...")
    
    from nps_model.metrics.nps import calcular_nps_total
    from nps_model.analysis.efectos import calcular_efectos_dimension
    from nps_model.utils.dates import quarter_to_months

    meses_q_act = quarter_to_months(quarter_actual)
    meses_q_ant = quarter_to_months(quarter_anterior)
    # Filter to months actually present in data
    meses_q_act = [m for m in meses_q_act if m in meses_disponibles]
    meses_q_ant = [m for m in meses_q_ant if m in meses_disponibles]

    df_mes_actual = df_filtered[df_filtered["END_DATE_MONTH"] == mes_actual]
    df_mes_anterior = df_filtered[df_filtered["END_DATE_MONTH"] == mes_anterior]
    
    nps_total_actual = df_mes_actual["NPS"].mean() * 100 if len(df_mes_actual) > 0 else 0
    nps_total_anterior = df_mes_anterior["NPS"].mean() * 100 if len(df_mes_anterior) > 0 else 0
    
    resultado_dimensiones = {}
    
    # Build dimension list from config
    dim_config = config.get('dimensiones', {})
    dimensiones_sellers = []
    for config_key, col_name in DIMENSION_CONFIG_MAP.items():
        if dim_config.get(config_key, False):
            dim_key = col_name  # use column name as key
            dimensiones_sellers.append((col_name, dim_key))
    
    # For OP (LINK/APICOW): add POINT_FLAG as cross-product dimension
    if update_tipo in ("LINK", "APICOW") and "POINT_FLAG" in df_filtered.columns:
        # Derive readable label
        df_filtered["POINT_FLAG_LABEL"] = df_filtered["POINT_FLAG"].map(
            {1: "Con uso Point", 0: "Sin uso Point"}
        ).fillna("Sin dato")
        dimensiones_sellers.append(("POINT_FLAG_LABEL", "POINT_FLAG_LABEL"))

    print(f"   Dimensiones habilitadas: {len(dimensiones_sellers)}")
    
    for col_name, dim_key in dimensiones_sellers:
        if col_name not in df_filtered.columns:
            print(f"\n   \u26a0\ufe0f  Dimension {col_name} no disponible en datos")
            continue
        
        print(f"\n   Analizando dimension: {col_name}")
        
        df_nps_dim = calcular_nps_total(
            df_filtered,
            group_by=[col_name, "END_DATE_MONTH"]
        )
        df_nps_dim = df_nps_dim[df_nps_dim["END_DATE_MONTH"].isin(meses_para_analisis)]
        
        df_meses = df_filtered[df_filtered["END_DATE_MONTH"].isin(meses_para_analisis)].dropna(subset=[col_name])
        counts = df_meses.groupby(["END_DATE_MONTH", col_name]).size().reset_index(name="count")
        totals = df_meses.groupby("END_DATE_MONTH").size().reset_index(name="total")
        df_shares_dim = counts.merge(totals, on="END_DATE_MONTH")
        df_shares_dim["share_%"] = df_shares_dim["count"] / df_shares_dim["total"] * 100
        df_shares_dim = df_shares_dim.drop(columns=["total"])
        
        if not df_nps_dim.empty and not df_shares_dim.empty:
            df_efectos = calcular_efectos_dimension(
                df_nps_dim, df_shares_dim, col_name,
                mes_actual, mes_anterior,
                nps_total_actual, nps_total_anterior
            )
            
            nps_pivot = df_nps_dim.pivot_table(
                index=col_name, columns="END_DATE_MONTH", values="NPS_score", aggfunc="first"
            )
            shares_pivot = df_shares_dim.pivot_table(
                index=col_name, columns="END_DATE_MONTH", values="share_%", aggfunc="first"
            )
            counts_pivot = df_shares_dim.pivot_table(
                index=col_name, columns="END_DATE_MONTH", values="count", aggfunc="first"
            )
            efectos_by_val = {
                row[col_name]: row.to_dict()
                for _, row in df_efectos.iterrows()
            }
            
            dim_completo = []
            for val in df_nps_dim[col_name].unique():
                nps_mes_dict = nps_pivot.loc[val].dropna().to_dict() if val in nps_pivot.index else {}

                # Quarter-level NPS: direct average of all records (not avg of monthly avgs)
                df_val = df_filtered[df_filtered[col_name] == val]
                df_val_q_act = df_val[df_val["END_DATE_MONTH"].isin(meses_q_act)]
                df_val_q_ant = df_val[df_val["END_DATE_MONTH"].isin(meses_q_ant)]
                nps_q_act_val = df_val_q_act["NPS"].mean() * 100 if len(df_val_q_act) > 0 else None
                nps_q_ant_val = df_val_q_ant["NPS"].mean() * 100 if len(df_val_q_ant) > 0 else None

                dim_completo.append({
                    "dimension": val,
                    "nps_por_mes": nps_mes_dict,
                    "nps_q_actual": round(nps_q_act_val, 4) if nps_q_act_val is not None else None,
                    "nps_q_anterior": round(nps_q_ant_val, 4) if nps_q_ant_val is not None else None,
                    "shares_por_mes": shares_pivot.loc[val].dropna().to_dict() if val in shares_pivot.index else {},
                    "counts_por_mes": counts_pivot.loc[val].dropna().to_dict() if val in counts_pivot.index else {},
                    "efectos": efectos_by_val.get(val, {}),
                })
            
            resultado_dimensiones[dim_key] = dim_completo
            print(f"      \u2705 {len(dim_completo)} valores analizados")
    
    # 6b. Drill-down: cross-dimension NPS (Nivel 2)
    drill_down_config = config.get('drill_down', {}).get(update_tipo, {})
    cross_col = drill_down_config.get('cross_dimension')
    cross_label = drill_down_config.get('cross_label', '')
    drill_down_results = {}

    if cross_col and cross_col in df_filtered.columns:
        # For each dimension that has data, calculate NPS by (dim_value × cross_col)
        mapeo_config = config.get('mapeo_motivo_dimension', [])
        dims_to_drill = set()
        for entry in mapeo_config:
            dk = entry.get('dimension_key')
            if dk and dk in resultado_dimensiones:
                dims_to_drill.add(dk)

        for dim_key in dims_to_drill:
            dim_data = resultado_dimensiones[dim_key]
            drill_for_dim = {}

            for item in dim_data:
                dim_val = item.get("dimension")
                if dim_val is None:
                    continue
                # Filter to this dim value
                df_sub = df_filtered[df_filtered[dim_key] == dim_val].dropna(subset=[cross_col])
                if len(df_sub) < 30:
                    continue

                cross_vals = df_sub[cross_col].unique()
                cross_items = []
                for cv in cross_vals:
                    df_cv_q_act = df_sub[(df_sub[cross_col] == cv) & (df_sub["END_DATE_MONTH"].isin(meses_q_act))]
                    df_cv_q_ant = df_sub[(df_sub[cross_col] == cv) & (df_sub["END_DATE_MONTH"].isin(meses_q_ant))]
                    n_act = len(df_cv_q_act)
                    n_ant = len(df_cv_q_ant)
                    if n_act < 10:
                        continue
                    nps_act = round(df_cv_q_act["NPS"].mean() * 100, 1)
                    nps_ant = round(df_cv_q_ant["NPS"].mean() * 100, 1) if n_ant >= 10 else None
                    share_act = round(n_act / len(df_sub[df_sub["END_DATE_MONTH"].isin(meses_q_act)]) * 100, 1)
                    cross_items.append({
                        "cross_value": str(cv),
                        "nps_q_actual": nps_act,
                        "nps_q_anterior": nps_ant,
                        "nps_var": round(nps_act - nps_ant, 1) if nps_ant is not None else None,
                        "share": share_act,
                        "n": n_act,
                    })

                if cross_items:
                    # Sort by absolute NPS variation (biggest mover first)
                    cross_items.sort(key=lambda x: abs(x.get("nps_var") or 0), reverse=True)
                    drill_for_dim[str(dim_val)] = cross_items

            if drill_for_dim:
                drill_down_results[dim_key] = {
                    "cross_dimension": cross_col,
                    "cross_label": cross_label,
                    "by_value": drill_for_dim,
                }

        if drill_down_results:
            print(f"\n   📊 Drill-down calculado: {list(drill_down_results.keys())} × {cross_col}")

    # 7. Mostrar resultados
    print("\n" + "="*80)
    print("RESULTADOS: SHARES DE MOTIVOS NPS SELLERS")
    print("="*80)
    
    for driver_name, driver_data in resultado.items():
        if not driver_data:
            continue
        print(f"\n\U0001f539 {driver_data.get('driver_name', driver_name)}")
        if "share_actual" in driver_data:
            print(f"   Share: {driver_data['share_actual']:.1f}%  |  Var MoM: {driver_data.get('var_share_mom', 0):+.1f}pp")
    
    # 8. Mostrar resultados de dimensiones
    if resultado_dimensiones:
        print("\n" + "="*80)
        print("RESULTADOS: NPS POR DIMENSIONES CON EFECTOS")
        print("="*80)
        for dim_key, dim_data in resultado_dimensiones.items():
            print(f"\n {dim_key}:")
            for dim in dim_data:
                print(f"   {str(dim.get('dimension', 'N/A')):30s} | NPS actual: {dim.get('nps_por_mes', {}).get(mes_actual, 0):5.1f}")
    
    # 9. Guardar JSON
    print(f"\nGuardando resultados en JSON...")
    
    output_path = str(project_root / "data" / f"checkpoint1_consolidado_{site}_{fecha_final}.json")
    output_consolidado = {
        "site": site,
        "mes_actual": mes_actual,
        "mes_anterior": mes_anterior,
        "mes_yoy": mes_yoy,
        "update_tipo": update_tipo,
        "meses_disponibles": meses_para_analisis,
        "drivers": resultado,
        "dimensiones": resultado_dimensiones if resultado_dimensiones else {},
        "drill_down": drill_down_results if drill_down_results else {}
    }
    
    import numpy as np

    class NumpyEncoder(json.JSONEncoder):
        def default(self, obj):
            if isinstance(obj, (np.integer,)):
                return int(obj)
            if isinstance(obj, (np.floating,)):
                return float(obj)
            if isinstance(obj, np.ndarray):
                return obj.tolist()
            return super().default(obj)

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(output_consolidado, f, indent=2, ensure_ascii=False, cls=NumpyEncoder)
    print(f"   \u2705 Datos consolidados guardados en: {output_path}")
    
    print(f"\n\u2705 CHECKPOINT 1 COMPLETADO")
    print(f"\nArchivos generados:")
    print(f"   - JSON: {output_path}")

if __name__ == "__main__":
    main()
