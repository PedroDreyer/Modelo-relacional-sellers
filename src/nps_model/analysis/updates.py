"""
Update-based filtering for NPS Relacional Sellers.

Updates define different analysis views:
- SMBs: All SMB sellers that use selling tools (Point, QR, OP), all sites
- Point: All segments but only product = Point
- OP: All segments but only product = API-Cow + Link

The filtering uses PRODUCTO_PRINCIPAL (from segmentation enrichment) or
SEGMENTO_TAMANO_SELLER / E_CODE for segment filtering.
"""

import pandas as pd
from typing import Optional


def filtrar_por_update(df: pd.DataFrame, update_tipo: str) -> pd.DataFrame:
    """
    Apply update-based filter to the NPS DataFrame.

    Args:
        df: DataFrame with NPS + enrichment data
        update_tipo: One of 'Point', 'SMBs', 'OP', 'all'

    Returns:
        Filtered DataFrame
    """
    if update_tipo == "all":
        return df

    if update_tipo == "SMBs":
        return _filtrar_smbs(df)
    elif update_tipo == "Point":
        return _filtrar_point(df)
    elif update_tipo == "OP":
        return _filtrar_op(df)
    elif update_tipo == "LINK":
        return _filtrar_link(df)
    elif update_tipo == "APICOW":
        return _filtrar_apicow(df)
    else:
        print(f"   ⚠️  Update tipo '{update_tipo}' no reconocido, usando 'all'")
        return df


def _filtrar_smbs(df: pd.DataFrame) -> pd.DataFrame:
    """Filter to SMB sellers only (all products, all sites).
    Also filters by FLAG_PIX_F = 'ST' (selling tools only) if available."""
    if 'E_CODE' in df.columns:
        mask = df['E_CODE'].str.upper().str.contains('SMB', na=False)
    elif 'SEGMENTO' in df.columns:
        mask = df['SEGMENTO'].str.upper().str.contains('SMB', na=False)
    elif 'SEGMENTO_TAMANO_SELLER' in df.columns:
        mask = df['SEGMENTO_TAMANO_SELLER'].str.upper().str.contains('SMB', na=False)
    else:
        print("   ⚠️  No se encontró columna de segmento para filtrar SMBs")
        return df

    # SMBs solo incluye selling tools (FLAG_PIX_F = 'ST')
    if 'FLAG_PIX_F' in df.columns:
        mask = mask & (df['FLAG_PIX_F'] == 'ST')
        print(f"   🏷️  Filtro FLAG_PIX_F = ST aplicado (solo selling tools)")

    result = df[mask].copy()
    print(f"   🏷️  Filtro SMBs: {len(result):,} de {len(df):,} registros")
    return result


def _filtrar_point(df: pd.DataFrame) -> pd.DataFrame:
    """Filter to Point product only (all segments)."""
    if 'E_CODE' in df.columns:
        mask = df['E_CODE'].str.upper().str.contains('POINT', na=False)
    elif 'PRODUCTO_PRINCIPAL' in df.columns:
        mask = df['PRODUCTO_PRINCIPAL'].str.upper() == 'POINT'
    elif 'POINT_FLAG' in df.columns:
        mask = df['POINT_FLAG'] == 1
    else:
        print("   ⚠️  No se encontró columna de producto para filtrar Point")
        return df

    result = df[mask].copy()
    print(f"   🏷️  Filtro Point: {len(result):,} de {len(df):,} registros")
    result = _excluir_hilo_lolo(result)
    return result


def _filtrar_op(df: pd.DataFrame) -> pd.DataFrame:
    """Filter to Online Payments (API-Cow + Link) only (all segments)."""
    if 'E_CODE' in df.columns:
        mask = df['E_CODE'].str.upper().str.contains('APICOW|LINK', na=False)
    elif 'PRODUCTO_PRINCIPAL' in df.columns:
        mask = df['PRODUCTO_PRINCIPAL'].str.upper() == 'OP'
    elif 'OP_FLAG' in df.columns:
        mask = df['OP_FLAG'] == 1
    else:
        print("   ⚠️  No se encontró columna de producto para filtrar OP")
        return df

    result = df[mask].copy()
    print(f"   🏷️  Filtro OP: {len(result):,} de {len(df):,} registros")
    return result


def _filtrar_link(df: pd.DataFrame) -> pd.DataFrame:
    """Filter to Link de Pago only. Excludes hilo/lolo and restrictos."""
    if 'E_CODE' in df.columns:
        mask = df['E_CODE'].str.upper().str.contains('LINK', na=False)
    elif 'LINK_FLAG' in df.columns:
        mask = df['LINK_FLAG'] == 1
    else:
        print("   ⚠️  No se encontró columna para filtrar LINK")
        return df

    result = df[mask].copy()
    result = _excluir_hilo_lolo(result)
    result = _filtrar_consideracion_ajustada(result)
    print(f"   🏷️  Filtro LINK: {len(result):,} de {len(df):,} registros")
    return result


def _filtrar_apicow(df: pd.DataFrame) -> pd.DataFrame:
    """Filter to API/Checkout only. Excludes hilo/lolo and restrictos."""
    if 'E_CODE' in df.columns:
        mask = df['E_CODE'].str.upper().str.contains('APICOW', na=False)
    elif 'API_FLAG' in df.columns:
        mask = df['API_FLAG'] == 1
    else:
        print("   ⚠️  No se encontró columna para filtrar APICOW")
        return df

    result = df[mask].copy()
    result = _excluir_hilo_lolo(result)
    result = _filtrar_consideracion_ajustada(result)
    print(f"   🏷️  Filtro APICOW: {len(result):,} de {len(df):,} registros")
    return result


def _filtrar_consideracion_ajustada(df: pd.DataFrame) -> pd.DataFrame:
    """Filtra sellers restrictos (CONSIDERACION_AJUSTADA=0) para OP."""
    if 'CONSIDERACION_AJUSTADA' in df.columns:
        n_antes = len(df)
        df = df[df['CONSIDERACION_AJUSTADA'] == 1].copy()
        n_excluidos = n_antes - len(df)
        if n_excluidos > 0:
            print(f"   🛡️  Excluidos restrictos (CONSIDERACION_AJUSTADA=0): {n_excluidos:,} registros")
    return df


def _derivar_tamano_seller(df: pd.DataFrame) -> pd.DataFrame:
    """
    Deriva columna TAMANO_SELLER desde E_CODE y SEGMENTO_TAMANO_SELLER.
    Lógica: E_CODE tiene prioridad (contiene LONGTAIL, SMB, HILO, LOLO).
    Si E_CODE no matchea, usa SEGMENTO_TAMANO_SELLER.
    Replica exactamente la columna "Tamaño seller" del Excel de OP.
    """
    df = df.copy()
    e_code = df['E_CODE'].str.upper().fillna('') if 'E_CODE' in df.columns else pd.Series('', index=df.index)
    seg = df['SEGMENTO_TAMANO_SELLER'].fillna('') if 'SEGMENTO_TAMANO_SELLER' in df.columns else pd.Series('', index=df.index)

    tamano = pd.Series('', index=df.index)
    tamano[e_code.str.contains('LONGTAIL', na=False)] = 'Longtail'
    tamano[e_code.str.contains('_SMB', na=False)] = 'SMB'
    tamano[e_code.str.contains('LOLO', na=False)] = 'lolo'
    tamano[e_code.str.contains('HILO', na=False)] = 'hilo'

    # Fallback a SEGMENTO_TAMANO_SELLER para los que no matchearon por E_CODE
    sin_match = tamano == ''
    seg_upper = seg.str.upper()
    tamano[sin_match & (seg_upper == 'LOLO')] = 'lolo'
    tamano[sin_match & (seg_upper == 'HILO')] = 'hilo'

    df['TAMANO_SELLER'] = tamano
    return df


def _excluir_hilo_lolo(df: pd.DataFrame) -> pd.DataFrame:
    """Excluye sellers hilo y lolo de OP. Solo Longtail + SMB.
    Usa columna derivada TAMANO_SELLER que combina E_CODE + SEGMENTO_TAMANO_SELLER."""
    df = _derivar_tamano_seller(df)
    excluir = df['TAMANO_SELLER'].isin(['hilo', 'lolo'])
    n_excluidos = excluir.sum()
    if n_excluidos > 0:
        df = df[~excluir].copy()
        print(f"   🏷️  Excluidos hilo/lolo: {n_excluidos:,} registros")
    return df


def get_dimensiones_por_update(update_tipo: str) -> dict:
    """
    Returns the dimension configuration for each update type.
    Automatically enables/disables dimensions based on what makes sense
    for the selected update. Redundant dimensions are disabled.

    Args:
        update_tipo: One of 'SMBs', 'Point', 'LINK', 'APICOW', 'all'

    Returns:
        Dict with dimension_name: True/False
    """
    # Base: all dimensions enabled (for 'all' and personalizado)
    base = {
        'analizar_segmento_tamano': True,
        'analizar_segmento_crossmp': True,
        'analizar_point_device_type': True,
        'analizar_pf_pj': True,
        'analizar_e_code': False,
        'analizar_producto_principal': True,
        'analizar_newbie_legacy': True,
        'analizar_region': True,
        'analizar_rango_tpv': True,
        'analizar_rango_tpn': True,
        'analizar_uso_credito': True,
        'analizar_tarjeta_credito': True,
        'analizar_estado_oferta_credito': True,
        'analizar_credit_group': True,
        'analizar_uso_inversiones': True,
        'analizar_pots_activo': False,
        'analizar_inversiones_flag': False,
        'analizar_asset_flag': False,
        'analizar_winner_flag': True,
        'analizar_only_transfer': True,
        'analizar_topoff': True,
        'analizar_tasa_aprobacion': True,
        'analizar_pricing': True,
        'analizar_scale_level': True,
        'analizar_modelo_device': True,
        'analizar_problema_funcionamiento': True,
        'analizar_tipo_problema': True,
    }

    if update_tipo == "SMBs":
        # SMB: cross producto principal
        # Redundante: segmento_tamano (ya filtró SMBs)
        base['analizar_segmento_tamano'] = False
        base['analizar_point_device_type'] = False

    elif update_tipo == "Point":
        # Point: cross segmento
        # Redundante: producto_principal (ya filtró Point)
        base['analizar_producto_principal'] = False
        base['analizar_only_transfer'] = False

    elif update_tipo in ("LINK", "APICOW"):
        # OP: cross segmento
        # Redundante: producto_principal, device Point
        base['analizar_producto_principal'] = False
        base['analizar_point_device_type'] = False
        base['analizar_modelo_device'] = False
        base['analizar_problema_funcionamiento'] = False
        base['analizar_tipo_problema'] = False
        base['analizar_only_transfer'] = False

    return base


def generar_resumen_update(df: pd.DataFrame, update_tipo: str) -> dict:
    """
    Generate executive summary metadata for an update view.

    Args:
        df: Already-filtered DataFrame
        update_tipo: Update type applied

    Returns:
        Dict with summary info for the HTML
    """
    n_total = len(df)
    nps_mean = df['NPS'].mean() * 100 if n_total > 0 else None

    # Count by category
    n_det = len(df[df['NPS'] == -1])
    n_neu = len(df[df['NPS'] == 0])
    n_pro = len(df[df['NPS'] == 1])

    resumen = {
        'update_tipo': update_tipo,
        'total_encuestas': n_total,
        'nps_score': round(nps_mean, 1) if nps_mean is not None else None,
        'n_promotores': n_pro,
        'n_neutros': n_neu,
        'n_detractores': n_det,
        'pct_promotores': round(n_pro / n_total * 100, 1) if n_total > 0 else 0,
        'pct_neutros': round(n_neu / n_total * 100, 1) if n_total > 0 else 0,
        'pct_detractores': round(n_det / n_total * 100, 1) if n_total > 0 else 0,
    }

    # Add product distribution if available
    if 'PRODUCTO_PRINCIPAL' in df.columns:
        prod_dist = df['PRODUCTO_PRINCIPAL'].value_counts(normalize=True).to_dict()
        resumen['distribucion_productos'] = {
            k: round(v * 100, 1) for k, v in prod_dist.items()
        }

    # Add segment distribution if available
    if 'SEGMENTO_TAMANO_SELLER' in df.columns:
        seg_dist = df['SEGMENTO_TAMANO_SELLER'].value_counts(normalize=True).to_dict()
        resumen['distribucion_segmentos'] = {
            str(k): round(v * 100, 1) for k, v in seg_dist.items()
        }

    return resumen
