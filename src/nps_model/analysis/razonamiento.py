"""
Automated Reasoning Engine for NPS Relacional Sellers — v2.

Implements the 4-block logic defined in LOGICA_RECUADRO_GRIS.md:

  Bloque 1 – Variación de NPS (QvsQ)
  Bloque 2 – Explicación por Motivos de Quejas (umbrales asimétricos)
  Bloque 3 – Asociación con Drivers (4 categorías)
  Bloque 4 – Análisis por cambio de MIX (Producto, Segmento, Persona)

All thresholds are read from config.yaml → umbrales.
The motivo↔dimension mapping comes from config.yaml → mapeo_motivo_dimension.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

import pandas as pd
import yaml

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_OTROS_PATTERNS = (
    "otros", "outros", "sin información", "sem informação",
    "outros / sem informação", "otro", "otro motivo",
)


def _es_motivo_otros(motivo: str) -> bool:
    if not motivo or not isinstance(motivo, str):
        return True
    m = motivo.strip().lower()
    return m in _OTROS_PATTERNS or "otros" in m or "outro" in m


def _safe_round(val, decimals=2):
    if val is None:
        return None
    return round(val, decimals)

# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------


def ejecutar_razonamiento(
    checkpoint1_data: dict,
    checkpoint3_data: dict | None = None,
    checkpoint4_data: dict | None = None,
    checkpoint5_data: dict | None = None,
    df_nps: pd.DataFrame | None = None,
    site: str = "MLA",
    mes_actual: str = "202601",
    config: dict | None = None,
    quarter_actual: str | None = None,
    quarter_anterior: str | None = None,
) -> dict:
    """Execute the full 4-block reasoning pipeline.

    Returns a structured dict consumed by ``generar_html_final.py``.
    """

    cfg_umbrales = (config or {}).get("umbrales", {})
    UMBRAL_NPS_ESTABLE = cfg_umbrales.get("umbral_nps_estable", 1.0)
    UMBRAL_PRINCIPAL = cfg_umbrales.get("umbral_principal", 0.5)
    UMBRAL_COMPENSACION = cfg_umbrales.get("umbral_compensacion", 0.9)
    UMBRAL_DRIVER_DIM = cfg_umbrales.get("umbral_driver_dim", 0.5)
    UMBRAL_PRODUCTO = cfg_umbrales.get("umbral_producto", 0.5)
    UMBRAL_DEVICE = cfg_umbrales.get("umbral_device", 1.0)

    mapeo_config = (config or {}).get("mapeo_motivo_dimension", [])

    drivers = checkpoint1_data.get("drivers", {})
    dimensiones = checkpoint1_data.get("dimensiones", {})

    # ------------------------------------------------------------------
    # BLOQUE 1: Variación de NPS (QvsQ)
    # ------------------------------------------------------------------
    bloque1 = _bloque1_variacion_nps(
        df_nps, dimensiones, mes_actual, site,
        quarter_actual=quarter_actual, quarter_anterior=quarter_anterior,
        config=config,
    )
    var_nps = bloque1.get("var_qvsq", 0) or 0
    nps_actual = bloque1.get("nps_actual")

    # Direction: -1 = NPS down, +1 = NPS up, 0 = stable
    if var_nps <= -UMBRAL_NPS_ESTABLE:
        direccion_nps = -1
    elif var_nps >= UMBRAL_NPS_ESTABLE:
        direccion_nps = 1
    else:
        direccion_nps = 0

    # ------------------------------------------------------------------
    # BLOQUE 2: Explicación por Motivos de Quejas
    # ------------------------------------------------------------------
    bloque2 = _bloque2_motivos_quejas(
        drivers, var_nps, direccion_nps,
        UMBRAL_PRINCIPAL, UMBRAL_COMPENSACION,
    )

    # ------------------------------------------------------------------
    # BLOQUE 3: Asociación con Drivers (enrichment)
    # ------------------------------------------------------------------
    bloque3 = _bloque3_asociacion_drivers(
        bloque2, dimensiones, checkpoint5_data,
        mapeo_config, UMBRAL_DRIVER_DIM, mes_actual,
        config=config,
    )

    # ------------------------------------------------------------------
    # BLOQUE 4: Análisis MIX (Producto, Segmento, Persona)
    # ------------------------------------------------------------------
    bloque4 = _bloque4_mix(
        dimensiones, UMBRAL_PRODUCTO, UMBRAL_DEVICE,
        mes_actual, config=config,
    )

    # ------------------------------------------------------------------
    # Generate narrative paragraphs
    # ------------------------------------------------------------------
    parrafo_quejas = _generar_parrafo_quejas(bloque2, bloque3, direccion_nps, checkpoint5_data)
    parrafo_mix = _generar_parrafo_mix(bloque4, bloque2, direccion_nps)
    parrafo_resumen = _generar_parrafo_resumen(
        bloque1, bloque2, bloque3, bloque4,
        parrafo_quejas, parrafo_mix,
        direccion_nps, site, quarter_actual,
    )

    return {
        "site": site,
        "mes_actual": mes_actual,
        "quarter_actual": quarter_actual,
        "quarter_anterior": quarter_anterior,
        "bloque1": bloque1,
        "bloque2": bloque2,
        "bloque3": bloque3,
        "bloque4": bloque4,
        "parrafo_quejas": parrafo_quejas,
        "parrafo_mix": parrafo_mix,
        "parrafo_resumen": parrafo_resumen,
        # Legacy keys consumed by old HTML (kept for backward compat)
        "variacion_nps_mom": _safe_round(var_nps),
        "explicacion_principal": _legacy_explicacion(bloque2),
        "nivel_1_quejas": _legacy_nivel1(bloque2),
        "nivel_2_productos": _legacy_nivel2(bloque4),
        "nivel_3_funcionamiento": _legacy_nivel3(bloque4),
        "cross_validation": {},
    }


# ===================================================================
# BLOQUE 1 — Variación de NPS
# ===================================================================

def _bloque1_variacion_nps(
    df_nps: pd.DataFrame | None,
    dimensiones: dict,
    mes_actual: str,
    site: str,
    quarter_actual: str | None = None,
    quarter_anterior: str | None = None,
    config: dict | None = None,
) -> dict:
    """Calculate QvsQ and YoY NPS."""
    from nps_model.metrics import calcular_nps_total
    from nps_model.utils.dates import (
        quarter_to_months, quarter_label,
        calcular_mes_año_anterior,
    )

    result: Dict[str, Any] = {
        "nps_actual": None,
        "nps_anterior": None,
        "var_qvsq": None,
        "var_yoy": None,
        "n_encuestas": 0,
        "label_actual": "",
        "label_anterior": "",
    }

    if df_nps is None or len(df_nps) == 0:
        return result

    nps_by_month = calcular_nps_total(df_nps, group_by=["END_DATE_MONTH"])
    nps_dict = nps_by_month.set_index("END_DATE_MONTH")["NPS_score"].to_dict()

    if quarter_actual and quarter_anterior:
        meses_act = quarter_to_months(quarter_actual)
        meses_ant = quarter_to_months(quarter_anterior)
        # Direct average of all records in the quarter (not average of monthly averages)
        # This matches generar_html_final.py and the PPT methodology
        df_q_act = df_nps[df_nps["END_DATE_MONTH"].isin(meses_act)]
        df_q_ant = df_nps[df_nps["END_DATE_MONTH"].isin(meses_ant)]
        nps_actual = df_q_act["NPS"].mean() * 100 if len(df_q_act) > 0 else None
        nps_anterior = df_q_ant["NPS"].mean() * 100 if len(df_q_ant) > 0 else None
        result["label_actual"] = quarter_label(quarter_actual)
        result["label_anterior"] = quarter_label(quarter_anterior)
        result["n_encuestas"] = len(df_q_act)
    else:
        from nps_model.utils.dates import calcular_mes_anterior
        mes_ant = calcular_mes_anterior(mes_actual)
        nps_actual = nps_dict.get(mes_actual)
        nps_anterior = nps_dict.get(mes_ant)
        result["n_encuestas"] = len(df_nps[df_nps["END_DATE_MONTH"] == mes_actual])

    result["nps_actual"] = _safe_round(nps_actual, 1)
    result["nps_anterior"] = _safe_round(nps_anterior, 1)

    if nps_actual is not None and nps_anterior is not None:
        result["var_qvsq"] = _safe_round(nps_actual - nps_anterior, 2)

    # YoY
    mes_yoy = calcular_mes_año_anterior(mes_actual)
    nps_yoy = nps_dict.get(mes_yoy)
    if nps_actual is not None and nps_yoy is not None:
        result["var_yoy"] = _safe_round(nps_actual - nps_yoy, 2)

    return result


# ===================================================================
# BLOQUE 2 — Motivos de Quejas
# ===================================================================

def _bloque2_motivos_quejas(
    drivers: dict,
    var_nps: float,
    direccion_nps: int,
    umbral_principal: float,
    umbral_compensacion: float,
) -> dict:
    """Identify principal and compensating complaint motivos."""

    todos: List[dict] = []
    for motivo, data in drivers.items():
        if _es_motivo_otros(motivo):
            continue
        # Preferir QvsQ (inyectado por generar_html_final) sobre MoM (de CP1)
        var_share = data.get("var_quejas_qvsq", data.get("var_quejas_mom", data.get("var_share_mom")))
        if var_share is None:
            continue
        todos.append({
            "motivo": motivo,
            "var_share": round(var_share, 2),
            "share_actual": round(data.get("share_actual", 0), 2),
            "share_anterior": round(data.get("share_anterior", data.get("quejas_anterior", 0)), 2),
        })

    principales: List[dict] = []
    compensaciones: List[dict] = []

    if direccion_nps < 0:
        # NPS bajó → principales = motivos que SUBEN ≥ umbral_principal
        principales = sorted(
            [m for m in todos if m["var_share"] >= umbral_principal],
            key=lambda x: x["var_share"], reverse=True,
        )
        compensaciones = sorted(
            [m for m in todos if m["var_share"] <= -umbral_compensacion],
            key=lambda x: x["var_share"],
        )
    elif direccion_nps > 0:
        # NPS subió → principales = motivos que BAJAN ≥ umbral_principal
        principales = sorted(
            [m for m in todos if m["var_share"] <= -umbral_principal],
            key=lambda x: x["var_share"],
        )
        compensaciones = sorted(
            [m for m in todos if m["var_share"] >= umbral_principal],
            key=lambda x: x["var_share"], reverse=True,
        )
    else:
        # NPS estable → todos los que varían ≥ umbral_principal en cualquier dirección
        principales = sorted(
            [m for m in todos if abs(m["var_share"]) >= umbral_principal],
            key=lambda x: abs(x["var_share"]), reverse=True,
        )

    return {
        "direccion_nps": direccion_nps,
        "principales": principales,
        "compensaciones": compensaciones,
        "todos": sorted(todos, key=lambda x: abs(x["var_share"]), reverse=True),
    }


# ===================================================================
# BLOQUE 3 — Asociación con Drivers (enrichment)
# ===================================================================

_CLASIFICACION_PRIORIDAD = {
    "EXPLICA_OK": 1,
    "EXPLICA_MIX": 2,
    "NO_EXPLICA": 3,
    "CONTRADICTORIO": 4,
}


def _bloque3_asociacion_drivers(
    bloque2: dict,
    dimensiones: dict,
    checkpoint5_data: dict | None,
    mapeo_config: list,
    umbral_dim: float,
    mes_actual: str,
    config: dict | None = None,
) -> dict:
    """For each relevant motivo, classify enrichment association."""

    motivos_relevantes = bloque2["principales"] + bloque2["compensaciones"]
    asociaciones: List[dict] = []
    dims_usadas: set = set()

    for mot in motivos_relevantes:
        motivo = mot["motivo"]
        var_share = mot["var_share"]

        dim_match = _find_dimension_match(motivo, mapeo_config)

        if dim_match is None:
            causa_raiz = _get_causa_raiz_cp5(motivo, checkpoint5_data)
            asociaciones.append({
                "motivo": motivo,
                "var_share": var_share,
                "clasificacion": "FALLBACK_CP5",
                "dimension_key": None,
                "descripcion_dim": None,
                "causa_raiz": causa_raiz,
                "wording": _wording_fallback(motivo, var_share, causa_raiz),
            })
            continue

        dim_key = dim_match["dimension_key"]
        desc = dim_match.get("descripcion", dim_key)
        use_fallback = dim_match.get("fallback_cp5", False)

        dim_data = dimensiones.get(dim_key, [])

        if not dim_data or dim_key in dims_usadas:
            if use_fallback or not dim_data:
                causa_raiz = _get_causa_raiz_cp5(motivo, checkpoint5_data)
                asociaciones.append({
                    "motivo": motivo,
                    "var_share": var_share,
                    "clasificacion": "FALLBACK_CP5",
                    "dimension_key": dim_key,
                    "descripcion_dim": desc,
                    "causa_raiz": causa_raiz,
                    "wording": _wording_fallback(motivo, var_share, causa_raiz),
                })
                continue

        relacion_inversa = dim_match.get("relacion_inversa", False)
        clasificacion, detalle = _clasificar_asociacion(
            var_share, dim_data, umbral_dim, mes_actual, desc,
            relacion_inversa=relacion_inversa,
        )

        dims_usadas.add(dim_key)

        # Always fetch CP5 causa raíz to enrich wording with user voice
        causa_raiz = _get_causa_raiz_cp5(motivo, checkpoint5_data)

        asociaciones.append({
            "motivo": motivo,
            "var_share": var_share,
            "clasificacion": clasificacion,
            "dimension_key": dim_key,
            "descripcion_dim": desc,
            "detalle": detalle,
            "causa_raiz": causa_raiz,
            "wording": _generar_wording(clasificacion, motivo, var_share, desc, detalle, causa_raiz),
        })

    return {"asociaciones": asociaciones}


def _find_dimension_match(motivo: str, mapeo_config: list) -> dict | None:
    import unicodedata
    def _strip_accents(s):
        return "".join(c for c in unicodedata.normalize("NFD", s) if unicodedata.category(c) != "Mn")
    motivo_norm = _strip_accents((motivo or "").lower())
    for entry in mapeo_config:
        patrones = entry.get("patrones", [])
        if any(_strip_accents(p) in motivo_norm for p in patrones):
            return entry
    return None


def _clasificar_asociacion(
    var_motivo: float,
    dim_data: list,
    umbral: float,
    mes_actual: str,
    desc: str,
    relacion_inversa: bool = False,
) -> tuple[str, dict]:
    """Classify driver association and find the sub-group with most NPS movement.

    Uses quarter-level NPS (nps_q_actual/nps_q_anterior) when available for
    more accurate sub-group identification. Falls back to monthly shares.
    """
    from nps_model.utils.dates import calcular_mes_anterior
    mes_anterior = calcular_mes_anterior(mes_actual)

    MIN_SHARE_PCT = 10.0  # Ignore sub-groups with <10% share (n too small for reliable NPS)

    # Compute NPS and share data for each sub-group
    candidates = []
    for item in dim_data:
        nps_act = item.get("nps_q_actual")
        nps_ant = item.get("nps_q_anterior")
        shares = item.get("shares_por_mes", {})
        sh_vals = [shares.get(m) for m in [mes_actual, mes_anterior] if shares.get(m) is not None]
        share_act = sh_vals[0] if sh_vals else None
        sh_act = shares.get(mes_actual)
        sh_ant = shares.get(mes_anterior)
        share_var = (sh_act - sh_ant) if (sh_act is not None and sh_ant is not None) else 0

        if share_act is not None and share_act < MIN_SHARE_PCT:
            continue  # Skip tiny sub-groups

        candidates.append({
            "item": item,
            "nps_act": nps_act,
            "nps_ant": nps_ant,
            "nps_var": (nps_act - nps_ant) if (nps_act is not None and nps_ant is not None) else 0,
            "share_act": share_act,
            "share_var": share_var,
            "dimension": item.get("dimension", ""),
        })

    # Find best sub-group: prefer the "positive" sub-group for binary dimensions
    # (e.g., "Usa inversiones" over "No usa inversiones" for motivo "Inversiones")
    # For binary dimensions, the "positive" is the one WITHOUT "no " / "sin " prefix
    NEGATIVE_PREFIXES = ("no ", "sin ", "0")
    def _is_positive(dim_name):
        low = str(dim_name).lower().strip()
        return not any(low.startswith(p) for p in NEGATIVE_PREFIXES)

    # Separate positive and negative candidates
    positive_candidates = [c for c in candidates if _is_positive(c["dimension"])]
    negative_candidates = [c for c in candidates if not _is_positive(c["dimension"])]

    # Find best among positive first, then fallback to all
    best_nps_item = None
    best_nps_var = 0.0
    max_share_var = 0.0
    best_share_item = None

    search_order = positive_candidates if positive_candidates else candidates
    for c in search_order:
        if abs(c["nps_var"]) > abs(best_nps_var):
            best_nps_var = c["nps_var"]
            best_nps_item = c
        if abs(c["share_var"]) > abs(max_share_var):
            max_share_var = c["share_var"]
            best_share_item = c

    # If no positive had movement, check negatives too
    if best_nps_item is None and negative_candidates:
        for c in negative_candidates:
            if abs(c["nps_var"]) > abs(best_nps_var):
                best_nps_var = c["nps_var"]
                best_nps_item = c
    if best_share_item is None:
        for c in candidates:
            if abs(c["share_var"]) > abs(max_share_var):
                max_share_var = c["share_var"]
                best_share_item = c

    best = best_nps_item or best_share_item
    max_var = max_share_var

    detalle = {
        "var_dimension": _safe_round(max_var),
        "item": best["dimension"] if best else "",
        "relacion_inversa": relacion_inversa,
    }

    if best and best.get("nps_act") is not None and best.get("nps_ant") is not None:
        detalle["nps_subgrupo_act"] = _safe_round(best["nps_act"])
        detalle["nps_subgrupo_ant"] = _safe_round(best["nps_ant"])
        detalle["nps_subgrupo_var"] = _safe_round(best["nps_var"])
        detalle["share_subgrupo"] = _safe_round(best["share_act"]) if best["share_act"] is not None else None
        detalle["subgrupo_name"] = best["dimension"]
        # Share anterior for wording (share_act - share_var)
        if best["share_act"] is not None:
            detalle["share_subgrupo_ant"] = _safe_round(best["share_act"] - best["share_var"])
            detalle["share_subgrupo_var"] = _safe_round(best["share_var"])
            detalle["aporte_pp"] = _safe_round(best["nps_var"] * best["share_act"] / 100)

    motivo_moves = abs(var_motivo) >= umbral

    # Use NPS QvsQ of sub-group for classification (more reliable than share MoM)
    nps_var = best_nps_var if best_nps_item else 0
    # dim_moves = either share moved OR NPS of sub-group moved significantly
    dim_moves = abs(max_var) >= umbral or abs(nps_var) >= 2.0

    # Direction: use NPS of sub-group (more meaningful than share)
    effective_var = nps_var if abs(nps_var) >= 2.0 else max_var
    same_direction = (var_motivo > 0 and effective_var > 0) or (var_motivo < 0 and effective_var < 0)
    opposite_direction = (var_motivo > 0 and effective_var < 0) or (var_motivo < 0 and effective_var > 0)

    if relacion_inversa:
        same_direction, opposite_direction = opposite_direction, same_direction

    if motivo_moves and dim_moves and same_direction:
        return "EXPLICA_OK", detalle
    if motivo_moves and not dim_moves:
        return "EXPLICA_MIX", detalle
    if motivo_moves and dim_moves and opposite_direction:
        return "CONTRADICTORIO", detalle
    return "NO_EXPLICA", detalle


def _get_causa_raiz_cp5(motivo: str, cp5: dict | None) -> str:
    if not cp5:
        return ""
    causas_por_motivo = cp5.get("causas_por_motivo", {})

    # 1. Buscar por nombre exacto
    causas = causas_por_motivo.get(motivo, {})
    if causas:
        causas_raiz = causas.get("causas_raiz", {})
        if causas_raiz:
            return list(causas_raiz.values())[0].get("titulo", "")

    # 2. Buscar por consolidación: los nombres en CP5 pueden ser raw (portugués)
    #    y el motivo buscado está consolidado (español). Usar los mismos patrones.
    from nps_model.utils.motivos import consolidar_motivo
    for cp5_motivo, cp5_causas in causas_por_motivo.items():
        if consolidar_motivo(cp5_motivo) == motivo:
            causas_raiz = cp5_causas.get("causas_raiz", {})
            if causas_raiz:
                return list(causas_raiz.values())[0].get("titulo", "")

    return ""


def _generar_wording(
    clasif: str, motivo: str, var: float, desc: str, detalle: dict,
    causa_raiz: str = "",
) -> str:
    dir_motivo = "aumento" if var > 0 else "mejora"
    # Suffix with user voice from CP5 when available
    voz_usuario = f". Usuarios mencionan: {causa_raiz}" if causa_raiz else ""
    var_dim = detalle.get("var_dimension", 0) or 0
    item_name = detalle.get("item", "")
    inversa = detalle.get("relacion_inversa", False)

    # Rich sub-group detail when available
    subgrupo = detalle.get("subgrupo_name", "")
    nps_ant = detalle.get("nps_subgrupo_ant")
    nps_act = detalle.get("nps_subgrupo_act")
    nps_var = detalle.get("nps_subgrupo_var")
    share = detalle.get("share_subgrupo")
    aporte = detalle.get("aporte_pp")
    has_rich = nps_ant is not None and nps_act is not None and share is not None

    share_ant_sg = detalle.get("share_subgrupo_ant")
    share_var_sg = detalle.get("share_subgrupo_var")

    if clasif == "EXPLICA_OK":
        if has_rich and abs(nps_var or 0) >= 1:
            # Build detail parts
            nps_part = f"NPS de {subgrupo} pasó de {nps_ant:.0f} a {nps_act:.0f} ({nps_var:+.0f}pp)"
            share_part = ""
            if share_ant_sg is not None and share_var_sg is not None and abs(share_var_sg) >= 0.5:
                dir_share = "creció" if share_var_sg > 0 else "cayó"
                share_part = f", participación {dir_share} de {share_ant_sg:.0f}% a {share:.0f}%"
            elif share is not None:
                share_part = f" ({share:.0f}% del total)"

            return (
                f"{dir_motivo} de quejas de {motivo} ({var:+.1f}pp QvsQ): "
                f"{nps_part}{share_part}{voz_usuario}"
            )
        # Fallback without rich data
        if inversa:
            dir_driver = "reducción" if var_dim < 0 else "aumento"
            return (
                f"{dir_motivo} de quejas de {motivo} ({var:+.1f}pp QvsQ) "
                f"explicado por {dir_driver} en {desc} ({var_dim:+.1f}pp en {item_name}){voz_usuario}"
            )
        return (
            f"{dir_motivo} en {motivo} ({var:+.1f}pp QvsQ) "
            f"relacionado con cambio en {desc} ({var_dim:+.1f}pp en {item_name}){voz_usuario}"
        )
    if clasif == "EXPLICA_MIX":
        return (
            f"{dir_motivo} en {motivo} ({var:+.1f}pp QvsQ) "
            f"no explicado por {desc} (se mantiene estable){voz_usuario}"
        )
    if clasif == "NO_EXPLICA":
        return (
            f"{dir_motivo} en {motivo} ({var:+.1f}pp QvsQ) "
            f"sin variación significativa en {desc}{voz_usuario}"
        )
    if clasif == "CONTRADICTORIO":
        if has_rich and abs(nps_var or 0) >= 1:
            dir_dim = "mejoró" if nps_var > 0 else "empeoró"
            share_ctx = ""
            if share_ant_sg is not None and share_var_sg is not None and abs(share_var_sg) >= 0.5:
                dir_sh = "creció" if share_var_sg > 0 else "cayó"
                share_ctx = f", participación {dir_sh} de {share_ant_sg:.0f}% a {share:.0f}%"
            else:
                share_ctx = f", {share:.0f}% del total"
            return (
                f"{dir_motivo} de quejas de {motivo} ({var:+.1f}pp QvsQ) "
                f"a pesar de que {subgrupo} {dir_dim} (NPS {nps_ant:.0f}→{nps_act:.0f}{share_ctx}){voz_usuario}"
            )
        if inversa:
            dir_dim = "aumentó" if var_dim > 0 else "se redujo"
            return (
                f"{dir_motivo} en {motivo} ({var:+.1f}pp QvsQ) "
                f"a pesar de que {desc} {dir_dim} ({var_dim:+.1f}pp){voz_usuario}"
            )
        dir_dim = "mejoró" if var_dim < 0 else "empeoró"
        return (
            f"{dir_motivo} en {motivo} ({var:+.1f}pp QvsQ) "
            f"a pesar de que {desc} {dir_dim} ({var_dim:+.1f}pp){voz_usuario}"
        )
    return f"{dir_motivo} de quejas de {motivo} ({var:+.1f}pp QvsQ){voz_usuario}"


def _wording_fallback(motivo: str, var: float, causa_raiz: str) -> str:
    dir_motivo = "aumento" if var > 0 else "disminución"
    base = f"{dir_motivo} de quejas de {motivo} ({var:+.1f}pp QvsQ)"
    if causa_raiz:
        return f"{base} donde los sellers reportan {causa_raiz}"
    return base


# ===================================================================
# BLOQUE 4 — MIX (Producto, Segmento, Persona)
# ===================================================================

def _bloque4_mix(
    dimensiones: dict,
    umbral_producto: float,
    umbral_device: float,
    mes_actual: str,
    config: dict | None = None,
) -> dict:
    """Decompose NPS change into MIX and NPS effects."""
    from nps_model.utils.dates import calcular_mes_anterior
    mes_anterior = calcular_mes_anterior(mes_actual)

    filtros = (config or {}).get("filtros", {})
    producto_filtro = filtros.get("producto", [])
    segmento_filtro = filtros.get("e_code", [])
    single_product = len(producto_filtro) == 1
    single_segment = len(segmento_filtro) == 1

    tablas: Dict[str, list] = {}

    if not single_product:
        tablas["producto"] = _calcular_mix_tabla(
            dimensiones.get("PRODUCTO_PRINCIPAL", []),
            mes_actual, mes_anterior, "Producto",
        )

    if not single_segment:
        tablas["segmento"] = _calcular_mix_tabla(
            dimensiones.get("E_CODE", dimensiones.get("SEGMENTO_TAMANO_SELLER", [])),
            mes_actual, mes_anterior, "Segmento",
        )

    tablas["persona"] = _calcular_mix_tabla(
        dimensiones.get("PF_PJ", []),
        mes_actual, mes_anterior, "Persona",
    )

    tablas["newbie_legacy"] = _calcular_mix_tabla(
        dimensiones.get("NEWBIE_LEGACY", []),
        mes_actual, mes_anterior, "Newbie/Legacy",
    )

    # Point device drill-down
    point_devices: list = []
    producto_items = tablas.get("producto", [])
    point_driver = any(
        "point" in (it.get("nombre", "")).lower()
        and abs(it.get("efecto_neto", 0)) >= umbral_producto
        for it in producto_items
    )
    if point_driver:
        point_devices = _calcular_mix_tabla(
            dimensiones.get("POINT_DEVICE_TYPE", []),
            mes_actual, mes_anterior, "Device",
        )

    return {
        "tablas": tablas,
        "point_devices": point_devices,
        "single_product": single_product,
        "single_segment": single_segment,
    }


def _calcular_mix_tabla(
    dim_list: list, mes_actual: str, mes_anterior: str, label: str,
) -> list:
    """Build MIX decomposition table for a dimension."""
    rows = []
    for item in dim_list:
        nombre = item.get("dimension", "")
        nps_mes = item.get("nps_por_mes", {})
        shares = item.get("shares_por_mes", {})
        efectos = item.get("efectos", {})

        nps_act = nps_mes.get(mes_actual)
        nps_ant = nps_mes.get(mes_anterior)
        sh_act = shares.get(mes_actual)
        sh_ant = shares.get(mes_anterior)
        var_nps = (nps_act - nps_ant) if (nps_act is not None and nps_ant is not None) else None
        var_share = (sh_act - sh_ant) if (sh_act is not None and sh_ant is not None) else None

        e_nps = efectos.get("Efecto_NPS")
        e_mix = efectos.get("Efecto_MIX")
        e_neto = efectos.get("Efecto_NETO")

        rows.append({
            "nombre": nombre,
            "nps_actual": _safe_round(nps_act, 1),
            "nps_anterior": _safe_round(nps_ant, 1),
            "var_nps": _safe_round(var_nps),
            "share_actual": _safe_round(sh_act, 1),
            "share_anterior": _safe_round(sh_ant, 1),
            "var_share": _safe_round(var_share),
            "efecto_nps": _safe_round(e_nps),
            "efecto_mix": _safe_round(e_mix),
            "efecto_neto": _safe_round(e_neto),
            "label": label,
        })

    rows.sort(key=lambda x: abs(x.get("efecto_neto") or 0), reverse=True)
    return rows


# ===================================================================
# Narrative generation
# ===================================================================

def _generar_parrafo_quejas(bloque2: dict, bloque3: dict, direccion_nps: int, cp5_data: dict | None = None) -> str:
    asociaciones = bloque3.get("asociaciones", [])
    principales = bloque2.get("principales", [])
    compensaciones = bloque2.get("compensaciones", [])

    if not principales and not compensaciones:
        return "No se identificaron variaciones significativas en los motivos de queja."

    # Solo top 3 motivos principales y top 2 compensaciones (sintético)
    MAX_PRINCIPALES = 3
    MAX_COMPENSACIONES = 2

    # Index asociaciones by motivo for driver lookup
    asoc_by_mot = {}
    for a in asociaciones:
        asoc_by_mot.setdefault(a.get("motivo", ""), []).append(a)

    def _motivo_corto(motivo_dict):
        """Genera texto corto con causa raíz de CP5 o driver si EXPLICA_OK."""
        mot = motivo_dict["motivo"]
        var = motivo_dict.get("var_share", 0)
        txt = f"<b>{mot}</b> ({var:+.1f}pp)"

        # 1. Buscar en asociaciones del bloque3 — usar detalle rico si disponible
        for a in asoc_by_mot.get(mot, []):
            if a.get("clasificacion") == "EXPLICA_OK":
                det = a.get("detalle", {})
                subgrupo = det.get("subgrupo_name", "")
                nps_ant = det.get("nps_subgrupo_ant")
                nps_act = det.get("nps_subgrupo_act")
                share = det.get("share_subgrupo")
                if nps_ant is not None and nps_act is not None and share is not None and abs(nps_act - nps_ant) >= 1:
                    nps_var = nps_act - nps_ant
                    txt += (f" — {a.get('descripcion_dim', '')} "
                            f"({nps_var:+.0f}pp NPS en {subgrupo}, "
                            f"representan {share:.0f}% del total)")
                else:
                    txt += f" — {a.get('descripcion_dim', '')}"
                return txt

        # 2. Buscar causa raíz de CP5 (análisis cualitativo)
        causa = _get_causa_raiz_cp5(mot, cp5_data)
        if causa:
            txt += f" — sellers reportan: <i>{causa}</i>"
            return txt

        return txt

    # Principales
    tops = principales[:MAX_PRINCIPALES]
    partes = [_motivo_corto(m) for m in tops]

    texto = ""
    if direccion_nps < 0:
        texto = "Esta caída se explica principalmente por aumento de quejas de "
    elif direccion_nps > 0:
        texto = "Esta suba se explica principalmente por disminución de quejas de "
    else:
        texto = "Los principales movimientos: "

    texto += "; ".join(partes) + "."

    # Compensaciones (breve)
    comps = compensaciones[:MAX_COMPENSACIONES]
    if comps:
        comp_partes = [_motivo_corto(m) for m in comps]
        if direccion_nps < 0:
            texto += " Compensado parcialmente por mejoras en "
        elif direccion_nps > 0:
            texto += " Sin embargo, se observan deterioros en "
        else:
            texto += " Compensaciones: "
        texto += "; ".join(comp_partes) + "."

    return texto


def _generar_parrafo_mix(bloque4: dict, bloque2: dict, direccion_nps: int) -> str:
    tablas = bloque4.get("tablas", {})
    producto_rows = tablas.get("producto", [])

    # Skip product mix when update already filters by single product (redundant)
    if bloque4.get("single_product"):
        return ""

    if not producto_rows:
        return ""

    top = producto_rows[0]
    efecto_neto = top.get("efecto_neto")
    if efecto_neto is None or abs(efecto_neto) < 0.3:
        return ""

    nombre = top["nombre"]
    e_mix = top.get("efecto_mix") or 0
    e_nps = top.get("efecto_nps") or 0
    var_share = top.get("var_share")

    dir_mix = "caída" if efecto_neto < 0 else "suba"
    texto = (
        f"El producto que más aportó a esta {dir_mix} fue {nombre} "
        f"({efecto_neto:+.1f}pp): "
        f"{e_mix:+.1f}pp por efecto mix"
    )
    if var_share is not None and abs(var_share) >= 0.1:
        texto += f" (su share {'bajó' if var_share < 0 else 'subió'} {abs(var_share):.1f}pp)"
    texto += f" y {e_nps:+.1f}pp por efecto NPS"

    # Point drill-down
    point_devices = bloque4.get("point_devices", [])
    if point_devices and "point" in nombre.lower():
        top_dev = point_devices[0]
        dev_name = top_dev.get("nombre", "")
        dev_var = top_dev.get("var_nps") or 0
        if abs(dev_var) >= 0.5:
            texto += (
                f" (se observa variación en dispositivos {dev_name} "
                f"{dev_var:+.1f}pp)"
            )

    texto += "."
    return texto


def _generar_parrafo_resumen(
    bloque1, bloque2, bloque3, bloque4,
    parrafo_quejas, parrafo_mix,
    direccion_nps, site, quarter_actual,
) -> str:
    nps_actual = bloque1.get("nps_actual")
    var_qvsq = bloque1.get("var_qvsq")
    var_yoy = bloque1.get("var_yoy")
    label_actual = bloque1.get("label_actual", "")

    if nps_actual is None:
        return "Datos insuficientes para generar resumen ejecutivo."

    var_qvsq_str = f"{var_qvsq:+.1f}pp QvsQ" if var_qvsq is not None else ""
    var_yoy_str = f" / {var_yoy:+.1f}pp YoY" if var_yoy is not None else ""
    label = label_actual or quarter_actual or ""

    texto = (
        f"En {label}, el NPS de {site} alcanzó {nps_actual:.0f}p.p. "
        f"({var_qvsq_str}{var_yoy_str}). "
    )

    if parrafo_quejas:
        texto += parrafo_quejas + " "

    if parrafo_mix:
        texto += parrafo_mix

    # Check Newbie/Legacy mix shift as additional factor
    newbie_items = bloque4.get("tablas", {}).get("newbie_legacy", [])
    if newbie_items:
        for item in newbie_items:
            if item.get("nombre", "").lower() in ("newbie", "newbies"):
                var_share = item.get("var_share")
                efecto_mix = item.get("efecto_mix")
                if var_share is not None and abs(var_share) >= 2:
                    dir_n = "más" if var_share > 0 else "menos"
                    texto += f" Se observa {dir_n} newbies en el mix ({var_share:+.1f}pp share)."

    return texto.strip()


# ===================================================================
# Legacy compatibility helpers
# ===================================================================

def _legacy_explicacion(bloque2: dict) -> str:
    principales = bloque2.get("principales", [])
    if principales:
        return f"quejas:{principales[0]['motivo']}"
    return "sin_explicacion_clara"


def _legacy_nivel1(bloque2: dict) -> dict:
    principales = bloque2.get("principales", [])
    return {
        "explica_variacion": len(principales) > 0,
        "motivo_principal": principales[0]["motivo"] if principales else None,
        "var_quejas_principal": principales[0]["var_share"] if principales else None,
        "motivos_significativos": bloque2.get("todos", []),
    }


def _legacy_nivel2(bloque4: dict) -> dict:
    tablas = bloque4.get("tablas", {})
    prod = tablas.get("producto", [])
    result = {"producto_driver": None, "var_nps_producto": None, "productos_analizados": prod}
    if prod and prod[0].get("efecto_neto") and abs(prod[0]["efecto_neto"]) > 0.5:
        result["producto_driver"] = prod[0]["nombre"]
        result["var_nps_producto"] = prod[0].get("var_nps")
    return result


def _legacy_nivel3(bloque4: dict) -> dict:
    devices = bloque4.get("point_devices", [])
    result = {"aplica": len(devices) > 0, "devices_analizados": devices, "device_driver": None}
    if devices and devices[0].get("var_nps") and abs(devices[0]["var_nps"]) > 1:
        result["device_driver"] = devices[0]["nombre"]
    return result
