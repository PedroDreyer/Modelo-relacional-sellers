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
    try:
        import math
        if math.isnan(val):
            return None
    except (TypeError, ValueError):
        pass
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
    drill_down_data = checkpoint1_data.get("drill_down", {})
    bloque3 = _bloque3_asociacion_drivers(
        bloque2, dimensiones, checkpoint5_data,
        mapeo_config, UMBRAL_DRIVER_DIM, mes_actual,
        config=config,
        drill_down=drill_down_data,
    )

    # ------------------------------------------------------------------
    # BLOQUE 4: Análisis MIX (Producto, Segmento, Persona)
    # ------------------------------------------------------------------
    bloque4 = _bloque4_mix(
        dimensiones, UMBRAL_PRODUCTO, UMBRAL_DEVICE,
        mes_actual, config=config,
    )

    # ------------------------------------------------------------------
    # BLOQUE 5: Tendencias de métricas no-queja (PdF, pricing penetración)
    # ------------------------------------------------------------------
    bloque5_tendencias = _bloque5_tendencias_metricas(
        dimensiones, df_nps, mes_actual, config=config,
        quarter_actual=quarter_actual, quarter_anterior=quarter_anterior,
        site=site,
    )

    # ------------------------------------------------------------------
    # Generate narrative paragraphs
    # ------------------------------------------------------------------
    parrafo_quejas = _generar_parrafo_quejas(bloque2, bloque3, direccion_nps, checkpoint5_data, drill_down_data=drill_down_data)
    parrafo_mix = _generar_parrafo_mix(bloque4, bloque2, direccion_nps)
    parrafo_tendencias = _generar_parrafo_tendencias(bloque5_tendencias, direccion_nps)
    parrafo_contexto = _generar_parrafo_contexto(site, quarter_actual, config)
    parrafo_resumen = _generar_parrafo_resumen(
        bloque1, bloque2, bloque3, bloque4,
        parrafo_quejas, parrafo_mix,
        direccion_nps, site, quarter_actual,
        parrafo_tendencias=parrafo_tendencias,
        parrafo_contexto=parrafo_contexto,
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

    # YoY: compare current quarter vs same quarter last year
    if quarter_actual:
        # Derive YoY quarter: e.g., 26Q1 → 25Q1
        yoy_year = int(quarter_actual[:2]) - 1
        yoy_quarter = f"{yoy_year:02d}{quarter_actual[2:]}"
        meses_yoy = quarter_to_months(yoy_quarter)
        df_q_yoy = df_nps[df_nps["END_DATE_MONTH"].isin(meses_yoy)]
        nps_yoy = df_q_yoy["NPS"].mean() * 100 if len(df_q_yoy) > 0 else None
    else:
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
    drill_down: dict | None = None,
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
        share_primario = dim_match.get("share_primario", False)
        clasificacion, detalle = _clasificar_asociacion(
            var_share, dim_data, umbral_dim, mes_actual, desc,
            relacion_inversa=relacion_inversa,
            share_primario=share_primario,
            dim_key_hint=dim_key,
            all_dimensiones=dimensiones,
        )

        dims_usadas.add(dim_key)

        # Drill-down: if we found a best sub-group, check cross-dimension
        if drill_down and dim_key in (drill_down or {}):
            dd = drill_down[dim_key]
            best_sg = detalle.get("subgrupo_name", "")
            dd_items = dd.get("by_value", {}).get(str(best_sg), [])
            if dd_items:
                # Pick the most relevant cross-value:
                # 1. Same direction as the sub-group NPS movement
                # 2. Minimum share >= 5% (avoid tiny segments)
                # 3. Among those, biggest absolute NPS variation
                sg_nps_var = detalle.get("nps_subgrupo_var", 0) or 0
                MIN_DD_SHARE = 5.0
                relevant = [
                    d for d in dd_items
                    if d.get("nps_var") is not None
                    and abs(d["nps_var"]) >= 1
                    and (d.get("share") or 0) >= MIN_DD_SHARE
                    # Same direction: if sub-group NPS went down, cross should also go down
                    and (sg_nps_var == 0 or (d["nps_var"] < 0) == (sg_nps_var < 0))
                ]
                # Sort by absolute NPS variation
                relevant.sort(key=lambda x: abs(x.get("nps_var") or 0), reverse=True)
                top_dd = relevant[0] if relevant else None
                if top_dd:
                    detalle["drill_down"] = {
                        "cross_label": dd.get("cross_label", ""),
                        "cross_value": top_dd["cross_value"],
                        "nps_var": top_dd["nps_var"],
                        "nps_act": top_dd["nps_q_actual"],
                        "nps_ant": top_dd["nps_q_anterior"],
                        "share": top_dd["share"],
                    }

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


def _analizar_tc_limite(
    all_dimensiones: dict,
    mes_actual: str,
    mes_anterior: str | None,
) -> dict:
    """Analiza oferta TC + límite TC para explicar movimientos de NPS en TC.

    Cadena lógica:
      1. Oferta TC: ¿le llega la oferta? Si no → se queja de crédito.
      2. Uso TC (FLAG_TARJETA_CREDITO): ya analizado como dimensión primaria.
      3. Límite TC: dentro de los que usan TC, ¿qué límite tienen?
         Más límite → mejor NPS. ¿Se corrió el share hacia rangos bajos?

    Compara encuesta (share en RANGO_LIMITE_TC) vs realidad (shares_real)
    para decir "se ve / no se ve reflejado en la realidad".

    Returns dict con datos y wording directo.
    """
    result: dict = {}

    def _get_share(item, mes):
        shares = item.get("shares_por_mes", {})
        v = shares.get(mes)
        if v is not None:
            return v
        if shares:
            return shares[sorted(shares.keys())[-1]]
        return None

    def _get_real_share(item, mes):
        shares = item.get("shares_real_por_mes", {})
        v = shares.get(mes)
        if v is not None:
            return v
        if shares:
            return shares[sorted(shares.keys())[-1]]
        return None

    # ── 1. Oferta TC ──────────────────────────────────────────────────
    oferta_tc_data = all_dimensiones.get("OFERTA_TC", [])
    for item in oferta_tc_data:
        dim_name = str(item.get("dimension", ""))
        if "con oferta" not in dim_name.lower():
            continue
        nps_act = item.get("nps_q_actual")
        nps_ant = item.get("nps_q_anterior")
        sh_act = _get_share(item, mes_actual)
        sh_ant = _get_share(item, mes_anterior) if mes_anterior else None
        rsh_act = _get_real_share(item, mes_actual)
        rsh_ant = _get_real_share(item, mes_anterior) if mes_anterior else None
        result["oferta_tc"] = {
            "label": dim_name,
            "nps_act": _safe_round(nps_act),
            "nps_ant": _safe_round(nps_ant),
            "nps_var": _safe_round(nps_act - nps_ant) if nps_act is not None and nps_ant is not None else None,
            "share_act": _safe_round(sh_act),
            "share_ant": _safe_round(sh_ant) if sh_ant else None,
            "share_var": _safe_round(sh_act - sh_ant) if sh_act and sh_ant else None,
            "real_share_act": _safe_round(rsh_act) if rsh_act else None,
            "real_share_ant": _safe_round(rsh_ant) if rsh_ant else None,
            "real_share_var": _safe_round(rsh_act - rsh_ant) if rsh_act and rsh_ant else None,
        }
        break

    # ── 2. Rangos de límite TC ────────────────────────────────────────
    limite_tc_data = all_dimensiones.get("RANGO_LIMITE_TC", [])
    rangos = []
    for item in limite_tc_data:
        dim_name = str(item.get("dimension", ""))
        if dim_name.lower() in ("sin tc", "sin dato", "nan", "none", ""):
            continue
        nps_act = item.get("nps_q_actual")
        nps_ant = item.get("nps_q_anterior")
        sh_act = _get_share(item, mes_actual)
        sh_ant = _get_share(item, mes_anterior) if mes_anterior else None
        rsh_act = _get_real_share(item, mes_actual)
        rsh_ant = _get_real_share(item, mes_anterior) if mes_anterior else None
        if nps_act is not None and sh_act is not None and sh_act >= 1:
            rangos.append({
                "rango": dim_name,
                "nps_act": _safe_round(nps_act),
                "nps_ant": _safe_round(nps_ant),
                "nps_var": _safe_round(nps_act - nps_ant) if nps_ant is not None else None,
                "share_act": _safe_round(sh_act),
                "share_ant": _safe_round(sh_ant) if sh_ant else None,
                "share_var": _safe_round(sh_act - sh_ant) if (sh_act is not None and sh_ant is not None) else None,
                "real_share_act": _safe_round(rsh_act) if rsh_act else None,
                "real_share_ant": _safe_round(rsh_ant) if rsh_ant else None,
                "real_share_var": _safe_round(rsh_act - rsh_ant) if (rsh_act is not None and rsh_ant is not None) else None,
            })
    rangos.sort(key=lambda x: x.get("share_act", 0), reverse=True)
    result["limite_rangos"] = rangos

    # ── 3. Shift analysis ─────────────────────────────────────────────
    # Ordenar por NPS (proxy de monto: más NPS ≈ más límite)
    # Si creció share de rangos bajos (bajo NPS) → shift hacia_abajo → peor NPS
    shift_encuesta = "estable"
    shift_real = "estable"
    rangos_con_var = [r for r in rangos if r.get("share_var") is not None]
    if len(rangos_con_var) >= 3:
        by_nps = sorted(rangos_con_var, key=lambda x: x.get("nps_act") or 0)
        low_half = by_nps[:len(by_nps) // 2]
        high_half = by_nps[len(by_nps) // 2:]
        low_shift = sum(r.get("share_var", 0) for r in low_half)
        high_shift = sum(r.get("share_var", 0) for r in high_half)
        if low_shift > 1 and high_shift < -1:
            shift_encuesta = "hacia_abajo"
        elif high_shift > 1 and low_shift < -1:
            shift_encuesta = "hacia_arriba"

    # Misma lógica para realidad
    rangos_con_real_var = [r for r in rangos if r.get("real_share_var") is not None]
    if len(rangos_con_real_var) >= 3:
        by_nps_r = sorted(rangos_con_real_var, key=lambda x: x.get("nps_act") or 0)
        low_r = by_nps_r[:len(by_nps_r) // 2]
        high_r = by_nps_r[len(by_nps_r) // 2:]
        low_shift_r = sum(r.get("real_share_var", 0) for r in low_r)
        high_shift_r = sum(r.get("real_share_var", 0) for r in high_r)
        if low_shift_r > 1 and high_shift_r < -1:
            shift_real = "hacia_abajo"
        elif high_shift_r > 1 and low_shift_r < -1:
            shift_real = "hacia_arriba"

    result["shift_encuesta"] = shift_encuesta
    result["shift_real"] = shift_real

    # ── 4. NPS de "Tiene TC MP" ───────────────────────────────────────
    tc_nps_var = None
    tc_data = all_dimensiones.get("FLAG_TARJETA_CREDITO", [])
    for item in tc_data:
        dim_name = str(item.get("dimension", ""))
        if any(k in dim_name.lower() for k in ("tiene", "con tc", "usa")):
            nps_act = item.get("nps_q_actual")
            nps_ant = item.get("nps_q_anterior")
            if nps_act is not None and nps_ant is not None:
                tc_nps_var = _safe_round(nps_act - nps_ant)
            break
    result["tc_nps_var"] = tc_nps_var

    # ── 5. Wording directo ────────────────────────────────────────────
    parts = []

    # TC NPS movement + límite como causa
    if tc_nps_var is not None and abs(tc_nps_var) >= 1 and shift_encuesta != "estable":
        dir_nps = "disminuye" if tc_nps_var < 0 else "aumenta"
        dir_limite = "menor" if shift_encuesta == "hacia_abajo" else "mayor"
        txt = f"{dir_nps} {abs(tc_nps_var):.0f}pp NPS de usuarios con TC por {dir_limite} límite en el share de encuestas"
        # Comparar con realidad
        if shift_real == shift_encuesta:
            txt += ", se ve reflejado en la realidad"
        elif shift_real == "estable":
            txt += ", no se ve reflejado en la realidad"
        else:
            txt += ", la realidad muestra tendencia opuesta"
        parts.append(txt)
    elif tc_nps_var is not None and abs(tc_nps_var) >= 1:
        # NPS se movió pero no hay shift claro de límite → reportar sin causa
        dir_nps = "disminuye" if tc_nps_var < 0 else "aumenta"
        parts.append(f"{dir_nps} {abs(tc_nps_var):.0f}pp NPS de usuarios con TC, sin cambio claro en distribución de límite")

    # Oferta TC (siempre reportar si cambió)
    oferta = result.get("oferta_tc", {})
    if oferta:
        sh_var = oferta.get("share_var")
        sh_act = oferta.get("share_act")
        o_nps_var = oferta.get("nps_var")
        if sh_act is not None and sh_var is not None and abs(sh_var) >= 0.5:
            dir_o = "creció" if sh_var > 0 else "cayó"
            txt = f"Oferta TC {dir_o} {abs(sh_var):.1f}pp ({sh_act:.0f}% de la base)"
            # Comparar con realidad
            rsh_var = oferta.get("real_share_var")
            if rsh_var is not None:
                if (sh_var > 0) == (rsh_var > 0):
                    txt += f", consistente con realidad ({rsh_var:+.1f}pp)"
                else:
                    txt += f", realidad muestra {rsh_var:+.1f}pp"
            parts.append(txt)

    result["wording"] = ". ".join(parts)
    return result


def _clasificar_asociacion(
    var_motivo: float,
    dim_data: list,
    umbral: float,
    mes_actual: str,
    desc: str,
    relacion_inversa: bool = False,
    share_primario: bool = False,
    dim_key_hint: str = "",
    all_dimensiones: dict | None = None,
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
        # Fallback: use latest available month if specific months not found
        if sh_act is None and shares:
            sorted_months = sorted(shares.keys(), reverse=True)
            sh_act = shares[sorted_months[0]]
            if sh_ant is None and len(sorted_months) >= 2:
                sh_ant = shares[sorted_months[1]]
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

    # CREDIT_GROUP special: enrich detalle with active groups (3-5) breakdown
    if dim_key_hint == "CREDIT_GROUP":
        active_groups = []
        total_active_share_act = 0
        total_active_share_ant = 0
        for c in candidates:
            dim_name = str(c.get("dimension", ""))
            # Groups 3, 4, 5 are the "active" credit users
            if any(dim_name.startswith(p) for p in ("3.", "4.", "5.")):
                sg = {
                    "grupo": dim_name,
                    "nps_act": _safe_round(c["nps_act"]) if c["nps_act"] is not None else None,
                    "nps_ant": _safe_round(c["nps_ant"]) if c["nps_ant"] is not None else None,
                    "nps_var": _safe_round(c["nps_var"]),
                    "share_act": _safe_round(c["share_act"]) if c["share_act"] is not None else None,
                    "share_var": _safe_round(c["share_var"]),
                }
                active_groups.append(sg)
                if c["share_act"] is not None:
                    total_active_share_act += c["share_act"]
                    total_active_share_ant += (c["share_act"] - c["share_var"])
        if active_groups:
            detalle["credit_breakdown"] = {
                "active_groups": active_groups,
                "total_active_share_act": _safe_round(total_active_share_act),
                "total_active_share_ant": _safe_round(total_active_share_ant),
                "total_active_share_var": _safe_round(total_active_share_act - total_active_share_ant),
            }

    # FLAG_USA_CREDITO special: enrich with TC detail
    if dim_key_hint == "FLAG_USA_CREDITO" and all_dimensiones:
        tc_data = all_dimensiones.get("FLAG_TARJETA_CREDITO", [])
        tc_detail = {}
        for item in tc_data:
            dim_name = str(item.get("dimension", ""))
            nps_act = item.get("nps_q_actual")
            nps_ant = item.get("nps_q_anterior")
            shares = item.get("shares_por_mes", {})
            sh_act = shares.get(mes_actual) or (shares[sorted(shares.keys())[-1]] if shares else None)
            if nps_act is not None and nps_ant is not None:
                tc_detail[dim_name] = {
                    "label": dim_name,
                    "nps_act": _safe_round(nps_act),
                    "nps_ant": _safe_round(nps_ant),
                    "nps_var": _safe_round(nps_act - nps_ant),
                    "share_act": _safe_round(sh_act) if sh_act else None,
                }
        if tc_detail:
            detalle["tc_breakdown"] = tc_detail

        # ── TC drill-down: oferta + límite ──
        # Lógica: más límite → mejor NPS. Si cae NPS de TC → ¿bajó el límite
        # promedio? ¿se corrió el share hacia rangos bajos?
        detalle["tc_limite_analysis"] = _analizar_tc_limite(
            all_dimensiones, mes_actual, mes_anterior,
        )

    # FLAG_USA_INVERSIONES special: enrich with WINNER, ASSET, POTS breakdown
    if dim_key_hint == "FLAG_USA_INVERSIONES" and all_dimensiones:
        inv_breakdown = {}
        for inv_dim in ["FLAG_WINNER", "FLAG_ASSET", "FLAG_POTS_ACTIVO"]:
            inv_data = all_dimensiones.get(inv_dim, [])
            for item in inv_data:
                dim_name = str(item.get("dimension", ""))
                # Look for the "positive" value (1, Winner, etc.)
                if dim_name in ("1", "True", "true") or not dim_name.lower().startswith(("0", "no", "sin", "false")):
                    nps_act = item.get("nps_q_actual")
                    nps_ant = item.get("nps_q_anterior")
                    shares = item.get("shares_por_mes", {})
                    sh_act = shares.get(mes_actual)
                    sh_ant = shares.get(mes_anterior)
                    if nps_act is not None and nps_ant is not None:
                        inv_breakdown[inv_dim] = {
                            "label": dim_name,
                            "nps_act": _safe_round(nps_act),
                            "nps_ant": _safe_round(nps_ant),
                            "nps_var": _safe_round(nps_act - nps_ant),
                            "share_act": _safe_round(sh_act) if sh_act else None,
                            "share_ant": _safe_round(sh_ant) if sh_ant else None,
                            "share_var": _safe_round(sh_act - sh_ant) if (sh_act and sh_ant) else None,
                        }
                    break  # Only take the first positive match per dim
        if inv_breakdown:
            detalle["inversiones_breakdown"] = inv_breakdown

    # FLAG_TARJETA_CREDITO special: enrich with oferta + límite TC
    if dim_key_hint == "FLAG_TARJETA_CREDITO" and all_dimensiones:
        detalle["tc_limite_analysis"] = _analizar_tc_limite(
            all_dimensiones, mes_actual, mes_anterior,
        )

    # PROBLEMA_FUNCIONAMIENTO special: enrich with TIPO_PROBLEMA breakdown
    if dim_key_hint == "PROBLEMA_FUNCIONAMIENTO" and all_dimensiones:
        tipo_data = all_dimensiones.get("TIPO_PROBLEMA", [])
        tipo_breakdown = []
        for item in tipo_data:
            dim_name = str(item.get("dimension", ""))
            if not dim_name or dim_name.lower() in ("nan", "none", ""):
                continue
            nps_act = item.get("nps_q_actual")
            nps_ant = item.get("nps_q_anterior")
            shares = item.get("shares_por_mes", {})
            sh_act = shares.get(mes_actual) or (shares[sorted(shares.keys())[-1]] if shares else None)
            if nps_act is not None and nps_ant is not None and sh_act is not None and sh_act >= 1:
                tipo_breakdown.append({
                    "tipo": dim_name,
                    "nps_act": _safe_round(nps_act),
                    "nps_ant": _safe_round(nps_ant),
                    "nps_var": _safe_round(nps_act - nps_ant),
                    "share_act": _safe_round(sh_act),
                })
        if tipo_breakdown:
            # Sort by share (most prevalent problems first)
            tipo_breakdown.sort(key=lambda x: x.get("share_act", 0), reverse=True)
            detalle["tipo_problema_breakdown"] = tipo_breakdown[:5]  # Top 5

    motivo_moves = abs(var_motivo) >= umbral

    if share_primario:
        # For share_primario dimensions (e.g., Top Off): share variation IS the signal
        # +share = better coverage, -share = worse coverage
        detalle["share_primario"] = True
        effective_var = max_share_var
        dim_moves = abs(max_share_var) >= umbral
    else:
        # Default: use NPS QvsQ of sub-group (more reliable than share MoM)
        nps_var = best_nps_var if best_nps_item else 0
        dim_moves = abs(max_var) >= umbral or abs(nps_var) >= 2.0
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

    # Drill-down (Nivel 2): cross-dimension detail
    dd = detalle.get("drill_down")
    dd_suffix = ""
    if dd:
        dd_cv = dd.get("cross_value", "")
        dd_var = dd.get("nps_var", 0)
        dd_share = dd.get("share", 0)
        dd_label = dd.get("cross_label", "")
        dd_suffix = f", principalmente en {dd_cv} ({dd_var:+.0f}pp NPS, {dd_share:.0f}% del {dd_label})"

    # Credit breakdown: summarize active groups (3-5) movement
    cb = detalle.get("credit_breakdown")
    credit_detail = ""
    if cb and cb.get("active_groups"):
        parts = []
        for ag in cb["active_groups"]:
            if ag.get("nps_var") is not None and abs(ag["nps_var"]) >= 1 and ag.get("share_act"):
                parts.append(f"{ag['grupo']}: {ag['nps_var']:+.0f}pp NPS ({ag['share_act']:.0f}%)")
        if parts:
            total_var = cb.get("total_active_share_var", 0)
            dir_total = "creció" if total_var > 0 else "cayó"
            credit_detail = f" [Usuarios activos de crédito (grupos 3-5): share total {dir_total} {abs(total_var):.1f}pp. {'; '.join(parts)}]"

    # Inversiones breakdown: WINNER, ASSET, POTS detail
    ib = detalle.get("inversiones_breakdown", {})
    inv_detail = ""
    if ib:
        parts = []
        for inv_key, inv_label in [("FLAG_WINNER", "Winners"), ("FLAG_ASSET", "Cuenta Remunerada"), ("FLAG_POTS_ACTIVO", "Pots/Cofrinhos")]:
            d = ib.get(inv_key)
            if d and d.get("nps_var") is not None and abs(d["nps_var"]) >= 1:
                share_info = f", {d['share_act']:.0f}%" if d.get("share_act") else ""
                parts.append(f"{inv_label}: {d['nps_var']:+.0f}pp NPS{share_info}")
        if parts:
            inv_detail = f" [{'; '.join(parts)}]"

    is_share_primario = detalle.get("share_primario", False)

    if clasif == "EXPLICA_OK":
        if is_share_primario and share_ant_sg is not None and share_var_sg is not None:
            dir_share = "creció" if share_var_sg > 0 else "cayó"
            detail = f"share de {subgrupo} {dir_share} de {share_ant_sg:.0f}% a {share:.0f}% ({share_var_sg:+.1f}pp)"
            if has_rich and abs(nps_var or 0) >= 1:
                detail += f", NPS {nps_var:+.0f}pp"
            return (
                f"{dir_motivo} de quejas de {motivo} ({var:+.1f}pp QvsQ): "
                f"{detail}{dd_suffix}{voz_usuario}"
            )
        if has_rich and abs(nps_var or 0) >= 1:
            nps_part = f"NPS de {subgrupo} pasó de {nps_ant:.0f} a {nps_act:.0f} ({nps_var:+.0f}pp)"
            share_part = ""
            if share_ant_sg is not None and share_var_sg is not None and abs(share_var_sg) >= 0.5:
                # Check if share moves opposite to the narrative (quejas direction)
                # For relacion_inversa: quejas down + share down = contradictory share movement
                share_contradicts = False
                if inversa and share_var_sg is not None:
                    # quejas down (var<0) but share of positive sub-group also down → "a pesar de"
                    # quejas up (var>0) but share of positive sub-group also up → "a pesar de"
                    share_contradicts = (var < 0 and share_var_sg < 0) or (var > 0 and share_var_sg > 0)
                dir_share = "creció" if share_var_sg > 0 else "cayó"
                if share_contradicts:
                    share_part = f", a pesar de caída de {abs(share_var_sg):.0f}pp en share de {subgrupo}" if share_var_sg < 0 else f", a pesar de crecimiento de {share_var_sg:.0f}pp en share"
                else:
                    share_part = f", participación {dir_share} de {share_ant_sg:.0f}% a {share:.0f}%"
            elif share is not None:
                share_part = f" ({share:.0f}% del total)"

            return (
                f"{dir_motivo} de quejas de {motivo} ({var:+.1f}pp QvsQ): "
                f"{nps_part}{share_part}{dd_suffix}{credit_detail}{inv_detail}{voz_usuario}"
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
    def _safe_abs(v):
        if v is None or v != v:
            return 0
        return abs(v)

    point_driver = any(
        "point" in (it.get("nombre", "")).lower()
        and _safe_abs(it.get("efecto_neto")) >= umbral_producto
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

def _generar_parrafo_quejas(bloque2: dict, bloque3: dict, direccion_nps: int, cp5_data: dict | None = None, drill_down_data: dict | None = None) -> str:
    """Generate narrative paragraphs for complaint motivos — one paragraph per motivo."""
    asociaciones = bloque3.get("asociaciones", [])
    principales = bloque2.get("principales", [])
    compensaciones = bloque2.get("compensaciones", [])

    if not principales and not compensaciones:
        return "No se identificaron variaciones significativas en los motivos de queja."

    # Re-rank principales by composite score: quejas variation + driver aporte
    # This balances "what sellers complain about" with "what actually moved NPS"
    # Without this, the model defaults to whichever motivo has the biggest quejas swing
    asoc_lookup = {}
    for a in asociaciones:
        asoc_lookup.setdefault(a.get("motivo", ""), []).append(a)

    def _composite_score(mot_dict):
        """Score = weighted combination of quejas variation and NPS aporte.
        quejas_weight=0.5: what sellers complain about matters
        aporte_weight=0.5: what actually moved NPS matters
        Only EXPLICA_OK drivers get the aporte bonus."""
        var_quejas = abs(mot_dict.get("var_share", 0))
        aporte = 0
        mot = mot_dict["motivo"]
        for a in asoc_lookup.get(mot, []):
            if a.get("clasificacion") == "EXPLICA_OK":
                det = a.get("detalle", {})
                ap = det.get("aporte_pp")
                if ap is not None:
                    aporte = abs(ap)
                break
        return var_quejas * 0.5 + aporte * 0.5

    principales = sorted(
        principales,
        key=lambda m: _composite_score(m),
        reverse=True,
    )

    MAX_PRINCIPALES = 3
    MAX_COMPENSACIONES = 2

    # Index associations by motivo
    asoc_by_mot: dict = {}
    for a in asociaciones:
        asoc_by_mot.setdefault(a.get("motivo", ""), []).append(a)

    paragraphs: list = []

    # --- Principal motivos (one paragraph each) ---
    for i, mot_dict in enumerate(principales[:MAX_PRINCIPALES]):
        mot = mot_dict["motivo"]
        var = mot_dict.get("var_share", 0)

        # Find best association for this motivo
        asoc = None
        for a in asoc_by_mot.get(mot, []):
            asoc = a
            break

        det = asoc.get("detalle", {}) if asoc else {}
        clasif = asoc.get("clasificacion", "") if asoc else ""
        subgrupo = det.get("subgrupo_name", "")
        nps_ant_sg = det.get("nps_subgrupo_ant")
        nps_act_sg = det.get("nps_subgrupo_act")
        nps_var_sg = det.get("nps_subgrupo_var") or 0
        share_sg = det.get("share_subgrupo")
        aporte = det.get("aporte_pp")
        is_share_prim = det.get("share_primario", False)
        share_ant_sg = det.get("share_subgrupo_ant")
        share_var_sg = det.get("share_subgrupo_var")

        has_rich = (
            clasif == "EXPLICA_OK"
            and nps_ant_sg is not None and nps_act_sg is not None
            and share_sg is not None and abs(nps_var_sg) >= 1
        )

        # --- Opener varies by position ---
        if i == 0:
            if direccion_nps < 0:
                opener = "Esta caída se explica principalmente por"
            elif direccion_nps > 0:
                opener = "Esta mejora se explica principalmente por"
            else:
                opener = "El principal movimiento viene de"
        else:
            opener = "También se observa movimiento en"

        # --- Always start with quejas direction + variation ---
        dir_q = "aumento" if var > 0 else "disminución"
        quejas_tag = f"{dir_q} de quejas de <b>{mot}</b> ({var:+.1f}pp)"

        # --- Build paragraph based on classification ---
        if is_share_prim and clasif == "EXPLICA_OK" and share_ant_sg is not None and share_var_sg is not None:
            # Share-primary (e.g., Top Off): quejas → share driver
            if abs(share_var_sg) < 1:
                share_desc = f"share de {subgrupo} estable en ~{share_sg:.0f}%"
            else:
                dir_sh = "creció" if share_var_sg > 0 else "cayó"
                share_desc = f"share de {subgrupo} {dir_sh} de {share_ant_sg:.0f}% a {share_sg:.0f}% ({share_var_sg:+.1f}pp)"
            p = f"{opener} {quejas_tag}: {share_desc}"
            if has_rich:
                dir_v = "mejoró" if nps_var_sg > 0 else "empeoró"
                p += f", y su NPS {dir_v} {abs(nps_var_sg):.1f}pp"

        elif has_rich:
            # Rich sub-group narrative: quejas → NPS driver with before/after + contribution
            dir_verb = "subió" if nps_var_sg > 0 else "bajó"
            nps_trans = f"de {nps_ant_sg:.1f} a {nps_act_sg:.1f} p.p."
            p = (
                f"{opener} {quejas_tag}: el grupo {subgrupo} "
                f"{dir_verb} {abs(nps_var_sg):.1f}pp en su NPS ({nps_trans})"
            )
            if aporte is not None:
                p += f", aportando <b>{aporte:+.1f}pp</b> al NPS total"
            if share_sg is not None:
                if i == 0:
                    p += f" dado que representa el {share_sg:.1f}% de la base"
                else:
                    p += f" con una penetración del {share_sg:.1f}%"

        elif clasif == "CONTRADICTORIO" and nps_ant_sg is not None and nps_act_sg is not None:
            dir_dim = "mejoró" if nps_var_sg > 0 else "empeoró"
            p = (
                f"{opener} {quejas_tag}, "
                f"a pesar de que {subgrupo} {dir_dim} "
                f"(NPS de {nps_ant_sg:.1f} a {nps_act_sg:.1f} p.p.)"
            )
            if share_sg is not None:
                p += f" — representa el {share_sg:.0f}% de la base"

        else:
            # Simple text (EXPLICA_MIX, NO_EXPLICA, FALLBACK_CP5, or no rich data)
            p = f"{opener} {quejas_tag}"
            desc_dim = asoc.get("descripcion_dim", "") if asoc else ""
            if clasif == "EXPLICA_MIX" and desc_dim:
                p += f", no explicado por {desc_dim} (se mantiene estable)"

        # Drill-down suffix (Nivel 2: cross-dimension)
        # Skip only if cross is by "producto" AND share >=90% (redundant — e.g., "Point" in Point update)
        dd = det.get("drill_down")
        _dd_dominated = (dd.get("share") or 0) >= 90 and dd.get("cross_label") == "producto" if dd else False
        if dd and dd.get("nps_var") is not None and not _dd_dominated:
            dd_cv = dd.get("cross_value", "")
            dd_var = dd.get("nps_var", 0)
            dd_share = dd.get("share", 0)
            dd_label = dd.get("cross_label", "total")
            p += f", principalmente en {dd_cv} ({dd_var:+.0f}pp NPS, {dd_share:.0f}% del {dd_label})"

        # TC breakdown detail (supplementary: FLAG_TARJETA_CREDITO) — report only positive group
        tc = det.get("tc_breakdown", {})
        if tc:
            # Only report "Tiene TC MP" (the relevant group), not the complement
            tc_pos = None
            for label, d in tc.items():
                if any(k in label.lower() for k in ("tiene", "con", "usa")):
                    tc_pos = (label, d)
                    break
            if tc_pos:
                label, d = tc_pos
                nps_var = d.get("nps_var", 0)
                if nps_var is not None and abs(nps_var) >= 1:
                    tc_text = f". NPS de sellers con TC {'cayó' if nps_var < 0 else 'subió'} {abs(nps_var):.0f}pp"
                    # Add TC × Segmento drill-down if available
                    tc_dd = (drill_down_data or {}).get("FLAG_TARJETA_CREDITO", {})
                    tc_dd_items = tc_dd.get("by_value", {}).get("Tiene TC MP", [])
                    if tc_dd_items:
                        # Pick the cross-value with most absolute NPS variation and min 5% share
                        # Skip if dominant (>=90%) — redundant (e.g. "SMB" in SMBs update)
                        tc_dd_relevant = [x for x in tc_dd_items if (x.get("share") or 0) >= 5 and (x.get("share") or 0) < 90 and x.get("nps_var") is not None]
                        if tc_dd_relevant:
                            best_tc_dd = max(tc_dd_relevant, key=lambda x: abs(x.get("nps_var", 0)))
                            tc_text += f", principalmente en {best_tc_dd['cross_value']} ({best_tc_dd['nps_var']:+.0f}pp NPS, {best_tc_dd['share']:.0f}% del {tc_dd.get('cross_label', 'total')})"
                    p += tc_text

        # TC límite + oferta (análisis unificado)
        tc_lim = det.get("tc_limite_analysis", {})
        tc_wording = tc_lim.get("wording", "")
        if tc_wording:
            p += ". " + tc_wording

        # Inversiones breakdown — brief: only show the biggest mover
        ib = det.get("inversiones_breakdown", {})
        if ib:
            biggest = None
            biggest_label = ""
            for inv_key, inv_label in [("FLAG_WINNER", "Winners"), ("FLAG_ASSET", "Cta Remunerada"), ("FLAG_POTS_ACTIVO", "Pots")]:
                d = ib.get(inv_key)
                if d and d.get("nps_var") is not None and abs(d["nps_var"]) >= 1:
                    if biggest is None or abs(d["nps_var"]) > abs(biggest["nps_var"]):
                        biggest = d
                        biggest_label = inv_label
            if biggest:
                dir_inv = "bajó" if biggest["nps_var"] < 0 else "subió"
                sh = f" ({biggest['share_act']:.0f}% de la base)" if biggest.get("share_act") else ""
                p += f". En particular, {biggest_label} {dir_inv} {abs(biggest['nps_var']):.0f}pp{sh}"

        # Tipo problema breakdown (PdF)
        tpb = det.get("tipo_problema_breakdown", [])
        if tpb:
            top_tipos = [t for t in tpb[:3] if t.get("share_act", 0) >= 2]
            if top_tipos:
                parts = [f"{t['tipo']} ({t['share_act']:.0f}%)" for t in top_tipos]
                p += f". Problemas principales: {', '.join(parts)}"

        # CP5 user voice
        causa = _get_causa_raiz_cp5(mot, cp5_data)
        if causa:
            p += f". Sellers reportan: <i>{causa}</i>"

        # Clean up and close
        p = p.rstrip(".") + "."
        paragraphs.append(p)

    # --- Compensaciones (brief, grouped in one paragraph) ---
    comps = compensaciones[:MAX_COMPENSACIONES]
    if comps:
        comp_parts = []
        for mot_dict in comps:
            mot = mot_dict["motivo"]
            var = mot_dict.get("var_share", 0)

            asoc = None
            for a in asoc_by_mot.get(mot, []):
                asoc = a
                break

            det = asoc.get("detalle", {}) if asoc else {}
            clasif_c = asoc.get("clasificacion", "") if asoc else ""
            subgrupo = det.get("subgrupo_name", "")
            nps_ant_c = det.get("nps_subgrupo_ant")
            nps_act_c = det.get("nps_subgrupo_act")
            nps_var_sg = det.get("nps_subgrupo_var") or 0
            share_sg = det.get("share_subgrupo")
            share_ant_c = det.get("share_subgrupo_ant")
            share_var_c = det.get("share_subgrupo_var")
            is_share_prim_c = det.get("share_primario", False)

            if is_share_prim_c and share_ant_c is not None and share_var_c is not None:
                # Share-primary (e.g., Top Off): explain with share + NPS explicitly
                if abs(share_var_c) < 1:
                    share_txt = f"share de {subgrupo} estable en ~{share_sg:.0f}%"
                else:
                    dir_sh = "creció" if share_var_c > 0 else "cayó"
                    share_txt = f"share de {subgrupo} {dir_sh} de {share_ant_c:.0f}% a {share_sg:.0f}%"
                txt = f"<b>{mot}</b> ({var:+.1f}pp): {share_txt}"
                if nps_ant_c is not None and nps_act_c is not None and abs(nps_var_sg) >= 1:
                    dir_nps = "subió" if nps_var_sg > 0 else "bajó"
                    txt += f", NPS {dir_nps} de {nps_ant_c:.0f} a {nps_act_c:.0f} p.p."
                comp_parts.append(txt)
            elif subgrupo and nps_ant_c is not None and nps_act_c is not None and abs(nps_var_sg) >= 1:
                # NPS-primary: explain with NPS before/after + share context
                dir_v = "subió" if nps_var_sg > 0 else "bajó"
                txt = f"<b>{mot}</b> ({var:+.1f}pp): NPS de {subgrupo} {dir_v} de {nps_ant_c:.0f} a {nps_act_c:.0f} p.p."
                if share_sg is not None:
                    txt += f" ({share_sg:.0f}% de la base)"
                comp_parts.append(txt)
            else:
                causa = _get_causa_raiz_cp5(mot, cp5_data)
                detail = f" — <i>{causa}</i>" if causa else ""
                comp_parts.append(f"<b>{mot}</b> ({var:+.1f}pp){detail}")

        if direccion_nps < 0:
            comp_text = "Compensado parcialmente por mejoras en "
        elif direccion_nps > 0:
            comp_text = "Sin embargo, se observan deterioros en "
        else:
            comp_text = "Compensaciones: "
        comp_text += "; ".join(comp_parts) + "."
        paragraphs.append(comp_text)

    return "</p><p style='margin-top:10px;'>".join(paragraphs)


def _generar_parrafo_mix(bloque4: dict, bloque2: dict, direccion_nps: int) -> str:
    tablas = bloque4.get("tablas", {})
    single_product = bloque4.get("single_product", False)
    single_segment = bloque4.get("single_segment", False)

    # Determine which table to use for the mix paragraph
    # For Point/LINK/APICOW (single_product): use segmento (SMB vs Longtail)
    # For SMBs (single_segment): use producto
    # For all: use producto
    if single_product and not single_segment:
        # Point, LINK, APICOW → use segmento as primary mix
        mix_rows = tablas.get("segmento", [])
        mix_label = "segmento"
    else:
        mix_rows = tablas.get("producto", [])
        mix_label = "producto"

    # Skip if the primary dimension is also filtered (redundant)
    if single_product and single_segment:
        return ""

    if not mix_rows:
        return ""

    top = mix_rows[0]
    efecto_neto = top.get("efecto_neto")
    if efecto_neto is None or (efecto_neto != efecto_neto) or abs(efecto_neto) < 0.3:
        return ""

    nombre = top["nombre"]
    _em = top.get("efecto_mix")
    e_mix = 0 if _em is None or (_em != _em) else _em
    _en = top.get("efecto_nps")
    e_nps = 0 if _en is None or (_en != _en) else _en
    var_share = top.get("var_share")
    sh_act = top.get("share_actual")
    sh_ant = top.get("share_anterior")
    nps_act = top.get("nps_actual")
    nps_ant = top.get("nps_anterior")

    # Narrative format: lead with group identity and share context
    texto = f"En la composición, {nombre}"
    if sh_act is not None and sh_ant is not None:
        dir_sh = "subiendo" if (var_share or 0) > 0 else "bajando"
        texto += f" representaba el {sh_act:.1f}% de la base ({dir_sh} desde {sh_ant:.1f}%)"

    if abs(e_mix) >= 0.1:
        texto += f", contribuyendo <b>{e_mix:+.1f}pp</b> por cambio de mix"
    elif sh_act is None:
        # Fallback without share data
        texto += f" aportó <b>{efecto_neto:+.1f}pp</b> al NPS total"

    if nps_act is not None and nps_ant is not None:
        var_nps_prod = nps_act - nps_ant
        dir_nps = "mejoró" if var_nps_prod > 0 else "bajó"
        texto += (
            f". El NPS del {mix_label} {dir_nps} {abs(var_nps_prod):.1f}pp "
            f"(de {nps_ant:.1f} a {nps_act:.1f} p.p.)"
        )
        if abs(e_nps) >= 0.1:
            texto += f", aportando {e_nps:+.1f}pp"
    elif abs(e_nps) >= 0.1:
        texto += f" y {e_nps:+.1f}pp por efecto NPS"

    # Point drill-down (only for producto mix)
    if mix_label == "producto":
        point_devices = bloque4.get("point_devices", [])
        if point_devices and "point" in nombre.lower():
            top_dev = point_devices[0]
            dev_name = top_dev.get("nombre", "")
            dev_var = top_dev.get("var_nps") or 0
            if abs(dev_var) >= 0.5:
                texto += f", principalmente en dispositivos {dev_name} ({dev_var:+.1f}pp)"

    texto += "."
    return texto


def _generar_parrafo_resumen(
    bloque1, bloque2, bloque3, bloque4,
    parrafo_quejas, parrafo_mix,
    direccion_nps, site, quarter_actual,
    parrafo_tendencias: str = "",
    parrafo_contexto: str = "",
) -> str:
    nps_actual = bloque1.get("nps_actual")
    var_qvsq = bloque1.get("var_qvsq")
    var_yoy = bloque1.get("var_yoy")
    label_actual = bloque1.get("label_actual", "")

    if nps_actual is None:
        return "Datos insuficientes para generar resumen ejecutivo."

    label = label_actual or quarter_actual or ""
    nps_anterior = bloque1.get("nps_anterior")
    label_anterior = bloque1.get("label_anterior", "")

    # Directional verb based on QvsQ variation
    if var_qvsq is not None and var_qvsq >= 0.5:
        dir_verb = "subiendo"
    elif var_qvsq is not None and var_qvsq <= -0.5:
        dir_verb = "bajando"
    else:
        dir_verb = "manteniéndose estable"

    texto = f"En {label}, el NPS de {site} alcanzó <b>{nps_actual:.1f} p.p.</b>"
    if var_qvsq is not None:
        texto += f", {dir_verb} <b>{var_qvsq:+.1f}pp</b> QvsQ"
        if nps_anterior is not None and label_anterior:
            texto += f" respecto a {label_anterior} ({nps_anterior:.1f} p.p.)"
    if var_yoy is not None:
        texto += f" ({var_yoy:+.1f}pp YoY)"
    texto += ". "

    if parrafo_quejas:
        texto += parrafo_quejas + " "

    # Additional mix observations (separate paragraph, includes product mix)
    mix_observations = []

    if parrafo_mix:
        mix_observations.append(parrafo_mix.strip())

    # Check segmento/producto mix (the one NOT used in the main mix paragraph)
    tablas = bloque4.get("tablas", {})
    # If main mix used segmento, check producto and vice versa
    for table_name, label in [("segmento", "segmento"), ("producto", "producto"), ("persona", "persona")]:
        items = tablas.get(table_name, [])
        for item in items:
            efecto_neto = item.get("efecto_neto")
            var_sh = item.get("var_share")
            nombre = item.get("nombre", "")
            if efecto_neto is not None and abs(efecto_neto) >= 1.0 and var_sh is not None and abs(var_sh) >= 2:
                dir_sh = "creció" if var_sh > 0 else "cayó"
                mix_observations.append(f"{nombre} {dir_sh} {abs(var_sh):.0f}pp share ({efecto_neto:+.1f}pp efecto neto)")

    # Check Newbie/Legacy mix shift
    newbie_items = tablas.get("newbie_legacy", [])
    for item in newbie_items:
        if item.get("nombre", "").lower() in ("newbie", "newbies"):
            var_share = item.get("var_share")
            if var_share is not None and abs(var_share) >= 2:
                dir_n = "más" if var_share > 0 else "menos"
                mix_observations.append(f"{dir_n} newbies ({var_share:+.1f}pp share)")

    if mix_observations:
        # parrafo_mix (if present) is always first and is a complete sentence
        # other observations are fragments joined with "En el mix se observa:"
        parts = []
        if parrafo_mix:
            parts.append(parrafo_mix.strip())
        short_obs = [o for o in mix_observations if o != parrafo_mix.strip()] if parrafo_mix else mix_observations
        if short_obs:
            parts.append("En el mix se observa: " + "; ".join(short_obs[:3]) + ".")
        texto += "</p><p style='margin-top:12px;'>" + " ".join(parts)

    # Tendencias de métricas no-queja (PdF, pricing penetración)
    # Insert BEFORE mix if high impact and coherent with NPS direction
    if parrafo_tendencias:
        texto += "</p><p style='margin-top:10px;'>" + parrafo_tendencias

    # Contexto de eventos externos
    if parrafo_contexto:
        texto += "</p><p style='margin-top:12px;font-style:italic;color:#555;'>" + parrafo_contexto

    return texto.strip()


# ===================================================================
# BLOQUE 5 — Tendencias de métricas no-queja
# ===================================================================

def _bloque5_tendencias_metricas(
    dimensiones: dict,
    df_nps: pd.DataFrame | None,
    mes_actual: str,
    config: dict | None = None,
    quarter_actual: str | None = None,
    quarter_anterior: str | None = None,
    site: str = "",
) -> list:
    """Detect significant QvsQ changes in non-complaint metrics (PdF, pricing penetration, region)."""
    from nps_model.utils.dates import quarter_to_months
    tendencias = []

    if df_nps is None or len(df_nps) == 0 or not quarter_actual or not quarter_anterior:
        return tendencias

    meses_act = quarter_to_months(quarter_actual)
    meses_ant = quarter_to_months(quarter_anterior)
    df_q_act = df_nps[df_nps["END_DATE_MONTH"].isin(meses_act)]
    df_q_ant = df_nps[df_nps["END_DATE_MONTH"].isin(meses_ant)]

    # 1. PdF trend: % sellers with PROBLEMA_FUNCIONAMIENTO = Si/Sim
    if "PROBLEMA_FUNCIONAMIENTO" in df_nps.columns:
        import unicodedata
        def _is_yes(val):
            if pd.isna(val):
                return False
            v = "".join(c for c in unicodedata.normalize("NFD", str(val).lower()) if unicodedata.category(c) != "Mn")
            return v in ("sim", "si", "1", "true", "yes")

        # Only count sellers who answered PdF (not NaN)
        act_answered = df_q_act[df_q_act["PROBLEMA_FUNCIONAMIENTO"].notna()]
        ant_answered = df_q_ant[df_q_ant["PROBLEMA_FUNCIONAMIENTO"].notna()]

        if len(act_answered) >= 30 and len(ant_answered) >= 30:
            pct_act = act_answered["PROBLEMA_FUNCIONAMIENTO"].apply(_is_yes).mean() * 100
            pct_ant = ant_answered["PROBLEMA_FUNCIONAMIENTO"].apply(_is_yes).mean() * 100
            var_pdf = pct_act - pct_ant

            if abs(var_pdf) >= 1.5:
                # Find which device had most PdF impact (PdF variation × volume)
                device_detail = ""
                if "MODELO_DEVICE" in df_nps.columns:
                    dev_stats = []
                    for dev in act_answered["MODELO_DEVICE"].dropna().unique():
                        if dev == "Otro":
                            continue
                        d_act = act_answered[act_answered["MODELO_DEVICE"] == dev]
                        d_ant = ant_answered[ant_answered["MODELO_DEVICE"] == dev]
                        if len(d_act) < 10 or len(d_ant) < 10:
                            continue
                        pdf_act = d_act["PROBLEMA_FUNCIONAMIENTO"].apply(_is_yes).mean() * 100
                        pdf_ant = d_ant["PROBLEMA_FUNCIONAMIENTO"].apply(_is_yes).mean() * 100
                        pdf_var = pdf_act - pdf_ant
                        # Impact = PdF variation × share of base (volume weight)
                        share = len(d_ant) / len(ant_answered) * 100
                        impact = abs(pdf_var) * share / 100
                        dev_stats.append({
                            "device": dev, "pdf_var": pdf_var,
                            "pdf_act": pdf_act, "pdf_ant": pdf_ant,
                            "share": share, "impact": impact,
                        })
                    if dev_stats:
                        # Pick device with highest impact (PdF var × volume)
                        best = max(dev_stats, key=lambda x: x["impact"])
                        if abs(best["pdf_var"]) >= 1.5:
                            device_detail = (
                                f", principalmente en {best['device']}"
                                f" (PdF {best['pdf_ant']:.0f}%>{best['pdf_act']:.0f}%,"
                                f" {best['share']:.0f}% de la base)"
                            )

                # Calculate NPS lift: gap between sellers with vs without PdF
                nps_lift = ""
                con = act_answered[act_answered["PROBLEMA_FUNCIONAMIENTO"].apply(_is_yes)]
                sin = act_answered[~act_answered["PROBLEMA_FUNCIONAMIENTO"].apply(_is_yes)]
                if len(con) >= 10 and len(sin) >= 10:
                    nps_con = con["NPS"].mean() * 100
                    nps_sin = sin["NPS"].mean() * 100
                    gap = nps_con - nps_sin
                    impacto_nps = var_pdf / 100 * gap
                    nps_lift = f", NPS con problema {nps_con:.0f} vs sin problema {nps_sin:.0f} ({gap:+.0f}pp gap, impacto estimado: {impacto_nps:+.1f}pp NPS)"

                tendencias.append({
                    "tipo": "pdf",
                    "label": "Problemas de funcionamiento",
                    "pct_actual": round(pct_act, 1),
                    "pct_anterior": round(pct_ant, 1),
                    "variacion": round(var_pdf, 1),
                    "device_detail": device_detail,
                    "nps_lift": nps_lift,
                })

    # 2. Pricing penetration trend
    if "FLAG_PRICING" in df_nps.columns:
        act_pricing = df_q_act["FLAG_PRICING"].notna()
        ant_pricing = df_q_ant["FLAG_PRICING"].notna()
        if act_pricing.sum() >= 30 and ant_pricing.sum() >= 30:
            pct_act = (df_q_act["FLAG_PRICING"] == "Con pricing escalas").sum() / len(df_q_act) * 100
            pct_ant = (df_q_ant["FLAG_PRICING"] == "Con pricing escalas").sum() / len(df_q_ant) * 100
            var_pricing = pct_act - pct_ant
            if abs(var_pricing) >= 2.0:
                tendencias.append({
                    "tipo": "pricing",
                    "label": "Penetracion pricing por escalas",
                    "pct_actual": round(pct_act, 1),
                    "pct_anterior": round(pct_ant, 1),
                    "variacion": round(var_pricing, 1),
                })

    # 3. Promoter reasons: when complaints drop, show what promoters value
    if "PROMOTION_REASON_NPS" in df_nps.columns:
        prom_act = df_q_act[df_q_act["NPS"] == 1]
        prom_ant = df_q_ant[df_q_ant["NPS"] == 1]
        if len(prom_act) >= 30 and len(prom_ant) >= 30:
            # Calculate share of each promoter reason
            motivos_excl = ['outro', 'otros', 'sin informacion', 'otro - por favor']
            prom_shares_act = prom_act["PROMOTION_REASON_NPS"].value_counts(normalize=True) * 100
            prom_shares_ant = prom_ant["PROMOTION_REASON_NPS"].value_counts(normalize=True) * 100
            # Find reasons that grew most (positive signal)
            prom_movers = []
            for mot in prom_shares_act.index:
                if pd.isna(mot) or any(e in str(mot).lower() for e in motivos_excl):
                    continue
                sh_act = prom_shares_act.get(mot, 0)
                sh_ant = prom_shares_ant.get(mot, 0)
                var = sh_act - sh_ant
                if var >= 2 and sh_act >= 5:  # grew >=2pp and has >=5% share
                    prom_movers.append({
                        "motivo": str(mot),
                        "share_act": round(sh_act, 1),
                        "share_ant": round(sh_ant, 1),
                        "var": round(var, 1),
                    })
            if prom_movers:
                prom_movers.sort(key=lambda x: x["var"], reverse=True)
                tendencias.append({
                    "tipo": "promotores",
                    "movers": prom_movers[:2],  # top 2
                })

    # 3b. Cross-product insight for LINK/APICOW: sellers who also use Point vs only-OP
    update_tipo = (config or {}).get("update", {}).get("tipo", "all")
    if update_tipo in ("LINK", "APICOW") and "POINT_FLAG" in df_nps.columns:
        def _nps(sub):
            nota = sub["NOTA_NPS"].apply(float)
            return ((nota >= 9).sum() - (nota <= 6).sum()) / len(nota) * 100 if len(nota) > 0 else None

        for label, mask_fn in [
            ("con uso de Point", lambda d: d["POINT_FLAG"] == 1),
            ("only " + update_tipo, lambda d: d["POINT_FLAG"] == 0),
        ]:
            act = df_q_act[mask_fn(df_q_act)]
            ant = df_q_ant[mask_fn(df_q_ant)]
            if len(act) >= 15 and len(ant) >= 15:
                nps_act = _nps(act)
                nps_ant = _nps(ant)
                if nps_act is not None and nps_ant is not None:
                    var = nps_act - nps_ant
                    share = len(ant) / len(df_q_ant) * 100 if len(df_q_ant) > 0 else 0
                    tendencias.append({
                        "tipo": "cross_product",
                        "label": label,
                        "nps_actual": round(nps_act, 1),
                        "nps_anterior": round(nps_ant, 1),
                        "variacion": round(var, 1),
                        "share": round(share, 0),
                    })

    # 4. Newbie/Legacy mix shift: if share of newbies changed >=3pp, mention impact
    if "NEWBIE_LEGACY" in df_nps.columns:
        def _calc_nps(sub):
            if len(sub) == 0:
                return None
            valid = sub[sub["NPS"].notna()]
            return valid["NPS"].mean() * 100 if len(valid) > 0 else None

        for cohort in ["Newbie"]:
            c_act = df_q_act[df_q_act["NEWBIE_LEGACY"] == cohort]
            c_ant = df_q_ant[df_q_ant["NEWBIE_LEGACY"] == cohort]
            valid_act = df_q_act["NEWBIE_LEGACY"].notna().sum()
            valid_ant = df_q_ant["NEWBIE_LEGACY"].notna().sum()
            if valid_act >= 30 and valid_ant >= 30:
                share_act = len(c_act) / valid_act * 100
                share_ant = len(c_ant) / valid_ant * 100
                var_share = share_act - share_ant
                if abs(var_share) >= 3:
                    nps_newbie = _calc_nps(c_act) if len(c_act) >= 10 else None
                    leg_act = df_q_act[df_q_act["NEWBIE_LEGACY"] == "Legacy"]
                    nps_legacy = _calc_nps(leg_act) if len(leg_act) >= 10 else None
                    gap = (nps_newbie - nps_legacy) if (nps_newbie is not None and nps_legacy is not None) else None
                    tendencias.append({
                        "tipo": "newbie_mix",
                        "share_actual": round(share_act, 1),
                        "share_anterior": round(share_ant, 1),
                        "var_share": round(var_share, 1),
                        "nps_newbie": round(nps_newbie, 1) if nps_newbie is not None else None,
                        "nps_legacy": round(nps_legacy, 1) if nps_legacy is not None else None,
                        "gap": round(gap, 1) if gap is not None else None,
                    })

    # 5. Region trend (only MLA): detect provinces with significant NPS variation
    if "REGION" in df_nps.columns and site == "MLA":
        def _nps_region(sub):
            nota = sub["NOTA_NPS"].apply(float) if "NOTA_NPS" in sub.columns else sub["NPS"]
            if "NOTA_NPS" in sub.columns:
                return ((nota >= 9).sum() - (nota <= 6).sum()) / len(nota) * 100 if len(nota) > 0 else None
            return sub["NPS"].mean() * 100 if len(sub) > 0 else None

        reg_movers = []
        for reg in df_nps["REGION"].dropna().unique():
            if reg in ("Sin Dato", "Sin dato", "None", ""):
                continue
            r_act = df_q_act[df_q_act["REGION"] == reg]
            r_ant = df_q_ant[df_q_ant["REGION"] == reg]
            if len(r_act) >= 15 and len(r_ant) >= 15:
                nps_a = _nps_region(r_act)
                nps_b = _nps_region(r_ant)
                if nps_a is not None and nps_b is not None:
                    var = nps_a - nps_b
                    share = len(r_act) / len(df_q_act) * 100 if len(df_q_act) > 0 else 0
                    if abs(var) >= 5 and share >= 5:
                        reg_movers.append({
                            "region": reg, "nps_act": round(nps_a, 1),
                            "nps_ant": round(nps_b, 1), "var": round(var, 1),
                            "share": round(share, 0),
                        })
        if reg_movers:
            reg_movers.sort(key=lambda x: abs(x["var"]), reverse=True)
            tendencias.append({
                "tipo": "region",
                "movers": reg_movers[:3],  # top 3
            })

    return tendencias


def _generar_parrafo_tendencias(tendencias: list, direccion_nps: int) -> str:
    """Generate narrative for non-complaint metric trends."""
    if not tendencias:
        return ""

    parts = []
    for t in tendencias:
        if t["tipo"] == "pdf":
            if t["variacion"] < 0:
                parts.append(
                    f"Recupero en problemas de funcionamiento: "
                    f"PdF bajo de {t['pct_anterior']:.0f}% a {t['pct_actual']:.0f}% "
                    f"({t['variacion']:+.1f}pp QvsQ){t.get('device_detail', '')}{t.get('nps_lift', '')}"
                )
            else:
                parts.append(
                    f"Aumento de problemas de funcionamiento: "
                    f"PdF subio de {t['pct_anterior']:.0f}% a {t['pct_actual']:.0f}% "
                    f"({t['variacion']:+.1f}pp QvsQ){t.get('device_detail', '')}{t.get('nps_lift', '')}"
                )
        elif t["tipo"] == "pricing":
            dir_p = "crecio" if t["variacion"] > 0 else "cayo"
            parts.append(
                f"Penetracion de pricing por escalas {dir_p} de "
                f"{t['pct_anterior']:.0f}% a {t['pct_actual']:.0f}% ({t['variacion']:+.1f}pp)"
            )

    # Cross-product: group all cross_product entries into one coherent sentence
    cross_items = [t for t in tendencias if t["tipo"] == "cross_product"]
    if len(cross_items) >= 2:
        # Find the one that moved most vs the one that stayed flat
        movers = sorted(cross_items, key=lambda x: abs(x["variacion"]), reverse=True)
        top = movers[0]
        flat = movers[-1]
        if abs(top["variacion"]) >= 3 and abs(flat["variacion"]) <= 2:
            parts.append(
                f"Sellers {top['label']} recuperaron NPS "
                f"(de {top['nps_anterior']:.0f} a {top['nps_actual']:.0f}, "
                f"{top['variacion']:+.0f}pp, {top['share']:.0f}% del mix) "
                f"mientras sellers {flat['label']} se mantienen flat "
                f"({flat['variacion']:+.1f}pp)"
            )
    elif len(cross_items) == 1:
        t = cross_items[0]
        if abs(t["variacion"]) >= 3:
            dir_c = "subieron" if t["variacion"] > 0 else "bajaron"
            parts.append(
                f"Sellers {t['label']} {dir_c} NPS "
                f"(de {t['nps_anterior']:.0f} a {t['nps_actual']:.0f}, "
                f"{t['variacion']:+.0f}pp, {t['share']:.0f}% del mix)"
            )

    # Promoter reasons (positive signals)
    prom_items = [t for t in tendencias if t["tipo"] == "promotores"]
    if prom_items:
        movers = prom_items[0]["movers"]
        mot_texts = [f"{m['motivo']} ({m['share_act']:.0f}%, {m['var']:+.1f}pp)" for m in movers]
        parts.append("Promotores destacan: " + ", ".join(mot_texts))

    # Newbie mix shift
    newbie_items = [t for t in tendencias if t["tipo"] == "newbie_mix"]
    if newbie_items:
        n = newbie_items[0]
        if n["var_share"] > 0:
            txt = f"Mayor share de sellers nuevos (Newbies {n['share_anterior']:.0f}%→{n['share_actual']:.0f}%, {n['var_share']:+.1f}pp)"
        else:
            txt = f"Menor share de sellers nuevos (Newbies {n['share_anterior']:.0f}%→{n['share_actual']:.0f}%, {n['var_share']:+.1f}pp)"
        if n.get("gap") is not None:
            txt += f", NPS Newbies {n['nps_newbie']:.0f} vs Legacy {n['nps_legacy']:.0f} ({n['gap']:+.0f}pp gap)"
        parts.append(txt)

    # Region movers (MLA only)
    reg_items = [t for t in tendencias if t["tipo"] == "region"]
    if reg_items:
        movers = reg_items[0]["movers"]
        mover_texts = []
        for m in movers:
            dir_r = "subió" if m["var"] > 0 else "bajó"
            mover_texts.append(f"{m['region']} {dir_r} {abs(m['var']):.0f}pp (NPS {m['nps_ant']:.0f}→{m['nps_act']:.0f}, {m['share']:.0f}% de la base)")
        parts.append("Variaciones por provincia: " + ", ".join(mover_texts))

    if not parts:
        return ""
    return "Adicionalmente: " + "; ".join(parts) + "."


# ===================================================================
# Contexto de eventos externos
# ===================================================================

def _generar_parrafo_contexto(site: str, quarter_actual: str | None, config: dict | None) -> str:
    """Load external events from config/eventos_externos.yaml and generate context paragraph."""
    from pathlib import Path
    import yaml as _yaml

    eventos_path = Path(__file__).parent.parent.parent.parent / "config" / "eventos_externos.yaml"
    if not eventos_path.exists():
        return ""

    try:
        with open(eventos_path, "r", encoding="utf-8") as f:
            data = _yaml.safe_load(f)
    except Exception:
        return ""

    eventos = data.get("eventos", [])
    if not eventos:
        return ""

    matching = []
    for ev in eventos:
        if ev.get("quarter") != quarter_actual:
            continue
        sites = ev.get("sites", [])
        if site not in sites and "all" not in sites:
            continue
        matching.append(ev)

    if not matching:
        return ""

    parts = []
    for ev in matching:
        evento = ev.get("evento", "")
        impacto = ev.get("impacto", "")
        if evento:
            txt = f"<b>{evento}</b>"
            if impacto:
                txt += f": {impacto}"
            parts.append(txt)

    if not parts:
        return ""

    return "Contexto: " + ". ".join(parts) + "."


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
