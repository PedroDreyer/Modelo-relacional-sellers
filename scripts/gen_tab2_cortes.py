"""
Tab 2 — Cortes & Drivers: dimensiones agrupadas por motivo (B3.x) +
cortes sin motivo asociado (B4.x). Formato unificado encuesta + real.

Exporta `generar_tab2()` que devuelve HTML string.
"""

from __future__ import annotations


def _val_cls(v):
    if v > 0.5:
        return "val-neg"
    if v < -0.5:
        return "val-pos"
    return "val-neutral"


def _nps_cls(v):
    if v >= 1:
        return "val-pos"
    if v <= -1:
        return "val-neg"
    return "val-neutral"


CLASIF_CSS = {
    "EXPLICA_OK": ("classify-ok", "#2e7d32", "#e8f5e9"),
    "EXPLICA_MIX": ("classify-mix", "#e65100", "#fff3e0"),
    "NO_EXPLICA": ("classify-no", "#757575", "#f5f5f5"),
    "CONTRADICTORIO": ("classify-contra", "#c62828", "#fce4ec"),
    "FALLBACK_CP5": ("classify-no", "#e65100", "#fff3e0"),
}

MOTIVO_COLORS = [
    "#e53935", "#2E7D32", "#4caf50", "#3F51B5", "#7E57C2",
    "#FF9800", "#009688", "#795548",
]


# ── unified table builder ────────────────────────────────────────────

def _build_unified_table(
    dim_list: list[dict],
    dim_label: str,
    q_ant: str,
    q_act: str,
    meses_q_ant: list[str],
    meses_q_act: list[str],
    has_real: bool = False,
    source_label: str = "",
    q_yoy: str = "",
    meses_q_yoy: list[str] | None = None,
) -> str:
    """Builds a unified Encuesta + Universo Total table for a dimension."""
    if not dim_list:
        return ""

    h = f'<div class="mix-section"><h3>{dim_label}'
    h += ' <span class="dim-chip encuesta">ENCUESTA</span>'
    if has_real and source_label:
        h += f' <span class="dim-chip real">{source_label}</span>'
    h += "</h3>\n"

    # Header
    h += '<table class="mix-table"><thead>'
    if has_real:
        h += f'<tr><th rowspan="2" style="vertical-align:bottom;">Dimensión</th>'
        h += f'<th colspan="5" style="background:#e8eaf6;color:#3949ab;border-bottom:1px solid #c5cae9;">📋 Encuesta</th>'
        h += f'<th colspan="3" style="background:#e8f5e9;color:#2e7d32;border-bottom:1px solid #c8e6c9;">📊 Universo Total</th></tr>'
        h += f'<tr>'
        h += f'<th style="background:#e8eaf6;color:#3949ab;">NPS {q_ant}</th>'
        h += f'<th style="background:#e8eaf6;color:#3949ab;">NPS {q_act}</th>'
        h += f'<th style="background:#e8eaf6;color:#3949ab;">Δ NPS</th>'
        h += f'<th style="background:#e8eaf6;color:#3949ab;">Share {q_ant}</th>'
        h += f'<th style="background:#e8eaf6;color:#3949ab;">Share {q_act}</th>'
        h += f'<th style="background:#e8f5e9;color:#2e7d32;">Share {q_ant}</th>'
        h += f'<th style="background:#e8f5e9;color:#2e7d32;">Share {q_act}</th>'
        h += f'<th style="background:#e8f5e9;color:#2e7d32;">Δ Share</th></tr>'
    else:
        h += f'<tr><th>Dimensión</th>'
        h += f'<th>NPS {q_ant}</th><th>NPS {q_act}</th><th>Δ NPS</th>'
        h += f'<th>Share {q_ant}</th><th>Share {q_act}</th><th>Δ Share</th></tr>'
    h += "</thead><tbody>"

    # Custom sort order for specific dimensions
    CUSTOM_ORDER = {
        "Alta (≥95%)": 0, "Media (85-95%)": 1, "Baja (<85%)": 2, "Sin datos": 3,
        "Usa inversiones": 0, "No usa inversiones": 1,
        "Usa credito": 0, "No usa credito": 1,
        "Tiene TC MP": 0, "Sin TC MP": 1,
        "Con Top Off": 0, "Sin Top Off": 1,
        "Con uso Point": 0, "Sin uso Point": 1, "Sin dato": 2,
        "Legacy": 0, "Newbie": 1,
    }
    sorted_list = sorted(dim_list, key=lambda x: CUSTOM_ORDER.get(x.get("dimension", ""), 99))

    for item in sorted_list:
        dv = item.get("dimension", "N/A")
        nps_mes = item.get("nps_por_mes", {})
        shares_mes = item.get("shares_por_mes", {})

        # Use quarter-level NPS if available (direct average), fallback to avg of monthly
        na = item.get("nps_q_anterior")
        if na is None:
            nps_vals_ant = [nps_mes.get(m) for m in meses_q_ant if nps_mes.get(m) is not None]
            na = sum(nps_vals_ant) / len(nps_vals_ant) if nps_vals_ant else None
        nb = item.get("nps_q_actual")
        if nb is None:
            nps_vals_act = [nps_mes.get(m) for m in meses_q_act if nps_mes.get(m) is not None]
            nb = sum(nps_vals_act) / len(nps_vals_act) if nps_vals_act else None

        sh_vals_ant = [shares_mes.get(m) for m in meses_q_ant if shares_mes.get(m) is not None]
        sh_vals_act = [shares_mes.get(m) for m in meses_q_act if shares_mes.get(m) is not None]
        sha = sum(sh_vals_ant) / len(sh_vals_ant) if sh_vals_ant else None
        shb = sum(sh_vals_act) / len(sh_vals_act) if sh_vals_act else None

        vn = (nb - na) if (na is not None and nb is not None) else None
        dsh = (shb - sha) if (sha is not None and shb is not None) else None

        na_s = f"{na:.0f}" if na is not None else "—"
        nb_s = f"{nb:.0f}" if nb is not None else "—"
        vn_s = f'<td class="{_nps_cls(vn)}">{vn:+.1f}</td>' if vn is not None else '<td class="val-neutral">—</td>'
        sha_s = f"{sha:.1f}%" if sha is not None else "—"
        shb_s = f"{shb:.1f}%" if shb is not None else "—"
        dsh_s = f'<td class="{_val_cls(-dsh if dsh else 0)}">{dsh:+.1f}pp</td>' if dsh is not None else '<td class="val-neutral">—</td>'

        h += f"<tr><td>{dv}</td><td>{na_s}</td><td>{nb_s}</td>{vn_s}"
        h += f"<td>{sha_s}</td><td>{shb_s}</td>"
        if has_real:
            real_mes = item.get("shares_real_por_mes", {})
            r_ant = [real_mes.get(m) for m in meses_q_ant if real_mes.get(m) is not None]
            r_act = [real_mes.get(m) for m in meses_q_act if real_mes.get(m) is not None]
            ra = sum(r_ant) / len(r_ant) if r_ant else None
            rb = sum(r_act) / len(r_act) if r_act else None
            dr = (rb - ra) if (ra is not None and rb is not None) else None
            ra_s = f"{ra:.1f}%" if ra is not None else "—"
            rb_s = f"{rb:.1f}%" if rb is not None else "—"
            dr_s = f'<td class="{_val_cls(-dr if dr else 0)}">{dr:+.1f}pp</td>' if dr is not None else '<td class="val-neutral">—</td>'
            h += f"<td>{ra_s}</td><td>{rb_s}</td>{dr_s}"
        else:
            h += dsh_s
        h += "</tr>"

    h += "</tbody></table></div>\n"
    return h


# ── association box builder ──────────────────────────────────────────

_CLASIF_LABELS = {
    "EXPLICA_OK": "Data + Voz",
    "EXPLICA_MIX": "Parcial",
    "NO_EXPLICA": "Sin driver",
    "CONTRADICTORIO": "Contradictorio",
    "FALLBACK_CP5": "Voz del seller",
}

def _build_assoc_box(asoc: dict) -> str:
    cls_key = asoc.get("clasificacion", "NO_EXPLICA")
    css_cls, fg, bg = CLASIF_CSS.get(cls_key, ("classify-no", "#757575", "#f5f5f5"))
    motivo = asoc.get("motivo", "")
    var = asoc.get("var_share", 0)
    wording = asoc.get("wording", "")
    causa = asoc.get("causa_raiz", "")
    label = _CLASIF_LABELS.get(cls_key, cls_key)
    h = f"""<div class="assoc-box" style="border-color:{fg};background:{bg};">
      <div class="label"><span class="classify-tag {css_cls}">{label}</span> <strong>{motivo} ({var:+.1f}pp)</strong></div>
      <p>{wording}</p>"""
    h += "</div>\n"
    return h


# ── motivo → dimension mapping ───────────────────────────────────────

MOTIVO_DIM_MAP = {
    "credito": {
        "dims": ["CREDIT_GROUP", "FLAG_USA_CREDITO", "FLAG_TARJETA_CREDITO", "ESTADO_OFERTA_CREDITO"],
        "label": "Créditos & Financiamiento",
        "source": "LK_MP_MAUS_CREDIT_PROFILE",
        "has_real": True,
    },
    "tasas": {
        "dims": ["RANGO_TPV", "RANGO_TPN"],
        "label": "Pricing / Escalas",
        "source": "LK_MP_MASTER_SELLERS",
        "has_real": True,
    },
    "inversiones": {
        "dims": ["FLAG_USA_INVERSIONES", "FLAG_INVERSIONES", "FLAG_ASSET", "FLAG_WINNER", "FLAG_POTS_ACTIVO"],
        "label": "Inversiones",
        "source": "DM_MP_INVESTMENTS_BY_PRODUCT",
        "has_real": True,
    },
    "calidad": {
        "dims": ["PROBLEMA_FUNCIONAMIENTO", "TIPO_PROBLEMA"],
        "label": "Problemas de funcionamiento",
        "source": "BT_NPS_TX_SELLERS_MP_DETAIL",
        "has_real": False,
    },
    "atencion": {
        "dims": ["FLAG_TOPOFF"],
        "label": "Atención al cliente",
        "source": "BT_CX_SELLERS_MP_TOP_OFF",
        "has_real": True,
    },
    "cobros_rechazados": {
        "dims": ["RANGO_APROBACION"],
        "label": "Tasa de Aprobación",
        "source": "BT_SCO_ORIGIN_REPORT",
        "has_real": True,
    },
}

MOTIVO_PATTERNS = {
    "credito": ["credito", "credit", "empresti", "cartao", "financ", "fred"],
    "tasas": ["tax", "comis", "tasas", "tarifa", "pric"],
    "inversiones": ["invest", "poupan", "cofrin", "invers", "retorno"],
    "calidad": ["funcion", "qualidade", "maquininha", "device", "problem"],
    "atencion": ["atend", "atencion", "top off", "cliente"],
    "cobros_rechazados": ["rechaz", "recusado", "pagamentos recusados", "cobros rechaz", "aprovacao", "aprovação"],
}

CORTES_SIN_MOTIVO = [
    ("PRODUCTO_PRINCIPAL", "Producto Principal", "LK_MP_MASTER_SELLERS"),
    ("E_CODE", "Segmento (E_CODE)", ""),
    ("PF_PJ", "PF / PJ", "LK_KYC_VAULT_USER"),
    ("NEWBIE_LEGACY", "Newbie / Legacy", "LK_MP_SEGMENTATION_SELLERS"),
    ("FLAG_ONLY_TRANSFER", "Flag Only Transfer", "LK_MP_SEGMENTATION_SELLERS"),
    ("SEGMENTO_TAMANO_SELLER", "Segmento Tamaño", ""),
    ("SEGMENTO_CROSSMP", "Cross MP", ""),
]


DIM_LABELS = {
    "CREDIT_GROUP": "FRED",
    "FLAG_USA_CREDITO": "Uso de Crédito",
    "FLAG_TARJETA_CREDITO": "Tarjeta de Crédito MP",
    "ESTADO_OFERTA_CREDITO": "Estado Oferta Crédito",
    "RANGO_TPV": "RANGO_TPV (USD)",
    "RANGO_TPN": "RANGO_TPN (transacciones)",
    "FLAG_USA_INVERSIONES": "FLAG_INVERSIONES",
    "FLAG_INVERSIONES": "FLAG_INVERSIONES (binario)",
    "FLAG_ASSET": "FLAG_ASSET (Cuenta Remunerada)",
    "FLAG_WINNER": "FLAG_WINNER (Rendimiento PLUS)",
    "FLAG_POTS_ACTIVO": "FLAG_POTS_ACTIVO",
    "PROBLEMA_FUNCIONAMIENTO": "Problema Funcionamiento",
    "TIPO_PROBLEMA": "Tipo de Problema",
    "FLAG_TOPOFF": "FLAG_TOPOFF",
    "RANGO_APROBACION": "Rango Aprobación",
}


def _match_motivo(motivo_name: str) -> str | None:
    import unicodedata
    raw = (motivo_name or "").lower()
    # Normalizar acentos: é→e, ã→a, etc.
    low = unicodedata.normalize("NFD", raw)
    low = "".join(c for c in low if unicodedata.category(c) != "Mn")
    for key, patterns in MOTIVO_PATTERNS.items():
        if any(p in low for p in patterns):
            return key
    return None


# ── main generator ──────────────────────────────────────────────────

def generar_tab2(
    *,
    dimensiones_ch1: dict,
    razonamiento: dict,
    q_label_ant: str,
    q_label_act: str,
    q_label_yoy: str = "",
    meses_q_ant: list[str],
    meses_q_act: list[str],
    meses_q_yoy: list[str] | None = None,
    variaciones_quejas: list[dict],
    update_tipo: str = "all",
) -> str:
    """Genera el HTML completo de Tab 2 (Cortes & Drivers)."""

    # Dimensions to hide per update (redundant with the filter)
    DIMS_TO_HIDE = {
        "SMBs": {"FLAG_ONLY_TRANSFER", "SEGMENTO_TAMANO_SELLER", "E_CODE"},
        "Point": {"PRODUCTO_PRINCIPAL", "FLAG_ONLY_TRANSFER"},
        "LINK": {"PRODUCTO_PRINCIPAL", "FLAG_ONLY_TRANSFER", "SEGMENTO_TAMANO_SELLER"},
        "APICOW": {"PRODUCTO_PRINCIPAL", "FLAG_ONLY_TRANSFER", "SEGMENTO_TAMANO_SELLER"},
    }
    dims_to_hide = DIMS_TO_HIDE.get(update_tipo, set())

    bloque3 = razonamiento.get("bloque3", {})
    asociaciones = bloque3.get("asociaciones", [])

    # Index associations by motivo
    asoc_by_motivo = {}
    for a in asociaciones:
        asoc_by_motivo.setdefault(a.get("motivo", ""), []).append(a)

    # Determine which motivos have drivers
    motivos_con_driver: list[tuple[str, float, str]] = []
    motivos_sin_driver: list[tuple[str, float]] = []

    for vq in variaciones_quejas:
        motivo = vq.get("motivo")
        if motivo is None:
            continue
        var = vq.get("var_mom", 0)
        matched = _match_motivo(motivo)
        if matched:
            motivos_con_driver.append((motivo, var, matched))
        else:
            motivos_sin_driver.append((motivo, var))

    html = """
  <div class="section">
    <div class="section-title">Cortes & Drivers</div>
    <p class="text-sm text-muted" style="margin-bottom:4px;">Organizado por <strong>motivo de queja</strong> con sus drivers (encuesta + dato real) debajo. Al final, cortes sin motivo asociado.</p>
  </div>
"""

    # ── Motivos con drivers ──────────────────────────────────────────
    seen_dims = set()
    for idx, (motivo, var, driver_key) in enumerate(motivos_con_driver):
        cfg = MOTIVO_DIM_MAP.get(driver_key, {})
        bc = MOTIVO_COLORS[idx % len(MOTIVO_COLORS)]

        if var > 0.3:
            badge = f'<span class="badge badge-up" style="font-size:12px;vertical-align:middle;margin-left:8px;">+{var:.1f}pp QvsQ</span>'
        elif var < -0.3:
            badge = f'<span class="badge badge-down" style="font-size:12px;vertical-align:middle;margin-left:8px;">{var:.1f}pp QvsQ</span>'
        else:
            badge = f'<span class="badge badge-stable" style="font-size:12px;vertical-align:middle;margin-left:8px;">{var:+.1f}pp</span>'

        # Association summary for the header
        asocs = asoc_by_motivo.get(motivo, [])
        causa_preview = ""
        if asocs and asocs[0].get("causa_raiz"):
            causa_preview = f' — <span style="color:#666;font-weight:normal;font-size:13px;">{asocs[0]["causa_raiz"]}</span>'

        html += f"""
  <details class="section" style="border-left:4px solid {bc};">
    <summary style="cursor:pointer;list-style:none;padding:12px 0;">
      <div class="section-title" style="border-bottom:none;margin-bottom:0;display:inline;">{motivo} {badge}{causa_preview}</div>
      <span style="float:right;color:#999;font-size:18px;">&#9660;</span>
    </summary>
    <div style="padding:8px 0 16px;">
"""
        for dim_key in cfg.get("dims", []):
            if dim_key in seen_dims:
                continue
            seen_dims.add(dim_key)
            dim_data = dimensiones_ch1.get(dim_key, [])
            if not dim_data:
                continue
            lbl = DIM_LABELS.get(dim_key, dim_key)
            html += _build_unified_table(
                dim_data, lbl, q_label_ant, q_label_act,
                meses_q_ant, meses_q_act,
                has_real=cfg.get("has_real", False),
                source_label=cfg.get("source", ""),
                q_yoy=q_label_yoy,
                meses_q_yoy=meses_q_yoy,
            )

        # Association box
        for a in asocs:
            html += _build_assoc_box(a)

        # If no dims had data, show placeholder
        if not any(dimensiones_ch1.get(dk) for dk in cfg.get("dims", [])):
            html += f'<div style="padding:24px;background:#f5f5f5;border-radius:8px;text-align:center;color:#999;">Dato real pendiente. Habilitar enriquecimiento para {cfg.get("label", driver_key)}.</div>\n'

        html += "    </div>\n  </details>\n"

    # ── Cortes sin motivo (sin header, solo tablas) ─────────────────
    for dim_key, label, source in CORTES_SIN_MOTIVO:
        if dim_key in seen_dims or dim_key in dims_to_hide:
            continue
        dim_data = dimensiones_ch1.get(dim_key, [])
        if not dim_data:
            continue
        # Skip dimensions where no item has NPS data for Q anterior or Q actual
        has_any_nps = any(
            any(item.get("nps_por_mes", {}).get(m) is not None for m in meses_q_ant + meses_q_act)
            for item in dim_data
        )
        if not has_any_nps:
            continue
        has_real = bool(source)
        html += '  <div class="section">\n'
        html += f'    <div class="dim-group-header">{label}'
        html += ' <span class="dim-chip encuesta">ENCUESTA</span>'
        if has_real:
            html += f' <span class="dim-chip real">{source}</span>'
        html += "</div>\n"
        html += _build_unified_table(
            dim_data, label, q_label_ant, q_label_act,
            meses_q_ant, meses_q_act,
            has_real=has_real,
            source_label=source,
            q_yoy=q_label_yoy,
            meses_q_yoy=meses_q_yoy,
        )
        html += "  </div>\n"

    return html
