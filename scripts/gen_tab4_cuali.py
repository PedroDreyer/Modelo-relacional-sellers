"""
Tab 4 — Análisis Cualitativo: causas raíz + voz del seller + driver de data.

Diseño profesional:
  - Card por motivo con header, composición, y driver de data integrado
  - Causas raíz con barra de frecuencia, descripción visible, y ejemplos expandibles
  - Conexión con razonamiento (Bloque 3) para mostrar data + voz unificados

Exporta `generar_tab4()` que devuelve HTML string.
"""

from __future__ import annotations
import html as html_mod

# Colores para barras de frecuencia por causa (soft, distinguishable)
CAUSA_COLORS = [
    "#5c6bc0", "#42a5f5", "#26a69a", "#66bb6a",
    "#ffa726", "#ef5350", "#ab47bc", "#78909c",
    "#8d6e63", "#ec407a",
]


def _esc(text: str) -> str:
    return html_mod.escape(text) if text else ""


def _badge(var: float) -> str:
    if var > 0.3:
        return f'<span class="badge badge-up" style="font-size:11px;vertical-align:middle;">{var:+.1f}pp QvsQ</span>'
    elif var < -0.3:
        return f'<span class="badge badge-down" style="font-size:11px;vertical-align:middle;">{var:.1f}pp QvsQ</span>'
    return f'<span class="badge badge-stable" style="font-size:11px;vertical-align:middle;">{var:+.1f}pp</span>'


def _find_driver_for_motivo(motivo: str, razonamiento: dict | None) -> dict | None:
    """Find the razonamiento association for a motivo (fuzzy match)."""
    if not razonamiento:
        return None
    import unicodedata
    def _norm(s):
        s = unicodedata.normalize("NFD", (s or "").lower())
        return "".join(c for c in s if unicodedata.category(c) != "Mn")

    motivo_norm = _norm(motivo)
    for asoc in razonamiento.get("bloque3", {}).get("asociaciones", []):
        if _norm(asoc.get("motivo", "")) == motivo_norm:
            return asoc
        # Partial match: check if motivo words are contained
        asoc_norm = _norm(asoc.get("motivo", ""))
        if motivo_norm and asoc_norm and (motivo_norm in asoc_norm or asoc_norm in motivo_norm):
            return asoc
    return None


_CLASIF_COLORS = {
    "EXPLICA_OK": ("#2e7d32", "#e8f5e9"),
    "EXPLICA_MIX": ("#e65100", "#fff3e0"),
    "NO_EXPLICA": ("#757575", "#f5f5f5"),
    "CONTRADICTORIO": ("#c62828", "#ffebee"),
    "FALLBACK_CP5": ("#1565c0", "#e3f2fd"),
}
_CLASIF_LABELS = {
    "EXPLICA_OK": "Data + Voz",
    "EXPLICA_MIX": "Parcial",
    "NO_EXPLICA": "Sin driver",
    "CONTRADICTORIO": "Contradictorio",
    "FALLBACK_CP5": "Voz del seller",
}


def generar_tab4(
    *,
    causas_raiz_data: dict | None,
    retagueo_data: dict | None,
    comments_variaciones_data: dict | None,
    variaciones_quejas: list[dict],
    q_label_ant: str = "",
    q_label_act: str = "",
    razonamiento: dict | None = None,
) -> str:
    """Genera HTML de Tab 4 (Análisis Cualitativo)."""

    html = ""

    if not causas_raiz_data or not causas_raiz_data.get("causas_por_motivo"):
        return '<div class="section"><p class="text-sm text-muted">No hay análisis cualitativo disponible.</p></div>\n'

    # Build var lookup from variaciones_quejas
    var_map = {}
    for vq in variaciones_quejas:
        m = vq.get("motivo")
        if m:
            var_map[m] = vq

    html += """
  <div class="section">
    <div class="section-title">Análisis Cualitativo — Voz del Seller</div>
    <p class="text-sm text-muted" style="margin-bottom:16px;">
      Causas raíz identificadas por análisis semántico de comentarios de detractores y neutros.
      Cada motivo muestra las causas principales, su frecuencia, y la conexión con drivers de datos reales.
    </p>
"""

    for idx, (motivo, datos) in enumerate(causas_raiz_data["causas_por_motivo"].items()):
        # Get variation data
        vq_data = var_map.get(motivo, {})
        var = vq_data.get("var_mom", 0)
        share_act = vq_data.get("share_actual", None)

        composicion = datos.get("composicion", {})
        total_com = datos.get("total_comentarios_analizados", 0)
        det = composicion.get("detractores", 0)
        neu = composicion.get("neutros", 0)
        det_pct = round(det / total_com * 100) if total_com else 0
        neu_pct = 100 - det_pct

        # Find driver association from razonamiento
        driver = _find_driver_for_motivo(motivo, razonamiento)

        # Collect causas
        causas = list(datos.get("causas_raiz", {}).values())

        # Card
        html += f"""
    <details class="section" style="border-left:4px solid {CAUSA_COLORS[idx % len(CAUSA_COLORS)]};margin-bottom:12px;" open>
      <summary style="cursor:pointer;list-style:none;padding:12px 0;">
        <div style="display:flex;align-items:center;gap:10px;flex-wrap:wrap;">
          <h4 style="margin:0;font-size:15px;">{_esc(motivo)}</h4>
          {_badge(var)}
          <span style="font-size:12px;color:#888;margin-left:auto;">{total_com} comentarios analizados</span>
          <span style="float:right;color:#999;font-size:18px;">&#9660;</span>
        </div>
      </summary>
      <div style="padding:4px 0 12px;">
"""

        # Composición bar (det vs neu) with labels
        html += f"""
        <div style="display:flex;align-items:center;gap:8px;margin-bottom:12px;">
          <div style="flex:1;display:flex;height:8px;border-radius:4px;overflow:hidden;background:#f0f0f0;">
            <div style="width:{det_pct}%;background:#ef5350;" title="Detractores: {det}"></div>
            <div style="width:{neu_pct}%;background:#ffa726;" title="Neutros: {neu}"></div>
          </div>
          <span style="font-size:11px;color:#ef5350;font-weight:600;">{det} det</span>
          <span style="font-size:11px;color:#ffa726;font-weight:600;">{neu} neu</span>
        </div>
"""

        # Driver box (from razonamiento)
        if driver:
            cls_key = driver.get("clasificacion", "NO_EXPLICA")
            fg, bg = _CLASIF_COLORS.get(cls_key, ("#757575", "#f5f5f5"))
            label = _CLASIF_LABELS.get(cls_key, cls_key)
            wording = driver.get("wording", "")

            html += f"""
        <div style="background:{bg};border:1px solid {fg}22;border-radius:8px;padding:10px 14px;margin-bottom:14px;">
          <div style="display:flex;align-items:center;gap:8px;margin-bottom:4px;">
            <span style="background:{fg};color:white;font-size:10px;font-weight:700;padding:2px 8px;border-radius:4px;text-transform:uppercase;">{label}</span>
          </div>
          <p style="margin:0;font-size:13px;color:#333;line-height:1.5;">{_esc(wording)}</p>
        </div>
"""

        # Causas raíz
        if causas:
            max_freq = max((c.get("frecuencia_pct", 0) for c in causas), default=1) or 1

            for cidx, causa in enumerate(causas):
                titulo = causa.get("titulo", "")
                descripcion = causa.get("descripcion", "")
                freq = causa.get("frecuencia_pct", 0)
                freq_abs = causa.get("frecuencia_abs", 0)
                freq_ant = causa.get("frecuencia_pct_anterior", None)
                bar_w = max(freq / max_freq * 100, 4)
                bar_color = CAUSA_COLORS[cidx % len(CAUSA_COLORS)]
                ejemplos = causa.get("ejemplos", [])

                # Variation badge for sub-cause
                var_badge = ""
                if freq_ant is not None:
                    var_sub = freq - freq_ant
                    if abs(var_sub) >= 1:
                        vcls = "badge-up" if var_sub > 0 else "badge-down"
                        var_badge = f' <span class="{vcls}" style="font-size:10px;padding:1px 5px;">{var_sub:+.0f}pp vs Q ant</span>'

                html += f"""
        <div style="margin-bottom:10px;padding:10px 14px;background:#fafafa;border-radius:8px;border:1px solid #eee;">
          <div style="display:flex;align-items:center;gap:8px;margin-bottom:6px;">
            <div style="font-weight:600;font-size:13px;flex:1;color:#333;">{_esc(titulo)}{var_badge}</div>
            <div style="font-size:12px;font-weight:700;color:{bar_color};white-space:nowrap;">{freq:.0f}% ({freq_abs})</div>
          </div>
          <div style="height:6px;border-radius:3px;background:#e0e0e0;margin-bottom:8px;">
            <div style="width:{bar_w:.0f}%;height:100%;border-radius:3px;background:{bar_color};"></div>
          </div>
"""
                # Description always visible
                if descripcion:
                    html += f'          <p style="margin:0 0 8px;font-size:12px;color:#666;line-height:1.4;">{_esc(descripcion)}</p>\n'

                # Example comments (always show up to 3, expandable for more)
                if ejemplos:
                    shown = ejemplos[:3]
                    for ej in shown:
                        if isinstance(ej, dict):
                            comment = ej.get("comentario", "")
                            cust_id = ej.get("cust_id", ej.get("CUS_CUST_ID", ""))
                        else:
                            comment = str(ej)
                            cust_id = ""
                        cust_tag = f' <span style="color:#bbb;font-size:10px;">[{cust_id}]</span>' if cust_id else ""
                        html += f'          <div style="font-size:12px;color:#555;padding:4px 0 4px 12px;border-left:2px solid #ddd;margin-bottom:4px;font-style:italic;">"{_esc(comment[:400])}"{cust_tag}</div>\n'

                    if len(ejemplos) > 3:
                        html += f'          <div style="font-size:11px;color:#999;padding-left:12px;">+{len(ejemplos) - 3} comentarios más</div>\n'

                html += "        </div>\n"

        html += """
      </div>
    </details>
"""

    html += "  </div>\n"

    # ── Retagueo de "Otros" ──────────────────────────────────────────
    if retagueo_data and retagueo_data.get("resumen_retagueo"):
        html += """
  <div class="section">
    <div class="section-title">Retagueo de "Otros"</div>
    <table class="mix-table"><thead><tr>
      <th>Motivo Reclasificado</th><th>Cantidad</th><th>%</th><th>Descripción</th>
    </tr></thead><tbody>
"""
        for mot, info in retagueo_data["resumen_retagueo"].items():
            html += f'      <tr><td>{mot}</td><td>{info.get("cantidad", 0)}</td>'
            html += f'<td>{info.get("porcentaje", 0):.1f}%</td>'
            html += f'<td>{info.get("descripcion", "")}</td></tr>\n'
        html += "    </tbody></table>\n  </div>\n"

    return html
