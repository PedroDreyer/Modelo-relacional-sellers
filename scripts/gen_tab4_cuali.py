"""
Tab 4 — Cualitativo: causas raíz CP5, retagueo, comentarios variaciones.

Exporta `generar_tab4()` que devuelve HTML string.
"""

from __future__ import annotations
import html as html_mod

MOTIVO_BORDER_COLORS = [
    "var(--red)", "var(--orange)", "var(--green)", "var(--purple)",
    "var(--blue)", "var(--teal)", "#607d8b", "#795548",
]

# Color palette for horizontal bars (soft, distinguishable)
BAR_COLORS = [
    "#5c6bc0", "#42a5f5", "#26a69a", "#66bb6a",
    "#ffa726", "#ef5350", "#ab47bc", "#78909c",
]


def _esc(text: str) -> str:
    return html_mod.escape(text) if text else ""


def generar_tab4(
    *,
    causas_raiz_data: dict | None,
    retagueo_data: dict | None,
    comments_variaciones_data: dict | None,
    variaciones_quejas: list[dict],
    q_label_ant: str = "",
    q_label_act: str = "",
) -> str:
    """Genera HTML de Tab 4 (Cualitativo)."""

    html = ""

    # ── Causas raíz ──────────────────────────────────────────────────
    if causas_raiz_data and causas_raiz_data.get("causas_por_motivo"):
        html += """
  <div class="section">
    <div class="section-title">Causas Raíz por Motivo de Queja</div>
"""
        # Build a var lookup from variaciones_quejas
        var_map = {}
        for vq in variaciones_quejas:
            m = vq.get("motivo")
            if m:
                var_map[m] = vq.get("var_mom", 0)

        for idx, (motivo, datos) in enumerate(causas_raiz_data["causas_por_motivo"].items()):
            var = var_map.get(motivo, 0)
            if var > 0.3:
                badge = f'<span class="badge badge-up" style="font-size:11px;vertical-align:middle;">{var:+.1f}pp</span>'
            elif var < -0.3:
                badge = f'<span class="badge badge-down" style="font-size:11px;vertical-align:middle;">{var:.1f}pp</span>'
            else:
                badge = f'<span class="badge badge-stable" style="font-size:11px;vertical-align:middle;">{var:+.1f}pp</span>'

            composicion = datos.get("composicion", {})
            total_com = datos.get("total_comentarios_analizados", 0)
            det = composicion.get("detractores", 0)
            neu = composicion.get("neutros", 0)

            # Composición bar (detractores vs neutros)
            det_pct = round(det / total_com * 100) if total_com else 0
            neu_pct = 100 - det_pct

            bc = MOTIVO_BORDER_COLORS[idx % len(MOTIVO_BORDER_COLORS)]

            # Collect causas for this motivo
            causas = []
            for cid, causa in datos.get("causas_raiz", {}).items():
                causas.append(causa)

            html += f"""
    <div class="quali-card" style="border-color:{bc};">
      <div style="display:flex;align-items:center;gap:10px;margin-bottom:8px;">
        <h4 style="margin:0;flex:1;">{_esc(motivo)} {badge}</h4>
        <span style="font-size:12px;color:#888;">{total_com} comentarios</span>
      </div>
      <div style="display:flex;height:6px;border-radius:3px;overflow:hidden;margin-bottom:14px;background:#f0f0f0;">
        <div style="width:{det_pct}%;background:#ef5350;" title="Detractores: {det}"></div>
        <div style="width:{neu_pct}%;background:#ffa726;" title="Neutros: {neu}"></div>
      </div>
"""
            # Render causas as horizontal bar chart
            max_freq = max((c.get("frecuencia_pct", 0) for c in causas), default=1) or 1

            for cidx, causa in enumerate(causas):
                titulo = causa.get("titulo", "")
                freq = causa.get("frecuencia_pct", 0)
                freq_abs = causa.get("frecuencia_abs", 0)
                freq_ant = causa.get("frecuencia_pct_anterior", None)
                bar_w = max(freq / max_freq * 100, 2)
                bar_color = BAR_COLORS[cidx % len(BAR_COLORS)]
                ejemplos = causa.get("ejemplos", [])[:2]

                # Variation badge for sub-motivo
                var_badge = ""
                if freq_ant is not None:
                    var_sub = freq - freq_ant
                    if abs(var_sub) >= 1:
                        vcls = "badge-up" if var_sub > 0 else "badge-down"
                        var_badge = f' <span class="{vcls}" style="font-size:10px;padding:1px 5px;">{var_sub:+.0f}pp</span>'

                html += f"""      <details class="causa-detail">
        <summary class="causa-bar-row">
          <div class="causa-bar-label">{_esc(titulo)}{var_badge}</div>
          <div class="causa-bar-track">
            <div class="causa-bar-fill" style="width:{bar_w:.0f}%;background:{bar_color};"></div>
          </div>
          <div class="causa-bar-value">{freq:.0f}%</div>
        </summary>
"""
                # Expandable: example comments with cust_id
                if ejemplos:
                    for ej in ejemplos:
                        if isinstance(ej, dict):
                            comment = ej.get("comentario", "")
                            cust_id = ej.get("cust_id", ej.get("CUS_CUST_ID", ""))
                        else:
                            comment = str(ej)
                            cust_id = ""
                        cust_tag = f' <span style="color:#aaa;font-size:11px;">[{cust_id}]</span>' if cust_id else ""
                        html += f'        <div class="comment-example">"{_esc(comment[:300])}"{cust_tag}</div>\n'
                html += "      </details>\n"

            html += "    </div>\n"

        html += "  </div>\n"
    else:
        html += '<div class="section"><p class="text-sm text-muted">No hay análisis cualitativo disponible.</p></div>\n'

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

    # ── Comentarios sobre variaciones ────────────────────────────────
    if comments_variaciones_data:
        html += """
  <div class="section">
    <div class="section-title">Comentarios sobre Variaciones</div>
"""
        for motivo, info in comments_variaciones_data.items():
            v = info.get("var_mom", 0)
            dir_text = info.get("direccion", "")
            clr = "var(--red)" if v > 0 else "var(--green)"
            html += f"""
    <div style="margin:8px 0;padding:10px 14px;background:#f8f9fa;border-left:4px solid {clr};border-radius:6px;">
      <div style="font-weight:600;font-size:13px;">{_esc(motivo)} <span style="color:{clr};font-weight:500;">({v:+.1f}pp — {dir_text})</span></div>
"""
            for c in info.get("comentarios", [])[:3]:
                nps_lbl = {-1: "Det", 0: "Neu"}.get(c.get("nps"), "")
                cust_id = c.get("cust_id", c.get("CUS_CUST_ID", ""))
                cust_tag = f" [{cust_id}]" if cust_id else ""
                html += f'      <div class="comment-example">"{_esc(c.get("comentario", ""))}" <span style="color:#999;">— {nps_lbl}{cust_tag}</span></div>\n'
            html += "    </div>\n"
        html += "  </div>\n"

    return html
