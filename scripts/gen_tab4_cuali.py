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
import json as json_mod
from nps_model.utils.motivos import consolidar_motivo

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


_DIM_LABELS = {
    "PRODUCTO_PRINCIPAL": "Producto",
    "NEWBIE_LEGACY": "Antigüedad",
    "FLAG_USA_CREDITO": "Crédito",
    "FLAG_TARJETA_CREDITO": "TC MP",
    "FLAG_USA_INVERSIONES": "Inversiones",
    "FLAG_TOPOFF": "Top Off",
    "FLAG_WINNER": "Winner",
    "FLAG_PRICING": "Pricing",
    "SCALE_LEVEL": "Escala",
}


def _render_causas_block(causas_por_motivo: dict, var_map: dict, razonamiento: dict | None, block_id: str, filter_dimensions: list[str] | None = None) -> str:
    """Render the causas raíz cards for a given quarter's data. Returns HTML string."""
    # This is extracted so we can call it twice (Q actual + Q anterior)
    html = ""
    # (delegated to main function below — see inline rendering)
    return html


def generar_tab4(
    *,
    causas_raiz_data: dict | None,
    causas_raiz_anterior: dict | None = None,
    promotores_data: dict | None = None,
    retagueo_data: dict | None,
    comments_variaciones_data: dict | None,
    variaciones_quejas: list[dict],
    q_label_ant: str = "",
    q_label_act: str = "",
    razonamiento: dict | None = None,
    filter_dimensions: list[str] | None = None,
) -> str:
    """Genera HTML de Tab 4 (Análisis Cualitativo)."""

    html = ""
    has_anterior = bool(causas_raiz_anterior and causas_raiz_anterior.get("causas_por_motivo"))

    if not causas_raiz_data or not causas_raiz_data.get("causas_por_motivo"):
        return '<div class="section"><p class="text-sm text-muted">No hay análisis cualitativo disponible.</p></div>\n'

    # Build var lookup from variaciones_quejas
    var_map = {}
    for vq in variaciones_quejas:
        m = vq.get("motivo")
        if m:
            var_map[m] = vq

    html += f"""
  <div class="section">
    <div class="section-title">Análisis Cualitativo — Voz del Seller</div>
    <p class="text-sm text-muted" style="margin-bottom:16px;">
      Causas raíz identificadas por análisis semántico de comentarios de detractores y neutros.
      Cada motivo muestra las causas principales, su frecuencia, y la conexión con drivers de datos reales.
    </p>
"""
    # Quarter toggle if both quarters available
    if has_anterior:
        html += f"""
    <div style="display:flex;gap:4px;margin-bottom:18px;">
      <button class="cuali-q-btn" data-q="actual" style="padding:8px 20px;border:2px solid #1a237e;border-radius:8px;font-weight:700;font-size:13px;cursor:pointer;background:#1a237e;color:white;" onclick="toggleCualiQ('actual')">{_esc(q_label_act)}</button>
      <button class="cuali-q-btn" data-q="anterior" style="padding:8px 20px;border:2px solid #e0e0e0;border-radius:8px;font-weight:700;font-size:13px;cursor:pointer;background:white;color:#555;" onclick="toggleCualiQ('anterior')">{_esc(q_label_ant)}</button>
    </div>
"""

    # ── Filter bar ──────────────────────────────────────────────────────
    if filter_dimensions:
        # Collect unique values per dimension from all ejemplos
        dim_values: dict[str, set] = {d: set() for d in filter_dimensions}
        for _m, _d in (causas_raiz_data.get("causas_por_motivo") or {}).items():
            for _c in (_d.get("causas_raiz") or {}).values():
                for _ej in _c.get("ejemplos", []):
                    for _dim, _val in (_ej.get("dims") or {}).items():
                        if _dim in dim_values and _val:
                            dim_values[_dim].add(str(_val))

        has_any = any(bool(v) for v in dim_values.values())
        if has_any:
            html += '    <div style="display:flex;flex-wrap:wrap;gap:8px;margin-bottom:18px;align-items:center;padding:10px 14px;background:#f8f9fa;border-radius:8px;border:1px solid #e0e0e0;">\n'
            html += '      <span style="font-size:12px;font-weight:600;color:#555;">Filtrar por:</span>\n'
            for dim in filter_dimensions:
                vals = sorted(dim_values.get(dim, set()))
                if not vals:
                    continue
                label = _DIM_LABELS.get(dim, dim)
                html += f'      <select class="cuali-filter" data-dim="{dim}" style="font-size:11px;padding:3px 6px;border:1px solid #ccc;border-radius:5px;background:white;">\n'
                html += f'        <option value="">{_esc(label)}: Todos</option>\n'
                for v in vals:
                    html += f'        <option value="{_esc(v)}">{_esc(v)}</option>\n'
                html += '      </select>\n'
            html += '    </div>\n'

    # Build Q anterior lookup by consolidated motivo name
    ant_motivo_lookup = {}
    if has_anterior:
        for m_ant, d_ant in (causas_raiz_anterior.get("causas_por_motivo") or {}).items():
            ant_motivo_lookup[consolidar_motivo(m_ant)] = d_ant

    # Wrap Q actual cards
    html += '    <div id="cuali-actual">\n'

    for idx, (motivo, datos) in enumerate(causas_raiz_data["causas_por_motivo"].items()):
        # Get variation data — consolidate CP5 motivo to match var_map keys
        motivo_cons = consolidar_motivo(motivo)
        vq_data = var_map.get(motivo_cons, {})
        var = vq_data.get("var_mom", 0)
        share_act = vq_data.get("share_actual", None)

        composicion = datos.get("composicion", {})
        total_com = datos.get("total_comentarios_analizados", 0)
        det = composicion.get("detractores", 0)
        neu = composicion.get("neutros", 0)
        det_pct = round(det / total_com * 100) if total_com else 0
        neu_pct = 100 - det_pct

        # Q anterior comparison at motivo level
        ant_datos = ant_motivo_lookup.get(motivo_cons, {})
        total_com_ant = ant_datos.get("total_comentarios_analizados", 0) if ant_datos else 0

        # Find driver association from razonamiento (try consolidated name first)
        driver = _find_driver_for_motivo(motivo_cons, razonamiento) or _find_driver_for_motivo(motivo, razonamiento)

        # Collect causas
        causas = list(datos.get("causas_raiz", {}).values())

        # Card
        html += f"""
    <details class="section" data-motivo-block="{idx}" style="border-left:4px solid {CAUSA_COLORS[idx % len(CAUSA_COLORS)]};margin-bottom:12px;" open>
      <summary style="cursor:pointer;list-style:none;padding:12px 0;">
        <div style="display:flex;align-items:center;gap:10px;flex-wrap:wrap;">
          <h4 style="margin:0;font-size:15px;">{_esc(motivo)}</h4>
          {_badge(var)}
          <span class="cuali-motivo-count" style="font-size:12px;color:#888;margin-left:auto;">{total_com} comentarios analizados{f' <span style="color:#aaa;">(Q ant: {total_com_ant})</span>' if total_com_ant > 0 else ''}</span>
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

                # Variation badge for sub-cause (vs Q anterior)
                var_badge = ""
                if freq_ant is not None:
                    var_sub = freq - freq_ant
                    if abs(var_sub) >= 1:
                        vcls = "badge-up" if var_sub > 0 else "badge-down"
                        var_badge = f' <span class="{vcls}" style="font-size:10px;padding:1px 5px;">{var_sub:+.0f}pp vs Q ant</span>'
                elif has_anterior and freq_ant is None:
                    # Causa exists in Q actual but not in Q anterior → new
                    var_badge = ' <span style="font-size:10px;padding:1px 6px;border-radius:8px;background:#e3f2fd;color:#1565c0;font-weight:600;">Nueva</span>'

                html += f"""
        <div data-causa-block style="margin-bottom:10px;padding:10px 14px;background:#fafafa;border-radius:8px;border:1px solid #eee;">
          <div style="display:flex;align-items:center;gap:8px;margin-bottom:6px;">
            <div style="font-weight:600;font-size:13px;flex:1;color:#333;">{_esc(titulo)}{var_badge}</div>
            <div style="font-size:12px;font-weight:700;color:{bar_color};white-space:nowrap;"><span class="cuali-freq">{freq:.0f}% ({freq_abs})</span></div>
          </div>
          <div style="height:6px;border-radius:3px;background:#e0e0e0;margin-bottom:8px;">
            <div class="cuali-bar-inner" data-base-pct="{bar_w:.1f}" style="width:{bar_w:.0f}%;height:100%;border-radius:3px;background:{bar_color};transition:width 0.3s;"></div>
          </div>
"""
                # Description always visible
                if descripcion:
                    html += f'          <p style="margin:0 0 8px;font-size:12px;color:#666;line-height:1.4;">{_esc(descripcion)}</p>\n'

                # Example comments with dimension tags for filtering
                if ejemplos:
                    shown = ejemplos[:3]
                    extra = ejemplos[3:]

                    for ej in shown:
                        if isinstance(ej, dict):
                            comment = ej.get("comentario", "")
                            cust_id = ej.get("cust_id", ej.get("CUS_CUST_ID", ""))
                            dims_json = _esc(json_mod.dumps(ej.get("dims", {}), ensure_ascii=False))
                        else:
                            comment, cust_id, dims_json = str(ej), "", "{}"
                        cust_tag = f' <span style="color:#bbb;font-size:10px;">[{cust_id}]</span>' if cust_id else ""
                        html += f'          <div class="cuali-comment" data-dims=\'{dims_json}\' style="font-size:12px;color:#555;padding:4px 0 4px 12px;border-left:2px solid #ddd;margin-bottom:4px;font-style:italic;">"{_esc(comment[:400])}"{cust_tag}</div>\n'

                    if extra:
                        html += f'          <details style="padding-left:12px;"><summary style="font-size:11px;color:#999;cursor:pointer;">+{len(extra)} comentarios más</summary>\n'
                        for ej in extra:
                            if isinstance(ej, dict):
                                comment = ej.get("comentario", "")
                                cust_id = ej.get("cust_id", ej.get("CUS_CUST_ID", ""))
                                dims_json = _esc(json_mod.dumps(ej.get("dims", {}), ensure_ascii=False))
                            else:
                                comment, cust_id, dims_json = str(ej), "", "{}"
                            cust_tag = f' <span style="color:#bbb;font-size:10px;">[{cust_id}]</span>' if cust_id else ""
                            html += f'          <div class="cuali-comment" data-dims=\'{dims_json}\' style="font-size:12px;color:#555;padding:4px 0 4px 12px;border-left:2px solid #ddd;margin-bottom:4px;font-style:italic;">"{_esc(comment[:400])}"{cust_tag}</div>\n'
                        html += '          </details>\n'

                html += "        </div>\n"

        html += """
      </div>
    </details>
"""

    html += "  </div>\n"

    # ── Filtering JS ─────────────────────────────────────────────────
    if filter_dimensions:
        html += """
  <script>
  (function(){
    const filters = document.querySelectorAll('.cuali-filter');
    if (!filters.length) return;

    function applyFilters(){
      const active = {};
      filters.forEach(f => { if(f.value) active[f.dataset.dim] = f.value; });
      const hasFilter = Object.keys(active).length > 0;

      // Filter comments
      document.querySelectorAll('.cuali-comment').forEach(el => {
        let dims;
        try { dims = JSON.parse(el.dataset.dims || '{}'); } catch(e){ dims = {}; }
        let show = true;
        if(hasFilter){
          for(const [dim, val] of Object.entries(active)){
            if(dims[dim] !== undefined && dims[dim] !== val){ show = false; break; }
          }
        }
        el.style.display = show ? '' : 'none';
      });

      // Update causa-level counts
      document.querySelectorAll('[data-causa-block]').forEach(block => {
        const comments = block.querySelectorAll('.cuali-comment');
        const visible = [...comments].filter(c => c.style.display !== 'none').length;
        const total = comments.length;
        const freqEl = block.querySelector('.cuali-freq');
        if(freqEl){
          if(hasFilter && total > 0){
            const pct = Math.round(visible / total * 100);
            freqEl.textContent = pct + '% (' + visible + '/' + total + ')';
          } else {
            freqEl.textContent = freqEl.dataset.orig || freqEl.textContent;
          }
          if(!freqEl.dataset.orig) freqEl.dataset.orig = freqEl.textContent;
        }
        const barEl = block.querySelector('.cuali-bar-inner');
        if(barEl && total > 0){
          const basePct = parseFloat(barEl.dataset.basePct) || 100;
          barEl.style.width = hasFilter ? (visible/total * basePct) + '%' : basePct + '%';
        }
      });

      // Update motivo-level counts
      document.querySelectorAll('[data-motivo-block]').forEach(mblock => {
        const comments = mblock.querySelectorAll('.cuali-comment');
        const visible = [...comments].filter(c => c.style.display !== 'none').length;
        const total = comments.length;
        const cEl = mblock.querySelector('.cuali-motivo-count');
        if(cEl){
          if(!cEl.dataset.orig) cEl.dataset.orig = cEl.textContent;
          cEl.textContent = hasFilter
            ? 'Mostrando ' + visible + ' de ' + total + ' comentarios'
            : cEl.dataset.orig;
        }
      });
    }

    filters.forEach(f => f.addEventListener('change', applyFilters));
  })();
  </script>
"""

    html += '    </div>\n'  # Close cuali-actual

    # ── Q Anterior block (hidden by default) ────────────────────────
    if has_anterior:
        html += '    <div id="cuali-anterior" style="display:none;">\n'
        ant_motivos = causas_raiz_anterior.get("causas_por_motivo", {})
        for idx_a, (motivo_a, datos_a) in enumerate(ant_motivos.items()):
            total_a = datos_a.get("total_comentarios_analizados", 0)
            comp_a = datos_a.get("composicion", {})
            det_a = comp_a.get("detractores", 0)
            neu_a = comp_a.get("neutros", 0)
            det_pct_a = det_a / total_a * 100 if total_a else 0
            neu_pct_a = neu_a / total_a * 100 if total_a else 0

            html += f"""
    <div class="cuali-card" style="background:white;border:1px solid #e0e0e0;border-radius:12px;padding:0;margin-bottom:16px;overflow:hidden;border-left:4px solid #9e9e9e;">
      <div style="padding:16px 20px;display:flex;justify-content:space-between;align-items:center;">
        <div style="display:flex;align-items:center;gap:12px;">
          <h3 style="margin:0;font-size:15px;font-weight:700;color:#333;">{_esc(motivo_a)}</h3>
        </div>
        <span style="font-size:12px;color:#888;">{total_a} comentarios analizados</span>
      </div>
      <div style="display:flex;height:6px;">
        <div style="width:{det_pct_a:.0f}%;background:#ef5350;"></div>
        <div style="width:{neu_pct_a:.0f}%;background:#ffa726;"></div>
      </div>
      <div style="padding:4px 20px 2px;display:flex;justify-content:flex-end;gap:12px;font-size:10px;color:#888;">
        <span style="color:#ef5350;font-weight:600;">{det_a} det</span>
        <span style="color:#ffa726;font-weight:600;">{neu_a} neu</span>
      </div>
      <div style="padding:8px 20px 16px;">
"""
            for ci, (cid_a, causa_a) in enumerate(datos_a.get("causas_raiz", {}).items()):
                titulo_a = causa_a.get("titulo", "Sin título")
                desc_a = causa_a.get("descripcion", "")
                freq_a = causa_a.get("frecuencia_pct", 0)
                freq_abs_a = causa_a.get("frecuencia_abs", 0)
                color = CAUSA_COLORS[ci % len(CAUSA_COLORS)]
                ejemplos_a = causa_a.get("ejemplos", [])

                html += f"""
        <div style="margin-bottom:14px;padding:12px;background:#fafafa;border-radius:8px;">
          <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:6px;">
            <strong style="font-size:13px;color:#333;">{_esc(titulo_a)}</strong>
            <span style="font-size:12px;font-weight:700;color:{color};">{freq_a:.0f}% ({freq_abs_a})</span>
          </div>
          <div style="height:5px;background:#e0e0e0;border-radius:3px;margin-bottom:8px;">
            <div style="width:{min(freq_a, 100):.0f}%;height:100%;background:{color};border-radius:3px;"></div>
          </div>
          <p style="font-size:12px;color:#555;margin:0 0 8px;">{_esc(desc_a)}</p>
"""
                if ejemplos_a:
                    html += '          <details style="margin-top:6px;"><summary style="font-size:11px;color:#888;cursor:pointer;">Ver ejemplos</summary>\n'
                    html += '          <div style="margin-top:6px;padding-left:10px;border-left:2px solid #e0e0e0;">\n'
                    for ej in ejemplos_a[:3]:
                        cust = ej.get("cust_id", "")
                        cust_tag = f' <span style="color:#aaa;font-size:10px;">[{_esc(cust)}]</span>' if cust else ""
                        html += f'            <p style="font-size:11px;color:#666;margin:4px 0;font-style:italic;">"{_esc(ej.get("comentario", ""))}"{cust_tag}</p>\n'
                    html += '          </div></details>\n'
                html += '        </div>\n'

            html += '      </div>\n    </div>\n'

        html += '    </div>\n'  # Close cuali-anterior

        # JS toggle
        html += """
    <script>
    function toggleCualiQ(q) {
      var act = document.getElementById('cuali-actual');
      var ant = document.getElementById('cuali-anterior');
      if (act) act.style.display = q === 'actual' ? 'block' : 'none';
      if (ant) ant.style.display = q === 'anterior' ? 'block' : 'none';
      document.querySelectorAll('.cuali-q-btn').forEach(function(btn) {
        if (btn.dataset.q === q) {
          btn.style.background = '#1a237e'; btn.style.color = 'white'; btn.style.borderColor = '#1a237e';
        } else {
          btn.style.background = 'white'; btn.style.color = '#555'; btn.style.borderColor = '#e0e0e0';
        }
      });
    }
    </script>
"""

    # ── Promotores: Voz Positiva ──────────────────────────────────────
    if promotores_data and promotores_data.get("causas_por_motivo"):
        html += """
  <div class="section" style="border-left:4px solid #4caf50;">
    <div class="section-title" style="color:#2e7d32;">🌟 Voz del Promotor — Qué valoran los sellers satisfechos</div>
    <p class="text-sm text-muted" style="margin-bottom:16px;">
      Causas raíz positivas identificadas por análisis de comentarios de promotores (NPS 9-10).
    </p>
"""
        prom_motivos = promotores_data.get("causas_por_motivo", {})
        for idx_p, (motivo_p, datos_p) in enumerate(prom_motivos.items()):
            total_p = datos_p.get("total_comentarios_analizados", 0)
            causas_p = datos_p.get("causas_raiz", {})

            html += f"""
    <div style="background:white;border:1px solid #c8e6c9;border-radius:10px;padding:16px 20px;margin-bottom:12px;">
      <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:10px;">
        <h4 style="margin:0;font-size:14px;color:#2e7d32;">{_esc(motivo_p)}</h4>
        <span style="font-size:11px;color:#888;">{total_p} comentarios</span>
      </div>
"""
            for ci_p, (cid_p, causa_p) in enumerate(causas_p.items()):
                titulo_p = causa_p.get("titulo", "")
                desc_p = causa_p.get("descripcion", "")
                freq_p = causa_p.get("frecuencia_pct", 0)
                freq_abs_p = causa_p.get("frecuencia_abs", 0)
                ejemplos_p = causa_p.get("ejemplos", [])
                color_p = "#66bb6a"

                html += f"""
      <div style="margin-bottom:10px;padding:10px 14px;background:#f1f8e9;border-radius:8px;">
        <div style="display:flex;justify-content:space-between;margin-bottom:4px;">
          <strong style="font-size:12px;color:#333;">{_esc(titulo_p)}</strong>
          <span style="font-size:11px;font-weight:700;color:{color_p};">{freq_p:.0f}% ({freq_abs_p})</span>
        </div>
        <div style="height:4px;background:#e0e0e0;border-radius:2px;margin-bottom:6px;">
          <div style="width:{min(freq_p, 100):.0f}%;height:100%;background:{color_p};border-radius:2px;"></div>
        </div>
"""
                if desc_p:
                    html += f'        <p style="font-size:11px;color:#555;margin:0 0 6px;">{_esc(desc_p)}</p>\n'
                if ejemplos_p:
                    html += '        <details><summary style="font-size:10px;color:#888;cursor:pointer;">Ver ejemplos</summary>\n'
                    html += '        <div style="margin-top:4px;padding-left:10px;border-left:2px solid #c8e6c9;">\n'
                    for ej_p in ejemplos_p[:3]:
                        cust_p = ej_p.get("cust_id", "")
                        cust_tag_p = f' <span style="color:#aaa;font-size:10px;">[{_esc(cust_p)}]</span>' if cust_p else ""
                        html += f'          <p style="font-size:11px;color:#666;margin:3px 0;font-style:italic;">"{_esc(ej_p.get("comentario", ""))}"{cust_tag_p}</p>\n'
                    html += '        </div></details>\n'
                html += '      </div>\n'

            html += '    </div>\n'

        html += '  </div>\n'

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
