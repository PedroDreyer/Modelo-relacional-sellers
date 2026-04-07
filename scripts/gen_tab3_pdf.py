"""
Tab 3 — PdF (Problemas de Funcionamiento): line chart + stacked bars por device.

Replica el estilo de la PPT:
  Slide 19: Line chart — % PdF por device (mPOS, POS, Smart, Tap) × quarter
  Slide 20: Stacked bars — motivos PdF por quarter, uno por device

Exporta `generar_tab3()` que recibe el DataFrame y devuelve HTML string.
"""

from __future__ import annotations

import json
import pandas as pd

# Device line colors (PPT style — high contrast)
DEVICE_COLORS = {
    "mPOS": {"line": "#37474f", "bg": "rgba(55,71,79,.1)"},
    "POS": {"line": "#78909c", "bg": "rgba(120,144,156,.1)"},
    "Smart": {"line": "#1e88e5", "bg": "rgba(30,136,229,.1)"},
    "Tap": {"line": "#e53935", "bg": "rgba(229,57,53,.1)"},
}

# Motivo colors for stacked bars (PPT style)
MOTIVO_COLORS = [
    "#5c6bc0",  # Bluetooth
    "#e53935",  # Chip
    "#b0bec5",  # WiFi
    "#ef5350",  # Rechazos
    "#ffa726",  # Congelamiento
    "#795548",  # Bateria
    "#9e9e9e",  # Otro
    "#7e57c2",  # Procesamiento lento
    "#26a69a",  # Errores de cobro
    "#78909c",  # Demora validacion
]

DEVICE_ORDER = ["mPOS", "Tap", "POS", "Smart"]
MIN_RESPONSES = 10


def _prep_pdf_data(df: pd.DataFrame) -> pd.DataFrame:
    """Prepare DataFrame: filter to sellers who answered PdF, add HAS_PROBLEM."""
    if "MODELO_DEVICE" not in df.columns or "PROBLEMA_FUNCIONAMIENTO" not in df.columns:
        return pd.DataFrame()

    df_point = df[df["MODELO_DEVICE"].notna()].copy()
    pf_lower = df_point["PROBLEMA_FUNCIONAMIENTO"].astype(str).str.lower()
    # Normalize: strip accents for matching (sí→si, não→nao)
    import unicodedata
    def _strip_accents(s):
        return "".join(c for c in unicodedata.normalize("NFD", s) if unicodedata.category(c) != "Mn")
    pf_norm = pf_lower.apply(_strip_accents)
    df_point = df_point[pf_norm.isin(["sim", "si", "nao", "no", "1", "0", "true", "false", "yes"])].copy()
    pf_norm_final = df_point["PROBLEMA_FUNCIONAMIENTO"].astype(str).str.lower().apply(_strip_accents)
    df_point["HAS_PROBLEM"] = pf_norm_final.isin(
        ["sim", "si", "1", "true", "yes"]
    )
    return df_point


def _chart_lines_pdf_by_device(df_point: pd.DataFrame, meses_por_q: dict[str, list[str]]) -> str:
    """Line chart: % PdF por device × quarter (PPT slide 19 style)."""
    if df_point.empty:
        return '<p class="text-sm text-muted">No hay datos de PdF.</p>'

    all_qs = sorted(meses_por_q.keys())
    devices = [d for d in DEVICE_ORDER if d in df_point["MODELO_DEVICE"].unique()
               and len(df_point[df_point["MODELO_DEVICE"] == d]) >= MIN_RESPONSES]

    if not devices:
        return '<p class="text-sm text-muted">Datos insuficientes para gráfico PdF.</p>'

    labels = json.dumps([q.replace("25", "Q").replace("26", "Q") if len(q) == 4 else q for q in all_qs])
    q_labels_raw = json.dumps(all_qs)

    datasets = []
    for dev in devices:
        colors = DEVICE_COLORS.get(dev, {"line": "#999", "bg": "rgba(153,153,153,.1)"})
        data = []
        for q in all_qs:
            meses = meses_por_q[q]
            sub = df_point[df_point["END_DATE_MONTH"].isin(meses) & (df_point["MODELO_DEVICE"] == dev)]
            pct = round(sub["HAS_PROBLEM"].mean() * 100, 1) if len(sub) >= MIN_RESPONSES else None
            data.append(pct)
        datasets.append({
            "label": dev,
            "data": data,
            "borderColor": colors["line"],
            "backgroundColor": colors["bg"],
            "fill": False,
            "tension": 0.3,
            "pointRadius": 4,
            "pointBackgroundColor": colors["line"],
            "borderWidth": 2,
            "borderDash": [5, 5] if dev == "Tap" else [],
        })
    datasets_json = json.dumps(datasets)

    # Total PdF per quarter (across all devices)
    totals = []
    for q in all_qs:
        meses = meses_por_q[q]
        sub = df_point[df_point["END_DATE_MONTH"].isin(meses)]
        pct = round(sub["HAS_PROBLEM"].mean() * 100) if len(sub) >= MIN_RESPONSES else None
        totals.append(pct)

    chart_id = "chartPdfLines"
    h = f"""
    <h3 style="font-size:14px;margin-bottom:4px;color:#555;">PdF % por Device Type</h3>
    <div style="display:flex;gap:16px;margin-bottom:8px;">
"""
    for qi, q in enumerate(all_qs):
        if totals[qi] is not None:
            h += f'      <div style="text-align:center;"><span style="background:#e0e0e0;border-radius:50%;display:inline-block;width:36px;height:36px;line-height:36px;font-weight:700;font-size:13px;">{totals[qi]}</span><div style="font-size:10px;color:#888;margin-top:2px;">{q}</div></div>\n'
    h += f"""    </div>
    <div style="max-width:700px;height:300px;margin:0 auto 20px;">
      <canvas id="{chart_id}"></canvas>
    </div>
    <script>
    (function() {{
      new Chart(document.getElementById('{chart_id}'), {{
        type: 'line',
        data: {{ labels: {json.dumps(all_qs)}, datasets: {datasets_json} }},
        options: {{
          responsive: true,
          maintainAspectRatio: false,
          plugins: {{
            legend: {{ position: 'top', labels: {{ font: {{ size: 11 }}, boxWidth: 14, padding: 10, usePointStyle: true }} }},
            datalabels: {{
              anchor: 'end', align: 'top', font: {{ size: 9, weight: 'bold' }},
              formatter: (v) => v !== null ? v + '%' : '',
              color: (ctx) => ctx.dataset.borderColor,
              offset: 4,
            }},
          }},
          scales: {{
            y: {{
              min: 0,
              max: 35,
              ticks: {{ callback: v => v + '%', font: {{ size: 11 }} }},
              grid: {{ color: '#f0f0f0' }},
            }},
            x: {{ ticks: {{ font: {{ size: 11 }} }} }},
          }},
        }},
        /* ChartDataLabels registered globally */
      }});
    }})();
    </script>
"""
    return h


def _chart_nps_by_device(df_full: pd.DataFrame, meses_por_q: dict[str, list[str]]) -> str:
    """Line chart: NPS por device type × quarter."""
    if df_full.empty or "NPS" not in df_full.columns or "MODELO_DEVICE" not in df_full.columns:
        return ""

    all_qs = sorted(meses_por_q.keys())
    devices = [d for d in DEVICE_ORDER if d in df_full["MODELO_DEVICE"].unique()
               and len(df_full[df_full["MODELO_DEVICE"] == d]) >= MIN_RESPONSES]
    if not devices:
        return ""

    datasets = []
    for dev in devices:
        colors = DEVICE_COLORS.get(dev, {"line": "#999", "bg": "rgba(153,153,153,.1)"})
        data = []
        for q in all_qs:
            meses = meses_por_q[q]
            sub = df_full[(df_full["END_DATE_MONTH"].isin(meses)) & (df_full["MODELO_DEVICE"] == dev)]
            if len(sub) >= MIN_RESPONSES:
                nps = sub["NPS"].mean() * 100  # NPS is -1/0/1, multiply by 100
                data.append(round(nps, 1) if nps == nps else None)
            else:
                data.append(None)
        datasets.append({
            "label": dev, "data": data,
            "borderColor": colors["line"], "backgroundColor": colors["bg"],
            "fill": False, "tension": 0.3, "pointRadius": 4,
            "pointBackgroundColor": colors["line"], "borderWidth": 2,
            "borderDash": [5, 5] if dev == "Tap" else [],
        })

    nps_datasets_json = json.dumps(datasets)
    nps_labels_json = json.dumps(all_qs)

    # Single chart, full width, lazy init for hidden tab
    h = f"""
    <h3 style="font-size:14px;margin-bottom:8px;color:#555;">NPS por Device Type</h3>
    <div style="max-width:700px;height:300px;margin:0 auto 20px;">
      <canvas id="chartNpsDevice"></canvas>
    </div>
    <script>
    (function() {{
      var _done = false;
      function init() {{
        if(_done) return; _done=true;
        new Chart(document.getElementById('chartNpsDevice'), {{
          type:'line',
          data:{{labels:{nps_labels_json},datasets:{nps_datasets_json}}},
          options:{{responsive:true,maintainAspectRatio:false,plugins:{{legend:{{position:'top',labels:{{font:{{size:11}},boxWidth:14,padding:10,usePointStyle:true}}}},datalabels:false}},scales:{{y:{{min:0,max:100,ticks:{{callback:function(v){{return v+' p.p.'}},font:{{size:11}}}},grid:{{color:'#f0f0f0'}}}},x:{{ticks:{{font:{{size:11}}}}}}}}}},
          plugins:[{{
            id:'nL',
            afterDatasetsDraw:function(chart){{
              var ctx=chart.ctx;ctx.save();ctx.font='bold 10px sans-serif';ctx.textAlign='center';ctx.textBaseline='bottom';
              chart.data.datasets.forEach(function(ds,di){{
                var meta=chart.getDatasetMeta(di);
                ctx.fillStyle=ds.borderColor||'#333';
                meta.data.forEach(function(pt,i){{if(ds.data[i]!==null)ctx.fillText(Math.round(ds.data[i]),pt.x,pt.y-6);}});
              }});
              ctx.restore();
            }}
          }}]
        }});
      }}
      var tab = document.getElementById('point');
      if(tab && tab.classList.contains('active')) {{ init(); }}
      else if(tab) {{
        var obs = new MutationObserver(function() {{
          if(tab.classList.contains('active')) {{ init(); obs.disconnect(); }}
        }});
        obs.observe(tab, {{attributes:true,attributeFilter:['class']}});
      }}
    }})();
    </script>
"""
    return h


def _chart_stacked_motivos_by_device(df_point: pd.DataFrame, device: str, meses_por_q: dict[str, list[str]]) -> str:
    """Stacked bar chart: motivos PdF por quarter for a specific device (PPT slide 20 style)."""
    df_dev = df_point[
        (df_point["MODELO_DEVICE"] == device)
        & (df_point["HAS_PROBLEM"] == True)
    ].copy()

    if "TIPO_PROBLEMA" not in df_dev.columns or df_dev.empty:
        return ""

    all_qs = sorted(meses_por_q.keys())

    # Denominator: all respondents for this device (Sim + Não)
    df_all_dev = df_point[df_point["MODELO_DEVICE"] == device]

    # Get top tipos across all quarters
    tipo_totals = df_dev["TIPO_PROBLEMA"].value_counts()
    top_tipos = [t for t in tipo_totals.index if pd.notna(t)][:8]

    if not top_tipos:
        return ""

    # Build datasets (one per tipo, stacked)
    datasets = []
    for ti, tipo in enumerate(top_tipos):
        data = []
        for q in all_qs:
            meses = meses_por_q[q]
            total = len(df_all_dev[df_all_dev["END_DATE_MONTH"].isin(meses)])
            count = len(df_dev[(df_dev["END_DATE_MONTH"].isin(meses)) & (df_dev["TIPO_PROBLEMA"] == tipo)])
            pct = round(count / total * 100, 1) if total >= MIN_RESPONSES else 0
            data.append(pct)
        color = MOTIVO_COLORS[ti % len(MOTIVO_COLORS)]
        label = str(tipo)
        datasets.append({
            "label": label,
            "data": data,
            "backgroundColor": color,
        })

    # Total PdF per quarter for this device
    totals = []
    for q in all_qs:
        meses = meses_por_q[q]
        sub = df_all_dev[df_all_dev["END_DATE_MONTH"].isin(meses)]
        pct = round(sub.get("HAS_PROBLEM", pd.Series()).mean() * 100) if len(sub) >= MIN_RESPONSES else None
        totals.append(pct)
    # Fix: df_all_dev may not have HAS_PROBLEM
    if "HAS_PROBLEM" not in df_all_dev.columns:
        totals = [None] * len(all_qs)

    chart_id = f"chartPdfMotivos_{device.replace(' ', '_')}"
    total_label = " | ".join([f"{q}: {t}%" for q, t in zip(all_qs, totals) if t is not None])

    h = f"""
  <div style="flex:1;min-width:280px;">
    <h4 style="text-align:center;margin-bottom:4px;">{device}</h4>
    <div style="text-align:center;font-size:10px;color:#888;margin-bottom:6px;">{total_label}</div>
    <div style="height:260px;">
      <canvas id="{chart_id}"></canvas>
    </div>
    <script>
    (function() {{
      new Chart(document.getElementById('{chart_id}'), {{
        type: 'bar',
        data: {{ labels: {json.dumps(all_qs)}, datasets: {json.dumps(datasets)} }},
        options: {{
          responsive: true,
          maintainAspectRatio: false,
          plugins: {{
            legend: {{ display: false }},
            datalabels: {{
              font: {{ size: 9 }},
              formatter: (v) => v >= 2 ? v + '%' : '',
              color: '#fff',
            }},
            tooltip: {{ mode: 'index', intersect: false }},
          }},
          scales: {{
            x: {{ stacked: true, ticks: {{ font: {{ size: 10 }} }} }},
            y: {{
              stacked: true,
              min: 0,
              max: 35,
              ticks: {{ callback: v => v + '%', font: {{ size: 10 }} }},
              grid: {{ color: '#f0f0f0' }},
            }},
          }},
        }},
        plugins: [ChartDataLabels, {{
          id: 'stackedTotal_{device.replace(" ", "_")}',
          afterDatasetsDraw(chart) {{
            const ctx = chart.ctx;
            const lastMeta = chart.getDatasetMeta(chart.data.datasets.length - 1);
            if (!lastMeta || !lastMeta.data) return;
            ctx.save();
            ctx.font = 'bold 11px sans-serif';
            ctx.fillStyle = '#333';
            ctx.textAlign = 'center';
            const totals = {json.dumps([t for t in totals])};
            lastMeta.data.forEach((bar, i) => {{
              if (totals[i] !== null) {{
                ctx.fillText(totals[i] + '%', bar.x, bar.y - 6);
              }}
            }});
            ctx.restore();
          }}
        }}],
      }});
    }})();
    </script>
  </div>
"""
    return h


# ── main generator ──────────────────────────────────────────────────

_DIM_LABELS = {
    "MODELO_DEVICE": "Device",
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


def _export_pdf_data_json(df_point: pd.DataFrame, filter_dims: list[str]) -> str:
    """Export compact JSON array for JS chart rebuilding."""
    import html as html_mod
    records = []
    # MODELO_DEVICE already exported as "dev" — exclude from filter_dims to avoid duplication
    extra_dims = [d for d in filter_dims if d != "MODELO_DEVICE"]
    cols_to_export = ["MODELO_DEVICE", "END_DATE_MONTH", "HAS_PROBLEM", "TIPO_PROBLEMA"] + extra_dims
    available = [c for c in cols_to_export if c in df_point.columns]
    for _, row in df_point[available].iterrows():
        r = {}
        for c in available:
            v = row[c]
            if pd.isna(v):
                continue
            if c == "HAS_PROBLEM":
                r["hp"] = 1 if v else 0
            elif c == "MODELO_DEVICE":
                r["dev"] = str(v)
            elif c == "END_DATE_MONTH":
                r["m"] = str(v)
            elif c == "TIPO_PROBLEMA":
                r["tp"] = str(v)
            else:
                r[c] = str(v)
        records.append(r)
    return json.dumps(records, ensure_ascii=False)


def generar_tab3(
    *,
    df: pd.DataFrame,
    meses_por_q: dict[str, list[str]],
    filter_dimensions: list[str] | None = None,
) -> str:
    """Genera el HTML completo de Tab 3 (PdF)."""

    df_point = _prep_pdf_data(df)

    if df_point.empty:
        return """
  <div class="section">
    <div class="section-title">Problemas de Funcionamiento (PdF)</div>
    <p class="text-sm text-muted">No hay datos de PdF disponibles para este update/site.</p>
  </div>
"""

    filter_dims = filter_dimensions or []

    html = """
  <div class="section">
    <div class="section-title">Problemas de Funcionamiento (PdF)</div>
"""

    # ── Filter bar (same style as Tab 4) ────────────────────────────
    if filter_dims:
        dim_values: dict[str, set] = {d: set() for d in filter_dims}
        for d in filter_dims:
            if d in df_point.columns:
                for v in df_point[d].dropna().unique():
                    dim_values[d].add(str(v))

        has_any = any(bool(v) for v in dim_values.values())
        if has_any:
            html += '    <div style="display:flex;flex-wrap:wrap;gap:8px;margin-bottom:18px;align-items:center;padding:10px 14px;background:#f8f9fa;border-radius:8px;border:1px solid #e0e0e0;">\n'
            html += '      <span style="font-size:12px;font-weight:600;color:#555;">Filtrar por:</span>\n'
            for dim in filter_dims:
                vals = sorted(dim_values.get(dim, set()))
                if not vals:
                    continue
                label = _DIM_LABELS.get(dim, dim)
                html += f'      <select class="pdf-filter" data-dim="{dim}" style="font-size:11px;padding:3px 6px;border:1px solid #ccc;border-radius:5px;background:white;">\n'
                html += f'        <option value="">{label}: Todos</option>\n'
                for v in vals:
                    html += f'        <option value="{v}">{v}</option>\n'
                html += '      </select>\n'
            html += '      <span class="pdf-filter-count" style="font-size:11px;color:#888;margin-left:auto;"></span>\n'
            html += '    </div>\n'

    # Chart 1: Line chart — % PdF por device
    html += _chart_lines_pdf_by_device(df_point, meses_por_q)

    # Chart 1b: NPS por device (lazy init for hidden tab, full width like PdF chart)
    nps_html = _chart_nps_by_device(df, meses_por_q)
    if nps_html:
        html += nps_html
    html += "  </div>\n"

    # Chart 2-5: Stacked bars — motivos PdF por device
    devices = [d for d in DEVICE_ORDER if d in df_point["MODELO_DEVICE"].unique()
               and len(df_point[df_point["MODELO_DEVICE"] == d]) >= MIN_RESPONSES]

    if devices:
        html += """
  <div class="section">
    <div class="section-title">Motivos de PdF por Device</div>
    <div style="display:flex;flex-wrap:wrap;gap:12px;">
"""
        for dev in devices:
            section = _chart_stacked_motivos_by_device(df_point, dev, meses_por_q)
            if section:
                html += section

        # Shared legend
        html += """    </div>
    <div style="display:flex;flex-wrap:wrap;gap:8px;justify-content:center;margin-top:12px;font-size:11px;">
"""
        all_tipos = df_point[df_point["HAS_PROBLEM"] == True]["TIPO_PROBLEMA"].value_counts().head(8)
        for ti, tipo in enumerate(all_tipos.index):
            if pd.isna(tipo):
                continue
            color = MOTIVO_COLORS[ti % len(MOTIVO_COLORS)]
            label = str(tipo)
            html += f'      <div style="display:flex;align-items:center;gap:4px;"><div style="width:12px;height:12px;background:{color};border-radius:2px;"></div>{label}</div>\n'

        html += "    </div>\n  </div>\n"

    # ── Embed data + JS for filter-driven chart rebuilding ───────────
    if filter_dims:
        data_json = _export_pdf_data_json(df_point, filter_dims)
        meses_json = json.dumps(meses_por_q)
        devices_json = json.dumps(devices if devices else [])
        device_colors_json = json.dumps(DEVICE_COLORS)
        motivo_colors_json = json.dumps(MOTIVO_COLORS)

        html += f"""
  <script>
  (function(){{
    const RAW = {data_json};
    const MESES_POR_Q = {meses_json};
    const DEVICES = {devices_json};
    const DEVICE_COLORS = {device_colors_json};
    const MOTIVO_COLORS = {motivo_colors_json};
    const MIN_N = {MIN_RESPONSES};
    const ALL_QS = Object.keys(MESES_POR_Q).sort();

    const filters = document.querySelectorAll('.pdf-filter');
    if(!filters.length) return;

    // Store original chart instances
    let lineChart = null;
    const barCharts = {{}};

    // Destroy and find existing charts
    Chart.instances && Object.values(Chart.instances).forEach(c => {{
      if(c.canvas.id === 'chartPdfLines') lineChart = c;
      DEVICES.forEach(dev => {{
        if(c.canvas.id === 'chartPdfMotivos_' + dev.replace(/ /g,'_')) barCharts[dev] = c;
      }});
    }});

    function getFiltered(){{
      const active = {{}};
      filters.forEach(f => {{ if(f.value) active[f.dataset.dim] = f.value; }});
      if(!Object.keys(active).length) return RAW;
      return RAW.filter(r => {{
        for(const [dim,val] of Object.entries(active)){{
          // MODELO_DEVICE is stored as "dev" in the data
          const key = dim === 'MODELO_DEVICE' ? 'dev' : dim;
          if(r[key] !== undefined && r[key] !== val) return false;
        }}
        return true;
      }});
    }}

    function rebuildLineChart(data){{
      if(!lineChart) return;
      const datasets = [];
      DEVICES.forEach(dev => {{
        const vals = [];
        ALL_QS.forEach(q => {{
          const meses = MESES_POR_Q[q];
          const sub = data.filter(r => r.dev === dev && meses.includes(r.m));
          vals.push(sub.length >= MIN_N ? Math.round(sub.filter(r=>r.hp===1).length / sub.length * 1000)/10 : null);
        }});
        const c = DEVICE_COLORS[dev] || {{line:'#999',bg:'rgba(153,153,153,.1)'}};
        datasets.push({{
          label: dev, data: vals,
          borderColor: c.line, backgroundColor: c.bg,
          fill: false, tension: 0.3, pointRadius: 4,
          pointBackgroundColor: c.line, borderWidth: 2,
          borderDash: dev === 'Tap' ? [5,5] : [],
        }});
      }});
      lineChart.data.datasets = datasets;
      lineChart.update();
    }}

    function rebuildBarCharts(data){{
      DEVICES.forEach(dev => {{
        const chart = barCharts[dev];
        if(!chart) return;
        const devData = data.filter(r => r.dev === dev);
        const problemData = devData.filter(r => r.hp === 1);
        const tipos = {{}};
        problemData.forEach(r => {{ if(r.tp) tipos[r.tp] = (tipos[r.tp]||0)+1; }});
        const topTipos = Object.entries(tipos).sort((a,b)=>b[1]-a[1]).slice(0,8).map(e=>e[0]);

        const datasets = topTipos.map((tipo, ti) => {{
          const vals = ALL_QS.map(q => {{
            const meses = MESES_POR_Q[q];
            const total = devData.filter(r => meses.includes(r.m)).length;
            const count = problemData.filter(r => meses.includes(r.m) && r.tp === tipo).length;
            return total >= MIN_N ? Math.round(count/total*1000)/10 : 0;
          }});
          return {{ label: tipo, data: vals, backgroundColor: MOTIVO_COLORS[ti % MOTIVO_COLORS.length] }};
        }});
        chart.data.datasets = datasets;
        chart.update();
      }});
    }}

    function applyFilters(){{
      const data = getFiltered();
      const countEl = document.querySelector('.pdf-filter-count');
      if(countEl){{
        const hasFilter = [...filters].some(f=>f.value);
        countEl.textContent = hasFilter ? 'Mostrando ' + data.length + ' de ' + RAW.length + ' registros' : '';
      }}
      rebuildLineChart(data);
      rebuildBarCharts(data);
    }}

    filters.forEach(f => f.addEventListener('change', applyFilters));
  }})();
  </script>
"""

    return html
