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
            legend: {{ position: 'bottom', labels: {{ font: {{ size: 11 }}, boxWidth: 14, padding: 10, usePointStyle: true }} }},
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
        plugins: [ChartDataLabels],
      }});
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
        # Shorten label
        label = str(tipo)
        if len(label) > 25:
            label = label[:22] + "..."
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

def generar_tab3(
    *,
    df: pd.DataFrame,
    meses_por_q: dict[str, list[str]],
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

    html = """
  <div class="section">
    <div class="section-title">Problemas de Funcionamiento (PdF)</div>
"""
    # Chart 1: Line chart — % PdF por device
    html += _chart_lines_pdf_by_device(df_point, meses_por_q)
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
        # Get all tipos used
        all_tipos = df_point[df_point["HAS_PROBLEM"] == True]["TIPO_PROBLEMA"].value_counts().head(8)
        for ti, tipo in enumerate(all_tipos.index):
            if pd.isna(tipo):
                continue
            color = MOTIVO_COLORS[ti % len(MOTIVO_COLORS)]
            label = str(tipo)[:30]
            html += f'      <div style="display:flex;align-items:center;gap:4px;"><div style="width:12px;height:12px;background:{color};border-radius:2px;"></div>{label}</div>\n'

        html += "    </div>\n  </div>\n"

    return html
