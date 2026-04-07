"""
Tab 1 — Resumen: NPS headline, narrativa, charts, detalle quejas.

Exporta `generar_tab1()` que recibe datos precalculados y devuelve HTML string.
También ejecutable standalone para testing.
"""

import json


# ── helpers ──────────────────────────────────────────────────────────

def _color_var(v):
    if v > 0.5:
        return "#d32f2f"
    if v < -0.5:
        return "#388e3c"
    return "#666"


def _fmt_mes(ms):
    # Si ya es un label de quarter (ej: "Q4 2025"), devolverlo tal cual
    if ms.startswith("Q") or not ms.isdigit():
        return ms
    nombres = {
        "01": "Ene", "02": "Feb", "03": "Mar", "04": "Abr",
        "05": "May", "06": "Jun", "07": "Jul", "08": "Ago",
        "09": "Sep", "10": "Oct", "11": "Nov", "12": "Dic",
    }
    return f"{nombres.get(ms[4:], ms[4:])} {ms[:4]}"


# ── main generator ──────────────────────────────────────────────────

def generar_tab1(
    *,
    site: str,
    quarter_actual: str,
    quarter_anterior: str,
    q_label_actual: str,
    q_label_anterior: str,
    nps_actual: float,
    var_qvsq: float,
    var_yoy: float,
    n_encuestas: int,
    parrafo_resumen: str,
    nps_evolucion: list[dict],
    motivos_top: list[dict],
    meses_quejas: list[str],
    variaciones_quejas: list[dict],
    tendencias_data: dict,
    causas_raiz_data: dict | None,
    razonamiento: dict,
    colores_config: dict,
) -> str:
    """Genera el HTML completo de Tab 1 (Resumen)."""

    mom_color = "#388e3c" if var_qvsq >= 0 else "#d32f2f"
    yoy_color = "#388e3c" if var_yoy >= 0 else "#d32f2f"

    # ── Narrative box ────────────────────────────────────────────────
    narrative = ""
    if parrafo_resumen:
        narrative = f"""
    <div class="narrative-box">{parrafo_resumen}</div>"""

    # ── Chart data ───────────────────────────────────────────────────
    chart_labels = json.dumps([_fmt_mes(d["mes"]) for d in nps_evolucion])
    chart_values = json.dumps([round(d["nps"]) for d in nps_evolucion])

    colores_default = [
        "#d32f2f", "#1565c0", "#2e7d32", "#ff6f00",
        "#6a1b9a", "#00838f", "#4e342e", "#455a64",
    ]

    def _color_motivo(motivo):
        import unicodedata
        if not colores_config or not isinstance(colores_config, dict):
            return "#9E9E9E"
        def _norm(s):
            s = (s or "").lower()
            s = unicodedata.normalize("NFD", s)
            return "".join(c for c in s if unicodedata.category(c) != "Mn")
        m_norm = _norm(motivo)
        for key in sorted((k for k in colores_config if k != "_default"), key=len, reverse=True):
            if _norm(key) in m_norm:
                return colores_config[key]
        return colores_config.get("_default", "#9E9E9E")

    quejas_datasets = []
    otros_dataset = None
    for i, mt in enumerate(motivos_top):
        c = _color_motivo(mt["motivo"])
        ds = {
            "label": mt["motivo"],
            "data": [d["valor"] for d in mt["serie"]],
            "backgroundColor": c,
        }
        # "Otros" goes last (above Resto) — hold it aside
        if mt["motivo"] == "Otros":
            otros_dataset = ds
        else:
            quejas_datasets.append(ds)

    # Calcular "Otros" para que el total cierre con 100 - NPS
    # nps_evolucion tiene {mes: label_quarter, nps: valor}
    nps_by_label = {d["mes"]: d["nps"] for d in nps_evolucion}
    n_periods = len(meses_quejas)
    otros_data = []
    for idx, ql in enumerate(meses_quejas):
        total_top8 = sum(ds["data"][idx] for ds in quejas_datasets if idx < len(ds["data"]))
        nps_val = nps_by_label.get(ql)
        if nps_val is None:
            otros_data.append(0)
            continue
        total_quejas_esperado = 100 - nps_val
        otros_val = max(0, round(total_quejas_esperado - total_top8, 2))
        otros_data.append(otros_val)
    # Add "Otros" last (on top of stack)
    if otros_dataset:
        quejas_datasets.append(otros_dataset)

    quejas_labels = json.dumps([_fmt_mes(m) for m in meses_quejas])
    quejas_ds_json = json.dumps(quejas_datasets)

    # Dynamic Y-axis max: round up to nearest 10 above max total quejas
    max_total_quejas = max(
        (100 - d["nps"] for d in nps_evolucion if d.get("nps") is not None),
        default=60,
    )
    quejas_y_max = int((max_total_quejas + 9) // 10 * 10)  # round up to nearest 10

    # ── Quejas cards ─────────────────────────────────────────────────
    bloque3 = razonamiento.get("bloque3", {})
    asociaciones = bloque3.get("asociaciones", [])
    asoc_by_motivo = {}
    for a in asociaciones:
        asoc_by_motivo.setdefault(a.get("motivo", ""), []).append(a)

    CLASIF_CSS = {
        "EXPLICA_OK": ("classify-ok", "Explicado por data"),
        "EXPLICA_MIX": ("classify-mix", "Sin cambio en driver"),
        "NO_EXPLICA": ("classify-no", "Sin variación"),
        "CONTRADICTORIO": ("classify-contra", "No coincide con data"),
        "FALLBACK_CP5": ("classify-cp5", "Basado en comentarios"),
    }

    # Override var_mom from razonamiento bloque2 (consistent source)
    bloque2 = razonamiento.get("bloque2", {})
    b2_var_map = {}
    for m in bloque2.get("todos", []):
        b2_var_map[m["motivo"]] = m["var_share"]

    cards_html = ""
    border_colors = [
        "var(--red)", "var(--orange)", "var(--green)", "var(--purple)",
        "var(--blue)", "var(--teal)", "#607d8b", "#795548",
    ]
    for idx, vq in enumerate(variaciones_quejas):
        motivo = vq.get("motivo")
        if motivo is None:
            continue
        var = b2_var_map.get(motivo, vq.get("var_mom", 0))
        q_act = vq.get("quejas_actual", 0)

        if var > 0.5:
            badge = f'<span class="badge badge-up">+{var:.1f}pp QvsQ</span>'
        elif var < -0.5:
            badge = f'<span class="badge badge-down">{var:.1f}pp QvsQ</span>'
        else:
            badge = f'<span class="badge badge-stable">{var:+.1f}pp</span>'

        # driver row from razonamiento
        driver_row = ""
        asocs = asoc_by_motivo.get(motivo, [])
        if asocs:
            a = asocs[0]
            cls_key = a.get("clasificacion", "NO_EXPLICA")
            css_cls, lbl = CLASIF_CSS.get(cls_key, ("classify-no", cls_key))
            wording = a.get("wording", "")
            driver_row = f'<div class="driver-row"><span class="classify-tag {css_cls}">{lbl}</span> {wording}</div>'
        else:
            driver_row = '<div class="driver-row"><span class="classify-tag classify-no">SIN DIMENSIÓN</span> Sin enriquecimiento mapeado</div>'

        # causa raiz from CP5
        causa_html = ""
        if causas_raiz_data and causas_raiz_data.get("causas_por_motivo", {}).get(motivo):
            causas = causas_raiz_data["causas_por_motivo"][motivo].get("causas_raiz", {})
            primera = next(iter(causas.values()), {}) if causas else {}
            if primera:
                causa_html = f'<div class="causa-raiz"><strong>Sellers dicen:</strong> {primera.get("titulo", "")}</div>'

        bc = border_colors[idx % len(border_colors)]
        detail_id = f"detail-{idx}"
        cards_html += f"""
      <div class="motivo-card" style="border-color:{bc};">
        <div class="card-header" onclick="var d=document.getElementById('{detail_id}');d.style.display=d.style.display==='none'?'block':'none';" style="cursor:pointer;">
          <h4>{motivo}</h4>{badge}
          <span class="expand-icon" style="font-size:12px;color:#999;margin-left:auto;">▼</span>
        </div>
        <div id="{detail_id}" class="card-detail" style="display:none;">
          {driver_row}
          {causa_html}
        </div>
      </div>"""

    # ── Compose tab HTML ─────────────────────────────────────────────
    html = f"""
  <div class="section">
    <div class="section-title">Resumen Ejecutivo</div>
    {narrative}
  </div>

  <div class="section">
    <div class="section-title">Evolución de Indicadores</div>
    <div class="chart-row">
      <div class="chart-col">
        <h3 style="font-size:14px;margin-bottom:12px;color:#555;">Evolución de NPS por Quarter</h3>
        <div class="chart-wrap"><canvas id="chartNPS"></canvas></div>
      </div>
      <div class="chart-col">
        <h3 style="font-size:14px;margin-bottom:12px;color:#555;">Evolución de Quejas por Motivo</h3>
        <div class="chart-wrap"><canvas id="chartQuejas"></canvas></div>
      </div>
    </div>
  </div>

  <div class="section">
    <div class="section-title">Detalle por Motivos de Queja</div>
    <div class="cards-grid">
      {cards_html}
    </div>
  </div>
"""

    # ── Chart JS (injected into page later) ──────────────────────────
    chart_js = f"""
const _npsLabels = {chart_labels};
const _npsValues = {chart_values};

new Chart(document.getElementById('chartNPS'), {{
  type:'line',
  data:{{labels:_npsLabels,datasets:[{{label:'NPS {site}',data:_npsValues,borderColor:'#2196f3',backgroundColor:'rgba(33,150,243,.1)',fill:true,tension:.35,pointRadius:5,pointBackgroundColor:'#2196f3',borderWidth:2.5}}]}},
  options:{{responsive:true,maintainAspectRatio:false,plugins:{{datalabels:{{display:false}},legend:{{display:true,position:'bottom',labels:{{font:{{size:11}},boxWidth:14,padding:8}}}},tooltip:{{callbacks:{{label:c=>c.parsed.y+' p.p.'}}}}}},scales:{{y:{{min:0,max:100,ticks:{{callback:v=>v+' p.p.'}}}}}}}},
  plugins:[{{
    id:'npsLabels',
    afterDatasetsDraw(chart){{
      const ctx=chart.ctx;
      ctx.save();
      ctx.font='bold 11px sans-serif';
      ctx.fillStyle='#333';
      ctx.textAlign='center';
      ctx.textBaseline='bottom';
      const meta=chart.getDatasetMeta(0);
      meta.data.forEach((pt,i)=>{{
        ctx.fillText(chart.data.datasets[0].data[i],pt.x,pt.y-8);
      }});
      ctx.restore();
    }}
  }}]
}});

const _quejasDS = {quejas_ds_json};
new Chart(document.getElementById('chartQuejas'), {{
  type:'bar',
  data:{{labels:{quejas_labels},datasets:_quejasDS}},
  options:{{
    responsive:true,maintainAspectRatio:false,
    plugins:{{
      datalabels:{{display:false}},
      legend:{{
        position:'bottom',
        labels:{{
          font:{{size:10}},
          boxWidth:12,
          boxHeight:12,
          padding:8,
          usePointStyle:false
        }}
      }},
      tooltip:{{callbacks:{{label:c=>c.dataset.label+': '+c.parsed.y.toFixed(1)+'%'}}}}
    }},
    scales:{{
      x:{{stacked:true,grid:{{display:false}},ticks:{{font:{{size:11,weight:'bold'}}}}}},
      y:{{stacked:true,min:0,max:{quejas_y_max},ticks:{{callback:v=>v+'%',stepSize:10,font:{{size:10}},color:'#999'}},grid:{{color:'#f5f5f5',drawBorder:false}}}}
    }},
    elements:{{bar:{{borderWidth:0,borderRadius:0,borderSkipped:false}}}}
  }},
  plugins:[{{
    id:'stackedLabels',
    afterDatasetsDraw(chart){{
      const ctx=chart.ctx;
      ctx.save();
      const umbralLabel = 2.0;
      ctx.font='bold 10px sans-serif';
      ctx.textAlign='center';
      ctx.textBaseline='middle';
      const lightColors = ['#FDD835','#AED581','#BDBDBD','#bdbdbd','#F48FB1','#4FC3F7','#CE93D8','#9E9E9E','#fff3e0','#e8f5e9','#e0e0e0','#FFD54F'];
      function needsDarkText(bgColor){{
        if(!bgColor) return false;
        return lightColors.some(c=>c.toLowerCase()===bgColor.toLowerCase());
      }}
      chart.data.datasets.forEach((ds,di)=>{{
        const meta=chart.getDatasetMeta(di);
        meta.data.forEach((bar,i)=>{{
          const v=ds.data[i];
          const h=bar.height||0;
          if(v>=umbralLabel && h>14){{
            ctx.fillStyle=needsDarkText(ds.backgroundColor)?'#333':'#fff';
            ctx.fillText(Math.round(v)+'%',bar.x,bar.y+h/2);
          }}
        }});
      }});
      const nLabels=chart.data.labels.length;
      ctx.font='bold 12px sans-serif';
      ctx.fillStyle='#333';
      ctx.textBaseline='bottom';
      for(let i=0;i<nLabels;i++){{
        let total=0;
        chart.data.datasets.forEach(ds=>{{total+=(ds.data[i]||0);}});
        let minY=chart.chartArea.bottom;
        chart.data.datasets.forEach((ds,di)=>{{
          const meta=chart.getDatasetMeta(di);
          const bar=meta.data[i];
          if(bar && bar.y<minY) minY=bar.y;
        }});
        ctx.fillText(Math.round(total)+'%',chart.getDatasetMeta(0).data[i].x,minY-4);
      }}
      ctx.restore();
    }}
  }}]
}});
"""

    return html, chart_js
