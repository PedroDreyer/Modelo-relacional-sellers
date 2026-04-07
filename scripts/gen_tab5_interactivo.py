"""
Tab 5 — Dashboard Interactivo: filtros globales + NPS + Quejas + Secciones completas.

Secciones:
  - Credits: Uso crédito, Oferta TC, Tarjeta TC, Límite TC, FRED Groups
  - Inversiones: Uso inversiones, POTS, Winner, Asset
  - Pricing: Con/Sin Pricing, Escala
  - PdF: por device, tipos de problema
  - Top Off: cobertura

Para cada dimensión muestra NPS Q_ant / Q_act / Var + Share encuestas + Share real (universo).
Todo adaptado al filtro.  Universo = realidad de mercado (no filtrable).
"""

from __future__ import annotations

import json
import html as html_mod
import pandas as pd

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

_FILTER_DIMS = [
    "PRODUCTO_PRINCIPAL", "NEWBIE_LEGACY", "FLAG_USA_CREDITO",
    "FLAG_TARJETA_CREDITO", "FLAG_USA_INVERSIONES", "FLAG_TOPOFF",
    "FLAG_WINNER", "FLAG_PRICING", "SCALE_LEVEL",
]

_METRICA_TABS = [
    ("credits",    "Credits",    "Créditos"),
    ("inversiones","Inversiones","Inversiones"),
    ("pricing",    "Pricing",    "Pricing por Escalas"),
    ("pdf",        "PdF",        "Problemas de Funcionamiento"),
    ("topoff",     "Top Off",    "Atención al Cliente"),
]


def _esc(text) -> str:
    return html_mod.escape(str(text)) if text else ""


def _agg_universo_q(universo: dict, meses_por_q: dict, q_ant: str, q_act: str) -> dict:
    """Pre-aggregate universo monthly shares → quarter-level averages.

    Returns:  { dim_key: { val: { q_ant: float|None, q_act: float|None } } }
    """
    out: dict = {}
    for dim_key, vals in universo.items():
        out[dim_key] = {}
        for val, mes_shares in vals.items():
            def _avg(q):
                meses = meses_por_q.get(q, [])
                nums = [mes_shares[m] for m in meses if m in mes_shares]
                return round(sum(nums) / len(nums), 1) if nums else None
            out[dim_key][val] = {"q_ant": _avg(q_ant), "q_act": _avg(q_act)}
    return out


def _export_raw_data(df: pd.DataFrame) -> str:
    """Export compact JSON for JS computation."""
    from nps_model.utils.motivos import consolidar_motivo
    import unicodedata

    extra_dims = [
        "OFERTA_TC", "RANGO_LIMITE_TC", "FLAG_POTS_ACTIVO",
        "FLAG_ASSET", "FLAG_INVERSIONES",
    ]
    filter_dims = [d for d in _FILTER_DIMS if d in df.columns]
    extra_available = [d for d in extra_dims if d in df.columns]

    records = []
    for _, row in df.iterrows():
        r: dict = {
            "m": str(row.get("END_DATE_MONTH", "")),
            "nps": int(row["NPS"]) if pd.notna(row.get("NPS")) else None,
        }
        mot = row.get("MOTIVO")
        if pd.notna(mot):
            r["mot"] = consolidar_motivo(str(mot))
        dev = row.get("MODELO_DEVICE")
        if pd.notna(dev):
            r["dev"] = str(dev)
        pf = row.get("PROBLEMA_FUNCIONAMIENTO")
        if pd.notna(pf):
            pf_norm = "".join(
                c for c in unicodedata.normalize("NFD", str(pf).lower())
                if unicodedata.category(c) != "Mn"
            )
            r["hp"] = 1 if pf_norm in ("sim", "si", "1", "true", "yes") else 0
        tp = row.get("TIPO_PROBLEMA")
        if pd.notna(tp):
            r["tp"] = str(tp)
        cg = row.get("CREDIT_GROUP")
        if pd.notna(cg):
            r["cg"] = str(cg)
        for d in filter_dims:
            v = row.get(d)
            if pd.notna(v):
                r[d] = str(v)
        # Extra dims
        _EXTRA_KEYS = {
            "OFERTA_TC": "ot", "RANGO_LIMITE_TC": "rlt",
            "FLAG_POTS_ACTIVO": "pots", "FLAG_ASSET": "asset",
            "FLAG_INVERSIONES": "finv",
        }
        for d in extra_available:
            v = row.get(d)
            if pd.notna(v):
                r[_EXTRA_KEYS.get(d, d)] = str(v)
        records.append(r)
    return json.dumps(records, ensure_ascii=False)


def generar_tab5(
    *,
    df: pd.DataFrame,
    meses_por_q: dict[str, list[str]],
    quarter_actual: str = "",
    quarter_anterior: str = "",
    colores_motivos: dict | None = None,
    universo_credits: dict | None = None,
    universo_inversiones: dict | None = None,
    universo_pricing: dict | None = None,
    universo_topoff: dict | None = None,
) -> str:
    """Genera HTML + JS del Tab 5 (Dashboard Interactivo)."""

    dims_available = [d for d in _FILTER_DIMS if d in df.columns]
    all_qs = sorted(meses_por_q.keys())
    colores_json = json.dumps(colores_motivos or {})
    data_json = _export_raw_data(df)
    meses_json = json.dumps(meses_por_q)

    # Pre-aggregate universo data to quarter level
    u_credits    = _agg_universo_q(universo_credits or {},    meses_por_q, quarter_anterior, quarter_actual)
    u_inversiones= _agg_universo_q(universo_inversiones or {}, meses_por_q, quarter_anterior, quarter_actual)
    u_pricing    = _agg_universo_q(universo_pricing or {},    meses_por_q, quarter_anterior, quarter_actual)
    u_topoff     = _agg_universo_q(universo_topoff or {},     meses_por_q, quarter_anterior, quarter_actual)

    universo_json = json.dumps({
        "credits": u_credits,
        "inversiones": u_inversiones,
        "pricing": u_pricing,
        "topoff": u_topoff,
    }, ensure_ascii=False)

    # Collect unique values per dimension for dropdowns
    dim_values: dict[str, list[str]] = {}
    for d in dims_available:
        vals = sorted(df[d].dropna().unique().astype(str))
        if vals:
            dim_values[d] = vals

    # ── HTML ────────────────────────────────────────────────────────
    html = """
  <div class="section">
    <div class="section-title">Dashboard Interactivo</div>
    <p class="text-sm text-muted" style="margin-bottom:12px;">
      Filtrá para recalcular todas las métricas en tiempo real. El universo (realidad de mercado) no se filtra.
    </p>
"""

    # Filter bar
    html += '    <div id="t5-filters" style="display:flex;flex-wrap:wrap;gap:8px;margin-bottom:18px;align-items:center;padding:10px 14px;background:#f8f9fa;border-radius:8px;border:1px solid #e0e0e0;">\n'
    html += '      <span style="font-size:12px;font-weight:600;color:#555;">Filtrar:</span>\n'
    for dim in dims_available:
        vals = dim_values.get(dim, [])
        if not vals:
            continue
        label = _DIM_LABELS.get(dim, dim)
        html += f'      <select class="t5-filter" data-dim="{dim}" style="font-size:11px;padding:3px 6px;border:1px solid #ccc;border-radius:5px;background:white;">\n'
        html += f'        <option value="">{_esc(label)}: Todos</option>\n'
        for v in vals:
            html += f'        <option value="{_esc(v)}">{_esc(v)}</option>\n'
        html += '      </select>\n'
    html += '      <button id="t5-clear" style="font-size:11px;padding:3px 10px;border:1px solid #ccc;border-radius:5px;background:#fff;cursor:pointer;">Limpiar</button>\n'
    html += '      <span id="t5-count" style="font-size:11px;color:#888;margin-left:auto;"></span>\n'
    html += '    </div>\n'

    # NPS + Quejas side by side
    html += '    <div style="display:grid;grid-template-columns:1fr 1fr;gap:16px;margin-bottom:20px;">\n'

    html += """      <div style="background:white;border:1px solid #e0e0e0;border-radius:10px;padding:20px;">
        <h4 style="margin:0 0 12px;font-size:14px;color:#555;">NPS</h4>
        <div style="display:flex;align-items:baseline;gap:12px;margin-bottom:8px;">
          <span id="t5-nps-value" style="font-size:42px;font-weight:700;color:#333;">--</span>
          <span style="font-size:14px;color:#888;">p.p.</span>
          <span id="t5-nps-var" style="font-size:16px;font-weight:600;"></span>
        </div>
        <div id="t5-nps-detail" style="font-size:12px;color:#888;margin-bottom:12px;"></div>
        <div style="height:200px;"><canvas id="t5ChartNps"></canvas></div>
      </div>
"""
    html += """      <div style="background:white;border:1px solid #e0e0e0;border-radius:10px;padding:20px;">
        <h4 style="margin:0 0 12px;font-size:14px;color:#555;">Evolución de Quejas por Motivo</h4>
        <div style="height:260px;"><canvas id="t5ChartQuejas"></canvas></div>
      </div>
"""
    html += '    </div>\n'

    # Métrica selector buttons
    html += '    <div style="display:flex;gap:6px;margin-bottom:16px;flex-wrap:wrap;">\n'
    for mid, mlabel, _ in _METRICA_TABS:
        active_style = "background:#FFE600;font-weight:700;border-color:#ccc;" if mid == "credits" else "background:#f5f5f5;"
        html += f'      <button class="t5-metrica-btn" data-metrica="{mid}" style="font-size:12px;padding:6px 16px;border:1px solid #ddd;border-radius:6px;cursor:pointer;{active_style}">{mlabel}</button>\n'
    html += '    </div>\n'

    # Métrica blocks
    for mid, _, mtitle in _METRICA_TABS:
        display = "block" if mid == "credits" else "none"
        html += f'    <div id="t5-metrica-{mid}" class="t5-metrica-block" style="display:{display};background:white;border:1px solid #e0e0e0;border-radius:10px;padding:20px;">\n'
        html += f'      <h4 style="margin:0 0 16px;font-size:14px;color:#555;">{_esc(mtitle)}</h4>\n'
        html += f'      <div id="t5-metrica-content-{mid}"></div>\n'
        html += '    </div>\n'

    html += '  </div>\n'

    # ── JS Engine ───────────────────────────────────────────────────
    html += f"""
  <script>
  (function(){{
    const RAW = {data_json};
    const MESES_Q = {meses_json};
    const ALL_QS = Object.keys(MESES_Q).sort();
    const Q_ACT = '{quarter_actual}';
    const Q_ANT = '{quarter_anterior}';
    const MOTIVOS_EXCL = ['otros','outros','sin informacion','sem informacao','otros motivos','outro'];
    const COLORES = {colores_json};
    const UNIVERSO = {universo_json};

    const filters = document.querySelectorAll('.t5-filter');
    const clearBtn = document.getElementById('t5-clear');
    let npsChart = null;
    let quejasChart = null;

    // ── Helpers ─────────────────────────────────────────────────────

    function getFiltered(){{
      const active = {{}};
      filters.forEach(f => {{ if(f.value) active[f.dataset.dim] = f.value; }});
      if(!Object.keys(active).length) return RAW;
      return RAW.filter(r => {{
        for(const [dim,val] of Object.entries(active)){{
          if(r[dim] !== undefined && r[dim] !== val) return false;
        }}
        return true;
      }});
    }}

    function calcNPS(data){{
      const valid = data.filter(r => r.nps !== null && r.nps !== undefined);
      if(!valid.length) return null;
      return valid.reduce((s,r) => s + r.nps, 0) / valid.length * 100;
    }}

    function calcNPSbyQ(data){{
      return ALL_QS.map(q => {{
        const meses = MESES_Q[q] || [];
        const sub = data.filter(r => meses.includes(r.m));
        return sub.length >= 10 ? Math.round(calcNPS(sub)*10)/10 : null;
      }});
    }}

    function calcQuejas(data){{
      const valid = data.filter(r => r.nps !== null && r.nps !== undefined);
      const total = valid.length;
      if(!total) return [];
      const det_neu = valid.filter(r => r.nps <= 0 && r.mot);
      const weighted = {{}};
      det_neu.forEach(r => {{
        const w = r.nps < 0 ? 2 : 1;
        weighted[r.mot] = (weighted[r.mot]||0) + w;
      }});
      return Object.entries(weighted)
        .filter(([m]) => !MOTIVOS_EXCL.some(e => m.toLowerCase().includes(e)))
        .map(([m,w]) => ({{motivo:m, share:Math.round(w/total*1000)/10}}))
        .sort((a,b) => b.share - a.share)
        .slice(0,12);
    }}

    function calcQuejasByQ(data, q){{
      const meses = MESES_Q[q] || [];
      return calcQuejas(data.filter(r => meses.includes(r.m)));
    }}

    // Compute NPS + share for a field value from survey data
    function calcDimStats(dAct, dAnt, keyFn){{
      const totalAct = dAct.length || 1;
      const totalAnt = dAnt.length || 1;
      const mapAct = {{}};
      const mapAnt = {{}};
      dAct.forEach(r => {{
        const v = keyFn(r);
        if(!v) return;
        if(!mapAct[v]) mapAct[v] = {{nps:[], count:0}};
        mapAct[v].count++;
        if(r.nps !== null && r.nps !== undefined) mapAct[v].nps.push(r.nps);
      }});
      dAnt.forEach(r => {{
        const v = keyFn(r);
        if(!v) return;
        if(!mapAnt[v]) mapAnt[v] = {{nps:[]}};
        if(r.nps !== null && r.nps !== undefined) mapAnt[v].nps.push(r.nps);
      }});
      // Merge keys from both periods
      const allVals = Array.from(new Set([...Object.keys(mapAct), ...Object.keys(mapAnt)]));
      return allVals.map(v => {{
        const act = mapAct[v] || {{nps:[], count:0}};
        const ant = mapAnt[v] || {{nps:[]}};
        const npsAct = act.nps.length ? act.nps.reduce((s,n)=>s+n,0)/act.nps.length*100 : null;
        const npsAnt = ant.nps.length ? ant.nps.reduce((s,n)=>s+n,0)/ant.nps.length*100 : null;
        return {{
          val: v,
          count: act.count,
          shareEnc: act.count / totalAct * 100,
          shareEncAnt: (mapAnt[v] ? dAnt.filter(r=>keyFn(r)===v).length : 0) / totalAnt * 100,
          npsAct, npsAnt,
        }};
      }}).sort((a,b) => b.count - a.count);
    }}

    // Render a comprehensive table + mini bar chart for one dimension
    function renderDimBlock(dAct, dAnt, keyFn, label, universoData, dimKey){{
      const stats = calcDimStats(dAct, dAnt, keyFn);
      const hasUniverso = universoData && dimKey && universoData[dimKey];
      const uData = hasUniverso ? universoData[dimKey] : null;

      let h = '<div style="margin-bottom:24px;">';
      h += '<div style="font-size:12px;font-weight:700;color:#1565c0;margin-bottom:8px;padding-bottom:4px;border-bottom:2px solid #e3f2fd;">' + label + '</div>';

      // Table
      h += '<div style="overflow-x:auto;"><table style="width:100%;border-collapse:collapse;font-size:12px;">';
      h += '<thead><tr style="background:#f5f5f5;font-size:11px;">';
      h += '<th style="padding:6px 8px;text-align:left;font-weight:600;">Grupo</th>';
      h += '<th style="padding:6px 8px;text-align:center;font-weight:600;">NPS ' + Q_ANT + '</th>';
      h += '<th style="padding:6px 8px;text-align:center;font-weight:600;">NPS ' + Q_ACT + '</th>';
      h += '<th style="padding:6px 8px;text-align:center;font-weight:600;">Δ NPS</th>';
      h += '<th style="padding:6px 8px;text-align:center;font-weight:600;">Share Enc. ' + Q_ANT + '</th>';
      h += '<th style="padding:6px 8px;text-align:center;font-weight:600;">Share Enc. ' + Q_ACT + '</th>';
      h += '<th style="padding:6px 8px;text-align:center;font-weight:600;">Δ Share</th>';
      if(uData) h += '<th style="padding:6px 8px;text-align:center;font-weight:600;color:#1976d2;">Real ' + Q_ACT + '</th>';
      h += '<th style="padding:6px 8px;text-align:center;font-weight:600;color:#888;">n</th>';
      h += '</tr></thead><tbody>';

      stats.forEach((s,i) => {{
        const bg = i % 2 === 0 ? '#fff' : '#fafafa';
        const nAct = s.npsAct !== null ? s.npsAct.toFixed(1) : '<span style="color:#ccc">--</span>';
        const nAnt = s.npsAnt !== null ? s.npsAnt.toFixed(1) : '<span style="color:#ccc">--</span>';
        const dNps = s.npsAct !== null && s.npsAnt !== null ? s.npsAct - s.npsAnt : null;
        const dShare = s.shareEnc - s.shareEncAnt;
        const npsColor = dNps === null ? '#888' : (dNps >= 0 ? '#388e3c' : '#d32f2f');
        const shareColor = dShare >= 0 ? '#388e3c' : '#d32f2f';
        const npsStr = dNps === null ? '--' : (dNps>=0?'+':'') + dNps.toFixed(1) + 'pp';
        const shareStr = (dShare>=0?'+':'') + dShare.toFixed(1) + 'pp';

        // Universo data
        let uAct = null, uAnt = null;
        if(uData && uData[s.val]) {{ uAct = uData[s.val].q_act; uAnt = uData[s.val].q_ant; }}

        h += '<tr style="background:' + bg + ';border-bottom:1px solid #eee;">';
        h += '<td style="padding:6px 8px;font-weight:500;">' + s.val + '</td>';
        h += '<td style="padding:6px 8px;text-align:center;">' + nAnt + '</td>';
        h += '<td style="padding:6px 8px;text-align:center;font-weight:600;">' + nAct + '</td>';
        h += '<td style="padding:6px 8px;text-align:center;color:' + npsColor + ';font-weight:700;">' + npsStr + '</td>';
        h += '<td style="padding:6px 8px;text-align:center;color:#666;">' + s.shareEncAnt.toFixed(1) + '%</td>';
        h += '<td style="padding:6px 8px;text-align:center;">' + s.shareEnc.toFixed(1) + '%</td>';
        h += '<td style="padding:6px 8px;text-align:center;color:' + shareColor + ';font-weight:600;">' + shareStr + '</td>';
        if(uData) {{
          const uActStr = uAct !== null ? uAct.toFixed(1) + '%' : '--';
          const dRealStr = (uAct !== null && uAnt !== null) ? ((uAct - uAnt) >= 0 ? '+' : '') + (uAct - uAnt).toFixed(1) + 'pp' : '';
          const dRealColor = (uAct !== null && uAnt !== null) ? (uAct - uAnt >= 0 ? '#1976d2' : '#e64a19') : '#888';
          h += '<td style="padding:6px 8px;text-align:center;color:#1976d2;font-weight:500;">' + uActStr + ' <span style="font-size:10px;color:' + dRealColor + ';">(' + dRealStr + ')</span></td>';
        }}
        h += '<td style="padding:6px 8px;text-align:center;color:#aaa;font-size:11px;">' + s.count + '</td>';
        h += '</tr>';
      }});

      h += '</tbody></table></div>';

      // Mini visual: enc share vs real share bar comparison (if universo available)
      if(uData && stats.length > 0){{
        const visStats = stats.filter(s => s.count > 0 && uData[s.val]);
        if(visStats.length > 0){{
          h += '<div style="margin-top:12px;display:flex;flex-wrap:wrap;gap:8px;">';
          visStats.forEach(s => {{
            const encPct = Math.round(s.shareEnc);
            const realPct = uData[s.val] && uData[s.val].q_act !== null ? Math.round(uData[s.val].q_act) : null;
            if(realPct === null) return;
            const maxW = Math.max(encPct, realPct, 1);
            h += '<div style="min-width:140px;flex:1;">';
            h += '<div style="font-size:10px;font-weight:600;color:#555;margin-bottom:3px;">' + s.val + '</div>';
            h += '<div style="font-size:10px;color:#888;margin-bottom:2px;">Enc: <span style="color:#1565c0;font-weight:600;">' + encPct + '%</span>';
            h += ' &nbsp;Real: <span style="color:#1976d2;font-weight:600;">' + realPct + '%</span></div>';
            h += '<div style="height:6px;background:#e3f2fd;border-radius:3px;margin-bottom:2px;">';
            h += '<div style="height:6px;background:#1565c0;border-radius:3px;width:' + Math.round(encPct/maxW*100) + '%;"></div></div>';
            h += '<div style="height:6px;background:#e8f5e9;border-radius:3px;">';
            h += '<div style="height:6px;background:#43a047;border-radius:3px;width:' + Math.round(realPct/maxW*100) + '%;"></div></div>';
            h += '</div>';
          }});
          h += '</div>';
          h += '<div style="margin-top:4px;font-size:10px;color:#aaa;">&#9646; Encuestas &nbsp; <span style="color:#43a047;">&#9646;</span> Universo (mercado)</div>';
        }}
      }}

      h += '</div>';
      return h;
    }}

    // ── NPS Block ──────────────────────────────────────────────────

    function rebuildNPS(data){{
      const mesAct = MESES_Q[Q_ACT] || [];
      const mesAnt = MESES_Q[Q_ANT] || [];
      const npsAct = calcNPS(data.filter(r => mesAct.includes(r.m)));
      const npsAnt = calcNPS(data.filter(r => mesAnt.includes(r.m)));
      const nAct = data.filter(r => mesAct.includes(r.m)).length;

      const valEl = document.getElementById('t5-nps-value');
      const varEl = document.getElementById('t5-nps-var');
      const detEl = document.getElementById('t5-nps-detail');

      valEl.textContent = npsAct !== null ? npsAct.toFixed(1) : '--';
      if(npsAct !== null && npsAnt !== null){{
        const v = npsAct - npsAnt;
        varEl.textContent = (v >= 0 ? '+' : '') + v.toFixed(1) + 'pp QvsQ';
        varEl.style.color = v >= 0 ? '#388e3c' : '#d32f2f';
      }} else {{ varEl.textContent = ''; }}
      detEl.textContent = nAct + ' encuestas en ' + Q_ACT + (npsAnt !== null ? ' | ' + Q_ANT + ': ' + npsAnt.toFixed(1) + ' p.p.' : '');

      const npsValues = calcNPSbyQ(data);
      if(npsChart){{
        npsChart.data.labels = ALL_QS;
        npsChart.data.datasets[0].data = npsValues;
        npsChart.update();
      }} else {{
        const ctx = document.getElementById('t5ChartNps');
        if(ctx){{
          npsChart = new Chart(ctx, {{
            type:'line',
            data:{{labels:ALL_QS,datasets:[{{label:'NPS',data:npsValues,borderColor:'#1565c0',backgroundColor:'rgba(21,101,192,.1)',fill:true,tension:0.3,pointRadius:5,pointBackgroundColor:'#1565c0',borderWidth:2}}]}},
            options:{{
              responsive:true,maintainAspectRatio:false,
              plugins:{{
                datalabels:{{anchor:'end',align:'top',font:{{size:11,weight:'bold'}},formatter:v=>v!==null?v.toFixed(0):'',color:'#1565c0'}},
                legend:{{display:false}},
              }},
              scales:{{
                y:{{ticks:{{callback:v=>v+' p.p.',font:{{size:10}}}},grid:{{color:'#f0f0f0'}}}},
                x:{{ticks:{{font:{{size:10}}}}}},
              }},
            }},
            plugins:[ChartDataLabels],
          }});
        }}
      }}
    }}

    // ── Quejas Block ──────────────────────────────────────────────

    function getColor(motivo){{
      const m = motivo.toLowerCase().normalize('NFD').replace(/[\u0300-\u036f]/g,'');
      const keys = Object.keys(COLORES).filter(k=>k!=='_default').sort((a,b)=>b.length-a.length);
      for(const k of keys){{
        if(m.includes(k.toLowerCase().normalize('NFD').replace(/[\u0300-\u036f]/g,''))) return COLORES[k];
      }}
      return COLORES._default||'#9E9E9E';
    }}

    function rebuildQuejas(data){{
      const qAct = calcQuejasByQ(data, Q_ACT);
      const topMotivos = qAct.map(q=>q.motivo);
      const datasets = [];
      topMotivos.forEach(mot=>{{
        const values = ALL_QS.map(q=>{{
          const quejas = calcQuejasByQ(data,q);
          const found = quejas.find(qj=>qj.motivo===mot);
          return found ? found.share : 0;
        }});
        datasets.push({{label:mot,data:values,backgroundColor:getColor(mot)}});
      }});
      const restoData = ALL_QS.map((q,qi)=>{{
        const meses = MESES_Q[q]||[];
        const qData = data.filter(r=>meses.includes(r.m));
        const nps = calcNPS(qData);
        if(nps===null) return 0;
        const totalTop = datasets.reduce((s,ds)=>s+(ds.data[qi]||0),0);
        return Math.max(0,Math.round((100-nps)*10-totalTop*10)/10);
      }});
      if(restoData.some(v=>v>0.1)) datasets.push({{label:'Resto',data:restoData,backgroundColor:'#e0e0e0'}});

      const canvas = document.getElementById('t5ChartQuejas');
      if(quejasChart) quejasChart.destroy();
      quejasChart = new Chart(canvas,{{
        type:'bar',
        data:{{labels:ALL_QS,datasets}},
        options:{{
          responsive:true,maintainAspectRatio:false,
          plugins:{{
            datalabels:{{display:false}},
            legend:{{position:'bottom',labels:{{font:{{size:9}},boxWidth:10,padding:6}}}},
            tooltip:{{callbacks:{{label:c=>c.dataset.label+': '+c.parsed.y.toFixed(1)+'%'}}}},
          }},
          scales:{{
            x:{{stacked:true,grid:{{display:false}},ticks:{{font:{{size:10,weight:'bold'}}}}}},
            y:{{stacked:true,min:0,ticks:{{callback:v=>v+'%',stepSize:10,font:{{size:9}}}},grid:{{color:'#f5f5f5'}}}},
          }},
          elements:{{bar:{{borderWidth:0,borderRadius:0}}}},
        }},
        plugins:[{{
          id:'stackTotals',
          afterDatasetsDraw(chart){{
            const ctx=chart.ctx;
            ctx.save();
            const nLabels=chart.data.labels.length;
            ctx.font='bold 11px sans-serif';ctx.fillStyle='#333';ctx.textAlign='center';ctx.textBaseline='bottom';
            for(let i=0;i<nLabels;i++){{
              let total=0;
              chart.data.datasets.forEach(ds=>{{total+=(ds.data[i]||0);}});
              let minY=chart.chartArea.bottom;
              chart.data.datasets.forEach((ds,di)=>{{const bar=chart.getDatasetMeta(di).data[i];if(bar&&bar.y<minY)minY=bar.y;}});
              ctx.fillText(Math.round(total)+'%',chart.getDatasetMeta(0).data[i].x,minY-4);
            }}
            ctx.font='bold 9px sans-serif';ctx.textBaseline='middle';
            chart.data.datasets.forEach((ds,di)=>{{
              const meta=chart.getDatasetMeta(di);
              meta.data.forEach((bar,i)=>{{
                const v=ds.data[i];const h=bar.height||0;
                if(v>=2&&h>12){{ctx.fillStyle='#fff';ctx.fillText(Math.round(v)+'%',bar.x,bar.y+h/2);}}
              }});
            }});
            ctx.restore();
          }}
        }}],
      }});
    }}

    // ── Métrica Blocks ────────────────────────────────────────────

    function rebuildMetrica(data, metrica){{
      const el = document.getElementById('t5-metrica-content-' + metrica);
      if(!el) return;
      const mesAct = MESES_Q[Q_ACT] || [];
      const mesAnt = MESES_Q[Q_ANT] || [];
      const dAct = data.filter(r => mesAct.includes(r.m));
      const dAnt = data.filter(r => mesAnt.includes(r.m));

      if(metrica === 'credits'){{
        let h = '';
        h += renderDimBlock(dAct, dAnt, r=>r.FLAG_USA_CREDITO, 'Uso de Crédito', UNIVERSO.credits, 'FLAG_USA_CREDITO');
        h += renderDimBlock(dAct, dAnt, r=>r.ot||r.OFERTA_TC, 'Oferta TC', null, null);
        h += renderDimBlock(dAct, dAnt, r=>r.FLAG_TARJETA_CREDITO, 'Uso Tarjeta de Crédito MP', UNIVERSO.credits, 'FLAG_TARJETA_CREDITO');
        h += renderDimBlock(dAct, dAnt, r=>r.rlt, 'Rango Límite TC (usuarios con TC)', null, null);
        h += renderDimBlock(dAct, dAnt, r=>r.cg, 'FRED Groups', UNIVERSO.credits, 'CREDIT_GROUP');
        el.innerHTML = h;

      }} else if(metrica === 'inversiones'){{
        let h = '';
        h += renderDimBlock(dAct, dAnt, r=>r.FLAG_USA_INVERSIONES, 'Uso de Inversiones', UNIVERSO.inversiones, 'FLAG_USA_INVERSIONES');
        h += renderDimBlock(dAct, dAnt, r=>r.pots||r.FLAG_POTS_ACTIVO, 'POTS Activo', UNIVERSO.inversiones, 'FLAG_POTS_ACTIVO');
        h += renderDimBlock(dAct, dAnt, r=>r.FLAG_WINNER, 'Winner (turbinado)', null, null);
        h += renderDimBlock(dAct, dAnt, r=>r.asset, 'Asset', null, null);
        el.innerHTML = h;

      }} else if(metrica === 'pricing'){{
        let h = '';
        h += renderDimBlock(dAct, dAnt, r=>r.FLAG_PRICING, 'Con/Sin Pricing Escalas', UNIVERSO.pricing, 'FLAG_PRICING');
        h += renderDimBlock(dAct, dAnt, r=>r.SCALE_LEVEL, 'Nivel de Escala', UNIVERSO.pricing, 'SCALE_LEVEL');
        el.innerHTML = h;

      }} else if(metrica === 'topoff'){{
        let h = '';
        h += renderDimBlock(dAct, dAnt, r=>r.FLAG_TOPOFF, 'Cobertura Top Off', UNIVERSO.topoff, 'FLAG_TOPOFF');
        el.innerHTML = h;

      }} else if(metrica === 'pdf'){{
        // PdF section
        const withDev = dAct.filter(r => r.dev);
        const withProb = withDev.filter(r => r.hp === 1);
        const pdfPct = withDev.length ? (withProb.length / withDev.length * 100).toFixed(1) : '--';
        let h = '';
        h += '<div style="font-size:28px;font-weight:700;margin-bottom:4px;">' + pdfPct + '% <span style="font-size:14px;font-weight:400;color:#888;">con problema de funcionamiento</span></div>';
        h += '<div style="font-size:11px;color:#aaa;margin-bottom:16px;">Base: respuestas con device identificado (' + withDev.length + ' enc.)</div>';

        // By device: NPS + PdF
        const devMap = {{}};
        withDev.forEach(r => {{
          if(!devMap[r.dev]) devMap[r.dev] = {{total:0,prob:0,nps:[],npsAnt:[]}};
          devMap[r.dev].total++;
          if(r.hp===1) devMap[r.dev].prob++;
          if(r.nps!==null&&r.nps!==undefined) devMap[r.dev].nps.push(r.nps);
        }});
        // Q_ant NPS by device
        dAnt.filter(r=>r.dev).forEach(r=>{{
          if(!devMap[r.dev]) devMap[r.dev] = {{total:0,prob:0,nps:[],npsAnt:[]}};
          if(r.nps!==null&&r.nps!==undefined) devMap[r.dev].npsAnt.push(r.nps);
        }});

        h += '<div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(160px,1fr));gap:10px;margin-bottom:20px;">';
        Object.entries(devMap).sort((a,b)=>b[1].total-a[1].total).forEach(([dev,d])=>{{
          const pdf = d.total ? (d.prob/d.total*100).toFixed(0) : '--';
          const npsA = d.nps.length ? (d.nps.reduce((s,n)=>s+n,0)/d.nps.length*100).toFixed(1) : '--';
          const npsAnt = d.npsAnt.length ? (d.npsAnt.reduce((s,n)=>s+n,0)/d.npsAnt.length*100).toFixed(1) : '--';
          const v = npsA!=='--'&&npsAnt!=='--' ? (parseFloat(npsA)-parseFloat(npsAnt)).toFixed(1) : null;
          const vColor = v!==null?(parseFloat(v)>=0?'#388e3c':'#d32f2f'):'#888';
          h += '<div style="background:#f9f9f9;border-radius:8px;padding:12px;border:1px solid #eee;">';
          h += '<div style="font-weight:700;font-size:13px;margin-bottom:6px;">' + dev + '</div>';
          h += '<div style="font-size:22px;font-weight:700;color:#e53935;">' + pdf + '%</div>';
          h += '<div style="font-size:10px;color:#888;margin-bottom:6px;">PdF</div>';
          h += '<div style="font-size:14px;font-weight:600;color:#333;">' + npsA + ' <span style="font-size:10px;color:' + vColor + ';">' + (v!==null?(v>=0?'+':'')+v+'pp':'') + '</span></div>';
          h += '<div style="font-size:10px;color:#aaa;">' + Q_ANT + ': ' + npsAnt + ' &nbsp;|&nbsp; ' + d.total + ' enc.</div>';
          h += '</div>';
        }});
        h += '</div>';

        // Top problemas
        if(withProb.length){{
          const tipos = {{}};
          withProb.forEach(r=>{{if(r.tp) tipos[r.tp]=(tipos[r.tp]||0)+1;}});
          const top = Object.entries(tipos).sort((a,b)=>b[1]-a[1]).slice(0,8);
          h += '<div style="font-size:12px;font-weight:700;color:#555;margin-bottom:8px;">Top Problemas de Funcionamiento</div>';
          h += '<div style="display:grid;grid-template-columns:1fr 1fr;gap:6px;">';
          top.forEach(([t,c])=>{{
            const pct = (c/withProb.length*100).toFixed(0);
            const barW = Math.round(c/withProb.length*100);
            h += '<div style="background:#fafafa;border-radius:6px;padding:8px;border:1px solid #f0f0f0;">';
            h += '<div style="font-size:11px;font-weight:500;margin-bottom:4px;">' + t + '</div>';
            h += '<div style="display:flex;align-items:center;gap:6px;">';
            h += '<div style="flex:1;height:6px;background:#eee;border-radius:3px;"><div style="height:6px;background:#ef9a9a;border-radius:3px;width:'+barW+'%;"></div></div>';
            h += '<span style="font-size:11px;font-weight:600;color:#e53935;">' + pct + '%</span></div>';
            h += '<div style="font-size:10px;color:#aaa;">' + c + ' casos</div>';
            h += '</div>';
          }});
          h += '</div>';
        }}
        el.innerHTML = h;
      }}
    }}

    // ── Orchestration ─────────────────────────────────────────────

    let activeMetrica = 'credits';

    function applyAll(){{
      const data = getFiltered();
      const hasFilter = [...filters].some(f=>f.value);
      const countEl = document.getElementById('t5-count');
      countEl.textContent = hasFilter ? 'Mostrando '+data.length+' de '+RAW.length+' registros' : data.length+' registros';
      rebuildNPS(data);
      rebuildQuejas(data);
      rebuildMetrica(data, activeMetrica);
    }}

    filters.forEach(f => f.addEventListener('change', applyAll));
    clearBtn.addEventListener('click', ()=>{{ filters.forEach(f=>{{f.value=''}}); applyAll(); }});

    document.querySelectorAll('.t5-metrica-btn').forEach(btn=>{{
      btn.addEventListener('click', ()=>{{
        activeMetrica = btn.dataset.metrica;
        document.querySelectorAll('.t5-metrica-block').forEach(b=>{{b.style.display='none';}});
        document.getElementById('t5-metrica-'+activeMetrica).style.display='block';
        document.querySelectorAll('.t5-metrica-btn').forEach(b=>{{
          b.style.background = b.dataset.metrica===activeMetrica ? '#FFE600' : '#f5f5f5';
          b.style.fontWeight = b.dataset.metrica===activeMetrica ? '700' : '400';
        }});
        rebuildMetrica(getFiltered(), activeMetrica);
      }});
    }});

    applyAll();
  }})();
  </script>
"""

    return html
