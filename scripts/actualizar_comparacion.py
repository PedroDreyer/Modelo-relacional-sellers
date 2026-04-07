"""Actualiza el HTML de comparacion con patrones de falla, next steps y resaltado."""
import json, sys, html as html_mod, time, re
from pathlib import Path

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root / "scripts"))

with open(project_root / "comparaciones/results_from_htmls.json", "r", encoding="utf-8") as f:
    results = json.load(f)

def esc(t):
    return html_mod.escape(str(t)) if t else ""

def highlight_matches(modelo_text, analista_text):
    """Highlight parts of modelo that match analista keywords."""
    if not modelo_text or not analista_text:
        return esc(modelo_text)

    # Extract key terms from analista
    keywords = []
    # Look for specific patterns
    patterns = [
        r'TC\b', r'tarjeta de cr[eé]dito', r'cr[eé]dito', r'comision', r'tasa',
        r'cuota', r'pricing', r'escala', r'AWS', r'PdF', r'funcionamiento',
        r'IIBB', r'repricing', r'Top Off', r'newbie', r'plazo', r'inversi',
        r'default', r'mora', r'rechaz', r'aprobaci', r'atenci[oó]n',
    ]

    analista_lower = analista_text.lower()
    relevant_patterns = []
    for p in patterns:
        if re.search(p.lower(), analista_lower):
            relevant_patterns.append(p)

    if not relevant_patterns:
        return esc(modelo_text)

    # Escape first, then highlight
    escaped = esc(modelo_text)
    for p in relevant_patterns:
        escaped = re.sub(
            f'({p})',
            r'<mark style="background:#FFEB9C;padding:1px 2px;border-radius:2px;">\1</mark>',
            escaped,
            flags=re.IGNORECASE
        )
    return escaped


# Failure patterns
failure_patterns_desc = {
    "AWS/Infraestructura": "El modelo no detecta eventos de infraestructura (caida AWS) como driver.",
    "TC/Campanas": "No prioriza TC cuando tiene alto impacto. Falta cross TC x Segmento.",
    "IIBB/Regulatorio": "Contexto regulatorio provincial no detectable. Falta dato de region.",
    "Repricing": "Tiene pricing actual pero no delta QvsQ. No detecta 'recupero post repricing'.",
    "Comisiones como default": "Cae en Comisiones como explicacion generica. Falta priorizacion.",
    "Priorizacion drivers": "Driver secundario con mas impacto que el principal no se promueve.",
}

fixes = {
    "AWS/Infraestructura": "Flag de incidentes + tendencia PdF QvsQ como driver de NPS",
    "TC/Campanas": "Drill-down TC x Segmento + promover driver cuando aporte_pp es mayor",
    "IIBB/Regulatorio": "REGION como enrichment + tabla de eventos regulatorios por quarter",
    "Repricing": "Delta pricing QvsQ + cross POINT_FLAG en analisis OP/LINK",
    "Comisiones como default": "Ajustar umbrales: si Comisiones no tiene sub-grupo con >5pp, buscar otro",
    "Priorizacion drivers": "Si driver secundario tiene mayor aporte_pp, promoverlo a principal",
}

# Assign patterns
pattern_map = [
    ["Priorizacion drivers"],
    ["TC/Campanas", "Priorizacion drivers"],
    [],
    ["AWS/Infraestructura"],
    ["TC/Campanas"],
    ["AWS/Infraestructura", "IIBB/Regulatorio"],
    ["Repricing"],
    ["TC/Campanas"],
    ["IIBB/Regulatorio"],
    ["Repricing"],
    ["Comisiones como default"],
    ["IIBB/Regulatorio"],
]
for i, pats in enumerate(pattern_map):
    results[i]["failure_patterns"] = pats

pattern_counts = {}
for r in results:
    for p in r.get("failure_patterns", []):
        pattern_counts[p] = pattern_counts.get(p, 0) + 1

total = len(results)
si = sum(1 for r in results if r.get("convalida") == "Si")
dir_ = sum(1 for r in results if r.get("convalida") == "Direccional")
no = sum(1 for r in results if r.get("convalida") == "No")

html = f"""<!DOCTYPE html>
<html><head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Comparacion Modelo vs Analista - NPS Relacional Sellers</title>
<style>
  * {{ margin:0; padding:0; box-sizing:border-box; }}
  body {{ font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif; background:#f5f5f5; padding:24px; }}
  .container {{ max-width:1600px; margin:0 auto; }}
  h1 {{ font-size:22px; margin-bottom:4px; }}
  h2 {{ font-size:18px; margin:28px 0 12px; color:#333; }}
  .subtitle {{ color:#888; font-size:13px; margin-bottom:16px; }}
  .summary {{ display:flex; gap:12px; margin:16px 0 24px; flex-wrap:wrap; }}
  .summary-card {{ padding:16px 28px; border-radius:10px; font-size:16px; font-weight:700; }}
  .card-si {{ background:#C6EFCE; color:#388E3C; }}
  .card-dir {{ background:#FFEB9C; color:#E65100; }}
  .card-no {{ background:#FFC7CE; color:#C62828; }}
  table {{ width:100%; border-collapse:collapse; background:white; border-radius:10px; overflow:hidden; box-shadow:0 1px 4px rgba(0,0,0,.1); margin-bottom:24px; }}
  th {{ background:#FFE600; padding:12px 14px; font-size:11px; text-align:left; border-bottom:2px solid #ddd; text-transform:uppercase; }}
  td {{ padding:14px 14px; font-size:12px; border-bottom:1px solid #eee; vertical-align:top; line-height:1.6; }}
  tr:hover {{ background:#fafafa; }}
  .badge-si {{ background:#C6EFCE; color:#388E3C; padding:4px 12px; border-radius:4px; font-weight:700; font-size:12px; }}
  .badge-dir {{ background:#FFEB9C; color:#E65100; padding:4px 12px; border-radius:4px; font-weight:700; font-size:12px; }}
  .badge-no {{ background:#FFC7CE; color:#C62828; padding:4px 12px; border-radius:4px; font-weight:700; font-size:12px; }}
  .pattern-box {{ background:white; border-radius:10px; padding:16px 20px; box-shadow:0 1px 4px rgba(0,0,0,.1); margin-bottom:12px; border-left:4px solid #C62828; }}
  .pattern-title {{ font-weight:700; font-size:14px; }}
  .pattern-count {{ background:#FFC7CE; color:#C62828; padding:2px 8px; border-radius:4px; font-size:11px; font-weight:600; margin-left:8px; }}
  .pattern-desc {{ font-size:12px; color:#555; line-height:1.6; margin-top:6px; }}
  .pattern-fix {{ font-size:12px; color:#1565C0; margin-top:8px; font-weight:600; padding:8px 12px; background:#E3F2FD; border-radius:6px; }}
  .next-step {{ font-size:11px; color:#1565C0; background:#E3F2FD; padding:6px 10px; border-radius:6px; margin-top:8px; line-height:1.5; }}
  .comment {{ font-size:11px; color:#666; margin-top:6px; line-height:1.4; }}
  mark {{ background:#FFEB9C; padding:1px 3px; border-radius:2px; }}
</style>
</head><body>
<div class="container">
  <h1>Comparacion Modelo vs Analista</h1>
  <p class="subtitle">NPS Relacional Sellers | 26Q1 vs 25Q4 | 12 corridas | {time.strftime("%Y-%m-%d")}</p>

  <div class="summary">
    <div class="summary-card card-si">Si: {si}/{total} ({si/total*100:.0f}%)</div>
    <div class="summary-card card-dir">Direccional: {dir_}/{total} ({dir_/total*100:.0f}%)</div>
    <div class="summary-card card-no">No: {no}/{total} ({no/total*100:.0f}%)</div>
  </div>

  <h2>Patrones de falla y proximos pasos</h2>
"""

for pattern, count in sorted(pattern_counts.items(), key=lambda x: -x[1]):
    html += f"""
  <div class="pattern-box">
    <div style="display:flex;align-items:center;">
      <span class="pattern-title">{esc(pattern)}</span>
      <span class="pattern-count">{count} corridas</span>
    </div>
    <div class="pattern-desc">{esc(failure_patterns_desc.get(pattern, ''))}</div>
    <div class="pattern-fix">Fix: {esc(fixes.get(pattern, ''))}</div>
  </div>
"""

html += """
  <h2>Detalle por corrida</h2>
  <table>
    <thead>
      <tr>
        <th style="width:5%;">Prod</th>
        <th style="width:3%;">Site</th>
        <th style="width:23%;">Explicacion Analista</th>
        <th style="width:27%;">Explicacion Modelo</th>
        <th style="width:7%;">Resultado</th>
        <th style="width:12%;">Por que</th>
        <th style="width:13%;">Proximo paso</th>
        <th style="width:3%;">HTML</th>
      </tr>
    </thead>
    <tbody>
"""

for r in results:
    convalida = r.get("convalida", "")
    badge_cls = {"Si": "badge-si", "Direccional": "badge-dir", "No": "badge-no"}.get(convalida, "")

    analista = r.get("explicacion_analista", "") or ""
    modelo = r.get("explicacion_modelo", "") or ""
    comment = r.get("comment", "") or ""
    next_step = r.get("next_step", "") or ""
    patterns = ", ".join(r.get("failure_patterns", []))
    html_path = r.get("html_path", "")

    # Highlight matching terms in modelo
    modelo_highlighted = highlight_matches(modelo, analista)

    html += f"""      <tr>
        <td><b>{esc(r['update_tipo'])}</b></td>
        <td><b>{esc(r['site'])}</b></td>
        <td>{esc(analista)}</td>
        <td>{modelo_highlighted}</td>
        <td><span class="{badge_cls}">{convalida}</span></td>
        <td><div class="comment">{esc(comment)}</div></td>
        <td><div class="next-step">{esc(next_step)}</div></td>
        <td style="text-align:center;">{f'<a href="../{html_path}" target="_blank" style="text-decoration:none;font-size:16px;" title="Abrir reporte completo">📊</a>' if html_path else '-'}</td>
      </tr>
"""

html += """    </tbody>
  </table>
</div>
</body></html>"""

with open(project_root / "comparaciones/comparacion_visual.html", "w", encoding="utf-8") as f:
    f.write(html)
print("HTML actualizado con next steps, highlight y celdas grandes")

from generar_comparacion import generate_excel
generate_excel(results, project_root / "comparaciones/comparacion_modelo_vs_analista.xlsx")
