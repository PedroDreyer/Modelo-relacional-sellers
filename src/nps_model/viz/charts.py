"""
Módulo para generar gráficos y visualizaciones del modelo NPS.

Este módulo contiene funciones para crear gráficos en formato base64
que se embeben directamente en el HTML del reporte.
"""

import io
import base64
import logging
from typing import Optional
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from ..utils.dates import convertir_mes_a_texto

logger = logging.getLogger(__name__)


def generar_grafico_evolucion_nps(
    df_nps: pd.DataFrame,
    mes_actual: str,
    mes_inicio: str,
    output_format: str = "base64",
) -> str:
    """
    Genera gráfico de línea con la evolución del NPS por site.
    
    Args:
        df_nps: DataFrame con columnas ['END_DATE_MONTH', 'SITE', 'NPS_score']
        mes_actual: Mes actual en formato YYYYMM
        mes_inicio: Mes de inicio para el gráfico (usualmente 12 meses atrás)
        output_format: Formato de salida ("base64" para HTML, "file" para guardar)
    
    Returns:
        String con la imagen en base64 o path al archivo si output_format="file"
    """
    try:
        # Filtrar datos para el rango de meses (desde mes_inicio hasta mes_actual)
        nps_grafico = df_nps[
            (df_nps['END_DATE_MONTH'] >= mes_inicio) & 
            (df_nps['END_DATE_MONTH'] <= mes_actual)
        ].copy()
        
        if len(nps_grafico) == 0:
            return ""
        
        # Pivotar para tener un site por columna
        pivot_nps = nps_grafico.pivot(
            index='END_DATE_MONTH', 
            columns='SITE', 
            values='NPS_score'
        )
        
        # Configuración de fuente
        plt.rcParams['font.family'] = 'sans-serif'
        plt.rcParams['font.sans-serif'] = ['Arial', 'Helvetica', 'DejaVu Sans']
        plt.rcParams['font.size'] = 12
        
        # Crear figura
        fig, ax = plt.subplots(figsize=(14, 4))
        fig.patch.set_facecolor('white')
        ax.set_facecolor('white')
        
        # Preparar datos
        ticks = list(pivot_nps.index)
        x = list(range(len(ticks)))
        
        # Colores por SITE (basados en colores de banderas)
        colores_site = {
            'MLB': '#00a650',   # Brasil - Verde
            'MLM': '#dc3545',   # México - Rojo
            'MLA': '#00bfff',   # Argentina - Celeste
            'MLC': '#dc3545',   # Chile - Rojo
            'MPE': '#dc3545',   # Perú - Rojo
            'MLU': '#3483fa',   # Uruguay - Azul
            'MCO': '#ffe600',   # Colombia - Amarillo
        }
        
        # Graficar cada SITE
        for site_col in pivot_nps.columns:
            y = pivot_nps[site_col].values
            color = colores_site.get(site_col, '#333333')
            
            # Línea con marcadores
            ax.plot(x, y, marker='o', label=site_col, color=color, 
                   linewidth=2, markersize=6)
            
            # Anotar valores en cada punto (redondeados sin decimales)
            for xi, yi in zip(x, y):
                if not np.isnan(yi):
                    ax.text(
                        xi, yi + 1.5, 
                        f"{round(yi)}", 
                        ha='center', 
                        va='bottom', 
                        fontsize=12, 
                        color='black'
                    )
        
        # ==========================================
        # CONFIGURACIÓN DEL GRÁFICO
        # ==========================================
        
        # Escala Y fija de 50 a 100
        ax.set_ylim(50, 100)
        
        # Eje X: etiquetas con formato MM/YYYY
        ax.set_xticks(x)
        ax.set_xticklabels(
            [m[4:] + "/" + m[:4] for m in ticks], 
            rotation=0, 
            ha='center', 
            fontsize=12
        )
        
        # Sin título dentro del gráfico (se agrega en HTML)
        
        # Ocultar eje Y y spines innecesarios
        ax.yaxis.set_visible(False)
        ax.grid(False)
        ax.spines['top'].set_visible(False)
        ax.spines['right'].set_visible(False)
        ax.spines['left'].set_visible(False)
        ax.spines['bottom'].set_visible(True)
        
        # Leyenda discreta
        ax.legend(loc='upper right', frameon=False)
        
        plt.tight_layout()
        
        # ==========================================
        # EXPORTAR
        # ==========================================
        
        if output_format == "base64":
            # Convertir a base64 para embeber en HTML
            buffer = io.BytesIO()
            plt.savefig(buffer, format='png', dpi=100, bbox_inches='tight')
            buffer.seek(0)
            image_base64 = base64.b64encode(buffer.read()).decode()
            plt.close(fig)
            return f"data:image/png;base64,{image_base64}"
        else:
            # Guardar como archivo
            filename = f"evolucion_nps_{mes_actual}.png"
            plt.savefig(filename, format='png', dpi=100, bbox_inches='tight')
            plt.close(fig)
            return filename
            
    except Exception as e:
        print(f"⚠️ Error generando gráfico de evolución NPS: {e}")
        import traceback
        traceback.print_exc()
        return ""


def generar_grafico_quejas(
    impacto_df: pd.DataFrame,
    mes_inicio: str,
    mes_final: str,
    output_format: str = "base64",
) -> str:
    """
    Genera gráfico de barras apiladas con evolución de quejas por motivo.
    
    Replica gráfico del notebook líneas 3519-3650.
    
    Args:
        impacto_df: DataFrame con impacto de quejas (meses × motivos)
                   Índice: meses (YYYYMM), Columnas: motivos, Valores: %
        mes_inicio: Mes de inicio para filtrar (formato YYYYMM)
        mes_final: Mes final para filtrar (formato YYYYMM)
        output_format: Formato de salida ("base64" para HTML, "file" para guardar)
    
    Returns:
        String con la imagen en base64 o path al archivo si output_format="file"
    
    Notes:
        - Barras apiladas verticales (100% stacked bar)
        - Colores predefinidos por motivo (del notebook)
        - Etiquetas dentro de barras si ≥ 2.0%
        - Total apilado encima de cada barra
    """
    try:
        if impacto_df.empty:
            logger.warning("DataFrame de impacto vacío")
            return ""
        
        # Filtrar por rango de meses (desde mes_inicio hasta mes_final)
        impacto_grafico = impacto_df[
            (impacto_df.index >= mes_inicio) & 
            (impacto_df.index <= mes_final)
        ].copy()
        
        # Función para ordenar motivos (principales por promedio, "otros" al final)
        def ordenar_motivos_quejas(df):
            """Ordena motivos por impacto promedio, 'Otros' y 'Sin información' al final"""
            promedios = df.mean().sort_values(ascending=False)
            cols_sin_otros = [col for col in promedios.index 
                            if 'Otros' not in col and 'Sin información' not in col]
            cols_otros = [col for col in promedios.index 
                         if 'Otros' in col or 'Sin información' in col]
            return cols_sin_otros + cols_otros
        
        columnas_ordenadas = ordenar_motivos_quejas(impacto_grafico)
        impacto_grafico = impacto_grafico[columnas_ordenadas]
        
        # Colores predefinidos por motivo (del notebook línea 3738)
        colores_motivos = {
            'Envio': 'green',
            'Calidad': 'purple',
            'Confianza': 'orange',
            'Precio': '#FFD700',
            'Costo de envío': '#FFFF99',
            'Pago': 'blue',
            'Sin información': 'lightgray',
            'Otros motivos': 'darkgray',
            'Otros sin clasificar': '#FFB6C1'
        }
        colores = [colores_motivos.get(c, '#999999') for c in impacto_grafico.columns]
        
        # Definir colores de fondo que necesitan texto blanco (colores oscuros)
        colores_oscuros = {'green', 'purple', 'orange', 'blue', 'darkgray'}
        
        # Configuración de fuente
        plt.rcParams['font.family'] = 'sans-serif'
        plt.rcParams['font.sans-serif'] = ['Arial', 'Helvetica', 'DejaVu Sans']
        plt.rcParams['font.size'] = 12
        
        # Crear figura
        fig, ax = plt.subplots(figsize=(14, 5))
        fig.patch.set_facecolor('white')
        ax.set_facecolor('white')
        
        # Preparar datos
        meses_graf = impacto_grafico.index
        x_pos = np.arange(len(meses_graf))
        bottom = np.zeros(len(meses_graf))
        
        # Crear barras apiladas
        for idx, motivo in enumerate(impacto_grafico.columns):
            valores = impacto_grafico[motivo].values
            bars = ax.bar(x_pos, valores, bottom=bottom, color=colores[idx], 
                         edgecolor='white', linewidth=0.5, label=motivo)
            
            # Agregar números DENTRO de las barras para valores >= 2.0
            color_fondo = colores[idx]
            for i, (bar, valor) in enumerate(zip(bars, valores)):
                if valor >= 2.0:
                    height = bar.get_height()
                    y_pos = bottom[i] + height / 2
                    # Color del texto basado en el color de fondo
                    color_texto = 'white' if color_fondo in colores_oscuros else 'black'
                    ax.text(bar.get_x() + bar.get_width() / 2, y_pos, 
                           f'{valor:.1f}%',
                           ha='center', va='center', fontsize=12, 
                           color=color_texto)
            
            bottom += valores
        
        # Totales arriba de cada barra
        for i, total in enumerate(bottom):
            ax.text(i, total + 0.5, f'{total:.1f}%', 
                   ha='center', va='bottom', fontsize=12, fontweight='bold')
        
        # Configuración del eje X
        ax.set_xticks(x_pos)
        ax.set_xticklabels([m[4:] + "/" + m[:4] for m in meses_graf], 
                          rotation=0, ha='center', fontsize=12)
        
        # Sin título dentro del gráfico (se agrega en HTML)
        
        # Ocultar eje Y y spines innecesarios
        ax.yaxis.set_visible(False)
        for spine in ['top', 'right', 'left']:
            ax.spines[spine].set_visible(False)
        
        # Leyenda centrada abajo, horizontal, sin marco
        ax.legend(loc='upper center', bbox_to_anchor=(0.5, -0.08), 
                 ncol=len(impacto_grafico.columns), frameon=False, fontsize=12)
        
        plt.tight_layout()
        
        # ==========================================
        # EXPORTAR
        # ==========================================
        
        if output_format == "base64":
            # Convertir a base64 para embeber en HTML
            buffer = io.BytesIO()
            plt.savefig(buffer, format='png', dpi=150, bbox_inches='tight',
                       facecolor='white', edgecolor='none')
            buffer.seek(0)
            image_base64 = base64.b64encode(buffer.read()).decode()
            plt.close(fig)
            return f"data:image/png;base64,{image_base64}"
        else:
            # Guardar como archivo
            filename = f"evolucion_quejas_{mes_final}.png"
            plt.savefig(filename, format='png', dpi=150, bbox_inches='tight',
                       facecolor='white', edgecolor='none')
            plt.close(fig)
            return filename
            
    except Exception as e:
        logger.error(f"Error generando gráfico de quejas: {e}")
        import traceback
        traceback.print_exc()
        return ""
