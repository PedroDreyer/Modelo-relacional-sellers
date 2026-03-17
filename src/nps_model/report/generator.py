"""
Generador de resumen ejecutivo en HTML
"""

import json
import logging
from pathlib import Path
from typing import Any, Optional

from jinja2 import Environment, FileSystemLoader, select_autoescape

from nps_model.utils.dates import convertir_mes_a_texto

logger = logging.getLogger(__name__)


class ReportGenerator:
    """
    Generador de resumen ejecutivo en HTML y JSON
    """

    def __init__(self, output_dir: str = "outputs"):
        """
        Args:
            output_dir: Directorio donde se guardarán los reportes
        """
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

        # Configurar Jinja2 para templates HTML
        template_dir = Path(__file__).parent / "templates"
        self.env = Environment(
            loader=FileSystemLoader(template_dir),
            autoescape=select_autoescape(["html", "xml"]),
        )

        # Registrar filtros personalizados
        self.env.filters["mes_texto"] = convertir_mes_a_texto
        self.env.filters["format_number"] = self._format_number
        self.env.filters["format_percent"] = self._format_percent
        self.env.filters["format_variation"] = self._format_variation

    @staticmethod
    def _format_number(value: Optional[float], decimals: int = 1) -> str:
        """Formatea un número con decimales"""
        if value is None:
            return "N/A"
        return f"{value:.{decimals}f}"

    @staticmethod
    def _format_percent(value: Optional[float], decimals: int = 1) -> str:
        """Formatea un porcentaje"""
        if value is None:
            return "N/A"
        return f"{value:.{decimals}f}%"

    @staticmethod
    def _format_variation(value: Optional[float], decimals: int = 1) -> str:
        """Formatea una variación con signo"""
        if value is None:
            return "N/A"
        sign = "+" if value > 0 else ""
        return f"{sign}{value:.{decimals}f}"

    def generate_json(
        self,
        data: dict[str, Any],
        site: str,
        fecha_final: str,
        file_prefix: str = "executive_summary",
    ) -> Path:
        """
        Genera archivo JSON con la estructura del resumen ejecutivo.
        
        Este JSON sirve para:
        - Testing (comparar contenidos)
        - Debugging
        - Como fuente para otros formatos (PDF, etc.)
        
        Args:
            data: Diccionario con todos los datos del resumen
            site: Código del site (ej: "MPE")
            fecha_final: Mes final en formato YYYYMM
            file_prefix: Prefijo del nombre del archivo
        
        Returns:
            Path del archivo JSON generado
        """
        filename = f"{file_prefix}_{site}_{fecha_final}.json"
        filepath = self.output_dir / filename

        logger.info(f"Generando JSON: {filepath}")

        # Guardar con formato legible
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2, default=str)

        logger.info(f"✅ JSON generado: {filepath}")

        return filepath

    def generate_html(
        self,
        data: dict[str, Any],
        site: str,
        fecha_final: str,
        file_prefix: str = "executive_summary",
    ) -> Path:
        """
        Genera el resumen ejecutivo en HTML.
        
        Args:
            data: Diccionario con todos los datos del resumen
            site: Código del site (ej: "MPE")
            fecha_final: Mes final en formato YYYYMM
            file_prefix: Prefijo del nombre del archivo
        
        Returns:
            Path del archivo HTML generado
        """
        filename = f"{file_prefix}_{site}_{fecha_final}.html"
        filepath = self.output_dir / filename

        logger.info(f"Generando HTML: {filepath}")

        # Cargar template
        template = self.env.get_template("executive_summary.html")

        # Renderizar
        html_content = template.render(
            site=site,
            fecha_final=fecha_final,
            fecha_texto=convertir_mes_a_texto(fecha_final, formato="largo"),
            **data,
        )

        # Guardar
        with open(filepath, "w", encoding="utf-8") as f:
            f.write(html_content)

        logger.info(f"✅ HTML generado: {filepath}")

        return filepath

    def generate_all(
        self,
        data: dict[str, Any],
        site: str,
        fecha_final: str,
        file_prefix: str = "executive_summary",
        generate_pdf: bool = False,
    ) -> dict[str, Path]:
        """
        Genera todos los formatos de reporte.
        
        Args:
            data: Diccionario con datos del resumen
            site: Código del site
            fecha_final: Mes final en formato YYYYMM
            file_prefix: Prefijo de nombres de archivo
            generate_pdf: Si generar también PDF
        
        Returns:
            Diccionario con paths de los archivos generados
        """
        logger.info(f"Generando reportes para {site} - {fecha_final}")

        outputs = {}

        # JSON
        outputs["json"] = self.generate_json(data, site, fecha_final, file_prefix)

        # HTML
        outputs["html"] = self.generate_html(data, site, fecha_final, file_prefix)

        # PDF (opcional)
        if generate_pdf:
            try:
                outputs["pdf"] = self.generate_pdf(
                    outputs["html"], site, fecha_final, file_prefix
                )
            except Exception as e:
                logger.warning(f"⚠️ No se pudo generar PDF: {e}")

        logger.info(f"✅ Reportes generados: {', '.join(outputs.keys())}")

        return outputs

    def generate_pdf(
        self,
        html_path: Path,
        site: str,
        fecha_final: str,
        file_prefix: str = "executive_summary",
    ) -> Path:
        """
        Genera PDF desde el HTML (requiere weasyprint).
        
        Args:
            html_path: Path del HTML fuente
            site: Código del site
            fecha_final: Mes final
            file_prefix: Prefijo del archivo
        
        Returns:
            Path del PDF generado
        """
        try:
            from weasyprint import HTML
        except ImportError:
            raise ImportError(
                "weasyprint no está instalado. "
                "Instala con: pip install nps-tx-model[pdf]"
            )

        filename = f"{file_prefix}_{site}_{fecha_final}.pdf"
        filepath = self.output_dir / filename

        logger.info(f"Generando PDF: {filepath}")

        # Convertir HTML a PDF
        HTML(filename=str(html_path)).write_pdf(str(filepath))

        logger.info(f"✅ PDF generado: {filepath}")

        return filepath
