"""
Loaders para cargar datos de NPS Relacional Sellers desde BigQuery

Incluye:
- DataLoader: carga encuestas NPS sellers
- EnrichmentLoader: carga fuentes de enriquecimiento (Credits, Transacciones, Inversiones, Segmentacion)
"""

import logging
import time
from pathlib import Path
from typing import Optional

import pandas as pd

from nps_model.io.bigquery_client import BigQueryClient
from nps_model.utils.cache import DataCache
from nps_model.utils.dates import (
    calcular_meses_atras,
    quarters_to_month_range,
    quarters_to_date_range,
    quarter_fecha_final,
)

logger = logging.getLogger(__name__)


class DataLoader:
    """
    Carga de datos desde BigQuery usando queries SQL versionadas con soporte de caché.
    Adaptado para NPS Relacional Sellers (solo encuestas, sin datos reales).
    """

    def __init__(self, bq_client: BigQueryClient, use_cache: bool = True, cache_dir: str = ".cache"):
        """
        Args:
            bq_client: Cliente de BigQuery ya inicializado
            use_cache: Si True, usa caché para evitar recargas de BigQuery
            cache_dir: Directorio para archivos de caché
        """
        self.client = bq_client
        self.sql_dir = Path(__file__).parent.parent / "sql"
        self.use_cache = use_cache
        self.cache = DataCache(cache_dir) if use_cache else None

    def _load_sql_template(self, sql_file: str) -> str:
        """
        Carga un template SQL desde el directorio sql/
        
        Args:
            sql_file: Nombre del archivo SQL (ej: "main_query.sql")
        
        Returns:
            Contenido del archivo SQL
        """
        sql_path = self.sql_dir / sql_file

        if not sql_path.exists():
            raise FileNotFoundError(f"SQL file no encontrado: {sql_path}")

        with open(sql_path, "r", encoding="utf-8") as f:
            return f.read()

    # Mapeo update_tipo -> filtro E_CODE a nivel SQL
    E_CODE_FILTERS = {
        "SMBs": "AND NPS_TX_E_CODE LIKE '%SMB%'",
        "Point": "AND NPS_TX_E_CODE LIKE '%POINT%'",
        "OP": "AND (NPS_TX_E_CODE LIKE '%APICOW%' OR NPS_TX_E_CODE LIKE '%LINK%')",
        "LINK": "AND NPS_TX_E_CODE LIKE '%LINK%'",
        "APICOW": "AND NPS_TX_E_CODE LIKE '%APICOW%'",
        "all": "",
    }

    def load_nps_data(
        self,
        sites: list[str],
        fecha_final: str,
        fecha_minima: Optional[str] = None,
        quarter_anterior: Optional[str] = None,
        quarter_actual: Optional[str] = None,
        producto: Optional[str] = None,
        update_tipo: Optional[str] = None,
    ) -> pd.DataFrame:
        """
        Carga los datos de NPS Relacional Sellers desde BigQuery.

        Si se proporcionan quarter_anterior y quarter_actual, el rango de fechas
        cubre exactamente ambos quarters. Si no, usa fecha_final con 13 meses atrás.

        Args:
            update_tipo: Tipo de update ('SMBs', 'Point', 'OP', 'all') para filtrar
                         E_CODE a nivel SQL.
        """
        print(f"   📊 Cargando datos NPS Sellers para: {', '.join(sites)}")

        # OP (LINK/APICOW) usa query especial con restricciones integradas
        if update_tipo in ("LINK", "APICOW", "OP"):
            sql_file = "main_query_op.sql"
            print(f"   📋 Usando query OP (con restricciones y CONSIDERACION_AJUSTADA)")
        else:
            sql_file = "main_query.sql"
        query_template = self._load_sql_template(sql_file)

        if quarter_anterior and quarter_actual:
            from nps_model.utils.dates import quarter_label, parse_quarter
            # Extender rango para cubrir 5 quarters (charts de evolución)
            _y, _qn = parse_quarter(quarter_actual)
            for _ in range(4):  # retroceder 4 quarters desde el actual
                _qn -= 1
                if _qn == 0:
                    _qn = 4
                    _y -= 1
            _q_inicio = f"{_y % 100}Q{_qn}"
            fecha_minima_calc, fecha_maxima_calc = quarters_to_date_range(_q_inicio, quarter_actual)
            print(f"   📅 Período: {quarter_label(quarter_anterior)} vs {quarter_label(quarter_actual)} (con 5Q de historia)")
        else:
            mes_inicio = calcular_meses_atras(fecha_final, 13)
            fecha_minima_calc = (
                f"{mes_inicio[:4]}-{mes_inicio[4:]}-01"
                if not fecha_minima
                else fecha_minima
            )
            año_final = int(fecha_final[:4])
            mes_final = int(fecha_final[4:])
            if mes_final == 12:
                fecha_maxima_calc = f"{año_final + 1}-01-01"
            else:
                fecha_maxima_calc = f"{año_final}-{mes_final + 1:02d}-01"
            print(f"   📅 Período: {fecha_final} (últimos 13 meses)")

        # Filtro E_CODE según update_tipo
        e_code_filter = self.E_CODE_FILTERS.get(update_tipo or "all", "")
        if e_code_filter:
            print(f"   🏷️  Filtro SQL E_CODE: {update_tipo}")

        print(f"   📆 Desde: {fecha_minima_calc} hasta: {fecha_maxima_calc}")

        cache_params = {
            "sites": sorted(sites),
            "fecha_final": fecha_final,
            "fecha_minima": fecha_minima_calc,
            "fecha_maxima": fecha_maxima_calc,
            "update_tipo": update_tipo or "all",
        }
        
        if self.use_cache and self.cache:
            df = self.cache.get(cache_params, data_type="nps_sellers")
            if df is not None:
                return df

        # Si no hay caché, ejecutar query
        # Formatear sites como tupla SQL: ('MLA', 'MLB')
        sites_sql = "('" + "', '".join(sites) + "')"

        query = query_template.format(
            sites=sites_sql,
            fecha_minima=fecha_minima_calc,
            fecha_maxima=fecha_maxima_calc,
            e_code_filter=e_code_filter,
        )

        # Ejecutar query
        print("   🔄 Ejecutando query en BigQuery...")
        print("      (Esto puede tardar 1-3 minutos según el volumen de datos)")
        
        import time
        start_time = time.time()
        
        df = self.client.query_to_dataframe(query)
        
        elapsed = time.time() - start_time
        print(f"   ⏱️  Query completada en {elapsed:.1f} segundos")
        
        # Guardar en caché
        if self.use_cache and self.cache:
            self.cache.set(cache_params, df, data_type="nps_sellers")

        if df.empty:
            print("   ⚠️ Query retornó 0 filas. Verifica parámetros y datos disponibles.")
        else:
            print(
                f"   ✅ Datos cargados: {len(df):,} filas, "
                f"{df['END_DATE_MONTH'].nunique()} meses, "
                f"{df['SITE'].nunique()} sites"
            )

        return df


class EnrichmentLoader:
    """
    Carga fuentes de enriquecimiento desde BigQuery para cruzar con datos NPS.
    
    Fuentes:
    - Credits: uso de crédito, oferta FRED, tarjeta de crédito MP
    - Transacciones: TPV/TPN por producto, rangos de volumen
    - Inversiones: uso de POTS, inversiones activas
    - Segmentacion: producto principal, NEW_MAS_FLAG, segmento, PF/PJ
    - Top Off: atención al cliente (FLAG_TOPOFF)
    """

    # E_CODE filters for NPS table queries
    E_CODE_FILTERS = {
        "SMBs": "AND NPS_TX_E_CODE LIKE '%SMB%'",
        "Point": "AND NPS_TX_E_CODE LIKE '%POINT%'",
        "OP": "AND (NPS_TX_E_CODE LIKE '%APICOW%' OR NPS_TX_E_CODE LIKE '%LINK%')",
        "LINK": "AND NPS_TX_E_CODE LIKE '%LINK%'",
        "APICOW": "AND NPS_TX_E_CODE LIKE '%APICOW%'",
        "all": "",
    }

    # Product filters for SEGMENTATION_SELLERS universo queries
    PRODUCT_FILTERS_SEG = {
        "Point": "AND s.POINT_FLAG = 1",
        "SMBs": "AND (s.POINT_FLAG = 1 OR s.QR_FLAG = 1 OR s.LINK_FLAG = 1 OR s.API_FLAG = 1)",
        "OP": "AND s.OP_FLAG = 1",
        "LINK": "AND s.LINK_FLAG = 1",
        "APICOW": "AND s.API_FLAG = 1",
        "all": "",
    }

    def __init__(
        self,
        bq_client: BigQueryClient,
        use_cache: bool = True,
        cache_dir: str = ".cache",
        use_dataflow_tables: bool = False,
        dataset_dataflow: str = "SBOX_NPS_ANALYTICS",
        quarter_anterior: Optional[str] = None,
        quarter_actual: Optional[str] = None,
        update_tipo: Optional[str] = None,
    ):
        self.client = bq_client
        self.sql_dir = Path(__file__).parent.parent / "sql"
        self.use_cache = use_cache
        self.cache = DataCache(cache_dir) if use_cache else None
        self.use_dataflow_tables = use_dataflow_tables
        self.dataset_dataflow = dataset_dataflow
        self.quarter_anterior = quarter_anterior
        self.quarter_actual = quarter_actual
        self.update_tipo = update_tipo or "all"

    def _calc_month_range(self, fecha_final: str) -> tuple[int, int]:
        """Devuelve (mes_min, mes_max) en YYYYMM (int). Usa quarters si están configurados."""
        if self.quarter_anterior and self.quarter_actual:
            return quarters_to_month_range(self.quarter_anterior, self.quarter_actual)
        mes_min_str = calcular_meses_atras(fecha_final, 12)
        mes_max_str = fecha_final
        return int(mes_min_str), int(mes_max_str)

    def _load_from_dataflow_table(
        self, table_name: str, where_clause: str, cache_type: str, cache_params: Optional[dict] = None
    ) -> pd.DataFrame:
        """Lee una tabla de enriquecimiento escrita por el job Dataflow."""
        project = getattr(self.client, "project_id", None) or "meli-bi-data"
        full_table = f"`{project}.{self.dataset_dataflow}.{table_name}`"
        query = f"SELECT * FROM {full_table} WHERE {where_clause}"
        params = cache_params or {"table": table_name, "where": where_clause}

        if self.use_cache and self.cache:
            df = self.cache.get(params, data_type=cache_type)
            if df is not None:
                print(f"   ✅ {cache_type}: desde caché ({len(df):,} filas)")
                return df

        print(f"   🔄 Leyendo {self.dataset_dataflow}.{table_name} en BigQuery...")
        start_time = time.time()
        try:
            df = self.client.query_to_dataframe(query)
        except Exception as e:
            err_str = str(e)
            if "Not found" in err_str or "404" in err_str:
                print(f"   ⚠️  Falta tabla Dataflow: {self.dataset_dataflow}.{table_name}")
                return pd.DataFrame()
            raise
        elapsed = time.time() - start_time
        print(f"   ⏱️  {cache_type} completado en {elapsed:.1f}s ({len(df):,} filas)")

        if self.use_cache and self.cache:
            self.cache.set(params, df, data_type=cache_type)
        return df

    def _load_sql_template(self, sql_file: str) -> str:
        sql_path = self.sql_dir / sql_file
        if not sql_path.exists():
            raise FileNotFoundError(f"SQL file no encontrado: {sql_path}")
        with open(sql_path, "r", encoding="utf-8") as f:
            return f.read()

    def _query_with_cache(
        self, sql_file: str, params: dict, cache_type: str
    ) -> pd.DataFrame:
        """Helper: carga SQL, sustituye params, ejecuta con cache."""
        cache_params = {**params, "sql_file": sql_file}

        if self.use_cache and self.cache:
            df = self.cache.get(cache_params, data_type=cache_type)
            if df is not None:
                print(f"   ✅ {cache_type}: cargado desde caché ({len(df):,} filas)")
                return df

        query_template = self._load_sql_template(sql_file)

        # Build sites SQL tuple
        sites = params.get("sites", [])
        sites_sql = "('" + "', '".join(sites) + "')"

        # Derive month-format params (YYYYMM) from date-format params (YYYY-MM-DD)
        fecha_min = params.get("fecha_minima", "")
        fecha_max = params.get("fecha_maxima", "")
        fecha_min_month = fecha_min.replace("-", "")[:6] if fecha_min else ""
        fecha_max_month = fecha_max.replace("-", "")[:6] if fecha_max else ""

        query = query_template.format(
            sites=sites_sql,
            fecha_minima=fecha_min,
            fecha_maxima=fecha_max,
            fecha_minima_month=fecha_min_month,
            fecha_maxima_month=fecha_max_month,
        )

        print(f"   🔄 Ejecutando query {sql_file} en BigQuery...")
        start_time = time.time()
        df = self.client.query_to_dataframe(query)
        elapsed = time.time() - start_time
        print(f"   ⏱️  {cache_type} completado en {elapsed:.1f}s ({len(df):,} filas)")

        if self.use_cache and self.cache:
            self.cache.set(cache_params, df, data_type=cache_type)

        return df

    def _calc_date_range(self, fecha_final: str) -> tuple[str, str]:
        """Calcula fecha_minima y fecha_maxima. Usa quarters si están configurados."""
        if self.quarter_anterior and self.quarter_actual:
            return quarters_to_date_range(self.quarter_anterior, self.quarter_actual)
        mes_inicio = calcular_meses_atras(fecha_final, 13)
        fecha_minima = f"{mes_inicio[:4]}-{mes_inicio[4:]}-01"

        año_final = int(fecha_final[:4])
        mes_final = int(fecha_final[4:])
        if mes_final == 12:
            fecha_maxima = f"{año_final + 1}-01-01"
        else:
            fecha_maxima = f"{año_final}-{mes_final + 1:02d}-01"

        return fecha_minima, fecha_maxima

    def load_credits(self, sites: list[str], fecha_final: str) -> pd.DataFrame:
        """
        Carga datos de créditos (FRED, tarjeta de crédito, ofertas).
        Fuente: enrichment_credits.sql (query con filtro NPS sellers para evitar OOM).
        """
        print("   💳 Cargando datos de Credits...")
        # Siempre usa query directa con filtro de sellers NPS (Dataflow sin filtro da OOM)
        fecha_minima, fecha_maxima = self._calc_date_range(fecha_final)
        params = {
            "sites": sorted(sites),
            "fecha_final": fecha_final,
            "fecha_minima": fecha_minima,
            "fecha_maxima": fecha_maxima,
        }
        return self._query_with_cache("enrichment_credits.sql", params, "enrichment_credits")

    def load_credits_universo(self, sites: list[str], fecha_final: str) -> pd.DataFrame:
        """
        Carga shares de FRED sobre base SEGMENTATION_SELLERS (universo real).
        Denominador: todos los sellers activos del producto en SEGMENTATION_SELLERS.
        """
        print("   💳 Cargando universo Credits (base: SEGMENTATION_SELLERS)...")
        fecha_minima, fecha_maxima = self._calc_date_range(fecha_final)
        product_filter = self.PRODUCT_FILTERS_SEG.get(self.update_tipo, "")
        params = {
            "sites": sorted(sites),
            "fecha_final": fecha_final,
            "fecha_minima": fecha_minima,
            "fecha_maxima": fecha_maxima,
            "update_tipo": self.update_tipo,
        }

        cache_params = {**params, "sql_file": "enrichment_credits_universo.sql"}
        if self.use_cache and self.cache:
            df = self.cache.get(cache_params, data_type="credits_universo")
            if df is not None:
                print(f"   ✅ credits_universo: cargado desde caché ({len(df):,} filas)")
                return df

        query_template = self._load_sql_template("enrichment_credits_universo.sql")
        sites_sql = "('" + "', '".join(sorted(sites)) + "')"
        fecha_min_month = fecha_minima.replace("-", "")[:6]
        fecha_max_month = fecha_maxima.replace("-", "")[:6]
        query = query_template.format(
            sites=sites_sql,
            fecha_minima_month=fecha_min_month,
            fecha_maxima_month=fecha_max_month,
            product_filter=product_filter,
        )

        print(f"   🔄 Ejecutando query enrichment_credits_universo.sql en BigQuery...")
        start_time = time.time()
        df = self.client.query_to_dataframe(query)
        elapsed = time.time() - start_time
        print(f"   ⏱️  credits_universo completado en {elapsed:.1f}s ({len(df):,} filas)")

        if self.use_cache and self.cache:
            self.cache.set(cache_params, df, data_type="credits_universo")

        return df

    _RANGO_CASE_MLB = """CASE
        WHEN LIMITE_OFRECIDO IS NULL OR LIMITE_OFRECIDO <= 0 THEN 'Sin TC'
        WHEN LIMITE_OFRECIDO <= 500   THEN 'Hasta R$500'
        WHEN LIMITE_OFRECIDO <= 2000  THEN 'R$500-2K'
        WHEN LIMITE_OFRECIDO <= 8000  THEN 'R$2K-8K'
        WHEN LIMITE_OFRECIDO <= 25000 THEN 'R$8K-25K'
        ELSE '+R$25K'
    END"""
    _RANGO_CASE_MLM = """CASE
        WHEN LIMITE_OFRECIDO IS NULL OR LIMITE_OFRECIDO <= 0 THEN 'Sin TC'
        WHEN LIMITE_OFRECIDO <= 3000  THEN 'Hasta $3K'
        WHEN LIMITE_OFRECIDO <= 10000 THEN '$3K-10K'
        WHEN LIMITE_OFRECIDO <= 30000 THEN '$10K-30K'
        WHEN LIMITE_OFRECIDO <= 80000 THEN '$30K-80K'
        ELSE '+$80K'
    END"""
    _RANGO_CASE_MLA = """CASE
        WHEN LIMITE_OFRECIDO IS NULL OR LIMITE_OFRECIDO <= 0 THEN 'Sin TC'
        WHEN LIMITE_OFRECIDO <= 100000  THEN 'Hasta $100K'
        WHEN LIMITE_OFRECIDO <= 500000  THEN '$100K-500K'
        WHEN LIMITE_OFRECIDO <= 1500000 THEN '$500K-1.5M'
        WHEN LIMITE_OFRECIDO <= 5000000 THEN '$1.5M-5M'
        ELSE '+$5M'
    END"""
    _RANGO_CASE_DEFAULT = _RANGO_CASE_MLB

    def load_tc_limite_universo(self, sites: list[str], fecha_final: str) -> pd.DataFrame:
        """
        Carga distribución RANGO_LIMITE_TC + OFERTA_TC sobre universo SEGMENTATION_SELLERS.
        """
        print("   💳 Cargando universo TC Límite (base: SEGMENTATION_SELLERS)...")
        fecha_minima, fecha_maxima = self._calc_date_range(fecha_final)
        fecha_min_month = fecha_minima.replace("-", "")[:6]
        fecha_max_month = fecha_maxima.replace("-", "")[:6]
        product_filter = self.PRODUCT_FILTERS_SEG.get(self.update_tipo, "")

        # Pick RANGO CASE based on site
        site = sites[0] if sites else "MLB"
        rango_case = {
            "MLB": self._RANGO_CASE_MLB,
            "MLM": self._RANGO_CASE_MLM,
            "MLA": self._RANGO_CASE_MLA,
        }.get(site, self._RANGO_CASE_DEFAULT)

        params = {
            "sites": sorted(sites),
            "fecha_final": fecha_final,
            "fecha_minima": fecha_minima,
            "fecha_maxima": fecha_maxima,
            "fecha_minima_month": fecha_min_month,
            "fecha_maxima_month": fecha_max_month,
            "update_tipo": self.update_tipo,
        }
        cache_params = {**params, "sql_file": "enrichment_tc_limite_universo.sql"}
        if self.use_cache and self.cache:
            df = self.cache.get(cache_params, data_type="tc_limite_universo")
            if df is not None:
                print(f"   ✅ tc_limite_universo: cargado desde caché ({len(df):,} filas)")
                return df

        query_template = self._load_sql_template("enrichment_tc_limite_universo.sql")
        sites_sql = "('" + "', '".join(sorted(sites)) + "')"
        query = query_template.format(
            sites=sites_sql,
            fecha_minima_month=fecha_min_month,
            fecha_maxima_month=fecha_max_month,
            product_filter=product_filter,
            rango_case=rango_case,
        )

        print(f"   🔄 Ejecutando query enrichment_tc_limite_universo.sql en BigQuery...")
        start_time = time.time()
        df = self.client.query_to_dataframe(query)
        elapsed = time.time() - start_time
        print(f"   ⏱️  tc_limite_universo completado en {elapsed:.1f}s ({len(df):,} filas)")

        if self.use_cache and self.cache:
            self.cache.set(cache_params, df, data_type="tc_limite_universo")
        return df

    def load_tc_limite(self, sites: list[str], fecha_final: str) -> pd.DataFrame:
        """
        Carga límite de TC ofrecido y consumido por seller.
        Fuente: enrichment_tc_limite.sql (BT_CCARD_CADASTRAL_20).
        """
        print("   💳 Cargando datos de Límite TC...")
        fecha_minima, fecha_maxima = self._calc_date_range(fecha_final)
        fecha_min_month = fecha_minima.replace("-", "")[:6]
        fecha_max_month = fecha_maxima.replace("-", "")[:6]
        params = {
            "sites": sorted(sites),
            "fecha_final": fecha_final,
            "fecha_minima": fecha_minima,
            "fecha_maxima": fecha_maxima,
            "fecha_minima_month": fecha_min_month,
            "fecha_maxima_month": fecha_max_month,
        }
        return self._query_with_cache("enrichment_tc_limite.sql", params, "enrichment_tc_limite")

    def load_transacciones(self, sites: list[str], fecha_final: str) -> pd.DataFrame:
        """
        Carga datos de transacciones (TPV, TPN por producto, rangos).
        Fuente: enrichment_transacciones.sql o tabla TRANSACCIONES_SELLERS (Dataflow).
        """
        print("   💰 Cargando datos de Transacciones...")
        if self.use_dataflow_tables:
            mes_min, mes_max = self._calc_month_range(fecha_final)
            sites_sql = "('" + "', '".join(sorted(sites)) + "')"
            where = f"SIT_SITE_ID IN {sites_sql} AND TIM_MONTH >= {mes_min} AND TIM_MONTH <= {mes_max}"
            return self._load_from_dataflow_table(
                "TRANSACCIONES_SELLERS", where, "enrichment_transacciones",
                {"sites": sorted(sites), "fecha_final": fecha_final, "where": where},
            )
        fecha_minima, fecha_maxima = self._calc_date_range(fecha_final)
        params = {
            "sites": sorted(sites),
            "fecha_final": fecha_final,
            "fecha_minima": fecha_minima,
            "fecha_maxima": fecha_maxima,
        }
        return self._query_with_cache("enrichment_transacciones.sql", params, "enrichment_transacciones")

    def load_inversiones(self, sites: list[str], fecha_final: str) -> pd.DataFrame:
        """
        Carga datos de inversiones (POTS, fondeo, inversiones activas).
        REMUNERADA_SELLERS tiene 1.96B rows → siempre filtrar por NPS sellers.
        """
        print("   📈 Cargando datos de Inversiones...")
        fecha_minima, fecha_maxima = self._calc_date_range(fecha_final)
        sql_file = "enrichment_inversiones_dataflow.sql" if self.use_dataflow_tables else "enrichment_inversiones.sql"
        params = {
            "sites": sorted(sites),
            "fecha_final": fecha_final,
            "fecha_minima": fecha_minima,
            "fecha_maxima": fecha_maxima,
        }
        return self._query_with_cache(sql_file, params, "enrichment_inversiones")

    def load_topoff(self, sites: list[str], fecha_final: str) -> pd.DataFrame:
        """
        Carga datos de atención al cliente (Top Off).
        Fuente: BT_CX_SELLERS_MP_TOP_OFF (tabla de estado, sin dimensión temporal).
        Join key: CUS_CUST_ID + SIT_SITE_ID (sin TIM_MONTH).
        """
        print("   🎯 Cargando datos de Top Off / Atención...")
        # Tabla de estado: no necesita fechas, solo sites
        cache_params = {
            "sites": sorted(sites),
            "sql_file": "enrichment_topoff.sql",
        }

        if self.use_cache and self.cache:
            df = self.cache.get(cache_params, data_type="enrichment_topoff")
            if df is not None:
                print(f"   ✅ enrichment_topoff: cargado desde caché ({len(df):,} filas)")
                return df

        query_template = self._load_sql_template("enrichment_topoff.sql")
        sites_sql = "('" + "', '".join(sorted(sites)) + "')"
        query = query_template.format(sites=sites_sql)

        print(f"   🔄 Ejecutando query enrichment_topoff.sql en BigQuery...")
        start_time = time.time()
        df = self.client.query_to_dataframe(query)
        elapsed = time.time() - start_time
        print(f"   ⏱️  enrichment_topoff completado en {elapsed:.1f}s ({len(df):,} filas)")

        if self.use_cache and self.cache:
            self.cache.set(cache_params, df, data_type="enrichment_topoff")

        return df

    def load_topoff_universo(self, sites: list[str], fecha_final: str) -> pd.DataFrame:
        """
        Carga distribución Top Off sobre base SEGMENTATION_SELLERS (universo real).
        """
        print("   🎯 Cargando universo Top Off (base: SEGMENTATION_SELLERS)...")
        fecha_minima, fecha_maxima = self._calc_date_range(fecha_final)
        product_filter = self.PRODUCT_FILTERS_SEG.get(self.update_tipo, "")
        params = {
            "sites": sorted(sites),
            "fecha_final": fecha_final,
            "update_tipo": self.update_tipo,
        }

        cache_params = {**params, "sql_file": "enrichment_topoff_universo.sql"}
        if self.use_cache and self.cache:
            df = self.cache.get(cache_params, data_type="topoff_universo")
            if df is not None:
                print(f"   ✅ topoff_universo: cargado desde caché ({len(df):,} filas)")
                return df

        query_template = self._load_sql_template("enrichment_topoff_universo.sql")
        sites_sql = "('" + "', '".join(sorted(sites)) + "')"
        fecha_min_month = fecha_minima.replace("-", "")[:6]
        fecha_max_month = fecha_maxima.replace("-", "")[:6]
        query = query_template.format(
            sites=sites_sql,
            fecha_minima_month=fecha_min_month,
            fecha_maxima_month=fecha_max_month,
            product_filter=product_filter,
        )

        print(f"   🔄 Ejecutando query enrichment_topoff_universo.sql en BigQuery...")
        start_time = time.time()
        df = self.client.query_to_dataframe(query)
        elapsed = time.time() - start_time
        print(f"   ⏱️  topoff_universo completado en {elapsed:.1f}s ({len(df):,} filas)")

        if self.use_cache and self.cache:
            self.cache.set(cache_params, df, data_type="topoff_universo")

        return df

    def load_inversiones_universo(self, sites: list[str], fecha_final: str) -> pd.DataFrame:
        """
        Carga distribución de inversiones sobre base SEGMENTATION_SELLERS (universo real).
        """
        print("   📈 Cargando universo Inversiones (base: SEGMENTATION_SELLERS)...")
        fecha_minima, fecha_maxima = self._calc_date_range(fecha_final)
        product_filter = self.PRODUCT_FILTERS_SEG.get(self.update_tipo, "")
        params = {
            "sites": sorted(sites),
            "fecha_final": fecha_final,
            "update_tipo": self.update_tipo,
        }

        cache_params = {**params, "sql_file": "enrichment_inversiones_universo.sql"}
        if self.use_cache and self.cache:
            df = self.cache.get(cache_params, data_type="inversiones_universo")
            if df is not None:
                print(f"   ✅ inversiones_universo: cargado desde caché ({len(df):,} filas)")
                return df

        query_template = self._load_sql_template("enrichment_inversiones_universo.sql")
        sites_sql = "('" + "', '".join(sorted(sites)) + "')"
        fecha_min_month = fecha_minima.replace("-", "")[:6]
        fecha_max_month = fecha_maxima.replace("-", "")[:6]
        query = query_template.format(
            sites=sites_sql,
            fecha_minima_month=fecha_min_month,
            fecha_maxima_month=fecha_max_month,
            product_filter=product_filter,
        )

        print(f"   🔄 Ejecutando query enrichment_inversiones_universo.sql en BigQuery...")
        start_time = time.time()
        df = self.client.query_to_dataframe(query)
        elapsed = time.time() - start_time
        print(f"   ⏱️  inversiones_universo completado en {elapsed:.1f}s ({len(df):,} filas)")

        if self.use_cache and self.cache:
            self.cache.set(cache_params, df, data_type="inversiones_universo")

        return df

    def load_region(self, sites: list[str], fecha_final: str) -> pd.DataFrame:
        """Carga provincia/region del seller desde KYC."""
        print("   🌎 Cargando datos de Region...")
        fecha_minima, fecha_maxima = self._calc_date_range(fecha_final)
        params = {
            "sites": sorted(sites),
            "fecha_final": fecha_final,
            "fecha_minima": fecha_minima,
            "fecha_maxima": fecha_maxima,
        }
        return self._query_with_cache("enrichment_region.sql", params, "enrichment_region")

    def load_pricing(self, sites: list[str], fecha_final: str) -> pd.DataFrame:
        """Carga datos de pricing por escalas (MLB: via POLITICAS_PRICING, MLA/MLM: via PRODUCT_PRICING)."""
        print("   💲 Cargando datos de Pricing por escalas...")
        fecha_minima, fecha_maxima = self._calc_date_range(fecha_final)

        # Choose SQL file based on site
        mlb_sites = {"MLB"}
        is_mlb = any(s in mlb_sites for s in sites)
        sql_file = "enrichment_pricing_mlb.sql" if is_mlb else "enrichment_pricing_mla_mlm.sql"

        params = {
            "sites": sorted(sites),
            "fecha_final": fecha_final,
            "fecha_minima": fecha_minima,
            "fecha_maxima": fecha_maxima,
        }
        return self._query_with_cache(sql_file, params, "enrichment_pricing")

    def load_pricing_universo(self, sites: list[str], fecha_final: str) -> pd.DataFrame:
        """
        Carga distribución de pricing sobre base SEGMENTATION_SELLERS (universo real).
        """
        print("   💲 Cargando universo Pricing (base: SEGMENTATION_SELLERS)...")
        fecha_minima, fecha_maxima = self._calc_date_range(fecha_final)
        product_filter = self.PRODUCT_FILTERS_SEG.get(self.update_tipo, "")

        mlb_sites = {"MLB"}
        is_mlb = any(s in mlb_sites for s in sites)
        sql_file = "enrichment_pricing_universo_mlb.sql" if is_mlb else "enrichment_pricing_universo_mla_mlm.sql"

        params = {
            "sites": sorted(sites),
            "fecha_final": fecha_final,
            "update_tipo": self.update_tipo,
        }

        cache_params = {**params, "sql_file": sql_file}
        if self.use_cache and self.cache:
            df = self.cache.get(cache_params, data_type="pricing_universo")
            if df is not None:
                print(f"   ✅ pricing_universo: cargado desde caché ({len(df):,} filas)")
                return df

        query_template = self._load_sql_template(sql_file)
        sites_sql = "('" + "', '".join(sorted(sites)) + "')"
        fecha_min_month = fecha_minima.replace("-", "")[:6]
        fecha_max_month = fecha_maxima.replace("-", "")[:6]
        query = query_template.format(
            sites=sites_sql,
            fecha_minima_month=fecha_min_month,
            fecha_maxima_month=fecha_max_month,
            product_filter=product_filter,
        )

        print(f"   🔄 Ejecutando query {sql_file} en BigQuery...")
        start_time = time.time()
        df = self.client.query_to_dataframe(query)
        elapsed = time.time() - start_time
        print(f"   ⏱️  pricing_universo completado en {elapsed:.1f}s ({len(df):,} filas)")

        if self.use_cache and self.cache:
            self.cache.set(cache_params, df, data_type="pricing_universo")

        return df

    def load_segmentacion_universo(self, sites: list[str], fecha_final: str) -> pd.DataFrame:
        """
        Carga distribución de Producto Principal, Newbie/Legacy, Flag Only Transfer
        sobre base SEGMENTATION_SELLERS (universo real).
        """
        print("   🏷️  Cargando universo Segmentación (base: SEGMENTATION_SELLERS)...")
        fecha_minima, fecha_maxima = self._calc_date_range(fecha_final)
        product_filter = self.PRODUCT_FILTERS_SEG.get(self.update_tipo, "")
        params = {
            "sites": sorted(sites),
            "fecha_final": fecha_final,
            "update_tipo": self.update_tipo,
        }

        cache_params = {**params, "sql_file": "enrichment_segmentacion_universo.sql"}
        if self.use_cache and self.cache:
            df = self.cache.get(cache_params, data_type="segmentacion_universo")
            if df is not None:
                print(f"   ✅ segmentacion_universo: cargado desde caché ({len(df):,} filas)")
                return df

        query_template = self._load_sql_template("enrichment_segmentacion_universo.sql")
        sites_sql = "('" + "', '".join(sorted(sites)) + "')"
        fecha_min_month = fecha_minima.replace("-", "")[:6]
        fecha_max_month = fecha_maxima.replace("-", "")[:6]
        query = query_template.format(
            sites=sites_sql,
            fecha_minima_month=fecha_min_month,
            fecha_maxima_month=fecha_max_month,
            product_filter=product_filter,
        )

        print(f"   🔄 Ejecutando query enrichment_segmentacion_universo.sql en BigQuery...")
        start_time = time.time()
        df = self.client.query_to_dataframe(query)
        elapsed = time.time() - start_time
        print(f"   ⏱️  segmentacion_universo completado en {elapsed:.1f}s ({len(df):,} filas)")

        if self.use_cache and self.cache:
            self.cache.set(cache_params, df, data_type="segmentacion_universo")

        return df

    def load_aprobacion_op(self, sites: list[str], fecha_final: str) -> pd.DataFrame:
        """
        Carga tasa de aprobación de pagos por seller para OP (LINK/APICOW).
        Solo se ejecuta cuando update_tipo es LINK o APICOW.
        """
        print("   💳 Cargando tasa de aprobación...")
        fecha_minima, fecha_maxima = self._calc_date_range(fecha_final)
        params = {
            "sites": sorted(sites),
            "fecha_final": fecha_final,
            "update_tipo": self.update_tipo,
        }

        cache_params = {**params, "sql_file": "enrichment_aprobacion_op.sql"}
        if self.use_cache and self.cache:
            df = self.cache.get(cache_params, data_type="aprobacion_op")
            if df is not None:
                print(f"   ✅ aprobacion_op: cargado desde caché ({len(df):,} filas)")
                return df

        query_template = self._load_sql_template("enrichment_aprobacion_op.sql")
        sites_sql = "('" + "', '".join(sorted(sites)) + "')"
        e_code_filter = self.E_CODE_FILTERS.get(self.update_tipo or "all", "")
        query = query_template.format(
            sites=sites_sql,
            fecha_minima=fecha_minima,
            fecha_maxima=fecha_maxima,
            e_code_filter=e_code_filter,
        )

        print(f"   🔄 Ejecutando query enrichment_aprobacion_op.sql en BigQuery...")
        start_time = time.time()
        df = self.client.query_to_dataframe(query)
        elapsed = time.time() - start_time
        print(f"   ⏱️  aprobacion_op completado en {elapsed:.1f}s ({len(df):,} filas)")

        if self.use_cache and self.cache:
            self.cache.set(cache_params, df, data_type="aprobacion_op")

        return df

    def load_aprobacion_universo(self, sites: list[str], fecha_final: str) -> pd.DataFrame:
        """
        Carga distribución de tasa de aprobación sobre SEGMENTATION_SELLERS (universo OP).
        """
        if self.update_tipo not in ("LINK", "APICOW"):
            return pd.DataFrame()

        print("   💳 Cargando universo Aprobación OP (base: SEGMENTATION_SELLERS)...")
        fecha_minima, fecha_maxima = self._calc_date_range(fecha_final)
        params = {
            "sites": sorted(sites),
            "fecha_final": fecha_final,
            "update_tipo": self.update_tipo,
        }

        cache_params = {**params, "sql_file": "enrichment_aprobacion_universo.sql"}
        if self.use_cache and self.cache:
            df = self.cache.get(cache_params, data_type="aprobacion_universo")
            if df is not None:
                print(f"   ✅ aprobacion_universo: cargado desde caché ({len(df):,} filas)")
                return df

        query_template = self._load_sql_template("enrichment_aprobacion_universo.sql")
        sites_sql = "('" + "', '".join(sorted(sites)) + "')"
        fecha_min_month = fecha_minima.replace("-", "")[:6]
        fecha_max_month = fecha_maxima.replace("-", "")[:6]
        query = query_template.format(
            sites=sites_sql,
            fecha_minima=fecha_minima,
            fecha_maxima=fecha_maxima,
            fecha_minima_month=fecha_min_month,
            fecha_maxima_month=fecha_max_month,
        )

        print(f"   🔄 Ejecutando query enrichment_aprobacion_universo.sql en BigQuery...")
        start_time = time.time()
        df = self.client.query_to_dataframe(query)
        elapsed = time.time() - start_time
        print(f"   ⏱️  aprobacion_universo completado en {elapsed:.1f}s ({len(df):,} filas)")

        if self.use_cache and self.cache:
            self.cache.set(cache_params, df, data_type="aprobacion_universo")

        return df

    def load_restricciones(self, sites: list[str], fecha_final: str) -> pd.DataFrame:
        """
        Carga flag CONSIDERACION_AJUSTADA para sellers OP (LINK/APICOW).
        Join con BT_RES_RESTRICTIONS_SENTENCES.
        Solo se ejecuta si update_tipo es LINK o APICOW.
        Join key: SURVEY_ID (QUALTRICS_RESPONSE_ID).
        """
        print("   🛡️  Cargando datos de Restricciones (CONSIDERACION_AJUSTADA)...")
        fecha_minima, fecha_maxima = self._calc_date_range(fecha_final)
        e_code_filter = DataLoader.E_CODE_FILTERS.get(self.update_tipo, "")

        params = {
            "sites": sorted(sites),
            "fecha_final": fecha_final,
            "fecha_minima": fecha_minima,
            "fecha_maxima": fecha_maxima,
            "update_tipo": self.update_tipo,
        }

        cache_params = {**params, "sql_file": "enrichment_restricciones.sql"}
        if self.use_cache and self.cache:
            df = self.cache.get(cache_params, data_type="enrichment_restricciones")
            if df is not None:
                print(f"   ✅ enrichment_restricciones: cargado desde caché ({len(df):,} filas)")
                return df

        query_template = self._load_sql_template("enrichment_restricciones.sql")
        sites_sql = "('" + "', '".join(sorted(sites)) + "')"
        query = query_template.format(
            sites=sites_sql,
            fecha_minima=fecha_minima,
            fecha_maxima=fecha_maxima,
            e_code_filter=e_code_filter,
        )

        print(f"   🔄 Ejecutando query enrichment_restricciones.sql en BigQuery...")
        start_time = time.time()
        df = self.client.query_to_dataframe(query)
        elapsed = time.time() - start_time
        print(f"   ⏱️  enrichment_restricciones completado en {elapsed:.1f}s ({len(df):,} filas)")

        if self.use_cache and self.cache:
            self.cache.set(cache_params, df, data_type="enrichment_restricciones")

        return df

    def load_segmentacion(self, sites: list[str], fecha_final: str) -> pd.DataFrame:
        """
        Carga datos de segmentación de sellers (producto principal, NEW_MAS_FLAG,
        segmento, PF/PJ, flags de producto).
        Fuente: enrichment_segmentacion.sql (siempre query directa con filtro NPS sellers).
        """
        print("   🏷️  Cargando datos de Segmentación...")
        # Siempre usa query directa con filtro NPS sellers (Dataflow sin filtro da OOM)
        fecha_minima, fecha_maxima = self._calc_date_range(fecha_final)
        params = {
            "sites": sorted(sites),
            "fecha_final": fecha_final,
            "fecha_minima": fecha_minima,
            "fecha_maxima": fecha_maxima,
        }
        return self._query_with_cache("enrichment_segmentacion.sql", params, "enrichment_segmentacion")
