"""
Cliente de BigQuery usando Application Default Credentials (ADC)
"""

import logging
import os
from typing import Optional

import pandas as pd
from google.auth import default
from google.cloud import bigquery
from google.cloud.exceptions import NotFound, Forbidden

try:
    from google.cloud import bigquery_storage
    _HAS_BQ_STORAGE = True
except ImportError:
    _HAS_BQ_STORAGE = False

logger = logging.getLogger(__name__)


class BigQueryClient:
    """
    Cliente de BigQuery que usa ADC (Application Default Credentials).
    
    El usuario debe estar autenticado previamente con:
    - gcloud auth application-default login
    - O tener GOOGLE_APPLICATION_CREDENTIALS configurado
    """

    def __init__(self, project_id: Optional[str] = None):
        """
        Inicializa el cliente de BigQuery.
        
        Args:
            project_id: ID del proyecto de BigQuery. Si no se especifica,
                       se intenta obtener desde GOOGLE_CLOUD_PROJECT o de las
                       credenciales por defecto.
        """
        self.project_id = project_id or os.getenv("GOOGLE_CLOUD_PROJECT")
        self.client: Optional[bigquery.Client] = None
        self.credentials = None
        self._initialized = False

    def initialize(self) -> None:
        """
        Inicializa la conexión con BigQuery usando ADC.
        
        Raises:
            Exception: Si no se puede autenticar o crear el cliente.
        """
        try:
            logger.info("Inicializando cliente de BigQuery con ADC...")

            # Obtener credenciales por defecto
            credentials, default_project = default(
                scopes=["https://www.googleapis.com/auth/bigquery"]
            )
            self.credentials = credentials

            # Usar project_id especificado o el del entorno
            if not self.project_id:
                self.project_id = default_project
                logger.info(f"Usando project_id de credenciales: {self.project_id}")

            if not self.project_id:
                raise ValueError(
                    "No se pudo determinar el project_id. "
                    "Configura GOOGLE_CLOUD_PROJECT o pasa project_id al constructor."
                )

            # Crear cliente
            self.client = bigquery.Client(
                credentials=credentials, project=self.project_id
            )

            # Crear cliente Storage API para descargas rapidas (Arrow/columnar)
            self._bqstorage_client = None
            if _HAS_BQ_STORAGE:
                try:
                    self._bqstorage_client = bigquery_storage.BigQueryReadClient(
                        credentials=credentials
                    )
                    logger.info("✅ BigQuery Storage API habilitada (descarga rápida)")
                except Exception as e:
                    logger.warning(f"⚠️ Storage API no disponible, usando REST: {e}")

            self._initialized = True
            logger.info(f"✅ Cliente BigQuery inicializado (project: {self.project_id})")

        except Exception as e:
            logger.error(f"❌ Error inicializando cliente BigQuery: {e}")
            raise

    def check_connection(self) -> dict:
        """
        Verifica la conexión con BigQuery ejecutando SELECT 1.
        
        Returns:
            dict con el resultado del diagnóstico:
                - success: bool
                - project_id: str o None
                - message: str
                - error: str o None
        """
        result = {
            "success": False,
            "project_id": None,
            "message": "",
            "error": None,
        }

        try:
            if not self._initialized:
                self.initialize()

            # Test 1: SELECT 1
            query = "SELECT 1 as test"
            query_job = self.client.query(query)
            rows = list(query_job.result())

            if len(rows) == 1 and rows[0]["test"] == 1:
                result["success"] = True
                result["project_id"] = self.project_id
                result["message"] = f"✅ Conexión exitosa a BigQuery (project: {self.project_id})"
                logger.info(result["message"])
            else:
                result["error"] = "SELECT 1 no retornó el resultado esperado"
                result["message"] = f"⚠️ {result['error']}"

        except Exception as e:
            result["error"] = str(e)
            result["message"] = f"❌ Error conectando a BigQuery: {e}"
            logger.error(result["message"])

        return result

    def check_table_access(self, table_ref: str) -> dict:
        """
        Verifica si una tabla existe y es accesible.
        
        Args:
            table_ref: Referencia completa de la tabla (project.dataset.table)
        
        Returns:
            dict con el resultado:
                - exists: bool
                - accessible: bool
                - row_count: int o None
                - error: str o None
        """
        result = {
            "exists": False,
            "accessible": False,
            "row_count": None,
            "error": None,
        }

        try:
            if not self._initialized:
                self.initialize()

            # Verificar existencia de la tabla
            try:
                table = self.client.get_table(table_ref)
                result["exists"] = True
                result["row_count"] = table.num_rows
            except NotFound:
                result["error"] = f"Tabla no encontrada: {table_ref}"
                return result
            except Forbidden:
                result["error"] = f"Acceso denegado a tabla: {table_ref}"
                return result

            # Intentar ejecutar una query simple
            query = f"SELECT COUNT(*) as cnt FROM `{table_ref}` LIMIT 1"
            query_job = self.client.query(query)
            rows = list(query_job.result())

            if rows:
                result["accessible"] = True
                logger.info(f"✅ Tabla accesible: {table_ref} ({result['row_count']:,} filas)")
            else:
                result["error"] = "No se pudo ejecutar COUNT(*) en la tabla"

        except Exception as e:
            result["error"] = str(e)
            logger.error(f"❌ Error verificando tabla {table_ref}: {e}")

        return result

    def query_to_dataframe(self, query: str, params: Optional[dict] = None) -> pd.DataFrame:
        """
        Ejecuta una query y retorna un DataFrame de pandas.
        
        Args:
            query: Query SQL a ejecutar
            params: Parámetros para query parametrizada (opcional)
        
        Returns:
            DataFrame con los resultados
        
        Raises:
            Exception: Si hay error en la ejecución
        """
        if not self._initialized:
            self.initialize()

        try:
            logger.info("Ejecutando query en BigQuery...")
            logger.debug(f"Query: {query[:200]}...")

            # Configurar job
            job_config = bigquery.QueryJobConfig()
            if params:
                # Convertir params a QueryParameters si es necesario
                pass

            # Ejecutar query
            query_job = self.client.query(query, job_config=job_config)

            # Convertir a DataFrame (usa Storage API si esta disponible)
            if self._bqstorage_client:
                df = query_job.to_dataframe(bqstorage_client=self._bqstorage_client)
            else:
                df = query_job.to_dataframe()

            logger.info(f"✅ Query ejecutada exitosamente: {len(df):,} filas obtenidas")
            return df

        except Exception as e:
            logger.error(f"❌ Error ejecutando query: {e}")
            raise

    def execute_query(self, query: str) -> list:
        """
        Ejecuta una query y retorna las filas como lista de dicts.
        
        Args:
            query: Query SQL a ejecutar
        
        Returns:
            Lista de diccionarios con los resultados
        """
        if not self._initialized:
            self.initialize()

        try:
            query_job = self.client.query(query)
            rows = [dict(row) for row in query_job.result()]
            return rows
        except Exception as e:
            logger.error(f"❌ Error ejecutando query: {e}")
            raise


def create_bigquery_client(project_id: Optional[str] = None) -> BigQueryClient:
    """
    Factory function para crear un cliente de BigQuery.
    
    Args:
        project_id: ID del proyecto (opcional)
    
    Returns:
        Instancia de BigQueryClient
    """
    client = BigQueryClient(project_id=project_id)
    client.initialize()
    return client
