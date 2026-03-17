"""
Sistema de caché para datos de BigQuery
"""

import hashlib
import json
import logging
import pickle
from pathlib import Path
from typing import Any, Optional

import pandas as pd

logger = logging.getLogger(__name__)


class DataCache:
    """
    Maneja el caché de datos de BigQuery para evitar recargas innecesarias.
    """

    def __init__(self, cache_dir: str = ".cache"):
        """
        Args:
            cache_dir: Directorio donde se guardan los archivos de caché
        """
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(exist_ok=True)

    def _generate_cache_key(self, params: dict) -> str:
        """
        Genera una key descriptiva basada en los parámetros principales de la query.
        
        Args:
            params: Diccionario con parámetros de la query
        
        Returns:
            String descriptivo con site y fecha (ej: "MLU_202512")
        """
        # Extraer parámetros principales para nombre descriptivo
        key_parts = []
        
        # Site (puede ser lista o string)
        if "sites" in params:
            sites = params["sites"]
            if isinstance(sites, list):
                key_parts.append("_".join(sorted(sites)))
            else:
                key_parts.append(str(sites))
        
        # Fecha final
        if "fecha_final" in params:
            key_parts.append(str(params["fecha_final"]))
        
        # Fecha mínima (para clasificaciones)
        if "fecha_minima" in params and "fecha_final" not in params:
            key_parts.append(str(params["fecha_minima"]))
        
        # Si no hay parámetros reconocidos, usar hash como fallback
        if not key_parts:
            params_str = json.dumps(params, sort_keys=True)
            hash_obj = hashlib.md5(params_str.encode())
            return hash_obj.hexdigest()
        
        return "_".join(key_parts)

    def _get_cache_path(self, cache_key: str, data_type: str = "nps") -> Path:
        """
        Retorna el path al archivo de caché con nombre descriptivo.
        
        Args:
            cache_key: Key descriptiva del caché (ej: "MLU_202512")
            data_type: Tipo de datos (nps, clasificaciones, datos_reales, etc.)
        
        Returns:
            Path al archivo de caché (ej: ".cache/nps_MLU_202512.pkl")
        """
        return self.cache_dir / f"{data_type}_{cache_key}.pkl"

    def get(
        self,
        params: dict,
        data_type: str = "nps",
    ) -> Optional[pd.DataFrame]:
        """
        Obtiene datos del caché si existen.
        
        Args:
            params: Parámetros de la query
            data_type: Tipo de datos
        
        Returns:
            DataFrame si existe en caché, None si no
        """
        cache_key = self._generate_cache_key(params)
        cache_path = self._get_cache_path(cache_key, data_type)

        if cache_path.exists():
            try:
                print(f"   💾 Encontrado caché: {cache_path.name}")
                with open(cache_path, "rb") as f:
                    cached_data = pickle.load(f)
                
                # Verificar que los parámetros coincidan
                cached_params = cached_data.get("params", {})
                if cached_params == params:
                    df = cached_data["data"]
                    print(f"   ✅ Caché VÁLIDO - Usando datos cacheados ({len(df):,} registros)")
                    # Mostrar info de qué está cacheado
                    if "sites" in cached_params:
                        print(f"      📍 Site: {cached_params['sites']}")
                    if "fecha_final" in cached_params:
                        print(f"      📅 Fecha: {cached_params['fecha_final']}")
                    return df
                else:
                    print(f"   ⚠️  Caché INVÁLIDO - Parámetros no coinciden")
                    print(f"      🔍 Cacheado: {cached_params}")
                    print(f"      🔍 Solicitado: {params}")
                    print(f"      🗑️  Eliminando caché obsoleto...")
                    cache_path.unlink()
            except Exception as e:
                print(f"   ⚠️  Error leyendo caché: {e}")
                if cache_path.exists():
                    cache_path.unlink()

        return None

    def set(
        self,
        params: dict,
        data: pd.DataFrame,
        data_type: str = "nps",
    ):
        """
        Guarda datos en el caché.
        
        Args:
            params: Parámetros de la query
            data: DataFrame a guardar
            data_type: Tipo de datos
        """
        cache_key = self._generate_cache_key(params)
        cache_path = self._get_cache_path(cache_key, data_type)

        try:
            cached_data = {
                "params": params,
                "data": data,
            }
            with open(cache_path, "wb") as f:
                pickle.dump(cached_data, f)
            
            # Tamaño del archivo
            size_mb = cache_path.stat().st_size / (1024 * 1024)
            print(f"   💾 Guardado en caché: {cache_path.name} ({size_mb:.1f} MB)")
            # Mostrar qué se guardó
            if "sites" in params:
                print(f"      📍 Site: {params['sites']}")
            if "fecha_final" in params:
                print(f"      📅 Fecha: {params['fecha_final']}")
        except Exception as e:
            print(f"   ❌ Error guardando caché: {e}")

    def clear(self, data_type: Optional[str] = None):
        """
        Limpia el caché.
        
        Args:
            data_type: Si se especifica, solo limpia ese tipo. Si es None, limpia todo.
        """
        if data_type:
            pattern = f"{data_type}_*.pkl"
        else:
            pattern = "*.pkl"

        count = 0
        for cache_file in self.cache_dir.glob(pattern):
            cache_file.unlink()
            count += 1

        logger.info(f"🗑️  Caché limpiado: {count} archivos eliminados")

    def info(self) -> dict:
        """
        Retorna información sobre el caché actual.
        
        Returns:
            Diccionario con información del caché
        """
        cache_files = list(self.cache_dir.glob("*.pkl"))
        total_size = sum(f.stat().st_size for f in cache_files)
        
        return {
            "cache_dir": str(self.cache_dir.absolute()),
            "num_files": len(cache_files),
            "total_size_mb": total_size / (1024 * 1024),
            "files": [
                {
                    "name": f.name,
                    "size_mb": f.stat().st_size / (1024 * 1024),
                    "modified": f.stat().st_mtime,
                }
                for f in cache_files
            ],
        }
