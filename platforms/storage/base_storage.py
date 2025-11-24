# ...existing code...
from __future__ import annotations
import logging
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional
import pandas as pd
# optional pyarrow HDFS support


def to_primitive(obj: Any) -> Any:
    """
    Convert dataclass, pandas, or nested structures to JSON-serializable primitives.
    """
    if obj is None:
        return None

    if isinstance(obj, dict):
        return {key: to_primitive(value) for key, value in obj.items()}

    if isinstance(obj, list):
        return [to_primitive(item) for item in obj]

    if isinstance(obj, pd.DataFrame):
        return obj.where(pd.notnull(obj), None).to_dict(orient="records")

    if isinstance(obj, pd.Series):
        return obj.where(pd.notnull(obj), None).to_dict()

    if isinstance(obj, (str, int, float, bool)):
        return obj

    try:
        return str(obj)
    except TypeError:
        return None


def _parse_namenode_uri(uri: Optional[str]) -> Dict[str, Optional[Any]]:
    """Parse hdfs://host:port → {'host': host, 'port': port}"""
    if not uri:
        return {}
    u = uri.replace("hdfs://", "")
    parts = u.split(":")
    host = parts[0] if parts else ""
    port = int(parts[1]) if len(parts) > 1 and parts[1].isdigit() else None
    return {"host": host, "port": port}

class StorageBackend(ABC):
    """Minimal backend interface."""

    @abstractmethod
    def save(self, dataset_name: str, data: Any, fmt: Optional[str] = None) -> Dict[str, Any]:
        """Persist data. Return dict summary."""
        pass

    # Additional CRUD/schema operations to be implemented by concrete backends
    @abstractmethod
    def create_database(self, name: str) -> Dict[str, Any]:
        pass

    @abstractmethod
    def delete_database(self, name: str) -> Dict[str, Any]:
        pass

    @abstractmethod
    def create_schema(self, name: str, schema: Optional[Dict] = None) -> Dict[str, Any]:
        """Create or apply schema (collection validation or directory structure)."""
        pass

    @abstractmethod
    def rename_schema(self, old_name: str, new_name: str) -> Dict[str, Any]:
        pass

    @abstractmethod
    def create_table(self, name: str, schema: Optional[Dict] = None) -> Dict[str, Any]:
        pass

    @abstractmethod
    def truncate_table(self, name: str) -> Dict[str, Any]:
        pass

    @abstractmethod
    def delete_table(self, name: str) -> Dict[str, Any]:
        pass

    @abstractmethod
    def rename_table(self, old_name: str, new_name: str) -> Dict[str, Any]:
        pass

    @abstractmethod
    def insert(self, target: str, data: Any) -> Dict[str, Any]:
        pass

    @abstractmethod
    def update(self, target: str, query: Dict[str, Any], update_doc: Dict[str, Any]) -> Dict[str, Any]:
        pass


class DataStorageOrchestrator:
    """
    Coordinate storing extracted objects to one or more storage backends.

    - Accepts an injectable list of StorageBackend implementations.
    - Each backend is responsible for its own errors; orchestrator aggregates results.
    """

    def __init__(self, storages: List[StorageBackend], logger: Optional[logging.Logger] = None):
        if not storages:
            raise ValueError("At least one storage backend is required")
        self.storages = storages
        self.logger = logger or logging.getLogger(__name__)

    def store(self, dataset_name: str, data: Any, fmt: str = "json") -> Dict[str, Any]:
        results: Dict[str, Any] = {}
        for backend in self.storages:
            name = backend.__class__.__name__
            try:
                result = backend.save(dataset_name, data, fmt)
                results[name] = result
                self.logger.info("%s: saved %s -> %s", name, dataset_name, result.get("path", result.get("collection", "")))
            except Exception as e:
                self.logger.exception("Backend %s failed to save dataset %s", name, dataset_name)
                results[name] = {"ok": False, "error": str(e)}
        return results
# ...existing code...





class MetaSurrogateRepository_postgresql:
    """Lưu surrogate key đã generate vào meta_surrogate_map"""

    def __init__(self, postgres_client):
        self.pg = postgres_client

    def get_or_create(self, natural_key: str, type_name: str, generator):
        sql_get = """
            SELECT surrogate_key FROM meta_surrogate_map
            WHERE natural_key=%s AND type=%s AND valid_to IS NULL;
        """
        res = self.pg.fetch_one(sql_get, (natural_key, type_name))

        if res:
            return res[0]

        # generate mới
        surrogate_key = generator.get_id()
        sql_ins = """
            INSERT INTO meta_surrogate_map(natural_key, surrogate_key, type, valid_from)
            VALUES(%s,%s,%s, NOW())
        """
        self.pg.execute(sql_ins, (natural_key, surrogate_key, type_name))
        return surrogate_key

class TransformDatawarehouse:
    """
    Base class – all shared components (Mongo, surrogate repo, table creator).
    """

    def __init__(self, schema_dw, datawarehouse_logger):
        self.datawarehouse_logger = datawarehouse_logger
        self.schema_dw = schema_dw
    
    def set_repo_postgresql(self, postgres_client):
        self.repo = MetaSurrogateRepository_postgresql(postgres_client)

    def get_datalake_mongo(self, MongoLoader, MongoStorageBackend, datalake_config: Dict):
        """
        Get a MongoStorageBackend using the provided datalake config.
        """
        mongo_loader = MongoLoader(
            username=datalake_config.get("username"),
            password=datalake_config.get("password"),
            host=datalake_config.get("host"),
            auth_source=datalake_config.get("authSource"),
            port=datalake_config.get("port"),
            database=datalake_config.get("database"),
        )
        return MongoStorageBackend(mongo_loader)
