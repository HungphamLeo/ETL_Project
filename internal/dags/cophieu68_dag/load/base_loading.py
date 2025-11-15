# ...existing code...
from __future__ import annotations

import json
import logging
import os
import shutil
import subprocess
import tempfile
from abc import ABC, abstractmethod
from dataclasses import asdict, is_dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence

import pandas as pd
from pymongo import MongoClient, errors as pymongo_errors

# optional pyarrow HDFS support
try:
    from pyarrow import fs as pa_fs

    _HAS_PYARROW = True
except Exception:
    _HAS_PYARROW = False

logger = logging.getLogger(__name__)


# ---------------------------
# Utilities
# ---------------------------
def _to_primitive(obj: Any) -> Any:
    """Convert dataclass / pandas / nested structures to JSON-serializable primitives."""
    if obj is None:
        return None
    if is_dataclass(obj):
        return _to_primitive(asdict(obj))
    if isinstance(obj, dict):
        return {k: _to_primitive(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_to_primitive(x) for x in obj]
    if isinstance(obj, pd.DataFrame):
        return obj.where(pd.notnull(obj), None).to_dict(orient="records")
    if isinstance(obj, pd.Series):
        return obj.where(pd.notnull(obj), None).to_dict()
    if isinstance(obj, (str, int, float, bool)):
        return obj
    try:
        return str(obj)
    except Exception:
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


# ---------------------------
# Interfaces (SRP)
# ---------------------------
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


# ---------------------------
# HDFS writer (single responsibility)
# ---------------------------
class HDFSWriter:
    """
    HDFS writer that prefers pyarrow.fs. Falls back to `hdfs` CLI if pyarrow not available.

    Responsibilities:
      - serialize data (json/parquet)
      - write to HDFS path under provided base_path
    """

    def __init__(self, namenode_uri: str, base_path: str, logger: Optional[logging.Logger] = None):
        self.nn_conf = _parse_namenode_uri(namenode_uri)
        self.base_path = base_path.rstrip("/")
        self.logger = logger or logging.getLogger(__name__)

    def _get_fs(self):
        if not _HAS_PYARROW:
            raise RuntimeError("pyarrow not available")
        return pa_fs.HadoopFileSystem(host=self.nn_conf.get("host"), port=self.nn_conf.get("port"))

    def _full_path(self, relative_path: str) -> str:
        return str(Path(self.base_path) / relative_path.lstrip("/"))

    def write_json(self, obj: Any, relative_path: str) -> str:
        """Write JSON-serializable object to HDFS. Returns full HDFS path."""
        payload = json.dumps(_to_primitive(obj), ensure_ascii=False, indent=2).encode("utf-8")
        hdfs_path = self._full_path(relative_path)
        # try pyarrow
        if _HAS_PYARROW:
            try:
                fs = self._get_fs()
                with fs.open_output_stream(hdfs_path) as out:
                    out.write(payload)
                self.logger.info("Wrote JSON to HDFS: %s", hdfs_path)
                return hdfs_path
            except Exception:
                self.logger.debug("pyarrow write failed, fallback to CLI", exc_info=True)
        # fallback to CLI
        self._write_via_cli(payload, hdfs_path, is_binary=True)
        return hdfs_path

    def write_parquet(self, df: pd.DataFrame, relative_path: str) -> str:
        """Write DataFrame as Parquet to HDFS. Returns full HDFS path."""
        hdfs_path = self._full_path(relative_path)
        tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".parquet")
        tmp.close()
        try:
            df.to_parquet(tmp.name, index=False)
            if _HAS_PYARROW:
                try:
                    fs = self._get_fs()
                    with open(tmp.name, "rb") as src, fs.open_output_stream(hdfs_path) as dest:
                        dest.write(src.read())
                    self.logger.info("Wrote Parquet to HDFS: %s", hdfs_path)
                    return hdfs_path
                except Exception:
                    self.logger.debug("pyarrow parquet write failed; using CLI", exc_info=True)
            # fallback to CLI (use file)
            self._write_via_cli_file(tmp.name, hdfs_path)
            return hdfs_path
        finally:
            try:
                os.unlink(tmp.name)
            except Exception:
                pass

    def _write_via_cli(self, payload: bytes, hdfs_path: str, is_binary: bool = True) -> None:
        """Write bytes payload to HDFS via CLI using a temp file."""
        tmp = tempfile.NamedTemporaryFile(delete=False)
        try:
            mode = "wb" if is_binary else "w"
            with open(tmp.name, mode, encoding=None if is_binary else "utf-8") as f:
                f.write(payload)
            self._write_via_cli_file(tmp.name, hdfs_path)
        finally:
            try:
                os.unlink(tmp.name)
            except Exception:
                pass

    def _write_via_cli_file(self, local_file: str, hdfs_path: str) -> None:
        hdfs_bin = shutil.which("hdfs")
        if not hdfs_bin:
            raise RuntimeError("pyarrow not available and 'hdfs' CLI not found")
        dest_dir = str(Path(hdfs_path).parent)
        subprocess.check_call([hdfs_bin, "dfs", "-mkdir", "-p", dest_dir])
        subprocess.check_call([hdfs_bin, "dfs", "-put", "-f", local_file, hdfs_path])
        self.logger.info("Wrote to HDFS via CLI: %s", hdfs_path)


# ---------------------------
# Mongo writer (single responsibility)
# ---------------------------
class MongoWriter:
    """
    Lightweight Mongo writer. Uses a short-lived client per call to ensure resource cleanup.
    """

    def __init__(self, username: str, 
                        password: str, 
                        authSource: str, 
                        database: str, 
                        collection: str, logger: Optional[logging.Logger] = None, **client_kwargs):
        
        self.username = username        # để trống nếu không bật auth
        self.password= password
        self.authSource= authSource
        self.database = database
        self.collection = collection
        self.uri = f"mongodb://{username}:{password}@localhost:27017/?authSource={authSource}"
        self.logger = logger or logging.getLogger(__name__)
        self.client_kwargs = client_kwargs

    def insert(self, docs: Sequence[Dict]) -> Dict[str, Any]:
        """Insert list of docs, return summary."""
        if not docs:
            return {"inserted_count": 0}
        client = MongoClient(self.uri, **self.client_kwargs)
        try:
            db = client[self.database]
            coll = db[self.collection]
            try:
                res = coll.insert_many(list(docs))
                count = len(res.inserted_ids)
                self.logger.info("Inserted %d docs -> %s.%s", count, self.database, self.collection)
                return {"inserted_count": count, "collection": self.collection}
            except pymongo_errors.BulkWriteError as bwe:
                # fallback to single inserts for partial failures
                self.logger.warning("Bulk insert failed, falling back to single inserts: %s", str(bwe))
                cnt = 0
                for d in docs:
                    try:
                        coll.insert_one(d)
                        cnt += 1
                    except Exception:
                        self.logger.debug("single insert failed", exc_info=True)
                return {"inserted_count": cnt, "collection": self.collection, "error": str(bwe)}
            except Exception as e:
                self.logger.error("Insert failed: %s", e)
                return {"inserted_count": 0, "collection": self.collection, "error": str(e)}
        finally:
            try:
                client.close()
            except Exception:
                pass


# ---------------------------
# Storage backend adapters (OCP)
# ---------------------------

class HDFSStorageBackend(StorageBackend):
    """Adapter to expose HDFSWriter as StorageBackend and provide basic DDL/DML-like ops."""

    def __init__(self, hdfs_writer: HDFSWriter):
        self.hdfs = hdfs_writer
        self.base = self.hdfs.base_path

    def _hdfs_path(self, name: str) -> str:
        return str(Path(self.base) / name.lstrip("/"))

    def _run_hdfs_cmd(self, args: List[str]) -> None:
        hdfs_bin = shutil.which("hdfs")
        if not hdfs_bin:
            raise RuntimeError("'hdfs' CLI not found")
        subprocess.check_call([hdfs_bin, "dfs", *args])

    def save(self, dataset_name: str, data: Any, fmt: str = "json") -> Dict[str, Any]:
        try:
            ts = int(pd.Timestamp.now().timestamp())
            prim = _to_primitive(data)
            if fmt == "parquet":
                df = pd.DataFrame(prim) if isinstance(prim, list) else pd.DataFrame([prim])
                rel = f"{dataset_name}/{ts}.parquet"
                path = self.hdfs.write_parquet(df, rel)
            else:
                rel = f"{dataset_name}/{ts}.json"
                path = self.hdfs.write_json(prim, rel)
            return {"ok": True, "path": path}
        except Exception as e:
            logger.exception("HDFS save failed")
            return {"ok": False, "error": str(e)}

    def create_database(self, name: str) -> Dict[str, Any]:
        try:
            p = self._hdfs_path(name)
            # mkdir -p
            self._run_hdfs_cmd(["-mkdir", "-p", p])
            return {"ok": True, "path": p}
        except Exception as e:
            return {"ok": False, "error": str(e)}

    def delete_database(self, name: str) -> Dict[str, Any]:
        try:
            p = self._hdfs_path(name)
            self._run_hdfs_cmd(["-rm", "-r", "-f", p])
            return {"ok": True, "path": p}
        except Exception as e:
            return {"ok": False, "error": str(e)}

    def create_schema(self, name: str, schema: Optional[Dict] = None) -> Dict[str, Any]:
        # For HDFS treat schema as a directory under DB
        try:
            p = self._hdfs_path(name)
            self._run_hdfs_cmd(["-mkdir", "-p", p])
            return {"ok": True, "path": p}
        except Exception as e:
            return {"ok": False, "error": str(e)}

    def rename_schema(self, old_name: str, new_name: str) -> Dict[str, Any]:
        try:
            oldp = self._hdfs_path(old_name)
            newp = self._hdfs_path(new_name)
            self._run_hdfs_cmd(["-mv", oldp, newp])
            return {"ok": True, "from": oldp, "to": newp}
        except Exception as e:
            return {"ok": False, "error": str(e)}

    def create_table(self, name: str, schema: Optional[Dict] = None) -> Dict[str, Any]:
        # create directory or empty file for table
        try:
            p = self._hdfs_path(name)
            self._run_hdfs_cmd(["-mkdir", "-p", p])
            return {"ok": True, "path": p}
        except Exception as e:
            return {"ok": False, "error": str(e)}

    def truncate_table(self, name: str) -> Dict[str, Any]:
        try:
            p = self._hdfs_path(name)
            # remove contents and recreate dir
            self._run_hdfs_cmd(["-rm", "-r", "-f", f"{p}/*"])
            return {"ok": True, "table": p}
        except Exception as e:
            return {"ok": False, "error": str(e)}

    def delete_table(self, name: str) -> Dict[str, Any]:
        try:
            p = self._hdfs_path(name)
            self._run_hdfs_cmd(["-rm", "-r", "-f", p])
            return {"ok": True, "table": p}
        except Exception as e:
            return {"ok": False, "error": str(e)}

    def rename_table(self, old_name: str, new_name: str) -> Dict[str, Any]:
        try:
            oldp = self._hdfs_path(old_name)
            newp = self._hdfs_path(new_name)
            self._run_hdfs_cmd(["-mv", oldp, newp])
            return {"ok": True, "from": oldp, "to": newp}
        except Exception as e:
            return {"ok": False, "error": str(e)}

    def insert(self, target: str, data: Any) -> Dict[str, Any]:
        # Write data under target path; target considered as directory/filename prefix
        try:
            prim = _to_primitive(data)
            if isinstance(prim, (list, dict, pd.DataFrame)):
                # prefer parquet if DataFrame-like
                if isinstance(prim, pd.DataFrame) or (isinstance(prim, list) and all(isinstance(x, dict) for x in prim)):
                    df = pd.DataFrame(prim) if not isinstance(prim, pd.DataFrame) else prim
                    rel = f"{target}/{int(pd.Timestamp.now().timestamp())}.parquet"
                    path = self.hdfs.write_parquet(df, rel)
                else:
                    rel = f"{target}/{int(pd.Timestamp.now().timestamp())}.json"
                    path = self.hdfs.write_json(prim, rel)
            else:
                rel = f"{target}/{int(pd.Timestamp.now().timestamp())}.json"
                path = self.hdfs.write_json(prim, rel)
            return {"ok": True, "path": path}
        except Exception as e:
            return {"ok": False, "error": str(e)}

    def update(self, target: str, query: Dict[str, Any], update_doc: Dict[str, Any]) -> Dict[str, Any]:
        # HDFS is file-store: no record-level update. Return unsupported.
        return {"ok": False, "error": "update not supported for HDFS (use rewrite)"}


class MongoStorageBackend(StorageBackend):
    """Adapter to expose MongoWriter as StorageBackend and provide DB/collection operations."""

    def __init__(self, mongo_writer: MongoWriter, client_kwargs: Optional[Dict] = None):
        self.mongo = mongo_writer
        self.client_kwargs = client_kwargs or {}

    def _client(self):
        return MongoClient(self.mongo.uri, **self.client_kwargs)

    def save(self, dataset_name: str, data: Any, fmt: str = "json") -> Dict[str, Any]:
        # map dataset_name -> collection by default
        try:
            docs = _to_primitive(data)
            docs_to_insert = docs if isinstance(docs, list) else [docs]
            client = self._client()
            try:
                db = client[self.mongo.database]
                coll = db[dataset_name] if dataset_name else db[self.mongo.collection]
                res = coll.insert_many(docs_to_insert)
                return {"ok": True, "inserted_count": len(res.inserted_ids), "collection": coll.name}
            finally:
                client.close()
        except Exception as e:
            logger.exception("Mongo save failed")
            return {"ok": False, "error": str(e)}

    def create_database(self, name: str) -> Dict[str, Any]:
        try:
            client = self._client()
            try:
                # creating a DB in Mongo is implicit; create a dummy collection then drop it
                db = client[name]
                db.create_collection("._init_collection")
                db["._init_collection"].drop()
                return {"ok": True, "database": name}
            finally:
                client.close()
        except Exception as e:
            return {"ok": False, "error": str(e)}

    def delete_database(self, name: str) -> Dict[str, Any]:
        try:
            client = self._client()
            try:
                client.drop_database(name)
                return {"ok": True, "database": name}
            finally:
                client.close()
        except Exception as e:
            return {"ok": False, "error": str(e)}

    def create_schema(self, name: str, schema: Optional[Dict] = None) -> Dict[str, Any]:
        """
        Create or update a MongoDB collection schema using JSON Schema validation.
        Works for both new and existing collections.
        """
        try:
            client = self._client()
            db = client[self.mongo.database]

            # If schema is provided, wrap it under $jsonSchema
            validator = {"$jsonSchema": schema} if schema else {}

            # --- CASE 1: Collection already exists → use collMod ---
            if name in db.list_collection_names():
                if schema:
                    try:
                        db.command({
                            "collMod": name,
                            "validator": validator,
                            "validationLevel": "strict"
                        })
                    except Exception as e:
                        return {
                            "ok": False,
                            "collection": name,
                            "error": f"collMod failed: {str(e)}"
                        }
                return {"ok": True, "collection": name, "action": "updated"}

            # --- CASE 2: Collection does NOT exist → create with validator ---
            create_cmd = {"validator": validator, "validationLevel": "strict"} if schema else {}

            try:
                db.create_collection(name, **create_cmd)
            except Exception as e:
                return {
                    "ok": False,
                    "collection": name,
                    "error": f"create_collection failed: {str(e)}"
                }

            return {"ok": True, "collection": name, "action": "created"}

        except Exception as e:
            return {"ok": False, "error": str(e)}

        finally:
            try:
                client.close()
            except:
                pass

    def rename_schema(self, old_name: str, new_name: str) -> Dict[str, Any]:
        # Renaming DB is not supported directly in Mongo; return explanatory error
        return {"ok": False, "error": "rename_schema not supported for MongoDB; use dump/restore or new DB + copy"}

    def create_table(self, name: str, schema: Optional[Dict] = None) -> Dict[str, Any]:
        # table == collection
        return self.create_schema(name, schema)

    def truncate_table(self, name: str) -> Dict[str, Any]:
        try:
            client = self._client()
            try:
                db = client[self.mongo.database]
                coll = db[name]
                res = coll.delete_many({})
                return {"ok": True, "deleted_count": res.deleted_count, "collection": name}
            finally:
                client.close()
        except Exception as e:
            return {"ok": False, "error": str(e)}
    def find_table(self, name: str) -> Dict[str, Any]:
        try:
            client = self._client()
            try:
                db = client[self.mongo.database]
                coll = db[name]
                res = coll.find({})
                list_res = list(res)
                return {"ok": True, "data": list_res, "collection": name}
            finally:
                client.close()
        except Exception as e:
            return {"ok": False, "error": str(e)}

    def delete_table(self, name: str) -> Dict[str, Any]:
        try:
            client = self._client()
            try:
                db = client[self.mongo.database]
                db.drop_collection(name)
                return {"ok": True, "collection": name}
            finally:
                client.close()
        except Exception as e:
            return {"ok": False, "error": str(e)}

    def rename_table(self, old_name: str, new_name: str) -> Dict[str, Any]:
        try:
            client = self._client()
            try:
                db = client[self.mongo.database]
                db[old_name].rename(new_name)
                return {"ok": True, "from": old_name, "to": new_name}
            finally:
                client.close()
        except Exception as e:
            return {"ok": False, "error": str(e)}

    def insert(self, target: str, data: Any) -> Dict[str, Any]:
        try:
            client = self._client()
            try:
                db = client[self.mongo.database]
                coll = db[target] if target else db[self.mongo.collection]
                docs = _to_primitive(data)
                docs_to_insert = docs if isinstance(docs, list) else [docs]
                res = coll.insert_many(docs_to_insert)
                return {"ok": True, "inserted_count": len(res.inserted_ids), "collection": coll.name}
            finally:
                client.close()
        except Exception as e:
            return {"ok": False, "error": str(e)}

    def update(self, target: str, query: Dict[str, Any], update_doc: Dict[str, Any]) -> Dict[str, Any]:
        try:
            client = self._client()
            try:
                db = client[self.mongo.database]
                coll = db[target] if target else db[self.mongo.collection]
                res = coll.update_many(query, update_doc)
                return {"ok": True, "matched_count": res.matched_count, "modified_count": res.modified_count, "collection": coll.name}
            finally:
                client.close()
        except Exception as e:
            return {"ok": False, "error": str(e)}


# ---------------------------
# Orchestrator (composition, single responsibility)
# ---------------------------
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