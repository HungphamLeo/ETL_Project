import os
import shutil
import subprocess
import tempfile
import json
import logging
from pathlib import Path
from typing import Any, Dict, Optional, List
import pandas as pd
from storage.base_storage import StorageBackend, to_primitive,_parse_namenode_uri

try:
    from pyarrow import fs as pa_fs
    _HAS_PYARROW = True
except Exception:
    _HAS_PYARROW = False


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
        payload = json.dumps(to_primitive(obj), ensure_ascii=False, indent=2).encode("utf-8")
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


class HDFSStorageBackend(StorageBackend):
    """Adapter to expose HDFSWriter as StorageBackend and provide basic DDL/DML-like ops."""

    def __init__(self, hdfs_writer: HDFSWriter, pipeline_logger: Optional[logging.Logger] = None):
        self.hdfs = hdfs_writer
        self.base = self.hdfs.base_path
        self.logger = pipeline_logger

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
            prim = to_primitive(data)
            if fmt == "parquet":
                df = pd.DataFrame(prim) if isinstance(prim, list) else pd.DataFrame([prim])
                rel = f"{dataset_name}/{ts}.parquet"
                path = self.hdfs.write_parquet(df, rel)
            else:
                rel = f"{dataset_name}/{ts}.json"
                path = self.hdfs.write_json(prim, rel)
            return {"ok": True, "path": path}
        except Exception as e:
            self.logger.exception("HDFS save failed")
            return {"ok": False, "error": str(e)}

    def create_database(self, name: str) -> Dict[str, Any]:
        try:
            p = self._hdfs_path(name)
            # mkdir -p
            self._run_hdfs_cmd(["-mkdir", "-p", p])
            return {"ok": True, "path": p}
        except Exception as e:
            self.logger.error("Create database failed: %s", e)
            return {"ok": False, "error": str(e)}
        
    def delete_database(self, name: str) -> Dict[str, Any]:
        try:
            p = self._hdfs_path(name)
            self._run_hdfs_cmd(["-rm", "-r", "-f", p])
            return {"ok": True, "path": p}
        except Exception as e:
            self.logger.error("Delete database failed: %s", e)
            return {"ok": False, "error": str(e)}

    def create_schema(self, name: str, schema: Optional[Dict] = None) -> Dict[str, Any]:
        # For HDFS treat schema as a directory under DB
        try:
            p = self._hdfs_path(name)
            self._run_hdfs_cmd(["-mkdir", "-p", p])
            return {"ok": True, "path": p}
        except Exception as e:
            self.logger.error("Create schema failed: %s", e)
            return {"ok": False, "error": str(e)}

    def rename_schema(self, old_name: str, new_name: str) -> Dict[str, Any]:
        try:
            oldp = self._hdfs_path(old_name)
            newp = self._hdfs_path(new_name)
            self._run_hdfs_cmd(["-mv", oldp, newp])
            return {"ok": True, "from": oldp, "to": newp}
        except Exception as e:
            self.logger.error("Rename schema failed: %s", e)
            return {"ok": False, "error": str(e)}

    def create_table(self, name: str, schema: Optional[Dict] = None) -> Dict[str, Any]:
        # create directory or empty file for table
        try:
            p = self._hdfs_path(name)
            self._run_hdfs_cmd(["-mkdir", "-p", p])
            return {"ok": True, "path": p}
        except Exception as e:
            self.logger.error("Create table failed: %s", e)
            return {"ok": False, "error": str(e)}

    def truncate_table(self, name: str) -> Dict[str, Any]:
        try:
            p = self._hdfs_path(name)
            # remove contents and recreate dir
            self._run_hdfs_cmd(["-rm", "-r", "-f", f"{p}/*"])
            return {"ok": True, "table": p}
        except Exception as e:
            self.logger.error("Truncate table failed: %s", e)
            return {"ok": False, "error": str(e)}

    def delete_table(self, name: str) -> Dict[str, Any]:
        try:
            p = self._hdfs_path(name)
            self._run_hdfs_cmd(["-rm", "-r", "-f", p])
            return {"ok": True, "table": p}
        except Exception as e:
            self.logger.error("Delete table failed: %s", e)
            return {"ok": False, "error": str(e)}

    def rename_table(self, old_name: str, new_name: str) -> Dict[str, Any]:
        try:
            oldp = self._hdfs_path(old_name)
            newp = self._hdfs_path(new_name)
            self._run_hdfs_cmd(["-mv", oldp, newp])
            return {"ok": True, "from": oldp, "to": newp}
        except Exception as e:
            self.logger.error("Rename table failed: %s", e)
            return {"ok": False, "error": str(e)}

    def insert(self, target: str, data: Any) -> Dict[str, Any]:
        # Write data under target path; target considered as directory/filename prefix
        try:
            prim = to_primitive(data)
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
            self.logger.exception("HDFS insert failed")
            return {"ok": False, "error": str(e)}

    def update(self, target: str, query: Dict[str, Any], update_doc: Dict[str, Any]) -> Dict[str, Any]:
        # HDFS is file-store: no record-level update. Return unsupported.
        return {"ok": False, "error": "update not supported for HDFS (use rewrite)"}