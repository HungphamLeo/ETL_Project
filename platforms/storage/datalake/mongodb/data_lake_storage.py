from typing import Any, Dict, Optional, Sequence
import logging
from pymongo import MongoClient, errors as pymongo_errors
from storage.base_storage import StorageBackend, to_primitive

class MongoWriter:
    """
    Lightweight Mongo writer. Uses a short-lived client per call to ensure resource cleanup.
    """

    def __init__(self, username: str, 
                        password: str, 
                        authSource: str, 
                        database: str, 
                        host: str, 
                        port: int,
                        collection: str, logger: Optional[logging.Logger] = None, **client_kwargs):
        
        self.username = username        # để trống nếu không bật auth
        self.password= password
        self.authSource= authSource
        self.database = database
        self.collection = collection
        self.uri = f"mongodb://{username}:{password}@{host}:{port}/?authSource={authSource}"
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


class MongoStorageBackend(StorageBackend):
    """Adapter to expose MongoWriter as StorageBackend and provide DB/collection operations."""

    def __init__(self, mongo_writer: MongoWriter, client_kwargs: Optional[Dict] = None, pipeline_logger: Optional[logging.Logger] = None):
        self.mongo = mongo_writer
        self.client_kwargs = client_kwargs or {}
        self.logger = pipeline_logger or logging.getLogger(__name__)

    def _client(self):
        return MongoClient(self.mongo.uri, **self.client_kwargs)

    def save(self, dataset_name: str, data: Any, fmt: str = "json") -> Dict[str, Any]:
        # map dataset_name -> collection by default
        try:
            docs = to_primitive(data)
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
            self.logger.exception("Mongo save failed")
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
            self.logger.exception("Create database failed")
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
            self.logger.exception("Delete database failed")
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
                        self.logger.error("collMod failed: %s", e)
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
                self.logger.error("Create collection failed: %s", e)
                return {
                    "ok": False,
                    "collection": name,
                    "error": f"create_collection failed: {str(e)}"
                }

            return {"ok": True, "collection": name, "action": "created"}

        except Exception as e:
            self.logger.exception("Create schema failed", e)
            return {"ok": False, "error": str(e)}

        finally:
            try:
                client.close()
            except:
                pass
    def rename_schema(self, old_name: str, new_name: str) -> Dict[str, Any]:
        try:
            client = self._client()
            try:
                db = client[self.mongo.database]
                db[old_name].rename(new_name)
                return {"ok": True, "from": old_name, "to": new_name}
            finally:
                client.close()
        except Exception as e:
            self.logger.exception("Rename schema failed", e)
            return {"ok": False, "error": str(e)}

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
            self.logger.exception("Truncate table failed", e)
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
            self.logger.exception("Find table failed", e)
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
            self.logger.exception("Delete table failed", e)
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
            self.logger.exception("Rename table failed", e)
            return {"ok": False, "error": str(e)}

    def insert(self, target: str, data: Any) -> Dict[str, Any]:
        try:
            client = self._client()
            try:
                db = client[self.mongo.database]
                coll = db[target] if target else db[self.mongo.collection]
                docs = to_primitive(data)
                docs_to_insert = docs if isinstance(docs, list) else [docs]
                res = coll.insert_many(docs_to_insert)
                return {"ok": True, "inserted_count": len(res.inserted_ids), "collection": coll.name}
            finally:
                client.close()
        except Exception as e:
            self.logger.exception("Insert failed", e)
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
            self.logger.exception("Update failed", e)
            return {"ok": False, "error": str(e)}
