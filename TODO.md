# ETL Project – MongoDB → Delta Lake Migration TODO

## Progress Tracker

### Nhóm 1 – Delta Lake Storage Backend
- [ ] `platforms/storage/deltalake/delta_schema_registry.py` – Bronze/Silver/Gold schema definitions
- [ ] `platforms/storage/deltalake/delta_lake_storage.py` – StorageBackend implementation (PySpark + Delta)

### Nhóm 2 – Delta Lake Loader (cophieu68)
- [ ] `platforms/ingestion/cophieu68/load/load_datalake_delta_cophieu68.py` – DeltaLoader replacing MongoLoader

### Nhóm 3 – Config Update
- [ ] `platforms/processing/prefect/config/cophieu68_config.yaml` – add `storage.deltalake` section
- [ ] `requirements_common.txt` – add delta-spark, pyspark

### Nhóm 4 – Subsystem Implementations
- [ ] `platforms/processing/transformer/data_cleansing.py` – Subsystem 1, 4, 5 (Profiling + Cleansing + Error Event)
- [ ] `platforms/processing/transformer/deduplication.py` – Subsystem 7
- [ ] `platforms/processing/transformer/scd_manager.py` – Subsystem 9 (SCD Type 1 & 2)
- [ ] `platforms/processing/transformer/surrogate_key_generator.py` – Subsystem 10
- [ ] `platforms/processing/transformer/metadata_repository.py` – Subsystem 34 (Lineage + Metadata)

### Nhóm 5 – Migration & Documentation
- [ ] `docs/mongodb_to_deltalake_migration.md` – Architecture Decision Record + Migration Phases
- [ ] `platforms/ingestion/cophieu68/load/migrate_mongo_to_delta.py` – Backfill script

---

## Status Legend
- [ ] Pending
- [x] Done
- [~] In Progress
