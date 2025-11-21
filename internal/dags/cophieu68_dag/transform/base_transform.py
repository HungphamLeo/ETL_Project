from utils import TableCreator
from internal.dags.cophieu68_dag.load.mongodb.load_datalake_cophieu68 import *
from internal.models.cophieu68_model.transform_models import DATA_WAREHOUSE_SCHEMA as schema_dw
class TransformDatawarehouse:
    """
    Base class – all shared components (Mongo, surrogate repo, table creator).
    """

    def __init__(self, datalake_config, datawarehouse_logger, postgres_client):
        self.datawarehouse_logger = datawarehouse_logger
        self.schema_dw = schema_dw

        # Mongo backend
        self.mongo = MongoStorageBackend(
            MongoLoader(
                username=datalake_config.get("username", ""),
                password=datalake_config.get("password", ""),
                host=datalake_config.get("host", "localhost"),
                authSource=datalake_config.get("authSource", "admin"),
                port=datalake_config.get("port", 27017),
                database=datalake_config.get("database", "ETL_Project"),
            )
        )
        self.repo = MetaSurrogateRepository(postgres_client)

class MetaSurrogateRepository:
    """Lưu surrogate key đã generate vào meta_surrogate_map"""

    def __init__(self, postgres_client):
        self.pg = postgres_client

    def get_or_create(self, natural_key: str, type_name: str, generator: TableCreator):
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