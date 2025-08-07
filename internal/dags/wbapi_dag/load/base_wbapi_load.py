from cmd.load_config import load_config
from src.logger import FastLogger
from internal.dags.load.wbapi_load import DatabaseConfig, SecurityManager
from cmd.load_config import load_config


class BaseDBLoader(DatabaseConfig, SecurityManager):
    """
    Lớp cơ sở cho các tác vụ tải dữ liệu vào cơ sở dữ liệu.
    Kế thừa từ DatabaseConfig để quản lý cấu hình/kết nối và SecurityManager để quản lý bảo mật/quyền truy cập.
    """

    def __init__(self, cursor, connection, logger=None, config_path: str = "config/database.yaml"):
        DatabaseConfig.__init__(self, config_path)
        SecurityManager.__init__(self, db_session=None)  # db_session có thể truyền vào nếu dùng SQLAlchemy
        self.cursor = cursor
        self.connection = connection
        self.logger = FastLogger(load_config()).get_logger()

    def execute(self, query, params=None):
        """
        Thực thi câu lệnh SQL (insert, update, delete, create, ...).
        """
        try:
            self.cursor.execute(query, params or {})
            self.logger.info(f"Executed query: {query}")
        except Exception as e:
            self.logger.error(f"Query failed: {e}")
            raise

    def executemany(self, query, param_list):
        """
        Thực thi nhiều câu lệnh cùng lúc (bulk insert/update).
        """
        try:
            self.cursor.executemany(query, param_list)
            self.logger.info(f"Executed many: {query}")
        except Exception as e:
            self.logger.error(f"Executemany failed: {e}")
            raise

    def commit(self):
        """
        Commit transaction.
        """
        try:
            self.connection.commit()
            self.logger.info("Transaction committed.")
        except Exception as e:
            self.logger.error(f"Commit failed: {e}")
            raise

    def rollback(self):
        """
        Rollback transaction.
        """
        try:
            self.connection.rollback()
            self.logger.info("Transaction rolled back.")
        except Exception as e:
            self.logger.error(f"Rollback failed: {e}")
            raise

    def create_or_replace_database(self, db_name):
        """
        Tạo database nếu chưa có.
        """
        try:
            self.execute(f"CREATE DATABASE IF NOT EXISTS {db_name}")
            self.logger.info(f"Database {db_name} created or already exists.")
        except Exception as e:
            self.logger.error(f"Create database failed: {e}")
            raise

    def create_or_replace_table(self, create_table_sql):
        """
        Tạo bảng nếu chưa có.
        """
        try:
            self.execute(create_table_sql)
        except Exception as e:
            self.logger.error(f"Create table failed: {e}")
            raise

    def insert_table(self, insert_sql, params):
        """
        Insert một bản ghi vào bảng.
        """
        try:
            self.execute(insert_sql, params)
            self.commit()
        except Exception as e:
            self.logger.error(f"Insert failed: {e}")
            self.rollback()
            raise

    def delete_table(self, table_name:str, where_clause=""):
        """
        Xóa bản ghi trong bảng với điều kiện where_clause.
        """
        if where_clause:
            where_clause = f"WHERE {where_clause}"
        else:
            where_clause = ""
        try:
            self.execute(f"DELETE FROM {table_name} {where_clause}")
            self.commit()
        except Exception as e:
            self.logger.error(f"Delete failed: {e}")
            self.rollback()
            raise
 

    def truncate_table(self, table_name):
        """
        Xóa toàn bộ dữ liệu trong bảng.
        """
        try:
            self.execute(f"TRUNCATE TABLE {table_name}")
            self.commit()
        except Exception as e:
            self.logger.error(f"Truncate failed: {e}")
            self.rollback()
            raise


    def grant_privileges(self, user, privileges, db_table):
        """
        Cấp quyền cho user trên bảng/database.
        """
        if isinstance(privileges, list):
            privileges = ', '.join(privileges)
        elif isinstance(privileges, str):
            privileges = privileges.replace(',', ', ')
        else:
            self.logger.error("Privileges must be a list or a comma-separated string.")
            # raise ValueError("Privileges must be a list or a comma-separated string.")
            return
        try:
            sql = f"GRANT {privileges} ON {db_table} TO '{user}'"
            self.execute(sql)
            self.logger.info(f"Granted privileges {privileges} to user {user} on {db_table}.")
        except Exception as e:
            self.logger.error(f"Grant privileges failed: {e}")
            raise

    def revoke_privileges(self, user, privileges, db_table):
        """
        Thu hồi quyền của user trên bảng/database.
        """
        if isinstance(privileges, list):
            privileges = ', '.join(privileges)
        elif isinstance(privileges, str):
            privileges = privileges.replace(',', ', ')
        else:
            self.logger.error("Privileges must be a list or a comma-separated string.")
            # raise ValueError("Privileges must be a list or a comma-separated string.")
            return
        try:
            sql = f"REVOKE {privileges} ON {db_table} FROM '{user}'"
            self.execute(sql)
            self.logger.info(f"Revoked privileges {privileges} from user {user} on {db_table}.")
        except Exception as e:
            self.logger.error(f"Revoke privileges failed: {e}")
            raise