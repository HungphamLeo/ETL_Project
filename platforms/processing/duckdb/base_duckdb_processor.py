from abc import ABC, abstractmethod
from platforms.processing.duckdb.duckdb_engine import DuckDBEngine

class BaseDuckDBProcessor(ABC):
    """
    Lớp cơ sở cho các Job xử lý dữ liệu bằng DuckDB.
    Thích hợp cho các tác vụ ad-hoc SQL, analytics, hoặc data bridging.
    """
    def __init__(self, engine: DuckDBEngine):
        self.engine = engine
        self.logger = engine.logger

    @abstractmethod
    def process(self, *args, **kwargs):
        """
        Bắt buộc ghi đè. Triển khai logic xử lý chính sử dụng DuckDB engine.
        """
        pass