from abc import ABC, abstractmethod
from typing import Optional, List
from platforms.processing.sqlmesh.sqlmesh_engine import SqlMeshEngine

class BaseSqlMeshProcessor(ABC):
    """
    Lớp cơ sở cho các Job xử lý biến đổi dữ liệu bằng SQLMesh.
    Sử dụng cho Silver Layer (Làm sạch, Deduplicate) và Gold Layer (KPI, Aggregation).
    """
    def __init__(self, engine: SqlMeshEngine):
        self.engine = engine
        self.logger = engine.logger

    @abstractmethod
    def process(self, *args, **kwargs):
        """
        Bắt buộc ghi đè. Cấu hình luồng thực thi cụ thể cho tác vụ Transform.
        """
        pass

    def execute_models(
        self, 
        environment: str = "prod", 
        start: Optional[str] = None, 
        end: Optional[str] = None,
        run_audits: bool = True
    ):
        """Hàm helper để chạy pipelines chuẩn hóa tích hợp audit Data Quality"""
        self.engine.run(environment=environment, start=start, end=end)
        if run_audits:
            self.engine.audit()