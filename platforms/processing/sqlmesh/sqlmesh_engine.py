import os
import logging
from typing import Optional, List, Dict, Any
from sqlmesh.core.context import Context
from dataclasses import dataclass

@dataclass
class SqlMeshConfig:
    """Cấu hình cho SQLMesh Engine"""
    project_path: str = "sqlmesh"  # Thư mục chứa config.yaml của SQLMesh
    gateway: str = "local_duckdb"
    
class SqlMeshEngine:
    """
    Lớp quản lý engine xử lý biến đổi dữ liệu bằng SQLMesh + DuckDB.
    Cung cấp các hàm lập lịch, kiểm tra và thực thi model programmatically.
    """
    def __init__(self, config: SqlMeshConfig, logger: Optional[logging.Logger] = None):
        self.config = config
        self.logger = logger or logging.getLogger(__name__)
        self.context = self._init_context()

    def _init_context(self) -> Context:
        """Khởi tạo SQLMesh Context từ thư mục project"""
        self.logger.info(f"🔄 Khởi tạo SQLMesh Context tại: {os.path.abspath(self.config.project_path)}")
        # Chuyển đổi thành absolute path để tránh lỗi đường dẫn khi chạy từ cron/orchestrator
        abs_path = os.path.abspath(self.config.project_path)
        return Context(paths=abs_path, gateway=self.config.gateway)

    def plan(
        self,
        environment: str = "prod",
        skip_backfill: bool = False,
        empty_backfill: bool = False,
    ) -> Any:
        """Lên kế hoạch và apply ngay (non-interactive).
        auto_apply=True + no_prompts=True đảm bảo plan được apply mà không cần
        người dùng confirm — bắt buộc khi chạy programmatically trong pipeline.

        skip_backfill=True  : bỏ qua backfill hoàn toàn (environment không được populate)
        empty_backfill=True : tạo table trống thay vì query S3
                              (dùng khi silver chưa có data hoặc S3 offline)"""
        self.logger.info(
            f"📋 Tạo và apply Execution Plan cho environment: '{environment}' "
            f"skip_backfill={skip_backfill} empty_backfill={empty_backfill}"
        )
        return self.context.plan(
            environment=environment,
            auto_apply=True,
            no_prompts=True,
            skip_backfill=skip_backfill,
            empty_backfill=empty_backfill,
        )

    def run(self, environment: str = "prod", start: Optional[str] = None, end: Optional[str] = None):
        """Thực thi pipeline biến đổi dữ liệu (Transform)"""
        self.logger.info(f"🚀 Thực thi SQLMesh pipeline. Env: {environment}, Từ {start} đến {end}")
        self.context.run(environment=environment, start=start, end=end)
        self.logger.info("✅ Hoàn tất chạy SQLMesh pipeline.")

    def audit(self, models: Optional[List[str]] = None):
        """Chạy kiểm tra Data Quality (Audits) trên các bảng đã định nghĩa"""
        self.logger.info(f"🕵️ Chạy Data Quality Audits cho các models: {models or 'ALL'}")
        if models:
            for model in models:
                self.context.audit(model=model)
        else:
            self.context.audit()
