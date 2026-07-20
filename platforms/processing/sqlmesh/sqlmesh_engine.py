import os
import logging
from typing import Optional, List, Dict, Any
from sqlmesh.core.context import Context
from dataclasses import dataclass, field

@dataclass
class SqlMeshConfig:
    """Cấu hình cho SQLMesh Engine"""
    project_path: str = "sqlmesh"   # Thư mục chứa config.yaml của SQLMesh
    gateway: str = "local_duckdb"


class SqlMeshEngine:
    """
    Lớp quản lý engine xử lý biến đổi dữ liệu bằng SQLMesh + DuckDB.

    Cung cấp các hàm lập lịch, kiểm tra và thực thi model programmatically.

    S3/MinIO config được inject sau khi Context khởi tạo bởi _configure_s3()
    vì pre_statements không khả dụng ở mọi phiên bản SQLMesh.
    """
    def __init__(self, config: SqlMeshConfig, logger: Optional[logging.Logger] = None):
        self.config = config
        self.logger = logger or logging.getLogger(__name__)
        self.context = self._init_context()
        # Inject S3/MinIO credentials vào DuckDB connection của SQLMesh Context
        self._configure_s3()

    # ------------------------------------------------------------------
    # Khởi tạo Context
    # ------------------------------------------------------------------

    def _init_context(self) -> Context:
        """Khởi tạo SQLMesh Context từ thư mục project."""
        self.logger.info(
            f"🔄 Khởi tạo SQLMesh Context tại: {os.path.abspath(self.config.project_path)}"
        )
        abs_path = os.path.abspath(self.config.project_path)
        return Context(paths=abs_path, gateway=self.config.gateway)

    # ------------------------------------------------------------------
    # S3/MinIO injection — thay thế pre_statements trong config.yaml
    # ------------------------------------------------------------------

    def _configure_s3(self) -> None:
        """Inject httpfs + S3/MinIO settings vào DuckDB connection của SQLMesh.

        Lý do dùng Python thay vì pre_statements trong config.yaml:
          - SQLMesh ≥ 0.130 hỗ trợ pre_statements trong DuckDB connection config.
          - Các phiên bản cũ hơn báo "Extra inputs are not permitted" khi gặp
            trường pre_statements trong config.yaml (Pydantic strict validation).
          - Dùng Python injection đảm bảo hoạt động trên mọi version SQLMesh.

        Cách tiếp cận:
          SQLMesh Context expose internal engine adapter qua context._engine_adapter.
          Engine adapter wrap DuckDB connection. Ta dùng adapter.execute() để chạy
          các lệnh SET s3_* — đây là DuckDB SQL thuần, không phụ thuộc SQLMesh version.

        S3_ENDPOINT env var đã được strip http:// prefix bởi deploy_full_pipeline.py
        (os.environ["S3_ENDPOINT"] = "HOST:PORT" trước khi SqlMeshEngine được tạo).
        """
        # Đọc credentials từ environment (đã được set bởi deploy_full_pipeline.py)
        s3_endpoint = os.getenv("S3_ENDPOINT", "localhost:9000")
        s3_key      = os.getenv("AWS_ACCESS_KEY_ID", "minioadmin")
        s3_secret   = os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin_secure_123@#")

        # Đảm bảo endpoint không có http:// prefix (double-strip để an toàn)
        s3_endpoint = s3_endpoint.split("://")[-1]

        sqls = [
            "INSTALL httpfs; LOAD httpfs;",
            f"SET s3_endpoint='{s3_endpoint}';",
            f"SET s3_access_key_id='{s3_key}';",
            f"SET s3_secret_access_key='{s3_secret}';",
            "SET s3_use_ssl=false;",
            "SET s3_url_style='path';",
            "SET s3_region='us-east-1';",
        ]

        try:
            adapter = self.context._engine_adapter
            for sql in sqls:
                adapter.execute(sql)
            self.logger.info(
                f"✅ SQLMesh S3/MinIO config injected via Python "
                f"(endpoint={s3_endpoint})"
            )
        except Exception as exc:
            # Nếu không lấy được adapter (bất thường), log warning và tiếp tục.
            # Gold sẽ fail sau khi plan() chạy — lỗi sẽ được catch ở GoldProcessor.
            self.logger.warning(
                f"⚠️  Không thể inject S3 config vào SQLMesh adapter: {exc}. "
                "Gold models sẽ không thể đọc silver data từ MinIO."
            )

    # ------------------------------------------------------------------
    # Plan / Run / Audit
    # ------------------------------------------------------------------

    def plan(
        self,
        environment: str = "prod",
        skip_backfill: bool = False,
        empty_backfill: bool = False,
    ) -> Any:
        """Lên kế hoạch và apply ngay (non-interactive).

        auto_apply=True + no_prompts=True đảm bảo plan được apply mà không cần
        người dùng confirm — bắt buộc khi chạy programmatically trong pipeline.

        skip_backfill=True  : bỏ qua backfill hoàn toàn
        empty_backfill=True : tạo table trống (dùng khi silver chưa có data)
        """
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

    def run(
        self,
        environment: str = "prod",
        start: Optional[str] = None,
        end: Optional[str] = None,
    ) -> None:
        """Thực thi pipeline biến đổi dữ liệu (Transform)."""
        self.logger.info(
            f"🚀 Thực thi SQLMesh pipeline. Env: {environment}, Từ {start} đến {end}"
        )
        self.context.run(environment=environment, start=start, end=end)
        self.logger.info("✅ Hoàn tất chạy SQLMesh pipeline.")

    def audit(self, models: Optional[List[str]] = None) -> None:
        """Chạy kiểm tra Data Quality (Audits) trên các bảng đã định nghĩa."""
        self.logger.info(f"🕵️ Chạy Data Quality Audits cho các models: {models or 'ALL'}")
        if models:
            for model in models:
                self.context.audit(model=model)
        else:
            self.context.audit()
