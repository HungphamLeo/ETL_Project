# (Code minh họa - không cần copy vào dự án ngay)
from platforms.processing.sqlmesh.sqlmesh_engine import SqlMeshConfig, SqlMeshEngine
from platforms.processing.sqlmesh.base_sqlmesh_processor import BaseSqlMeshProcessor
from datetime import date

class SilverTransformationProcessor(BaseSqlMeshProcessor):
    def process(self, target_date: str):
        self.logger.info(f"Bắt đầu chuyển đổi dữ liệu tầng Silver cho ngày {target_date}...")
        
        # Chỉ chạy khoảng thời gian mục tiêu để load incremental
        self.execute_models(
            environment="prod",
            start=target_date,
            end=target_date,
            run_audits=True # Tự động chạy DQ tests sau khi insert
        )

# Khởi tạo và chạy:
config = SqlMeshConfig(project_path="sqlmesh", gateway="local_duckdb")
engine = SqlMeshEngine(config)
processor = SilverTransformationProcessor(engine)

processor.process(target_date=date.today().strftime("%Y-%m-%d"))
