# %%
# ...existing code...
"""
Refactored test runner for extract_cophieu68.
Functions:
 - init_env(): chuẩn bị sys.path
 - init_config(): khởi tạo ETLPipelineConfig
 - build_crawler(): tạo instance crawler an toàn
 - get_test_symbols(): lấy danh sách symbols (config -> market list -> fallback)
 - safe_serialize(): serialize kết quả cho JSON
 - run_symbol_tests(): chạy các test per-symbol
 - run_non_symbol_tests(): chạy các test không cần symbol
 - save_summary(): lưu file JSON tóm tắt
 - main(): chạy toàn bộ
Paste whole cell vào notebook và chạy.
"""


# PROJECT_ROOT = "/mnt/c/Users/Admin/Downloads/Project/Github/ETL_Project"

# def init_env():
#     if PROJECT_ROOT not in sys.path:
#         sys.path.insert(0, PROJECT_ROOT)

# # imports that depend on project path
# init_env()
from internal.dags.cophieu68_dag.extract.extract_cophieu68 import extract_cophieu68
from internal.dags.ETL_Orchestra.main_orchestra_etl import ETLPipelineConfig


config_path = "/mnt/c/Users/Admin/Downloads/Project/Github/ETL_Project/internal/config/web_craw_config/cophieu68_config.yaml"
config = ETLPipelineConfig(config_path=config_path)
pipeline_config = config.config
extract_pipeline_logger = config.etl_extract_logger
# extract_pipeline_logger = config.etl_extract_logger
# extract_pipeline_logger = config.etl_extract_logger
crawler = extract_cophieu68(config.config, config.etl_extract_logger)
# set attributes mà extract_cophieu68.__init__ thường tạo
try:
    crawler.endpoint = crawler.crawler_cfg.get("endpoints", {})
except Exception:
    crawler.endpoint = {}



# %%
market_list  = crawler.crawl_cashflow_statement(symbol="HPG")
print(market_list)

# %%


# %%


# %%



