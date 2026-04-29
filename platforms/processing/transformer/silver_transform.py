from prefect import task, flow
import subprocess

@task(name="Run SQLMesh Silver Models")
def run_sqlmesh_transform():
    """
    Kích hoạt SQLMesh thông qua command line để chạy các model tầng Silver.
    SQLMesh sẽ tự động biết ngày nào cần chạy (Idempotent).
    """
    print("⚡ Bắt đầu thực thi DuckDB + SQLMesh cho tầng Silver...")
    
    try:
        # Lệnh 'sqlmesh run' sẽ biên dịch và chạy các model
        result = subprocess.run(
            ["sqlmesh", "run", "silver"], 
            check=True, 
            capture_output=True, 
            text=True
        )
        print("✅ SQLMesh Transform Thành công!")
        print(result.stdout)
    except subprocess.CalledProcessError as e:
        print("❌ Lỗi khi chạy SQLMesh:")
        print(e.stderr)
        raise e

@flow(name="Daily Lakehouse Pipeline (Bronze to Silver)", log_prints=True)
def run_end_to_end_pipeline():
    """Luồng tổng kết hợp Phase 2 và Phase 3"""
    print("🚀 BẮT ĐẦU PIPELINE END-TO-END")
    
    # Ở thực tế, bạn sẽ import task từ phase 2 vào đây:
    # raw_data = extract_stock_data()
    # load_to_bronze(raw_data)
    
    # Chạy Transform tầng Silver
    run_sqlmesh_transform()

if __name__ == "__main__":
    run_end_to_end_pipeline()


# HÀNH ĐỘNG CỦA BẠN
# Khởi tạo sqlmesh init duckdb và cấu hình config.yaml.

# Lưu file model .sql vào thư mục models/silver/.

# Chạy câu lệnh sqlmesh plan trong terminal. SQLMesh sẽ phân tích model của bạn và cho biết nó sẽ chạy những gì. Gõ y để apply.

# Chạy Prefect flow bằng lệnh: python phase3_silver_transform.py.