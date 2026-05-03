-- Tạo user hungpham với password
CREATE USER hungpham WITH PASSWORD 'hungpham@123';

-- Grant tất cả quyền trên database etl_project
GRANT ALL PRIVILEGES ON DATABASE etl_project TO hungpham;

-- Tạo schema private và gán quyền cho hungpham
CREATE SCHEMA IF NOT EXISTS private AUTHORIZATION hungpham;

