#!/bin/bash

# WorldBank ETL Pipeline Deployment Script
# This script deploys the ETL components to Airflow environment

set -e  # Exit on any error

# Configuration
AIRFLOW_HOME=${AIRFLOW_HOME:-"/opt/airflow"}
PROJECT_ROOT=${PROJECT_ROOT:-"/opt/etl_project"}
DAGS_FOLDER="${AIRFLOW_HOME}/dags"
PLUGINS_FOLDER="${AIRFLOW_HOME}/plugins"
CONFIG_FOLDER="${PROJECT_ROOT}/config"

echo "🚀 Starting WorldBank ETL Pipeline Deployment..."

# Function to create directories
create_directories() {
    echo "📁 Creating directory structure..."
    
    mkdir -p "${DAGS_FOLDER}/internal/dags/wbapi_dag/extract"
    mkdir -p "${DAGS_FOLDER}/internal/dags/wbapi_dag/transform" 
    mkdir -p "${DAGS_FOLDER}/internal/dags/wbapi_dag/load"
    mkdir -p "${DAGS_FOLDER}/internal/dags/wbapi_dag/config"
    mkdir -p "${CONFIG_FOLDER}"
    
    echo "✅ Directory structure created"
}

# Function to deploy core components
deploy_components() {
    echo "📦 Deploying ETL components..."
    
    # Deploy the refactored components
    cp "${PROJECT_ROOT}/internal/dags/wbapi_dag/load/db_loader_service.py" \
       "${DAGS_FOLDER}/internal/dags/wbapi_dag/load/"
    
    cp "${PROJECT_ROOT}/internal/dags/wbapi_dag/config/pipeline_config_security.py" \
       "${DAGS_FOLDER}/internal/dags/wbapi_dag/config/"
    
    # Deploy the extract operator (refactored version)
    cp "${PROJECT_ROOT}/internal/dags/wbapi_dag/extract/worldbank_extract_operator.py" \
       "${DAGS_FOLDER}/internal/dags/wbapi_dag/extract/"
    
    # Deploy DAG file
    cp "${PROJECT_ROOT}/dags/worldbank_etl_dag.py" \
       "${DAGS_FOLDER}/"
    
    echo "✅ Core components deployed"
}

# Function to deploy configuration files
deploy_configs() {
    echo "⚙️  Deploying configuration files..."
    
    # Create sample configuration if not exists
    if [ ! -f "${CONFIG_FOLDER}/data_craw_web_config.yaml" ]; then
        cat > "${CONFIG_FOLDER}/data_craw_web_config.yaml" << EOF
# WorldBank ETL Pipeline Configuration
environment: development

database:
  primary:
    host: localhost
    port: 3306
    username: etl_user
    password: etl_password
    database: worldbank_data
    charset: utf8mb4
  
  read_replica:
    enabled: false
    hosts:
      - host: localhost
        port: 3307

  ssl:
    enabled: false
    ca_cert_path: ""
    client_cert_path: ""
    client_key_path: ""

encryption_key_path: config/encryption.key

kafka:
  bootstrap_servers: "localhost:9092"
  default_producer_config:
    acks: all
    retries: 3
  default_consumer_config:
    auto_offset_reset: earliest

spark:
  app_name: "WorldBank ETL"
  master: "local[*]"
  packages:
    - "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0"
  conf:
    "spark.sql.adaptive.enabled": "true"
    "spark.sql.adaptive.coalescePartitions.enabled": "true"

hdfs:
  namenode: "hdfs://localhost:9000"
  data_dir: "/data/worldbank"

airflow:
  default_args:
    owner: data-engineering
    depends_on_past: false
    email_on_failure: true
    email_on_retry: false
    retries: 2
    retry_delay_sec: 300
    execution_timeout_sec: 7200

monitoring:
  enabled: true
  metrics_endpoint: "http://localhost:9090/metrics"
  alert_email: "data-team@company.com"
EOF
    fi
    
    # Create database configuration
    if [ ! -f "${CONFIG_FOLDER}/database.yaml" ]; then
        cat > "${CONFIG_FOLDER}/database.yaml" << EOF
database:
  primary:
    host: localhost
    port: 3306
    username: etl_user
    password: etl_password
    database: worldbank_data
    charset: utf8mb4

  read_replica:
    enabled: false
    hosts:
      - host: localhost
        port: 3307

  ssl:
    enabled: false

encryption_key_path: config/encryption.key
EOF
    fi
    
    echo "✅ Configuration files deployed"
}

# Function to set up Python dependencies
setup_dependencies() {
    echo "🐍 Setting up Python dependencies..."
    
    # Create requirements.txt if not exists
    if [ ! -f "${PROJECT_ROOT}/requirements.txt" ]; then
        cat > "${PROJECT_ROOT}/requirements.txt" << EOF
# WorldBank ETL Pipeline Dependencies
apache-airflow>=2.5.0
pandas>=1.5.0
numpy>=1.21.0
pymysql>=1.0.0
sqlalchemy>=1.4.0
cryptography>=3.4.0
bcrypt>=3.2.0
PyYAML>=6.0
requests>=2.28.0
kafka-python>=2.0.0
pyspark>=3.3.0
hdfs>=2.6.0
wbgapi>=1.0.0
fastapi>=0.85.0
uvicorn>=0.18.0
EOF
    fi
    
    # Install dependencies
    pip install -r "${PROJECT_ROOT}/requirements.txt"
    
    echo "✅ Dependencies installed"
}

# Function to create database tables
setup_database() {
    echo "🗄️  Setting up database..."
    
    # Create database setup script
    cat > "${PROJECT_ROOT}/scripts/setup_database.sql" << EOF
-- WorldBank ETL Database Setup
CREATE DATABASE IF NOT EXISTS worldbank_data 
CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

USE worldbank_data;

-- Security tables
CREATE TABLE IF NOT EXISTS sec_roles (
    id INT PRIMARY KEY AUTO_INCREMENT,
    name VARCHAR(50) UNIQUE NOT NULL,
    description TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS sec_users (
    id INT PRIMARY KEY AUTO_INCREMENT,
    username VARCHAR(50) UNIQUE NOT NULL,
    password_hash VARCHAR(255) NOT NULL,
    email VARCHAR(100) UNIQUE NOT NULL,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS sec_user_roles (
    id INT PRIMARY KEY AUTO_INCREMENT,
    user_id INT NOT NULL,
    role_id INT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (user_id) REFERENCES sec_users(id) ON DELETE CASCADE,
    FOREIGN KEY (role_id) REFERENCES sec_roles(id) ON DELETE CASCADE,
    UNIQUE KEY unique_user_role (user_id, role_id)
);

-- Insert default roles
INSERT IGNORE INTO sec_roles (name, description) VALUES 
('admin', 'Full system access'),
('data_engineer', 'ETL pipeline access'),
('analyst', 'Read-only data access');

-- Create ETL user
CREATE USER IF NOT EXISTS 'etl_user'@'%' IDENTIFIED BY 'etl_password';
GRANT SELECT, INSERT, UPDATE, DELETE, CREATE, DROP, INDEX, ALTER 
ON worldbank_data.* TO 'etl_user'@'%';
FLUSH PRIVILEGES;
EOF

    echo "✅ Database setup script created"
    echo "📝 Run the following to setup database:"
    echo "   mysql -u root -p < ${PROJECT_ROOT}/scripts/setup_database.sql"
}

# Function to create systemd service (optional)
create_service() {
    echo "🔧 Creating systemd service..."
    
    cat > "/tmp/worldbank-etl.service" << EOF
[Unit]
Description=WorldBank ETL Pipeline
After=network.target

[Service]
Type=simple
User=airflow
Group=airflow
Environment=AIRFLOW_HOME=${AIRFLOW_HOME}
Environment=PYTHONPATH=${PROJECT_ROOT}
ExecStart=${AIRFLOW_HOME}/bin/airflow scheduler
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
EOF
    
    echo "✅ Service file created at /tmp/worldbank-etl.service"
    echo "📝 To install: sudo mv /tmp/worldbank-etl.service /etc/systemd/system/"
}

# Function to validate deployment
validate_deployment() {
    echo "🔍 Validating deployment..."
    
    # Check if files exist
    required_files=(
        "${DAGS_FOLDER}/worldbank_etl_dag.py"
        "${DAGS_FOLDER}/internal/dags/wbapi_dag/load/db_loader_service.py"
        "${DAGS_FOLDER}/internal/dags/wbapi_dag/config/pipeline_config_security.py"
        "${CONFIG_FOLDER}/data_craw_web_config.yaml"
    )
    
    for file in "${required_files[@]}"; do
        if [ ! -f "$file" ]; then
            echo "❌ Missing file: $file"
            exit 1
        fi
    done
    
    # Test Python import
    cd "${DAGS_FOLDER}"
    python3 -c "
import sys
sys.path.append('${PROJECT_ROOT}')
try:
    from internal.dags.wbapi_dag.load.db_loader_service import DatabaseLoaderService
    from internal.dags.wbapi_dag.config.pipeline_config_security import get_pipeline_config
    print('✅ Python imports successful')
except ImportError as e:
    print(f'❌ Import error: {e}')
    sys.exit(1)
"
    
    echo "✅ Deployment validation successful"
}

# Function to show usage instructions
show_usage() {
    echo "📋 Deployment completed! Next steps:"
    echo ""
    echo "1. Start Airflow services:"
    echo "   airflow db init"
    echo "   airflow users create --username admin --password admin \\"
    echo "     --firstname Admin --lastname User \\"
    echo "     --role Admin --email admin@example.com"
    echo ""
    echo "2. Start Airflow scheduler and webserver:"
    echo "   airflow scheduler &"
    echo "   airflow webserver -p 8080 &"
    echo ""
    echo "3. Access Airflow UI: http://localhost:8080"
    echo "   Username: admin"
    echo "   Password: admin"
    echo ""
    echo "4. Enable the DAG: worldbank_etl_pipeline"
    echo ""
    echo "5. Configure Airflow Variables (optional):"
    echo "   airflow variables set etl_pipeline_config '{\"environment\": \"production\"}'"
    echo ""
    echo "6. Set up monitoring (optional):"
    echo "   - Configure email settings in airflow.cfg"
    echo "   - Set up Prometheus/Grafana for metrics"
    echo ""
    echo "🎉 Happy ETL-ing!"
}

# Main deployment function
main() {
    echo "🔧 WorldBank ETL Pipeline Deployment"
    echo "======================================"
    
    create_directories
    deploy_components
    deploy_configs
    setup_dependencies
    setup_database
    create_service
    validate_deployment
    show_usage
    
    echo ""
    echo "🎯 Deployment completed successfully!"
}

# Check if script is run directly
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    main "$@"
fi