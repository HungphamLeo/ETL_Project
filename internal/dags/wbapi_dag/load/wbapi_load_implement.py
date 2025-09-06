"""
Pipeline Configuration & Security Manager - ETL Pipeline Core Infrastructure
Handles configuration management, security, encryption, and user management
"""

import os
import logging
from typing import Dict, Any, Optional
from datetime import timedelta
from sqlalchemy.orm import Session
from sqlalchemy import text
from cryptography.fernet import Fernet
import bcrypt
from airflow.models import Variable
from cmd_.load_config import load_config
from src.logger import FastLogger


class ETLPipelineConfig:
    """Centralized configuration manager for ETL pipeline"""

    def __init__(self, config_path: Optional[str] = None):
        self.config_path = config_path
        self.config = self._load_config()
        self.logger = self._setup_logger()

    def _load_config(self) -> Dict[str, Any]:
        """Load configuration with Airflow Variable fallback"""
        try:
            # Try Airflow Variable first
            etl_config = Variable.get(
                "etl_pipeline_config", 
                default_var=None, 
                deserialize_json=True
            )
            if etl_config:
                return etl_config
        except Exception:
            pass

        # Fallback to YAML file
        return load_config(self.config_path)

    def _setup_logger(self) -> logging.Logger:
        """Setup logger instance"""
        return FastLogger(self.config).get_logger()

    def get_database_url(self, use_replica: bool = False) -> str:
        """Build database connection URL with SSL support"""
        try:
            db_config = self.config['database']
            
            # Choose primary or replica
            if use_replica and db_config.get('read_replica', {}).get('enabled'):
                replicas = db_config['read_replica']['hosts']
                host_config = replicas[0]  # Simple selection
                host, port = host_config['host'], host_config['port']
            else:
                primary = db_config['primary']
                host, port = primary['host'], primary.get('port', 3306)

            primary = db_config['primary']
            url = (
                f"mysql+pymysql://{primary['username']}:{primary['password']}"
                f"@{host}:{port}/{primary['database']}"
                f"?charset={primary.get('charset', 'utf8mb4')}"
            )

            # Add SSL parameters
            ssl_config = db_config.get('ssl', {})
            if ssl_config.get('enabled'):
                ssl_params = [
                    f"{k}={v}" for k, v in ssl_config.items() 
                    if k != 'enabled' and v
                ]
                if ssl_params:
                    url += "&" + "&".join(ssl_params)

            return url
        except Exception as e:
            self.logger.error(f"Failed to build database URL: {e}")
            raise

    def get_kafka_config(self, consumer_group: Optional[str] = None) -> Dict[str, Any]:
        """Get Kafka configuration for producer/consumer"""
        kafka_config = self.config.get('kafka', {})
        
        config = {
            **kafka_config.get('default_config', {}),
            **kafka_config.get('producer_config', {})
        }
        
        if consumer_group:
            config.update({
                'group_id': consumer_group,
                **kafka_config.get('consumer_config', {})
            })
        
        return config

    def get_spark_config(self) -> Dict[str, Any]:
        """Get Spark configuration with HDFS integration"""
        spark_config = self.config.get('spark', {})
        hdfs_config = self.config.get('hdfs', {})
        
        return {
            'app_name': spark_config.get('app_name', 'ETL_Pipeline'),
            'master': spark_config.get('master', 'local[*]'),
            'packages': spark_config.get('packages', []),
            'conf': {
                **spark_config.get('conf', {}),
                'spark.hadoop.fs.defaultFS': hdfs_config.get('namenode', 'hdfs://localhost:9000'),
            }
        }

    def get_airflow_default_args(self) -> Dict[str, Any]:
        """Get Airflow DAG default arguments"""
        airflow_config = self.config.get('airflow', {}).get('default_args', {})
        
        return {
            'owner': airflow_config.get('owner', 'data-engineering'),
            'depends_on_past': airflow_config.get('depends_on_past', False),
            'email_on_failure': airflow_config.get('email_on_failure', True),
            'email_on_retry': airflow_config.get('email_on_retry', False),
            'retries': airflow_config.get('retries', 1),
            'retry_delay': timedelta(seconds=airflow_config.get('retry_delay_sec', 300)),
            'execution_timeout': timedelta(seconds=airflow_config.get('execution_timeout_sec', 7200)),
        }

    def get_environment(self) -> str:
        """Get current environment"""
        return self.config.get('environment', 'development')

    def is_production(self) -> bool:
        return self.get_environment() == 'production'

    def is_development(self) -> bool:
        return self.get_environment() == 'development'

    def get_monitoring_config(self) -> Dict[str, Any]:
        """Get monitoring and alerting configuration"""
        return self.config.get('monitoring', {})


class DatabaseConfig:
    """Database configuration with encryption support"""

    def __init__(self, config_path: str = "config/database.yaml"):
        self.config_path = config_path
        self.config = load_config(self.config_path)
        self.logger = FastLogger(self.config).get_logger()
        
        # Initialize encryption
        self._encryption_key = self._get_or_create_encryption_key()
        self._cipher_suite = Fernet(self._encryption_key)

    def _get_or_create_encryption_key(self) -> bytes:
        """Get or create encryption key for sensitive data"""
        key_path = self.config.get('encryption_key_path', 'config/encryption.key')
        
        try:
            if os.path.exists(key_path):
                with open(key_path, 'rb') as key_file:
                    return key_file.read()
            else:
                # Generate new key
                key = Fernet.generate_key()
                os.makedirs(os.path.dirname(key_path), exist_ok=True)
                
                with open(key_path, 'wb') as key_file:
                    key_file.write(key)
                
                # Secure file permissions
                os.chmod(key_path, 0o600)
                self.logger.info(f"New encryption key generated at {key_path}")
                return key
                
        except Exception as e:
            self.logger.error(f"Encryption key handling failed: {e}")
            raise

    def encrypt_sensitive_data(self, data: str) -> str:
        """Encrypt sensitive data"""
        if not isinstance(data, str):
            raise ValueError("Data must be a string")
        
        try:
            return self._cipher_suite.encrypt(data.encode()).decode()
        except Exception as e:
            self.logger.error(f"Encryption failed: {e}")
            raise

    def decrypt_sensitive_data(self, encrypted_data: str) -> str:
        """Decrypt sensitive data"""
        if not isinstance(encrypted_data, str):
            raise ValueError("Encrypted data must be a string")
        
        try:
            return self._cipher_suite.decrypt(encrypted_data.encode()).decode()
        except Exception as e:
            self.logger.error(f"Decryption failed: {e}")
            raise

    def get_connection_url(self, use_replica: bool = False) -> str:
        """Generate database connection URL with SSL support"""
        try:
            db_config = self.config['database']
            
            if use_replica and db_config.get('read_replica', {}).get('enabled'):
                replica = db_config['read_replica']['hosts'][0]
                host, port = replica['host'], replica['port']
            else:
                primary = db_config['primary']
                host, port = primary['host'], primary.get('port', 3306)

            primary_config = db_config['primary']
            url = (
                f"mysql+pymysql://{primary_config['username']}:{primary_config['password']}"
                f"@{host}:{port}/{primary_config['database']}"
                f"?charset={primary_config.get('charset', 'utf8mb4')}"
            )

            # SSL configuration
            ssl_config = db_config.get('ssl', {})
            if ssl_config.get('enabled'):
                ssl_params = [
                    f"ssl_ca={ssl_config.get('ca_cert_path')}",
                    f"ssl_cert={ssl_config.get('client_cert_path')}",
                    f"ssl_key={ssl_config.get('client_key_path')}"
                ]
                url += "&" + "&".join(filter(None, ssl_params))

            return url
            
        except Exception as e:
            self.logger.error(f"Failed to build connection URL: {e}")
            raise


class SecurityManager:
    """Security manager for user authentication and authorization"""

    def __init__(self, db_session: Session):
        self.db_session = db_session
        self.logger = FastLogger(load_config()).get_logger()

    def hash_password(self, password: str) -> str:
        """Hash password using bcrypt"""
        try:
            salt = bcrypt.gensalt(rounds=12)
            return bcrypt.hashpw(password.encode('utf-8'), salt).decode('utf-8')
        except Exception as e:
            self.logger.error(f"Password hashing failed: {e}")
            raise

    def verify_password(self, password: str, password_hash: str) -> bool:
        """Verify password against hash"""
        try:
            return bcrypt.checkpw(password.encode('utf-8'), password_hash.encode('utf-8'))
        except Exception as e:
            self.logger.error(f"Password verification failed: {e}")
            return False

    def create_user(self, username: str, password: str, email: str, role_name: str) -> int:
        """Create new user with role assignment"""
        try:
            password_hash = self.hash_password(password)

            # Insert user
            user_query = text("""
                INSERT INTO sec_users (username, password_hash, email, is_active, created_at)
                VALUES (:username, :password_hash, :email, :is_active, NOW())
            """)
            
            self.db_session.execute(user_query, {
                "username": username,
                "password_hash": password_hash,
                "email": email,
                "is_active": True
            })

            # Get user ID
            user_id_query = text("SELECT LAST_INSERT_ID() as user_id")
            user_id = self.db_session.execute(user_id_query).scalar()

            # Assign role
            role_query = text("""
                INSERT INTO sec_user_roles (user_id, role_id, created_at)
                SELECT :user_id, id, NOW() FROM sec_roles WHERE name = :role_name
            """)
            
            self.db_session.execute(role_query, {
                "user_id": user_id,
                "role_name": role_name
            })

            self.db_session.commit()
            self.logger.info(f"User {username} created with role {role_name}")
            
            return user_id

        except Exception as e:
            self.logger.error(f"User creation failed: {e}")
            self.db_session.rollback()
            raise

    def grant_table_privileges(self, username: str, privileges: str, table_name: str):
        """Grant database privileges to user (requires database admin privileges)"""
        try:
            if isinstance(privileges, list):
                privileges = ', '.join(privileges)

            grant_query = text(f"GRANT {privileges} ON {table_name} TO :username")
            self.db_session.execute(grant_query, {"username": username})
            self.db_session.commit()
            
            self.logger.info(f"Granted {privileges} on {table_name} to {username}")
            
        except Exception as e:
            self.logger.error(f"Grant privileges failed: {e}")
            self.db_session.rollback()
            raise


# Singleton pattern for configuration
_pipeline_config: Optional[ETLPipelineConfig] = None

def get_pipeline_config() -> ETLPipelineConfig:
    """Get singleton pipeline configuration instance"""
    global _pipeline_config
    if _pipeline_config is None:
        _pipeline_config = ETLPipelineConfig()
    return _pipeline_config


# Factory function for database config
def create_database_config(config_path: Optional[str] = None) -> DatabaseConfig:
    """Create database configuration instance"""
    return DatabaseConfig(config_path or "config/database.yaml")


# Factory function for security manager
def create_security_manager(db_session: Session) -> SecurityManager:
    """Create security manager instance"""
    return SecurityManager(db_session)