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



class DatabaseConfig:
    """Database configuration with encryption support"""

    def __init__(self, pipeline_config, pipeline_logger):
        self.config = pipeline_config
        self.logger = pipeline_logger
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

    def __init__(self, pipeline_logger, pipeline_config, db_session):
        self.db_session = db_session
        self.config = pipeline_config
        self.logger = pipeline_logger


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
            self.loggers.error(f"User creation failed: {e}")
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


# # Singleton pattern for configuration
# _pipeline_config: Optional[ETLPipelineConfig] = None

# def get_pipeline_config() -> ETLPipelineConfig:
#     """Get singleton pipeline configuration instance"""
#     global _pipeline_config
#     if _pipeline_config is None:
#         _pipeline_config = ETLPipelineConfig()
#     return _pipeline_config


# # Factory function for database config
# def create_database_config(config_path: Optional[str] = None) -> DatabaseConfig:
#     """Create database configuration instance"""
#     return DatabaseConfig(config_path or "config/database.yaml")


# # Factory function for security manager
# def create_security_manager(db_session: Session) -> SecurityManager:
#     """Create security manager instance"""l
#     return SecurityManager(db_session)