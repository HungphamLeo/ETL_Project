import os
import yaml
import hashlib
import secrets
from dataclasses import dataclass, asdict
from typing import Optional, List, Dict, Any, Union, Tuple
from decimal import Decimal
from datetime import datetime, date
from src.logger import FastLogger
from contextlib import contextmanager
import threading
from functools import wraps

import pymysql as mysql
from pymysql.cursors import DictCursor
from sqlalchemy import create_engine, text, MetaData, Table
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.pool import QueuePool
from cryptography.fernet import Fernet
from internal.config.load_config import load_config

logger = FastLogger(load_config()).get_logger()

from pydantic import BaseModel, validator
from marshmallow import Schema, fields, ValidationError

class DatabaseConfig:
    """
    Quản lý cấu hình database, hỗ trợ mã hóa dữ liệu nhạy cảm và sinh connection string an toàn.
    """

    def __init__(self, config_path: str = "config/database.yaml"):
        """
        Khởi tạo DatabaseConfig với đường dẫn file cấu hình.
        """
        self.config_path = config_path
        try:
            self.config = load_config(self.config_path)
        except Exception as e:
            logger.error(f"Failed to load config: {e}")
            raise
        try:
            self._encryption_key = self._get_encryption_key()
            self._cipher_cuite = Fernet(self._encryption_key)
        except Exception as e:
            logger.error(f"Failed to initialize encryption: {e}")
            raise

    def _get_encryption_key(self) -> bytes:
        """
        Lấy hoặc tạo encryption key cho data encryption.
        Returns:
            bytes: encryption key
        """
        key_path = self.config.get('encryption_key_path', 'config/encryption.key')
        try:
            if os.path.exists(key_path):
                with open(key_path, 'rb') as key_file:
                    return key_file.read()
            else:
                key = Fernet.generate_key()
                os.makedirs(os.path.dirname(key_path), exist_ok=True)
                with open(key_path, 'wb') as key_file:
                    key_file.write(key)
                os.chmod(key_path, 0o600)
                return key
        except Exception as e:
            logger.error(f"Error handling encryption key: {e}")
            raise

    def encrypt_sentitive_data(self, data: str) -> str:
        """
        Encrypt sensitive data using Fernet symmetric encryption.
        Args:
            data (str): Dữ liệu cần mã hóa.
        Returns:
            str: Dữ liệu đã được mã hóa.
        Raises:
            ValueError: Nếu data không phải là string.
        """
        try:
            if not isinstance(data, str):
                raise ValueError("Data must be a string")
            return self._cipher_cuite.encrypt(data.encode()).decode()
        except Exception as e:
            logger.error(f"Encryption failed: {e}")
            raise

    def decrypt_sentitive_data(self, encrypted_data: str) -> str:
        """
        Decrypt encrypted data using Fernet symmetric encryption.
        Args:
            encrypted_data (str): Dữ liệu đã mã hóa.
        Returns:
            str: Dữ liệu sau khi giải mã.
        Raises:
            ValueError: Nếu encrypted_data không phải là string.
        """
        try:
            if not isinstance(encrypted_data, str):
                raise ValueError("Encrypted data must be a string")
            return self._cipher_cuite.decrypt(encrypted_data.encode()).decode()
        except Exception as e:
            logger.error(f"Decryption failed: {e}")
            raise

    def get_db_url(self, read_replica: bool = False) -> str:
        """
        Sinh database URL dựa trên cấu hình, hỗ trợ SSL và read replica.
        Args:
            read_replica (bool): Nếu True, lấy thông tin từ read replica.
        Returns:
            str: Database connection string.
        """
        try:
            if read_replica and self.config['database']['read_replica']['enabled']:
                replicas = self.config['database']['read_replica']['hosts']
                replica = replicas[0]  # Simple round-robin
                host = replica['host']
                port = replica['port']
            else:
                host = self.config['database']['primary']['host']
                port = self.config['database']['primary']['port']

            db_config = self.config['database']['primary']

            connection_string = (
                f"mysql+pymysql://{db_config['username']}:{db_config['password']}"
                f"@{host}:{port}/{db_config['database']}"
                f"?charset={db_config['charset']}"
            )

            if self.config['database']['ssl']['enabled']:
                ssl_config = self.config['database']['ssl']
                connection_string += (
                    f"&ssl_ca={ssl_config['ca_cert_path']}"
                    f"&ssl_cert={ssl_config['client_cert_path']}"
                    f"&ssl_key={ssl_config['client_key_path']}"
                )

            return connection_string
        except Exception as e:
            logger.error(f"Failed to build DB URL: {e}")
            raise

class SecurityManager:
    """
    Quản lý bảo mật, hash mật khẩu, xác thực và phân quyền người dùng.
    """

    def __init__(self, db_session: Session):
        """
        Khởi tạo SecurityManager với SQLAlchemy session.
        """
        self.db_session = db_session

    def hash_password(self, password: str) -> str:
        """
        Hash password với bcrypt.
        Args:
            password (str): Mật khẩu gốc.
        Returns:
            str: Hash của mật khẩu.
        """
        try:
            import bcrypt
            salt = bcrypt.gensalt(rounds=12)
            return bcrypt.hashpw(password.encode('utf-8'), salt).decode('utf-8')
        except Exception as e:
            logger.error(f"Hash password failed: {e}")
            raise

    def verify_password(self, password: str, hashed: str) -> bool:
        """
        Kiểm tra mật khẩu nhập vào với hash đã lưu.
        Args:
            password (str): Mật khẩu người dùng nhập.
            hashed (str): Hash đã lưu trong DB.
        Returns:
            bool: True nếu đúng, False nếu sai.
        """
        try:
            import bcrypt
            return bcrypt.checkpw(password.encode('utf-8'), hashed.encode('utf-8'))
        except Exception as e:
            logger.error(f"Verify password failed: {e}")
            return False

    def create_user(self, username: str, password: str, email: str, role_name: str) -> int:
        """
        Tạo user mới và gán role cho user.
        Args:
            username (str): Tên đăng nhập.
            password (str): Mật khẩu.
            email (str): Email.
            role_name (str): Tên role cần gán.
        Returns:
            int: user_id vừa tạo.
        """
        try:
            password_hash = self.hash_password(password)

            user_query = text("""
                INSERT INTO sec_users (username, password_hash, email, is_active)
                VALUES (:username, :password_hash, :email, :is_active)
            """)
            self.db_session.execute(
                user_query,
                {
                    "username": username,
                    "password_hash": password_hash,
                    "email": email,
                    "is_active": True
                }
            )
            self.db_session.commit()

            user_id_query = text("""
                SELECT id FROM sec_users WHERE username = :username
            """)
            user_id = self.db_session.execute(user_id_query, {"username": username}).scalar()

            role_query = text("""
                INSERT INTO sec_user_roles (user_id, role_id)
                SELECT :user_id, id FROM sec_roles WHERE name = :role_name
            """)
            self.db_session.execute(
                role_query,
                {
                    "user_id": user_id,
                    "role_name": role_name
                }
            )
            self.db_session.commit()

            return user_id
        except Exception as e:
            logger.error(f"Create user failed: {e}")
            self.db_session.rollback()
            raise