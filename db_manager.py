import mysql.connector
from mysql.connector import pooling
import os
from dotenv import load_dotenv
from contextlib import contextmanager
import time

load_dotenv()

class DatabaseManager:
    """MariaDB 連接池管理"""
    
    def __init__(self):
        self.pool = None
        self._init_pool()
    
    def _init_pool(self):
        """初始化連接池"""
        try:
            db_config = {
                'host': os.getenv('DB_HOST'),
                'port': int(os.getenv('DB_PORT', '3306')),
                'user': os.getenv('DB_USER'),
                'password': os.getenv('DB_PASSWORD'),
                'database': os.getenv('DB_NAME'),
                'charset': 'utf8mb4',
                'collation': 'utf8mb4_unicode_ci',
                'pool_name': 'video_conversion_pool',
                'pool_size': 5,
                'pool_reset_session': True,
                'autocommit': False
            }
            
            self.pool = mysql.connector.pooling.MySQLConnectionPool(**db_config)
            print("Database connection pool initialized")
            
        except mysql.connector.Error as err:
            print(f"Error initializing connection pool: {err}")
            raise
    
    @contextmanager
    def get_connection(self):
        """取得連接上下文管理器"""
        conn = None
        try:
            conn = self.pool.get_connection()
            yield conn
        except mysql.connector.Error as err:
            if conn:
                conn.rollback()
            print(f"Database connection error: {err}")
            raise
        finally:
            if conn and conn.is_connected():
                conn.close()
    
    @contextmanager
    def get_cursor(self, dictionary=True):
        """取得游標上下文管理器"""
        with self.get_connection() as conn:
            cursor = conn.cursor(dictionary=dictionary)
            try:
                yield cursor, conn
            finally:
                cursor.close()
    
    def execute_query(self, query, params=None, fetch=False):
        """執行查詢"""
        with self.get_cursor(dictionary=True) as (cursor, conn):
            try:
                cursor.execute(query, params or ())
                if fetch:
                    result = cursor.fetchall()
                    conn.commit()
                    return result
                else:
                    conn.commit()
                    return cursor.rowcount
            except mysql.connector.Error as err:
                conn.rollback()
                print(f"Query execution error: {err}")
                raise
    
    def execute_transaction(self, queries):
        """執行交易"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            try:
                for query, params in queries:
                    cursor.execute(query, params or ())
                conn.commit()
                return True
            except mysql.connector.Error as err:
                conn.rollback()
                print(f"Transaction error: {err}")
                raise
    
    def health_check(self):
        """資料庫健康檢查"""
        try:
            with self.get_cursor(dictionary=False) as (cursor, conn):
                cursor.execute("SELECT 1")
                result = cursor.fetchone()
                return result[0] == 1
        except Exception as e:
            print(f"Health check failed: {e}")
            return False

# 全域資料庫管理器實例
db_manager = DatabaseManager()
