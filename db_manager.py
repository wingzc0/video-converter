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
        # 連接池延遲初始化：模組載入時不嘗試連線，首次 get_connection() 時才建立。
        # 這讓單元測試可以在不依賴真實 DB 的情況下 import 此模組。

    def _init_pool(self):
        """初始化連接池（首次呼叫 get_connection() 時觸發）"""
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
                # pool_size=5：掃描、處理、API 伺服器各自需要連線，5 個足夠同時服務多執行緒而不超出資料庫上限
                'pool_size': 5,
                'pool_reset_session': True,
                # autocommit=False：所有操作需明確 commit，確保原子性；
                # execute_query 執行後立即 commit，所以不適合用來執行需跨語句的 SELECT FOR UPDATE
                'autocommit': False
            }

            self.pool = mysql.connector.pooling.MySQLConnectionPool(**db_config)
            print("Database connection pool initialized")

        except mysql.connector.Error as err:
            print(f"Error initializing connection pool: {err}")
            raise

    @contextmanager
    def get_connection(self):
        """取得連接上下文管理器（首次呼叫時觸發連接池初始化）"""
        if self.pool is None:
            self._init_pool()
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
        """執行單一 SQL 語句並自動 commit。

        Args:
            query:  SQL 語句，參數使用 %s 佔位符。
            params: 參數元組（可選）。
            fetch:  True → SELECT 查詢，回傳結果列表；
                    False → INSERT/UPDATE/DELETE，回傳受影響行數（int）。

        Returns:
            list[dict] | int: fetch=True 時回傳 fetchall() 結果（dict 列表）；
                              fetch=False 時回傳 cursor.rowcount。

        Note:
            此方法不得用於 SELECT FOR UPDATE：每次呼叫都會立即 commit，
            行鎖會在 commit 後釋放，無法保護後續的寫入操作。
            跨語句原子操作請改用 execute_transaction()。
        """
        with self.get_cursor(dictionary=True) as (cursor, conn):
            try:
                cursor.execute(query, params or ())
                if fetch:
                    result = cursor.fetchall()
                    # SELECT 完成後立即 commit，釋放所有隱式鎖
                    # 重要限制：此方法不得用於 SELECT FOR UPDATE，
                    # 因為 commit 後行鎖會立即釋放，無法保護後續的寫入操作
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
        """在單一交易中依序執行多個 SQL 語句，全部成功才 commit，任一失敗則 rollback。

        Args:
            queries: list of (sql_string, params_tuple) pairs。

        Returns:
            list[int]: 各語句的 rowcount，順序對應 queries。
                       可用第一個元素判斷 UPDATE 是否實際命中資料列。

        Note:
            需要多語句原子操作時（如：先 UPDATE 再 DELETE），應使用此方法而非
            多次呼叫 execute_query()；execute_query() 每次各自 commit，
            無法保證跨語句的原子性。
            此方法不得用於 SELECT FOR UPDATE（commit 後行鎖立即釋放）。
        """
        with self.get_connection() as conn:
            cursor = conn.cursor()
            try:
                rowcounts = []
                for query, params in queries:
                    cursor.execute(query, params or ())
                    rowcounts.append(cursor.rowcount)
                conn.commit()
                return rowcounts
            except mysql.connector.Error as err:
                conn.rollback()
                print(f"Transaction error: {err}")
                raise
            finally:
                cursor.close()
    
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
