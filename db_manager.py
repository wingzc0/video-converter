import os
import sqlite3
import threading
from contextlib import contextmanager
from dotenv import load_dotenv

import mysql.connector
from mysql.connector import pooling

from sql_dialect import SqlDialect, create_dialect

load_dotenv()


class DatabaseManager:
    """資料庫管理器，支援 MariaDB 與 SQLite 雙後端。

    透過環境變數 DB_TYPE 選擇後端（預設 mariadb）：
    - DB_TYPE=mariadb：使用 mysql.connector connection pool；
      需設定 DB_HOST / DB_PORT / DB_USER / DB_PASSWORD / DB_NAME。
    - DB_TYPE=sqlite：使用 sqlite3，以 threading.local 為每個執行緒
      維護各自的連線；需設定 DB_PATH（預設 ./data/converter.db）。

    公開介面（兩種後端行為一致）：
    - execute_query(query, params, fetch)
    - execute_transaction(queries)
    - get_connection() — context manager
    - get_cursor(dictionary) — context manager
    - health_check()
    - db_type — 'mariadb' 或 'sqlite'
    """

    def __init__(self):
        self.pool = None             # MariaDB 連接池（延遲初始化）
        self._local = threading.local()  # SQLite per-thread 連線
        self._db_type = None         # 快取，避免重複讀取 env
        self._dialect = None         # 快取，避免重複建立 SqlDialect 實例

    # ------------------------------------------------------------------
    # Backend selection
    # ------------------------------------------------------------------

    @property
    def db_type(self):
        """回傳目前使用的資料庫後端（'mariadb' 或 'sqlite'）"""
        if self._db_type is None:
            self._db_type = os.getenv('DB_TYPE', 'mariadb').lower()
        return self._db_type

    @property
    def dialect(self) -> SqlDialect:
        """回傳目前後端對應的 SqlDialect 實例（延遲初始化，單次建立後快取）。

        透過 sql_dialect.create_dialect() 工廠函式建立；
        新增後端只需在 sql_dialect._DIALECTS 登錄，無需修改此類別。
        """
        if self._dialect is None:
            self._dialect = create_dialect(self.db_type)
        return self._dialect

    # ------------------------------------------------------------------
    # Query translation — delegated to SqlDialect
    # ------------------------------------------------------------------

    # ------------------------------------------------------------------
    # MariaDB backend — connection pool
    # ------------------------------------------------------------------

    def _init_pool(self):
        """初始化 MariaDB 連接池（首次呼叫 get_connection() 時觸發）。

        連接池延遲初始化：模組載入時不嘗試連線，首次 get_connection()
        時才建立。這讓單元測試可以在不依賴真實 DB 的情況下 import 此模組。
        """
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
                # pool_size=5：掃描、處理、API 伺服器各自需要連線，
                # 5 個足夠同時服務多執行緒而不超出資料庫上限
                'pool_size': 5,
                'pool_reset_session': True,
                # autocommit=False：所有操作需明確 commit，確保原子性；
                # execute_query 執行後立即 commit，所以不適合用來執行
                # 需跨語句的 SELECT FOR UPDATE
                'autocommit': False,
            }
            self.pool = pooling.MySQLConnectionPool(**db_config)
            print("Database connection pool initialized")
        except mysql.connector.Error as err:
            print(f"Error initializing connection pool: {err}")
            raise

    @contextmanager
    def _mariadb_connection(self):
        """取得 MariaDB 連線（內部用，首次呼叫時觸發連接池初始化）"""
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

    # ------------------------------------------------------------------
    # SQLite backend — per-thread connection
    # ------------------------------------------------------------------

    def _get_sqlite_conn(self):
        """取得目前執行緒的 SQLite 連線（延遲初始化）。

        使用 threading.local() 確保每個執行緒有各自的連線，避免跨執行
        緒共享連線導致的線程安全問題。WAL 模式允許多個讀者同時讀取，
        適合 daemon 多執行緒環境。
        """
        if not getattr(self._local, 'conn', None):
            db_path = os.getenv('DB_PATH', './data/converter.db')
            if db_path != ':memory:':
                db_dir = os.path.dirname(os.path.abspath(db_path))
                os.makedirs(db_dir, exist_ok=True)
            conn = sqlite3.connect(db_path)
            # 讓查詢結果支援 dict 風格存取，與 MariaDB 行為一致
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA foreign_keys = ON")
            # WAL 模式：允許多個讀者與一個寫者同時操作，適合多執行緒 daemon
            conn.execute("PRAGMA journal_mode = WAL")
            conn.execute("PRAGMA synchronous = NORMAL")
            # 多 process 架構下（scan_daemon + process_daemon 同時寫入），
            # 預設 busy_timeout=0 會讓搶不到寫鎖的 process 立即拿到 SQLITE_BUSY。
            # 設為 5000ms 讓 SQLite 自動 retry，避免非必要的寫入失敗。
            conn.execute("PRAGMA busy_timeout = 5000")
            self._local.conn = conn
            print("SQLite connection initialized")
        return self._local.conn

    # ------------------------------------------------------------------
    # Unified public context managers
    # ------------------------------------------------------------------

    @contextmanager
    def get_connection(self):
        """取得連接上下文管理器。

        MariaDB：從連接池取得連線，用後歸還。
        SQLite：回傳目前執行緒的持久連線（不關閉）。
        """
        if self.db_type == 'sqlite':
            yield self._get_sqlite_conn()
        else:
            with self._mariadb_connection() as conn:
                yield conn

    @contextmanager
    def get_cursor(self, dictionary=True):
        """取得游標上下文管理器。

        Args:
            dictionary: True → 回傳 dict 風格游標（MariaDB）或 Row 游標（SQLite）；
                        False → 回傳 tuple 游標。
        """
        with self.get_connection() as conn:
            if self.db_type == 'sqlite':
                if not dictionary:
                    conn.row_factory = None
                cursor = conn.cursor()
                try:
                    yield cursor, conn
                finally:
                    cursor.close()
                    # 還原 row_factory
                    if not dictionary:
                        conn.row_factory = sqlite3.Row
            else:
                cursor = conn.cursor(dictionary=dictionary)
                try:
                    yield cursor, conn
                finally:
                    cursor.close()

    # ------------------------------------------------------------------
    # Unified public API
    # ------------------------------------------------------------------

    def execute_query(self, query, params=None, fetch=False):
        """執行單一 SQL 語句並自動 commit。

        Args:
            query:  SQL 語句，參數使用 %s 佔位符（SQLite 後端自動轉換為 ?）。
            params: 參數元組（可選）。
            fetch:  True → SELECT 查詢，回傳結果列表（list[dict]）；
                    False → INSERT/UPDATE/DELETE，回傳受影響行數（int）。

        Returns:
            list[dict] | int

        Note:
            此方法不得用於 SELECT FOR UPDATE：每次呼叫都會立即 commit，
            行鎖會在 commit 後釋放，無法保護後續的寫入操作。
            跨語句原子操作請改用 execute_transaction()。
        """
        if self.db_type == 'sqlite':
            return self._sqlite_execute_query(query, params, fetch)
        return self._mariadb_execute_query(query, params, fetch)

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
        if self.db_type == 'sqlite':
            return self._sqlite_execute_transaction(queries)
        return self._mariadb_execute_transaction(queries)

    def health_check(self):
        """資料庫健康檢查，回傳 True 表示連線正常"""
        try:
            if self.db_type == 'sqlite':
                conn = self._get_sqlite_conn()
                row = conn.execute("SELECT 1").fetchone()
                return row[0] == 1
            else:
                with self.get_cursor(dictionary=False) as (cursor, conn):
                    cursor.execute("SELECT 1")
                    result = cursor.fetchone()
                    return result[0] == 1
        except Exception as e:
            print(f"Health check failed: {e}")
            return False

    # ------------------------------------------------------------------
    # MariaDB query execution
    # ------------------------------------------------------------------

    def _mariadb_execute_query(self, query, params, fetch):
        with self.get_cursor(dictionary=True) as (cursor, conn):
            try:
                cursor.execute(query, params or ())
                if fetch:
                    result = cursor.fetchall()
                    # SELECT 完成後立即 commit，釋放所有隱式鎖
                    conn.commit()
                    return result
                else:
                    conn.commit()
                    return cursor.rowcount
            except mysql.connector.Error as err:
                conn.rollback()
                print(f"Query execution error: {err}")
                raise

    def _mariadb_execute_transaction(self, queries):
        with self._mariadb_connection() as conn:
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

    # ------------------------------------------------------------------
    # SQLite query execution
    # ------------------------------------------------------------------

    def _sqlite_execute_query(self, query, params, fetch):
        conn = self._get_sqlite_conn()
        translated = self.dialect.translate_query(query)
        try:
            cursor = conn.execute(translated, params or ())
            if fetch:
                rows = cursor.fetchall()
                conn.commit()
                return [dict(row) for row in rows]
            else:
                conn.commit()
                return cursor.rowcount
        except Exception as err:
            conn.rollback()
            print(f"Query execution error: {err}")
            raise

    def _sqlite_execute_transaction(self, queries):
        conn = self._get_sqlite_conn()
        try:
            rowcounts = []
            for query, params in queries:
                translated = self.dialect.translate_query(query)
                cursor = conn.execute(translated, params or ())
                rowcounts.append(cursor.rowcount)
            conn.commit()
            return rowcounts
        except Exception as err:
            conn.rollback()
            print(f"Transaction error: {err}")
            raise


# 全域資料庫管理器實例
db_manager = DatabaseManager()
