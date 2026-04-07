"""
Unit tests for db_manager.py
測試 DatabaseManager 類別的連接池管理、查詢執行和交易功能。
"""
import os
import sys
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch, call

sys.path.insert(0, str(Path(__file__).parent.parent))


class TestDatabaseManager(unittest.TestCase):
    """DatabaseManager 類別測試"""

    def setUp(self):
        """設定測試環境"""
        # 重置環境變數
        self.env_backup = {
            'DB_TYPE': os.getenv('DB_TYPE'),
            'DB_HOST': os.getenv('DB_HOST'),
            'DB_PORT': os.getenv('DB_PORT'),
            'DB_USER': os.getenv('DB_USER'),
            'DB_PASSWORD': os.getenv('DB_PASSWORD'),
            'DB_NAME': os.getenv('DB_NAME'),
            'DB_PATH': os.getenv('DB_PATH'),
        }
        
        # 設定測試用的環境變數
        os.environ['DB_TYPE'] = 'mariadb'
        os.environ['DB_HOST'] = 'localhost'
        os.environ['DB_PORT'] = '3306'
        os.environ['DB_USER'] = 'test_user'
        os.environ['DB_PASSWORD'] = 'test_password'
        os.environ['DB_NAME'] = 'test_db'
        os.environ.pop('DB_PATH', None)

        # 重新載入模組以使用新的環境變數
        if 'db_manager' in sys.modules:
            del sys.modules['db_manager']
        
        from db_manager import DatabaseManager
        self.DatabaseManager = DatabaseManager

    def tearDown(self):
        """還原環境變數"""
        for key, value in self.env_backup.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value
        
        # 清理模組快取
        if 'db_manager' in sys.modules:
            del sys.modules['db_manager']

    @patch('db_manager.pooling.MySQLConnectionPool')
    def test_init_pool_success(self, mock_pool_class):
        """測試連接池初始化成功"""
        mock_pool = MagicMock()
        mock_pool_class.return_value = mock_pool
        
        db_manager = self.DatabaseManager()
        db_manager._init_pool()
        
        # 驗證連接池已建立
        self.assertIsNotNone(db_manager.pool)
        mock_pool_class.assert_called_once()
        
        # 驗證連接池配置
        call_kwargs = mock_pool_class.call_args[1]
        self.assertEqual(call_kwargs['host'], 'localhost')
        self.assertEqual(call_kwargs['port'], 3306)
        self.assertEqual(call_kwargs['user'], 'test_user')
        self.assertEqual(call_kwargs['password'], 'test_password')
        self.assertEqual(call_kwargs['database'], 'test_db')
        self.assertEqual(call_kwargs['pool_name'], 'video_conversion_pool')
        self.assertEqual(call_kwargs['pool_size'], 5)
        self.assertEqual(call_kwargs['charset'], 'utf8mb4')
        self.assertEqual(call_kwargs['autocommit'], False)

    @patch('db_manager.pooling.MySQLConnectionPool')
    def test_init_pool_failure(self, mock_pool_class):
        """測試連接池初始化失敗"""
        import mysql.connector
        
        mock_pool_class.side_effect = mysql.connector.Error("Connection failed")
        
        db_manager = self.DatabaseManager()
        
        with self.assertRaises(mysql.connector.Error):
            db_manager._init_pool()

    @patch('db_manager.pooling.MySQLConnectionPool')
    def test_get_connection_lazy_initialization(self, mock_pool_class):
        """測試連接的延遲初始化（首次呼叫 get_connection 時才建立連接池）"""
        mock_pool = MagicMock()
        mock_connection = MagicMock()
        mock_pool.get_connection.return_value = mock_connection
        mock_pool_class.return_value = mock_pool
        
        db_manager = self.DatabaseManager()
        
        # 初始時 pool 應為 None
        self.assertIsNone(db_manager.pool)
        
        # 呼叫 get_connection 時應觸發 _init_pool
        with db_manager.get_connection() as conn:
            self.assertIsNotNone(db_manager.pool)
            mock_pool_class.assert_called_once()
        
        # 驗證連接已正確關閉
        mock_connection.close.assert_called()

    @patch('db_manager.pooling.MySQLConnectionPool')
    def test_get_connection_rollback_on_error(self, mock_pool_class):
        """測試發生錯誤時自動 rollback"""
        import mysql.connector
        
        mock_pool = MagicMock()
        mock_connection = MagicMock()
        mock_pool.get_connection.return_value = mock_connection
        mock_pool_class.return_value = mock_pool
        
        db_manager = self.DatabaseManager()
        
        # 模擬在 context manager 中拋出錯誤
        with self.assertRaises(mysql.connector.Error):
            with db_manager.get_connection() as conn:
                raise mysql.connector.Error("Test error")
        
        # 驗證 rollback 被呼叫（至少一次）
        mock_connection.rollback.assert_called()

    @patch('db_manager.pooling.MySQLConnectionPool')
    def test_execute_query_fetch_true(self, mock_pool_class):
        """測試 execute_query 與 fetch=True（SELECT 查詢）"""
        mock_pool = MagicMock()
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        
        mock_pool.get_connection.return_value = mock_connection
        mock_connection.cursor.return_value = mock_cursor
        mock_pool_class.return_value = mock_pool
        
        expected_result = [{'id': 1, 'name': 'test'}]
        mock_cursor.fetchall.return_value = expected_result
        
        db_manager = self.DatabaseManager()
        result = db_manager.execute_query("SELECT * FROM test", fetch=True)
        
        # 驗證結果
        self.assertEqual(result, expected_result)
        
        # 驗證 SQL 執行
        mock_cursor.execute.assert_called_once_with("SELECT * FROM test", ())
        mock_cursor.fetchall.assert_called_once()
        mock_connection.commit.assert_called()

    @patch('db_manager.pooling.MySQLConnectionPool')
    def test_execute_query_fetch_false(self, mock_pool_class):
        """測試 execute_query 與 fetch=False（INSERT/UPDATE/DELETE）"""
        mock_pool = MagicMock()
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        
        mock_pool.get_connection.return_value = mock_connection
        mock_connection.cursor.return_value = mock_cursor
        mock_pool_class.return_value = mock_pool
        
        mock_cursor.rowcount = 5
        
        db_manager = self.DatabaseManager()
        result = db_manager.execute_query(
            "UPDATE test SET name=%s WHERE id=%s",
            params=('new_name', 1),
            fetch=False
        )
        
        # 驗證結果
        self.assertEqual(result, 5)
        
        # 驗證 SQL 執行
        mock_cursor.execute.assert_called_once_with(
            "UPDATE test SET name=%s WHERE id=%s",
            ('new_name', 1)
        )
        mock_connection.commit.assert_called()

    @patch('db_manager.pooling.MySQLConnectionPool')
    def test_execute_query_rollback_on_error(self, mock_pool_class):
        """測試 execute_query 發生錯誤時 rollback"""
        import mysql.connector
        
        mock_pool = MagicMock()
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        
        mock_pool.get_connection.return_value = mock_connection
        mock_connection.cursor.return_value = mock_cursor
        mock_pool_class.return_value = mock_pool
        
        mock_cursor.execute.side_effect = mysql.connector.Error("Query failed")
        
        db_manager = self.DatabaseManager()
        
        with self.assertRaises(mysql.connector.Error):
            db_manager.execute_query("INVALID SQL")
        
        # 驗證 rollback 被呼叫（至少一次）
        mock_connection.rollback.assert_called()

    @patch('db_manager.pooling.MySQLConnectionPool')
    def test_execute_transaction_success(self, mock_pool_class):
        """測試 execute_transaction 成功執行多個 SQL"""
        mock_pool = MagicMock()
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        
        mock_pool.get_connection.return_value = mock_connection
        mock_connection.cursor.return_value = mock_cursor
        mock_pool_class.return_value = mock_pool
        
        mock_cursor.rowcount = 1
        
        queries = [
            ("UPDATE test SET status=%s WHERE id=%s", ('pending', 1)),
            ("DELETE FROM locks WHERE task_id=%s", (1,)),
        ]
        
        db_manager = self.DatabaseManager()
        results = db_manager.execute_transaction(queries)
        
        # 驗證結果
        self.assertEqual(results, [1, 1])
        
        # 驗證所有 SQL 都被執行
        self.assertEqual(mock_cursor.execute.call_count, 2)
        mock_connection.commit.assert_called_once()
        mock_connection.rollback.assert_not_called()

    @patch('db_manager.pooling.MySQLConnectionPool')
    def test_execute_transaction_rollback_on_error(self, mock_pool_class):
        """測試 execute_transaction 發生錯誤時 rollback"""
        import mysql.connector
        
        mock_pool = MagicMock()
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        
        mock_pool.get_connection.return_value = mock_connection
        mock_connection.cursor.return_value = mock_cursor
        mock_pool_class.return_value = mock_pool
        
        # 第二個查詢失敗
        mock_cursor.execute.side_effect = [None, mysql.connector.Error("Transaction failed")]
        
        queries = [
            ("UPDATE test SET status=%s WHERE id=%s", ('pending', 1)),
            ("DELETE FROM locks WHERE task_id=%s", (1,)),
        ]
        
        db_manager = self.DatabaseManager()
        
        with self.assertRaises(mysql.connector.Error):
            db_manager.execute_transaction(queries)
        
        # 驗證 rollback 被呼叫（至少一次）
        mock_connection.rollback.assert_called()
        # commit 不應被呼叫
        mock_connection.commit.assert_not_called()

    @patch('db_manager.pooling.MySQLConnectionPool')
    def test_health_check_success(self, mock_pool_class):
        """測試健康檢查成功"""
        mock_pool = MagicMock()
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        
        mock_pool.get_connection.return_value = mock_connection
        mock_connection.cursor.return_value = mock_cursor
        mock_pool_class.return_value = mock_pool
        
        mock_cursor.fetchone.return_value = (1,)
        
        db_manager = self.DatabaseManager()
        result = db_manager.health_check()
        
        self.assertTrue(result)
        mock_cursor.execute.assert_called_with("SELECT 1")

    @patch('db_manager.pooling.MySQLConnectionPool')
    def test_health_check_failure(self, mock_pool_class):
        """測試健康檢查失敗"""
        mock_pool = MagicMock()
        mock_pool.get_connection.side_effect = Exception("Connection failed")
        mock_pool_class.return_value = mock_pool
        
        db_manager = self.DatabaseManager()
        result = db_manager.health_check()
        
        self.assertFalse(result)

    @patch('db_manager.pooling.MySQLConnectionPool')
    def test_get_cursor_dictionary_true(self, mock_pool_class):
        """測試 get_cursor 與 dictionary=True"""
        mock_pool = MagicMock()
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        
        mock_pool.get_connection.return_value = mock_connection
        mock_connection.cursor.return_value = mock_cursor
        mock_pool_class.return_value = mock_pool
        
        db_manager = self.DatabaseManager()
        
        with db_manager.get_cursor(dictionary=True) as (cursor, conn):
            self.assertEqual(cursor, mock_cursor)
            self.assertEqual(conn, mock_connection)
        
        # 驗證 cursor 以 dictionary=True 建立
        mock_connection.cursor.assert_called_with(dictionary=True)
        # 驗證 cursor 已關閉
        mock_cursor.close.assert_called()

    @patch('db_manager.pooling.MySQLConnectionPool')
    def test_get_cursor_dictionary_false(self, mock_pool_class):
        """測試 get_cursor 與 dictionary=False"""
        mock_pool = MagicMock()
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        
        mock_pool.get_connection.return_value = mock_connection
        mock_connection.cursor.return_value = mock_cursor
        mock_pool_class.return_value = mock_pool
        
        db_manager = self.DatabaseManager()
        
        with db_manager.get_cursor(dictionary=False) as (cursor, conn):
            self.assertEqual(cursor, mock_cursor)
            self.assertEqual(conn, mock_connection)
        
        # 驗證 cursor 以 dictionary=False 建立
        mock_connection.cursor.assert_called_with(dictionary=False)

    def test_dialect_property_returns_mariadb_dialect(self):
        """測試 DB_TYPE=mariadb 時 dialect property 回傳 MariaDBDialect"""
        from sql_dialect import MariaDBDialect
        db_manager = self.DatabaseManager()
        self.assertIsInstance(db_manager.dialect, MariaDBDialect)

    def test_dialect_property_cached(self):
        """測試 dialect property 快取同一實例"""
        db_manager = self.DatabaseManager()
        d1 = db_manager.dialect
        d2 = db_manager.dialect
        self.assertIs(d1, d2)


class TestSQLiteDatabaseManager(unittest.TestCase):
    """SQLite 後端 DatabaseManager 整合測試（使用 :memory: 資料庫）"""

    def setUp(self):
        """設定 SQLite 測試環境"""
        self.env_backup = {
            'DB_TYPE': os.getenv('DB_TYPE'),
            'DB_PATH': os.getenv('DB_PATH'),
        }
        os.environ['DB_TYPE'] = 'sqlite'
        os.environ['DB_PATH'] = ':memory:'
        if 'db_manager' in sys.modules:
            del sys.modules['db_manager']
        from db_manager import DatabaseManager
        self.db = DatabaseManager()
        # 手動建立測試用資料表
        conn = self.db._get_sqlite_conn()
        conn.execute('''
            CREATE TABLE IF NOT EXISTS test_table (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT,
                value INTEGER DEFAULT 0
            )
        ''')
        conn.commit()

    def tearDown(self):
        for key, value in self.env_backup.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value
        if 'db_manager' in sys.modules:
            del sys.modules['db_manager']

    def test_db_type_property(self):
        """測試 db_type 回傳 sqlite"""
        self.assertEqual(self.db.db_type, 'sqlite')

    def test_dialect_property_returns_sqlite_dialect(self):
        """測試 dialect property 回傳 SQLiteDialect 實例"""
        from sql_dialect import SQLiteDialect
        self.assertIsInstance(self.db.dialect, SQLiteDialect)

    def test_dialect_property_cached(self):
        """測試 dialect property 快取同一實例"""
        d1 = self.db.dialect
        d2 = self.db.dialect
        self.assertIs(d1, d2)

    def test_translate_query_placeholder(self):
        """測試 dialect.translate_query 轉換 %s → ?"""
        result = self.db.dialect.translate_query("SELECT * FROM t WHERE id=%s AND name=%s")
        self.assertEqual(result, "SELECT * FROM t WHERE id=? AND name=?")

    def test_translate_query_insert_ignore(self):
        """測試 dialect.translate_query 轉換 INSERT IGNORE → INSERT OR IGNORE"""
        result = self.db.dialect.translate_query("INSERT IGNORE INTO t (a) VALUES (%s)")
        self.assertEqual(result, "INSERT OR IGNORE INTO t (a) VALUES (?)")

    def test_translate_query_insert_ignore_case_insensitive(self):
        """測試 insert ignore（小寫）也能正確轉換"""
        result = self.db.dialect.translate_query("insert ignore into t (a) values (?)")
        self.assertIn("INSERT OR IGNORE", result.upper())

    def test_execute_query_insert_returns_rowcount(self):
        """測試 INSERT execute_query 回傳受影響行數"""
        result = self.db.execute_query(
            "INSERT INTO test_table (name, value) VALUES (%s, %s)",
            params=('hello', 42),
            fetch=False,
        )
        self.assertEqual(result, 1)

    def test_execute_query_select_returns_list(self):
        """測試 SELECT execute_query 回傳 list[dict]"""
        self.db.execute_query(
            "INSERT INTO test_table (name, value) VALUES (%s, %s)",
            params=('world', 99),
        )
        rows = self.db.execute_query(
            "SELECT * FROM test_table WHERE name=%s", params=('world',), fetch=True
        )
        self.assertIsInstance(rows, list)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]['name'], 'world')
        self.assertEqual(rows[0]['value'], 99)

    def test_execute_query_update_returns_rowcount(self):
        """測試 UPDATE execute_query 回傳受影響行數"""
        self.db.execute_query(
            "INSERT INTO test_table (name, value) VALUES (%s, %s)", params=('x', 1)
        )
        count = self.db.execute_query(
            "UPDATE test_table SET value=%s WHERE name=%s", params=(2, 'x'), fetch=False
        )
        self.assertEqual(count, 1)

    def test_execute_transaction_commits(self):
        """測試 execute_transaction 原子提交"""
        queries = [
            ("INSERT INTO test_table (name, value) VALUES (%s, %s)", ('a', 1)),
            ("INSERT INTO test_table (name, value) VALUES (%s, %s)", ('b', 2)),
        ]
        results = self.db.execute_transaction(queries)
        self.assertEqual(results, [1, 1])
        rows = self.db.execute_query("SELECT * FROM test_table", fetch=True)
        self.assertEqual(len(rows), 2)

    def test_execute_transaction_rollback_on_error(self):
        """測試 execute_transaction 失敗時 rollback"""
        queries = [
            ("INSERT INTO test_table (name, value) VALUES (%s, %s)", ('rollback_test', 1)),
            ("INVALID SQL ??? (%s)", ('x',)),
        ]
        with self.assertRaises(Exception):
            self.db.execute_transaction(queries)
        rows = self.db.execute_query(
            "SELECT * FROM test_table WHERE name=%s", params=('rollback_test',), fetch=True
        )
        self.assertEqual(len(rows), 0)

    def test_health_check_returns_true(self):
        """測試 SQLite health_check 回傳 True"""
        self.assertTrue(self.db.health_check())

    def test_get_connection_context_manager(self):
        """測試 get_connection context manager 可正常取得連線"""
        with self.db.get_connection() as conn:
            self.assertIsNotNone(conn)
            row = conn.execute("SELECT 1").fetchone()
            self.assertEqual(row[0], 1)

    def test_get_cursor_dictionary_true(self):
        """測試 get_cursor dictionary=True 回傳 Row 游標"""
        self.db.execute_query(
            "INSERT INTO test_table (name, value) VALUES (%s, %s)", params=('cur_test', 7)
        )
        with self.db.get_cursor(dictionary=True) as (cursor, conn):
            cursor.execute("SELECT * FROM test_table WHERE name='cur_test'")
            row = cursor.fetchone()
            self.assertEqual(row['name'], 'cur_test')


class TestGlobalDbManager(unittest.TestCase):
    """測試全域 db_manager 實例"""

    def test_global_instance_exists(self):
        """測試全域 db_manager 實例存在"""
        # 確保環境變數已設定以避免連線錯誤
        os.environ.setdefault('DB_TYPE', 'mariadb')
        os.environ.setdefault('DB_HOST', 'localhost')
        os.environ.setdefault('DB_PORT', '3306')
        os.environ.setdefault('DB_USER', 'test')
        os.environ.setdefault('DB_PASSWORD', 'test')
        os.environ.setdefault('DB_NAME', 'test')
        
        from db_manager import db_manager
        self.assertIsNotNone(db_manager)
        self.assertIsNone(db_manager.pool)  # 延遲初始化，初始應為 None


if __name__ == '__main__':
    unittest.main()
