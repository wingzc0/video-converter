"""
Unit tests for init_db.py
測試 init_database() 函數的資料庫初始化功能。
"""
import os
import sys
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch, call

sys.path.insert(0, str(Path(__file__).parent.parent))


class TestInitDatabase(unittest.TestCase):
    """init_database() 函數測試"""

    def setUp(self):
        """設定測試環境"""
        # 重置環境變數
        self.env_backup = {
            'DB_HOST': os.getenv('DB_HOST'),
            'DB_PORT': os.getenv('DB_PORT'),
            'DB_USER': os.getenv('DB_USER'),
            'DB_PASSWORD': os.getenv('DB_PASSWORD'),
            'DB_NAME': os.getenv('DB_NAME'),
        }
        
        # 設定測試用的環境變數
        os.environ['DB_HOST'] = 'localhost'
        os.environ['DB_PORT'] = '3306'
        os.environ['DB_USER'] = 'test_user'
        os.environ['DB_PASSWORD'] = 'test_password'
        os.environ['DB_NAME'] = 'test_db'
        
        # 重新載入模組以使用新的環境變數
        if 'init_db' in sys.modules:
            del sys.modules['init_db']

    def tearDown(self):
        """還原環境變數"""
        for key, value in self.env_backup.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value
        
        # 清理模組快取
        if 'init_db' in sys.modules:
            del sys.modules['init_db']

    @patch('init_db.mysql.connector.connect')
    def test_init_database_success(self, mock_connect):
        """測試資料庫初始化成功"""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        
        mock_connect.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cursor
        mock_conn.is_connected.return_value = True
        
        from init_db import init_database
        init_database()
        
        # 驗證連接建立
        mock_connect.assert_called_once()
        call_kwargs = mock_connect.call_args[1]
        self.assertEqual(call_kwargs['host'], 'localhost')
        self.assertEqual(call_kwargs['port'], 3306)
        self.assertEqual(call_kwargs['user'], 'test_user')
        self.assertEqual(call_kwargs['password'], 'test_password')
        
        # 驗證 SQL 執行順序
        expected_calls = [
            call("CREATE DATABASE IF NOT EXISTS test_db CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"),
            call("USE test_db"),
        ]
        
        # 檢查 CREATE DATABASE 和 USE 被呼叫
        mock_cursor.execute.assert_any_call("CREATE DATABASE IF NOT EXISTS test_db CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci")
        mock_cursor.execute.assert_any_call("USE test_db")
        
        # 驗證 conversion_tasks 表建立
        create_table_call_found = False
        for call_arg in mock_cursor.execute.call_args_list:
            sql = call_arg[0][0]
            if 'CREATE TABLE IF NOT EXISTS conversion_tasks' in sql:
                create_table_call_found = True
                # 驗證必要的欄位
                self.assertIn('input_path VARCHAR(1024)', sql)
                self.assertIn('output_path VARCHAR(1024)', sql)
                self.assertIn('status ENUM', sql)
                self.assertIn('is_processing BOOLEAN', sql)
                self.assertIn('retry_count INT', sql)
        self.assertTrue(create_table_call_found, "conversion_tasks table creation not found")
        
        # 驗證 processing_lock 表建立
        lock_table_call_found = False
        for call_arg in mock_cursor.execute.call_args_list:
            sql = call_arg[0][0]
            if 'CREATE TABLE IF NOT EXISTS processing_lock' in sql:
                lock_table_call_found = True
                self.assertIn('task_id INT PRIMARY KEY', sql)
                self.assertIn('worker_id VARCHAR(50)', sql)
                self.assertIn('FOREIGN KEY (task_id) REFERENCES conversion_tasks(id)', sql)
        self.assertTrue(lock_table_call_found, "processing_lock table creation not found")
        
        # 驗證複合索引建立
        index_call_found = False
        for call_arg in mock_cursor.execute.call_args_list:
            sql = call_arg[0][0]
            if 'CREATE INDEX IF NOT EXISTS idx_conversion_tasks_status_processing' in sql:
                index_call_found = True
                self.assertIn('(status, is_processing)', sql)
        self.assertTrue(index_call_found, "Composite index creation not found")
        
        # 驗證 commit 被呼叫
        mock_conn.commit.assert_called_once()
        
        # 驗證資源清理
        mock_cursor.close.assert_called()
        mock_conn.close.assert_called()

    @patch('init_db.mysql.connector.connect')
    def test_init_database_access_denied_error(self, mock_connect):
        """測試存取被拒錯誤處理"""
        import mysql.connector
        from mysql.connector import errorcode
        
        mock_error = mysql.connector.Error("Access denied")
        mock_error.errno = errorcode.ER_ACCESS_DENIED_ERROR
        mock_connect.side_effect = mock_error
        
        from init_db import init_database
        # 不應拋出例外，而是印出錯誤訊息
        init_database()
        
        # 驗證連接嘗試
        mock_connect.assert_called_once()

    @patch('init_db.mysql.connector.connect')
    def test_init_database_bad_db_error(self, mock_connect):
        """測試資料庫不存在錯誤處理"""
        import mysql.connector
        from mysql.connector import errorcode
        
        mock_error = mysql.connector.Error("Database does not exist")
        mock_error.errno = errorcode.ER_BAD_DB_ERROR
        mock_connect.side_effect = mock_error
        
        from init_db import init_database
        init_database()
        
        mock_connect.assert_called_once()

    @patch('init_db.mysql.connector.connect')
    def test_init_database_general_error(self, mock_connect):
        """測試一般資料庫錯誤處理"""
        import mysql.connector
        
        mock_error = mysql.connector.Error("General database error")
        mock_connect.side_effect = mock_error
        
        from init_db import init_database
        init_database()
        
        mock_connect.assert_called_once()

    @patch('init_db.mysql.connector.connect')
    def test_init_database_connection_cleanup_on_error(self, mock_connect):
        """測試發生錯誤時正確清理資源"""
        import mysql.connector
        
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        
        mock_connect.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cursor
        mock_conn.is_connected.return_value = True
        
        # 模擬在執行 SQL 時拋出錯誤
        mock_cursor.execute.side_effect = mysql.connector.Error("SQL error")
        
        from init_db import init_database
        init_database()
        
        # 驗證資源清理
        mock_cursor.close.assert_called()
        mock_conn.close.assert_called()

    @patch('init_db.mysql.connector.connect')
    def test_init_database_cursor_not_created(self, mock_connect):
        """測試當 cursor 未建立時的清理邏輯"""
        import mysql.connector
        
        mock_conn = MagicMock()
        
        mock_connect.return_value = mock_conn
        mock_conn.is_connected.return_value = True
        
        # 模擬 cursor() 拋出錯誤
        mock_conn.cursor.side_effect = mysql.connector.Error("Cannot create cursor")
        
        from init_db import init_database
        init_database()
        
        # 驗證 conn.close 仍被呼叫
        mock_conn.close.assert_called()

    @patch('init_db.mysql.connector.connect')
    def test_init_database_connection_not_connected(self, mock_connect):
        """測試當連線未建立時的清理邏輯"""
        import mysql.connector
        
        mock_conn = MagicMock()
        
        mock_connect.return_value = mock_conn
        mock_conn.is_connected.return_value = False
        
        from init_db import init_database
        init_database()
        
        # 驗證 conn.close 不被呼叫（因為連線未建立）
        mock_conn.close.assert_not_called()

    def test_missing_db_name_raises_error(self):
        """測試 DB_NAME 未設定時應拋出 ValueError"""
        os.environ.pop('DB_NAME', None)

        with patch('dotenv.load_dotenv'):
            from init_db import init_database

        with patch('init_db.mysql.connector.connect') as mock_connect:
            mock_conn = MagicMock()
            mock_conn.cursor.return_value = MagicMock()
            mock_conn.is_connected.return_value = True
            mock_connect.return_value = mock_conn

            # DB_NAME 缺失不應靜默建立名為 "None" 的資料庫，應拋出 ValueError
            with self.assertRaises(ValueError) as ctx:
                init_database()
            self.assertIn('DB_NAME', str(ctx.exception))

    def test_env_variables_loading(self):
        """測試環境變數正確載入"""
        # 設定自訂端口
        os.environ['DB_PORT'] = '3307'
        
        from init_db import init_database
        
        with patch('init_db.mysql.connector.connect') as mock_connect:
            mock_conn = MagicMock()
            mock_cursor = MagicMock()
            mock_connect.return_value = mock_conn
            mock_conn.cursor.return_value = mock_cursor
            mock_conn.is_connected.return_value = True
            
            init_database()
            
            # 驗證自訂端口被使用
            call_kwargs = mock_connect.call_args[1]
            self.assertEqual(call_kwargs['port'], 3307)

    def test_default_port(self):
        """測試預設端口為 3306"""
        # 移除 DB_PORT 環境變數，並阻止 load_dotenv 從 .env 重新載入
        os.environ.pop('DB_PORT', None)
        
        with patch('dotenv.load_dotenv'):
            from init_db import init_database
        
        with patch('init_db.mysql.connector.connect') as mock_connect:
            mock_conn = MagicMock()
            mock_cursor = MagicMock()
            mock_connect.return_value = mock_conn
            mock_conn.cursor.return_value = mock_cursor
            mock_conn.is_connected.return_value = True
            
            init_database()
            
            # 驗證預設端口
            call_kwargs = mock_connect.call_args[1]
            self.assertEqual(call_kwargs['port'], 3306)


class TestInitDatabaseMainBlock(unittest.TestCase):
    """測試 __main__ 區塊"""

    def test_main_block_execution(self):
        """測試直接執行腳本時呼叫 init_database"""
        import runpy

        with patch.dict(os.environ, {
            'DB_HOST': 'localhost',
            'DB_PORT': '3306',
            'DB_USER': 'test',
            'DB_PASSWORD': 'test',
            'DB_NAME': 'test_db',
        }):
            with patch('mysql.connector.connect') as mock_connect, \
                 patch('dotenv.load_dotenv'):
                mock_conn = MagicMock()
                mock_cursor = MagicMock()
                mock_connect.return_value = mock_conn
                mock_conn.cursor.return_value = mock_cursor
                mock_conn.is_connected.return_value = True

                # 以 __main__ 身份執行 init_db，驗證 if __name__ == '__main__' 區塊有呼叫 init_database()
                runpy.run_module('init_db', run_name='__main__')

                mock_connect.assert_called_once()


if __name__ == '__main__':
    unittest.main()
