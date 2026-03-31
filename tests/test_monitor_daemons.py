"""
Unit tests for monitor_daemons.py
測試 DaemonMonitor 類別的監控功能。
"""
import os
import sys
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch, call
from datetime import datetime, timedelta

sys.path.insert(0, str(Path(__file__).parent.parent))


class TestDaemonMonitorInit(unittest.TestCase):
    """DaemonMonitor 初始化測試"""

    @patch('monitor_daemons.requests.get')
    def test_init_with_default_url(self, mock_get):
        """測試使用預設 URL 初始化"""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_get.return_value = mock_response
        
        # 設定環境變數
        os.environ.pop('API_SERVER_URL', None)
        
        from monitor_daemons import DaemonMonitor
        monitor = DaemonMonitor()
        
        self.assertEqual(monitor.api_url, 'http://localhost:5000')
        self.assertEqual(monitor.refresh_interval, 2)
        self.assertTrue(monitor.is_running)
        
        # 驗證 API 連接檢查
        mock_get.assert_called_with('http://localhost:5000/api/health', timeout=5)

    @patch('monitor_daemons.requests.get')
    def test_init_with_custom_url(self, mock_get):
        """測試使用自訂 URL 初始化"""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_get.return_value = mock_response
        
        from monitor_daemons import DaemonMonitor
        monitor = DaemonMonitor(api_url='http://custom:8080', refresh_interval=5)
        
        self.assertEqual(monitor.api_url, 'http://custom:8080')
        self.assertEqual(monitor.refresh_interval, 5)
        
        # 驗證 endpoints 正確設定
        self.assertEqual(monitor.endpoints['scan_progress'], 'http://custom:8080/api/progress/scan')
        self.assertEqual(monitor.endpoints['process_progress'], 'http://custom:8080/api/progress/process')
        self.assertEqual(monitor.endpoints['system_status'], 'http://custom:8080/api/progress/system')
        self.assertEqual(monitor.endpoints['task_stats'], 'http://custom:8080/api/progress/stats')

    @patch('monitor_daemons.requests.get')
    def test_init_api_connection_success(self, mock_get):
        """測試 API 連接成功"""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_get.return_value = mock_response
        
        from monitor_daemons import DaemonMonitor
        monitor = DaemonMonitor()
        
        self.assertIsNotNone(monitor)

    @patch('monitor_daemons.requests.get')
    def test_init_api_connection_failure_status_code(self, mock_get):
        """測試 API 連接失敗（狀態碼錯誤）"""
        mock_response = MagicMock()
        mock_response.status_code = 500
        mock_get.return_value = mock_response
        
        from monitor_daemons import DaemonMonitor
        
        with self.assertRaises(SystemExit) as ctx:
            DaemonMonitor()
        
        self.assertEqual(ctx.exception.code, 1)

    @patch('monitor_daemons.requests.get')
    def test_init_api_connection_error(self, mock_get):
        """測試 API 連接錯誤（ConnectionError）"""
        import requests
        mock_get.side_effect = requests.exceptions.ConnectionError()
        
        from monitor_daemons import DaemonMonitor
        
        with self.assertRaises(SystemExit) as ctx:
            DaemonMonitor()
        
        self.assertEqual(ctx.exception.code, 1)

    @patch('monitor_daemons.requests.get')
    def test_init_api_general_exception(self, mock_get):
        """測試 API 連接一般例外"""
        mock_get.side_effect = Exception("Unexpected error")
        
        from monitor_daemons import DaemonMonitor
        
        with self.assertRaises(SystemExit) as ctx:
            DaemonMonitor()
        
        self.assertEqual(ctx.exception.code, 1)


class TestGetProgress(unittest.TestCase):
    """get_progress() 方法測試"""

    @patch('monitor_daemons.requests.get')
    def test_get_progress_success(self, mock_get):
        """測試成功獲取進度"""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {'status': 'running', 'progress': 50}
        mock_get.return_value = mock_response
        
        from monitor_daemons import DaemonMonitor
        monitor = DaemonMonitor.__new__(DaemonMonitor)
        monitor.api_url = 'http://localhost:5000'
        
        result = monitor.get_progress('http://localhost:5000/api/progress/scan')
        
        self.assertEqual(result['status'], 'running')
        self.assertEqual(result['progress'], 50)

    @patch('monitor_daemons.requests.get')
    def test_get_progress_http_error(self, mock_get):
        """測試 HTTP 錯誤處理"""
        mock_response = MagicMock()
        mock_response.status_code = 500
        mock_get.return_value = mock_response
        
        from monitor_daemons import DaemonMonitor
        monitor = DaemonMonitor.__new__(DaemonMonitor)
        
        result = monitor.get_progress('http://localhost:5000/api/progress/scan')
        
        self.assertIn('error', result)
        self.assertEqual(result['error'], 'HTTP 500')

    @patch('monitor_daemons.requests.get')
    def test_get_progress_request_exception(self, mock_get):
        """測試請求例外處理"""
        import requests
        mock_get.side_effect = requests.exceptions.RequestException("Timeout")
        
        from monitor_daemons import DaemonMonitor
        monitor = DaemonMonitor.__new__(DaemonMonitor)
        
        result = monitor.get_progress('http://localhost:5000/api/progress/scan')
        
        self.assertIn('error', result)
        self.assertEqual(result['error'], 'Timeout')

    @patch('monitor_daemons.requests.get')
    def test_get_progress_invalid_json(self, mock_get):
        """測試無效 JSON 回應處理"""
        import json
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.side_effect = json.JSONDecodeError("Invalid JSON", "", 0)
        mock_get.return_value = mock_response
        
        from monitor_daemons import DaemonMonitor
        monitor = DaemonMonitor.__new__(DaemonMonitor)
        
        result = monitor.get_progress('http://localhost:5000/api/progress/scan')
        
        self.assertIn('error', result)
        self.assertEqual(result['error'], 'Invalid JSON response')

    @patch('monitor_daemons.requests.get')
    def test_get_progress_string_to_int_conversion(self, mock_get):
        """測試字串轉整數的型別轉換"""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {'total': '100', 'completed': '50'}
        mock_get.return_value = mock_response
        
        from monitor_daemons import DaemonMonitor
        monitor = DaemonMonitor.__new__(DaemonMonitor)
        
        result = monitor.get_progress('http://localhost:5000/api/progress/stats')
        
        # 驗證字串被轉換為整數
        self.assertEqual(result['total'], 100)
        self.assertEqual(result['completed'], 50)
        self.assertIsInstance(result['total'], int)
        self.assertIsInstance(result['completed'], int)

    @patch('monitor_daemons.requests.get')
    def test_get_progress_invalid_string_conversion(self, mock_get):
        """測試無效字串轉換處理"""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {'total': 'invalid', 'completed': 'also_invalid'}
        mock_get.return_value = mock_response
        
        from monitor_daemons import DaemonMonitor
        monitor = DaemonMonitor.__new__(DaemonMonitor)
        
        result = monitor.get_progress('http://localhost:5000/api/progress/stats')
        
        # 驗證無效字串被轉換為 0
        self.assertEqual(result['total'], 0)
        self.assertEqual(result['completed'], 0)


class TestFormatDuration(unittest.TestCase):
    """format_duration() 方法測試"""

    def setUp(self):
        from monitor_daemons import DaemonMonitor
        self.monitor = DaemonMonitor.__new__(DaemonMonitor)

    def test_format_duration_seconds(self):
        """測試格式化秒數"""
        result = self.monitor.format_duration(30)
        self.assertEqual(result, "30 秒")

    def test_format_duration_minutes(self):
        """測試格式化分鐘"""
        result = self.monitor.format_duration(125)
        self.assertEqual(result, "2 分 5 秒")

    def test_format_duration_hours(self):
        """測試格式化小時"""
        result = self.monitor.format_duration(7320)
        self.assertEqual(result, "2 小時 2 分")

    def test_format_duration_negative(self):
        """測試負數處理（系統時鐘向後調整）"""
        result = self.monitor.format_duration(-10)
        self.assertEqual(result, "0 秒")

    def test_format_duration_zero(self):
        """測試零值"""
        result = self.monitor.format_duration(0)
        self.assertEqual(result, "0 秒")


class TestFormatFileSize(unittest.TestCase):
    """format_file_size() 方法測試"""

    def setUp(self):
        from monitor_daemons import DaemonMonitor
        self.monitor = DaemonMonitor.__new__(DaemonMonitor)

    def test_format_file_size_bytes(self):
        """測試格式化位元組"""
        result = self.monitor.format_file_size(512)
        self.assertEqual(result, "512 B")

    def test_format_file_size_kb(self):
        """測試格式化 KB"""
        result = self.monitor.format_file_size(2048)
        self.assertEqual(result, "2.0 KB")

    def test_format_file_size_mb(self):
        """測試格式化 MB"""
        result = self.monitor.format_file_size(5 * 1024 * 1024)
        self.assertEqual(result, "5.0 MB")

    def test_format_file_size_gb(self):
        """測試格式化 GB"""
        result = self.monitor.format_file_size(3 * 1024 * 1024 * 1024)
        self.assertEqual(result, "3.0 GB")

    def test_format_file_size_none(self):
        """測試 None 值"""
        result = self.monitor.format_file_size(None)
        self.assertEqual(result, "N/A")

    def test_format_file_size_negative(self):
        """測試負數值"""
        result = self.monitor.format_file_size(-100)
        self.assertEqual(result, "N/A")


class TestGetDaemonStatus(unittest.TestCase):
    """get_daemon_status() 方法測試"""

    def setUp(self):
        from monitor_daemons import DaemonMonitor
        self.monitor = DaemonMonitor.__new__(DaemonMonitor)
        self.monitor.api_url = 'http://localhost:5000'

    @patch('monitor_daemons.os.path.exists')
    def test_get_daemon_status_no_pid_file(self, mock_exists):
        """測試 PID 檔案不存在"""
        mock_exists.return_value = False
        
        result = self.monitor.get_daemon_status('scan')
        
        self.assertEqual(result['status'], 'stopped')
        self.assertIsNone(result['pid'])
        self.assertEqual(result['uptime'], 0)

    @patch('monitor_daemons.os.kill')
    @patch('monitor_daemons.open')
    @patch('monitor_daemons.os.path.exists')
    def test_get_daemon_status_running(self, mock_exists, mock_open, mock_kill):
        """測試 daemon 運行中"""
        mock_exists.return_value = True
        
        # 模擬 PID 檔案讀取
        mock_file = MagicMock()
        mock_file.__enter__ = lambda s: s
        mock_file.__exit__ = MagicMock(return_value=False)
        mock_file.read.return_value = '12345'
        
        # 模擬程序存在
        mock_kill.return_value = None
        
        # 模擬 /proc/uptime 和 /proc/pid/stat
        # start_time_ticks = 1000, clk_tck=100 → start_seconds=10.0
        # system_uptime = 100.0 → expected uptime = 90.0
        proc_stat_content = ' '.join(['0'] * 21 + ['1000'])
        proc_uptime_content = '100.0 12345.00'
        
        # 建立三個不同的 mock 物件：PID file, /proc/pid/stat, /proc/uptime
        pid_file_mock = MagicMock()
        pid_file_mock.__enter__ = lambda s: s
        pid_file_mock.__exit__ = MagicMock(return_value=False)
        pid_file_mock.read.return_value = '12345'
        
        stat_file_mock = MagicMock()
        stat_file_mock.__enter__ = lambda s: s
        stat_file_mock.__exit__ = MagicMock(return_value=False)
        stat_file_mock.read.return_value = proc_stat_content
        
        uptime_file_mock = MagicMock()
        uptime_file_mock.__enter__ = lambda s: s
        uptime_file_mock.__exit__ = MagicMock(return_value=False)
        uptime_file_mock.read.return_value = proc_uptime_content
        
        # 建立路徑對應的 mock，避免依賴 open() 呼叫順序
        def open_side_effect(path, *args, **kwargs):
            path_str = str(path)
            if path_str.endswith('.pid'):
                return pid_file_mock
            elif path_str.endswith('/stat'):
                return stat_file_mock
            else:
                return uptime_file_mock
        mock_open.side_effect = open_side_effect

        with patch('os.sysconf', return_value=100):
            result = self.monitor.get_daemon_status('scan')
        
        self.assertEqual(result['status'], 'running')
        self.assertEqual(result['pid'], 12345)
        # uptime = 100.0 - (1000/100) = 90.0
        self.assertGreater(result['uptime'], 80)  # 允許一些誤差
        self.assertLessEqual(result['uptime'], 100)

    @patch('monitor_daemons.os.kill')
    @patch('monitor_daemons.open')
    @patch('monitor_daemons.os.path.exists')
    def test_get_daemon_status_stopped_oserror(self, mock_exists, mock_open, mock_kill):
        """測試 daemon 已停止（OSError）"""
        mock_exists.return_value = True
        
        mock_file = MagicMock()
        mock_file.__enter__ = lambda s: s
        mock_file.__exit__ = MagicMock(return_value=False)
        mock_file.read.return_value = '12345'
        mock_open.return_value = mock_file
        
        # 模擬程序不存在
        mock_kill.side_effect = OSError()
        
        result = self.monitor.get_daemon_status('scan')
        
        self.assertEqual(result['status'], 'stopped')
        self.assertEqual(result['pid'], 12345)
        self.assertEqual(result['uptime'], 0)

    @patch('monitor_daemons.os.path.exists')
    def test_get_daemon_status_exception(self, mock_exists):
        """測試一般例外處理"""
        mock_exists.side_effect = Exception("Unexpected error")
        
        result = self.monitor.get_daemon_status('scan')
        
        self.assertEqual(result['status'], 'unknown')
        self.assertIn('error', result)


class TestDisplayMethods(unittest.TestCase):
    """顯示相關方法測試"""

    def setUp(self):
        from monitor_daemons import DaemonMonitor
        self.monitor = DaemonMonitor.__new__(DaemonMonitor)
        self.monitor.api_url = 'http://localhost:5000'

    def test_get_color(self):
        """測試顏色代碼獲取"""
        colors = ['red', 'green', 'yellow', 'blue', 'magenta', 'cyan', 'white', 'dim', 'bold', 'underline', 'reset']
        
        for color in colors:
            result = self.monitor.get_color(color)
            self.assertIsInstance(result, str)
        
        # 測試未知顏色
        result = self.monitor.get_color('unknown')
        self.assertEqual(result, '')

    def test_get_status_color(self):
        """測試狀態顏色獲取"""
        scan_color = self.monitor.get_status_color('scan')
        process_color = self.monitor.get_status_color('process')
        other_color = self.monitor.get_status_color('other')
        
        self.assertIn(scan_color, ['\x1b[94m', ''])  # blue or empty
        self.assertIn(process_color, ['\x1b[92m', ''])  # green or empty
        self.assertIn(other_color, ['\x1b[97m', ''])  # white or empty

    @patch('sys.stdout')
    def test_display_scan_progress(self, mock_stdout):
        """測試掃描進度顯示"""
        progress_data = {
            'status': 'scanning',
            'last_scan_time': datetime.now().isoformat(),
            'files_scanned': 100,
            'tasks_added': 50,
            'error_count': 2
        }
        
        self.monitor.display_scan_progress(progress_data)
        
        # 驗證輸出被呼叫（不檢查具體內容，因為包含 ANSI 顏色代碼）
        self.assertTrue(mock_stdout.write.called)

    @patch('sys.stdout')
    def test_display_process_progress(self, mock_stdout):
        """測試處理進度顯示"""
        progress_data = {
            'status': 'processing',
            'last_check_time': datetime.now().isoformat(),
            'tasks_processing': 5,
            'tasks_completed': 100,
            'tasks_failed': 2,
            'queue_size': 10,
            'active_workers': 3,
            'max_workers': 5
        }
        
        self.monitor.display_process_progress(progress_data)
        
        self.assertTrue(mock_stdout.write.called)

    @patch('sys.stdout')
    def test_display_task_stats(self, mock_stdout):
        """測試任務統計顯示"""
        task_stats = {
            'total': 200,
            'pending': 50,
            'processing': 10,
            'completed': 135,
            'failed': 5,
            'retried': 3,
            'avg_duration': 300.5
        }
        
        self.monitor.display_task_stats(task_stats)
        
        self.assertTrue(mock_stdout.write.called)

    def test_create_progress_bar(self):
        """測試進度條建立"""
        result = self.monitor.create_progress_bar(75, 100, length=20)
        
        # 驗證進度條格式
        self.assertIn('[', result)
        self.assertIn(']', result)
        self.assertIn('%', result)
        self.assertTrue(result.startswith('\x1b['))  # 有顏色代碼

    def test_create_progress_bar_zero_total(self):
        """測試總數為零時的進度條"""
        result = self.monitor.create_progress_bar(0, 0, length=20)
        
        self.assertIn('0%', result)

    def test_create_progress_bar_string_input(self):
        """測試字串輸入的進度條"""
        result = self.monitor.create_progress_bar('50', '100', length=20)
        
        self.assertIn('50.0%', result)

    def test_create_progress_bar_invalid_input(self):
        """測試無效輸入的進度條"""
        result = self.monitor.create_progress_bar('invalid', 'also_invalid', length=20)
        
        self.assertIn('N/A', result)


class TestHandleShutdown(unittest.TestCase):
    """handle_shutdown() 方法測試"""

    @patch('monitor_daemons.sys.exit')
    def test_handle_shutdown(self, mock_exit):
        """測試關閉信號處理"""
        from monitor_daemons import DaemonMonitor
        monitor = DaemonMonitor.__new__(DaemonMonitor)
        monitor.is_running = True
        
        monitor.handle_shutdown(None, None)
        
        self.assertFalse(monitor.is_running)
        mock_exit.assert_called_with(0)


class TestCheckApiConnection(unittest.TestCase):
    """check_api_connection() 方法測試"""

    @patch('monitor_daemons.requests.get')
    def test_check_api_connection_success(self, mock_get):
        """測試 API 連接檢查成功"""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_get.return_value = mock_response
        
        from monitor_daemons import DaemonMonitor
        monitor = DaemonMonitor.__new__(DaemonMonitor)
        monitor.api_url = 'http://localhost:5000'
        
        # 不應拋出例外
        monitor.check_api_connection()

    @patch('monitor_daemons.requests.get')
    def test_check_api_connection_bad_status(self, mock_get):
        """測試 API 連接檢查狀態碼錯誤"""
        mock_response = MagicMock()
        mock_response.status_code = 500
        mock_get.return_value = mock_response
        
        from monitor_daemons import DaemonMonitor
        monitor = DaemonMonitor.__new__(DaemonMonitor)
        monitor.api_url = 'http://localhost:5000'
        
        with self.assertRaises(SystemExit) as ctx:
            monitor.check_api_connection()
        
        self.assertEqual(ctx.exception.code, 1)

    @patch('monitor_daemons.requests.get')
    def test_check_api_connection_timeout(self, mock_get):
        """測試 API 連接超時"""
        import requests
        mock_get.side_effect = requests.exceptions.Timeout()
        
        from monitor_daemons import DaemonMonitor
        monitor = DaemonMonitor.__new__(DaemonMonitor)
        monitor.api_url = 'http://localhost:5000'
        
        with self.assertRaises(SystemExit) as ctx:
            monitor.check_api_connection()
        
        self.assertEqual(ctx.exception.code, 1)


if __name__ == '__main__':
    unittest.main()
