"""
Unit tests for daemons/base_daemon.py
測試 check_pid_file() 的各種 PID 檔案情境，以及 status() 回傳值。
"""
import os
import sys
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

sys.path.insert(0, str(Path(__file__).parent.parent))


def _make_base_daemon(pid_file):
    """建立一個最小化的具體 BaseDaemon 子類別，以便測試基礎類別方法"""
    with patch.dict('os.environ', {
        'INPUT_DIRECTORY': '/tmp',
        'OUTPUT_DIRECTORY': '/tmp',
    }):
        from daemons.base_daemon import BaseDaemon

        class ConcreteDaemon(BaseDaemon):
            def run(self): pass
            def get_progress(self): return {}
            def get_current_status(self): return {}

        d = ConcreteDaemon(
            name='test_daemon',
            default_pid_file=str(pid_file),
            default_log_file='/tmp/test.log',
            default_stderr_log_file='/tmp/test_error.log',
        )
        return d


class TestCheckPidFile(unittest.TestCase):
    """check_pid_file() 各種情境"""

    def setUp(self):
        import tempfile
        self.tmp = Path(tempfile.mkdtemp())
        self.pid_file = self.tmp / 'test.pid'

    def tearDown(self):
        if self.pid_file.exists():
            self.pid_file.unlink()

    def test_no_pid_file_does_nothing(self):
        """PID 檔不存在時，直接通過，不拋例外"""
        d = _make_base_daemon(self.pid_file)
        d.check_pid_file()  # 不應拋例外

    def test_stale_pid_file_is_removed(self):
        """PID 檔存在但程序已死 → 自動刪除 PID 檔"""
        self.pid_file.write_text('99999999')  # 不存在的 PID
        d = _make_base_daemon(self.pid_file)
        with patch('os.kill', side_effect=ProcessLookupError):
            d.check_pid_file()
        self.assertFalse(self.pid_file.exists())

    def test_running_pid_calls_sys_exit(self):
        """PID 檔存在且程序仍在運行 → sys.exit(1)"""
        self.pid_file.write_text('12345')
        d = _make_base_daemon(self.pid_file)
        with patch('os.kill', return_value=None):  # os.kill(pid, 0) 不拋例外 = 程序存在
            with self.assertRaises(SystemExit) as ctx:
                d.check_pid_file()
        self.assertEqual(ctx.exception.code, 1)

    def test_permission_error_calls_sys_exit(self):
        """程序存在但屬於其他使用者 (PermissionError) → sys.exit(1)"""
        self.pid_file.write_text('1')
        d = _make_base_daemon(self.pid_file)
        with patch('os.kill', side_effect=PermissionError):
            with self.assertRaises(SystemExit) as ctx:
                d.check_pid_file()
        self.assertEqual(ctx.exception.code, 1)

    def test_corrupt_pid_file_is_removed(self):
        """PID 檔內容無法解析時，視為殘留檔自動刪除"""
        self.pid_file.write_text('not_a_number')
        d = _make_base_daemon(self.pid_file)
        d.check_pid_file()
        self.assertFalse(self.pid_file.exists())


class TestStatus(unittest.TestCase):
    """status() 回傳 running/stopped/unknown"""

    def setUp(self):
        import tempfile
        self.tmp = Path(tempfile.mkdtemp())
        self.pid_file = self.tmp / 'test.pid'

    def tearDown(self):
        if self.pid_file.exists():
            self.pid_file.unlink()

    def test_no_pid_file_returns_stopped(self):
        d = _make_base_daemon(self.pid_file)
        result = d.status()
        self.assertEqual(result['status'], 'stopped')

    def test_running_process_returns_running(self):
        self.pid_file.write_text(str(os.getpid()))  # 使用自身 PID 模擬存活程序
        d = _make_base_daemon(self.pid_file)
        result = d.status()
        self.assertEqual(result['status'], 'running')
        self.assertEqual(result['pid'], os.getpid())

    def test_dead_pid_returns_stopped(self):
        self.pid_file.write_text('99999999')
        d = _make_base_daemon(self.pid_file)
        result = d.status()
        self.assertEqual(result['status'], 'stopped')


class TestGetProcessUptime(unittest.TestCase):
    """_get_process_uptime(pid) — 使用 /proc/{pid}/stat 與 /proc/uptime 計算正確的行程存活秒數"""

    def setUp(self):
        from daemons.base_daemon import _get_process_uptime
        self.get_uptime = _get_process_uptime

    def _mock_proc_files(self, start_ticks, system_uptime_seconds, clk_tck=100):
        """建立 mock，模擬 /proc/{pid}/stat 與 /proc/uptime 的讀取結果"""
        # /proc/{pid}/stat 格式：欄位 22（index 21）是 starttime (ticks)
        stat_line = ' '.join(['0'] * 21 + [str(start_ticks)])
        uptime_line = f'{system_uptime_seconds} 12345.00'

        mock_open = MagicMock()
        mock_open.return_value.__enter__ = lambda s: s
        mock_open.return_value.__exit__ = MagicMock(return_value=False)
        mock_open.return_value.read.side_effect = [stat_line, uptime_line]
        return mock_open

    @patch('os.sysconf', return_value=100)
    def test_correct_uptime_formula(self, mock_sysconf):
        """uptime = system_uptime - start_ticks / SC_CLK_TCK"""
        # process started at tick 500 with clk_tck=100 → process_start_seconds=5.0
        # system uptime = 100.0 → expected uptime = 95.0
        mock_open = self._mock_proc_files(start_ticks=500, system_uptime_seconds=100.0)
        with patch('builtins.open', mock_open):
            result = self.get_uptime(pid=1234)
        self.assertAlmostEqual(result, 95.0, places=1)

    def test_missing_stat_file_returns_zero(self):
        """/proc/{pid}/stat 不存在時應回傳 0"""
        with patch('builtins.open', side_effect=FileNotFoundError):
            result = self.get_uptime(pid=99999999)
        self.assertEqual(result, 0)

    def test_process_not_found_returns_zero(self):
        """任何例外（包括行程不存在）都應回傳 0，而非拋出例外"""
        with patch('builtins.open', side_effect=Exception('unexpected')):
            result = self.get_uptime(pid=1)
        self.assertEqual(result, 0)

    @patch('os.sysconf', return_value=100)
    def test_returns_float(self, _):
        """回傳值應為 float（或可轉換為 float），而非 int"""
        mock_open = self._mock_proc_files(start_ticks=200, system_uptime_seconds=50.5)
        with patch('builtins.open', mock_open):
            result = self.get_uptime(pid=1)
        self.assertIsInstance(result, float)


if __name__ == '__main__':
    unittest.main()
