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


if __name__ == '__main__':
    unittest.main()
