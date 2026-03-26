"""
Unit tests for daemon_ctl.py — API server management functions.

Covers:
  _read_api_pid()  — PID 檔讀取與程序驗證
  cmd_api_start()  — 背景 / 前景啟動
  cmd_api_stop()   — 正常停止 / SIGKILL fallback / 已停止
  cmd_api_status() — running / stopped 輸出
"""
import os
import sys
import signal
import unittest
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch, call

sys.path.insert(0, str(Path(__file__).parent.parent))

import daemon_ctl


def _env(tmp_dir: str) -> dict:
    """回傳將 VIDEO_CONVERTER_RUN_DIR 指向臨時目錄的 env patch dict"""
    return {'VIDEO_CONVERTER_RUN_DIR': tmp_dir}


# ---------------------------------------------------------------------------
# _read_api_pid()
# ---------------------------------------------------------------------------

class TestReadApiPid(unittest.TestCase):
    """_read_api_pid() — 讀取並驗證 PID 檔"""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()

    def tearDown(self):
        self.tmp.cleanup()

    def _pid_file(self) -> Path:
        return Path(self.tmp.name) / 'api.pid'

    def test_returns_none_when_pid_file_missing(self):
        with patch.dict('os.environ', _env(self.tmp.name)):
            result = daemon_ctl._read_api_pid()
        self.assertIsNone(result)

    def test_returns_pid_when_process_running(self):
        self._pid_file().write_text('12345')
        with patch.dict('os.environ', _env(self.tmp.name)):
            with patch('os.kill'):          # kill(12345, 0) 不拋例外 = 程序存在
                result = daemon_ctl._read_api_pid()
        self.assertEqual(result, 12345)

    def test_returns_none_when_process_dead(self):
        self._pid_file().write_text('12345')
        with patch.dict('os.environ', _env(self.tmp.name)):
            with patch('os.kill', side_effect=ProcessLookupError):
                result = daemon_ctl._read_api_pid()
        self.assertIsNone(result)

    def test_returns_none_when_pid_file_content_invalid(self):
        self._pid_file().write_text('not_a_pid')
        with patch.dict('os.environ', _env(self.tmp.name)):
            result = daemon_ctl._read_api_pid()
        self.assertIsNone(result)

    def test_returns_none_when_pid_file_deleted_between_exist_and_read(self):
        """TOCTOU: exists() 後 read_text() 前檔案被刪除，不應拋例外"""
        self._pid_file().write_text('12345')
        with patch.dict('os.environ', _env(self.tmp.name)):
            # 讓 read_text 拋 FileNotFoundError 模擬競態
            with patch.object(Path, 'read_text', side_effect=FileNotFoundError):
                result = daemon_ctl._read_api_pid()
        self.assertIsNone(result)


# ---------------------------------------------------------------------------
# cmd_api_start()
# ---------------------------------------------------------------------------

class TestCmdApiStart(unittest.TestCase):
    """cmd_api_start() — 背景 / 前景啟動"""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()

    def tearDown(self):
        self.tmp.cleanup()

    def _pid_file(self) -> Path:
        return Path(self.tmp.name) / 'api.pid'

    def test_does_not_start_when_already_running(self):
        with patch.dict('os.environ', _env(self.tmp.name)):
            with patch.object(daemon_ctl, '_read_api_pid', return_value=9999):
                with patch('subprocess.Popen') as mock_popen:
                    with patch('builtins.print') as mock_print:
                        daemon_ctl.cmd_api_start()

        mock_popen.assert_not_called()
        mock_print.assert_any_call("api_server is already running.")

    def test_background_start_calls_popen_with_foreground_flag(self):
        pid_file = self._pid_file()

        def write_pid_on_popen(cmd, **kwargs):
            # 模擬子程序寫入 PID 檔
            pid_file.write_text('5678')
            return MagicMock()

        with patch.dict('os.environ', _env(self.tmp.name)):
            with patch.object(daemon_ctl, '_read_api_pid', return_value=None):
                with patch('subprocess.Popen', side_effect=write_pid_on_popen) as mock_popen:
                    with patch('time.sleep'):
                        daemon_ctl.cmd_api_start(foreground=False)

        mock_popen.assert_called_once()
        cmd = mock_popen.call_args[0][0]
        self.assertIn('api', cmd)
        self.assertIn('--foreground', cmd)

    def test_background_start_prints_pid_after_pid_file_appears(self):
        pid_file = self._pid_file()

        def write_pid_on_popen(cmd, **kwargs):
            pid_file.write_text('5678')
            return MagicMock()

        with patch.dict('os.environ', _env(self.tmp.name)):
            with patch.object(daemon_ctl, '_read_api_pid', return_value=None):
                with patch('subprocess.Popen', side_effect=write_pid_on_popen):
                    with patch('time.sleep'):
                        with patch('builtins.print') as mock_print:
                            daemon_ctl.cmd_api_start(foreground=False)

        printed = ' '.join(str(c) for c in mock_print.call_args_list)
        self.assertIn('5678', printed)

    def test_background_start_warns_if_pid_file_never_written(self):
        with patch.dict('os.environ', _env(self.tmp.name)):
            with patch.object(daemon_ctl, '_read_api_pid', return_value=None):
                with patch('subprocess.Popen', return_value=MagicMock()):
                    with patch('time.sleep'):
                        with patch('builtins.print') as mock_print:
                            daemon_ctl.cmd_api_start(foreground=False)

        printed = ' '.join(str(c) for c in mock_print.call_args_list)
        self.assertIn('Warning', printed)

    def test_foreground_start_writes_pid_file_and_calls_start_api_server(self):
        pid_file = self._pid_file()
        mock_server = MagicMock()

        with patch.dict('os.environ', _env(self.tmp.name)):
            with patch.object(daemon_ctl, '_read_api_pid', return_value=None):
                with patch.dict('sys.modules', {
                    'api': MagicMock(),
                    'api.server': MagicMock(start_api_server=mock_server),
                }):
                    daemon_ctl.cmd_api_start(foreground=True)

        mock_server.assert_called_once()
        # 前景模式結束後 PID 檔應被清除
        self.assertFalse(pid_file.exists())


# ---------------------------------------------------------------------------
# cmd_api_stop()
# ---------------------------------------------------------------------------

class TestCmdApiStop(unittest.TestCase):
    """cmd_api_stop() — 正常停止 / SIGKILL fallback / 已停止"""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()

    def tearDown(self):
        self.tmp.cleanup()

    def _pid_file(self) -> Path:
        return Path(self.tmp.name) / 'api.pid'

    def test_prints_not_running_when_no_pid(self):
        with patch.dict('os.environ', _env(self.tmp.name)):
            with patch.object(daemon_ctl, '_read_api_pid', return_value=None):
                with patch('builtins.print') as mock_print:
                    daemon_ctl.cmd_api_stop()

        mock_print.assert_called_once_with("api_server is not running.")

    def test_sends_sigterm_and_removes_pid_file_on_clean_stop(self):
        pid_file = self._pid_file()
        pid_file.write_text('5678')

        kill_log = []

        def fake_kill(pid, sig):
            kill_log.append((pid, sig))
            if sig == 0:
                raise ProcessLookupError   # 程序已停止，退出等待迴圈

        with patch.dict('os.environ', _env(self.tmp.name)):
            with patch.object(daemon_ctl, '_read_api_pid', return_value=5678):
                with patch('os.kill', side_effect=fake_kill):
                    with patch('time.sleep'):
                        daemon_ctl.cmd_api_stop()

        self.assertIn((5678, signal.SIGTERM), kill_log)
        self.assertFalse(pid_file.exists())

    def test_sends_sigkill_when_process_does_not_terminate(self):
        pid_file = self._pid_file()
        pid_file.write_text('5678')

        kill_log = []

        def fake_kill(pid, sig):
            kill_log.append((pid, sig))
            # 程序無論如何都不消失（kill 0 不拋例外）

        with patch.dict('os.environ', _env(self.tmp.name)):
            with patch.object(daemon_ctl, '_read_api_pid', return_value=5678):
                with patch('os.kill', side_effect=fake_kill):
                    with patch('time.sleep'):
                        daemon_ctl.cmd_api_stop()

        self.assertIn((5678, signal.SIGKILL), kill_log)

    def test_handles_process_already_gone_when_sending_sigterm(self):
        with patch.dict('os.environ', _env(self.tmp.name)):
            with patch.object(daemon_ctl, '_read_api_pid', return_value=5678):
                with patch('os.kill', side_effect=ProcessLookupError):
                    with patch('builtins.print') as mock_print:
                        daemon_ctl.cmd_api_stop()

        printed = ' '.join(str(c) for c in mock_print.call_args_list)
        self.assertIn('already stopped', printed)

    def test_does_not_raise_if_pid_file_already_gone(self):
        """PID 檔在 stop 過程中消失不應拋例外（missing_ok=True）"""
        # PID 檔不存在，但 _read_api_pid 仍回傳有效 PID
        with patch.dict('os.environ', _env(self.tmp.name)):
            with patch.object(daemon_ctl, '_read_api_pid', return_value=5678):
                with patch('os.kill', side_effect=ProcessLookupError):
                    # 不應拋 FileNotFoundError
                    daemon_ctl.cmd_api_stop()


# ---------------------------------------------------------------------------
# cmd_api_status()
# ---------------------------------------------------------------------------

class TestCmdApiStatus(unittest.TestCase):
    """cmd_api_status() — running / stopped 輸出"""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()

    def tearDown(self):
        self.tmp.cleanup()

    def _printed(self, mock_print) -> str:
        return ' '.join(str(c) for c in mock_print.call_args_list)

    def test_shows_running_with_pid_and_endpoint(self):
        env = {**_env(self.tmp.name), 'API_SERVER_HOST': '127.0.0.1', 'API_SERVER_PORT': '8080'}
        with patch.dict('os.environ', env):
            with patch.object(daemon_ctl, '_read_api_pid', return_value=1234):
                with patch('builtins.print') as mock_print:
                    daemon_ctl.cmd_api_status()

        output = self._printed(mock_print)
        self.assertIn('running', output)
        self.assertIn('1234', output)
        self.assertIn('8080', output)
        self.assertIn('127.0.0.1', output)

    def test_shows_stopped_when_no_pid(self):
        with patch.dict('os.environ', _env(self.tmp.name)):
            with patch.object(daemon_ctl, '_read_api_pid', return_value=None):
                with patch('builtins.print') as mock_print:
                    daemon_ctl.cmd_api_status()

        output = self._printed(mock_print)
        self.assertIn('stopped', output)


if __name__ == '__main__':
    unittest.main()
