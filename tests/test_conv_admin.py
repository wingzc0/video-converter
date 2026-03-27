"""
Unit tests for conv_admin.py
測試 cmd_reset_maxed_failed 與 cmd_cleanup_stale；DB 與 input() 呼叫均以 mock 取代。
"""
import sys
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch, call

sys.path.insert(0, str(Path(__file__).parent.parent))


class TestCmdResetMaxedFailed(unittest.TestCase):
    """cmd_reset_maxed_failed() — 列出並重置超過最大重試次數的失敗任務"""

    def _run(self, tasks, confirmed='y', max_retries=3):
        with patch('task_manager.db_manager') as mock_db, \
             patch('builtins.input', return_value=confirmed), \
             patch('builtins.print'):
            mock_db.execute_query.return_value = tasks
            from conv_admin import cmd_reset_maxed_failed
            cmd_reset_maxed_failed(max_retries=max_retries)
            return mock_db

    def test_no_tasks_skips_db_update(self):
        """找不到任務時不應呼叫 UPDATE"""
        mock_db = self._run(tasks=[])
        # 僅呼叫了 SELECT（1次），不應有第 2 次 UPDATE
        self.assertEqual(mock_db.execute_query.call_count, 1)

    def test_confirmed_calls_update(self):
        """使用者確認 'y' 後，應呼叫 UPDATE 重置狀態"""
        tasks = [
            {'id': 1, 'input_path': '/input/a.mp4', 'retry_count': 3, 'error_message': 'err'},
            {'id': 2, 'input_path': '/input/b.mp4', 'retry_count': 5, 'error_message': 'err'},
        ]
        mock_db = self._run(tasks=tasks, confirmed='y')
        self.assertEqual(mock_db.execute_query.call_count, 2)
        update_query = mock_db.execute_query.call_args_list[1][0][0]
        self.assertIn('pending', update_query.lower())
        self.assertIn('retry_count=0', update_query.lower().replace(' ', ''))

    def test_aborted_skips_update(self):
        """使用者輸入非 'y' 時，不應呼叫 UPDATE"""
        tasks = [{'id': 1, 'input_path': '/input/a.mp4', 'retry_count': 3, 'error_message': ''}]
        mock_db = self._run(tasks=tasks, confirmed='n')
        self.assertEqual(mock_db.execute_query.call_count, 1)

    def test_select_uses_correct_threshold(self):
        """SELECT 查詢應使用 max_retries 作為 retry_count 閾值"""
        with patch('task_manager.db_manager') as mock_db, \
             patch('builtins.input', return_value='n'), \
             patch('builtins.print'):
            mock_db.execute_query.return_value = [
                {'id': 1, 'input_path': '/input/a.mp4', 'retry_count': 5, 'error_message': ''}
            ]
            from conv_admin import cmd_reset_maxed_failed
            cmd_reset_maxed_failed(max_retries=5)
            select_params = mock_db.execute_query.call_args_list[0][0][1]
            self.assertIn(5, select_params)

    def test_update_passes_all_task_ids(self):
        """UPDATE 的 IN (...) 應包含所有查詢到的任務 id"""
        tasks = [
            {'id': 10, 'input_path': '/a.mp4', 'retry_count': 3, 'error_message': ''},
            {'id': 20, 'input_path': '/b.mp4', 'retry_count': 4, 'error_message': ''},
        ]
        mock_db = self._run(tasks=tasks, confirmed='y')
        update_params = mock_db.execute_query.call_args_list[1][0][1]
        self.assertIn(10, update_params)
        self.assertIn(20, update_params)


class TestCmdCleanupStale(unittest.TestCase):
    """cmd_cleanup_stale() — 清除長時間卡在 processing 的任務，使用 COALESCE"""

    def _run(self, tasks, hours=24):
        with patch('task_manager.db_manager') as mock_db, \
             patch('builtins.print'):
            mock_db.execute_query.return_value = tasks
            from conv_admin import cmd_cleanup_stale
            cmd_cleanup_stale(hours=hours)
            return mock_db

    def test_no_stale_tasks_skips_update(self):
        """無過時任務時不應呼叫 UPDATE"""
        mock_db = self._run(tasks=[])
        self.assertEqual(mock_db.execute_query.call_count, 1)

    def test_select_uses_coalesce(self):
        """SELECT 查詢應使用 COALESCE(start_time, updated_at, created_at)"""
        with patch('task_manager.db_manager') as mock_db, \
             patch('builtins.print'):
            mock_db.execute_query.return_value = []
            from conv_admin import cmd_cleanup_stale
            cmd_cleanup_stale(hours=4)
            select_query = mock_db.execute_query.call_args_list[0][0][0]
            self.assertIn('COALESCE', select_query.upper())
            self.assertIn('start_time', select_query)
            self.assertIn('updated_at', select_query)

    def test_stale_tasks_are_updated_and_lock_deleted(self):
        """每個過時任務應觸發 UPDATE + DELETE processing_lock"""
        tasks = [{'id': 7}, {'id': 8}]
        mock_db = self._run(tasks=tasks)
        # 1 SELECT + 2*(UPDATE + DELETE) = 5 calls
        self.assertEqual(mock_db.execute_query.call_count, 5)
        # 確認 UPDATE 將 status 設為 failed
        update_query = mock_db.execute_query.call_args_list[1][0][0]
        self.assertIn('failed', update_query.lower())


class TestCmdKillStaleFfmpeg(unittest.TestCase):
    """cmd_kill_stale_ffmpeg() — 掃描並 kill 孤兒 ffmpeg 程序"""

    def _make_proc(self, pid, cmdline):
        proc = MagicMock()
        proc.pid = pid
        proc.info = {'name': 'ffmpeg', 'cmdline': cmdline}
        return proc

    @patch('task_manager.db_manager')
    @patch('psutil.process_iter')
    @patch('conv_admin._get_process_daemon_descendant_pids', return_value=set())
    def test_kills_orphan_with_known_task(self, _pids, mock_iter, mock_db):
        """孤兒 ffmpeg 且 source file 有 DB 記錄時應被 kill"""
        import os, signal
        orphan = self._make_proc(1234, ['ffmpeg', '-i', '/input/a.mp4', '/output/a.mp4'])
        mock_iter.return_value = [orphan]
        mock_db.execute_query.return_value = [{'id': 1, 'input_path': '/input/a.mp4', 'status': 'processing'}]

        with patch('os.kill') as mock_kill, patch('builtins.print'):
            from conv_admin import cmd_kill_stale_ffmpeg
            cmd_kill_stale_ffmpeg(dry_run=False)

        mock_kill.assert_called_once_with(1234, signal.SIGKILL)

    @patch('task_manager.db_manager')
    @patch('psutil.process_iter')
    @patch('conv_admin._get_process_daemon_descendant_pids', return_value=set())
    def test_dry_run_does_not_kill(self, _pids, mock_iter, mock_db):
        """dry_run=True 時只列印，不呼叫 os.kill"""
        orphan = self._make_proc(5678, ['ffmpeg', '-i', '/input/b.mp4', '/output/b.mp4'])
        mock_iter.return_value = [orphan]
        mock_db.execute_query.return_value = [{'id': 2, 'input_path': '/input/b.mp4', 'status': 'processing'}]

        with patch('os.kill') as mock_kill, patch('builtins.print'):
            from conv_admin import cmd_kill_stale_ffmpeg
            cmd_kill_stale_ffmpeg(dry_run=True)

        mock_kill.assert_not_called()

    @patch('task_manager.db_manager')
    @patch('psutil.process_iter')
    @patch('conv_admin._get_process_daemon_descendant_pids', return_value={9999})
    def test_skips_daemon_child(self, _pids, mock_iter, mock_db):
        """daemon 子程序不應被 kill"""
        child = self._make_proc(9999, ['ffmpeg', '-i', '/input/c.mp4', '/output/c.mp4'])
        mock_iter.return_value = [child]

        with patch('os.kill') as mock_kill, patch('builtins.print'):
            from conv_admin import cmd_kill_stale_ffmpeg
            cmd_kill_stale_ffmpeg(dry_run=False)

        mock_kill.assert_not_called()

    @patch('task_manager.db_manager')
    @patch('psutil.process_iter')
    @patch('conv_admin._get_process_daemon_descendant_pids', return_value=set())
    def test_skips_unknown_input_path(self, _pids, mock_iter, mock_db):
        """ffmpeg 的 source file 不在 DB 時不應被 kill"""
        orphan = self._make_proc(7777, ['ffmpeg', '-i', '/other/unknown.mp4', '/tmp/out.mp4'])
        mock_iter.return_value = [orphan]
        mock_db.execute_query.return_value = []  # 查無此 task

        with patch('os.kill') as mock_kill, patch('builtins.print'):
            from conv_admin import cmd_kill_stale_ffmpeg
            cmd_kill_stale_ffmpeg(dry_run=False)

        mock_kill.assert_not_called()

    @patch('psutil.process_iter')
    @patch('conv_admin._get_process_daemon_descendant_pids', return_value=set())
    def test_skips_non_ffmpeg_process(self, _pids, mock_iter):
        """非 ffmpeg 程序不應被處理"""
        proc = MagicMock()
        proc.pid = 8888
        proc.info = {'name': 'python3', 'cmdline': ['python3', 'script.py']}
        mock_iter.return_value = [proc]

        with patch('os.kill') as mock_kill, patch('builtins.print'):
            from conv_admin import cmd_kill_stale_ffmpeg
            cmd_kill_stale_ffmpeg(dry_run=False)

        mock_kill.assert_not_called()

    @patch('task_manager.db_manager')
    @patch('psutil.process_iter')
    @patch('conv_admin._get_process_daemon_descendant_pids', return_value=set())
    def test_skips_completed_task_ffmpeg(self, _pids, mock_iter, mock_db):
        """source file 在 DB 中但 status='completed' → 不 kill"""
        import os, signal
        orphan = self._make_proc(2222, ['ffmpeg', '-i', '/input/done.mp4', '/tmp/out.mp4'])
        mock_iter.return_value = [orphan]
        mock_db.execute_query.return_value = [{'id': 10, 'status': 'completed', 'output_path': '/out/done.mp4'}]

        with patch('os.kill') as mock_kill, patch('builtins.print'):
            from conv_admin import cmd_kill_stale_ffmpeg
            cmd_kill_stale_ffmpeg(dry_run=False)

        mock_kill.assert_not_called()

    @patch('task_manager.db_manager')
    @patch('psutil.process_iter')
    @patch('conv_admin._get_process_daemon_descendant_pids', return_value=set())
    def test_skips_failed_task_ffmpeg(self, _pids, mock_iter, mock_db):
        """source file 在 DB 中但 status='failed' → 不 kill"""
        import os, signal
        orphan = self._make_proc(3333, ['ffmpeg', '-i', '/input/err.mp4', '/tmp/out.mp4'])
        mock_iter.return_value = [orphan]
        mock_db.execute_query.return_value = [{'id': 11, 'status': 'failed', 'output_path': '/out/err.mp4'}]

        with patch('os.kill') as mock_kill, patch('builtins.print'):
            from conv_admin import cmd_kill_stale_ffmpeg
            cmd_kill_stale_ffmpeg(dry_run=False)

        mock_kill.assert_not_called()


if __name__ == '__main__':
    unittest.main()
