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
            # execute_transaction returns list of rowcounts; [1, 1] = UPDATE matched + DELETE ran
            mock_db.execute_transaction.return_value = [1, 1]
            from conv_admin import cmd_cleanup_stale
            cmd_cleanup_stale(hours=hours)
            return mock_db

    def test_no_stale_tasks_skips_update(self):
        """無過時任務時不應呼叫 UPDATE"""
        mock_db = self._run(tasks=[])
        self.assertEqual(mock_db.execute_query.call_count, 1)
        self.assertEqual(mock_db.execute_transaction.call_count, 0)

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
        """每個過時任務應原子性執行 UPDATE（含 TOCTOU 防護） + DELETE processing_lock"""
        tasks = [{'id': 7}, {'id': 8}]
        mock_db = self._run(tasks=tasks)
        # 1 SELECT + 2 atomic transactions
        self.assertEqual(mock_db.execute_query.call_count, 1)
        self.assertEqual(mock_db.execute_transaction.call_count, 2)
        queries = mock_db.execute_transaction.call_args_list[0][0][0]
        update_query = queries[0][0]
        self.assertIn('failed', update_query.lower())
        self.assertIn("status = 'processing'", update_query)
        self.assertIn("is_processing = TRUE", update_query)
        self.assertIn('processing_lock', queries[1][0].lower())

    def test_toctou_completed_task_not_counted(self):
        """UPDATE 影響 0 列（任務已完成）時，不應計入 cleaned"""
        with patch('task_manager.db_manager') as mock_db, \
             patch('builtins.print'):
            mock_db.execute_query.return_value = [{'id': 99}]
            mock_db.execute_transaction.return_value = [0, 0]  # UPDATE matched nothing
            from conv_admin import cmd_cleanup_stale
            cmd_cleanup_stale()
            # execute_transaction called once but cleaned should be 0 → no warning printed


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


class TestCmdResetTask(unittest.TestCase):
    """cmd_reset_task() — 重設指定 task ID 為 pending"""

    @patch('task_manager.db_manager')
    def test_resets_valid_task(self, mock_db):
        """有效 task ID 應被重設"""
        mock_db.execute_query.side_effect = [
            # get_task_detail SELECT
            [{'id': 42, 'input_path': '/a.mp4', 'output_path': '/out/a.mp4',
              'status': 'failed', 'retry_count': 3, 'error_message': 'timeout'}],
            # reset_tasks_to_pending UPDATE
            1,
        ]
        with patch('builtins.print'):
            from conv_admin import cmd_reset_task
            cmd_reset_task([42])
        update_calls = [c for c in mock_db.execute_query.call_args_list
                        if 'UPDATE' in str(c)]
        self.assertEqual(len(update_calls), 1)

    @patch('task_manager.db_manager')
    def test_skips_nonexistent_task(self, mock_db):
        """不存在的 task ID 應被跳過，不執行 UPDATE"""
        mock_db.execute_query.return_value = []  # get_task_detail → not found
        with patch('builtins.print'):
            from conv_admin import cmd_reset_task
            cmd_reset_task([999])
        update_calls = [c for c in mock_db.execute_query.call_args_list
                        if 'UPDATE' in str(c)]
        self.assertEqual(len(update_calls), 0)

    @patch('task_manager.db_manager')
    def test_mixed_valid_and_invalid(self, mock_db):
        """混合有效與無效 ID — 只重設有效的"""
        mock_db.execute_query.side_effect = [
            # task 10 found
            [{'id': 10, 'input_path': '/a.mp4', 'output_path': '/out/a.mp4',
              'status': 'failed', 'retry_count': 2, 'error_message': None}],
            # task 999 not found
            [],
            # reset_tasks_to_pending UPDATE
            1,
        ]
        with patch('builtins.print'):
            from conv_admin import cmd_reset_task
            cmd_reset_task([10, 999])
        update_calls = [c for c in mock_db.execute_query.call_args_list
                        if 'UPDATE' in str(c)]
        self.assertEqual(len(update_calls), 1)
        # Verify only task 10 was in the UPDATE
        self.assertIn('10', str(update_calls[0]))


class TestCmdAddFile(unittest.TestCase):
    """cmd_add_file() — 手動新增影片檔至轉檔資料庫"""

    def setUp(self):
        import tempfile
        self.tmp = Path(tempfile.mkdtemp())
        self.video = self.tmp / 'test.mp4'
        self.video.touch()

    def tearDown(self):
        import shutil
        shutil.rmtree(self.tmp, ignore_errors=True)

    @patch('task_manager.db_manager')
    @patch('conv_admin.get_video_info')
    def test_adds_new_file(self, mock_info, mock_db):
        """有效影片檔應成功插入 DB"""
        mock_info.return_value = {'resolution': '1920x1080', 'width': 1920, 'height': 1080}
        mock_db.execute_query.return_value = 1  # INSERT → 1 row affected
        with patch('builtins.print'), \
             patch.dict('os.environ', {'INPUT_DIRECTORY': str(self.tmp),
                                       'OUTPUT_DIRECTORY': str(self.tmp / 'out')}):
            from conv_admin import cmd_add_file
            cmd_add_file([str(self.video)])
        insert_calls = [c for c in mock_db.execute_query.call_args_list
                        if 'INSERT' in str(c)]
        self.assertEqual(len(insert_calls), 1)
        # output path should be .mp4
        self.assertIn('480p_test.mp4', str(insert_calls[0]))

    @patch('task_manager.db_manager')
    @patch('conv_admin.get_video_info')
    def test_already_exists_in_db(self, mock_info, mock_db):
        """已在 DB 中的檔案應跳過（INSERT IGNORE → 0 rows）"""
        mock_info.return_value = {'resolution': '1920x1080', 'width': 1920, 'height': 1080}
        mock_db.execute_query.side_effect = [
            0,  # INSERT IGNORE → 0 (already exists)
            [{'id': 5, 'status': 'pending', 'output_path': '/out/480p_test.mp4'}],  # get_task_by_input_path
        ]
        with patch('builtins.print'), \
             patch.dict('os.environ', {'INPUT_DIRECTORY': str(self.tmp),
                                       'OUTPUT_DIRECTORY': str(self.tmp / 'out')}):
            from conv_admin import cmd_add_file
            cmd_add_file([str(self.video)])
        insert_calls = [c for c in mock_db.execute_query.call_args_list
                        if 'INSERT' in str(c)]
        self.assertEqual(len(insert_calls), 1)

    def test_file_not_found(self):
        """不存在的檔案應被跳過"""
        with patch('builtins.print'), \
             patch.dict('os.environ', {'INPUT_DIRECTORY': str(self.tmp),
                                       'OUTPUT_DIRECTORY': str(self.tmp / 'out')}):
            from conv_admin import cmd_add_file
            cmd_add_file(['/nonexistent/video.mp4'])
        # No exception → test passes

    def test_unsupported_extension(self):
        """不支援的副檔名應被跳過"""
        doc = self.tmp / 'report.pdf'
        doc.touch()
        with patch('builtins.print'), \
             patch.dict('os.environ', {'INPUT_DIRECTORY': str(self.tmp),
                                       'OUTPUT_DIRECTORY': str(self.tmp / 'out')}):
            from conv_admin import cmd_add_file
            cmd_add_file([str(doc)])
        # No DB call expected

    @patch('task_manager.db_manager')
    @patch('conv_admin.get_video_info')
    def test_ffprobe_failure_skipped(self, mock_info, mock_db):
        """ffprobe 失敗時應跳過該檔案"""
        mock_info.return_value = None
        with patch('builtins.print'), \
             patch.dict('os.environ', {'INPUT_DIRECTORY': str(self.tmp),
                                       'OUTPUT_DIRECTORY': str(self.tmp / 'out')}):
            from conv_admin import cmd_add_file
            cmd_add_file([str(self.video)])
        mock_db.execute_query.assert_not_called()

    @patch('task_manager.db_manager')
    @patch('conv_admin.get_video_info')
    def test_file_outside_input_dir_uses_local_output(self, mock_info, mock_db):
        """INPUT_DIRECTORY 外的檔案，output 應放在同目錄"""
        import tempfile
        other_dir = Path(tempfile.mkdtemp())
        try:
            other_file = other_dir / 'external.mp4'
            other_file.touch()
            mock_info.return_value = {'resolution': '1280x720', 'width': 1280, 'height': 720}
            mock_db.execute_query.return_value = 1
            with patch('builtins.print'), \
                 patch.dict('os.environ', {'INPUT_DIRECTORY': str(self.tmp),
                                           'OUTPUT_DIRECTORY': str(self.tmp / 'out')}):
                from conv_admin import cmd_add_file
                cmd_add_file([str(other_file)])
            insert_calls = [c for c in mock_db.execute_query.call_args_list
                            if 'INSERT' in str(c)]
            self.assertEqual(len(insert_calls), 1)
            # output should be in same dir as input (not in OUTPUT_DIRECTORY)
            self.assertIn(str(other_dir), str(insert_calls[0]))
        finally:
            import shutil
            shutil.rmtree(other_dir, ignore_errors=True)


class TestDryRunResetTask(unittest.TestCase):
    """cmd_reset_task() --dry-run — 顯示會重設的任務但不寫入 DB"""

    @patch('task_manager.db_manager')
    def test_dry_run_does_not_update_db(self, mock_db):
        """dry_run=True 時不應執行 UPDATE"""
        mock_db.execute_query.side_effect = [
            [{'id': 42, 'input_path': '/a.mp4', 'output_path': '/out/a.mp4',
              'status': 'failed', 'retry_count': 3, 'error_message': 'timeout'}],
        ]
        with patch('builtins.print'):
            from conv_admin import cmd_reset_task
            cmd_reset_task([42], dry_run=True)
        update_calls = [c for c in mock_db.execute_query.call_args_list
                        if 'UPDATE' in str(c)]
        self.assertEqual(len(update_calls), 0)

    @patch('task_manager.db_manager')
    def test_dry_run_prints_would_reset(self, mock_db):
        """dry_run=True 時應印出 Dry-run 提示與數量"""
        mock_db.execute_query.return_value = [
            {'id': 5, 'input_path': '/v.mp4', 'output_path': '/out/v.mp4',
             'status': 'failed', 'retry_count': 1, 'error_message': None}
        ]
        printed = []
        with patch('builtins.print', side_effect=lambda *a, **kw: printed.append(' '.join(str(x) for x in a))):
            from conv_admin import cmd_reset_task
            cmd_reset_task([5], dry_run=True)
        self.assertTrue(any('Dry-run' in line for line in printed))


class TestDryRunAddFile(unittest.TestCase):
    """cmd_add_file() --dry-run — 顯示會新增的檔案但不寫入 DB"""

    def setUp(self):
        import tempfile
        self.tmp = Path(tempfile.mkdtemp())
        self.video = self.tmp / 'test.mp4'
        self.video.touch()

    def tearDown(self):
        import shutil
        shutil.rmtree(self.tmp, ignore_errors=True)

    @patch('task_manager.db_manager')
    @patch('conv_admin.get_video_info')
    def test_dry_run_does_not_insert(self, mock_info, mock_db):
        """dry_run=True 時不應執行 INSERT"""
        mock_info.return_value = {'resolution': '1920x1080', 'width': 1920, 'height': 1080}
        with patch('builtins.print'), \
             patch.dict('os.environ', {'INPUT_DIRECTORY': str(self.tmp),
                                       'OUTPUT_DIRECTORY': str(self.tmp / 'out')}):
            from conv_admin import cmd_add_file
            cmd_add_file([str(self.video)], dry_run=True)
        insert_calls = [c for c in mock_db.execute_query.call_args_list
                        if 'INSERT' in str(c)]
        self.assertEqual(len(insert_calls), 0)

    @patch('task_manager.db_manager')
    @patch('conv_admin.get_video_info')
    def test_dry_run_prints_would_add(self, mock_info, mock_db):
        """dry_run=True 時應印出 DRY-RUN 提示與輸出路徑"""
        mock_info.return_value = {'resolution': '1280x720', 'width': 1280, 'height': 720}
        printed = []
        with patch('builtins.print', side_effect=lambda *a, **kw: printed.append(' '.join(str(x) for x in a))), \
             patch.dict('os.environ', {'INPUT_DIRECTORY': str(self.tmp),
                                       'OUTPUT_DIRECTORY': str(self.tmp / 'out')}):
            from conv_admin import cmd_add_file
            cmd_add_file([str(self.video)], dry_run=True)
        self.assertTrue(any('DRY-RUN' in line for line in printed))
        self.assertTrue(any('480p_test.mp4' in line for line in printed))

    @patch('conv_admin.get_video_info')
    def test_dry_run_skips_missing_file(self, mock_info):
        """不存在的檔案在 dry-run 模式下亦應被跳過"""
        with patch('builtins.print'), \
             patch.dict('os.environ', {'INPUT_DIRECTORY': str(self.tmp),
                                       'OUTPUT_DIRECTORY': str(self.tmp / 'out')}):
            from conv_admin import cmd_add_file
            cmd_add_file(['/nonexistent/video.mp4'], dry_run=True)
        mock_info.assert_not_called()


# ---------------------------------------------------------------------------
# cmd_stats
# ---------------------------------------------------------------------------

class TestCmdStats(unittest.TestCase):
    """cmd_stats() — 顯示任務統計、目前轉檔中任務與最近失敗任務"""

    def _make_stats(self, total=10, pending=3, processing=2, completed=4, failed=1,
                    retried=0, avg_duration=120):
        return {
            'total': total, 'pending': pending, 'processing': processing,
            'completed': completed, 'failed': failed,
            'retried': retried, 'avg_duration': avg_duration,
        }

    def _setup_mock_db(self, mock_db, stats, processing_tasks=None, failed_tasks=None):
        """設定 mock_db：dialect 回傳佔位字串；execute_query 依序回傳各查詢結果。"""
        mock_db.dialect.timestampdiff_seconds.return_value = '0'
        processing_tasks = processing_tasks if processing_tasks is not None else []
        failed_tasks = failed_tasks if failed_tasks is not None else []
        mock_db.execute_query.side_effect = [[stats], processing_tasks, failed_tasks]

    @patch('task_manager.db_manager')
    def test_prints_current_datetime(self, mock_db):
        """標題列應包含當前日期時間"""
        self._setup_mock_db(mock_db, self._make_stats())
        printed = []
        with patch('builtins.print', side_effect=lambda *a, **kw: printed.append(' '.join(str(x) for x in a))):
            from conv_admin import cmd_stats
            cmd_stats()
        header = next((l for l in printed if 'Task Statistics' in l), '')
        import re
        self.assertTrue(re.search(r'\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}', header))

    @patch('task_manager.db_manager')
    def test_prints_processing_tasks(self, mock_db):
        """目前轉檔中的任務資訊應被印出"""
        processing = [
            {'id': 7, 'input_path': '/mnt/video.mp4',
             'start_time': '2026-01-01 00:00:00', 'retry_count': 0},
        ]
        self._setup_mock_db(mock_db, self._make_stats(processing=1), processing_tasks=processing)
        printed = []
        with patch('builtins.print', side_effect=lambda *a, **kw: printed.append(' '.join(str(x) for x in a))):
            from conv_admin import cmd_stats
            cmd_stats()
        self.assertTrue(any('video.mp4' in l for l in printed))
        self.assertTrue(any('Currently processing' in l for l in printed))

    @patch('task_manager.db_manager')
    def test_no_processing_section_when_empty(self, mock_db):
        """無轉檔中任務時不應印出 Currently processing 段落"""
        self._setup_mock_db(mock_db, self._make_stats(processing=0), processing_tasks=[])
        printed = []
        with patch('builtins.print', side_effect=lambda *a, **kw: printed.append(' '.join(str(x) for x in a))):
            from conv_admin import cmd_stats
            cmd_stats()
        self.assertFalse(any('Currently processing' in l for l in printed))

    @patch('task_manager.db_manager')
    def test_failed_limit_controls_recent_failed_count(self, mock_db):
        """failed_limit 參數應傳遞給 get_recent_failed_tasks()"""
        failed_tasks = [
            {'id': i, 'input_path': f'/f{i}.mp4', 'error_message': 'err',
             'retry_count': 1, 'updated_at': '2026-01-01'}
            for i in range(3)
        ]
        self._setup_mock_db(mock_db, self._make_stats(failed=3), failed_tasks=failed_tasks)
        with patch('builtins.print'):
            from conv_admin import cmd_stats
            cmd_stats(failed_limit=3)
        # 第 3 次 execute_query 呼叫（get_recent_failed_tasks）應帶 limit=3
        calls = mock_db.execute_query.call_args_list
        last_params = calls[-1][0][1]
        self.assertIn(3, last_params)

    @patch('task_manager.db_manager')
    def test_failed_limit_zero_skips_failed_section(self, mock_db):
        """failed_limit=0 時不應查詢或印出失敗任務"""
        mock_db.dialect.timestampdiff_seconds.return_value = '0'
        mock_db.execute_query.side_effect = [[self._make_stats(failed=5)], []]
        printed = []
        with patch('builtins.print', side_effect=lambda *a, **kw: printed.append(' '.join(str(x) for x in a))):
            from conv_admin import cmd_stats
            cmd_stats(failed_limit=0)
        self.assertFalse(any('Recent failed' in l for l in printed))
        # 只應有 2 次 execute_query：統計 + 轉檔中任務；不應有第 3 次
        self.assertEqual(mock_db.execute_query.call_count, 2)

    @patch('task_manager.db_manager')
    def test_no_data_prints_message(self, mock_db):
        """get_task_statistics 回傳 None 時應印出 No data 訊息"""
        mock_db.dialect.timestampdiff_seconds.return_value = '0'
        mock_db.execute_query.return_value = []
        printed = []
        with patch('builtins.print', side_effect=lambda *a, **kw: printed.append(' '.join(str(x) for x in a))):
            from conv_admin import cmd_stats
            cmd_stats()
        self.assertTrue(any('No data' in l for l in printed))


class TestCmdTaskInfo(unittest.TestCase):
    """cmd_task_info() — 顯示指定 task ID 的完整資訊"""

    def _make_task(self, **kwargs):
        defaults = {
            'id': 42,
            'input_path': '/nas/video.avi',
            'output_path': '/out/480p_video.mp4',
            'source_resolution': '720x480',
            'target_resolution': '480p',
            'status': 'completed',
            'progress': 100.00,
            'is_processing': False,
            'start_time': '2024-01-01 10:00:00',
            'end_time': '2024-01-01 10:30:00',
            'error_message': None,
            'retry_count': 0,
            'created_at': '2024-01-01 09:00:00',
            'updated_at': '2024-01-01 10:30:00',
        }
        defaults.update(kwargs)
        return defaults

    @patch('task_manager.db_manager')
    def test_shows_task_info(self, mock_db):
        """有效 task ID 應印出欄位資訊"""
        mock_db.execute_query.return_value = [self._make_task()]
        printed = []
        with patch('builtins.print', side_effect=lambda *a, **kw: printed.append(' '.join(str(x) for x in a))), \
             patch('pathlib.Path.exists', return_value=False):
            from conv_admin import cmd_task_info
            cmd_task_info([42])
        output = '\n'.join(printed)
        self.assertIn('Task 42', output)
        self.assertIn('completed', output)
        self.assertIn('720x480', output)
        self.assertIn('480p', output)

    @patch('task_manager.db_manager')
    def test_not_found_prints_error(self, mock_db):
        """不存在的 task ID 應印出錯誤訊息，不拋出例外"""
        mock_db.execute_query.return_value = []
        printed = []
        with patch('builtins.print', side_effect=lambda *a, **kw: printed.append(' '.join(str(x) for x in a))):
            from conv_admin import cmd_task_info
            cmd_task_info([999])
        output = '\n'.join(printed)
        self.assertIn('999', output)
        self.assertIn('not found', output)

    @patch('task_manager.db_manager')
    def test_shows_error_message_when_present(self, mock_db):
        """有 error_message 的任務應印出錯誤訊息"""
        mock_db.execute_query.return_value = [
            self._make_task(status='failed', error_message='NFS stall detected')
        ]
        printed = []
        with patch('builtins.print', side_effect=lambda *a, **kw: printed.append(' '.join(str(x) for x in a))), \
             patch('pathlib.Path.exists', return_value=False):
            from conv_admin import cmd_task_info
            cmd_task_info([42])
        self.assertTrue(any('NFS stall detected' in l for l in printed))

    @patch('task_manager.db_manager')
    def test_multiple_ids_all_queried(self, mock_db):
        """多個 task ID 應各自查詢 DB"""
        mock_db.execute_query.side_effect = [
            [self._make_task(id=1)],
            [self._make_task(id=2)],
        ]
        with patch('builtins.print'), \
             patch('pathlib.Path.exists', return_value=False):
            from conv_admin import cmd_task_info
            cmd_task_info([1, 2])
        self.assertEqual(mock_db.execute_query.call_count, 2)

    @patch('task_manager.db_manager')
    def test_elapsed_computed_from_timestamps(self, mock_db):
        """start/end_time 齊全時應計算並顯示 elapsed"""
        mock_db.execute_query.return_value = [
            self._make_task(start_time='2024-01-01 10:00:00', end_time='2024-01-01 11:05:30')
        ]
        printed = []
        with patch('builtins.print', side_effect=lambda *a, **kw: printed.append(' '.join(str(x) for x in a))), \
             patch('pathlib.Path.exists', return_value=False):
            from conv_admin import cmd_task_info
            cmd_task_info([42])
        output = '\n'.join(printed)
        self.assertIn('1h', output)
        self.assertIn('5m', output)


if __name__ == '__main__':
    unittest.main()
