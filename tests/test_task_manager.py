"""
Unit tests for task_manager.TaskRepository
每個 public method 至少涵蓋：正常路徑、邊界條件、DB 例外三種情境。
"""
import sys
import unittest
from pathlib import Path
from unittest.mock import patch, MagicMock

sys.path.insert(0, str(Path(__file__).parent.parent))

from task_manager import TaskRepository, find_orphaned_ffmpeg_candidates


def _repo():
    return TaskRepository()


# ---------------------------------------------------------------------------
# get_pending_tasks
# ---------------------------------------------------------------------------

class TestGetPendingTasks(unittest.TestCase):

    @patch('task_manager.db_manager')
    def test_returns_task_list(self, mock_db):
        mock_db.execute_query.return_value = [
            {'id': 1, 'input_path': '/a.mp4', 'output_path': '/out/a.mp4', 'source_resolution': '1920x1080'},
            {'id': 2, 'input_path': '/b.mp4', 'output_path': '/out/b.mp4', 'source_resolution': '1280x720'},
        ]
        result = _repo().get_pending_tasks()
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0]['id'], 1)

    @patch('task_manager.db_manager')
    def test_limit_passed_to_query(self, mock_db):
        mock_db.execute_query.return_value = []
        _repo().get_pending_tasks(limit=5)
        args = mock_db.execute_query.call_args[0]
        self.assertIn(5, args[1])

    @patch('task_manager.db_manager')
    def test_db_error_returns_empty_list(self, mock_db):
        mock_db.execute_query.side_effect = Exception('conn error')
        self.assertEqual(_repo().get_pending_tasks(), [])


# ---------------------------------------------------------------------------
# get_task_by_id
# ---------------------------------------------------------------------------

class TestGetTaskById(unittest.TestCase):

    @patch('task_manager.db_manager')
    def test_returns_task_when_found(self, mock_db):
        mock_db.execute_query.return_value = [{'input_path': '/a.mp4', 'output_path': '/out/a.mp4'}]
        result = _repo().get_task_by_id(42)
        self.assertEqual(result['input_path'], '/a.mp4')

    @patch('task_manager.db_manager')
    def test_returns_none_when_not_found(self, mock_db):
        mock_db.execute_query.return_value = []
        self.assertIsNone(_repo().get_task_by_id(999))

    @patch('task_manager.db_manager')
    def test_db_error_returns_none(self, mock_db):
        mock_db.execute_query.side_effect = Exception('timeout')
        self.assertIsNone(_repo().get_task_by_id(1))


# ---------------------------------------------------------------------------
# get_task_detail
# ---------------------------------------------------------------------------

class TestGetTaskDetail(unittest.TestCase):

    @patch('task_manager.db_manager')
    def test_returns_full_detail(self, mock_db):
        mock_db.execute_query.return_value = [{
            'id': 42, 'input_path': '/a.mp4', 'output_path': '/out/a.mp4',
            'status': 'failed', 'retry_count': 3, 'error_message': 'timeout'
        }]
        result = _repo().get_task_detail(42)
        self.assertEqual(result['status'], 'failed')
        self.assertEqual(result['retry_count'], 3)

    @patch('task_manager.db_manager')
    def test_returns_none_when_not_found(self, mock_db):
        mock_db.execute_query.return_value = []
        self.assertIsNone(_repo().get_task_detail(999))

    @patch('task_manager.db_manager')
    def test_db_error_returns_none(self, mock_db):
        mock_db.execute_query.side_effect = Exception('db down')
        self.assertIsNone(_repo().get_task_detail(1))


# ---------------------------------------------------------------------------
# get_task_statistics
# ---------------------------------------------------------------------------

class TestGetTaskStatistics(unittest.TestCase):

    @patch('task_manager.db_manager')
    def test_returns_stats_dict(self, mock_db):
        stats = {'total': 100, 'pending': 50, 'processing': 5,
                 'completed': 40, 'failed': 5, 'retried': 3, 'avg_duration': 30.0}
        mock_db.execute_query.return_value = [stats]
        result = _repo().get_task_statistics()
        self.assertEqual(result['total'], 100)
        self.assertEqual(result['avg_duration'], 30.0)

    @patch('task_manager.db_manager')
    def test_returns_none_when_empty(self, mock_db):
        mock_db.execute_query.return_value = []
        self.assertIsNone(_repo().get_task_statistics())

    @patch('task_manager.db_manager')
    def test_db_error_returns_none(self, mock_db):
        mock_db.execute_query.side_effect = Exception('db down')
        self.assertIsNone(_repo().get_task_statistics())


# ---------------------------------------------------------------------------
# get_task_by_input_path
# ---------------------------------------------------------------------------

class TestGetTaskByInputPath(unittest.TestCase):

    @patch('task_manager.db_manager')
    def test_returns_task_when_found(self, mock_db):
        mock_db.execute_query.return_value = [{'id': 7, 'status': 'completed', 'output_path': '/out/a.mp4'}]
        result = _repo().get_task_by_input_path('/a.mp4')
        self.assertEqual(result['id'], 7)

    @patch('task_manager.db_manager')
    def test_returns_none_when_not_found(self, mock_db):
        mock_db.execute_query.return_value = []
        self.assertIsNone(_repo().get_task_by_input_path('/unknown.mp4'))

    @patch('task_manager.db_manager')
    def test_db_error_returns_none(self, mock_db):
        mock_db.execute_query.side_effect = Exception('err')
        self.assertIsNone(_repo().get_task_by_input_path('/a.mp4'))


# ---------------------------------------------------------------------------
# requeue_missing_output
# ---------------------------------------------------------------------------

class TestRequeueMissingOutput(unittest.TestCase):

    @patch('task_manager.db_manager')
    def test_executes_update(self, mock_db):
        mock_db.execute_query.return_value = 1
        _repo().requeue_missing_output('/a.mp4')
        query = mock_db.execute_query.call_args[0][0]
        self.assertIn('pending', query.lower())
        self.assertIn('is_processing', query)

    @patch('task_manager.db_manager')
    def test_input_path_passed_as_param(self, mock_db):
        mock_db.execute_query.return_value = 1
        _repo().requeue_missing_output('/videos/foo.mp4')
        params = mock_db.execute_query.call_args[0][1]
        self.assertIn('/videos/foo.mp4', params)

    @patch('task_manager.db_manager')
    def test_db_error_does_not_raise(self, mock_db):
        mock_db.execute_query.side_effect = Exception('err')
        _repo().requeue_missing_output('/a.mp4')  # should not raise


# ---------------------------------------------------------------------------
# insert_task
# ---------------------------------------------------------------------------

class TestInsertTask(unittest.TestCase):

    @patch('task_manager.db_manager')
    def test_returns_rows_affected_on_insert(self, mock_db):
        mock_db.execute_query.return_value = 1
        result = _repo().insert_task('/a.mp4', '/out/a.mp4', '1920x1080')
        self.assertEqual(result, 1)

    @patch('task_manager.db_manager')
    def test_returns_zero_on_duplicate(self, mock_db):
        mock_db.execute_query.return_value = 0
        result = _repo().insert_task('/a.mp4', '/out/a.mp4', '1920x1080')
        self.assertEqual(result, 0)

    @patch('task_manager.db_manager')
    def test_uses_insert_ignore(self, mock_db):
        mock_db.execute_query.return_value = 1
        _repo().insert_task('/a.mp4', '/out/a.mp4', '1920x1080')
        query = mock_db.execute_query.call_args[0][0]
        self.assertIn('INSERT IGNORE', query)

    @patch('task_manager.db_manager')
    def test_status_is_pending(self, mock_db):
        mock_db.execute_query.return_value = 1
        _repo().insert_task('/a.mp4', '/out/a.mp4', '1920x1080')
        query = mock_db.execute_query.call_args[0][0]
        self.assertIn("'pending'", query)

    @patch('task_manager.db_manager')
    def test_db_error_returns_zero(self, mock_db):
        mock_db.execute_query.side_effect = Exception('err')
        self.assertEqual(_repo().insert_task('/a.mp4', '/out/a.mp4', '1920x1080'), 0)


# ---------------------------------------------------------------------------
# get_maxed_failed_tasks
# ---------------------------------------------------------------------------

class TestGetMaxedFailedTasks(unittest.TestCase):

    @patch('task_manager.db_manager')
    def test_returns_task_list(self, mock_db):
        mock_db.execute_query.return_value = [
            {'id': 1, 'input_path': '/a.mp4', 'retry_count': 3, 'error_message': 'err'},
        ]
        result = _repo().get_maxed_failed_tasks(max_retries=3)
        self.assertEqual(len(result), 1)

    @patch('task_manager.db_manager')
    def test_max_retries_passed_to_query(self, mock_db):
        mock_db.execute_query.return_value = []
        _repo().get_maxed_failed_tasks(max_retries=5)
        params = mock_db.execute_query.call_args[0][1]
        self.assertIn(5, params)

    @patch('task_manager.db_manager')
    def test_db_error_returns_empty_list(self, mock_db):
        mock_db.execute_query.side_effect = Exception('err')
        self.assertEqual(_repo().get_maxed_failed_tasks(), [])


# ---------------------------------------------------------------------------
# get_recent_failed_tasks
# ---------------------------------------------------------------------------

class TestGetRecentFailedTasks(unittest.TestCase):

    @patch('task_manager.db_manager')
    def test_returns_task_list(self, mock_db):
        mock_db.execute_query.return_value = [
            {'id': 10, 'input_path': '/a.mp4', 'error_message': 'timeout',
             'retry_count': 1, 'updated_at': '2026-01-01 00:00:00'},
        ]
        result = _repo().get_recent_failed_tasks(limit=5)
        self.assertEqual(result[0]['id'], 10)

    @patch('task_manager.db_manager')
    def test_limit_passed_to_query(self, mock_db):
        mock_db.execute_query.return_value = []
        _repo().get_recent_failed_tasks(limit=3)
        params = mock_db.execute_query.call_args[0][1]
        self.assertIn(3, params)

    @patch('task_manager.db_manager')
    def test_orders_by_updated_at_desc(self, mock_db):
        mock_db.execute_query.return_value = []
        _repo().get_recent_failed_tasks()
        query = mock_db.execute_query.call_args[0][0]
        self.assertIn('updated_at DESC', query)

    @patch('task_manager.db_manager')
    def test_db_error_returns_empty_list(self, mock_db):
        mock_db.execute_query.side_effect = Exception('err')
        self.assertEqual(_repo().get_recent_failed_tasks(), [])


# ---------------------------------------------------------------------------
# reset_tasks_to_pending
# ---------------------------------------------------------------------------

class TestResetTasksToPending(unittest.TestCase):

    @patch('task_manager.db_manager')
    def test_returns_actual_rowcount(self, mock_db):
        """回傳 DB 實際更新的 rowcount，而非 len(task_ids)"""
        mock_db.execute_query.return_value = 2  # 只有 2 筆實際更新（其中 1 筆不存在）
        result = _repo().reset_tasks_to_pending([1, 2, 3])
        self.assertEqual(result, 2)

    @patch('task_manager.db_manager')
    def test_query_sets_pending_and_zeroes_retry(self, mock_db):
        mock_db.execute_query.return_value = 1
        _repo().reset_tasks_to_pending([42])
        query = mock_db.execute_query.call_args[0][0]
        self.assertIn('pending', query.lower())
        self.assertIn('retry_count=0', query)

    @patch('task_manager.db_manager')
    def test_placeholders_match_id_count(self, mock_db):
        mock_db.execute_query.return_value = 2
        # concat 表達式含一個 %s 佔位符，模擬真實後端行為
        mock_db.dialect.concat.return_value = '%s'
        _repo().reset_tasks_to_pending([10, 20])
        query = mock_db.execute_query.call_args[0][0]
        params = mock_db.execute_query.call_args[0][1]
        # params = (reason_string, id1, id2) → 1 reason %s + len(ids) %s
        self.assertEqual(query.count('%s'), len(params))

    @patch('task_manager.db_manager')
    def test_custom_reason_appears_in_query_params(self, mock_db):
        """reason 參數應出現在 SQL 參數中"""
        mock_db.execute_query.return_value = 1
        _repo().reset_tasks_to_pending([5], reason='manual reset via --reset-task')
        params = mock_db.execute_query.call_args[0][1]
        self.assertIn('manual reset via --reset-task', params[0])

    @patch('task_manager.db_manager')
    def test_db_error_returns_zero(self, mock_db):
        mock_db.execute_query.side_effect = Exception('err')
        self.assertEqual(_repo().reset_tasks_to_pending([1, 2]), 0)

    @patch('task_manager.db_manager')
    def test_empty_list_returns_zero_without_query(self, mock_db):
        result = _repo().reset_tasks_to_pending([])
        self.assertEqual(result, 0)
        mock_db.execute_query.assert_not_called()


# ---------------------------------------------------------------------------
# cleanup_orphaned_flags
# ---------------------------------------------------------------------------

class TestCleanupOrphanedFlags(unittest.TestCase):

    @patch('task_manager.db_manager')
    def test_returns_sum_of_both_zombie_types(self, mock_db):
        """type-1（is_processing=TRUE）+ type-2（is_processing=FALSE）兩者之和"""
        mock_db.execute_query.side_effect = [3, 2]
        self.assertEqual(_repo().cleanup_orphaned_flags(), 5)

    @patch('task_manager.db_manager')
    def test_returns_zero_when_nothing_to_clean(self, mock_db):
        mock_db.execute_query.side_effect = [0, 0]
        self.assertEqual(_repo().cleanup_orphaned_flags(), 0)

    @patch('task_manager.db_manager')
    def test_type1_query_resets_status_and_clears_lock(self, mock_db):
        """type-1：WHERE is_processing=TRUE → 同時重設 status 與 is_processing"""
        mock_db.execute_query.side_effect = [0, 0]
        _repo().cleanup_orphaned_flags()
        first_query = mock_db.execute_query.call_args_list[0][0][0]
        self.assertIn("status = 'pending'", first_query)
        self.assertIn("is_processing = FALSE", first_query)
        self.assertIn("is_processing = TRUE", first_query)

    @patch('task_manager.db_manager')
    def test_type2_query_targets_unlocked_zombie(self, mock_db):
        """type-2：WHERE is_processing=FALSE AND updated_at < threshold → 重設 status"""
        mock_db.execute_query.side_effect = [0, 0]
        _repo().cleanup_orphaned_flags()
        second_query = mock_db.execute_query.call_args_list[1][0][0]
        self.assertIn("status = 'pending'", second_query)
        self.assertIn("is_processing = FALSE", second_query)
        # 驗證 interval_ago 以 5 分鐘為參數呼叫（實際表達式由 SqlDialect 負責，此處不重複測試）
        mock_db.dialect.interval_ago.assert_called_once_with(5)

    @patch('task_manager.db_manager')
    def test_db_error_returns_zero(self, mock_db):
        mock_db.execute_query.side_effect = Exception('err')
        self.assertEqual(_repo().cleanup_orphaned_flags(), 0)


# ---------------------------------------------------------------------------
# find_orphaned_ffmpeg_candidates (module-level function)
# ---------------------------------------------------------------------------

class TestFindOrphanedFfmpegCandidates(unittest.TestCase):

    def _make_proc(self, pid, cmdline):
        p = MagicMock()
        p.pid = pid
        p.info = {'name': 'ffmpeg', 'cmdline': cmdline}
        return p

    @patch('task_manager.db_manager')
    @patch('task_manager.find_orphaned_ffmpeg_candidates.__module__')
    def test_returns_candidate_for_active_task(self, _mod, mock_db):
        """pending/processing タスクのfmpeg → candidate に含まれる"""
        import sys
        import types
        # psutil mock
        psutil_mock = types.ModuleType('psutil')
        proc = self._make_proc(1234, ['ffmpeg', '-i', '/videos/foo.mp4', '/out/foo.mp4'])
        psutil_mock.process_iter = MagicMock(return_value=[proc])
        psutil_mock.NoSuchProcess = Exception
        psutil_mock.AccessDenied = Exception
        mock_db.execute_query.return_value = [{'id': 7, 'status': 'processing', 'output_path': '/out/foo.mp4'}]
        with patch.dict(sys.modules, {'psutil': psutil_mock}):
            result = find_orphaned_ffmpeg_candidates(_repo(), excluded_pids=set())
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]['pid'], 1234)
        self.assertEqual(result[0]['task_id'], 7)

    @patch('task_manager.db_manager')
    def test_excludes_pids_in_excluded_set(self, mock_db):
        """excluded_pids に含まれる PID はスキップ"""
        import sys
        import types
        psutil_mock = types.ModuleType('psutil')
        proc = self._make_proc(5555, ['ffmpeg', '-i', '/v/a.mp4', '/out/a.mp4'])
        psutil_mock.process_iter = MagicMock(return_value=[proc])
        psutil_mock.NoSuchProcess = Exception
        psutil_mock.AccessDenied = Exception
        with patch.dict(sys.modules, {'psutil': psutil_mock}):
            result = find_orphaned_ffmpeg_candidates(_repo(), excluded_pids={5555})
        self.assertEqual(result, [])
        mock_db.execute_query.assert_not_called()

    @patch('task_manager.db_manager')
    def test_skips_completed_task(self, mock_db):
        """status=completed のタスク → candidate に含まれない"""
        import sys
        import types
        psutil_mock = types.ModuleType('psutil')
        proc = self._make_proc(2222, ['ffmpeg', '-i', '/v/done.mp4', '/out/done.mp4'])
        psutil_mock.process_iter = MagicMock(return_value=[proc])
        psutil_mock.NoSuchProcess = Exception
        psutil_mock.AccessDenied = Exception
        mock_db.execute_query.return_value = [{'id': 3, 'status': 'completed', 'output_path': '/out/done.mp4'}]
        with patch.dict(sys.modules, {'psutil': psutil_mock}):
            result = find_orphaned_ffmpeg_candidates(_repo(), excluded_pids=set())
        self.assertEqual(result, [])

    def test_returns_empty_if_psutil_missing(self):
        """psutil 未インストールの場合は空リストを返す"""
        import sys
        with patch.dict(sys.modules, {'psutil': None}):
            result = find_orphaned_ffmpeg_candidates(_repo(), excluded_pids=set())
        self.assertEqual(result, [])


# ---------------------------------------------------------------------------
# SQLite integration tests — use a real temporary SQLite database
# ---------------------------------------------------------------------------

class TestTaskRepositorySQLite(unittest.TestCase):
    """TaskRepository 整合測試：使用真實的 SQLite 暫存資料庫。

    透過 tempfile 建立獨立的 SQLite 檔案，確保 init_db 與 db_manager
    操作的是同一個資料庫，完整測試 SQL 方言相容性。
    """

    def setUp(self):
        import os
        import tempfile
        self._tmp = tempfile.NamedTemporaryFile(suffix='.db', delete=False)
        self._tmp.close()
        self._db_path = self._tmp.name

        self._env_backup = {
            'DB_TYPE': os.getenv('DB_TYPE'),
            'DB_PATH': os.getenv('DB_PATH'),
        }
        os.environ['DB_TYPE'] = 'sqlite'
        os.environ['DB_PATH'] = self._db_path

        # 重載模組，確保使用新的環境變數
        for mod in ('db_manager', 'task_manager', 'init_db'):
            sys.modules.pop(mod, None)

        from init_db import init_database
        init_database()

        # 重設 db_manager（init_db 建立的連線與 db_manager 使用同一個檔案）
        sys.modules.pop('db_manager', None)
        sys.modules.pop('task_manager', None)

        from task_manager import TaskRepository as TR
        self.TaskRepository = TR

    def tearDown(self):
        import os
        for key, value in self._env_backup.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value
        for mod in ('db_manager', 'task_manager', 'init_db'):
            sys.modules.pop(mod, None)
        try:
            os.unlink(self._db_path)
        except OSError:
            pass

    def _repo(self):
        return self.TaskRepository()

    def _insert_task(self, input_path='/input/video.mp4', output_path='/output/video.mp4',
                     status='pending', retry_count=0):
        """輔助方法：直接插入一筆測試任務"""
        import db_manager as dm
        dm.db_manager.execute_query(
            "INSERT INTO conversion_tasks (input_path, output_path, status, retry_count) "
            "VALUES (%s, %s, %s, %s)",
            params=(input_path, output_path, status, retry_count),
        )
        rows = dm.db_manager.execute_query(
            "SELECT id FROM conversion_tasks WHERE input_path=%s", params=(input_path,), fetch=True
        )
        return rows[0]['id']

    def test_add_task(self):
        """測試 insert_task 新增任務"""
        repo = self._repo()
        repo.insert_task('/in/a.mp4', '/out/a.mp4', '1920x1080')
        import db_manager as dm
        rows = dm.db_manager.execute_query(
            "SELECT * FROM conversion_tasks WHERE input_path='/in/a.mp4'", fetch=True
        )
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]['status'], 'pending')
        self.assertEqual(rows[0]['source_resolution'], '1920x1080')

    def test_add_task_duplicate_ignored(self):
        """測試重複 insert_task 不拋出例外（INSERT OR IGNORE）"""
        repo = self._repo()
        repo.insert_task('/in/dup.mp4', '/out/dup.mp4', '1280x720')
        repo.insert_task('/in/dup.mp4', '/out/dup.mp4', '1280x720')  # 重複應被 IGNORE
        import db_manager as dm
        rows = dm.db_manager.execute_query(
            "SELECT * FROM conversion_tasks WHERE input_path='/in/dup.mp4'", fetch=True
        )
        self.assertEqual(len(rows), 1)

    def test_get_pending_tasks(self):
        """測試 get_pending_tasks 回傳 pending 任務"""
        self._insert_task('/in/p1.mp4', '/out/p1.mp4', status='pending')
        self._insert_task('/in/p2.mp4', '/out/p2.mp4', status='completed')
        repo = self._repo()
        result = repo.get_pending_tasks(limit=10)
        paths = [r['input_path'] for r in result]
        self.assertIn('/in/p1.mp4', paths)
        self.assertNotIn('/in/p2.mp4', paths)

    def test_acquire_task_lock(self):
        """測試 acquire_task_lock 正確設定 is_processing"""
        task_id = self._insert_task('/in/lock.mp4', '/out/lock.mp4')
        repo = self._repo()
        success = repo.acquire_task_lock(task_id, 'worker-1')
        self.assertTrue(success)
        import db_manager as dm
        rows = dm.db_manager.execute_query(
            "SELECT is_processing FROM conversion_tasks WHERE id=%s", params=(task_id,), fetch=True
        )
        self.assertEqual(rows[0]['is_processing'], 1)

    def test_acquire_task_lock_already_locked(self):
        """測試已上鎖的任務無法再次取得鎖"""
        task_id = self._insert_task('/in/lock2.mp4', '/out/lock2.mp4')
        repo = self._repo()
        repo.acquire_task_lock(task_id, 'worker-1')
        success2 = repo.acquire_task_lock(task_id, 'worker-2')
        self.assertFalse(success2)

    def test_update_task_status(self):
        """測試 update_task_status 更新狀態"""
        task_id = self._insert_task('/in/upd.mp4', '/out/upd.mp4')
        repo = self._repo()
        repo.update_task_status(task_id, 'processing')
        import db_manager as dm
        rows = dm.db_manager.execute_query(
            "SELECT status FROM conversion_tasks WHERE id=%s", params=(task_id,), fetch=True
        )
        self.assertEqual(rows[0]['status'], 'processing')

    def test_complete_task(self):
        """測試 update_task_status completed 將狀態設為 completed 並清除 is_processing"""
        task_id = self._insert_task('/in/done.mp4', '/out/done.mp4', status='processing')
        repo = self._repo()
        repo.update_task_status(task_id, 'completed')
        import db_manager as dm
        rows = dm.db_manager.execute_query(
            "SELECT status, is_processing FROM conversion_tasks WHERE id=%s",
            params=(task_id,), fetch=True
        )
        self.assertEqual(rows[0]['status'], 'completed')
        self.assertEqual(rows[0]['is_processing'], 0)

    def test_fail_task(self):
        """測試 update_task_status failed 將狀態設為 failed 並記錄錯誤訊息"""
        task_id = self._insert_task('/in/fail.mp4', '/out/fail.mp4', status='processing')
        repo = self._repo()
        repo.update_task_status(task_id, 'failed', error_message='ffmpeg error')
        import db_manager as dm
        rows = dm.db_manager.execute_query(
            "SELECT status, error_message, is_processing FROM conversion_tasks WHERE id=%s",
            params=(task_id,), fetch=True
        )
        self.assertEqual(rows[0]['status'], 'failed')
        self.assertIn('ffmpeg error', rows[0]['error_message'])
        self.assertEqual(rows[0]['is_processing'], 0)

    def test_get_task_statistics(self):
        """測試 get_task_statistics 回傳各狀態計數"""
        self._insert_task('/in/s1.mp4', '/out/s1.mp4', status='pending')
        self._insert_task('/in/s2.mp4', '/out/s2.mp4', status='completed')
        self._insert_task('/in/s3.mp4', '/out/s3.mp4', status='failed')
        repo = self._repo()
        stats = repo.get_task_statistics()
        self.assertIsNotNone(stats)
        self.assertIn('pending', stats)
        self.assertGreaterEqual(stats['pending'], 1)
        self.assertGreaterEqual(stats['completed'], 1)
        self.assertGreaterEqual(stats['failed'], 1)

    def test_retry_failed_tasks(self):
        """測試 retry_failed_tasks 重設失敗任務為 pending"""
        task_id = self._insert_task('/in/retry.mp4', '/out/retry.mp4', status='failed', retry_count=1)
        repo = self._repo()
        count = repo.retry_failed_tasks(max_retries=3)
        import db_manager as dm
        rows = dm.db_manager.execute_query(
            "SELECT status FROM conversion_tasks WHERE id=%s", params=(task_id,), fetch=True
        )
        self.assertEqual(rows[0]['status'], 'pending')

    def test_get_task_by_id(self):
        """測試 get_task_by_id 回傳正確任務"""
        task_id = self._insert_task('/in/byid.mp4', '/out/byid.mp4')
        repo = self._repo()
        task = repo.get_task_by_id(task_id)
        self.assertIsNotNone(task)
        self.assertEqual(task['input_path'], '/in/byid.mp4')

    def test_get_task_by_id_not_found(self):
        """測試 get_task_by_id 找不到時回傳 None"""
        repo = self._repo()
        task = repo.get_task_by_id(99999)
        self.assertIsNone(task)


if __name__ == '__main__':
    unittest.main()
