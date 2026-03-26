"""
Unit tests for daemons/process_daemon.py
測試時間限制、重試、過時任務清理邏輯；DB 呼叫均以 mock 取代。
"""
import sys
import unittest
from datetime import datetime, time as dtime, timedelta
from pathlib import Path
from unittest.mock import MagicMock, patch, call

sys.path.insert(0, str(Path(__file__).parent.parent))


def _make_process_daemon(**env_overrides):
    """建立 ProcessDaemon，注入環境變數以通過 validate_settings()"""
    env = {
        'INPUT_DIRECTORY': '/tmp',
        'OUTPUT_DIRECTORY': '/tmp',
        'ENABLE_TIME_RESTRICTION': 'false',
        'ALLOWED_START_TIME': '22:00',
        'ALLOWED_END_TIME': '06:00',
        'MAX_RETRIES': '3',
        'RETRY_INTERVAL_CYCLES': '10',
        'STALE_HOURS': '1',
    }
    env.update(env_overrides)
    with patch.dict('os.environ', env):
        from daemons.process_daemon import ProcessDaemon
        return ProcessDaemon(check_interval=60, max_workers=1)


class TestParseTime(unittest.TestCase):
    """ProcessDaemon._parse_time() 靜態方法"""

    def setUp(self):
        self.daemon = _make_process_daemon()

    def test_valid_time(self):
        t = self.daemon._parse_time('14:30')
        self.assertEqual(t, dtime(14, 30))

    def test_midnight(self):
        self.assertEqual(self.daemon._parse_time('00:00'), dtime(0, 0))

    def test_invalid_falls_back_to_2200(self):
        self.assertEqual(self.daemon._parse_time('not_a_time'), dtime(22, 0))

    def test_with_leading_space(self):
        self.assertEqual(self.daemon._parse_time(' 08:00'), dtime(8, 0))


class TestIsTimeAllowed(unittest.TestCase):
    """is_time_allowed() — 含跨日時段支援"""

    def _daemon_with_restriction(self, start, end):
        return _make_process_daemon(
            ENABLE_TIME_RESTRICTION='true',
            ALLOWED_START_TIME=start,
            ALLOWED_END_TIME=end,
        )

    def test_restriction_disabled_always_allowed(self):
        d = _make_process_daemon(ENABLE_TIME_RESTRICTION='false')
        with patch('daemons.process_daemon.datetime') as mock_dt:
            mock_dt.now.return_value.time.return_value = dtime(12, 0)
            self.assertTrue(d.is_time_allowed())

    def test_same_day_window_inside(self):
        d = self._daemon_with_restriction('08:00', '22:00')
        with patch('daemons.process_daemon.datetime') as mock_dt:
            mock_dt.now.return_value.time.return_value = dtime(15, 0)
            self.assertTrue(d.is_time_allowed())

    def test_same_day_window_outside(self):
        d = self._daemon_with_restriction('08:00', '22:00')
        with patch('daemons.process_daemon.datetime') as mock_dt:
            mock_dt.now.return_value.time.return_value = dtime(23, 0)
            self.assertFalse(d.is_time_allowed())

    def test_overnight_window_after_start(self):
        """22:00-06:00，現在 23:30 → 允許"""
        d = self._daemon_with_restriction('22:00', '06:00')
        with patch('daemons.process_daemon.datetime') as mock_dt:
            mock_dt.now.return_value.time.return_value = dtime(23, 30)
            self.assertTrue(d.is_time_allowed())

    def test_overnight_window_before_end(self):
        """22:00-06:00，現在 03:00 → 允許"""
        d = self._daemon_with_restriction('22:00', '06:00')
        with patch('daemons.process_daemon.datetime') as mock_dt:
            mock_dt.now.return_value.time.return_value = dtime(3, 0)
            self.assertTrue(d.is_time_allowed())

    def test_overnight_window_outside(self):
        """22:00-06:00，現在 12:00 → 不允許"""
        d = self._daemon_with_restriction('22:00', '06:00')
        with patch('daemons.process_daemon.datetime') as mock_dt:
            mock_dt.now.return_value.time.return_value = dtime(12, 0)
            self.assertFalse(d.is_time_allowed())

    def test_exactly_at_start_time(self):
        """邊界值：剛好等於 start time → 允許"""
        d = self._daemon_with_restriction('22:00', '06:00')
        with patch('daemons.process_daemon.datetime') as mock_dt:
            mock_dt.now.return_value.time.return_value = dtime(22, 0)
            self.assertTrue(d.is_time_allowed())

    def test_exactly_at_end_time(self):
        """邊界值：剛好等於 end time → 允許"""
        d = self._daemon_with_restriction('22:00', '06:00')
        with patch('daemons.process_daemon.datetime') as mock_dt:
            mock_dt.now.return_value.time.return_value = dtime(6, 0)
            self.assertTrue(d.is_time_allowed())


class TestGetTimeUntilAllowed(unittest.TestCase):
    """get_time_until_allowed() — 計算等待秒數"""

    def test_restriction_disabled_returns_zero(self):
        d = _make_process_daemon(ENABLE_TIME_RESTRICTION='false')
        self.assertEqual(d.get_time_until_allowed(), 0)

    def test_already_in_window_returns_zero(self):
        d = _make_process_daemon(
            ENABLE_TIME_RESTRICTION='true',
            ALLOWED_START_TIME='22:00',
            ALLOWED_END_TIME='06:00',
        )
        with patch('daemons.process_daemon.datetime') as mock_dt:
            mock_dt.now.return_value.time.return_value = dtime(23, 0)
            # 讓 is_time_allowed 的內部 datetime.now() 也返回同一時間
            mock_dt.now.return_value = datetime(2024, 1, 1, 23, 0, 0)
            self.assertEqual(d.get_time_until_allowed(), 0)

    def test_outside_window_returns_positive_seconds(self):
        """白天 12:00，等到 22:00 = 10 小時 = 36000 秒"""
        d = _make_process_daemon(
            ENABLE_TIME_RESTRICTION='true',
            ALLOWED_START_TIME='22:00',
            ALLOWED_END_TIME='06:00',
        )
        now = datetime(2024, 1, 1, 12, 0, 0)
        with patch('daemons.process_daemon.datetime') as mock_dt:
            mock_dt.now.return_value = now
            secs = d.get_time_until_allowed()
        self.assertAlmostEqual(secs, 36000, delta=60)

    def test_just_past_start_time_waits_23_hours(self):
        """23:00 剛過，start=22:00 的下一個視窗在明天 22:00 = 23 小時"""
        d = _make_process_daemon(
            ENABLE_TIME_RESTRICTION='true',
            ALLOWED_START_TIME='10:00',
            ALLOWED_END_TIME='18:00',
        )
        now = datetime(2024, 1, 1, 19, 0, 0)  # 19:00，視窗 10-18 已過
        with patch('daemons.process_daemon.datetime') as mock_dt:
            mock_dt.now.return_value = now
            secs = d.get_time_until_allowed()
        self.assertAlmostEqual(secs, 15 * 3600, delta=60)


class TestRetryFailedTasks(unittest.TestCase):
    """retry_failed_tasks() — 重置失敗任務回 pending"""

    @patch('daemons.process_daemon.db_manager')
    def test_retries_tasks_below_max(self, mock_db):
        d = _make_process_daemon(MAX_RETRIES='3')
        mock_db.execute_query.side_effect = [
            [{'id': 1, 'retry_count': 1}, {'id': 2, 'retry_count': 2}],  # SELECT
            1, 1,  # 兩次 UPDATE
        ]
        count = d.retry_failed_tasks()
        self.assertEqual(count, 2)

    @patch('daemons.process_daemon.db_manager')
    def test_no_tasks_returns_zero(self, mock_db):
        d = _make_process_daemon()
        mock_db.execute_query.return_value = []
        self.assertEqual(d.retry_failed_tasks(), 0)

    @patch('daemons.process_daemon.db_manager')
    def test_db_error_returns_zero(self, mock_db):
        d = _make_process_daemon()
        mock_db.execute_query.side_effect = Exception('DB error')
        self.assertEqual(d.retry_failed_tasks(), 0)


class TestCleanupStaleTasks(unittest.TestCase):
    """cleanup_stale_tasks() — 標記卡住超過 STALE_HOURS 的任務為 failed"""

    @patch('daemons.process_daemon.db_manager')
    def test_marks_stale_tasks_as_failed(self, mock_db):
        d = _make_process_daemon(STALE_HOURS='1')
        # SELECT → 2 stale tasks；每個 task 有 2 次 DB 呼叫（UPDATE + DELETE processing_lock）
        mock_db.execute_query.side_effect = [
            [{'id': 10}, {'id': 11}],  # SELECT stale tasks
            1, 1,  # task 10: UPDATE, DELETE
            1, 1,  # task 11: UPDATE, DELETE
        ]
        count = d.cleanup_stale_tasks()
        self.assertEqual(count, 2)

    @patch('daemons.process_daemon.db_manager')
    def test_no_stale_tasks_returns_zero(self, mock_db):
        d = _make_process_daemon()
        mock_db.execute_query.return_value = []
        self.assertEqual(d.cleanup_stale_tasks(), 0)

    @patch('daemons.process_daemon.db_manager')
    def test_stale_threshold_passed_to_query(self, mock_db):
        """確認傳給 DB 的時間閾值約等於現在 - STALE_HOURS"""
        d = _make_process_daemon(STALE_HOURS='2')
        mock_db.execute_query.return_value = []
        before = datetime.now()
        d.cleanup_stale_tasks()
        after = datetime.now()

        # 取出 SELECT query 傳入的時間參數
        select_call = mock_db.execute_query.call_args_list[0]
        threshold_str = select_call[0][1][0]  # 第一個位置參數的第一個元素
        threshold = datetime.strptime(threshold_str, '%Y-%m-%d %H:%M:%S')

        expected_low = before - timedelta(hours=2, seconds=1)
        expected_high = after - timedelta(hours=2) + timedelta(seconds=1)
        self.assertGreater(threshold, expected_low)
        self.assertLess(threshold, expected_high)

    @patch('daemons.process_daemon.db_manager')
    def test_db_error_returns_zero(self, mock_db):
        d = _make_process_daemon()
        mock_db.execute_query.side_effect = Exception('timeout')
        self.assertEqual(d.cleanup_stale_tasks(), 0)


class TestAcquireReleaseLock(unittest.TestCase):
    """acquire_task_lock() / release_task_lock() — 防止重複處理"""

    @patch('daemons.process_daemon.db_manager')
    def test_acquire_success(self, mock_db):
        d = _make_process_daemon()
        mock_db.execute_query.return_value = 1  # 1 row updated
        self.assertTrue(d.acquire_task_lock(task_id=5, worker_id='worker_0'))

    @patch('daemons.process_daemon.db_manager')
    def test_acquire_fail_already_locked(self, mock_db):
        d = _make_process_daemon()
        mock_db.execute_query.return_value = 0  # 0 rows updated = already locked
        self.assertFalse(d.acquire_task_lock(task_id=5, worker_id='worker_0'))

    @patch('daemons.process_daemon.db_manager')
    def test_release_lock(self, mock_db):
        d = _make_process_daemon()
        mock_db.execute_query.return_value = 1
        d.release_task_lock(task_id=5, worker_id='worker_0')
        mock_db.execute_query.assert_called_once()
        query = mock_db.execute_query.call_args[0][0]
        self.assertIn('is_processing', query)
        self.assertIn('FALSE', query.upper())


if __name__ == '__main__':
    unittest.main()
