"""
Unit tests for daemons/scan_daemon.py
測試路徑過濾、檔案跳過邏輯；DB 與 ffprobe 呼叫均以 mock 取代。
"""
import sys
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

sys.path.insert(0, str(Path(__file__).parent.parent))


def _make_scan_daemon(tmp_path):
    """建立 ScanDaemon，注入臨時目錄以通過 validate_settings()"""
    (tmp_path / 'input').mkdir()
    (tmp_path / 'output').mkdir()
    with patch.dict('os.environ', {
        'INPUT_DIRECTORY': str(tmp_path / 'input'),
        'OUTPUT_DIRECTORY': str(tmp_path / 'output'),
        'IGNORE_DIRECTORIES': '',
    }):
        from daemons.scan_daemon import ScanDaemon
        return ScanDaemon(scan_interval=60)


class TestShouldSkipFile(unittest.TestCase):
    """should_skip_file() — 跳過以 480p_ 開頭的已轉換輸出檔"""

    def setUp(self):
        import tempfile
        self.tmp = tempfile.mkdtemp()
        self.daemon = _make_scan_daemon(Path(self.tmp))

    def test_skip_480p_prefix(self):
        self.assertTrue(self.daemon.should_skip_file('480p_video.mp4'))

    def test_skip_480p_uppercase(self):
        # 大小寫敏感：480P_ 不應被跳過
        self.assertFalse(self.daemon.should_skip_file('480P_video.mp4'))

    def test_do_not_skip_normal_file(self):
        self.assertFalse(self.daemon.should_skip_file('video.mp4'))

    def test_do_not_skip_file_containing_480p(self):
        # 只有前綴才跳過，中間出現不算
        self.assertFalse(self.daemon.should_skip_file('my_480p_video.mp4'))

    def test_skip_480p_prefix_only_filename(self):
        self.assertTrue(self.daemon.should_skip_file('480p_'))


class TestShouldIgnorePath(unittest.TestCase):
    """should_ignore_path() — 絕對路徑前綴比對 + 相對路徑任意位置比對"""

    def setUp(self):
        import tempfile
        self.tmp = tempfile.mkdtemp()

    def _make_daemon_with_ignore(self, ignore_dirs, ignore_output_dir='false'):
        (Path(self.tmp) / 'input').mkdir(exist_ok=True)
        (Path(self.tmp) / 'output').mkdir(exist_ok=True)
        with patch.dict('os.environ', {
            'INPUT_DIRECTORY': str(Path(self.tmp) / 'input'),
            'OUTPUT_DIRECTORY': str(Path(self.tmp) / 'output'),
            'IGNORE_DIRECTORIES': ','.join(ignore_dirs),
            'IGNORE_OUTPUT_DIR': ignore_output_dir,
        }):
            from daemons.scan_daemon import ScanDaemon
            return ScanDaemon(scan_interval=60)

    # ── 絕對路徑 ────────────────────────────────────────────────────────────

    def test_abs_exact_match_ignored(self):
        ignore = str(Path(self.tmp) / 'input' / 'skip_me')
        daemon = self._make_daemon_with_ignore([ignore])
        self.assertTrue(daemon.should_ignore_path(Path(ignore)))

    def test_abs_subpath_is_ignored(self):
        """絕對路徑的子目錄也應被忽略"""
        ignore = str(Path(self.tmp) / 'input' / 'skip_me')
        daemon = self._make_daemon_with_ignore([ignore])
        self.assertTrue(daemon.should_ignore_path(
            Path(self.tmp) / 'input' / 'skip_me' / 'subdir'
        ))

    def test_abs_prefix_only_not_ignored(self):
        """/data/out 不應誤匹配 /data/output（字串前綴的 bug）"""
        ignore = str(Path(self.tmp) / 'input' / 'out')
        daemon = self._make_daemon_with_ignore([ignore])
        self.assertFalse(daemon.should_ignore_path(
            Path(self.tmp) / 'input' / 'output'
        ))

    def test_abs_non_ignored_path_allowed(self):
        ignore = str(Path(self.tmp) / 'input' / 'skip_me')
        daemon = self._make_daemon_with_ignore([ignore])
        self.assertFalse(daemon.should_ignore_path(
            Path(self.tmp) / 'input' / 'keep_me'
        ))

    # ── 相對路徑（單層名稱） ─────────────────────────────────────────────────

    def test_rel_single_matches_at_root_level(self):
        """單層相對路徑應匹配輸入目錄根層的同名目錄"""
        daemon = self._make_daemon_with_ignore(['@Recycle'])
        self.assertTrue(daemon.should_ignore_path(
            Path(self.tmp) / 'input' / '@Recycle'
        ))

    def test_rel_single_matches_nested(self):
        """單層相對路徑應匹配掃描樹任意深度的同名目錄"""
        daemon = self._make_daemon_with_ignore(['@Recycle'])
        self.assertTrue(daemon.should_ignore_path(
            Path(self.tmp) / 'input' / 'a' / 'b' / '@Recycle' / 'deep'
        ))

    def test_rel_single_no_partial_match(self):
        """單層相對路徑不應匹配包含該名稱的其他目錄（如 @Recycle2）"""
        daemon = self._make_daemon_with_ignore(['@Recycle'])
        self.assertFalse(daemon.should_ignore_path(
            Path(self.tmp) / 'input' / '@Recycle2'
        ))

    # ── 相對路徑（多層） ─────────────────────────────────────────────────────

    def test_rel_multilevel_matches_at_root(self):
        """多層相對路徑應匹配輸入目錄根層的連續子目錄"""
        daemon = self._make_daemon_with_ignore(['BCD/ABC'])
        self.assertTrue(daemon.should_ignore_path(
            Path(self.tmp) / 'input' / 'BCD' / 'ABC' / 'video.mp4'
        ))

    def test_rel_multilevel_matches_nested(self):
        """多層相對路徑應匹配掃描樹任意位置的連續子目錄"""
        daemon = self._make_daemon_with_ignore(['BCD/ABC'])
        self.assertTrue(daemon.should_ignore_path(
            Path(self.tmp) / 'input' / 'x' / 'BCD' / 'ABC' / 'video.mp4'
        ))

    def test_rel_multilevel_no_partial_match(self):
        """多層相對路徑不應只匹配部分層（BCD 而非 BCD/ABC）"""
        daemon = self._make_daemon_with_ignore(['BCD/ABC'])
        self.assertFalse(daemon.should_ignore_path(
            Path(self.tmp) / 'input' / 'BCD' / 'OTHER'
        ))

    # ── 其他 ────────────────────────────────────────────────────────────────

    def test_empty_ignore_list(self):
        daemon = self._make_daemon_with_ignore([])
        self.assertFalse(daemon.should_ignore_path(Path(self.tmp) / 'input' / 'anything'))

    def test_ignore_output_dir_env(self):
        """IGNORE_OUTPUT_DIR=true 應自動忽略 OUTPUT_DIRECTORY"""
        daemon = self._make_daemon_with_ignore([], ignore_output_dir='true')
        self.assertTrue(daemon.should_ignore_path(
            Path(self.tmp) / 'output' / 'video.mp4'
        ))

    def test_ignore_output_dir_false(self):
        """IGNORE_OUTPUT_DIR=false 時輸出目錄不應被自動忽略"""
        daemon = self._make_daemon_with_ignore([], ignore_output_dir='false')
        self.assertFalse(daemon.should_ignore_path(
            Path(self.tmp) / 'output' / 'video.mp4'
        ))


class TestScanDirectoryFiltering(unittest.TestCase):
    """scan_directory() 整合測試：確認各過濾條件的行為"""

    def setUp(self):
        import tempfile, os
        self.tmp = Path(tempfile.mkdtemp())
        self.input_dir = self.tmp / 'input'
        self.output_dir = self.tmp / 'output'
        self.input_dir.mkdir()
        self.output_dir.mkdir()

    def _make_daemon(self, ignore=''):
        with patch.dict('os.environ', {
            'INPUT_DIRECTORY': str(self.input_dir),
            'OUTPUT_DIRECTORY': str(self.output_dir),
            'IGNORE_DIRECTORIES': ignore,
            'MIN_RESOLUTION': '481',
        }):
            from daemons.scan_daemon import ScanDaemon
            return ScanDaemon(scan_interval=60)

    @patch('task_manager.db_manager')
    @patch('daemons.scan_daemon.get_video_info')
    def test_skips_480p_prefixed_files(self, mock_info, mock_db):
        """以 480p_ 開頭的檔案不應加入 DB"""
        (self.input_dir / '480p_already_converted.mp4').touch()
        mock_db.execute_query.return_value = []
        daemon = self._make_daemon()
        daemon.scan_directory()
        # get_video_info 不應被呼叫（檔案應在 should_skip_file 就被跳過）
        mock_info.assert_not_called()

    @patch('task_manager.db_manager')
    @patch('daemons.scan_daemon.get_video_info')
    def test_skips_unsupported_extension(self, mock_info, mock_db):
        """不支援的副檔名應被跳過"""
        (self.input_dir / 'document.pdf').touch()
        mock_db.execute_query.return_value = []
        daemon = self._make_daemon()
        daemon.scan_directory()
        mock_info.assert_not_called()

    @patch('task_manager.db_manager')
    @patch('daemons.scan_daemon.get_video_info')
    def test_skips_low_resolution_video(self, mock_info, mock_db):
        """解析度低於 MIN_RESOLUTION 的影片不應加入 DB"""
        (self.input_dir / 'small.mp4').touch()
        mock_db.execute_query.return_value = []  # 未在 DB 中
        mock_info.return_value = {'width': 640, 'height': 360, 'resolution': '640x360'}
        daemon = self._make_daemon()
        daemon.scan_directory()
        # execute_query 只應被呼叫一次（SELECT 檢查是否已在 DB），不應有 INSERT
        insert_calls = [c for c in mock_db.execute_query.call_args_list
                        if 'INSERT' in str(c)]
        self.assertEqual(len(insert_calls), 0)

    @patch('task_manager.db_manager')
    @patch('daemons.scan_daemon.get_video_info')
    def test_adds_new_hd_video_to_db(self, mock_info, mock_db):
        """未在 DB 且解析度足夠的影片應以 INSERT IGNORE 加入 DB"""
        (self.input_dir / 'hd_video.mp4').touch()
        mock_db.execute_query.side_effect = [[], 1]  # SELECT → 空, INSERT → 1 row
        mock_info.return_value = {'width': 1920, 'height': 1080, 'resolution': '1920x1080'}
        daemon = self._make_daemon()
        daemon.scan_directory()
        insert_calls = [c for c in mock_db.execute_query.call_args_list
                        if 'INSERT' in str(c)]
        self.assertEqual(len(insert_calls), 1)
        self.assertEqual(daemon.scan_progress['tasks_added'], 1)

    @patch('task_manager.db_manager')
    @patch('daemons.scan_daemon.get_video_info')
    def test_output_always_mp4_for_mpg(self, mock_info, mock_db):
        """mpg 輸入檔的 output_path 應一律使用 .mp4 副檔名"""
        (self.input_dir / 'video.mpg').touch()
        mock_db.execute_query.side_effect = [[], 1]
        mock_info.return_value = {'width': 1920, 'height': 1080, 'resolution': '1920x1080'}
        daemon = self._make_daemon()
        daemon.scan_directory()
        insert_calls = [c for c in mock_db.execute_query.call_args_list
                        if 'INSERT' in str(c)]
        self.assertEqual(len(insert_calls), 1)
        insert_args = str(insert_calls[0])
        self.assertIn('480p_video_mpg.mp4', insert_args)
        self.assertNotIn('.mpg', insert_args.split('480p_video')[1])

    @patch('task_manager.db_manager')
    @patch('daemons.scan_daemon.get_video_info')
    def test_output_always_mp4_for_mxf(self, mock_info, mock_db):
        """mxf 輸入檔的 output_path 應一律使用 .mp4 副檔名"""
        (self.input_dir / 'clip.MXF').touch()
        mock_db.execute_query.side_effect = [[], 1]
        mock_info.return_value = {'width': 3840, 'height': 2160, 'resolution': '3840x2160'}
        daemon = self._make_daemon()
        daemon.scan_directory()
        insert_calls = [c for c in mock_db.execute_query.call_args_list
                        if 'INSERT' in str(c)]
        self.assertEqual(len(insert_calls), 1)
        insert_args = str(insert_calls[0])
        self.assertIn('480p_clip_mxf.mp4', insert_args)
        self.assertNotIn('.MXF', insert_args.split('480p_clip')[1])

    @patch('task_manager.db_manager')
    @patch('daemons.scan_daemon.get_video_info')
    def test_mp4_input_keeps_clean_name(self, mock_info, mock_db):
        """.mp4 輸入不加原始副檔名後綴，維持 480p_video.mp4"""
        (self.input_dir / 'video.mp4').touch()
        mock_db.execute_query.side_effect = [[], 1]
        mock_info.return_value = {'width': 1280, 'height': 720, 'resolution': '1280x720'}
        daemon = self._make_daemon()
        daemon.scan_directory()
        insert_calls = [c for c in mock_db.execute_query.call_args_list
                        if 'INSERT' in str(c)]
        self.assertEqual(len(insert_calls), 1)
        self.assertIn('480p_video.mp4', str(insert_calls[0]))
        self.assertNotIn('480p_video_mp4.mp4', str(insert_calls[0]))

    @patch('task_manager.db_manager')
    @patch('daemons.scan_daemon.get_video_info')
    def test_skips_already_in_db_pending(self, mock_info, mock_db):
        """DB 中已有 pending 記錄時，不應呼叫 ffprobe"""
        (self.input_dir / 'existing.mp4').touch()
        mock_db.execute_query.return_value = [{'id': 1, 'status': 'pending'}]
        daemon = self._make_daemon()
        daemon.scan_directory()
        mock_info.assert_not_called()

    @patch('task_manager.db_manager')
    @patch('daemons.scan_daemon.get_video_info')
    def test_skips_already_in_db_processing(self, mock_info, mock_db):
        """DB 中已有 processing 記錄時，不應呼叫 ffprobe"""
        (self.input_dir / 'existing.mp4').touch()
        mock_db.execute_query.return_value = [{'id': 1, 'status': 'processing'}]
        daemon = self._make_daemon()
        daemon.scan_directory()
        mock_info.assert_not_called()

    @patch('task_manager.db_manager')
    @patch('daemons.scan_daemon.get_video_info')
    def test_requeues_completed_with_missing_output(self, mock_info, mock_db):
        """DB 中 completed 但輸出檔不存在時，應重置為 pending"""
        (self.input_dir / 'existing.mp4').touch()
        # 輸出檔不建立（模擬遺失）
        mock_db.execute_query.return_value = [{'id': 1, 'status': 'completed'}]
        mock_info.return_value = {'width': 1920, 'height': 1080, 'resolution': '1920x1080'}
        daemon = self._make_daemon()
        daemon.scan_directory()
        update_calls = [c for c in mock_db.execute_query.call_args_list
                        if 'UPDATE' in str(c) and 'pending' in str(c)]
        self.assertGreater(len(update_calls), 0)


class TestCleanupDeletedSources(unittest.TestCase):
    """cleanup_deleted_sources() — source 刪除後清理輸出檔與 DB 記錄"""

    def setUp(self):
        import tempfile
        self.tmp = Path(tempfile.mkdtemp())
        self.input_dir  = self.tmp / 'input'
        self.output_dir = self.tmp / 'output'
        self.input_dir.mkdir()
        self.output_dir.mkdir()

    def _make_daemon(self):
        with patch.dict('os.environ', {
            'INPUT_DIRECTORY': str(self.input_dir),
            'OUTPUT_DIRECTORY': str(self.output_dir),
            'IGNORE_DIRECTORIES': '',
        }):
            from daemons.scan_daemon import ScanDaemon
            return ScanDaemon(scan_interval=60)

    @patch('task_manager.db_manager')
    def test_deletes_output_and_task_when_source_removed(self, mock_db):
        """source 不存在且有 output 時，刪除 output 並移除 DB 記錄"""
        output_file = self.output_dir / '480p_video.mp4'
        output_file.touch()

        mock_db.execute_query.return_value = [{
            'id': 1,
            'input_path': str(self.input_dir / 'video.mp4'),  # source 不建立
            'output_path': str(output_file),
            'status': 'completed',
        }]
        mock_db.execute_transaction.return_value = [1]

        daemon = self._make_daemon()
        deleted = daemon.cleanup_deleted_sources()

        self.assertEqual(deleted, 1)
        self.assertFalse(output_file.exists())            # output 已刪除
        mock_db.execute_transaction.assert_called_once()  # delete_task 已呼叫

    @patch('task_manager.db_manager')
    def test_deletes_task_only_when_no_output(self, mock_db):
        """source 不存在且無 output 時（pending/failed），只移除 DB 記錄"""
        mock_db.execute_query.return_value = [{
            'id': 2,
            'input_path': str(self.input_dir / 'video.mp4'),  # source 不建立
            'output_path': '',
            'status': 'pending',
        }]
        mock_db.execute_transaction.return_value = [1]

        daemon = self._make_daemon()
        deleted = daemon.cleanup_deleted_sources()

        self.assertEqual(deleted, 1)
        mock_db.execute_transaction.assert_called_once()

    @patch('task_manager.db_manager')
    def test_skips_task_when_source_still_exists(self, mock_db):
        """source 仍存在時，不應刪除任何東西"""
        source = self.input_dir / 'alive.mp4'
        source.touch()
        output_file = self.output_dir / '480p_alive.mp4'
        output_file.touch()

        mock_db.execute_query.return_value = [{
            'id': 3,
            'input_path': str(source),
            'output_path': str(output_file),
            'status': 'completed',
        }]

        daemon = self._make_daemon()
        deleted = daemon.cleanup_deleted_sources()

        self.assertEqual(deleted, 0)
        self.assertTrue(output_file.exists())          # output 未被刪除
        mock_db.execute_transaction.assert_not_called()

    @patch('task_manager.db_manager')
    def test_skips_processing_tasks(self, mock_db):
        """cleanup 方法的 SQL 查詢已排除 processing 任務（get_tasks_for_source_cleanup 以 status != processing 過濾）"""
        # 模擬 DB 回傳空清單（processing 任務被過濾掉）
        mock_db.execute_query.return_value = []

        daemon = self._make_daemon()
        deleted = daemon.cleanup_deleted_sources()

        self.assertEqual(deleted, 0)
        mock_db.execute_transaction.assert_not_called()

    @patch('task_manager.db_manager')
    def test_keeps_task_when_output_deletion_fails(self, mock_db):
        """output 檔刪除失敗時，應保留 DB 記錄（避免資料不一致）"""
        output_file = self.output_dir / '480p_video.mp4'
        output_file.touch()

        mock_db.execute_query.return_value = [{
            'id': 4,
            'input_path': str(self.input_dir / 'video.mp4'),
            'output_path': str(output_file),
            'status': 'completed',
        }]

        daemon = self._make_daemon()
        # mock Path.unlink() 拋出 OSError
        with patch.object(Path, 'unlink', side_effect=OSError("Permission denied")):
            deleted = daemon.cleanup_deleted_sources()

        self.assertEqual(deleted, 0)                       # 沒有刪除任何 task
        mock_db.execute_transaction.assert_not_called()    # delete_task 未呼叫

    @patch('task_manager.db_manager')
    def test_task_claimed_by_process_daemon_not_counted(self, mock_db):
        """delete_task 回傳 False 時（任務已被 process_daemon 搶走），不計入 deleted"""
        mock_db.execute_query.return_value = [{
            'id': 5,
            'input_path': str(self.input_dir / 'video.mp4'),
            'output_path': '',
            'status': 'pending',
        }]
        # execute_transaction 回傳 rowcounts：tasks DELETE=0
        # 代表 DELETE WHERE status != 'processing' 沒有命中（任務已被搶走）
        mock_db.execute_transaction.return_value = [0]

        daemon = self._make_daemon()
        deleted = daemon.cleanup_deleted_sources()

        self.assertEqual(deleted, 0)  # 不計入 deleted，因為 delete_task 回傳 False


class TestGetTasksForSourceCleanupEscape(unittest.TestCase):
    """get_tasks_for_source_cleanup() — LIKE 模式 escape 驗證"""

    def setUp(self):
        import tempfile
        self.tmp = Path(tempfile.mkdtemp())
        (self.tmp / 'input_dir').mkdir()
        (self.tmp / 'output').mkdir()

    def _make_daemon(self, input_dir):
        with patch.dict('os.environ', {
            'INPUT_DIRECTORY': str(input_dir),
            'OUTPUT_DIRECTORY': str(self.tmp / 'output'),
            'IGNORE_DIRECTORIES': '',
        }):
            from daemons.scan_daemon import ScanDaemon
            return ScanDaemon(scan_interval=60)

    @patch('task_manager.db_manager')
    def test_underscore_in_path_is_escaped(self, mock_db):
        """目錄名稱含 _ 時，LIKE 模式中的 _ 應被 escape，不當作萬用字元"""
        mock_db.execute_query.return_value = []
        input_dir = self.tmp / 'input_dir'

        daemon = self._make_daemon(input_dir)
        daemon.task_repo.get_tasks_for_source_cleanup(str(input_dir))

        call_args = mock_db.execute_query.call_args
        pattern = call_args[0][1][0]   # positional args: (sql, params) → params[0]
        # escape 後底線應變為 !_，不是原始的 _
        self.assertIn('!_', pattern)
        self.assertNotIn('input_dir/%', pattern)  # 原始未 escape 的模式不應出現


if __name__ == '__main__':
    unittest.main()
