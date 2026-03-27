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
    """should_ignore_path() — 使用 Path.relative_to() 精確比對，避免前綴誤判"""

    def setUp(self):
        import tempfile
        self.tmp = tempfile.mkdtemp()

    def _make_daemon_with_ignore(self, ignore_dirs):
        (Path(self.tmp) / 'input').mkdir(exist_ok=True)
        (Path(self.tmp) / 'output').mkdir(exist_ok=True)
        with patch.dict('os.environ', {
            'INPUT_DIRECTORY': str(Path(self.tmp) / 'input'),
            'OUTPUT_DIRECTORY': str(Path(self.tmp) / 'output'),
            'IGNORE_DIRECTORIES': ','.join(ignore_dirs),
        }):
            from daemons.scan_daemon import ScanDaemon
            return ScanDaemon(scan_interval=60)

    def test_exact_match_ignored(self):
        ignore = str(Path(self.tmp) / 'input' / 'skip_me')
        daemon = self._make_daemon_with_ignore([ignore])
        self.assertTrue(daemon.should_ignore_path(Path(ignore)))

    def test_subpath_is_ignored(self):
        """子目錄也應被忽略"""
        ignore = str(Path(self.tmp) / 'input' / 'skip_me')
        daemon = self._make_daemon_with_ignore([ignore])
        self.assertTrue(daemon.should_ignore_path(
            Path(self.tmp) / 'input' / 'skip_me' / 'subdir'
        ))

    def test_prefix_only_not_ignored(self):
        """/data/out 不應誤匹配 /data/output（字串前綴的 bug）"""
        ignore = str(Path(self.tmp) / 'input' / 'out')
        daemon = self._make_daemon_with_ignore([ignore])
        self.assertFalse(daemon.should_ignore_path(
            Path(self.tmp) / 'input' / 'output'
        ))

    def test_non_ignored_path_allowed(self):
        ignore = str(Path(self.tmp) / 'input' / 'skip_me')
        daemon = self._make_daemon_with_ignore([ignore])
        self.assertFalse(daemon.should_ignore_path(
            Path(self.tmp) / 'input' / 'keep_me'
        ))

    def test_empty_ignore_list(self):
        daemon = self._make_daemon_with_ignore([])
        self.assertFalse(daemon.should_ignore_path(Path(self.tmp) / 'input' / 'anything'))


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


if __name__ == '__main__':
    unittest.main()
