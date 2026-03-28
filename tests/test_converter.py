"""
Unit tests for converter.py
測試 get_video_info、convert_to_480p、parse_time_to_seconds
所有 subprocess 呼叫均以 mock 取代，不需要真實的 ffmpeg/ffprobe。
"""
import json
import sys
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch, call

# 將專案根目錄加入 Python 路徑
sys.path.insert(0, str(Path(__file__).parent.parent))

from converter import convert_to_480p, get_video_duration, get_video_info, parse_time_to_seconds, compute_output_name


class TestParseTimeToSeconds(unittest.TestCase):
    """parse_time_to_seconds('HH:MM:SS.mmm') → float"""

    def test_zero(self):
        self.assertAlmostEqual(parse_time_to_seconds('00:00:00.000'), 0.0)

    def test_seconds_only(self):
        self.assertAlmostEqual(parse_time_to_seconds('00:00:30.000'), 30.0)

    def test_minutes_and_seconds(self):
        self.assertAlmostEqual(parse_time_to_seconds('00:01:30.500'), 90.5)

    def test_hours(self):
        self.assertAlmostEqual(parse_time_to_seconds('01:00:00.000'), 3600.0)

    def test_full_timestamp(self):
        self.assertAlmostEqual(parse_time_to_seconds('01:23:45.678'), 5025.678, places=2)

    def test_invalid_returns_zero(self):
        self.assertEqual(parse_time_to_seconds('N/A'), 0.0)

    def test_empty_string(self):
        self.assertEqual(parse_time_to_seconds(''), 0.0)


class TestGetVideoInfo(unittest.TestCase):
    """get_video_info() 使用 mock subprocess，不呼叫真實 ffprobe"""

    def _make_ffprobe_output(self, width, height):
        return json.dumps({
            'streams': [
                {'codec_type': 'video', 'width': width, 'height': height},
                {'codec_type': 'audio'},
            ]
        })

    @patch('converter.subprocess.run')
    def test_returns_resolution_dict(self, mock_run):
        mock_run.return_value = MagicMock(
            stdout=self._make_ffprobe_output(1920, 1080),
            returncode=0
        )
        info = get_video_info('/fake/video.mp4')
        self.assertIsNotNone(info)
        self.assertEqual(info['width'], 1920)
        self.assertEqual(info['height'], 1080)
        self.assertEqual(info['resolution'], '1920x1080')

    @patch('converter.subprocess.run')
    def test_no_video_stream_returns_none(self, mock_run):
        mock_run.return_value = MagicMock(
            stdout=json.dumps({'streams': [{'codec_type': 'audio'}]}),
            returncode=0
        )
        self.assertIsNone(get_video_info('/fake/audio_only.mp4'))

    @patch('converter.subprocess.run', side_effect=Exception('ffprobe not found'))
    def test_ffprobe_error_returns_none(self, _):
        self.assertIsNone(get_video_info('/fake/video.mp4'))

    @patch('converter.subprocess.run')
    def test_uses_first_video_stream(self, mock_run):
        """若有多個 video stream，應取第一個"""
        mock_run.return_value = MagicMock(
            stdout=json.dumps({
                'streams': [
                    {'codec_type': 'video', 'width': 640, 'height': 480},
                    {'codec_type': 'video', 'width': 1280, 'height': 720},
                ]
            }),
            returncode=0
        )
        info = get_video_info('/fake/video.mp4')
        self.assertEqual(info['width'], 640)

    @patch('converter.subprocess.run')
    def test_missing_width_height_returns_none(self, mock_run):
        """video stream 缺少 width/height 時應回傳 None，不產生 'NonexNone' 字串"""
        mock_run.return_value = MagicMock(
            stdout=json.dumps({'streams': [{'codec_type': 'video'}]}),
            returncode=0
        )
        self.assertIsNone(get_video_info('/fake/video.mp4'))

    @patch('converter.subprocess.run')
    def test_null_width_returns_none(self, mock_run):
        """width 為 null（JSON null → Python None）時應回傳 None"""
        mock_run.return_value = MagicMock(
            stdout=json.dumps({'streams': [{'codec_type': 'video', 'width': None, 'height': 1080}]}),
            returncode=0
        )
        self.assertIsNone(get_video_info('/fake/video.mp4'))


class TestGetVideoDuration(unittest.TestCase):
    """get_video_duration() 使用 mock subprocess"""

    @patch('converter.subprocess.run')
    def test_returns_duration(self, mock_run):
        mock_run.return_value = MagicMock(
            stdout='123.456\n',  # get_video_duration 用 -of default=noprint_wrappers=1:nokey=1，輸出純數字
            returncode=0
        )
        self.assertAlmostEqual(get_video_duration('/fake/video.mp4'), 123.456)

    @patch('converter.subprocess.run', side_effect=Exception('error'))
    def test_error_returns_zero(self, _):
        self.assertEqual(get_video_duration('/fake/video.mp4'), 0.0)


class TestConvertTo480p(unittest.TestCase):
    """convert_to_480p() 使用 mock subprocess.Popen"""

    def _make_mock_process(self, stderr_lines=None, returncode=0):
        """建立模擬的 subprocess.Popen 物件"""
        mock_proc = MagicMock()
        mock_proc.returncode = returncode
        mock_proc.wait.return_value = returncode
        lines = [line.encode() for line in (stderr_lines or [])] + [b'']
        mock_proc.stderr.readline.side_effect = lines
        return mock_proc

    @patch('converter.get_video_duration', return_value=100.0)
    @patch('converter.subprocess.Popen')
    def test_successful_conversion_returns_true(self, mock_popen, _):
        mock_popen.return_value = self._make_mock_process(returncode=0)
        success, error = convert_to_480p('/input.mp4', '/output.mp4')
        self.assertTrue(success)
        self.assertIsNone(error)

    @patch('converter.get_video_duration', return_value=100.0)
    @patch('converter.subprocess.Popen')
    def test_failed_conversion_returns_false(self, mock_popen, _):
        mock_popen.return_value = self._make_mock_process(returncode=1)
        success, error = convert_to_480p('/input.mp4', '/output.mp4')
        self.assertFalse(success)
        self.assertIsNotNone(error)

    @patch('converter.get_video_duration', return_value=100.0)
    @patch('converter.subprocess.Popen')
    def test_failed_conversion_includes_stderr(self, mock_popen, _):
        """失敗時 error 應包含 ffmpeg stderr 的最後幾行"""
        mock_popen.return_value = self._make_mock_process(
            stderr_lines=['Invalid data found when processing input'],
            returncode=1,
        )
        success, error = convert_to_480p('/input.mp4', '/output.mp4')
        self.assertFalse(success)
        self.assertIn('Invalid data found', error)

    @patch('converter.get_video_duration', return_value=100.0)
    @patch('converter.subprocess.Popen')
    def test_success_has_no_error(self, mock_popen, _):
        """成功時 error 應為 None"""
        mock_popen.return_value = self._make_mock_process(
            stderr_lines=['Some info line'],
            returncode=0,
        )
        success, error = convert_to_480p('/input.mp4', '/output.mp4')
        self.assertTrue(success)
        self.assertIsNone(error)

    @patch('converter.get_video_duration', return_value=100.0)
    @patch('converter.subprocess.Popen')
    def test_progress_callback_called(self, mock_popen, _):
        """確認 time= 行會觸發 progress_callback"""
        mock_popen.return_value = self._make_mock_process(
            stderr_lines=[
                'frame=  10 fps=25 time=00:00:50.00 bitrate=1000',
            ],
            returncode=0
        )
        callback = MagicMock()
        convert_to_480p('/input.mp4', '/output.mp4', progress_callback=callback)
        callback.assert_called_once()
        progress_value = callback.call_args[0][0]
        self.assertAlmostEqual(progress_value, 50.0, delta=1.0)

    @patch('converter.get_video_duration', return_value=100.0)
    @patch('converter.subprocess.Popen')
    def test_progress_capped_at_99_9(self, mock_popen, _):
        """進度最大值應被限制在 99.9%，不應顯示 100%"""
        mock_popen.return_value = self._make_mock_process(
            stderr_lines=['time=00:02:00.00 bitrate=1000'],  # 超過 duration
            returncode=0
        )
        callback = MagicMock()
        convert_to_480p('/input.mp4', '/output.mp4', progress_callback=callback)
        for call_args in callback.call_args_list:
            self.assertLessEqual(call_args[0][0], 99.9)

    @patch('converter.get_video_duration', return_value=100.0)
    @patch('converter.subprocess.Popen')
    def test_unicode_error_kills_process(self, mock_popen, _):
        """非 UTF-8 字元不應讓 ffmpeg 成為孤兒（errors='ignore' 保護）"""
        mock_proc = MagicMock()
        mock_proc.wait.return_value = 0
        # 包含非 UTF-8 bytes，但 errors='ignore' 應能正常處理
        mock_proc.stderr.readline.side_effect = [b'\xa3\xb4 time=00:00:10.00', b'']
        mock_popen.return_value = mock_proc
        success, error = convert_to_480p('/input.mp4', '/output.mp4')
        self.assertTrue(success)

    @patch('converter.get_video_duration', return_value=100.0)
    @patch('converter.subprocess.Popen')
    def test_stall_timeout_kills_ffmpeg(self, mock_popen, _):
        """stall_timeout 超時後應殺掉 ffmpeg 並回傳 (False, <reason>)"""
        import threading as _threading

        mock_proc = MagicMock()
        # stderr.readline 永遠阻塞，直到 kill() 被呼叫後才回傳空字串
        kill_event = _threading.Event()

        def _blocking_readline():
            kill_event.wait(timeout=5)
            return b''

        mock_proc.stderr.readline.side_effect = _blocking_readline
        mock_proc.poll.return_value = None
        mock_proc.wait.return_value = -9

        def _do_kill():
            kill_event.set()
            mock_proc.poll.return_value = -9

        mock_proc.kill.side_effect = _do_kill
        mock_popen.return_value = mock_proc

        success, error = convert_to_480p(
            '/input.mp4', '/output.mp4',
            ffmpeg_stall_timeout=1,  # 1 秒無進度即 timeout
        )
        self.assertFalse(success)
        self.assertIn('stall', error.lower())


class TestComputeOutputName(unittest.TestCase):
    """compute_output_name() — 輸出檔名計算"""

    def test_mp4_input_no_suffix(self):
        self.assertEqual(compute_output_name(Path('/input/video.mp4')), '480p_video.mp4')

    def test_mp4_uppercase_no_suffix(self):
        self.assertEqual(compute_output_name(Path('/input/clip.MP4')), '480p_clip.mp4')

    def test_mpg_adds_suffix(self):
        self.assertEqual(compute_output_name(Path('/input/video.mpg')), '480p_video_mpg.mp4')

    def test_mxf_adds_suffix(self):
        self.assertEqual(compute_output_name(Path('/input/clip.MXF')), '480p_clip_mxf.mp4')

    def test_avi_adds_suffix(self):
        self.assertEqual(compute_output_name(Path('/input/movie.avi')), '480p_movie_avi.mp4')

    def test_mkv_adds_suffix(self):
        self.assertEqual(compute_output_name(Path('/input/show.mkv')), '480p_show_mkv.mp4')

    def test_no_collision_between_mpg_and_mp4(self):
        """video.mpg と video.mp4 は異なる出力名を持つこと"""
        self.assertNotEqual(
            compute_output_name(Path('/input/video.mpg')),
            compute_output_name(Path('/input/video.mp4')),
        )


if __name__ == '__main__':
    unittest.main()
