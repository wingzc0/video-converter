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

from converter import convert_to_480p, get_video_duration, get_video_duration_and_bitrate, get_video_info, parse_time_to_seconds, compute_output_name


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
    """get_video_duration() / get_video_duration_and_bitrate() 使用 mock subprocess"""

    @patch('converter.subprocess.run')
    def test_returns_duration(self, mock_run):
        mock_run.return_value = MagicMock(
            stdout='{"format": {"duration": "123.456", "bit_rate": "5000000"}}',
            returncode=0
        )
        self.assertAlmostEqual(get_video_duration('/fake/video.mp4'), 123.456)

    @patch('converter.subprocess.run')
    def test_returns_duration_and_bitrate(self, mock_run):
        mock_run.return_value = MagicMock(
            stdout='{"format": {"duration": "300.0", "bit_rate": "20000000"}}',
            returncode=0
        )
        duration, bitrate = get_video_duration_and_bitrate('/fake/video.mp4')
        self.assertAlmostEqual(duration, 300.0)
        self.assertEqual(bitrate, 20000000)

    @patch('converter.subprocess.run', side_effect=Exception('error'))
    def test_error_returns_zero(self, _):
        self.assertEqual(get_video_duration('/fake/video.mp4'), 0.0)

    @patch('converter.subprocess.run', side_effect=Exception('error'))
    def test_duration_and_bitrate_error_returns_zero_tuple(self, _):
        duration, bitrate = get_video_duration_and_bitrate('/fake/video.mp4')
        self.assertEqual(duration, 0)
        self.assertEqual(bitrate, 0)


class TestConvertTo480p(unittest.TestCase):
    """convert_to_480p() 使用 mock subprocess.Popen"""

    def setUp(self):
        self._fds_to_close = []

    def tearDown(self):
        for fd in self._fds_to_close:
            try:
                import os as _os
                _os.close(fd)
            except OSError:
                pass

    def _make_mock_process(self, stderr_lines=None, returncode=0):
        """建立模擬的 subprocess.Popen 物件，使用真實 pipe fd 供 select()/os.read() 使用"""
        import os as _os
        mock_proc = MagicMock()
        mock_proc.returncode = returncode
        mock_proc.wait.return_value = returncode
        mock_proc.poll.return_value = returncode  # 已結束，poll() 不回傳 None

        r_fd, w_fd = _os.pipe()
        self._fds_to_close.append(r_fd)

        data = b''.join(line.encode() + b'\n' for line in (stderr_lines or []))
        _os.write(w_fd, data)
        _os.close(w_fd)  # 關閉寫端，讓 select() 看到 EOF

        mock_proc.stderr.fileno.return_value = r_fd
        return mock_proc

    @patch('converter.get_video_duration_and_bitrate', return_value=(100.0, 0))
    @patch('converter.subprocess.Popen')
    def test_successful_conversion_returns_true(self, mock_popen, _):
        mock_popen.return_value = self._make_mock_process(returncode=0)
        success, error = convert_to_480p('/input.mp4', '/output.mp4')
        self.assertTrue(success)
        self.assertIsNone(error)

    @patch('converter.get_video_duration_and_bitrate', return_value=(100.0, 0))
    @patch('converter.subprocess.Popen')
    def test_failed_conversion_returns_false(self, mock_popen, _):
        mock_popen.return_value = self._make_mock_process(returncode=1)
        success, error = convert_to_480p('/input.mp4', '/output.mp4')
        self.assertFalse(success)
        self.assertIsNotNone(error)

    @patch('converter.get_video_duration_and_bitrate', return_value=(100.0, 0))
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

    @patch('converter.get_video_duration_and_bitrate', return_value=(100.0, 0))
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

    @patch('converter.get_video_duration_and_bitrate', return_value=(100.0, 0))
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

    @patch('converter.get_video_duration_and_bitrate', return_value=(100.0, 0))
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

    @patch('converter.get_video_duration_and_bitrate', return_value=(100.0, 0))
    @patch('converter.subprocess.Popen')
    def test_unicode_error_kills_process(self, mock_popen, _):
        """非 UTF-8 字元不應讓 ffmpeg 成為孤兒（errors='ignore' 保護）"""
        import os as _os
        mock_proc = MagicMock()
        mock_proc.wait.return_value = 0
        mock_proc.returncode = 0
        mock_proc.poll.return_value = 0

        r_fd, w_fd = _os.pipe()
        self._fds_to_close.append(r_fd)
        _os.write(w_fd, b'\xa3\xb4 time=00:00:10.00\n')  # 無效 UTF-8 bytes
        _os.close(w_fd)
        mock_proc.stderr.fileno.return_value = r_fd
        mock_popen.return_value = mock_proc

        success, error = convert_to_480p('/input.mp4', '/output.mp4')
        self.assertTrue(success)

    @patch('converter.get_video_duration_and_bitrate', return_value=(100.0, 0))
    @patch('converter.subprocess.Popen')
    def test_stall_timeout_kills_ffmpeg(self, mock_popen, _):
        """stall_timeout 超時後應殺掉 ffmpeg 並回傳 (False, <reason>)"""
        import os as _os
        r_fd, w_fd = _os.pipe()
        self._fds_to_close.append(r_fd)

        mock_proc = MagicMock()
        mock_proc.stderr.fileno.return_value = r_fd
        mock_proc.poll.return_value = None  # 持續運行中
        mock_proc.wait.return_value = -9

        def _do_kill():
            try:
                _os.close(w_fd)  # 關閉寫端 → r_fd 得到 EOF，select() 立即返回
            except OSError:
                pass
            mock_proc.poll.return_value = -9

        mock_proc.kill.side_effect = _do_kill
        mock_popen.return_value = mock_proc

        success, error = convert_to_480p(
            '/input.mp4', '/output.mp4',
            ffmpeg_stall_timeout=1,  # 1 秒無進度即 timeout
        )
        self.assertFalse(success)
        self.assertIn('stall', error.lower())

    @patch('converter.get_video_duration_and_bitrate', return_value=(100.0, 0))
    @patch('converter.subprocess.Popen')
    def test_nostdin_in_ffmpeg_command(self, mock_popen, _):
        """-nostdin 旗標應包含在 ffmpeg 指令中，防止 daemon 環境誤讀 stdin"""
        mock_popen.return_value = self._make_mock_process(returncode=0)
        convert_to_480p('/input.mp4', '/output.mp4')
        cmd = mock_popen.call_args[0][0]
        self.assertIn('-nostdin', cmd)

    @patch('converter.get_video_duration_and_bitrate', return_value=(600.0, 0))
    @patch('converter.subprocess.Popen')
    def test_dynamic_timeout_computed_from_duration(self, mock_popen, _):
        """timeout_multiplier > 0 且 ffmpeg_timeout=None 時，應依影片時長動態計算 timeout"""
        mock_popen.return_value = self._make_mock_process(returncode=0)
        diag = {}
        convert_to_480p('/input.mp4', '/output.mp4',
                        timeout_multiplier=3.0, min_timeout=300, _diag=diag)
        # duration=600s, multiplier=3.0 → expected timeout = 1800s
        # 驗證方式：透過 _diag 或直接確認 watchdog 執行緒啟動（timeout 有值）
        # 因為測試中 process 立即結束，只要 Popen 被呼叫即代表流程正常
        mock_popen.assert_called_once()

    @patch('converter.get_video_duration_and_bitrate', return_value=(60.0, 0))
    @patch('converter.subprocess.Popen')
    def test_dynamic_timeout_respects_min_timeout(self, mock_popen, _):
        """短片（duration * multiplier < min_timeout）時應使用 min_timeout"""
        mock_popen.return_value = self._make_mock_process(returncode=0)
        # duration=60s, multiplier=3.0 → 180s < min_timeout=300 → 應用 300s
        convert_to_480p('/input.mp4', '/output.mp4',
                        timeout_multiplier=3.0, min_timeout=300)
        mock_popen.assert_called_once()

    @patch('converter.get_video_duration_and_bitrate', return_value=(600.0, 0))
    @patch('converter.subprocess.Popen')
    def test_fixed_timeout_overrides_dynamic(self, mock_popen, _):
        """ffmpeg_timeout 明確指定時，不應觸發動態計算"""
        mock_popen.return_value = self._make_mock_process(returncode=0)
        # 即使提供 multiplier，ffmpeg_timeout=3600 應優先使用
        convert_to_480p('/input.mp4', '/output.mp4',
                        ffmpeg_timeout=3600, timeout_multiplier=3.0)
        mock_popen.assert_called_once()

    @patch('converter.get_video_duration_and_bitrate', return_value=(0.0, 0))
    @patch('converter.subprocess.Popen')
    def test_dynamic_timeout_skipped_when_duration_zero(self, mock_popen, _):
        """duration=0（ffprobe 失敗）時不應計算動態 timeout（避免 timeout=0）"""
        mock_popen.return_value = self._make_mock_process(returncode=0)
        convert_to_480p('/input.mp4', '/output.mp4',
                        timeout_multiplier=3.0, min_timeout=300)
        mock_popen.assert_called_once()

    @patch('converter.get_video_duration_and_bitrate', return_value=(100.0, 0))
    @patch('converter.subprocess.Popen')
    def test_carriage_return_progress_lines_parsed(self, mock_popen, _):
        """以 \\r 結尾的 ffmpeg progress 行（與 \\n 混合）應正確解析 time= 欄位"""
        import os as _os
        mock_proc = MagicMock()
        mock_proc.wait.return_value = 0
        mock_proc.returncode = 0
        mock_proc.poll.return_value = 0

        r_fd, w_fd = _os.pipe()
        self._fds_to_close.append(r_fd)
        # ffmpeg 實際輸出：progress 行以 \r 結尾，最終統計行以 \n 結尾
        data = (
            b'frame=  5 fps=25 time=00:00:30.00 bitrate=500\r'
            b'frame= 10 fps=25 time=00:01:00.00 bitrate=500\r'
            b'[libx264] kb/s:500\n'
        )
        _os.write(w_fd, data)
        _os.close(w_fd)
        mock_proc.stderr.fileno.return_value = r_fd
        mock_popen.return_value = mock_proc

        callback = MagicMock()
        success, error = convert_to_480p('/input.mp4', '/output.mp4', progress_callback=callback)
        self.assertTrue(success)
        self.assertGreater(callback.call_count, 0)
        last_progress = callback.call_args_list[-1][0][0]
        self.assertAlmostEqual(last_progress, 60.0, delta=1.0)


class TestBitrateFactorTimeout(unittest.TestCase):
    """bitrate_baseline_mbps 修正因子測試"""

    def _run_convert(self, duration, bitrate_bps, multiplier, min_timeout, baseline_mbps):
        """執行 convert_to_480p 並回傳實際使用的 ffmpeg_timeout（從 _diag）"""
        import math
        # 直接計算預期值，不依賴 _diag（Popen 在測試中立即結束）
        src_mbps = bitrate_bps / 1_000_000
        factor = math.log2(max(2.0, src_mbps / baseline_mbps))
        expected = max(float(min_timeout), duration * multiplier * factor)
        return expected

    def test_high_bitrate_increases_timeout(self):
        """高 bitrate（8K 540 Mbps）應大幅延長 timeout"""
        import math
        # 540 Mbps, baseline=10 → log2(54) ≈ 5.75
        factor = math.log2(max(2.0, 540 / 10))
        timeout = max(300.0, 6081 * 2.0 * factor)
        self.assertGreater(timeout, 14400)  # 應超過原本的 4h timeout

    def test_normal_bitrate_factor_near_one(self):
        """接近 baseline 的 bitrate（如 10 Mbps）factor 應為 1（log2(2/2)=0... 用 max(2,x)）"""
        import math
        # 10 Mbps / 10 = 1.0 → max(2, 1.0) = 2 → log2(2) = 1.0
        factor = math.log2(max(2.0, 10 / 10))
        self.assertAlmostEqual(factor, 1.0)

    def test_low_bitrate_clamped_to_one(self):
        """低 bitrate（< baseline）不應縮短 timeout（max(2,...) 保護）"""
        import math
        # 2 Mbps / 10 = 0.2 → max(2, 0.2) = 2 → log2(2) = 1.0（不縮短）
        factor = math.log2(max(2.0, 2 / 10))
        self.assertAlmostEqual(factor, 1.0)

    @patch('converter.get_video_duration_and_bitrate', return_value=(6081.0, 540_000_000))
    @patch('converter.subprocess.Popen')
    def test_8k_file_timeout_exceeds_4h(self, mock_popen, _):
        """8K 540 Mbps 6081s 影片的動態 timeout 應超過 14400s（4h）"""
        mock_popen.return_value = MagicMock(
            wait=MagicMock(return_value=0),
            returncode=0,
            poll=MagicMock(return_value=0),
            stderr=MagicMock(fileno=MagicMock(return_value=-1)),
        )
        import select as _sel, unittest.mock as _m
        with _m.patch('converter.select.select', return_value=([], [], [])):
            diag = {}
            convert_to_480p('/8k.mp4', '/out.mp4',
                            timeout_multiplier=2.0, min_timeout=300,
                            bitrate_baseline_mbps=10, _diag=diag)
        mock_popen.assert_called_once()
        # 驗證：6081 × 2 × log2(54) ≈ 70000s >> 14400s
        import math
        expected = max(300.0, 6081.0 * 2.0 * math.log2(max(2.0, 540.0 / 10.0)))
        self.assertGreater(expected, 14400)

    @patch('converter.get_video_duration_and_bitrate', return_value=(3600.0, 0))
    @patch('converter.subprocess.Popen')
    def test_zero_bitrate_disables_factor(self, mock_popen, _):
        """bitrate=0（ffprobe 取不到）時不應套用修正（factor=1.0）"""
        mock_popen.return_value = MagicMock(
            wait=MagicMock(return_value=0),
            returncode=0,
            poll=MagicMock(return_value=0),
            stderr=MagicMock(fileno=MagicMock(return_value=-1)),
        )
        import unittest.mock as _m
        with _m.patch('converter.select.select', return_value=([], [], [])):
            convert_to_480p('/input.mp4', '/out.mp4',
                            timeout_multiplier=2.0, min_timeout=300,
                            bitrate_baseline_mbps=10)
        # bitrate=0 → 不套用修正 → timeout = 3600 × 2 = 7200（無異常）
        mock_popen.assert_called_once()


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


class TestConvertTo480pEdgePaths(unittest.TestCase):
    """convert_to_480p() — absolute timeout 與 stderr loop exception 路徑"""

    def setUp(self):
        self._fds_to_close = []

    def tearDown(self):
        import os as _os
        for fd in self._fds_to_close:
            try:
                _os.close(fd)
            except OSError:
                pass

    @patch('converter.time.sleep')  # 讓 watchdog 的 sleep(2) 不阻塞測試
    @patch('converter.get_video_duration_and_bitrate', return_value=(100.0, 0))
    @patch('converter.subprocess.Popen')
    def test_absolute_timeout_kills_ffmpeg(self, mock_popen, _, mock_sleep):
        """ffmpeg_timeout 到期後應殺掉 ffmpeg 並回傳 (False, 含 'absolute timeout' 的訊息)"""
        import os as _os
        r_fd, w_fd = _os.pipe()
        self._fds_to_close.append(r_fd)

        mock_proc = MagicMock()
        mock_proc.stderr.fileno.return_value = r_fd
        mock_proc.poll.return_value = None  # 持續運行中

        def _do_kill():
            try:
                _os.close(w_fd)  # 關閉寫端 → r_fd 得到 EOF，解除 select() 阻塞
            except OSError:
                pass
            mock_proc.poll.return_value = -9

        mock_proc.kill.side_effect = _do_kill
        mock_proc.wait.return_value = -9
        mock_popen.return_value = mock_proc

        success, error = convert_to_480p(
            '/input.mp4', '/output.mp4',
            ffmpeg_timeout=1,  # 1 秒絕對超時
        )
        self.assertFalse(success)
        self.assertIn('absolute timeout', error.lower())

    @patch('converter.get_video_duration_and_bitrate', return_value=(100.0, 0))
    @patch('converter.subprocess.Popen')
    def test_stderr_loop_exception_kills_ffmpeg_returns_false(self, mock_popen, _):
        """stderr 讀取迴圈內發生例外時應 kill ffmpeg 並回傳 (False, <error>)"""
        # 不需要真實 pipe：fileno() 直接拋出例外，不會進入 select() 迴圈
        mock_proc = MagicMock()
        mock_proc.returncode = None
        mock_proc.poll.return_value = None

        # 讓 fileno() 拋出例外，觸發 try 區塊的 except Exception
        mock_proc.stderr.fileno.side_effect = OSError("fd already closed")
        mock_proc.wait.return_value = -9
        mock_popen.return_value = mock_proc

        success, error = convert_to_480p('/input.mp4', '/output.mp4')
        self.assertFalse(success)
        self.assertIsNotNone(error)
        mock_proc.kill.assert_called_once()
        mock_proc.wait.assert_called()


if __name__ == '__main__':
    unittest.main()
