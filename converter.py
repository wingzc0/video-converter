import subprocess
import json
import os
import select
import threading
from pathlib import Path
import time

def get_video_info(input_path):
    """以 ffprobe 取得影片的第一個視訊串流解析度。

    Args:
        input_path: 影片檔案路徑（字串）。

    Returns:
        dict|None: 成功時回傳 {'width': int, 'height': int, 'resolution': 'WxH'}；
                   找不到視訊串流、width/height 為 None、或發生錯誤時回傳 None。
    """
    cmd = [
        'ffprobe',
        '-v', 'quiet',
        '-print_format', 'json',
        '-show_streams',
        '-show_format',
        input_path
    ]
    
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        data = json.loads(result.stdout)
        
        # 獲取第一個視訊流的解析度
        for stream in data.get('streams', []):
            if stream.get('codec_type') == 'video':
                width = stream.get('width')
                height = stream.get('height')
                if width is None or height is None:
                    return None
                return {
                    'width': width,
                    'height': height,
                    'resolution': f"{width}x{height}"
                }
        return None
    except Exception as e:
        print(f"Error getting video info: {e}")
        return None


def compute_output_name(file_path):
    """輸出檔名計算（僅檔名，不含目錄路徑）。

    規則：
    - .mp4 輸入：480p_{stem}.mp4          （e.g. video.mp4 → 480p_video.mp4）
    - 其他格式：480p_{stem}_{ext}.mp4     （e.g. video.mpg → 480p_video_mpg.mp4）

    非 .mp4 輸入加入原始副檔名後綴，避免同目錄下相同 stem 不同格式的輸出路徑衝突。
    """
    p = Path(file_path)
    orig_suffix = p.suffix[1:].lower()
    if orig_suffix == "mp4":
        return f"480p_{p.stem}.mp4"
    return f"480p_{p.stem}_{orig_suffix}.mp4"

def convert_to_480p(input_path, output_path, progress_callback=None,
                    ffmpeg_timeout=None, ffmpeg_stall_timeout=None,
                    timeout_multiplier=0.0, min_timeout=300,
                    _diag=None):
    """使用 ffmpeg 將影片轉換為 480p H.264/AAC，支援進度回調與超時保護。

    Args:
        input_path:          輸入影片路徑（字串）。
        output_path:         輸出 .mp4 路徑（字串）；已存在時以 -y 覆蓋。
        progress_callback:   可選回調 `f(progress: float)`，progress 範圍 0–99.9；
                             100% 由 process_task 在確認輸出檔存在後才設定。
        ffmpeg_timeout:      整體轉檔絕對上限（秒）。None 時若 timeout_multiplier > 0
                             則自動依影片時長計算；0 表示不限制。
        ffmpeg_stall_timeout: 多久未收到 ffmpeg 進度輸出即視為停頓（秒）。None 表示不限制。
        timeout_multiplier:  動態 timeout 倍數（僅在 ffmpeg_timeout 為 None 時生效）。
                             0 表示停用動態計算。
        min_timeout:         動態計算的最低保障秒數（預設 300s）。

    Returns:
        tuple[bool, str|None]: 成功時 (True, None)；失敗時 (False, 錯誤原因字串)。
    """
    # FFmpeg命令：自動縮放至480p並保持比例
    cmd = [
        'ffmpeg',
        # -nostdin：停用 stdin 互動模式，避免 ffmpeg 將 stdin 的二進位資料（如 NFS 來源檔案的位元組）
        # 誤判為鍵盤指令（例如 'q' 字元導致 ffmpeg 提早優雅退出並回傳 RC=0）。
        # 在 daemon 環境下 ffmpeg 的 fd 0 會繼承到來源檔案，若不加此參數
        # mpeg/mxf 二進位資料中的 0x71 ('q') 位元組將觸發 ffmpeg 中途停止。
        '-nostdin',
        '-i', input_path,
        # scale=-2:480：高度固定為 480px，寬度由 ffmpeg 自動計算並取偶數（-2）以滿足 H.264 編碼對偶數寬度的要求
        '-vf', 'scale=-2:480',  # 自動計算寬度保持比例
        '-c:v', 'libx264',      # H.264編碼
        # crf=23：恆定品質因子，範圍 0-51，數值越小品質越高檔案越大；23 為 libx264 預設值，在品質與檔案大小之間取得良好平衡
        '-crf', '23',           # 品質參數 (18-28，值越小品質越好)
        # preset=medium：編碼速度與壓縮率的折衷，slower 可得到更小檔案但耗時更久；批次轉檔時 medium 能兼顧速度與壓縮率
        '-preset', 'medium',    # 編碼速度/壓縮率平衡
        '-c:a', 'aac',          # 音訊編碼
        '-b:a', '128k',         # 音訊位元率
        # 注意：不使用 +faststart，因為輸出目錄在 NFS 上；
        # +faststart 需要對輸出檔案做 seek+rewrite，NFS 的隨機寫入會 silent truncate，
        # 導致 ffmpeg 回傳 exit code 0 但輸出檔案不完整
        '-y',                   # 覆蓋輸出檔案
        output_path
    ]
    
    try:
        # 獲取影片總時長
        duration = get_video_duration(input_path)

        # 動態 timeout：若未指定固定 ffmpeg_timeout 且設有 timeout_multiplier，
        # 依影片時長計算：max(min_timeout, duration * timeout_multiplier)
        if ffmpeg_timeout is None and timeout_multiplier > 0 and duration > 0:
            ffmpeg_timeout = max(float(min_timeout), duration * timeout_multiplier)
        
        # 執行轉換並實時追蹤進度
        # 使用 binary 模式讀取 stderr，避免非 UTF-8 字元（如部分影片 metadata）造成 UnicodeDecodeError
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )

        # Watchdog：獨立執行緒監控 stall timeout 與 absolute timeout，
        # 兩者任一超時皆殺掉 ffmpeg 並設旗標，主執行緒讀取旗標後回傳 False
        timeout_reason = [None]  # 使用 list 讓 closure 可修改

        def _watchdog(proc, start_time, last_progress_time_ref):
            while proc.poll() is None:
                now = time.monotonic()
                if ffmpeg_timeout and (now - start_time) >= ffmpeg_timeout:
                    timeout_reason[0] = f"ffmpeg absolute timeout ({ffmpeg_timeout}s)"
                    proc.kill()
                    return
                if ffmpeg_stall_timeout and (now - last_progress_time_ref[0]) >= ffmpeg_stall_timeout:
                    timeout_reason[0] = f"ffmpeg stall timeout ({ffmpeg_stall_timeout}s without progress)"
                    proc.kill()
                    return
                time.sleep(2)  # 每 2 秒輪詢一次，兼顧即時性與 CPU 佔用

        start_time = time.monotonic()
        last_progress_time = [start_time]  # list 讓 watchdog closure 可讀取最新值

        if ffmpeg_timeout or ffmpeg_stall_timeout:
            watchdog = threading.Thread(
                target=_watchdog,
                args=(process, start_time, last_progress_time),
                daemon=True,
            )
            watchdog.start()

        current_time = 0
        stderr_tail = []  # 收集 ffmpeg stderr 最後幾行，失敗時用於診斷
        # FFmpeg 進度資訊以 '\r' 結尾寫入 stderr（而非 '\n'），
        # 使用 select() + os.read() 以支援 '\r' 和 '\n' 兩種行結尾，
        # 使 last_progress_time 在每次進度更新（約每 200ms）時都能即時更新，
        # 避免 stall timeout 在長轉檔過程中因長期等不到 '\n' 而誤判 ffmpeg 卡住。
        return_code = None  # 確保 except 後若有程式碼變動時不會出現 NameError
        # 限制 progress_callback 頻率：最快每 5 秒或進度差達 1% 才回呼一次，
        # 避免 \r-aware 讀取模式對 DB 產生過高頻率的寫入
        _last_cb_time = start_time
        _last_cb_progress = 0.0
        try:
            stderr_fd = process.stderr.fileno()
            _buf = b''
            while True:
                # select() 等待 stderr 有資料，1 秒 timeout 以便 watchdog 能即時 kill
                try:
                    rlist, _, _ = select.select([stderr_fd], [], [], 1.0)
                except (select.error, ValueError):
                    break
                if rlist:
                    chunk = os.read(stderr_fd, 4096)
                    if not chunk:  # EOF：ffmpeg 已結束
                        break
                    _buf += chunk
                elif process.poll() is not None:
                    # 沒有新資料且程序已結束
                    break

                # 以 '\r' 或 '\n' 分割並逐行處理（ffmpeg 進度行以 '\r' 結尾）
                while _buf:
                    cr = _buf.find(b'\r')
                    lf = _buf.find(b'\n')
                    if cr < 0 and lf < 0:
                        break  # 尚無完整的一行
                    pos = min(x for x in (cr, lf) if x >= 0)
                    line_bytes = _buf[:pos]
                    _buf = _buf[pos + 1:]

                    line = line_bytes.decode('utf-8', errors='ignore')
                    stripped = line.strip()

                    # 保留最後 20 行非進度行供失敗診斷
                    if stripped and not stripped.startswith('frame='):
                        stderr_tail.append(stripped)
                        if len(stderr_tail) > 20:
                            stderr_tail.pop(0)

                    # 解析 time= 欄位以追蹤進度並重設 stall timer
                    if 'time=' in line:
                        time_str = line.split('time=')[1].split(' ')[0].strip()
                        current_time = parse_time_to_seconds(time_str)
                        last_progress_time[0] = time.monotonic()

                        if duration > 0 and progress_callback:
                            progress = min(99.9, (current_time / duration) * 100)
                            now = time.monotonic()
                            # 節流：進度差 ≥ 1% 或距上次回呼 ≥ 5 秒才呼叫
                            if (abs(progress - _last_cb_progress) >= 1.0 or
                                    now - _last_cb_time >= 5.0):
                                progress_callback(progress)
                                _last_cb_time = now
                                _last_cb_progress = progress
            return_code = process.wait()
        except Exception as e:
            print(f"Conversion error: {e}")
            # 確保 ffmpeg 子程序不會成為孤兒程序繼續佔用資源；
            # process 已被 kill，wait() 不會因 pipe buffer 滿而死鎖
            process.kill()
            process.wait()
            return False, str(e)
        finally:
            # 無論正常結束或例外，確保 stdout/stderr pipe fd 都被正確關閉，避免 fd 洩漏
            for fd in [process.stdout, process.stderr]:
                if fd is not None:
                    try:
                        fd.close()
                    except OSError:
                        pass

        if return_code == 0:
            # 將診斷資訊回填給呼叫者（如有傳入 _diag dict）
            if _diag is not None:
                _diag['current_time'] = current_time
                _diag['stderr_tail'] = stderr_tail
            return True, None
        # 組合失敗原因：timeout 原因優先，其次附上 ffmpeg stderr 最後幾行
        base_reason = timeout_reason[0] or "ffmpeg exited with non-zero return code"
        if stderr_tail:
            # 只取最後 3 行避免 error_message 過長（DB 欄位限 1000 字元）
            snippet = ' | '.join(stderr_tail[-3:])
            reason = f"{base_reason} | stderr: {snippet}"
        else:
            reason = base_reason
        print(f"Conversion failed: {reason}")
        return False, reason
        
    except Exception as e:
        print(f"Conversion error: {e}")
        return False, str(e)

def get_video_duration(input_path):
    """獲取影片總時長（秒）"""
    cmd = [
        'ffprobe',
        '-v', 'quiet',
        '-show_entries', 'format=duration',
        '-of', 'default=noprint_wrappers=1:nokey=1',
        input_path
    ]
    
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        return float(result.stdout.strip())
    except Exception as e:
        print(f"Error getting video duration: {e}")
        return 0

def parse_time_to_seconds(time_str):
    """將時間字串轉換為秒數 (HH:MM:SS.mmm)"""
    # FFmpeg 輸出格式固定為 HH:MM:SS.mmm，例如 01:23:45.678；
    # 需同時支援毫秒（小數部分），因此使用 float() 解析秒數欄位
    try:
        parts = time_str.split(':')
        if len(parts) == 3:
            hours = float(parts[0])
            minutes = float(parts[1])
            seconds = float(parts[2])
            return hours * 3600 + minutes * 60 + seconds
        return 0
    except Exception as e:
        print(f"Error parsing time: {e}")
        return 0
