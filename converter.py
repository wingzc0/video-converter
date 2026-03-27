import subprocess
import json
import os
import threading
from pathlib import Path
import time

def get_video_info(input_path):
    """獲取影片資訊，包括解析度"""
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
                return {
                    'width': stream.get('width'),
                    'height': stream.get('height'),
                    'resolution': f"{stream.get('width')}x{stream.get('height')}"
                }
        return None
    except Exception as e:
        print(f"Error getting video info: {e}")
        return None

def convert_to_480p(input_path, output_path, progress_callback=None,
                    ffmpeg_timeout=None, ffmpeg_stall_timeout=None):
    """使用FFmpeg將影片轉換為480p，支援進度回調與超時保護

    Args:
        ffmpeg_timeout: 整體轉檔絕對上限（秒）。None 表示不限制。
        ffmpeg_stall_timeout: 多久未收到 ffmpeg 進度輸出即視為停頓（秒）。None 表示不限制。
    """
    # FFmpeg命令：自動縮放至480p並保持比例
    cmd = [
        'ffmpeg',
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
                time.sleep(2)

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
        # FFmpeg 將進度資訊寫入 stderr 而非 stdout；
        # 逐行讀取 stderr 以解析 time= 欄位，當 readline() 回傳空字串表示子程序輸出已結束
        try:
            while True:
                line = process.stderr.readline()
                if not line:
                    break
                # 忽略無法解碼的字元，不中斷進度讀取
                line = line.decode('utf-8', errors='ignore')
                
                # 保留最後 20 行 stderr 供失敗診斷，進度行（frame=/fps=/time= 開頭）通常不含有效錯誤訊息
                stripped = line.strip()
                if stripped and not stripped.startswith('frame='):
                    stderr_tail.append(stripped)
                    if len(stderr_tail) > 20:
                        stderr_tail.pop(0)

                # 解析FFmpeg輸出以追蹤進度
                if 'time=' in line:
                    time_str = line.split('time=')[1].split(' ')[0].strip()
                    current_time = parse_time_to_seconds(time_str)
                    last_progress_time[0] = time.monotonic()
                    
                    if duration > 0 and progress_callback:
                        # 進度最高上限 99.9%，100% 保留給 process_task 確認輸出檔案存在後才設定，
                        # 避免 FFmpeg 回傳成功但輸出檔案尚未完整寫入時就顯示 100%
                        progress = min(99.9, (current_time / duration) * 100)  # 保留100%給完成狀態
                        progress_callback(progress)
        except Exception as e:
            print(f"Conversion error: {e}")
            # 確保 ffmpeg 子程序不會成為孤兒程序繼續佔用資源；
            # 先關閉 stdout/stderr pipe 避免 pipe buffer 滿時 wait() 死鎖
            process.kill()
            try:
                process.stdout.close()
            except Exception:
                pass
            try:
                process.stderr.close()
            except Exception:
                pass
            process.wait()
            return False, str(e)
        
        return_code = process.wait()
        try:
            process.stdout.close()
        except Exception:
            pass
        try:
            process.stderr.close()
        except Exception:
            pass
        if return_code == 0:
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
