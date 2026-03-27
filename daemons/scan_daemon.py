import time
import threading
from datetime import datetime, timedelta
from .base_daemon import BaseDaemon, _get_process_uptime
from converter import get_video_info
from task_manager import TaskRepository
from pathlib import Path
import os

class ScanDaemon(BaseDaemon):
    """
    掃描 daemon，負責定期掃描目錄並添加新任務到資料庫
    """
    
    def __init__(self, scan_interval=300):
        # 使用 .env 中的設定
        super().__init__(
            name="scan_daemon",
            default_pid_file=os.getenv('SCAN_DAEMON_PID_FILE', '/var/run/video-converter/scanner.pid'),
            default_log_file=os.getenv('SCAN_DAEMON_LOG_FILE', '/var/log/video-converter/scanner.log'),
            default_stderr_log_file=os.getenv('SCAN_DAEMON_ERROR_LOG_FILE', '/var/log/video-converter/scanner_error.log')
        )
        self.scan_interval = scan_interval  # 掃描間隔（秒）
        self.base_input_dir = Path(os.getenv('INPUT_DIRECTORY', '')).resolve()
        self.base_output_dir = Path(os.getenv('OUTPUT_DIRECTORY', '')).resolve()
        self.supported_extensions = set(ext.strip().lower() for ext in os.getenv('SUPPORTED_EXTENSIONS', '.mp4,.mkv,.avi,.mov,.flv,.wmv,.m4v,.webm').split(','))
        self.min_resolution = int(os.getenv('MIN_RESOLUTION', '481'))

        # 解析忽略目錄清單，轉為 resolved Path 物件以便精確比對
        raw_ignore = os.getenv('IGNORE_DIRECTORIES', '')
        self.ignore_directories = [
            Path(d.strip()).resolve()
            for d in raw_ignore.split(',') if d.strip()
        ]

        # 輸出檔長度驗證設定已移至 process_daemon（轉檔完成後才驗證）

        self.scan_progress = {
            'status': 'idle',
            'last_scan_time': None,
            'files_scanned': 0,
            'tasks_added': 0,
            'errors': []
        }
        
        # 驗證設定
        self.validate_settings()
        self.task_repo = TaskRepository(self.logger)
    
    def validate_settings(self):
        """驗證設定"""
        if not self.base_input_dir.exists():
            raise ValueError(f"Input directory not found: {self.base_input_dir}")
        if not self.base_input_dir.is_dir():
            raise ValueError(f"Input path is not a directory: {self.base_input_dir}")
        
        self.logger.info(f"Scan daemon initialized with interval: {self.scan_interval} seconds")
        self.logger.info(f"Input directory: {self.base_input_dir}")
        self.logger.info(f"Output directory: {self.base_output_dir}")
    
    def scan_directory(self):
        """掃描目錄並添加新任務"""
        self.scan_progress['status'] = 'scanning'
        self.scan_progress['last_scan_time'] = datetime.now()
        self.scan_progress['files_scanned'] = 0
        self.scan_progress['tasks_added'] = 0
        self.scan_progress['errors'] = []
        
        self.logger.info("Starting directory scan...")
        
        try:
            for root, dirs, files in os.walk(str(self.base_input_dir)):
                current_dir = Path(root)
                
                # 檢查是否在忽略目錄中
                if self.should_ignore_path(current_dir):
                    continue
                
                for filename in files:
                    self.scan_progress['files_scanned'] += 1
                    
                    file_path = current_dir / filename
                    file_ext = file_path.suffix.lower()
                    
                    if file_ext not in self.supported_extensions:
                        continue

                    # 跳過已轉換的輸出檔案（以 480p_ 開頭）
                    if self.should_skip_file(filename):
                        continue
                    
                    # ── Step 1: DB 查詢（無 NFS I/O）────────────────────────────────
                    # 同時取出 output_path，供 completed 狀態直接驗證輸出檔，避免重跑 ffprobe
                    task = self.task_repo.get_task_by_input_path(str(file_path))
                    if task:
                        db_status = task.get('status', '')
                        if db_status in ('pending', 'processing', 'failed'):
                            continue

                        if db_status == 'completed':
                            stored_output = task.get('output_path', '')
                            if stored_output and Path(stored_output).exists():
                                continue  # 輸出檔存在，無需任何動作
                            # 輸出檔遺失：重新排入佇列
                            self.logger.warning(
                                f"Output missing for completed task, re-queuing: {file_path.name}"
                            )
                            self.task_repo.requeue_missing_output(str(file_path))
                            continue

                    # ── Step 2: 計算輸出路徑（純字串運算，無 NFS I/O）────────────────
                    # 一律使用 .mp4 副檔名：converter 輸出 H.264+AAC，
                    # 僅 MP4 容器能完全相容，mpg/mxf/avi 等容器會導致 mux 失敗。
                    relative_path = file_path.relative_to(self.base_input_dir)
                    output_dir = self.base_output_dir / relative_path.parent
                    stem = file_path.stem
                    output_path = output_dir / f"480p_{stem}.mp4"

                    # ── Step 3: 輸出檔已存在則跳過（一次 stat，避免 ffprobe）────────
                    if output_path.exists():
                        continue

                    # ── Step 4: 僅對全新且尚無輸出的檔案呼叫 ffprobe ────────────────
                    try:
                        video_info = get_video_info(str(file_path))
                        if not video_info:
                            continue

                        width, height = map(int, video_info['resolution'].split('x'))
                        # 以 height 判斷是否需要轉換：影片解析度標準（480p、720p、1080p）皆以高度為基準；
                        # 若 height < min_resolution（預設 481），表示已是 480p 或更低，無需轉換
                        if height < self.min_resolution:
                            continue

                        output_dir.mkdir(parents=True, exist_ok=True)

                        # 使用 INSERT IGNORE 防止 TOCTOU race condition：
                        # 若兩個 scan 程序同時掃到同一個檔案，不會因 UNIQUE 限制而拋出例外
                        rows = self.task_repo.insert_task(
                            str(file_path), str(output_path), video_info['resolution']
                        )

                        # rows=0 表示 INSERT IGNORE 遇到 UNIQUE 衝突而靜默忽略（另一個 scan 已先插入），
                        # 不應計入本次新增的任務數
                        if rows > 0:
                            self.scan_progress['tasks_added'] += 1

                    except Exception as e:
                        error_msg = f"Error processing {file_path}: {str(e)}"
                        self.logger.error(error_msg)
                        self.scan_progress['errors'].append(error_msg)
            
            self.logger.info(f"Scan completed. Files scanned: {self.scan_progress['files_scanned']}, Tasks added: {self.scan_progress['tasks_added']}")
            
        except Exception as e:
            error_msg = f"Error during directory scan: {str(e)}"
            self.logger.error(error_msg)
            self.scan_progress['errors'].append(error_msg)
        finally:
            self.scan_progress['status'] = 'idle'
    
    def should_ignore_path(self, path):
        """檢查路徑是否應該被忽略，使用 Path.relative_to() 進行精確比對，
        避免字串前綴誤匹配（如 /data/out 誤匹配 /data/output）"""
        resolved = Path(path).resolve()
        for ignore_dir in self.ignore_directories:
            if resolved == ignore_dir:
                return True
            try:
                resolved.relative_to(ignore_dir)
                return True
            except ValueError:
                pass
        return False

    def should_skip_file(self, filename):
        """跳過已轉換的輸出檔案（以 480p_ 開頭）"""
        return filename.startswith('480p_')
    
    def run(self):
        """執行掃描 daemon"""
        self.logger.info("Scan daemon started")
        
        while self.is_running:
            try:
                self.scan_directory()
                
                # 等待下次掃描
                for _ in range(self.scan_interval):
                    if not self.is_running:
                        break
                    time.sleep(1)
            
            except Exception as e:
                self.logger.error(f"Error in scan daemon: {str(e)}")
                time.sleep(60)  # 錯誤後等待1分鐘再重試
    
    def get_progress(self):
        """獲取掃描進度"""
        return {
            'daemon_type': 'scan',
            'status': self.scan_progress['status'],
            'last_scan_time': self.scan_progress['last_scan_time'].isoformat() if self.scan_progress['last_scan_time'] else None,
            'files_scanned': self.scan_progress['files_scanned'],
            'tasks_added': self.scan_progress['tasks_added'],
            'error_count': len(self.scan_progress['errors']),
            'uptime': _get_process_uptime(os.getpid()),
        }

    def get_current_status(self):
        """獲取掃描 daemon 的目前狀態"""
        base_status = super().get_current_status()
    
        # 獲取 daemon 基本狀態
        daemon_status = self.status()
    
        return {
            **base_status,
            'daemon_type': 'scan',
            'status': self.scan_progress['status'],
            'pid': daemon_status.get('pid'),
            'uptime': daemon_status.get('uptime', 0),
            'last_scan_time': self.scan_progress['last_scan_time'].isoformat() if self.scan_progress['last_scan_time'] else None,
            'files_scanned': self.scan_progress['files_scanned'],
            'tasks_added': self.scan_progress['tasks_added'],
            'error_count': len(self.scan_progress['errors']),
            'errors': self.scan_progress['errors'][:10],  # 只保留最近10個錯誤
            'last_update': datetime.now().isoformat()
        }
