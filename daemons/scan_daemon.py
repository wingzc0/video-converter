import time
import threading
from datetime import datetime
from .base_daemon import BaseDaemon
from db_manager import db_manager
from converter import get_video_info
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
        self.scan_progress = {
            'status': 'idle',
            'last_scan_time': None,
            'files_scanned': 0,
            'tasks_added': 0,
            'errors': []
        }
        
        # 驗證設定
        self.validate_settings()
    
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
                    
                    # 檢查檔案是否已存在於資料庫
                    query = "SELECT id FROM conversion_tasks WHERE input_path = %s LIMIT 1"
                    result = db_manager.execute_query(query, (str(file_path),), fetch=True)
                    
                    if result:
                        continue
                    
                    # 檢查解析度
                    try:
                        video_info = get_video_info(str(file_path))
                        if not video_info:
                            continue
                        
                        width, height = map(int, video_info['resolution'].split('x'))
                        if height < self.min_resolution:
                            continue
                        
                        # 添加到資料庫
                        relative_path = file_path.relative_to(self.base_input_dir)
                        output_dir = self.base_output_dir / relative_path.parent
                        output_dir.mkdir(parents=True, exist_ok=True)
                        output_path = output_dir / f"480p_{filename}"
                        
                        query = '''
                        INSERT INTO conversion_tasks 
                        (input_path, output_path, source_resolution, status)
                        VALUES (%s, %s, %s, 'pending')
                        '''
                        db_manager.execute_query(query, (str(file_path), str(output_path), video_info['resolution']))
                        
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
        """檢查路徑是否應該被忽略"""
        # 實作忽略邏輯，這裡簡化
        ignore_dirs = os.getenv('IGNORE_DIRECTORIES', '').split(',')
        for ignore_dir in ignore_dirs:
            if ignore_dir and str(path).startswith(ignore_dir):
                return True
        return False
    
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
            'uptime': time.time() - os.stat(f"/proc/{os.getpid()}").st_ctime if os.path.exists(f"/proc/{os.getpid()}") else 0
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
