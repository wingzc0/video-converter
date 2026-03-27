import os
import json
import sys
import time
import signal
import logging
import logging.handlers
import threading
from pathlib import Path
from datetime import datetime
from abc import ABC, abstractmethod
from daemon import DaemonContext
from daemon.pidfile import TimeoutPIDLockFile
from dotenv import load_dotenv

load_dotenv()

def _get_process_uptime(pid):
    """回傳行程已執行秒數（使用 /proc/{pid}/stat 正確計算，而非 st_ctime）"""
    try:
        with open(f'/proc/{pid}/stat') as f:
            start_ticks = int(f.read().split()[21])
        clk_tck = os.sysconf(os.sysconf_names['SC_CLK_TCK'])
        with open('/proc/uptime') as f:
            system_uptime = float(f.read().split()[0])
        return system_uptime - (start_ticks / clk_tck)
    except Exception:
        return 0


class BaseDaemon(ABC):
    """
    基礎 daemon 類別，提供共用功能，支援從 .env 讀取 PID 和 log 路徑
    """
    
    def __init__(self, name, default_pid_file=None, default_log_file=None, default_stderr_log_file=None):
        self.name = name
        
        # 從環境變數讀取路徑，使用預設值作為備份
        self.pid_file = os.getenv(f'{name.upper()}_PID_FILE', default_pid_file or f"/var/run/{name}.pid")
        self.log_file = os.getenv(f'{name.upper()}_LOG_FILE', default_log_file or f"/var/log/{name}.log")
        self.stderr_log_file = os.getenv(f'{name.upper()}_ERROR_LOG_FILE', default_stderr_log_file or f"/var/log/{name}_error.log")
        
        # 確保目錄存在
        self.ensure_directories_exist()
        
        self.is_running = False
        self.daemon_context = None
        self.logger = self.setup_logger()
        
        # SIGTERM/SIGINT 皆導向 handle_shutdown，將 is_running 設為 False；
        # 主迴圈下一次迭代偵測到 is_running=False 後會自行結束，實現優雅停機而非強制中止
        signal.signal(signal.SIGTERM, self.handle_shutdown)
        signal.signal(signal.SIGINT, self.handle_shutdown)

        # 狀態檔案設定
        # 狀態以 JSON 檔案寫出，供 API 伺服器讀取，避免 daemon 與 API 伺服器之間的直接耦合；
        # API 伺服器不需知道 daemon 內部實作，只需讀取固定路徑的 JSON 即可取得最新狀態
        self.status_file = os.getenv(f'{name.upper()}_STATUS_FILE', f"/var/run/video-converter/{name}_status.json")
        self.status_update_interval = int(os.getenv(f'{name.upper()}_STATUS_UPDATE_INTERVAL', '10'))  # 預設10秒更新一次
        self.status_thread = None
        self.status_running = False
        
        # 確保狀態檔案目錄存在
        self.ensure_status_directory()
    
    def ensure_status_directory(self):
        """確保狀態檔案目錄存在"""
        try:
            status_dir = Path(self.status_file).parent
            if not status_dir.exists():
                status_dir.mkdir(parents=True, exist_ok=True)
                self.logger.info(f"Created status directory: {status_dir}")
            
            # 設定目錄權限
            status_dir.chmod(0o755)
            
        except Exception as e:
            self.logger.error(f"Error creating status directory: {e}")
    
    def get_current_status(self):
        """獲取目前狀態，子類別需實現"""
        return {
            'daemon_type': self.name,
            'status': 'unknown',
            'pid': os.getpid(),
            'uptime': _get_process_uptime(os.getpid()),
            'last_update': datetime.now().isoformat(),
            'error_count': 0,
            'errors': []
        }
    
    def write_status_file(self):
        """將目前狀態寫入狀態檔案"""
        try:
            status = self.get_current_status()
            
            # 確保目錄存在
            status_dir = Path(self.status_file).parent
            if not status_dir.exists():
                status_dir.mkdir(parents=True, exist_ok=True)
            
            # 寫入狀態檔案
            with open(self.status_file, 'w') as f:
                json.dump(status, f, indent=2)
            
            self.logger.debug(f"Status written to {self.status_file}")
            
        except Exception as e:
            self.logger.error(f"Error writing status file: {e}")
    
    def update_status_loop(self):
        """定期更新狀態檔案的循環"""
        self.logger.info(f"Status update thread started for {self.name}")
        
        while self.status_running:
            try:
                self.write_status_file()
                time.sleep(self.status_update_interval)
            except Exception as e:
                self.logger.error(f"Error in status update loop: {e}")
                time.sleep(5)  # 錯誤後等待5秒再重試
    
    def start_status_monitoring(self):
        """啟動狀態監控"""
        self.status_running = True
        self.status_thread = threading.Thread(target=self.update_status_loop)
        self.status_thread.daemon = True
        self.status_thread.start()
        self.logger.info(f"Status monitoring started for {self.name}")
    
    def stop_status_monitoring(self):
        """停止狀態監控"""
        self.status_running = False
        if self.status_thread and self.status_thread.is_alive():
            self.status_thread.join(timeout=5)
        self.logger.info(f"Status monitoring stopped for {self.name}")
    
    def ensure_directories_exist(self):
        """確保 PID 和 log 目錄存在，並設定正確權限"""
        try:
            # 確保 log 目錄存在
            log_dir = Path(self.log_file).parent
            if not log_dir.exists():
                log_dir.mkdir(parents=True, exist_ok=True)
                self.set_directory_permissions(log_dir)
            
            # 確保錯誤 log 目錄存在
            error_log_dir = Path(self.stderr_log_file).parent
            if not error_log_dir.exists():
                error_log_dir.mkdir(parents=True, exist_ok=True)
                self.set_directory_permissions(error_log_dir)
            
            # 確保 PID 目錄存在
            pid_dir = Path(self.pid_file).parent
            if not pid_dir.exists():
                pid_dir.mkdir(parents=True, exist_ok=True)
                self.set_directory_permissions(pid_dir)
            
            # 檢查目錄權限
            self.check_directory_permissions()
            
        except Exception as e:
            print(f"Error creating directories: {e}")
            raise
    
    def set_directory_permissions(self, directory):
        """設定目錄權限"""
        try:
            # 設定目錄權限為 755 (rwxr-xr-x)
            directory.chmod(0o755)
            
            # 嘗試設定擁有者（需要 root 權限，如果失敗則忽略）
            try:
                import pwd
                import grp
                # 獲取當前使用者和群組
                current_user = pwd.getpwuid(os.getuid()).pw_name
                current_group = grp.getgrgid(os.getgid()).gr_name
                
                # 設定擁有者（如果是以 root 執行）
                if os.getuid() == 0:
                    os.chown(str(directory), 
                            pwd.getpwnam(current_user).pw_uid, 
                            grp.getgrnam(current_group).gr_gid)
            except (ImportError, KeyError, PermissionError):
                # 無法設定擁有者，繼續執行
                pass
            
            print(f"Directory created with proper permissions: {directory}")
            
        except Exception as e:
            print(f"Warning: Could not set permissions for {directory}: {e}")
    
    def check_directory_permissions(self):
        """檢查目錄權限並顯示警告"""
        directories = [
            Path(self.log_file).parent,
            Path(self.stderr_log_file).parent,
            Path(self.pid_file).parent
        ]
        
        for directory in directories:
            try:
                if not os.access(str(directory), os.W_OK):
                    print(f"Warning: No write permission for directory: {directory}")
                    print(f"Current user: {os.getlogin()}")
                    print(f"Directory permissions: {oct(directory.stat().st_mode)[-3:]}")
            except Exception as e:
                print(f"Warning: Could not check permissions for {directory}: {e}")
    
    def setup_logger(self):
        """設定 logger，支援環境變數配置"""
        logger = logging.getLogger(self.name)
        
        # 從環境變數讀取 log level
        log_level = os.getenv('LOG_LEVEL', 'INFO').upper()
        logger.setLevel(getattr(logging, log_level, logging.INFO))
        
        # 移除現有的 handler
        logger.handlers = []
        
        # 檔案 handler
        try:
            file_handler = logging.handlers.WatchedFileHandler(self.log_file)
            file_handler.setFormatter(logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            ))
            logger.addHandler(file_handler)
        except Exception as e:
            print(f"Error setting up file handler for {self.log_file}: {e}")
            # 回退到標準輸出
            console_handler = logging.StreamHandler(sys.stdout)
            console_handler.setFormatter(logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            ))
            logger.addHandler(console_handler)
        
        # 標準輸出 handler（僅在非 daemon 模式下）
        if not hasattr(sys.stdout, 'fileno'):
            try:
                console_handler = logging.StreamHandler(sys.stdout)
                console_handler.setFormatter(logging.Formatter(
                    '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
                ))
                logger.addHandler(console_handler)
            except Exception as e:
                print(f"Error setting up console handler: {e}")
        
        return logger
    
    def daemonize(self):
        """將程式轉為 daemon 模式"""
        try:
            # 確保 PID 檔案目錄可寫
            pid_dir = Path(self.pid_file).parent
            if not os.access(str(pid_dir), os.W_OK):
                self.logger.error(f"PID directory not writable: {pid_dir}")
                raise PermissionError(f"PID directory not writable: {pid_dir}")
            
            # 確保 log 檔案目錄可寫
            log_dir = Path(self.log_file).parent
            if not os.access(str(log_dir), os.W_OK):
                self.logger.error(f"Log directory not writable: {log_dir}")
                raise PermissionError(f"Log directory not writable: {log_dir}")
            
            self.daemon_context = DaemonContext(
                pidfile=TimeoutPIDLockFile(self.pid_file, 5),
                stdout=open(self.log_file, 'a+'),
                stderr=open(self.stderr_log_file, 'a+'),
                working_directory=os.getcwd(),
                umask=0o002,
                # files_preserve=[]：明確指定不需保留的額外檔案描述符；
                # DaemonContext 在 double-fork 後會關閉所有非必要的 fd，
                # 若有需要跨 fork 保留的 fd（如 socket）應在此列出，避免它們被意外關閉
                files_preserve=[],
                # signal_map 在 DaemonContext 進入後重新綁定信號，
                # 因為 double-fork 後原本在父行程設定的 signal handler 可能已失效
                signal_map={
                    signal.SIGTERM: self.handle_shutdown,
                    signal.SIGINT: self.handle_shutdown,
                }
            )
            
            # DaemonContext.__enter__ 執行 double-fork daemonization：
            # 第一次 fork 脫離終端，第二次 fork 確保 daemon 不是 session leader，防止重新獲取 tty
            with self.daemon_context:
                # DaemonContext 在 double-fork 後會關閉所有 fd（files_preserve=[]），
                # 包含 setup_logger() 在 __init__ 建立的 WatchedFileHandler stream fd，
                # 因此必須在進入 daemon context 後重新初始化 logger，確保 fd 有效
                self.logger = self.setup_logger()
                # 在 daemonized 子程序內才啟動 status monitoring，確保 pid/狀態資訊正確
                self.start_status_monitoring()
                self.logger.info(f"{self.name} daemon started with PID: {os.getpid()}")
                self.is_running = True
                self.run()
                
        except Exception as e:
            self.logger.error(f"Failed to daemonize: {e}")
            raise
    
    def run_in_foreground(self):
        """在前景執行（用於測試）"""
        try:
            self.logger.info(f"{self.name} started in foreground mode with PID: {os.getpid()}")
            self.is_running = True
            self.run()
        except KeyboardInterrupt:
            self.handle_shutdown(None, None)
        except Exception as e:
            self.logger.error(f"Error in foreground mode: {e}")
            raise
    
    @abstractmethod
    def run(self):
        """子類別必須實現的執行方法"""
        pass
    
    @abstractmethod
    def get_progress(self):
        """子類別必須實現的進度獲取方法"""
        pass
    
    def handle_shutdown(self, signum, frame):
        """處理關閉信號"""
        self.logger.info(f"Received signal {signum}, shutting down gracefully...")
        self.is_running = False
        if self.daemon_context:
            self.daemon_context.terminate(signal.SIGTERM, frame)
        sys.exit(0)
    
    def check_pid_file(self):
        """檢查 PID 檔案是否存在，若存在則確認程序是否仍在執行。
        - 程序仍在執行：印出錯誤訊息並結束程式
        - 程序已不存在（殘留 PID 檔）：自動刪除後繼續啟動
        """
        pid_path = Path(self.pid_file)
        if not pid_path.exists():
            return

        try:
            pid = int(pid_path.read_text().strip())
        except (ValueError, OSError):
            # PID 檔內容無法讀取，視為殘留檔直接刪除
            pid_path.unlink(missing_ok=True)
            return

        # 檢查程序是否存在（os.kill(pid, 0) 不會送信號，只檢查程序是否存在）
        try:
            os.kill(pid, 0)
            # 程序存在，拒絕啟動
            print(f"Error: {self.name} is already running with PID {pid} (PID file: {self.pid_file})")
            print("If the process is no longer running, delete the PID file manually and retry.")
            sys.exit(1)
        except ProcessLookupError:
            # 程序已不存在，殘留 PID 檔，刪除後繼續
            print(f"Warning: Stale PID file found (PID {pid} is not running). Removing {self.pid_file}")
            pid_path.unlink(missing_ok=True)
        except PermissionError:
            # 程序存在但屬於其他使用者，同樣拒絕啟動
            print(f"Error: {self.name} is already running with PID {pid} (owned by another user).")
            sys.exit(1)

    def start(self, daemon_mode=True):
        """啟動 daemon，包含狀態監控"""
        self.check_pid_file()
        if daemon_mode:
            self.daemonize()
        else:
            # 前景模式在同一程序內啟動 status monitoring
            self.start_status_monitoring()
            self.run_in_foreground()
    
    def stop(self):
        """停止 daemon，包含狀態監控"""
        self.stop_status_monitoring()
        try:
            with open(self.pid_file, 'r') as f:
                pid = int(f.read().strip())
            
            os.kill(pid, signal.SIGTERM)
            self.logger.info(f"Sent SIGTERM to process {pid}")
            
            # 等待程序結束
            for _ in range(10):
                if not os.path.exists(f"/proc/{pid}"):
                    break
                time.sleep(1)
            else:
                # 強制結束
                os.kill(pid, signal.SIGKILL)
                self.logger.warning(f"Force killed process {pid}")
            
            # 清理 PID 檔案
            if os.path.exists(self.pid_file):
                os.remove(self.pid_file)
            
            self.logger.info(f"{self.name} daemon stopped successfully")
            return True
        
        except FileNotFoundError:
            self.logger.warning(f"PID file not found: {self.pid_file}")
            return False
        except ProcessLookupError:
            self.logger.warning(f"Process {pid} not found, cleaning up PID file")
            if os.path.exists(self.pid_file):
                os.remove(self.pid_file)
            return True
        except Exception as e:
            self.logger.error(f"Error stopping daemon: {e}")
            return False
    
    def restart(self):
        """重新啟動 daemon"""
        if self.stop():
            time.sleep(2)  # 等待清理
            self.start()
            return True
        return False
    
    def status(self):
        """檢查 daemon 狀態"""
        try:
            with open(self.pid_file, 'r') as f:
                pid = int(f.read().strip())
            
            if os.path.exists(f"/proc/{pid}"):
                return {
                    'status': 'running',
                    'pid': pid,
                    'uptime': _get_process_uptime(pid)
                }
            else:
                return {'status': 'stopped', 'pid': pid}
        
        except FileNotFoundError:
            return {'status': 'stopped'}
        except Exception as e:
            return {'status': 'unknown', 'error': str(e)}
