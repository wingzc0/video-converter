import time
import threading
from datetime import datetime, timedelta
from .base_daemon import BaseDaemon, _get_process_uptime
from converter import convert_to_480p, get_video_duration
from task_manager import TaskRepository
import queue
from pathlib import Path
import os
import signal

class ProcessDaemon(BaseDaemon):
    """
    處理 daemon，負責處理資料庫中的任務
    """
    
    def __init__(self, check_interval=60, max_workers=2):
        # 使用 .env 中的設定
        super().__init__(
            name="process_daemon",
            default_pid_file=os.getenv('PROCESS_DAEMON_PID_FILE', '/var/run/video-converter/processor.pid'),
            default_log_file=os.getenv('PROCESS_DAEMON_LOG_FILE', '/var/log/video-converter/processor.log'),
            default_stderr_log_file=os.getenv('PROCESS_DAEMON_ERROR_LOG_FILE', '/var/log/video-converter/processor_error.log')
        )
        self.check_interval = check_interval  # 檢查間隔（秒）
        self.max_workers = max_workers
        self.task_queue = queue.Queue()
        self.worker_threads = []
        self.processing_progress = {
            'status': 'idle',
            'last_check_time': None,
            'tasks_processing': 0,
            'tasks_completed': 0,
            'tasks_failed': 0,
            'errors': []
        }
        self.worker_locks = {}

        # 重試與清理設定
        self.max_retries = int(os.getenv('MAX_RETRIES', '3'))
        self.retry_interval_cycles = int(os.getenv('RETRY_INTERVAL_CYCLES', '10'))
        self.stale_hours = float(os.getenv('STALE_HOURS', '1'))
        self._check_cycle = 0  # 累計 check 次數，用於控制重試頻率

        # 輸出檔長度驗證：轉檔完成後確認輸出時長與來源差距不超過此閾值（秒）
        # 設為 0 可停用長度驗證
        self.duration_threshold = float(os.getenv('DURATION_THRESHOLD', '2.0'))

        # ffmpeg 超時保護
        # FFMPEG_TIMEOUT：整體轉檔絕對上限（秒），0 表示不限制
        # FFMPEG_STALL_TIMEOUT：多久無進度輸出即視為 NFS stall（秒），0 表示不限制
        _ft = int(os.getenv('FFMPEG_TIMEOUT', '7200'))
        _fst = int(os.getenv('FFMPEG_STALL_TIMEOUT', '300'))
        self.ffmpeg_timeout = _ft if _ft > 0 else None
        self.ffmpeg_stall_timeout = _fst if _fst > 0 else None

        # 時間限制設定
        self.enable_time_restriction = os.getenv('ENABLE_TIME_RESTRICTION', 'false').strip().lower() == 'true'
        self.allowed_start_time = self._parse_time(os.getenv('ALLOWED_START_TIME', '22:00'))
        self.allowed_end_time = self._parse_time(os.getenv('ALLOWED_END_TIME', '06:00'))
        
        # 驗證設定
        self.validate_settings()
        self.task_repo = TaskRepository(self.logger)
    
    def validate_settings(self):
        """驗證設定"""
        self.logger.info(f"Process daemon initialized with {self.max_workers} workers")
        self.logger.info(f"Check interval: {self.check_interval} seconds")
        self.logger.info(f"Max retries: {self.max_retries}, retry every {self.retry_interval_cycles} cycles, stale after {self.stale_hours}h")
        self.logger.info(
            f"ffmpeg timeout: {self.ffmpeg_timeout or 'disabled'}s, "
            f"stall timeout: {self.ffmpeg_stall_timeout or 'disabled'}s"
        )
        if self.enable_time_restriction:
            self.logger.info(f"Time restriction enabled: {self.allowed_start_time.strftime('%H:%M')} - {self.allowed_end_time.strftime('%H:%M')}")

    @staticmethod
    def _parse_time(time_str):
        """將 'HH:MM' 字串轉為 datetime.time 物件"""
        try:
            h, m = map(int, time_str.strip().split(':'))
            from datetime import time as dtime
            return dtime(h, m)
        except Exception:
            from datetime import time as dtime
            return dtime(22, 0)

    def is_time_allowed(self):
        """檢查目前時間是否在允許轉檔的時段內"""
        if not self.enable_time_restriction:
            return True
        current = datetime.now().time()
        start, end = self.allowed_start_time, self.allowed_end_time
        if start > end:
            # 跨日時段，例如 22:00 - 06:00
            return current >= start or current <= end
        return start <= current <= end

    def get_time_until_allowed(self):
        """計算距離下一個允許時段開始的秒數"""
        if not self.enable_time_restriction or self.is_time_allowed():
            return 0
        now = datetime.now()
        start = self.allowed_start_time
        target = now.replace(hour=start.hour, minute=start.minute, second=0, microsecond=0)
        if target <= now:
            target += timedelta(days=1)
        return max(0, (target - now).total_seconds())
    
    def get_pending_tasks(self):
        """獲取待處理的任務"""
        return self.task_repo.get_pending_tasks()

    def update_task_status(self, task_id, status, progress=None, error_message=None):
        """更新任務狀態"""
        self.task_repo.update_task_status(task_id, status, progress, error_message)

    def acquire_task_lock(self, task_id, worker_id):
        """取得任務鎖：原子性設定 is_processing=TRUE 並寫入 processing_lock"""
        return self.task_repo.acquire_task_lock(task_id, worker_id)

    def release_task_lock(self, task_id, worker_id):
        """釋放任務鎖：清除 is_processing 旗標並移除 processing_lock 紀錄"""
        return self.task_repo.release_task_lock(task_id, worker_id)
    
    def process_task(self, task_id, worker_id):
        """處理單個任務"""
        try:
            # 取得任務詳細資訊
            task = self.task_repo.get_task_by_id(task_id)

            if not task:
                self.logger.warning(f"Task {task_id} not found")
                return

            input_path = task['input_path']
            output_path = task['output_path']
            
            # 檢查檔案是否存在
            if not os.path.exists(input_path):
                self.update_task_status(task_id, 'failed', error_message=f"Input file not found: {input_path}")
                self.processing_progress['tasks_failed'] += 1
                return
            
            # 檢查輸出目錄是否存在
            output_dir = Path(output_path).parent
            output_dir.mkdir(parents=True, exist_ok=True)
            
            # 定義進度回調函數
            def progress_callback(progress):
                self.update_task_status(task_id, 'processing', progress=progress)
            
            # 執行轉檔
            success, conv_error = convert_to_480p(
                input_path, output_path, progress_callback,
                ffmpeg_timeout=self.ffmpeg_timeout,
                ffmpeg_stall_timeout=self.ffmpeg_stall_timeout,
            )
            
            # 更新最終狀態
            if success:
                # 轉檔成功後驗證輸出時長，避免儲存不完整的輸出檔
                if self.duration_threshold > 0:
                    src_dur = get_video_duration(input_path)
                    out_dur = get_video_duration(output_path)
                    if src_dur == 0:
                        # 無法讀取來源時長（NFS 暫時性問題或來源損毀），
                        # 保留輸出並標記完成，避免誤刪可能完整的輸出檔
                        self.logger.warning(
                            f"Task {task_id}: Could not read source duration (ffprobe returned 0), skipping validation"
                        )
                    elif out_dur == 0:
                        # ffprobe 無法讀取輸出檔，無法確認是否完整；
                        # 標記為 failed 交由重試機制處理，避免靜默接受損毀輸出
                        error_msg = "Could not verify output duration (ffprobe returned 0); marked for retry"
                        self.logger.warning(f"Task {task_id}: {error_msg}")
                        self.update_task_status(task_id, 'failed', error_message=error_msg)
                        self.processing_progress['tasks_failed'] += 1
                        return
                    elif abs(src_dur - out_dur) > self.duration_threshold:
                        error_msg = (
                            f"Incomplete output: src={src_dur:.1f}s, out={out_dur:.1f}s, "
                            f"diff={src_dur - out_dur:.1f}s > threshold={self.duration_threshold}s"
                        )
                        self.logger.warning(f"Task {task_id}: {error_msg}")
                        Path(output_path).unlink(missing_ok=True)
                        self.update_task_status(task_id, 'failed', error_message=error_msg)
                        self.processing_progress['tasks_failed'] += 1
                        return
                self.update_task_status(task_id, 'completed', progress=100.0)
                self.processing_progress['tasks_completed'] += 1
                self.logger.info(f"Task {task_id} completed successfully: {output_path}")
            else:
                error_msg = conv_error or "Conversion failed"
                self.update_task_status(task_id, 'failed', error_message=error_msg)
                self.processing_progress['tasks_failed'] += 1
                self.logger.error(f"Task {task_id} failed: {input_path} ({error_msg})")
            
        except Exception as e:
            error_msg = f"Error processing task {task_id}: {str(e)}"
            self.logger.error(error_msg)
            self.update_task_status(task_id, 'failed', error_message=error_msg)
            self.processing_progress['tasks_failed'] += 1
            self.processing_progress['errors'].append(error_msg)
        finally:
            pass  # 鎖的釋放由 worker() 統一管理，確保任何例外路徑都能釋放
    
    def worker(self, worker_id):
        """工作執行緒"""
        self.logger.info(f"Worker {worker_id} started")
        
        while self.is_running:
            try:
                task_id = self.task_queue.get(timeout=1)
            except queue.Empty:
                continue
            lock_acquired = False
            try:
                # 取得 DB 層級的任務鎖，防止多 worker（或跨程序）同時處理同一任務；
                # 若鎖取得失敗（任務已被其他 worker 取走），直接略過
                if not self.acquire_task_lock(task_id, worker_id):
                    self.logger.debug(f"Worker {worker_id}: task {task_id} already locked, skipping")
                    continue
                lock_acquired = True
                with self.worker_locks[worker_id]:
                    self.process_task(task_id, worker_id)
            except Exception as e:
                self.logger.error(f"Worker {worker_id} error: {str(e)}")
                time.sleep(1)
            finally:
                # release_task_lock 集中在 worker() 管理：process_task() 內部任何例外、
                # worker_locks 操作失敗等情況都能確保 is_processing 旗標被清除，
                # 避免 cleanup_stale_tasks() 誤判為卡住任務
                if lock_acquired:
                    self.release_task_lock(task_id, worker_id)
                # 無論成功、失敗或例外，都必須呼叫 task_done()，
                # 否則 queue 內部計數器不會歸零，若未來使用 join() 會導致永久阻塞
                self.task_queue.task_done()
        
        self.logger.info(f"Worker {worker_id} stopped")
    
    def retry_failed_tasks(self):
        """將未超過重試上限的失敗任務重新排入 pending，每 retry_interval_cycles 個 check cycle 執行一次"""
        return self.task_repo.retry_failed_tasks(self.max_retries)

    def _get_daemon_descendant_pids(self):
        """回傳目前 process daemon 所有子孫 PID 的集合（含自身）"""
        try:
            import psutil
            me = psutil.Process(os.getpid())
            pids = {me.pid}
            for child in me.children(recursive=True):
                pids.add(child.pid)
            return pids
        except Exception:
            return {os.getpid()}

    def kill_orphaned_ffmpeg(self):
        """
        掃描系統中所有不在本 daemon 子孫樹下的 ffmpeg 程序，
        若其 -i 參數指向的 source file 存在於 DB 的任務中，則 kill 之。
        """
        try:
            import psutil
        except ImportError:
            return

        daemon_pids = self._get_daemon_descendant_pids()
        killed = 0

        for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
            try:
                if proc.info['name'] != 'ffmpeg':
                    continue
                if proc.pid in daemon_pids:
                    continue

                cmdline = proc.info['cmdline'] or []
                # 從 cmdline 找 -i 後的 input_path
                input_path = None
                for idx, arg in enumerate(cmdline):
                    if arg == '-i' and idx + 1 < len(cmdline):
                        input_path = cmdline[idx + 1]
                        break

                if not input_path:
                    continue

                task = self.task_repo.get_task_by_input_path(input_path)
                if task is None:
                    continue

                # Only kill if the task is in an active state; skip completed/failed
                # to avoid killing unrelated ffmpeg processes using the same source file.
                if task.get('status') not in ('pending', 'processing'):
                    continue

                self.logger.warning(
                    f"Killing orphaned ffmpeg PID {proc.pid} "
                    f"(task_id={task['id']}, status={task.get('status')}, input={input_path})"
                )
                try:
                    os.kill(proc.pid, signal.SIGKILL)
                    killed += 1
                except ProcessLookupError:
                    pass  # 程序已自行結束

            except (psutil.NoSuchProcess, psutil.AccessDenied):
                continue

        if killed:
            self.logger.warning(f"Killed {killed} orphaned ffmpeg process(es)")
        return killed

    def cleanup_stale_tasks(self):
        """將卡在 processing 超過 stale_hours 的任務標記為 failed，每次 check cycle 都執行；
        同時 kill 不在本 daemon 下且 source file 有 DB 記錄的孤兒 ffmpeg 程序。"""
        self.kill_orphaned_ffmpeg()
        return self.task_repo.cleanup_stale_tasks(self.stale_hours)

    def check_and_process_tasks(self):
        """檢查並處理任務"""
        self._check_cycle += 1
        self.processing_progress['status'] = 'checking'
        self.processing_progress['last_check_time'] = datetime.now()
        
        try:
            # 每次都清理過時任務
            self.cleanup_stale_tasks()

            # 每 retry_interval_cycles 次才執行一次重試
            if self._check_cycle % self.retry_interval_cycles == 1:
                self.retry_failed_tasks()

            pending_tasks = self.get_pending_tasks()
            
            if pending_tasks:
                self.logger.info(f"Found {len(pending_tasks)} pending tasks")
                for task in pending_tasks:
                    # 主執行緒負責將任務 ID 放入 task_queue，
                    # worker 執行緒從 queue 取出後再各自競爭 acquire_task_lock，
                    # 確保每個任務只被一個 worker 實際執行
                    self.task_queue.put(task['id'])
            
            self.processing_progress['status'] = 'processing'
            self.processing_progress['tasks_processing'] = self.task_queue.qsize()
            
        except Exception as e:
            error_msg = f"Error checking tasks: {str(e)}"
            self.logger.error(error_msg)
            self.processing_progress['errors'].append(error_msg)
        finally:
            if self.processing_progress['status'] == 'checking':
                self.processing_progress['status'] = 'idle'
    
    def run(self):
        """執行處理 daemon"""
        self.logger.info("Process daemon started")

        # 清理上次崩潰留下的孤兒 is_processing 旗標，避免這些任務永遠無法被處理
        try:
            cleaned = self.task_repo.cleanup_orphaned_flags()
            if cleaned:
                self.logger.info(f"Cleaned up {cleaned} orphaned is_processing flag(s) from previous run")
        except Exception as e:
            self.logger.error(f"Error cleaning up orphaned tasks: {str(e)}")

        # 建立工作執行緒
        for i in range(self.max_workers):
            worker_id = f"worker_{i}"
            self.worker_locks[worker_id] = threading.Lock()
            thread = threading.Thread(target=self.worker, args=(worker_id,))
            # daemon=True：主執行緒結束時 worker 執行緒自動終止，不需要額外的 join 或停止邏輯
            thread.daemon = True
            thread.start()
            self.worker_threads.append(thread)
        
        # 主循環
        while self.is_running:
            try:
                # 時間限制：若不在允許時段，等到允許時間
                if not self.is_time_allowed():
                    wait_secs = self.get_time_until_allowed()
                    self.logger.info(
                        f"Time restriction active. Waiting {wait_secs:.0f}s until "
                        f"{self.allowed_start_time.strftime('%H:%M')}"
                    )
                    self.processing_progress['status'] = 'time_restricted'
                    # 分段等待，以便能及時響應停止訊號
                    waited = 0
                    while self.is_running and waited < wait_secs:
                        time.sleep(min(60, wait_secs - waited))
                        waited += 60
                        if self.is_time_allowed():
                            break
                    self.processing_progress['status'] = 'idle'
                    continue

                self.check_and_process_tasks()
                
                # 等待下次檢查
                for _ in range(self.check_interval):
                    if not self.is_running:
                        break
                    time.sleep(1)
            
            except Exception as e:
                self.logger.error(f"Error in process daemon: {str(e)}")
                time.sleep(60)  # 錯誤後等待1分鐘再重試
    
    def get_progress(self):
        """獲取處理進度"""
        return {
            'daemon_type': 'process',
            'status': self.processing_progress['status'],
            'last_check_time': self.processing_progress['last_check_time'].isoformat() if self.processing_progress['last_check_time'] else None,
            'tasks_processing': self.processing_progress['tasks_processing'],
            'tasks_completed': self.processing_progress['tasks_completed'],
            'tasks_failed': self.processing_progress['tasks_failed'],
            'error_count': len(self.processing_progress['errors']),
            'queue_size': self.task_queue.qsize(),
            'active_workers': len([t for t in self.worker_threads if t.is_alive()]),
            'max_workers': self.max_workers,
            'uptime': _get_process_uptime(os.getpid()),
        }

    def get_current_status(self):
        """獲取處理 daemon 的目前狀態"""
        base_status = super().get_current_status()
    
        # 獲取 daemon 基本狀態
        daemon_status = self.status()
    
        return {
            **base_status,
            'daemon_type': 'process',
            'status': self.processing_progress['status'],
            'pid': daemon_status.get('pid'),
            'uptime': daemon_status.get('uptime', 0),
            'last_check_time': self.processing_progress['last_check_time'].isoformat() if self.processing_progress['last_check_time'] else None,
            'tasks_processing': self.processing_progress['tasks_processing'],
            'tasks_completed': self.processing_progress['tasks_completed'],
            'tasks_failed': self.processing_progress['tasks_failed'],
            'queue_size': self.task_queue.qsize() if hasattr(self, 'task_queue') else 0,
            'active_workers': len([t for t in self.worker_threads if t.is_alive()]) if hasattr(self, 'worker_threads') else 0,
            'max_workers': self.max_workers,
            'error_count': len(self.processing_progress['errors']),
            'errors': self.processing_progress['errors'][:10],  # 只保留最近10個錯誤
            'last_update': datetime.now().isoformat()
        }
