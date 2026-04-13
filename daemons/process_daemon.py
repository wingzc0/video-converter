import time
import threading
from datetime import datetime, timedelta, time as dtime
from .base_daemon import BaseDaemon, _get_process_uptime
from converter import convert_to_480p, get_video_duration
from task_manager import TaskRepository
import queue
from pathlib import Path
import os
import signal


# ---------------------------------------------------------------------------
# 時間限制輔助函式（模組層級，可供 conv_admin.py 等外部工具 import）
# ---------------------------------------------------------------------------

def _parse_allowed_time(time_str: str, default: dtime) -> dtime:
    """將 'HH:MM' 字串轉為 datetime.time 物件；解析失敗回傳 default。"""
    try:
        h, m = map(int, time_str.strip().split(':'))
        return dtime(h, m)
    except Exception:
        return default


def get_time_restriction_status(
    enabled: bool | None = None,
    start_str: str | None = None,
    end_str: str | None = None,
) -> dict:
    """回傳時間限制的完整狀態。

    當 enabled/start_str/end_str 為 None 時從環境變數讀取（適用於 daemon 未執行的情況）。
    傳入參數時使用傳入值（適用於從 daemon status 檔讀取真實設定）。

    Returns:
        dict with keys:
            enabled (bool):  是否啟用時間限制
            allowed (bool):  目前時間是否在允許時段（enabled=False 時恆為 True）
            start   (dtime): 允許開始時間
            end     (dtime): 允許結束時間
            wait_secs (int): 距下一個允許時段的秒數（allowed=True 時為 0）
            source (str):    'daemon'（從 status 檔讀）或 'env'（從環境變數讀）
    """
    if enabled is None:
        enabled = os.getenv('ENABLE_TIME_RESTRICTION', 'false').strip().lower() == 'true'
        start = _parse_allowed_time(os.getenv('ALLOWED_START_TIME', '22:00'), dtime(22, 0))
        end   = _parse_allowed_time(os.getenv('ALLOWED_END_TIME',   '06:00'), dtime(6,  0))
        source = 'env'
    else:
        start = _parse_allowed_time(start_str or '22:00', dtime(22, 0))
        end   = _parse_allowed_time(end_str   or '06:00', dtime(6,  0))
        source = 'daemon'

    if not enabled:
        return dict(enabled=False, allowed=True, start=start, end=end, wait_secs=0, source=source)

    current = datetime.now().time()
    if start > end:
        allowed = current >= start or current <= end
    else:
        allowed = start <= current <= end

    wait_secs = 0
    if not allowed:
        now = datetime.now()
        target = now.replace(hour=start.hour, minute=start.minute, second=0, microsecond=0)
        if target <= now:
            target += timedelta(days=1)
        wait_secs = max(0, int((target - now).total_seconds()))

    return dict(enabled=True, allowed=allowed, start=start, end=end, wait_secs=wait_secs, source=source)

class ProcessDaemon(BaseDaemon):
    """處理 Daemon，從資料庫取出 pending 任務並以 ffmpeg 轉檔為 480p。

    功能:
        - 執行緒池：max_workers 個 worker 執行緒並行轉檔
        - 任務排序：retry_count ASC, created_at ASC（新任務優先）
        - 資料庫列鎖：is_processing 旗標防止重複處理
        - ffmpeg 雙層超時：stall timeout（無進度）+ absolute timeout
        - 轉檔後驗證輸出時長，差異超過 DURATION_THRESHOLD 則標為 failed
        - 失敗重試：retry_count < MAX_RETRIES 的任務定期重新排入 pending
        - Stale 清理：卡在 processing 超過 STALE_HOURS 的任務標為 failed

    主要屬性:
        check_interval (int): 輪詢 DB 的間隔秒數
        max_workers (int): 並行 worker 執行緒數
        max_retries (int): 任務最大重試次數（來自 MAX_RETRIES 環境變數）
        stale_hours (float): 判定任務 stale 的閒置時數（來自 STALE_HOURS 環境變數）
    """
    
    def __init__(self, check_interval=60, max_workers=2):
        """初始化處理 Daemon。

        Args:
            check_interval: 每隔多少秒輪詢資料庫一次待處理任務（秒）。
                            由 daemon_ctl.py 從 CHECK_INTERVAL 環境變數讀入。
            max_workers: 同時執行轉檔的工作執行緒數量。
                         由 daemon_ctl.py 從 MAX_WORKERS 環境變數讀入。
        """
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
        # FFMPEG_TIMEOUT：整體轉檔絕對上限（秒），0 表示動態計算（依 FFMPEG_TIMEOUT_MULTIPLIER）
        # FFMPEG_STALL_TIMEOUT：多久無進度輸出即視為 NFS stall（秒），0 表示不限制
        _ft = int(os.getenv('FFMPEG_TIMEOUT', '0'))
        _fst = int(os.getenv('FFMPEG_STALL_TIMEOUT', '300'))
        self.ffmpeg_timeout = _ft if _ft > 0 else None
        self.ffmpeg_stall_timeout = _fst if _fst > 0 else None

        # 動態 timeout 參數（僅在 FFMPEG_TIMEOUT=0 時生效）
        # timeout = max(FFMPEG_TIMEOUT_MIN, video_duration * FFMPEG_TIMEOUT_MULTIPLIER)
        self.timeout_multiplier = float(os.getenv('FFMPEG_TIMEOUT_MULTIPLIER', '2.0'))
        self.min_timeout = int(os.getenv('FFMPEG_TIMEOUT_MIN', '300'))

        # 時間限制設定
        self.enable_time_restriction = os.getenv('ENABLE_TIME_RESTRICTION', 'false').strip().lower() == 'true'
        self.allowed_start_time = _parse_allowed_time(os.getenv('ALLOWED_START_TIME', '22:00'), dtime(22, 0))
        self.allowed_end_time   = _parse_allowed_time(os.getenv('ALLOWED_END_TIME',   '06:00'), dtime(6,  0))
        
        # 驗證設定
        self.validate_settings()
        self.task_repo = TaskRepository(self.logger)
    
    def validate_settings(self):
        """驗證設定"""
        self.logger.info(f"Process daemon initialized with {self.max_workers} workers")
        self.logger.info(f"Check interval: {self.check_interval} seconds")
        self.logger.info(f"Max retries: {self.max_retries}, retry every {self.retry_interval_cycles} cycles, stale after {self.stale_hours}h")
        self.logger.info(
            f"ffmpeg timeout: {self.ffmpeg_timeout or f'dynamic ({self.timeout_multiplier}x, min {self.min_timeout}s)'}s, "
            f"stall timeout: {self.ffmpeg_stall_timeout or 'disabled'}s"
        )
        if self.enable_time_restriction:
            self.logger.info(f"Time restriction enabled: {self.allowed_start_time.strftime('%H:%M')} - {self.allowed_end_time.strftime('%H:%M')}")

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
            _diag = {}
            success, conv_error = convert_to_480p(
                input_path, output_path, progress_callback,
                ffmpeg_timeout=self.ffmpeg_timeout,
                ffmpeg_stall_timeout=self.ffmpeg_stall_timeout,
                timeout_multiplier=self.timeout_multiplier,
                min_timeout=self.min_timeout,
                _diag=_diag,
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
                        # ffprobe 無法讀取輸出檔，檔案可能損毀或不完整；
                        # 刪除輸出檔並標記為 failed 交由重試機制處理，
                        # 與時長不符的處理保持一致，避免損毀檔案留存佔用空間
                        error_msg = "Could not verify output duration (ffprobe returned 0); marked for retry"
                        self.logger.warning(f"Task {task_id}: {error_msg}")
                        Path(output_path).unlink(missing_ok=True)
                        self.update_task_status(task_id, 'failed', error_message=error_msg)
                        self.processing_progress['tasks_failed'] += 1
                        return
                    elif abs(src_dur - out_dur) > self.duration_threshold:
                        ffmpeg_last_time = _diag.get('current_time', -1)
                        ffmpeg_stderr = _diag.get('stderr_tail', [])
                        diag_str = f"ffmpeg_last_time={ffmpeg_last_time:.1f}s"
                        if ffmpeg_stderr:
                            diag_str += f", stderr_tail=[{' | '.join(ffmpeg_stderr[-2:])}]"
                        error_msg = (
                            f"Incomplete output: src={src_dur:.1f}s, out={out_dur:.1f}s, "
                            f"diff={src_dur - out_dur:.1f}s > threshold={self.duration_threshold}s"
                        )
                        self.logger.warning(f"Task {task_id}: {error_msg} | diag: {diag_str}")
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
            # 取出任務後再次確認時間限制，避免 queue 清空期間的競爭視窗
            if not self.is_time_allowed():
                self.logger.debug(f"Worker {worker_id}: skipping task {task_id} (time restriction)")
                self.task_queue.task_done()
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
                time.sleep(1)  # 例外後短暫等待，避免錯誤迴圈耗盡 CPU
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
        from task_manager import find_orphaned_ffmpeg_candidates

        daemon_pids = self._get_daemon_descendant_pids()
        candidates = find_orphaned_ffmpeg_candidates(self.task_repo, daemon_pids)
        killed = 0

        for c in candidates:
            self.logger.warning(
                f"Killing orphaned ffmpeg PID {c['pid']} "
                f"(task_id={c['task_id']}, status={c['status']}, input={c['input_path']})"
            )
            try:
                os.kill(c['pid'], signal.SIGKILL)
                killed += 1
            except ProcessLookupError:
                pass  # 程序已自行結束

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
            if self._check_cycle % self.retry_interval_cycles == 0:
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
                    # 已在執行中的轉檔（ffmpeg 已啟動）不會被中斷，會自行完成
                    active = sum(1 for t in self.worker_threads if t.is_alive())
                    if active:
                        self.logger.info(
                            f"{active} worker(s) still running — in-progress conversions "
                            "will complete before time restriction fully takes effect"
                        )
                    self.processing_progress['status'] = 'time_restricted'
                    # 清空 task_queue：避免已排入的任務在限制期間被 worker 繼續執行。
                    # 使用 get_nowait() + except Empty 而非 empty() + get_nowait()，
                    # 避免兩者之間 worker 搶先消費造成競爭視窗。
                    drained = 0
                    while True:
                        try:
                            self.task_queue.get_nowait()
                            self.task_queue.task_done()
                            drained += 1
                        except queue.Empty:
                            break
                    if drained:
                        self.logger.info(f"Drained {drained} queued task(s) due to time restriction")
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
            'last_update': datetime.now().isoformat(),
            # daemon 啟動時讀取的時間限制設定（非目前 .env 內容）
            'time_restriction_enabled': self.enable_time_restriction,
            'time_restriction_start': self.allowed_start_time.strftime('%H:%M'),
            'time_restriction_end': self.allowed_end_time.strftime('%H:%M'),
        }
