import time
import threading
from datetime import datetime, timedelta
from .base_daemon import BaseDaemon
from db_manager import db_manager
from converter import convert_to_480p
import queue
from pathlib import Path
import os

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

        # 時間限制設定
        self.enable_time_restriction = os.getenv('ENABLE_TIME_RESTRICTION', 'false').strip().lower() == 'true'
        self.allowed_start_time = self._parse_time(os.getenv('ALLOWED_START_TIME', '22:00'))
        self.allowed_end_time = self._parse_time(os.getenv('ALLOWED_END_TIME', '06:00'))
        
        # 驗證設定
        self.validate_settings()
    
    def validate_settings(self):
        """驗證設定"""
        self.logger.info(f"Process daemon initialized with {self.max_workers} workers")
        self.logger.info(f"Check interval: {self.check_interval} seconds")
        self.logger.info(f"Max retries: {self.max_retries}, retry every {self.retry_interval_cycles} cycles, stale after {self.stale_hours}h")
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
        try:
            # 只取 status='pending' 且 is_processing=FALSE 的任務；
            # is_processing=TRUE 表示已有 worker 正在處理（或上次崩潰留下的孤兒旗標），
            # 必須在 run() 啟動時先清理孤兒旗標，否則這些任務永遠不會被取出
            query = '''
            SELECT id, input_path, output_path, source_resolution
            FROM conversion_tasks 
            WHERE status = 'pending' 
            AND is_processing = FALSE
            ORDER BY created_at ASC
            LIMIT 100
            '''
            return db_manager.execute_query(query, fetch=True)
        except Exception as e:
            self.logger.error(f"Error getting pending tasks: {str(e)}")
            return []
    
    def update_task_status(self, task_id, status, progress=None, error_message=None):
        """更新任務狀態"""
        try:
            updates = []
            params = []

            if status:
                updates.append('status = %s')
                params.append(status)

            if progress is not None:
                updates.append('progress = %s')
                params.append(min(100.0, max(0.0, progress)))
            
            if error_message:
                updates.append('error_message = %s')
                params.append(error_message[:1000])
            
            if status in ['completed', 'failed']:
                updates.append('end_time = CURRENT_TIMESTAMP')

            if not updates:
                # updates 為空表示呼叫者未傳入任何要更新的欄位，跳過 UPDATE 避免執行空語句
                return

            query = f"UPDATE conversion_tasks SET {', '.join(updates)} WHERE id = %s"
            params.append(task_id)
            
            db_manager.execute_query(query, tuple(params))
            
        except Exception as e:
            self.logger.error(f"Error updating task status: {str(e)}")
    
    def acquire_task_lock(self, task_id, worker_id):
        """取得任務鎖"""
        try:
            # 原子性 UPDATE：WHERE 子句同時檢查 status='pending' 和 is_processing=FALSE，
            # 資料庫的行級鎖保證同一時間只有一個 UPDATE 能成功修改同一列；
            # 若另一個 worker 同時執行相同的 UPDATE，其中一個的 rows_affected 會是 0，
            # 從而安全地排除競爭條件，無需額外的應用層鎖
            query = '''
            UPDATE conversion_tasks 
            SET is_processing = TRUE, start_time = CURRENT_TIMESTAMP
            WHERE id = %s AND status = 'pending' AND is_processing = FALSE
            '''
            rows_affected = db_manager.execute_query(query, (task_id,))
            return rows_affected > 0
        except Exception as e:
            self.logger.error(f"Error acquiring task lock: {str(e)}")
            return False
    
    def release_task_lock(self, task_id, worker_id):
        """釋放任務鎖"""
        try:
            query = "UPDATE conversion_tasks SET is_processing = FALSE WHERE id = %s"
            db_manager.execute_query(query, (task_id,))
            return True
        except Exception as e:
            self.logger.error(f"Error releasing task lock: {str(e)}")
            return False
    
    def process_task(self, task_id, worker_id):
        """處理單個任務"""
        try:
            # 取得任務詳細資訊
            query = "SELECT input_path, output_path FROM conversion_tasks WHERE id = %s"
            result = db_manager.execute_query(query, (task_id,), fetch=True)
            
            if not result:
                self.logger.warning(f"Task {task_id} not found")
                return
            
            task = result[0]
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
            success = convert_to_480p(input_path, output_path, progress_callback)
            
            # 更新最終狀態
            if success:
                self.update_task_status(task_id, 'completed', progress=100.0)
                self.processing_progress['tasks_completed'] += 1
                self.logger.info(f"Task {task_id} completed successfully: {output_path}")
            else:
                self.update_task_status(task_id, 'failed', error_message="Conversion failed")
                self.processing_progress['tasks_failed'] += 1
                self.logger.error(f"Task {task_id} failed: {input_path}")
            
        except Exception as e:
            error_msg = f"Error processing task {task_id}: {str(e)}"
            self.logger.error(error_msg)
            self.update_task_status(task_id, 'failed', error_message=error_msg)
            self.processing_progress['tasks_failed'] += 1
            self.processing_progress['errors'].append(error_msg)
        finally:
            self.release_task_lock(task_id, worker_id)
    
    def worker(self, worker_id):
        """工作執行緒"""
        self.logger.info(f"Worker {worker_id} started")
        
        while self.is_running:
            try:
                task_id = self.task_queue.get(timeout=1)
                with self.worker_locks[worker_id]:
                    self.process_task(task_id, worker_id)
                self.task_queue.task_done()
            except queue.Empty:
                continue
            except Exception as e:
                self.logger.error(f"Worker {worker_id} error: {str(e)}")
                time.sleep(1)
        
        self.logger.info(f"Worker {worker_id} stopped")
    
    def retry_failed_tasks(self):
        """將未超過重試上限的失敗任務重新排入 pending，每 retry_interval_cycles 個 check cycle 執行一次"""
        try:
            query = '''
            SELECT id, COALESCE(retry_count, 0) AS retry_count
            FROM conversion_tasks
            WHERE status = 'failed'
            AND COALESCE(retry_count, 0) < %s
            ORDER BY retry_count ASC, created_at ASC
            LIMIT 100
            '''
            failed_tasks = db_manager.execute_query(query, (self.max_retries,), fetch=True)
            if not failed_tasks:
                return 0

            retried = 0
            for task in failed_tasks:
                task_id = task['id']
                new_retry_count = task['retry_count'] + 1
                update_query = '''
                UPDATE conversion_tasks
                SET status = 'pending',
                    is_processing = FALSE,
                    retry_count = %s,
                    error_message = CONCAT('Retry #', %s, ': ', COALESCE(error_message, ''))
                WHERE id = %s
                '''
                db_manager.execute_query(update_query, (new_retry_count, new_retry_count, task_id))
                retried += 1

            if retried:
                self.logger.info(f"Retried {retried} failed task(s) (max_retries={self.max_retries})")
            return retried

        except Exception as e:
            self.logger.error(f"Error retrying failed tasks: {str(e)}")
            return 0

    def cleanup_stale_tasks(self):
        """將卡在 processing 超過 stale_hours 的任務標記為 failed，每次 check cycle 都執行"""
        try:
            stale_time = datetime.now() - timedelta(hours=self.stale_hours)
            query = '''
            SELECT id
            FROM conversion_tasks
            WHERE status = 'processing'
            AND is_processing = TRUE
            AND (start_time IS NULL OR start_time < %s)
            '''
            stale_tasks = db_manager.execute_query(
                query, (stale_time.strftime('%Y-%m-%d %H:%M:%S'),), fetch=True
            )
            if not stale_tasks:
                return 0

            cleaned = 0
            for task in stale_tasks:
                task_id = task['id']
                error_msg = f"Task marked as stale after {self.stale_hours}h (was processing)"
                update_query = '''
                UPDATE conversion_tasks
                SET status = 'failed',
                    is_processing = FALSE,
                    error_message = %s,
                    end_time = CURRENT_TIMESTAMP
                WHERE id = %s
                '''
                db_manager.execute_query(update_query, (error_msg, task_id))
                # 清除殘留的 processing_lock
                db_manager.execute_query("DELETE FROM processing_lock WHERE task_id = %s", (task_id,))
                cleaned += 1

            if cleaned:
                self.logger.warning(f"Cleaned up {cleaned} stale task(s) (>{self.stale_hours}h in processing)")
            return cleaned

        except Exception as e:
            self.logger.error(f"Error cleaning up stale tasks: {str(e)}")
            return 0

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
            query = "UPDATE conversion_tasks SET is_processing = FALSE WHERE status = 'pending' AND is_processing = TRUE"
            cleaned = db_manager.execute_query(query)
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
            'uptime': time.time() - os.stat(f"/proc/{os.getpid()}").st_ctime if os.path.exists(f"/proc/{os.getpid()}") else 0
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
