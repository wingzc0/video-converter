import time
import threading
from datetime import datetime
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
        
        # 驗證設定
        self.validate_settings()
    
    def validate_settings(self):
        """驗證設定"""
        self.logger.info(f"Process daemon initialized with {self.max_workers} workers")
        self.logger.info(f"Check interval: {self.check_interval} seconds")
    
    def get_pending_tasks(self):
        """獲取待處理的任務"""
        try:
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
            updates = ['status = %s']
            params = [status]
            
            if progress is not None:
                updates.append('progress = %s')
                params.append(min(100.0, max(0.0, progress)))
            
            if error_message:
                updates.append('error_message = %s')
                params.append(error_message[:1000])
            
            if status in ['completed', 'failed']:
                updates.append('end_time = CURRENT_TIMESTAMP')
            
            query = f"UPDATE conversion_tasks SET {', '.join(updates)} WHERE id = %s"
            params.append(task_id)
            
            db_manager.execute_query(query, tuple(params))
            
        except Exception as e:
            self.logger.error(f"Error updating task status: {str(e)}")
    
    def acquire_task_lock(self, task_id, worker_id):
        """取得任務鎖"""
        try:
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
    
    def check_and_process_tasks(self):
        """檢查並處理任務"""
        self.processing_progress['status'] = 'checking'
        self.processing_progress['last_check_time'] = datetime.now()
        
        try:
            pending_tasks = self.get_pending_tasks()
            
            if pending_tasks:
                self.logger.info(f"Found {len(pending_tasks)} pending tasks")
                for task in pending_tasks:
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
        
        # 建立工作執行緒
        for i in range(self.max_workers):
            worker_id = f"worker_{i}"
            self.worker_locks[worker_id] = threading.Lock()
            thread = threading.Thread(target=self.worker, args=(worker_id,))
            thread.daemon = True
            thread.start()
            self.worker_threads.append(thread)
        
        # 主循環
        while self.is_running:
            try:
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
