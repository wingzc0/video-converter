import os
import time
import threading
import queue
from pathlib import Path
from converter import convert_to_480p, get_video_info
from db_manager import db_manager
from dotenv import load_dotenv

load_dotenv()

class VideoConverterService:
    """影片轉檔服務"""
    
    def __init__(self, base_input_dir, base_output_dir):
        """
        初始化轉檔服務
        
        Args:
            base_input_dir: 基礎輸入目錄，用於計算相對路徑
            base_output_dir: 基礎輸出目錄，將在其中重建相同的目錄結構
        """
        self.base_input_dir = Path(base_input_dir).resolve()
        self.base_output_dir = Path(base_output_dir).resolve()
        
        # 確保基礎輸出目錄存在
        self.base_output_dir.mkdir(parents=True, exist_ok=True)
        
        self.task_queue = queue.Queue()
        self.max_workers = int(os.getenv('MAX_WORKERS', '2'))
        self.worker_threads = []
        self.is_running = False
        self.worker_locks = {}
        
        # 檢查資料庫連接
        if not db_manager.health_check():
            raise Exception("Database connection failed")
    
    def get_output_path(self, input_path):
        """
        根據輸入路徑生成對應的輸出路徑，維持相同的目錄結構
        
        Args:
            input_path: 輸入檔案的完整路徑
            
        Returns:
            Path: 輸出檔案的完整路徑
        """
        input_path = Path(input_path).resolve()
        
        # 計算相對於基礎輸入目錄的相對路徑
        try:
            relative_path = input_path.relative_to(self.base_input_dir)
        except ValueError:
            print(f"Warning: {input_path} is not under base input directory {self.base_input_dir}")
            # 如果檔案不在基礎目錄下，使用檔案名稱作為相對路徑
            relative_path = input_path.name
        
        # 獲取相對目錄路徑（不含檔案名）
        relative_dir = relative_path.parent
        filename = relative_path.name
        
        # 在輸出目錄中重建相同的目錄結構
        output_dir = self.base_output_dir / relative_dir
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # 生成輸出檔案名稱（添加 480p_ 前綴）
        output_filename = f"480p_{filename}"
        output_path = output_dir / output_filename
        
        return output_path
    
    def add_task(self, input_path):
        """添加轉檔任務到資料庫，維持目錄結構"""
        try:
            input_path = Path(input_path).resolve()
            
            # 檢查是否已存在相同任務
            query = "SELECT id FROM conversion_tasks WHERE input_path = %s LIMIT 1"
            result = db_manager.execute_query(query, (str(input_path),), fetch=True)
            
            if result:
                print(f"Task already exists for {input_path}")
                return False
            
            # 檢查檔案是否存在
            if not input_path.exists():
                print(f"Input file not found: {input_path}")
                return False
            
            # 獲取影片資訊
            video_info = get_video_info(str(input_path))
            source_resolution = video_info['resolution'] if video_info else 'unknown'
            
            # 生成輸出路徑（維持目錄結構）
            output_path = self.get_output_path(input_path)
            
            # 檢查輸出目錄是否可寫
            output_dir = output_path.parent
            if not output_dir.exists():
                output_dir.mkdir(parents=True, exist_ok=True)
            
            # 插入任務
            query = '''
            INSERT INTO conversion_tasks 
            (input_path, output_path, source_resolution, status)
            VALUES (%s, %s, %s, 'pending')
            '''
            
            db_manager.execute_query(query, (str(input_path), str(output_path), source_resolution))
            
            # 獲取新插入的任務ID
            query = "SELECT LAST_INSERT_ID() as task_id"
            result = db_manager.execute_query(query, fetch=True)
            task_id = result[0]['task_id'] if result else None
            
            if task_id:
                # 加入處理佇列
                self.task_queue.put(task_id)
                print(f"Task added: {input_path} -> {output_path}")
                print(f"  Directory structure preserved: {output_path.relative_to(self.base_output_dir)}")
                return True
            
            return False
            
        except Exception as e:
            print(f"Error adding task: {e}")
            return False
    
    def update_task_status(self, task_id, status, progress=None, error_message=None, is_processing=None):
        """更新任務狀態"""
        try:
            updates = []
            params = []
            
            if status:
                updates.append("status = %s")
                params.append(status)
            
            if progress is not None:
                updates.append("progress = %s")
                params.append(min(100.0, max(0.0, progress)))  # 確保進度在0-100之間
            
            if is_processing is not None:
                updates.append("is_processing = %s")
                params.append(is_processing)
            
            if error_message:
                updates.append("error_message = %s")
                params.append(error_message[:1000])  # 限制錯誤訊息長度
            
            if status in ['completed', 'failed']:
                updates.append("end_time = CURRENT_TIMESTAMP")
            
            if not updates:
                return
            
            query = f"UPDATE conversion_tasks SET {', '.join(updates)} WHERE id = %s"
            params.append(task_id)
            
            db_manager.execute_query(query, tuple(params))
            
        except Exception as e:
            print(f"Error updating task status: {e}")
    
    def acquire_task_lock(self, task_id, worker_id):
        """取得任務鎖"""
        try:
            # 檢查任務是否可以被處理
            query = '''
            SELECT id FROM conversion_tasks 
            WHERE id = %s AND status = 'pending' AND is_processing = FALSE
            FOR UPDATE
            '''
            result = db_manager.execute_query(query, (task_id,), fetch=True)
            
            if not result:
                return False
            
            # 取得鎖
            query = '''
            INSERT INTO processing_lock (task_id, worker_id)
            VALUES (%s, %s)
            ON DUPLICATE KEY UPDATE worker_id = VALUES(worker_id), locked_at = CURRENT_TIMESTAMP
            '''
            db_manager.execute_query(query, (task_id, worker_id))
            
            # 更新任務狀態
            self.update_task_status(task_id, 'processing', is_processing=True)
            return True
            
        except Exception as e:
            print(f"Error acquiring task lock: {e}")
            return False
    
    def release_task_lock(self, task_id, worker_id):
        """釋放任務鎖"""
        try:
            query = "DELETE FROM processing_lock WHERE task_id = %s AND worker_id = %s"
            db_manager.execute_query(query, (task_id, worker_id))
            return True
        except Exception as e:
            print(f"Error releasing task lock: {e}")
            return False
    
    def process_task(self, task_id, worker_id):
        """處理單個轉檔任務"""
        try:
            # 取得任務詳細資訊
            query = '''
            SELECT id, input_path, output_path, source_resolution
            FROM conversion_tasks WHERE id = %s
            '''
            result = db_manager.execute_query(query, (task_id,), fetch=True)
            
            if not result:
                print(f"Task {task_id} not found")
                return False
            
            task = result[0]
            input_path = task['input_path']
            output_path = task['output_path']
            
            # 檢查檔案是否存在
            if not os.path.exists(input_path):
                self.update_task_status(task_id, 'failed', error_message=f"Input file not found: {input_path}")
                return False
            
            # 檢查輸出目錄是否存在，不存在則建立
            output_dir = Path(output_path).parent
            output_dir.mkdir(parents=True, exist_ok=True)
            
            # 檢查輸出目錄是否可寫
            if not os.access(output_dir, os.W_OK):
                self.update_task_status(task_id, 'failed', 
                                      error_message=f"Output directory not writable: {output_dir}")
                return False
            
            # 取得任務鎖
            if not self.acquire_task_lock(task_id, worker_id):
                print(f"Could not acquire lock for task {task_id}")
                return False
            
            try:
                # 定義進度回調函數
                def progress_callback(progress):
                    self.update_task_status(task_id, None, progress=progress)
                
                # 執行轉檔
                success = convert_to_480p(input_path, output_path, progress_callback)
                
                # 檢查輸出檔案是否成功生成
                if success and os.path.exists(output_path):
                    file_size = os.path.getsize(output_path)
                    if file_size == 0:
                        success = False
                        error_msg = "Output file is empty"
                elif success:
                    success = False
                    error_msg = "Output file not created"
                
                # 更新最終狀態
                if success:
                    self.update_task_status(task_id, 'completed', progress=100.0)
                    print(f"✓ Task {task_id} completed successfully")
                    print(f"  Output: {output_path}")
                    print(f"  File size: {os.path.getsize(output_path) / 1024 / 1024:.2f} MB")
                else:
                    error_msg = error_msg if 'error_msg' in locals() else "Conversion failed"
                    self.update_task_status(task_id, 'failed', error_message=error_msg)
                    print(f"✗ Task {task_id} failed: {error_msg}")
                
                return success
                
            finally:
                # 釋放鎖
                self.release_task_lock(task_id, worker_id)
                
        except Exception as e:
            error_msg = f"Task processing error: {str(e)}"
            self.update_task_status(task_id, 'failed', error_message=error_msg)
            print(error_msg)
            return False
    
    def worker(self, worker_id):
        """工作執行緒"""
        print(f"Worker {worker_id} started")
        self.worker_locks[worker_id] = threading.Lock()
        
        while self.is_running:
            try:
                task_id = self.task_queue.get(timeout=1)
                
                with self.worker_locks[worker_id]:
                    self.process_task(task_id, worker_id)
                
                self.task_queue.task_done()
                time.sleep(0.1)  # 避免過度競爭
                
            except queue.Empty:
                continue
            except Exception as e:
                print(f"Worker {worker_id} error: {e}")
                time.sleep(1)
        
        print(f"Worker {worker_id} stopped")
    
    def start(self):
        """啟動轉檔服務"""
        self.is_running = True
        
        # 清理之前可能遺留的鎖
        query = "DELETE FROM processing_lock"
        db_manager.execute_query(query)
        
        # 建立工作執行緒
        for i in range(self.max_workers):
            worker_id = f"worker_{i}"
            thread = threading.Thread(target=self.worker, args=(worker_id,))
            thread.daemon = True
            thread.start()
            self.worker_threads.append(thread)
        
        print(f"Video converter service started with {self.max_workers} workers")
        print(f"Base input directory: {self.base_input_dir}")
        print(f"Base output directory: {self.base_output_dir}")
        print(f"Directory structure will be preserved in output")
    
    def stop(self):
        """停止轉檔服務"""
        print("Stopping video converter service...")
        self.is_running = False
        
        # 等待所有工作執行緒完成
        for thread in self.worker_threads:
            thread.join(timeout=5.0)
        
        # 清理鎖
        query = "DELETE FROM processing_lock"
        db_manager.execute_query(query)
        
        print("Video converter service stopped")
    
    def get_task_status(self, task_id):
        """獲取任務狀態"""
        try:
            query = '''
            SELECT id, input_path, output_path, source_resolution, 
                   status, progress, is_processing, start_time, end_time,
                   error_message, created_at, updated_at
            FROM conversion_tasks WHERE id = %s
            '''
            result = db_manager.execute_query(query, (task_id,), fetch=True)
            
            if not result:
                return None
            
            task = result[0]
            return {
                'id': task['id'],
                'input_path': task['input_path'],
                'output_path': task['output_path'],
                'source_resolution': task['source_resolution'],
                'status': task['status'],
                'progress': float(task['progress']) if task['progress'] else 0.0,
                'is_processing': bool(task['is_processing']),
                'start_time': task['start_time'],
                'end_time': task['end_time'],
                'error_message': task['error_message'],
                'created_at': task['created_at'],
                'updated_at': task['updated_at']
            }
            
        except Exception as e:
            print(f"Error getting task status: {e}")
            return None
    
    def scan_directory(self, input_dir):
        """
        掃描目錄並添加所有影片檔案到任務佇列
        
        Args:
            input_dir: 要掃描的輸入目錄
        """
        supported_extensions = {'.mp4', '.mkv', '.avi', '.mov', '.flv', '.wmv', '.m4v', '.webm'}
        
        input_dir = Path(input_dir).resolve()
        
        if not input_dir.exists():
            print(f"Directory not found: {input_dir}")
            return 0
        
        print(f"Scanning directory: {input_dir}")
        print(f"Preserving directory structure relative to: {self.base_input_dir}")
        
        added_count = 0
        skipped_count = 0
        
        for root, dirs, files in os.walk(str(input_dir)):
            for filename in files:
                file_ext = os.path.splitext(filename)[1].lower()
                if file_ext in supported_extensions:
                    input_path = Path(root) / filename
                    
                    # 檢查是否已經存在相同路徑的任務
                    query = "SELECT id FROM conversion_tasks WHERE input_path = %s LIMIT 1"
                    result = db_manager.execute_query(query, (str(input_path),), fetch=True)
                    
                    if result:
                        print(f"  Skipping (already exists): {input_path.relative_to(self.base_input_dir)}")
                        skipped_count += 1
                        continue
                    
                    if self.add_task(input_path):
                        added_count += 1
        
        print(f"\nScan completed:")
        print(f"Added {added_count} new tasks")
        print(f"Skipped {skipped_count} existing tasks")
        print(f"Total processed: {added_count + skipped_count}")
        return added_count
    
    def show_directory_structure(self, directory, max_depth=3):
        """
        顯示目錄結構預覽
        
        Args:
            directory: 要顯示的目錄
            max_depth: 最大深度
        """
        directory = Path(directory).resolve()
        print(f"\nDirectory structure preview for: {directory}")
        print("=" * 60)
        
        def _show_structure(current_dir, depth=0):
            if depth > max_depth:
                return
            
            indent = "  " * depth
            print(f"{indent}📁 {current_dir.name}/")
            
            # 顯示子目錄
            subdirs = [d for d in current_dir.iterdir() if d.is_dir()]
            subdirs.sort()
            
            for subdir in subdirs[:3]:  # 只顯示前3個子目錄
                _show_structure(subdir, depth + 1)
            
            if len(subdirs) > 3:
                print(f"{'  ' * (depth + 1)}📁 ... ({len(subdirs) - 3} more directories)")
            
            # 顯示檔案
            files = [f for f in current_dir.iterdir() if f.is_file()]
            files.sort()
            
            for file in files[:5]:  # 只顯示前5個檔案
                print(f"{'  ' * (depth + 1)}📄 {file.name}")
            
            if len(files) > 5:
                print(f"{'  ' * (depth + 1)}📄 ... ({len(files) - 5} more files)")
        
        _show_structure(directory)
        print("=" * 60)

def main():
    """主程式"""
    try:
        # 設定基礎目錄
        BASE_INPUT_DIR = '/path/to/input/videos'  # 修改為您的輸入目錄
        BASE_OUTPUT_DIR = '/path/to/output/videos'  # 修改為您的輸出目錄
        
        # 驗證目錄存在
        if not os.path.exists(BASE_INPUT_DIR):
            print(f"Error: Input directory not found: {BASE_INPUT_DIR}")
            print("Please create the directory or modify the path in the code.")
            return
        
        # 顯示目錄結構預覽
        converter_preview = VideoConverterService(BASE_INPUT_DIR, BASE_OUTPUT_DIR)
        converter_preview.show_directory_structure(BASE_INPUT_DIR)
        
        # 確認使用者
        confirm = input("\nDo you want to proceed with this directory structure? (y/n): ")
        if confirm.lower() != 'y':
            print("Operation cancelled.")
            return
        
        # 初始化轉檔服務
        converter = VideoConverterService(BASE_INPUT_DIR, BASE_OUTPUT_DIR)
        
        # 啟動服務
        converter.start()
        
        # 掃描目錄並添加任務
        converter.scan_directory(BASE_INPUT_DIR)
        
        # 顯示輸出目錄結構預覽
        print(f"\nOutput directory structure will be created under: {BASE_OUTPUT_DIR}")
        print("Example output path structure:")
        example_input = Path(BASE_INPUT_DIR) / "category1/subcategory/video.mp4"
        example_output = converter.get_output_path(example_input)
        print(f"Input:  {example_input.relative_to(BASE_INPUT_DIR)}")
        print(f"Output: {example_output.relative_to(BASE_OUTPUT_DIR)}")
        
        # 顯示目前任務狀態
        def show_status():
            while converter.is_running:
                try:
                    query = '''
                    SELECT COUNT(*) as total,
                           SUM(CASE WHEN status = 'pending' THEN 1 ELSE 0 END) as pending,
                           SUM(CASE WHEN status = 'processing' THEN 1 ELSE 0 END) as processing,
                           SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) as completed,
                           SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) as failed
                    FROM conversion_tasks
                    '''
                    result = db_manager.execute_query(query, fetch=True)
                    
                    if result:
                        stats = result[0]
                        print(f"\rStatus - Total: {stats['total']}, "
                              f"Pending: {stats['pending']}, "
                              f"Processing: {stats['processing']}, "
                              f"Completed: {stats['completed']}, "
                              f"Failed: {stats['failed']}", end='', flush=True)
                    
                    time.sleep(2)
                except Exception as e:
                    print(f"Status display error: {e}")
                    time.sleep(2)
        
        # 啟動狀態顯示執行緒
        status_thread = threading.Thread(target=show_status)
        status_thread.daemon = True
        status_thread.start()
        
        # 等待所有任務完成
        print("\n\nWaiting for all tasks to complete...")
        converter.task_queue.join()
        
        # 等待一段時間讓最後的更新完成
        time.sleep(2)
        
        print("\n\nAll tasks completed!")
        
        # 顯示最終統計
        query = '''
        SELECT COUNT(*) as total,
               SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) as completed,
               SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) as failed
        FROM conversion_tasks
        '''
        result = db_manager.execute_query(query, fetch=True)
        
        if result:
            stats = result[0]
            print(f"\nFinal Statistics:")
            print(f"Total tasks: {stats['total']}")
            print(f"Successfully completed: {stats['completed']}")
            print(f"Failed: {stats['failed']}")
            
            if stats['failed'] > 0:
                print("\nFailed tasks:")
                query = "SELECT id, input_path, output_path, error_message FROM conversion_tasks WHERE status = 'failed'"
                failed_tasks = db_manager.execute_query(query, fetch=True)
                for task in failed_tasks:
                    print(f"\nTask ID {task['id']}:")
                    print(f"  Input:  {task['input_path']}")
                    print(f"  Output: {task['output_path']}")
                    print(f"  Error:  {task['error_message']}")
        
        # 顯示成功的轉檔結果預覽
        print("\nSuccessful conversions preview:")
        query = """
        SELECT input_path, output_path 
        FROM conversion_tasks 
        WHERE status = 'completed' 
        ORDER BY completed_at DESC 
        LIMIT 5
        """
        try:
            successful_tasks = db_manager.execute_query("""
                SELECT input_path, output_path 
                FROM conversion_tasks 
                WHERE status = 'completed' 
                LIMIT 5
            """, fetch=True)
            
            if successful_tasks:
                for i, task in enumerate(successful_tasks, 1):
                    input_path = Path(task['input_path'])
                    output_path = Path(task['output_path'])
                    print(f"\n{i}. Input:  {input_path.relative_to(BASE_INPUT_DIR)}")
                    print(f"   Output: {output_path.relative_to(BASE_OUTPUT_DIR)}")
                    print(f"   Size:   {os.path.getsize(output_path) / 1024 / 1024:.2f} MB")
            else:
                print("No successful conversions to display.")
                
        except Exception as e:
            print(f"Could not retrieve successful conversions: {e}")
        
    except KeyboardInterrupt:
        print("\n\nShutting down gracefully...")
    except Exception as e:
        print(f"Main program error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        if 'converter' in locals():
            converter.stop()

if __name__ == '__main__':
    main()
