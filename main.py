import os
import time
import threading
import queue
import argparse
from pathlib import Path
from datetime import datetime, time as dt_time, timedelta
from converter import convert_to_480p, get_video_info
from db_manager import db_manager
from dotenv import load_dotenv

load_dotenv()

class VideoConverterService:
    """影片轉檔服務"""
    
    def __init__(self, args=None):
        """
        從 .env 檔案初始化設定
        
        Args:
            args: 命令列參數物件
        """
        self.args = args or argparse.Namespace(
            scan_only=False,
            process_only=False,
            no_interactive=False,
            force=False,
            verbose=False,
            process_pending=True  # 預設處理 pending 任務
        )
        
        # 從 .env 讀取路徑
        self.base_input_dir = Path(os.getenv('INPUT_DIRECTORY', '')).resolve()
        self.base_output_dir = Path(os.getenv('OUTPUT_DIRECTORY', '')).resolve()
        
        # 時間限制設定
        self.enable_time_restriction = os.getenv('ENABLE_TIME_RESTRICTION', 'true').lower() == 'true'
        self.allowed_start_time = self.parse_time(os.getenv('ALLOWED_START_TIME', '22:00'))
        self.allowed_end_time = self.parse_time(os.getenv('ALLOWED_END_TIME', '06:00'))
        self.check_interval = int(os.getenv('CHECK_INTERVAL', '300'))  # 預設5分鐘
        
        # 忽略目錄設定
        ignore_dirs_str = os.getenv('IGNORE_DIRECTORIES', '')
        self.ignore_directories = self.parse_ignore_directories(ignore_dirs_str)
        
        # 驗證設定（除非強制執行）
        if not self.args.force:
            self.validate_settings()
        elif self.args.verbose:
            print("⚠️  Running in force mode - skipping configuration validation")
        
        # 獲取其他設定
        self.max_workers = int(os.getenv('MAX_WORKERS', '2'))
        self.supported_extensions = set(ext.strip().lower() for ext in os.getenv('SUPPORTED_EXTENSIONS', '.mp4,.mkv,.avi,.mov,.flv,.wmv,.m4v,.webm').split(','))
        self.min_resolution = int(os.getenv('MIN_RESOLUTION', '481'))
        self.ignore_output_dir = os.getenv('IGNORE_OUTPUT_DIR', 'true').lower() == 'true'
        
        # 確保基礎輸出目錄存在
        self.base_output_dir.mkdir(parents=True, exist_ok=True)
        
        self.task_queue = queue.Queue()
        self.worker_threads = []
        self.is_running = False
        self.worker_locks = {}
        self.paused_workers = {}  # 追蹤暫停的工作執行緒
        
        # 檢查資料庫連接
        if not db_manager.health_check():
            raise Exception("Database connection failed")
    
    def process_pending_tasks(self):
        """
        處理資料庫中狀態為 pending 的任務
        
        Returns:
            int: 加入佇列的 pending 任務數量
        """
        if not self.args.process_pending:
            if self.args.verbose:
                print("⏭️  Skipping processing of pending tasks (disabled by argument)")
            return 0
        
        try:
            # 查詢所有 pending 且未被處理的任務
            query = '''
            SELECT id, input_path, output_path, source_resolution
            FROM conversion_tasks 
            WHERE status = 'pending' 
            AND is_processing = FALSE
            ORDER BY created_at ASC
            '''
            
            pending_tasks = db_manager.execute_query(query, fetch=True)
            
            if not pending_tasks:
                if self.args.verbose:
                    print("📭 No pending tasks found in database")
                return 0
            
            print(f"\n🔄 Found {len(pending_tasks)} pending tasks in database")
            
            processed_count = 0
            skipped_count = 0
            
            for task in pending_tasks:
                task_id = task['id']
                input_path = task['input_path']
                output_path = task['output_path']
                
                try:
                    # 檢查輸入檔案是否存在
                    if not os.path.exists(input_path):
                        error_msg = f"Input file not found: {input_path}"
                        self.update_task_status(task_id, 'failed', error_message=error_msg)
                        if self.args.verbose:
                            print(f"  ❌ Task {task_id}: {error_msg}")
                        skipped_count += 1
                        continue
                    
                    # 檢查輸出目錄是否可寫
                    output_dir = Path(output_path).parent
                    if not output_dir.exists():
                        output_dir.mkdir(parents=True, exist_ok=True)
                    
                    if not os.access(output_dir, os.W_OK):
                        error_msg = f"Output directory not writable: {output_dir}"
                        self.update_task_status(task_id, 'failed', error_message=error_msg)
                        if self.args.verbose:
                            print(f"  ❌ Task {task_id}: {error_msg}")
                        skipped_count += 1
                        continue
                    
                    # 重新檢查檔案是否應該被跳過
                    skip, reason = self.should_skip_file(input_path)
                    if skip:
                        error_msg = f"File should be skipped: {reason}"
                        self.update_task_status(task_id, 'failed', error_message=error_msg)
                        if self.args.verbose:
                            print(f"  ❌ Task {task_id}: {error_msg}")
                        skipped_count += 1
                        continue
                    
                    # 檢查解析度
                    if self.args.skip_low_resolution:
                        video_info = get_video_info(input_path)
                        if video_info:
                            try:
                                width, height = map(int, video_info['resolution'].split('x'))
                                if height < self.min_resolution:
                                    error_msg = f"File has low resolution: {video_info['resolution']}, required: {self.min_resolution}p"
                                    self.update_task_status(task_id, 'failed', error_message=error_msg)
                                    if self.args.verbose:
                                        print(f"  ❌ Task {task_id}: {error_msg}")
                                    skipped_count += 1
                                    continue
                            except (ValueError, AttributeError):
                                if self.args.verbose:
                                    print(f"  ⚠️  Task {task_id}: Could not parse resolution for {input_path}")
                    
                    # 將任務加入佇列
                    self.task_queue.put(task_id)
                    processed_count += 1
                    
                    if self.args.verbose:
                        print(f"  ✅ Task {task_id} added to queue: {input_path} -> {output_path}")
                
                except Exception as e:
                    error_msg = f"Error processing pending task {task_id}: {str(e)}"
                    self.update_task_status(task_id, 'failed', error_message=error_msg)
                    if self.args.verbose:
                        print(f"  ❌ Task {task_id}: {error_msg}")
                    skipped_count += 1
            
            print(f"✅ Processed {processed_count} pending tasks")
            if skipped_count > 0:
                print(f"⏭️  Skipped {skipped_count} pending tasks due to errors")
            
            return processed_count
            
        except Exception as e:
            print(f"❌ Error processing pending tasks: {e}")
            if self.args.verbose:
                import traceback
                traceback.print_exc()
            return 0
   
    def parse_ignore_directories(self, ignore_dirs_str):
        """解析忽略目錄字串為 Path 物件列表"""
        if not ignore_dirs_str.strip():
            return []
        
        ignore_dirs = []
        dir_paths = [d.strip() for d in ignore_dirs_str.split(',') if d.strip()]
        
        for dir_path in dir_paths:
            try:
                # 處理相對路徑和絕對路徑
                if dir_path.startswith('/') or ':' in dir_path or dir_path.startswith('\\'):
                    # 絕對路徑
                    resolved_path = Path(dir_path).resolve()
                else:
                    # 相對路徑 - 相對於 base_input_dir
                    resolved_path = (self.base_input_dir / dir_path).resolve()
                
                # 檢查路徑是否存在
                if resolved_path.exists():
                    if resolved_path.is_dir():
                        ignore_dirs.append(resolved_path)
                        if self.args.verbose:
                            print(f"✅ Ignored directory added: {resolved_path}")
                    else:
                        if self.args.verbose:
                            print(f"⚠️  Warning: '{dir_path}' exists but is not a directory")
                else:
                    # 路徑不存在，但仍加入忽略清單（可能在執行時建立）
                    ignore_dirs.append(resolved_path)
                    if self.args.verbose:
                        print(f"ℹ️  Ignored directory (may not exist yet): {resolved_path}")
                    
            except Exception as e:
                print(f"❌ Error processing ignored directory '{dir_path}': {e}")
        
        return ignore_dirs
    
    def parse_time(self, time_str):
        """解析時間字串為 time 物件"""
        try:
            if ':' in time_str:
                hours, minutes = map(int, time_str.split(':'))
                return dt_time(hour=hours, minute=minutes)
            else:
                # 如果只有小時
                hours = int(time_str)
                return dt_time(hour=hours, minute=0)
        except Exception as e:
            print(f"Error parsing time '{time_str}': {e}")
            # 回退到預設值
            if 'start' in str(time_str).lower():
                return dt_time(hour=22, minute=0)  # 晚上10點
            else:
                return dt_time(hour=6, minute=0)   # 早上6點
    
    def validate_settings(self):
        """驗證 .env 中的設定"""
        errors = []
        
        # 檢查 input directory
        if not self.base_input_dir or str(self.base_input_dir) == '.':
            errors.append("INPUT_DIRECTORY is not set in .env file")
        elif not self.base_input_dir.exists():
            errors.append(f"Input directory not found: {self.base_input_dir}")
        elif not self.base_input_dir.is_dir():
            errors.append(f"Input path is not a directory: {self.base_input_dir}")
        
        # 檢查 output directory
        if not self.base_output_dir or str(self.base_output_dir) == '.':
            errors.append("OUTPUT_DIRECTORY is not set in .env file")
        
        # 檢查必要的環境變數
        required_env_vars = ['DB_HOST', 'DB_PORT', 'DB_USER', 'DB_PASSWORD', 'DB_NAME']
        for var in required_env_vars:
            if not os.getenv(var):
                errors.append(f"{var} is not set in .env file")
        
        if errors:
            print("\n❌ Configuration Errors:")
            for error in errors:
                print(f"  - {error}")
            print("\nPlease fix the errors in your .env file and try again.")
            raise ValueError("Invalid configuration settings")
        
        # 路徑關係檢查
        if self.is_subdirectory(self.base_output_dir, self.base_input_dir):
            print(f"\n⚠️  Warning: Output directory is inside input directory:")
            print(f"   Input:  {self.base_input_dir}")
            print(f"   Output: {self.base_output_dir}")
            print(f"   The program will ignore files in the output directory during scanning.")
        else:
            if self.args.verbose:
                print(f"\n✅ Output directory is outside input directory structure")
        
        # 顯示忽略目錄設定
        if self.args.verbose:
            print(f"\n📁 Ignored directories configuration:")
            if self.ignore_directories:
                for i, ignore_dir in enumerate(self.ignore_directories, 1):
                    exists = ignore_dir.exists()
                    status = "✅ EXISTS" if exists else "⚠️  NOT FOUND"
                    rel_path = ignore_dir.relative_to(self.base_input_dir) if self.is_subdirectory(ignore_dir, self.base_input_dir) else ignore_dir
                    print(f"   {i}. {rel_path} - {status}")
            else:
                print("   No directories configured to ignore")
        
        # 時間設定檢查
        if self.enable_time_restriction:
            if self.args.verbose:
                print(f"\n⏰ Time restriction enabled:")
                print(f"   Allowed period: {self.allowed_start_time.strftime('%H:%M')} - {self.allowed_end_time.strftime('%H:%M')}")
                print(f"   Check interval: {self.check_interval} seconds")
            
            # 驗證時間設定
            if self.allowed_start_time == self.allowed_end_time:
                print("   ⚠️  Warning: Start time equals end time. This may not be what you want.")
            
            # 顯示目前時間和狀態
            current_time = datetime.now().time()
            is_allowed = self.is_time_allowed()
            status = "✅ ALLOWED" if is_allowed else "❌ NOT ALLOWED"
            if self.args.verbose:
                print(f"   Current time: {current_time.strftime('%H:%M:%S')} - {status}")
        else:
            if self.args.verbose:
                print("\n⏰ Time restriction disabled")
        
        if self.args.verbose:
            print("✅ Configuration validation passed")
    
    def is_time_allowed(self):
        """檢查目前時間是否允許轉檔"""
        if not self.enable_time_restriction:
            return True
        
        current_time = datetime.now().time()
        
        # 處理跨日情況 (例如 22:00 - 06:00)
        if self.allowed_start_time > self.allowed_end_time:
            # 跨日情況：允許時間為 start_time 到午夜，以及凌晨到 end_time
            if current_time >= self.allowed_start_time or current_time <= self.allowed_end_time:
                return True
            return False
        else:
            # 同日情況：允許時間為 start_time 到 end_time
            if self.allowed_start_time <= current_time <= self.allowed_end_time:
                return True
            return False
    
    def get_time_until_allowed(self):
        """計算距離下一個允許時間還有多久（秒）"""
        if not self.enable_time_restriction:
            return 0
        
        current_datetime = datetime.now()
        current_time = current_datetime.time()
        
        if self.allowed_start_time > self.allowed_end_time:
            # 跨日情況
            if current_time <= self.allowed_end_time:
                # 現在時間在凌晨，距離 start_time 還有時間
                target_time = current_datetime.replace(
                    hour=self.allowed_start_time.hour,
                    minute=self.allowed_start_time.minute,
                    second=0,
                    microsecond=0
                )
                if target_time < current_datetime:
                    target_time += timedelta(days=1)
            elif current_time >= self.allowed_start_time:
                # 現在時間在晚上，距離 end_time 還有時間
                target_time = current_datetime.replace(
                    hour=self.allowed_end_time.hour,
                    minute=self.allowed_end_time.minute,
                    second=0,
                    microsecond=0
                )
                if target_time < current_datetime:
                    target_time += timedelta(days=1)
            else:
                # 現在時間在白天，距離 start_time 還有時間
                target_time = current_datetime.replace(
                    hour=self.allowed_start_time.hour,
                    minute=self.allowed_start_time.minute,
                    second=0,
                    microsecond=0
                )
                if target_time < current_datetime:
                    target_time += timedelta(days=1)
        else:
            # 同日情況
            if current_time < self.allowed_start_time:
                # 距離 start_time 還有時間
                target_time = current_datetime.replace(
                    hour=self.allowed_start_time.hour,
                    minute=self.allowed_start_time.minute,
                    second=0,
                    microsecond=0
                )
                if target_time < current_datetime:
                    target_time += timedelta(days=1)
            elif current_time > self.allowed_end_time:
                # 距離下一個 start_time 還有時間
                target_time = current_datetime.replace(
                    hour=self.allowed_start_time.hour,
                    minute=self.allowed_start_time.minute,
                    second=0,
                    microsecond=0
                )
                if target_time < current_datetime:
                    target_time += timedelta(days=1)
            else:
                # 現在時間在允許範圍內
                return 0
        
        time_diff = (target_time - current_datetime).total_seconds()
        return max(0, time_diff)
    
    def is_subdirectory(self, child_path, parent_path):
        """
        檢查 child_path 是否是 parent_path 的子目錄
        
        Args:
            child_path: 可能的子目錄路徑
            parent_path: 父目錄路徑
            
        Returns:
            bool: 如果 child_path 是 parent_path 的子目錄，回傳 True
        """
        try:
            # 使用 resolve() 獲取絕對路徑
            child = Path(child_path).resolve()
            parent = Path(parent_path).resolve()
            
            # 檢查相對路徑
            child.relative_to(parent)
            return True
        except ValueError:
            # ValueError 表示 child 不是 parent 的子目錄
            return False
        except Exception as e:
            print(f"Error checking subdirectory: {e}")
            return False
    
    def should_ignore_path(self, path):
        """
        檢查路徑是否應該被忽略
        
        Args:
            path: 要檢查的路徑
            
        Returns:
            bool, str: (是否忽略, 忽略原因)
        """
        path = Path(path).resolve()
        
        # 檢查是否在忽略目錄中
        for ignore_dir in self.ignore_directories:
            if self.is_subdirectory(path, ignore_dir) or path == ignore_dir:
                relative_path = path.relative_to(ignore_dir) if self.is_subdirectory(path, ignore_dir) else path.name
                return True, f"Path is in ignored directory: {ignore_dir.name}/{relative_path}"
        
        # 檢查是否在輸出目錄中且設定要忽略
        if self.ignore_output_dir and self.is_subdirectory(path, self.base_output_dir):
            return True, f"Path is in output directory (ignored): {path.relative_to(self.base_output_dir)}"
        
        # 檢查是否在輸入目錄外
        if not self.is_subdirectory(path, self.base_input_dir):
            return True, f"Path is outside input directory structure: {path}"
        
        return False, ""
    
    def should_skip_file(self, file_path):
        """
        檢查檔案是否應該被跳過
        
        Args:
            file_path: 檔案路徑
            
        Returns:
            bool, str: (是否跳過, 跳過原因)
        """
        file_path = Path(file_path).resolve()
        
        # 先檢查是否應該忽略整個路徑
        ignore, reason = self.should_ignore_path(file_path)
        if ignore:
            return True, reason
        
        # 檢查是否是轉檔後的檔案（以 480p_ 開頭）
        if file_path.name.startswith('480p_'):
            return True, "File is already converted (starts with '480p_')"
        
        return False, ""
    
    def get_output_path(self, input_path):
        """
        根據輸入路徑生成對應的輸出路徑，維持相同的目錄結構
        
        Args:
            input_path: 輸入檔案的完整路徑
            
        Returns:
            Path: 輸出檔案的完整路徑
        """
        input_path = Path(input_path).resolve()
        
        # 檢查檔案是否應該被處理
        skip, reason = self.should_skip_file(input_path)
        if skip:
            raise ValueError(f"File should be skipped: {reason}")
        
        # 計算相對於基礎輸入目錄的相對路徑
        try:
            relative_path = input_path.relative_to(self.base_input_dir)
        except ValueError:
            raise ValueError(f"File {input_path} is not under base input directory {self.base_input_dir}")
        
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
            
            # 檢查檔案是否應該被跳過
            skip, reason = self.should_skip_file(input_path)
            if skip:
                if self.args.verbose:
                    print(f"Skipping file: {input_path}")
                    print(f"  Reason: {reason}")
                return False
            
            # 檢查是否已存在相同任務
            query = "SELECT id FROM conversion_tasks WHERE input_path = %s LIMIT 1"
            result = db_manager.execute_query(query, (str(input_path),), fetch=True)
            
            if result:
                if self.args.verbose:
                    print(f"Task already exists for {input_path}")
                return False
            
            # 檢查檔案是否存在
            if not input_path.exists():
                if self.args.verbose:
                    print(f"Input file not found: {input_path}")
                return False
            
            # 獲取影片資訊
            video_info = get_video_info(str(input_path))
            if not video_info:
                if self.args.verbose:
                    print(f"Could not get video info for: {input_path}")
                return False
                
            source_resolution = video_info['resolution']
            
            # 檢查解析度是否需要轉檔
            try:
                width, height = map(int, source_resolution.split('x'))
                if height < self.min_resolution:
                    if self.args.skip_low_resolution:
                        if self.args.verbose:
                            print(f"Skipping file (resolution too low): {input_path}")
                            print(f"  Current resolution: {source_resolution}, Minimum required: {self.min_resolution}p")
                        return False
                    else:
                        if self.args.verbose:
                            print(f"⚠️  File has low resolution but --skip-low-resolution not set: {input_path}")
            except (ValueError, AttributeError):
                if self.args.verbose:
                    print(f"Could not parse resolution for: {input_path}")
                return False
            
            # 生成輸出路徑（維持目錄結構）
            output_path = self.get_output_path(input_path)
            
            # 檢查輸出目錄是否可寫
            output_dir = output_path.parent
            if not output_dir.exists():
                output_dir.mkdir(parents=True, exist_ok=True)
            
            # 檢查是否已經存在輸出檔案
            if output_path.exists():
                # 檢查是否要覆蓋
                file_size = output_path.stat().st_size
                if file_size > 0:
                    if not self.args.force_overwrite:
                        if self.args.verbose:
                            print(f"Output file already exists: {output_path}")
                            print(f"  File size: {file_size / 1024 / 1024:.2f} MB")
                            print(f"  Use --force-overwrite to overwrite existing files")
                        return False
            
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
                if self.args.verbose:
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
            
            # 重新檢查檔案是否應該被處理
            skip, reason = self.should_skip_file(input_path)
            if skip:
                self.update_task_status(task_id, 'failed', error_message=f"File should be skipped: {reason}")
                print(f"Task {task_id} skipped: {reason}")
                return False
            
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
                    elif file_size < 1024:  # 小於1KB可能是失敗
                        success = False
                        error_msg = f"Output file too small: {file_size} bytes"
                elif success:
                    success = False
                    error_msg = "Output file not created"
                
                # 更新最終狀態
                if success:
                    self.update_task_status(task_id, 'completed', progress=100.0)
                    if self.args.verbose:
                        print(f"✓ Task {task_id} completed successfully")
                        print(f"  Output: {output_path}")
                        print(f"  File size: {os.path.getsize(output_path) / 1024 / 1024:.2f} MB")
                else:
                    error_msg = error_msg if 'error_msg' in locals() else "Conversion failed"
                    self.update_task_status(task_id, 'failed', error_message=error_msg)
                    if self.args.verbose:
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
        self.paused_workers[worker_id] = False
        
        while self.is_running:
            try:
                # 檢查時間是否允許
                if not self.is_time_allowed():
                    if not self.paused_workers[worker_id]:
                        print(f"\nWorker {worker_id} paused - outside allowed time window")
                        print(f"Current time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
                        print(f"Allowed period: {self.allowed_start_time.strftime('%H:%M')} - {self.allowed_end_time.strftime('%H:%M')}")
                        self.paused_workers[worker_id] = True
                    
                    # 等待直到允許時間或檢查間隔
                    sleep_time = min(self.check_interval, self.get_time_until_allowed())
                    time.sleep(sleep_time)
                    continue
                
                # 如果之前被暫停，現在恢復
                if self.paused_workers[worker_id]:
                    print(f"\nWorker {worker_id} resumed - now within allowed time window")
                    print(f"Current time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
                    self.paused_workers[worker_id] = False
                
                # 獲取任務
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
        
        # 處理 pending 任務
        if self.args.process_pending:
            self.process_pending_tasks()
        
        # 建立工作執行緒
        for i in range(self.max_workers):
            worker_id = f"worker_{i}"
            thread = threading.Thread(target=self.worker, args=(worker_id,))
            thread.daemon = True
            thread.start()
            self.worker_threads.append(thread)
        
        if self.args.verbose:
            print(f"\nVideo converter service started with {self.max_workers} workers")
            print(f"Base input directory: {self.base_input_dir}")
            print(f"Base output directory: {self.base_output_dir}")
            
            if self.enable_time_restriction:
                print(f"⏰ Time restriction active: {self.allowed_start_time.strftime('%H:%M')} - {self.allowed_end_time.strftime('%H:%M')}")
            else:
                print("⏰ Time restriction disabled")
            
            print(f"Supported extensions: {', '.join(self.supported_extensions)}")
            print(f"Minimum resolution for conversion: {self.min_resolution}p")
            print(f"Processing pending tasks: {'✅ Enabled' if self.args.process_pending else '❌ Disabled'}")
    
    def stop(self):
        """停止轉檔服務"""
        if self.args.verbose:
            print("\nStopping video converter service...")
        self.is_running = False
        
        # 等待所有工作執行緒完成
        for thread in self.worker_threads:
            thread.join(timeout=5.0)
        
        # 清理鎖
        query = "DELETE FROM processing_lock"
        db_manager.execute_query(query)
        
        if self.args.verbose:
            print("Video converter service stopped")

    def retry_failed_tasks(self, max_retries=3):
        """
        重試失敗的任務
        
        Args:
            max_retries: 最大重試次數
            
        Returns:
            int: 重試的任務數量
        """
        try:
            # 查詢失敗的任務，按失敗次數排序
            query = '''
            SELECT id, input_path, output_path, error_message,
                   COALESCE(retry_count, 0) as retry_count
            FROM conversion_tasks 
            WHERE status = 'failed'
            AND COALESCE(retry_count, 0) < %s
            ORDER BY retry_count ASC, created_at ASC
            LIMIT 100
            '''
            
            failed_tasks = db_manager.execute_query(query, (max_retries,), fetch=True)
            
            if not failed_tasks:
                if self.args.verbose:
                    print("📭 No failed tasks available for retry")
                return 0
            
            print(f"\n🔄 Found {len(failed_tasks)} failed tasks available for retry")
            
            retried_count = 0
            
            for task in failed_tasks:
                task_id = task['id']
                retry_count = task['retry_count'] + 1
                
                try:
                    # 更新重試次數和狀態
                    query = '''
                    UPDATE conversion_tasks 
                    SET status = 'pending', 
                        is_processing = FALSE,
                        retry_count = %s,
                        error_message = CONCAT('Retry #', %s, ': ', error_message)
                    WHERE id = %s
                    '''
                    db_manager.execute_query(query, (retry_count, retry_count, task_id))
                    
                    # 將任務加入佇列
                    self.task_queue.put(task_id)
                    retried_count += 1
                    
                    if self.args.verbose:
                        print(f"  🔄 Task {task_id} marked for retry (attempt #{retry_count})")
                
                except Exception as e:
                    print(f"  ❌ Error retrying task {task_id}: {e}")
            
            if retried_count > 0:
                print(f"✅ {retried_count} failed tasks marked for retry")
            
            return retried_count
            
        except Exception as e:
            print(f"❌ Error retrying failed tasks: {e}")
            if self.args.verbose:
                import traceback
                traceback.print_exc()
            return 0
    
    def cleanup_stale_tasks(self, hours=24):
        """
        清理過時的任務（長時間未完成的任務）
        
        Args:
            hours: 視為過時的小時數
            
        Returns:
            int: 清理的任務數量
        """
        try:
            # 計算過時時間
            stale_time = datetime.now() - timedelta(hours=hours)
            
            # 查詢過時的處理中任務
            query = '''
            SELECT id, input_path, status
            FROM conversion_tasks 
            WHERE status = 'processing'
            AND (start_time IS NULL OR start_time < %s)
            AND is_processing = TRUE
            '''
            
            stale_tasks = db_manager.execute_query(query, (stale_time.strftime('%Y-%m-%d %H:%M:%S'),), fetch=True)
            
            if not stale_tasks:
                if self.args.verbose:
                    print(f"📭 No stale tasks found (older than {hours} hours)")
                return 0
            
            print(f"\n🧹 Found {len(stale_tasks)} stale tasks (older than {hours} hours)")
            
            cleaned_count = 0
            
            for task in stale_tasks:
                task_id = task['id']
                task_status = task['status']
                
                try:
                    # 更新為失敗狀態
                    error_msg = f"Task marked as stale after {hours} hours (was {task_status})"
                    query = '''
                    UPDATE conversion_tasks 
                    SET status = 'failed',
                        is_processing = FALSE,
                        error_message = %s,
                        end_time = CURRENT_TIMESTAMP
                    WHERE id = %s
                    '''
                    db_manager.execute_query(query, (error_msg, task_id))
                    
                    # 清理鎖
                    query = "DELETE FROM processing_lock WHERE task_id = %s"
                    db_manager.execute_query(query, (task_id,))
                    
                    cleaned_count += 1
                    
                    if self.args.verbose:
                        print(f"  🧹 Task {task_id} cleaned up: {error_msg}")
                
                except Exception as e:
                    print(f"  ❌ Error cleaning up task {task_id}: {e}")
            
            if cleaned_count > 0:
                print(f"✅ {cleaned_count} stale tasks cleaned up")
            
            return cleaned_count
            
        except Exception as e:
            print(f"❌ Error cleaning up stale tasks: {e}")
            if self.args.verbose:
                import traceback
                traceback.print_exc()
            return 0
   
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
    
    def scan_directory(self, input_dir=None):
        """
        掃描目錄並添加所有影片檔案到任務佇列
        
        Args:
            input_dir: 要掃描的輸入目錄，如果為 None 則使用 base_input_dir
        """
        if input_dir is None:
            input_dir = self.base_input_dir
        
        input_dir = Path(input_dir).resolve()
        
        if not input_dir.exists():
            if self.args.verbose:
                print(f"Directory not found: {input_dir}")
            return 0
        
        if self.args.verbose:
            print(f"\nScanning directory: {input_dir}")
            print(f"Base input directory: {self.base_input_dir}")
            print(f"Base output directory: {self.base_output_dir}")
            print(f"Ignore output directory: {self.ignore_output_dir}")
            print(f"Skipping files in output directory and already converted files...")
        
        added_count = 0
        skipped_count = 0
        ignored_count = 0
        
        for root, dirs, files in os.walk(str(input_dir)):
            current_dir = Path(root)
            
            # 檢查目前目錄是否應該被忽略
            ignore_dir, reason = self.should_ignore_path(current_dir)
            if ignore_dir:
                if self.args.verbose:
                    print(f"Ignoring directory: {current_dir}")
                    print(f"  Reason: {reason}")
                ignored_count += len(files)
                continue
            
            for filename in files:
                file_path = current_dir / filename
                file_ext = file_path.suffix.lower()
                
                # 檢查副檔名
                if file_ext not in self.supported_extensions:
                    continue
                
                # 檢查檔案是否應該被跳過
                skip, reason = self.should_skip_file(file_path)
                if skip:
                    if self.args.verbose:
                        print(f"  Skipping: {file_path.relative_to(self.base_input_dir)}")
                        print(f"    Reason: {reason}")
                    skipped_count += 1
                    continue
                
                # 檢查是否已經存在相同路徑的任務
                query = "SELECT id FROM conversion_tasks WHERE input_path = %s LIMIT 1"
                result = db_manager.execute_query(query, (str(file_path),), fetch=True)
                
                if result:
                    if self.args.verbose:
                        print(f"  Skipping (already in database): {file_path.relative_to(self.base_input_dir)}")
                    skipped_count += 1
                    continue
                
                # 檢查解析度是否需要轉檔
                video_info = get_video_info(str(file_path))
                if video_info:
                    try:
                        width, height = map(int, video_info['resolution'].split('x'))
                        if height < self.min_resolution:
                            if self.args.skip_low_resolution:
                                if self.args.verbose:
                                    print(f"  Skipping (resolution too low): {file_path.relative_to(self.base_input_dir)}")
                                    print(f"    Resolution: {video_info['resolution']}, Required: {self.min_resolution}p")
                                skipped_count += 1
                                continue
                    except (ValueError, AttributeError):
                        if self.args.verbose:
                            print(f"  Could not parse resolution for: {file_path.relative_to(self.base_input_dir)}")
                        skipped_count += 1
                        continue
                
                if self.add_task(file_path):
                    added_count += 1
        
        if self.args.verbose:
            print(f"\nScan completed:")
            print(f"Added {added_count} new tasks")
            print(f"Skipped {skipped_count} files (already converted, in output directory, or low resolution)")
            print(f"Ignored {ignored_count} files (in ignored directories)")
            print(f"Total processed: {added_count + skipped_count + ignored_count}")
        return added_count
    
    def show_directory_structure(self, directory=None, max_depth=3):
        """
        顯示目錄結構預覽（僅在互動模式下執行）
        
        Args:
            directory: 要顯示的目錄，如果為 None 則使用 base_input_dir
            max_depth: 最大深度
        """
        if self.args.no_interactive:
            return
        
        if directory is None:
            directory = self.base_input_dir
        
        directory = Path(directory).resolve()
        print(f"\nDirectory structure preview for: {directory}")
        print("=" * 60)
        
        def _show_structure(current_dir, depth=0):
            if depth > max_depth:
                return
            
            indent = "  " * depth
            
            # 檢查是否應該忽略此目錄
            ignore_dir, reason = self.should_ignore_path(current_dir)
            if ignore_dir:
                # 檢查是否是特別設定的忽略目錄
                is_special_ignore = False
                ignore_reason = reason
                
                for ignore_dir_path in self.ignore_directories:
                    if current_dir == ignore_dir_path or self.is_subdirectory(current_dir, ignore_dir_path):
                        is_special_ignore = True
                        ignore_reason = f"Configured ignored directory: {ignore_dir_path.name}"
                        break
                
                status = "[CONFIG IGNORED]" if is_special_ignore else "[IGNORED]"
                print(f"{indent}📁 {current_dir.name}/ {status} - {ignore_reason}")
                return
            
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
            
            file_count = 0
            for file in files[:5]:  # 只顯示前5個檔案
                file_ext = file.suffix.lower()
                skip, reason = self.should_skip_file(file)
                if skip:
                    # 檢查是否是因為忽略目錄而跳過
                    is_ignore_dir_skip = False
                    for ignore_dir_path in self.ignore_directories:
                        if self.is_subdirectory(file, ignore_dir_path):
                            is_ignore_dir_skip = True
                            break
                    
                    status = "[CONFIG IGNORED]" if is_ignore_dir_skip else f"[SKIPPED] {reason}"
                else:
                    status = "[PROCESS]" if file_ext in self.supported_extensions else "[IGNORED]"
                print(f"{'  ' * (depth + 1)}📄 {file.name} {status}")
                file_count += 1
            
            if len(files) > 5:
                print(f"{'  ' * (depth + 1)}📄 ... ({len(files) - file_count} more files)")
        
        _show_structure(directory)
        print("=" * 60)
        
        # 顯示路徑關係
        print(f"\nPath relationship:")
        print(f"Output directory in input directory: {self.is_subdirectory(self.base_output_dir, self.base_input_dir)}")
        print(f"Ignore output directory: {self.ignore_output_dir}")
        
        # 顯示忽略目錄摘要
        print(f"\n📁 Ignored directories summary:")
        print(f"Total configured ignored directories: {len(self.ignore_directories)}")
        for i, ignore_dir in enumerate(self.ignore_directories, 1):
            exists = ignore_dir.exists()
            status = "✅" if exists else "❌"
            rel_path = ignore_dir.relative_to(self.base_input_dir) if self.is_subdirectory(ignore_dir, self.base_input_dir) else ignore_dir
            print(f"   {i}. {status} {rel_path}")
        
        # 顯示時間設定
        if self.enable_time_restriction:
            print(f"\n⏰ Time restriction settings:")
            print(f"   Enabled: {self.enable_time_restriction}")
            print(f"   Allowed period: {self.allowed_start_time.strftime('%H:%M')} - {self.allowed_end_time.strftime('%H:%M')}")
            current_time = datetime.now().time()
            is_allowed = self.is_time_allowed()
            status = "✅ CURRENTLY ALLOWED" if is_allowed else "❌ CURRENTLY NOT ALLOWED"
            print(f"   Current time: {current_time.strftime('%H:%M:%S')} - {status}")

def parse_arguments():
    """解析命令列參數"""
    parser = argparse.ArgumentParser(description='Batch video converter to 480p')
    
    # 主要操作模式
    group = parser.add_mutually_exclusive_group()
    group.add_argument('--scan-only', action='store_true', 
                      help='Only scan directories and add tasks to database, do not process')
    group.add_argument('--process-only', action='store_true', 
                      help='Only process existing tasks in database, do not scan new files')
    
    # 任務處理選項
    parser.add_argument('--process-pending', action='store_true', default=True,
                      help='Process pending tasks from database (default: enabled)')
    parser.add_argument('--no-process-pending', action='store_false', dest='process_pending',
                      help='Do not process pending tasks from database')
    parser.add_argument('--retry-failed', action='store_true',
                      help='Retry failed tasks (up to 3 attempts)')
    parser.add_argument('--cleanup-stale', action='store_true',
                      help='Clean up stale tasks (older than 24 hours)')
    parser.add_argument('--stale-hours', type=int, default=24,
                      help='Hours threshold for stale tasks (default: 24)')
    
    # 執行控制
    parser.add_argument('--no-interactive', action='store_true',
                      help='Run in non-interactive mode (no confirmation prompts)')
    parser.add_argument('--force', action='store_true',
                      help='Force execution, skip configuration validation')
    parser.add_argument('--force-overwrite', action='store_true',
                      help='Overwrite existing output files')
    parser.add_argument('--skip-low-resolution', action='store_true',
                      help='Skip files that are already 480p or lower resolution')
    
    # 輸出控制
    parser.add_argument('--verbose', action='store_true',
                      help='Enable verbose output')
    parser.add_argument('--quiet', action='store_true',
                      help='Suppress all output except errors')
    
    # 其他選項
    parser.add_argument('--max-workers', type=int,
                      help='Override MAX_WORKERS setting from .env file')
    
    return parser.parse_args()

def main():
    """主程式"""
    args = parse_arguments()
    
    # 設定日誌級別
    if args.quiet:
        # 重定向標準輸出到 /dev/null
        import sys
        sys.stdout = open(os.devnull, 'w')
    
    try:
        # 初始化轉檔服務
        converter = VideoConverterService(args)
        
        # 如果指定了 --max-workers，覆蓋設定
        if args.max_workers is not None:
            converter.max_workers = args.max_workers
        
        # 清理過時任務
        if args.cleanup_stale:
            converter.cleanup_stale_tasks(args.stale_hours)
        
        # 重試失敗的任務
        if args.retry_failed:
            converter.retry_failed_tasks()
        
        # 顯示目錄結構預覽（僅在互動模式下）
        if not args.no_interactive:
            converter.show_directory_structure()
            
            # 確認使用者
            confirm = input("\nDo you want to proceed with this directory structure? (y/n): ")
            if confirm.lower() != 'y':
                print("Operation cancelled.")
                return
        
        # 掃描目錄並添加任務（除非是 --process-only）
        if not args.process_only:
            if args.verbose:
                print("\n🔍 Scanning directories for video files...")
            converter.scan_directory()
        
        # 處理任務（除非是 --scan-only）
        if not args.scan_only:
            if args.verbose:
                print("\n🚀 Starting video conversion service...")
            
            # 檢查時間限制
            if converter.enable_time_restriction and not converter.is_time_allowed():
                if not args.force:
                    current_time = datetime.now().strftime('%H:%M:%S')
                    allowed_period = f"{converter.allowed_start_time.strftime('%H:%M')} - {converter.allowed_end_time.strftime('%H:%M')}"
                    print(f"\n⏰ Not allowed to run at current time: {current_time}")
                    print(f"   Allowed period: {allowed_period}")
                    print(f"   Use --force to override time restriction")
                    return
            
            # 啟動服務
            converter.start()
            
            # 顯示目前任務狀態
            def show_status():
                while converter.is_running:
                    try:
                        # 顯示時間資訊
                        current_time = datetime.now()
                        time_str = current_time.strftime('%Y-%m-%d %H:%M:%S')
                        
                        # 顯示時間限制狀態
                        if converter.enable_time_restriction:
                            is_allowed = converter.is_time_allowed()
                            time_status = "✅ ACTIVE" if is_allowed else "⏸️  PAUSED"
                            time_info = f"[{time_str}] {time_status} | Period: {converter.allowed_start_time.strftime('%H:%M')}-{converter.allowed_end_time.strftime('%H:%M')}"
                        else:
                            time_info = f"[{time_str}] ⏰ DISABLED"
                        
                        # 顯示任務狀態
                        query = '''
                        SELECT 
                            COUNT(*) as total,
                            SUM(CASE WHEN status = 'pending' THEN 1 ELSE 0 END) as pending,
                            SUM(CASE WHEN status = 'processing' THEN 1 ELSE 0 END) as processing,
                            SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) as completed,
                            SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) as failed,
                            SUM(CASE WHEN retry_count > 0 THEN 1 ELSE 0 END) as retried
                        FROM conversion_tasks
                        '''
                        result = db_manager.execute_query(query, fetch=True)
                        
                        if result:
                            stats = result[0]
                            status_str = (f"Status - Total: {stats['total']}, "
                                        f"Pending: {stats['pending']}, "
                                        f"Processing: {stats['processing']}, "
                                        f"Completed: {stats['completed']}, "
                                        f"Failed: {stats['failed']}, "
                                        f"Retried: {stats['retried']}")
                            
                            if not args.quiet:
                                print(f"\r{time_info} | {status_str}", end='', flush=True)
                        
                        time.sleep(2)
                    except Exception as e:
                        if args.verbose:
                            print(f"Status display error: {e}")
                        time.sleep(2)
            
            # 啟動狀態顯示執行緒
            status_thread = threading.Thread(target=show_status)
            status_thread.daemon = True
            status_thread.start()
            
            # 等待所有任務完成
            if args.verbose:
                print("\n\n⏳ Waiting for all tasks to complete...")
            converter.task_queue.join()
            
            # 等待一段時間讓最後的更新完成
            time.sleep(2)
            
            if args.verbose:
                print("\n\n✅ All tasks completed!")
        
        # 顯示最終統計
        query = '''
        SELECT 
            COUNT(*) as total,
            SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) as completed,
            SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) as failed,
            SUM(CASE WHEN retry_count > 0 THEN 1 ELSE 0 END) as retried,
            AVG(TIMESTAMPDIFF(SECOND, start_time, end_time)) as avg_duration
        FROM conversion_tasks
        WHERE status IN ('completed', 'failed')
        '''
        result = db_manager.execute_query(query, fetch=True)
        
        if result and not args.quiet:
            stats = result[0]
            print(f"\n📊 Final Statistics:")
            print(f"Total tasks: {stats['total']}")
            print(f"Successfully completed: {stats['completed']}")
            print(f"Failed: {stats['failed']}")
            print(f"Retried tasks: {stats['retried'] or 0}")
            if stats['avg_duration']:
                avg_minutes = float(stats['avg_duration']) / 60
                print(f"Average processing time: {avg_minutes:.1f} minutes")
            
            if stats['failed'] > 0:
                print("\n❌ Failed tasks details:")
                query = """
                SELECT id, input_path, output_path, error_message, 
                       retry_count, created_at, updated_at
                FROM conversion_tasks 
                WHERE status = 'failed'
                ORDER BY updated_at DESC
                LIMIT 10
                """
                failed_tasks = db_manager.execute_query(query, fetch=True)
                for task in failed_tasks:
                    print(f"\nTask ID {task['id']}:")
                    print(f"  Input:  {task['input_path']}")
                    print(f"  Output: {task['output_path']}")
                    print(f"  Error:  {task['error_message']}")
                    print(f"  Retries: {task['retry_count'] or 0}")
                    print(f"  Created: {task['created_at']}")
                    print(f"  Updated: {task['updated_at']}")
        
        # 顯示成功的轉檔結果預覽
        if not args.quiet:
            print("\n✅ Successful conversions preview:")
            try:
                query = """
                SELECT id, input_path, output_path, source_resolution,
                       created_at, updated_at
                FROM conversion_tasks 
                WHERE status = 'completed' 
                ORDER BY updated_at DESC 
                LIMIT 5
                """
                successful_tasks = db_manager.execute_query(query, fetch=True)
                
                if successful_tasks:
                    for i, task in enumerate(successful_tasks, 1):
                        input_path = Path(task['input_path'])
                        output_path = Path(task['output_path'])
                        
                        try:
                            relative_input = input_path.relative_to(converter.base_input_dir)
                            relative_output = output_path.relative_to(converter.base_output_dir)
                        except ValueError:
                            relative_input = input_path
                            relative_output = output_path
                        
                        print(f"\n{i}. Task ID: {task['id']}")
                        print(f"   Input:  {relative_input}")
                        print(f"   Output: {relative_output}")
                        print(f"   Resolution: {task['source_resolution']} → 480p")
                        print(f"   Completed: {task['updated_at']}")
                        
                        if output_path.exists():
                            file_size_mb = output_path.stat().st_size / 1024 / 1024
                            print(f"   Size:   {file_size_mb:.2f} MB")
                else:
                    print("No successful conversions to display.")
                    
            except Exception as e:
                print(f"Could not retrieve successful conversions: {e}")
        
    except KeyboardInterrupt:
        if not args.quiet:
            print("\n\n🛑 Shutting down gracefully...")
    except Exception as e:
        print(f"❌ Main program error: {e}")
        if args.verbose:
            import traceback
            traceback.print_exc()
    finally:
        if 'converter' in locals():
            converter.stop()

if __name__ == '__main__':
    main()
