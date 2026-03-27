import os
import sys
import json
import time
import threading
import logging
from datetime import datetime
from pathlib import Path
from flask import Flask, jsonify, request
from flask_cors import CORS
from flask_socketio import SocketIO, emit
from dotenv import load_dotenv
from db_manager import db_manager

load_dotenv()

class APIServer:
    """
    API 伺服器，讀取 daemon 狀態檔案提供 API 服務
    """
    
    def __init__(self, host='0.0.0.0', port=5000):
        self.host = host
        self.port = port
        # setup_logger() 必須在 create_app() 之前呼叫，
        # 因為 create_app() 內部會使用 self.logger
        self.setup_logger()
        self.app = self.create_app()
        self.socketio = SocketIO(self.app, cors_allowed_origins="*", async_mode='threading')
        
        # 狀態檔案路徑
        self.scan_status_file = os.getenv('SCAN_DAEMON_STATUS_FILE', '/var/run/video-converter/scanner_status.json')
        self.process_status_file = os.getenv('PROCESS_DAEMON_STATUS_FILE', '/var/run/video-converter/processor_status.json')
        self.status_dir = os.getenv('API_SERVER_STATUS_DIR', '/var/run/video-converter')
        
        # 快取狀態
        self.scan_status = None
        self.process_status = None
        self.last_scan_update = 0
        self.last_process_update = 0
        
        # 狀態更新設定
        # 快取 TTL 1 秒：WebSocket broadcast 每 2 秒推送一次，
        # 1 秒 TTL 確保每次廣播都能讀到最新狀態，同時避免每次廣播都觸發磁碟 I/O
        self.status_cache_ttl = 1  # 狀態快取 TTL (秒)
        self.is_running = False
        self.status_thread = None
        
        self.setup_routes()
        self.setup_socketio_events()
    
    def setup_logger(self):
        """設定 logger"""
        self.logger = logging.getLogger('api-server')
        self.logger.setLevel(logging.INFO)
        
        # 移除現有的 handler
        self.logger.handlers = []
        
        # 檔案 handler
        log_file = os.getenv('API_SERVER_LOG_FILE', '/var/log/video-converter/api.log')
        log_dir = Path(log_file).parent
        log_dir.mkdir(parents=True, exist_ok=True)
        
        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        ))
        self.logger.addHandler(file_handler)
        
        # 標準輸出 handler
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        ))
        self.logger.addHandler(console_handler)
    
    def create_app(self):
        """建立 Flask 應用"""
        app = Flask(__name__)
        secret_key = os.getenv('SECRET_KEY')
        if not secret_key:
            import secrets
            secret_key = secrets.token_hex(32)
            self.logger.warning("SECRET_KEY not set in environment; using a randomly generated key. Sessions will be invalidated on restart.")
        app.config['SECRET_KEY'] = secret_key
        app.config['JSON_AS_ASCII'] = False
        CORS(app)
        return app
    
    def load_status_file(self, file_path):
        """讀取狀態檔案"""
        if not os.path.exists(file_path):
            self.logger.debug(f"Status file not found: {file_path}")
            return None
        
        try:
            # 檢查檔案修改時間
            file_mtime = os.path.getmtime(file_path)
            current_time = time.time()
            
            # 如果檔案太舊，視為無效
            # 60 秒 TTL：daemon 每 10 秒更新一次狀態檔案；
            # 若超過 60 秒未更新，表示 daemon 已崩潰或停止，應視狀態為不可用而非顯示過期資訊
            if current_time - file_mtime > 60:  # 60秒內的檔案才有效
                self.logger.warning(f"Status file is too old: {file_path} (modified {current_time - file_mtime:.1f} seconds ago)")
                return None
            
            with open(file_path, 'r') as f:
                status = json.load(f)
            
            # 驗證狀態檔案格式
            if not isinstance(status, dict):
                self.logger.error(f"Invalid status file format: {file_path}")
                return None
            
            return status
            
        except json.JSONDecodeError as e:
            self.logger.error(f"JSON decode error in {file_path}: {e}")
            return None
        except Exception as e:
            self.logger.error(f"Error loading status file {file_path}: {e}")
            return None
    
    def get_cached_scan_status(self):
        """獲取快取的掃描 daemon 狀態"""
        current_time = time.time()
        
        # 如果快取過期或沒有快取，重新讀取
        if current_time - self.last_scan_update > self.status_cache_ttl or self.scan_status is None:
            self.scan_status = self.load_status_file(self.scan_status_file)
            self.last_scan_update = current_time
        
        return self.scan_status
    
    def get_cached_process_status(self):
        """獲取快取的處理 daemon 狀態"""
        current_time = time.time()
        
        # 如果快取過期或沒有快取，重新讀取
        if current_time - self.last_process_update > self.status_cache_ttl or self.process_status is None:
            self.process_status = self.load_status_file(self.process_status_file)
            self.last_process_update = current_time
        
        return self.process_status
    
    def get_system_status(self):
        """獲取系統狀態"""
        try:
            # 嘗試匯入 psutil，如果沒有安裝則跳過
            import psutil
            cpu_percent = psutil.cpu_percent(interval=0.1)
            memory = psutil.virtual_memory()
            disk = psutil.disk_usage('/')
            
            return {
                'cpu_percent': cpu_percent,
                'memory_percent': memory.percent,
                'memory_used': memory.used,
                'memory_total': memory.total,
                'disk_percent': disk.percent,
                'disk_used': disk.used,
                'disk_total': disk.total,
                'server_time': datetime.now().isoformat(),
                'server_uptime': time.time() - self.start_time
            }
        except ImportError:
            self.logger.warning("psutil not installed, skipping system metrics")
            return {
                'cpu_percent': 'N/A',
                'memory_percent': 'N/A',
                'server_time': datetime.now().isoformat(),
                'server_uptime': time.time() - self.start_time
            }
        except Exception as e:
            self.logger.error(f"Error getting system status: {e}")
            return {'error': str(e)}
    
    def get_task_stats(self):
        """獲取任務統計（從資料庫）"""
        try:
            query = '''
            SELECT 
                COUNT(*) as total,
                SUM(CASE WHEN status = 'pending' THEN 1 ELSE 0 END) as pending,
                SUM(CASE WHEN status = 'processing' THEN 1 ELSE 0 END) as processing,
                SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) as completed,
                SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) as failed,
                SUM(CASE WHEN retry_count > 0 THEN 1 ELSE 0 END) as retried,
                AVG(TIMESTAMPDIFF(SECOND, start_time, end_time)) as avg_duration
            FROM conversion_tasks
            WHERE status IN ('pending', 'processing', 'completed', 'failed')
            '''
            result = db_manager.execute_query(query, fetch=True)
            return result[0] if result else {}
        except Exception as e:
            self.logger.error(f"Error getting task stats: {e}")
            return {'error': str(e)}
    
    def broadcast_status(self):
        """定期廣播狀態"""
        # 此執行緒由 start() 啟動，設為 daemon=True，主執行緒（Flask）結束時自動終止
        # 廣播的事件名稱：scan_progress、process_progress、system_status、task_stats、all_progress；
        # 狀態資料來自 JSON 檔案而非直接查詢 daemon，解耦 API 伺服器與 daemon 內部實作
        while self.is_running:
            try:
                scan_status = self.get_cached_scan_status()
                process_status = self.get_cached_process_status()
                system_status = self.get_system_status()
                task_stats = self.get_task_stats()
                
                timestamp = time.time()
                
                # 透過 WebSocket 廣播
                self.socketio.emit('scan_progress', scan_status or {'error': 'No scan status available'})
                self.socketio.emit('process_progress', process_status or {'error': 'No process status available'})
                self.socketio.emit('system_status', system_status)
                self.socketio.emit('task_stats', task_stats)
                self.socketio.emit('all_progress', {
                    'scan': scan_status,
                    'process': process_status,
                    'system': system_status,
                    'stats': task_stats,
                    'timestamp': timestamp
                })
                
                self.logger.debug(f"Status broadcast completed at {datetime.now().isoformat()}")
                
                time.sleep(2)  # 每2秒廣播一次：頻率足夠讓前端即時顯示，又不會對資料庫或磁碟造成過大壓力
            
            except Exception as e:
                self.logger.error(f"Error in broadcast loop: {e}")
                time.sleep(5)
    
    def setup_routes(self):
        """設定 REST API 路由"""
        
        @self.app.route('/api/health', methods=['GET'])
        def health_check():
            """健康檢查"""
            return jsonify({
                'status': 'healthy',
                'timestamp': time.time(),
                'server_time': datetime.now().isoformat(),
                'version': '1.0.0'
            })
        
        @self.app.route('/api/status', methods=['GET'])
        def get_status():
            """獲取所有 daemon 的狀態"""
            scan_status = self.get_cached_scan_status()
            process_status = self.get_cached_process_status()
            system_status = self.get_system_status()
            task_stats = self.get_task_stats()
            
            return jsonify({
                'scan_daemon': scan_status or {'status': 'not available'},
                'process_daemon': process_status or {'status': 'not available'},
                'system': system_status,
                'stats': task_stats,
                'timestamp': time.time()
            })
        
        @self.app.route('/api/progress/scan', methods=['GET'])
        def get_scan_progress():
            """獲取掃描進度"""
            scan_status = self.get_cached_scan_status()
            if not scan_status:
                return jsonify({'error': 'Scan daemon status not available'}), 404
            return jsonify(scan_status)
        
        @self.app.route('/api/progress/process', methods=['GET'])
        def get_process_progress():
            """獲取處理進度"""
            process_status = self.get_cached_process_status()
            if not process_status:
                return jsonify({'error': 'Process daemon status not available'}), 404
            return jsonify(process_status)
        
        @self.app.route('/api/progress/system', methods=['GET'])
        def get_system_status():
            """獲取系統狀態"""
            return jsonify(self.get_system_status())
        
        @self.app.route('/api/progress/stats', methods=['GET'])
        def get_task_stats():
            """獲取任務統計"""
            return jsonify(self.get_task_stats())
    
    def setup_socketio_events(self):
        """設定 WebSocket 事件"""
        
        @self.socketio.on('connect')
        def handle_connect():
            """處理連接"""
            self.logger.info(f"Client connected: {request.sid}")
            emit('connection_response', {'status': 'connected', 'sid': request.sid})
        
        @self.socketio.on('disconnect')
        def handle_disconnect():
            """處理斷線"""
            self.logger.info(f"Client disconnected: {request.sid}")
        
        @self.socketio.on('request_progress')
        def handle_request_progress(data):
            """處理進度請求"""
            daemon_type = data.get('daemon_type', 'all')
            
            if daemon_type == 'scan':
                scan_status = self.get_cached_scan_status()
                emit('scan_progress', scan_status or {'error': 'No scan status available'})
            elif daemon_type == 'process':
                process_status = self.get_cached_process_status()
                emit('process_progress', process_status or {'error': 'No process status available'})
            elif daemon_type == 'all':
                scan_status = self.get_cached_scan_status()
                process_status = self.get_cached_process_status()
                system_status = self.get_system_status()
                task_stats = self.get_task_stats()
                
                emit('all_progress', {
                    'scan': scan_status,
                    'process': process_status,
                    'system': system_status,
                    'stats': task_stats,
                    'timestamp': time.time()
                })
    
    def start(self):
        """啟動 API 伺服器"""
        self.start_time = time.time()
        
        # 確保狀態目錄存在
        Path(self.status_dir).mkdir(parents=True, exist_ok=True)
        
        # 啟動狀態廣播執行緒
        self.is_running = True
        self.status_thread = threading.Thread(target=self.broadcast_status)
        self.status_thread.daemon = True
        self.status_thread.start()
        
        self.logger.info(f"Starting API server on {self.host}:{self.port}")
        self.logger.info(f"Scan status file: {self.scan_status_file}")
        self.logger.info(f"Process status file: {self.process_status_file}")
        
        self.socketio.run(self.app, host=self.host, port=self.port, debug=False)
    
    def stop(self):
        """停止 API 伺服器"""
        self.logger.info("Stopping API server...")
        self.is_running = False
        if self.status_thread:
            self.status_thread.join(timeout=5)
        self.logger.info("API server stopped")

def start_api_server():
    """啟動 API 伺服器"""
    try:
        # 設定參數
        host = os.getenv('API_SERVER_HOST', '0.0.0.0')
        port = int(os.getenv('API_SERVER_PORT', '5000'))
        
        # 建立並啟動伺服器
        server = APIServer(host=host, port=port)
        server.start()
        
    except KeyboardInterrupt:
        print("\nAPI server stopped by user")
    except Exception as e:
        print(f"Error starting API server: {e}")
        sys.exit(1)

if __name__ == '__main__':
    start_api_server()
