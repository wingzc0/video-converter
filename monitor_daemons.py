#!/usr/bin/env python3
"""
Daemon Monitor - 監控 scan-only 和 process-only daemon 的處理狀態
"""

import os
import sys
import time
import json
import argparse
import requests
from datetime import datetime, timedelta
from pathlib import Path
from dotenv import load_dotenv
import platform
import signal

# 載入環境變數
load_dotenv()

class DaemonMonitor:
    """Daemon 監控類別"""
    
    def __init__(self, api_url=None, refresh_interval=2):
        self.api_url = api_url or os.getenv('API_SERVER_URL', 'http://localhost:5000')
        self.refresh_interval = refresh_interval
        self.is_running = True
        
        # 設定信號處理
        signal.signal(signal.SIGINT, self.handle_shutdown)
        signal.signal(signal.SIGTERM, self.handle_shutdown)
        
        # 初始化 API endpoint
        self.endpoints = {
            'scan_progress': f"{self.api_url}/api/progress/scan",
            'process_progress': f"{self.api_url}/api/progress/process",
            'system_status': f"{self.api_url}/api/progress/system",
            'task_stats': f"{self.api_url}/api/progress/stats"
        }
        
        # 檢查 API 連接
        self.check_api_connection()
    
    def handle_shutdown(self, signum, frame):
        """處理關閉信號"""
        self.is_running = False
        print("\n\n監控程式已停止")
        sys.exit(0)
    
    def check_api_connection(self):
        """檢查 API 連接"""
        try:
            response = requests.get(f"{self.api_url}/api/health", timeout=5)
            if response.status_code != 200:
                print(f"⚠️  API 伺服器回應狀態碼: {response.status_code}")
                print(f"   API URL: {self.api_url}")
                print("   請確認 API 伺服器是否正在執行")
                sys.exit(1)
        except requests.exceptions.ConnectionError:
            print(f"❌ 無法連接到 API 伺服器: {self.api_url}")
            print("   請確認 API 伺服器是否正在執行，或檢查 API_SERVER_URL 設定")
            sys.exit(1)
        except Exception as e:
            print(f"❌ 檢查 API 連接時發生錯誤: {str(e)}")
            sys.exit(1)
    
    def get_progress(self, endpoint):
        """獲取進度資訊，增加型別驗證"""
        try:
            response = requests.get(endpoint, timeout=5)
            if response.status_code == 200:
                data = response.json()
                
                # 驗證 task_stats 中的數值型別
                if 'total' in data and isinstance(data['total'], str):
                    try:
                        data['total'] = int(data['total'])
                    except ValueError:
                        data['total'] = 0
                
                if 'completed' in data and isinstance(data['completed'], str):
                    try:
                        data['completed'] = int(data['completed'])
                    except ValueError:
                        data['completed'] = 0
                
                return data
            else:
                return {'error': f'HTTP {response.status_code}'}
        except requests.exceptions.RequestException as e:
            return {'error': str(e)}
        except json.JSONDecodeError:
            return {'error': 'Invalid JSON response'}
        except Exception as e:
            return {'error': f'Unexpected error: {str(e)}'}
    
    def format_duration(self, seconds):
        """格式化持續時間"""
        if seconds < 0:
            seconds = 0  # 系統時鐘向後調整時可能出現負值，夾至 0
        
        if seconds < 60:
            return f"{seconds:.0f} 秒"
        elif seconds < 3600:
            minutes = seconds // 60
            remaining = seconds % 60
            return f"{minutes:.0f} 分 {remaining:.0f} 秒"
        else:
            hours = seconds // 3600
            minutes = (seconds % 3600) // 60
            return f"{hours:.0f} 小時 {minutes:.0f} 分"
    
    def format_file_size(self, size_bytes):
        """格式化檔案大小"""
        if size_bytes is None or size_bytes < 0:
            return "N/A"
        
        if size_bytes < 1024:
            return f"{size_bytes} B"
        elif size_bytes < 1024 * 1024:
            return f"{size_bytes / 1024:.1f} KB"
        elif size_bytes < 1024 * 1024 * 1024:
            return f"{size_bytes / (1024 * 1024):.1f} MB"
        else:
            return f"{size_bytes / (1024 * 1024 * 1024):.1f} GB"
    
    def get_daemon_status(self, daemon_type):
        """獲取 daemon 狀態"""
        try:
            # 從 PID 檔案檢查 daemon 狀態
            pid_file = None
            if daemon_type == 'scan':
                pid_file = os.getenv('SCAN_DAEMON_PID_FILE', '/var/run/video-converter/scanner.pid')
            elif daemon_type == 'process':
                pid_file = os.getenv('PROCESS_DAEMON_PID_FILE', '/var/run/video-converter/processor.pid')
            
            if not pid_file or not os.path.exists(pid_file):
                return {
                    'status': 'stopped',
                    'pid': None,
                    'uptime': 0
                }
            
            with open(pid_file, 'r') as f:
                pid = int(f.read().strip())
            
            # 檢查行程是否存在
            try:
                # os.kill(pid, 0) 不發送任何信號，只檢查目標行程是否存在且有權限發送信號；
                # 若行程不存在會拋出 OSError(ESRCH)，若無權限則拋出 OSError(EPERM)
                os.kill(pid, 0)  # 不會發送信號，只檢查是否存在
                # 獲取 uptime
                uptime = 0
                try:
                    with open(f'/proc/{pid}/stat', 'r') as f:
                        stat = f.read().split()
                        # /proc/[pid]/stat 第 22 欄位（索引 21）是行程啟動時間，
                        # 單位為從系統開機起算的 clock ticks（非 Unix timestamp），
                        # 需除以 SC_CLK_TCK 才能換算成秒數
                        start_time_ticks = int(stat[21])
                        clock_ticks_per_second = os.sysconf(os.sysconf_names['SC_CLK_TCK'])
                        start_time_seconds = start_time_ticks / clock_ticks_per_second
                        # 從 /proc/uptime 取得系統已運行秒數，與行程啟動時間相減得到正確 uptime；
                        # 不能用 time.time() - start_time_seconds（兩者時間基準不同）
                        with open('/proc/uptime') as uf:
                            system_uptime = float(uf.read().split()[0])
                        uptime = system_uptime - start_time_seconds
                except:
                    uptime = 0
                
                return {
                    'status': 'running',
                    'pid': pid,
                    'uptime': uptime
                }
            except OSError:
                # 行程不存在
                return {
                    'status': 'stopped',
                    'pid': pid,
                    'uptime': 0
                }
                
        except Exception as e:
            return {
                'status': 'unknown',
                'error': str(e),
                'pid': None,
                'uptime': 0
            }
    
    def display_monitor(self, continuous=False):
        """顯示監控畫面"""
        clear_command = 'cls' if platform.system() == 'Windows' else 'clear'
        
        while self.is_running:
            # 清除螢幕
            if continuous:
                os.system(clear_command)
            
            # 顯示標題
            current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            print(f"\n{'=' * 80}")
            print(f"🎬 Video Converter Daemon Monitor - {current_time}")
            print(f"{'=' * 80}")
            print(f"API Server: {self.api_url}")
            print(f"Refresh Interval: {self.refresh_interval} seconds")
            print(f"{'=' * 80}\n")
            
            # 顯示掃描 daemon 狀態
            print(f"🔍 {self.get_status_color('scan')} Scan Daemon Status")
            print(f"{'-' * 60}")
            
            scan_daemon_status = self.get_daemon_status('scan')
            scan_progress = self.get_progress(self.endpoints['scan_progress'])
            
            self.display_daemon_info('scan', scan_daemon_status, scan_progress)
            print()
            
            # 顯示處理 daemon 狀態
            print(f"⚙️  {self.get_status_color('process')} Process Daemon Status")
            print(f"{'-' * 60}")
            
            process_daemon_status = self.get_daemon_status('process')
            process_progress = self.get_progress(self.endpoints['process_progress'])
            
            self.display_daemon_info('process', process_daemon_status, process_progress)
            print()
            
            # 顯示任務統計
            print(f"📊 {self.get_color('cyan')} Task Statistics")
            print(f"{'-' * 60}")
            
            task_stats = self.get_progress(self.endpoints['task_stats'])
            self.display_task_stats(task_stats)
            print()
            
            # 顯示操作提示
            print(f"{self.get_color('dim')}")
            print("操作提示:")
            print("  q - 退出監控程式")
            print("  r - 手動重新整理")
            print("  c - 清除螢幕")
            print(f"  Ctrl+C - 退出監控程式{self.get_color('reset')}")
            print()
            
            if not continuous:
                break
            
            # 等待下一次更新
            start_time = time.time()
            while time.time() - start_time < self.refresh_interval:
                if not self.is_running:
                    break
                time.sleep(0.1)
    
    def get_status_color(self, daemon_type):
        """獲取狀態顏色"""
        if daemon_type == 'scan':
            return self.get_color('blue')
        elif daemon_type == 'process':
            return self.get_color('green')
        return self.get_color('white')
    
    def get_color(self, color_name):
        """獲取 ANSI 顏色代碼"""
        colors = {
            'red': '\033[91m',
            'green': '\033[92m',
            'yellow': '\033[93m',
            'blue': '\033[94m',
            'magenta': '\033[95m',
            'cyan': '\033[96m',
            'white': '\033[97m',
            'dim': '\033[90m',
            'bold': '\033[1m',
            'underline': '\033[4m',
            'reset': '\033[0m'
        }
        return colors.get(color_name, '')
    
    def display_daemon_info(self, daemon_type, daemon_status, progress_data):
        """顯示 daemon 資訊"""
        color = self.get_status_color(daemon_type)
        
        # 顯示 daemon 狀態
        status_color = self.get_color('green') if daemon_status['status'] == 'running' else self.get_color('red')
        status_text = daemon_status['status'].upper()
        
        print(f"狀態: {status_color}{status_text}{self.get_color('reset')}", end=" | ")
        
        if daemon_status.get('pid'):
            print(f"PID: {daemon_status['pid']}", end=" | ")
        
        if daemon_status.get('uptime'):
            uptime_str = self.format_duration(daemon_status['uptime'])
            print(f"運行時間: {uptime_str}")
        else:
            print()
        
        # 顯示進度資訊
        if progress_data.get('error'):
            print(f"  {self.get_color('red')}⚠️  獲取進度時發生錯誤: {progress_data['error']}{self.get_color('reset')}")
            return
        
        if daemon_type == 'scan':
            self.display_scan_progress(progress_data)
        else:
            self.display_process_progress(progress_data)
    
    def display_scan_progress(self, progress_data):
        """顯示掃描進度"""
        if not progress_data:
            print("  ⚠️  沒有可用的掃描進度資料")
            return
        
        status = progress_data.get('status', 'unknown')
        last_scan_time = progress_data.get('last_scan_time')
        files_scanned = progress_data.get('files_scanned', 0)
        tasks_added = progress_data.get('tasks_added', 0)
        error_count = progress_data.get('error_count', 0)
        
        # 顯示目前狀態
        status_text = {
            'idle': '閒置中',
            'scanning': '掃描中',
            'checking': '檢查中'
        }.get(status, status)
        
        status_color = self.get_color('green') if status == 'idle' else self.get_color('yellow')
        print(f"狀態: {status_color}{status_text}{self.get_color('reset')}")
        
        # 顯示上次掃描時間
        if last_scan_time:
            try:
                last_scan_dt = datetime.fromisoformat(last_scan_time)
                time_diff = datetime.now() - last_scan_dt
                time_str = self.format_duration(time_diff.total_seconds())
                print(f"上次掃描: {last_scan_dt.strftime('%H:%M:%S')} ({time_str} 前)")
            except:
                print(f"上次掃描: {last_scan_time}")
        
        # 顯示掃描統計
        print(f"掃描檔案數: {files_scanned}")
        print(f"新增任務數: {tasks_added}")
        
        # 顯示錯誤
        if error_count > 0:
            error_color = self.get_color('red')
            print(f"錯誤數: {error_color}{error_count}{self.get_color('reset')}")
    
    def display_process_progress(self, progress_data):
        """顯示處理進度"""
        if not progress_data:
            print("  ⚠️  沒有可用的處理進度資料")
            return
        
        status = progress_data.get('status', 'unknown')
        last_check_time = progress_data.get('last_check_time')
        tasks_processing = progress_data.get('tasks_processing', 0)
        tasks_completed = progress_data.get('tasks_completed', 0)
        tasks_failed = progress_data.get('tasks_failed', 0)
        queue_size = progress_data.get('queue_size', 0)
        active_workers = progress_data.get('active_workers', 0)
        max_workers = progress_data.get('max_workers', 0)
        
        # 顯示目前狀態
        status_text = {
            'idle': '閒置中',
            'checking': '檢查任務中',
            'processing': '處理中'
        }.get(status, status)
        
        status_color = {
            'idle': self.get_color('green'),
            'processing': self.get_color('yellow'),
            'checking': self.get_color('blue')
        }.get(status, self.get_color('white'))
        
        print(f"狀態: {status_color}{status_text}{self.get_color('reset')}")
        
        # 顯示上次檢查時間
        if last_check_time:
            try:
                last_check_dt = datetime.fromisoformat(last_check_time)
                time_diff = datetime.now() - last_check_dt
                time_str = self.format_duration(time_diff.total_seconds())
                print(f"上次檢查: {last_check_dt.strftime('%H:%M:%S')} ({time_str} 前)")
            except:
                print(f"上次檢查: {last_check_time}")
        
        # 顯示任務統計
        print(f"處理中任務: {tasks_processing}")
        print(f"已完成任務: {tasks_completed}")
        print(f"失敗任務: {self.get_color('red')}{tasks_failed}{self.get_color('reset')}")
        
        # 顯示佇列和工作執行緒
        if queue_size is not None:
            print(f"佇列大小: {queue_size}")
        
        if active_workers is not None and max_workers is not None:
            worker_status = f"{active_workers}/{max_workers}"
            worker_color = self.get_color('green') if active_workers > 0 else self.get_color('yellow')
            print(f"工作執行緒: {worker_color}{worker_status}{self.get_color('reset')}")
    
    def display_task_stats(self, task_stats):
        """顯示任務統計，增加型別安全檢查"""
        if not task_stats or task_stats.get('error'):
            print("  ⚠️  無法獲取任務統計資料")
            return
        
        # 確保所有數值都是數字類型
        # safe_int 的必要性：資料庫的 SUM()/COUNT() 可能回傳 Decimal 或 None 型別，
        # 直接傳給字串格式化或算術運算會導致 TypeError；透過 float() 中間轉換可同時處理 Decimal 和數值字串
        def safe_int(value, default=0):
            try:
                return int(float(value)) if value is not None else default
            except (ValueError, TypeError):
                return default
        
        total = safe_int(task_stats.get('total', 0))
        pending = safe_int(task_stats.get('pending', 0))
        processing = safe_int(task_stats.get('processing', 0))
        completed = safe_int(task_stats.get('completed', 0))
        failed = safe_int(task_stats.get('failed', 0))
        retried = safe_int(task_stats.get('retried', 0))
        
        # 處理 avg_duration
        avg_duration = task_stats.get('avg_duration', 0)
        try:
            avg_duration = float(avg_duration) if avg_duration is not None else 0
        except (ValueError, TypeError):
            avg_duration = 0
        
        # 進度條
        progress_bar = self.create_progress_bar(completed, total) if total > 0 else "N/A"
        
        print(f"總任務數: {total}")
        print(f"待處理: {pending}")
        print(f"處理中: {processing}")
        print(f"已完成: {self.get_color('green')}{completed}{self.get_color('reset')}")
        print(f"失敗: {self.get_color('red')}{failed}{self.get_color('reset')}")
        print(f"重試次數: {retried}")
        
        if avg_duration > 0:
            avg_minutes = avg_duration / 60
            print(f"平均處理時間: {avg_minutes:.1f} 分鐘")
        
        print(f"整體進度: {progress_bar}")
    
    def create_progress_bar(self, current, total, length=30):
        """建立進度條，支援字串和數字輸入"""
        # 確保 current 和 total 是數字類型
        try:
            current = float(current) if current is not None else 0
            total = float(total) if total is not None else 0
        except (ValueError, TypeError):
            return f"[{'░' * length}] N/A"
        
        if total <= 0:
            return f"[{' ' * length}] 0%"
        
        progress = min(1.0, current / total)
        filled_length = int(length * progress)
        bar = '█' * filled_length + '░' * (length - filled_length)
        percentage = progress * 100
        
        if percentage < 30:
            # 低於 30%：以紅色警示進度嚴重落後
            color = self.get_color('red')
        elif percentage < 70:
            # 30-70%：以黃色表示進行中
            color = self.get_color('yellow')
        else:
            # 70% 以上：以綠色表示接近完成
            color = self.get_color('green')
        
        return f"{color}[{bar}] {percentage:.1f}%{self.get_color('reset')}"

def main():
    """主程式"""
    parser = argparse.ArgumentParser(description='監控 scan-only 和 process-only daemon 的處理狀態')
    parser.add_argument('-u', '--url', help='API 伺服器 URL (預設: http://localhost:5000)')
    parser.add_argument('-i', '--interval', type=int, default=2, help='更新間隔（秒），設為 0 則手動更新 (預設: 2)')
    parser.add_argument('-c', '--continuous', action='store_true', help='持續監控模式（預設: 單次顯示）')
    parser.add_argument('--no-color', action='store_true', help='關閉彩色輸出')
    
    args = parser.parse_args()
    
    # 檢查必要的套件
    try:
        import requests
    except ImportError as e:
        print(f"❌ 缺少必要的 Python 套件: {str(e)}")
        print("請安裝依賴套件:")
        print("  pip install requests python-dotenv")
        sys.exit(1)
    
    # 檢查 .env 檔案
    env_path = Path('.env')
    if not env_path.exists():
        print(f"⚠️  找不到 .env 檔案，將使用預設設定")
    
    # 建立監控器
    monitor = DaemonMonitor(
        api_url=args.url,
        refresh_interval=args.interval
    )
    
    # 連續監控模式
    if args.continuous or args.interval > 0:
        try:
            print("啟動連續監控模式，按 'q' 退出，按 'r' 重新整理，按 'c' 清除螢幕...")
            monitor.display_monitor(continuous=True)
        except KeyboardInterrupt:
            print("\n\n監控程式已停止")
    else:
        # 單次顯示模式
        monitor.display_monitor(continuous=False)

if __name__ == '__main__':
    main()
