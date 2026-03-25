#!/usr/bin/env python3
"""
啟動掃描 daemon 的腳本
"""
import sys
import os
from pathlib import Path

# 將專案根目錄加入 Python 路徑
project_root = Path(__file__).parent.resolve()
sys.path.insert(0, str(project_root))

from daemons.scan_daemon import ScanDaemon

def main():
    # 設定掃描間隔（可從環境變數讀取）
    scan_interval = int(os.getenv('SCAN_INTERVAL', '300'))
    
    daemon = ScanDaemon(scan_interval=scan_interval)
    
    # 檢查是否在前景模式
    foreground = '--foreground' in sys.argv or '-f' in sys.argv
    
    if foreground:
        print(f"Starting scan daemon in foreground mode (PID: {os.getpid()})")
        print(f"PID file: {daemon.pid_file}")
        print(f"Log file: {daemon.log_file}")
        print(f"Error log file: {daemon.stderr_log_file}")
        daemon.run_in_foreground()
    else:
        print(f"Starting scan daemon in background mode")
        print(f"PID file: {daemon.pid_file}")
        print(f"Log file: {daemon.log_file}")
        print(f"Error log file: {daemon.stderr_log_file}")
        daemon.start(daemon_mode=True)

if __name__ == '__main__':
    main()
