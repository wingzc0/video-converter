#!/usr/bin/env python3
"""
啟動處理 daemon 的腳本
"""
import sys
import os
from pathlib import Path

# 將專案根目錄加入 Python 路徑
project_root = Path(__file__).parent.resolve()
sys.path.insert(0, str(project_root))

from daemons.process_daemon import ProcessDaemon

def main():
    # 設定參數（可從環境變數讀取）
    check_interval = int(os.getenv('CHECK_INTERVAL', '60'))
    max_workers = int(os.getenv('MAX_WORKERS', '2'))
    
    daemon = ProcessDaemon(
        check_interval=check_interval,
        max_workers=max_workers
    )
    
    # 檢查是否在前景模式
    foreground = '--foreground' in sys.argv or '-f' in sys.argv
    
    if foreground:
        print(f"Starting process daemon in foreground mode (PID: {os.getpid()})")
        print(f"PID file: {daemon.pid_file}")
        print(f"Log file: {daemon.log_file}")
        print(f"Error log file: {daemon.stderr_log_file}")
        daemon.run_in_foreground()
    else:
        print(f"Starting process daemon in background mode")
        print(f"PID file: {daemon.pid_file}")
        print(f"Log file: {daemon.log_file}")
        print(f"Error log file: {daemon.stderr_log_file}")
        daemon.start(daemon_mode=True)

if __name__ == '__main__':
    main()
