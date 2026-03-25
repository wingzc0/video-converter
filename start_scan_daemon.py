#!/usr/bin/env python3
"""
掃描 daemon 管理腳本

用法:
  python start_scan_daemon.py [start] [--foreground|-f]   啟動 daemon（預設指令）
  python start_scan_daemon.py stop                        停止 daemon
  python start_scan_daemon.py restart [--foreground|-f]   重新啟動 daemon
  python start_scan_daemon.py status                      顯示 daemon 狀態
"""
import sys
import os
import json
from pathlib import Path

# 將專案根目錄加入 Python 路徑
project_root = Path(__file__).parent.resolve()
sys.path.insert(0, str(project_root))

from daemons.scan_daemon import ScanDaemon


def make_daemon():
    scan_interval = int(os.getenv('SCAN_INTERVAL', '300'))
    return ScanDaemon(scan_interval=scan_interval)


def cmd_start(daemon, foreground=False):
    print(f"Starting scan daemon in {'foreground' if foreground else 'background'} mode")
    print(f"PID file: {daemon.pid_file}")
    print(f"Log file: {daemon.log_file}")
    print(f"Error log file: {daemon.stderr_log_file}")
    if foreground:
        print(f"PID: {os.getpid()}")
        daemon.run_in_foreground()
    else:
        daemon.start(daemon_mode=True)


def cmd_stop(daemon):
    status = daemon.status()
    if status['status'] != 'running':
        print("scan_daemon is not running.")
        return
    pid = status['pid']
    print(f"Stopping scan daemon (PID: {pid})...")
    if daemon.stop():
        print("scan_daemon stopped successfully.")
    else:
        print("Failed to stop scan_daemon. Check logs for details.")
        sys.exit(1)


def cmd_restart(daemon, foreground=False):
    status = daemon.status()
    if status['status'] == 'running':
        print(f"Stopping scan daemon (PID: {status['pid']})...")
        daemon.stop()
        import time; time.sleep(2)
    cmd_start(daemon, foreground)


def cmd_status(daemon):
    status = daemon.status()
    pid = status.get('pid')
    state = status.get('status', 'unknown')

    # 嘗試讀取 status JSON 取得更詳細資訊
    status_file = Path(daemon.pid_file).parent / 'scanner_status.json'
    detail = {}
    if status_file.exists():
        try:
            detail = json.loads(status_file.read_text())
        except Exception:
            pass

    icon = '✅' if state == 'running' else '❌'
    print(f"{icon} scan_daemon: {state}" + (f" (PID: {pid})" if pid else ""))
    if detail:
        last_scan = detail.get('last_scan_time', 'N/A')
        files = detail.get('files_scanned', 0)
        added = detail.get('tasks_added', 0)
        errors = detail.get('error_count', 0)
        print(f"   Last scan  : {last_scan}")
        print(f"   Files scan : {files}")
        print(f"   Tasks added: {added}")
        print(f"   Errors     : {errors}")


def main():
    args = sys.argv[1:]
    foreground = '--foreground' in args or '-f' in args
    args = [a for a in args if a not in ('--foreground', '-f')]

    command = args[0] if args else 'start'

    daemon = make_daemon()

    if command == 'start':
        cmd_start(daemon, foreground)
    elif command == 'stop':
        cmd_stop(daemon)
    elif command == 'restart':
        cmd_restart(daemon, foreground)
    elif command == 'status':
        cmd_status(daemon)
    else:
        print(f"Unknown command: {command}")
        print("Usage: start_scan_daemon.py [start|stop|restart|status] [--foreground|-f]")
        sys.exit(1)


if __name__ == '__main__':
    main()
