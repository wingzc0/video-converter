#!/usr/bin/env python3
"""
處理 daemon 管理腳本

用法:
  python start_process_daemon.py [start] [--foreground|-f]   啟動 daemon（預設指令）
  python start_process_daemon.py stop                        停止 daemon
  python start_process_daemon.py restart [--foreground|-f]   重新啟動 daemon
  python start_process_daemon.py status                      顯示 daemon 狀態
"""
import sys
import os
import json
from pathlib import Path

# 將專案根目錄加入 Python 路徑
project_root = Path(__file__).parent.resolve()
sys.path.insert(0, str(project_root))

from daemons.process_daemon import ProcessDaemon


def make_daemon():
    check_interval = int(os.getenv('CHECK_INTERVAL', '60'))
    max_workers = int(os.getenv('MAX_WORKERS', '2'))
    return ProcessDaemon(check_interval=check_interval, max_workers=max_workers)


def cmd_start(daemon, foreground=False):
    print(f"Starting process daemon in {'foreground' if foreground else 'background'} mode")
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
        print("process_daemon is not running.")
        return
    pid = status['pid']
    print(f"Stopping process daemon (PID: {pid})...")
    if daemon.stop():
        print("process_daemon stopped successfully.")
    else:
        print("Failed to stop process_daemon. Check logs for details.")
        sys.exit(1)


def cmd_restart(daemon, foreground=False):
    status = daemon.status()
    if status['status'] == 'running':
        print(f"Stopping process daemon (PID: {status['pid']})...")
        daemon.stop()
        import time; time.sleep(2)
    cmd_start(daemon, foreground)


def cmd_status(daemon):
    status = daemon.status()
    pid = status.get('pid')
    state = status.get('status', 'unknown')

    # 嘗試讀取 status JSON 取得更詳細資訊
    status_file = Path(daemon.pid_file).parent / 'processor_status.json'
    detail = {}
    if status_file.exists():
        try:
            detail = json.loads(status_file.read_text())
        except Exception:
            pass

    icon = '✅' if state == 'running' else '❌'
    print(f"{icon} process_daemon: {state}" + (f" (PID: {pid})" if pid else ""))
    if detail:
        last_check = detail.get('last_check_time', 'N/A')
        processing = detail.get('tasks_processing', 0)
        completed = detail.get('tasks_completed', 0)
        failed = detail.get('tasks_failed', 0)
        queue = detail.get('queue_size', 0)
        workers = detail.get('active_workers', 0)
        max_workers = detail.get('max_workers', 0)
        errors = detail.get('error_count', 0)
        print(f"   Last check : {last_check}")
        print(f"   Processing : {processing}  |  Queue: {queue}")
        print(f"   Completed  : {completed}  |  Failed: {failed}")
        print(f"   Workers    : {workers}/{max_workers}  |  Errors: {errors}")


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
        print("Usage: start_process_daemon.py [start|stop|restart|status] [--foreground|-f]")
        sys.exit(1)


if __name__ == '__main__':
    main()
