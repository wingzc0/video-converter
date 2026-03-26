#!/usr/bin/env python3
"""
Daemon 統一管理腳本

用法:
  python daemon_ctl.py <target> [command] [--foreground|-f]

target:
  scan      掃描 daemon
  process   處理 daemon
  all       同時操作兩個 daemon

command（預設 start）:
  start     啟動 daemon
  stop      停止 daemon
  restart   重新啟動 daemon
  status    顯示 daemon 狀態

選項:
  --foreground, -f   在前景執行（適合除錯或 systemd 直接管理）

範例:
  python daemon_ctl.py scan
  python daemon_ctl.py process start --foreground
  python daemon_ctl.py all stop
  python daemon_ctl.py all status
  python daemon_ctl.py scan restart -f
"""
import sys
import os
import json
import time
from pathlib import Path

project_root = Path(__file__).parent.resolve()
sys.path.insert(0, str(project_root))


# ---------------------------------------------------------------------------
# Daemon 建立
# ---------------------------------------------------------------------------

def make_scan_daemon():
    from daemons.scan_daemon import ScanDaemon
    scan_interval = int(os.getenv('SCAN_INTERVAL', '300'))
    return ScanDaemon(scan_interval=scan_interval)


def make_process_daemon():
    from daemons.process_daemon import ProcessDaemon
    check_interval = int(os.getenv('CHECK_INTERVAL', '60'))
    max_workers = int(os.getenv('MAX_WORKERS', '2'))
    return ProcessDaemon(check_interval=check_interval, max_workers=max_workers)


# ---------------------------------------------------------------------------
# 指令實作
# ---------------------------------------------------------------------------

def cmd_start(daemon, name, foreground=False):
    print(f"Starting {name} in {'foreground' if foreground else 'background'} mode")
    print(f"PID file: {daemon.pid_file}")
    print(f"Log file: {daemon.log_file}")
    print(f"Error log file: {daemon.stderr_log_file}")
    if foreground:
        print(f"PID: {os.getpid()}")
        daemon.run_in_foreground()
    else:
        daemon.start(daemon_mode=True)


def cmd_stop(daemon, name):
    status = daemon.status()
    if status['status'] != 'running':
        print(f"{name} is not running.")
        return
    pid = status['pid']
    print(f"Stopping {name} (PID: {pid})...")
    if daemon.stop():
        print(f"{name} stopped successfully.")
    else:
        print(f"Failed to stop {name}. Check logs for details.")
        sys.exit(1)


def cmd_restart(daemon, name, foreground=False):
    status = daemon.status()
    if status['status'] == 'running':
        print(f"Stopping {name} (PID: {status['pid']})...")
        daemon.stop()
        time.sleep(2)
    cmd_start(daemon, name, foreground)


def cmd_status_scan(daemon):
    status = daemon.status()
    pid = status.get('pid')
    state = status.get('status', 'unknown')

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
        print(f"   Last scan  : {detail.get('last_scan_time', 'N/A')}")
        print(f"   Files scan : {detail.get('files_scanned', 0)}")
        print(f"   Tasks added: {detail.get('tasks_added', 0)}")
        print(f"   Errors     : {detail.get('error_count', 0)}")


def cmd_status_process(daemon):
    status = daemon.status()
    pid = status.get('pid')
    state = status.get('status', 'unknown')

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
        print(f"   Last check : {detail.get('last_check_time', 'N/A')}")
        print(f"   Processing : {detail.get('tasks_processing', 0)}  |  Queue: {detail.get('queue_size', 0)}")
        print(f"   Completed  : {detail.get('tasks_completed', 0)}  |  Failed: {detail.get('tasks_failed', 0)}")
        print(f"   Workers    : {detail.get('active_workers', 0)}/{detail.get('max_workers', 0)}  |  Errors: {detail.get('error_count', 0)}")


# ---------------------------------------------------------------------------
# 主程式
# ---------------------------------------------------------------------------

USAGE = """\
Usage: python daemon_ctl.py <target> [command] [--foreground|-f]

  target:   scan | process | all
  command:  start (default) | stop | restart | status
  -f / --foreground   run in foreground
"""


def main():
    args = sys.argv[1:]

    if not args or args[0] in ('-h', '--help'):
        print(__doc__)
        sys.exit(0)

    foreground = '--foreground' in args or '-f' in args
    args = [a for a in args if a not in ('--foreground', '-f')]

    target = args[0]
    command = args[1] if len(args) > 1 else 'start'

    if target not in ('scan', 'process', 'all'):
        print(f"Unknown target: {target!r}\n{USAGE}")
        sys.exit(1)

    if command not in ('start', 'stop', 'restart', 'status'):
        print(f"Unknown command: {command!r}\n{USAGE}")
        sys.exit(1)

    # 'all' + foreground 不合理（兩個 foreground 無法同時跑）
    if target == 'all' and foreground and command in ('start', 'restart'):
        print("Error: --foreground cannot be used with target 'all'")
        sys.exit(1)

    targets = ['scan', 'process'] if target == 'all' else [target]

    for t in targets:
        if t == 'scan':
            daemon = make_scan_daemon()
            name = 'scan_daemon'
            status_fn = cmd_status_scan
        else:
            daemon = make_process_daemon()
            name = 'process_daemon'
            status_fn = cmd_status_process

        if command == 'start':
            cmd_start(daemon, name, foreground)
        elif command == 'stop':
            cmd_stop(daemon, name)
        elif command == 'restart':
            cmd_restart(daemon, name, foreground)
        elif command == 'status':
            status_fn(daemon)


if __name__ == '__main__':
    main()
