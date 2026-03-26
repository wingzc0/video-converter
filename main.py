#!/usr/bin/env python3
"""
bcvnas-converter 管理工具

> ⚠️  轉檔邏輯已移至 daemon 版本，本腳本僅作為管理 / 診斷工具使用。
> 啟動轉檔請改用：
>   python3 start_scan_daemon.py [start|stop|restart|status]
>   python3 start_process_daemon.py [start|stop|restart|status]

用法:
  python3 main.py --daemon-status
  python3 main.py --daemon-stop   [--daemon scan|process|all]
  python3 main.py --daemon-restart [--daemon scan|process|all]
  python3 main.py --show-dirs
  python3 main.py --stats
  python3 main.py --retry-failed
  python3 main.py --cleanup-stale [--stale-hours N]
"""
import os
import argparse
import sys
from pathlib import Path
from datetime import datetime, timedelta

from db_manager import db_manager
from dotenv import load_dotenv

load_dotenv()

# ---------------------------------------------------------------------------
# Daemon management helpers
# ---------------------------------------------------------------------------

def _make_scan_daemon():
    from daemons.scan_daemon import ScanDaemon
    return ScanDaemon(scan_interval=int(os.getenv('SCAN_INTERVAL', '300')))

def _make_process_daemon():
    from daemons.process_daemon import ProcessDaemon
    return ProcessDaemon(
        check_interval=int(os.getenv('CHECK_INTERVAL', '60')),
        max_workers=int(os.getenv('MAX_WORKERS', '1')),
    )

def _print_scan_status(daemon):
    import json
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
    print(f"  {icon} scan_daemon   : {state}" + (f"  (PID: {pid})" if pid else ""))
    if detail:
        print(f"     Last scan  : {detail.get('last_scan_time', 'N/A')}")
        print(f"     Files scan : {detail.get('files_scanned', 0)}")
        print(f"     Tasks added: {detail.get('tasks_added', 0)}")
        print(f"     Errors     : {detail.get('error_count', 0)}")

def _print_process_status(daemon):
    import json
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
    print(f"  {icon} process_daemon: {state}" + (f"  (PID: {pid})" if pid else ""))
    if detail:
        print(f"     Last check : {detail.get('last_check_time', 'N/A')}")
        print(f"     Processing : {detail.get('tasks_processing', 0)}  |  Queue: {detail.get('queue_size', 0)}")
        print(f"     Completed  : {detail.get('tasks_completed', 0)}  |  Failed: {detail.get('tasks_failed', 0)}")
        print(f"     Workers    : {detail.get('active_workers', 0)}/{detail.get('max_workers', 0)}  |  Errors: {detail.get('error_count', 0)}")

def cmd_daemon_status(target):
    print("=== Daemon Status ===")
    if target in ('scan', 'all'):
        _print_scan_status(_make_scan_daemon())
    if target in ('process', 'all'):
        _print_process_status(_make_process_daemon())

def cmd_daemon_stop(target):
    if target in ('scan', 'all'):
        d = _make_scan_daemon()
        s = d.status()
        if s['status'] == 'running':
            print(f"Stopping scan_daemon (PID: {s['pid']})...")
            d.stop() and print("scan_daemon stopped.") or print("Failed to stop scan_daemon.")
        else:
            print("scan_daemon is not running.")
    if target in ('process', 'all'):
        d = _make_process_daemon()
        s = d.status()
        if s['status'] == 'running':
            print(f"Stopping process_daemon (PID: {s['pid']})...")
            d.stop() and print("process_daemon stopped.") or print("Failed to stop process_daemon.")
        else:
            print("process_daemon is not running.")

def cmd_daemon_restart(target):
    import time
    if target in ('scan', 'all'):
        d = _make_scan_daemon()
        s = d.status()
        if s['status'] == 'running':
            print(f"Stopping scan_daemon (PID: {s['pid']})...")
            d.stop()
            time.sleep(2)
        print("Starting scan_daemon...")
        d.start(daemon_mode=True)
    if target in ('process', 'all'):
        d = _make_process_daemon()
        s = d.status()
        if s['status'] == 'running':
            print(f"Stopping process_daemon (PID: {s['pid']})...")
            d.stop()
            time.sleep(2)
        print("Starting process_daemon...")
        d.start(daemon_mode=True)

# ---------------------------------------------------------------------------
# Directory structure preview
# ---------------------------------------------------------------------------

def cmd_show_dirs():
    input_dir  = Path(os.getenv('INPUT_DIRECTORY', '')).resolve()
    output_dir = Path(os.getenv('OUTPUT_DIRECTORY', '')).resolve()
    ignore_raw = os.getenv('IGNORE_DIRECTORIES', '')
    ignore_dirs = [Path(d.strip()).resolve() for d in ignore_raw.split(',') if d.strip()]
    supported   = set(e.strip().lower() for e in
                      os.getenv('SUPPORTED_EXTENSIONS',
                                '.mp4,.mkv,.avi,.mov,.flv,.wmv,.m4v,.webm').split(','))

    if not input_dir.exists():
        print(f"❌ INPUT_DIRECTORY not found: {input_dir}")
        return

    print(f"\nDirectory structure preview: {input_dir}")
    print("=" * 70)

    def _is_ignored(p):
        rp = Path(p).resolve()
        for ig in ignore_dirs:
            if rp == ig:
                return True
            try:
                rp.relative_to(ig)
                return True
            except ValueError:
                pass
        return False

    def _walk(d, depth=0):
        if depth > 3 or _is_ignored(d):
            if _is_ignored(d):
                print(f"{'  ' * depth}📁 {d.name}/  [IGNORED]")
            return
        print(f"{'  ' * depth}📁 {d.name}/")
        entries = sorted(d.iterdir()) if d.exists() else []
        dirs  = [e for e in entries if e.is_dir()][:3]
        files = [e for e in entries if e.is_file()][:5]
        for sub in dirs:
            _walk(sub, depth + 1)
        extra_dirs = sum(1 for e in (sorted(d.iterdir()) if d.exists() else []) if e.is_dir()) - len(dirs)
        if extra_dirs > 0:
            print(f"{'  ' * (depth + 1)}📁 ... ({extra_dirs} more directories)")
        for f in files:
            ext = f.suffix.lower()
            if f.name.startswith('480p_'):
                tag = '[CONVERTED]'
            elif ext in supported:
                tag = '[VIDEO]'
            else:
                tag = '[OTHER]'
            print(f"{'  ' * (depth + 1)}📄 {f.name}  {tag}")
        extra_files = sum(1 for e in (sorted(d.iterdir()) if d.exists() else []) if e.is_file()) - len(files)
        if extra_files > 0:
            print(f"{'  ' * (depth + 1)}📄 ... ({extra_files} more files)")

    _walk(input_dir)
    print("=" * 70)
    print(f"\nOutput directory : {output_dir}")
    print(f"Ignored dirs     : {len(ignore_dirs)}")
    for i, ig in enumerate(ignore_dirs, 1):
        icon = '✅' if ig.exists() else '❌'
        print(f"  {i}. {icon} {ig}")

# ---------------------------------------------------------------------------
# Task statistics
# ---------------------------------------------------------------------------

def cmd_stats():
    query = """
    SELECT
        COUNT(*) AS total,
        SUM(status = 'pending')    AS pending,
        SUM(status = 'processing') AS processing,
        SUM(status = 'completed')  AS completed,
        SUM(status = 'failed')     AS failed,
        SUM(retry_count > 0)       AS retried,
        AVG(CASE WHEN status IN ('completed','failed')
            THEN TIMESTAMPDIFF(SECOND, start_time, end_time) END) AS avg_sec
    FROM conversion_tasks
    """
    rows = db_manager.execute_query(query, fetch=True)
    if not rows:
        print("No data in conversion_tasks.")
        return
    s = rows[0]
    print("=== Task Statistics ===")
    print(f"  Total      : {s['total']}")
    print(f"  Pending    : {s['pending']}")
    print(f"  Processing : {s['processing']}")
    print(f"  Completed  : {s['completed']}")
    print(f"  Failed     : {s['failed']}")
    print(f"  Retried    : {s['retried'] or 0}")
    if s['avg_sec']:
        print(f"  Avg time   : {float(s['avg_sec']) / 60:.1f} min")

    # 最近 5 筆失敗
    if s['failed'] and int(s['failed']) > 0:
        print("\n  Recent failed tasks (up to 5):")
        q2 = """
        SELECT id, input_path, error_message, retry_count, updated_at
        FROM conversion_tasks WHERE status='failed'
        ORDER BY updated_at DESC LIMIT 5
        """
        for t in db_manager.execute_query(q2, fetch=True):
            print(f"    [{t['id']}] {Path(t['input_path']).name}")
            print(f"          error={t['error_message']}  retries={t['retry_count']}  at={t['updated_at']}")

# ---------------------------------------------------------------------------
# Retry failed / cleanup stale  (one-shot maintenance)
# ---------------------------------------------------------------------------

def cmd_retry_failed(max_retries=3):
    query = """
    SELECT id, retry_count FROM conversion_tasks
    WHERE status='failed' AND retry_count < %s
    """
    tasks = db_manager.execute_query(query, (max_retries,), fetch=True)
    if not tasks:
        print("No failed tasks eligible for retry.")
        return
    for t in tasks:
        new_count = t['retry_count'] + 1
        db_manager.execute_query(
            """UPDATE conversion_tasks
               SET status='pending', is_processing=FALSE,
                   retry_count=%s,
                   error_message=CONCAT('Retry #',%s,': ',COALESCE(error_message,''))
               WHERE id=%s""",
            (new_count, new_count, t['id'])
        )
    print(f"Retried {len(tasks)} task(s) (max_retries={max_retries}).")

def cmd_cleanup_stale(hours=24):
    stale_time = (datetime.now() - timedelta(hours=hours)).strftime('%Y-%m-%d %H:%M:%S')
    tasks = db_manager.execute_query(
        """SELECT id FROM conversion_tasks
           WHERE status='processing' AND is_processing=TRUE
           AND (start_time IS NULL OR start_time < %s)""",
        (stale_time,), fetch=True
    )
    if not tasks:
        print(f"No stale tasks (>{hours}h in processing).")
        return
    for t in tasks:
        db_manager.execute_query(
            """UPDATE conversion_tasks
               SET status='failed', is_processing=FALSE,
                   error_message=%s, end_time=CURRENT_TIMESTAMP
               WHERE id=%s""",
            (f"Stale after {hours}h (manual cleanup)", t['id'])
        )
        db_manager.execute_query(
            "DELETE FROM processing_lock WHERE task_id=%s", (t['id'],)
        )
    print(f"Cleaned up {len(tasks)} stale task(s) (>{hours}h in processing).")

# ---------------------------------------------------------------------------
# Argument parser & entry point
# ---------------------------------------------------------------------------

def parse_arguments():
    parser = argparse.ArgumentParser(
        description='bcvnas-converter 管理工具',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
範例:
  python3 main.py --daemon-status
  python3 main.py --daemon-stop
  python3 main.py --daemon-restart --daemon scan
  python3 main.py --show-dirs
  python3 main.py --stats
  python3 main.py --retry-failed
  python3 main.py --cleanup-stale --stale-hours 2
"""
    )

    action = parser.add_mutually_exclusive_group(required=True)
    action.add_argument('--daemon-status',  action='store_true', help='顯示 daemon 狀態')
    action.add_argument('--daemon-stop',    action='store_true', help='停止 daemon')
    action.add_argument('--daemon-restart', action='store_true', help='重新啟動 daemon')
    action.add_argument('--show-dirs',      action='store_true', help='預覽目錄結構')
    action.add_argument('--stats',          action='store_true', help='顯示任務統計')
    action.add_argument('--retry-failed',   action='store_true', help='手動重試失敗任務')
    action.add_argument('--cleanup-stale',  action='store_true', help='手動清除過時任務')

    parser.add_argument('--daemon', choices=['scan', 'process', 'all'], default='all',
                        help='指定要操作的 daemon（預設: all，僅用於 --daemon-stop/restart/status）')
    parser.add_argument('--stale-hours', type=float, default=24,
                        help='過時任務的時間閾值（小時，預設 24，僅用於 --cleanup-stale）')
    parser.add_argument('--max-retries', type=int, default=3,
                        help='最大重試次數（預設 3，僅用於 --retry-failed）')
    return parser.parse_args()


def main():
    args = parse_arguments()

    if args.daemon_status:
        cmd_daemon_status(args.daemon)
    elif args.daemon_stop:
        cmd_daemon_stop(args.daemon)
    elif args.daemon_restart:
        cmd_daemon_restart(args.daemon)
    elif args.show_dirs:
        cmd_show_dirs()
    elif args.stats:
        cmd_stats()
    elif args.retry_failed:
        cmd_retry_failed(args.max_retries)
    elif args.cleanup_stale:
        cmd_cleanup_stale(args.stale_hours)


if __name__ == '__main__':
    main()
