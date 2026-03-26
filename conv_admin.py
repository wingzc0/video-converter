#!/usr/bin/env python3
"""
bcvnas-converter 資料庫診斷與維護工具

> ⚠️  Daemon 管理（start/stop/restart/status）請改用 daemon_ctl.py。
> 本腳本僅提供資料庫診斷與維護功能。

用法:
  python3 conv_admin.py --show-dirs
  python3 conv_admin.py --stats
  python3 conv_admin.py --retry-failed
  python3 conv_admin.py --cleanup-stale [--stale-hours N]
  python3 conv_admin.py --reset-maxed-failed [--max-retries N]
"""
import os
import argparse
from pathlib import Path
from datetime import datetime, timedelta

from db_manager import db_manager
from dotenv import load_dotenv

load_dotenv()

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
        db_manager.execute_query(
            """UPDATE conversion_tasks
               SET status='pending', is_processing=FALSE,
                   error_message=CONCAT('Retry #',%s,': ',COALESCE(error_message,''))
               WHERE id=%s""",
            (t['retry_count'], t['id'])
        )
    print(f"Retried {len(tasks)} task(s) (max_retries={max_retries}).")

def cmd_reset_maxed_failed(max_retries=3):
    tasks = db_manager.execute_query(
        """SELECT id, input_path, retry_count, error_message
           FROM conversion_tasks
           WHERE status='failed' AND retry_count >= %s
           ORDER BY updated_at DESC""",
        (max_retries,), fetch=True
    )
    if not tasks:
        print(f"No failed tasks with retry_count >= {max_retries}.")
        return
    print(f"Found {len(tasks)} task(s) with retry_count >= {max_retries}:")
    for t in tasks:
        print(f"  [{t['id']}] {Path(t['input_path']).name}  (retries={t['retry_count']})")
    confirm = input(f"\nReset all {len(tasks)} task(s) to pending with retry_count=0? [y/N] ").strip().lower()
    if confirm != 'y':
        print("Aborted.")
        return
    ids = [t['id'] for t in tasks]
    placeholders = ','.join(['%s'] * len(ids))
    db_manager.execute_query(
        f"""UPDATE conversion_tasks
            SET status='pending', is_processing=FALSE,
                retry_count=0,
                error_message=CONCAT('Reset from maxed-failed: ', COALESCE(error_message,''))
            WHERE id IN ({placeholders})""",
        tuple(ids)
    )
    print(f"Reset {len(tasks)} task(s) to pending.")


def cmd_cleanup_stale(hours=24):
    stale_time = (datetime.now() - timedelta(hours=hours)).strftime('%Y-%m-%d %H:%M:%S')
    tasks = db_manager.execute_query(
        """SELECT id FROM conversion_tasks
           WHERE status='processing' AND is_processing=TRUE
           AND COALESCE(start_time, updated_at, created_at) < %s""",
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
        description='bcvnas-converter 資料庫診斷與維護工具',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
範例:
  python3 conv_admin.py --show-dirs
  python3 conv_admin.py --stats
  python3 conv_admin.py --retry-failed
  python3 conv_admin.py --cleanup-stale --stale-hours 2
  python3 conv_admin.py --reset-maxed-failed
  python3 conv_admin.py --reset-maxed-failed --max-retries 5
"""
    )

    action = parser.add_mutually_exclusive_group(required=True)
    action.add_argument('--show-dirs',          action='store_true', help='預覽目錄結構')
    action.add_argument('--stats',               action='store_true', help='顯示任務統計')
    action.add_argument('--retry-failed',        action='store_true', help='手動重試失敗任務')
    action.add_argument('--cleanup-stale',       action='store_true', help='手動清除過時任務')
    action.add_argument('--reset-maxed-failed',  action='store_true', help='重設已達重試上限的失敗任務為 pending（retry_count 歸零）')

    parser.add_argument('--stale-hours', type=float, default=24,
                        help='過時任務的時間閾值（小時，預設 24，僅用於 --cleanup-stale）')
    parser.add_argument('--max-retries', type=int, default=3,
                        help='最大重試次數（預設 3，僅用於 --retry-failed）')
    return parser.parse_args()


def main():
    args = parse_arguments()

    if args.show_dirs:
        cmd_show_dirs()
    elif args.stats:
        cmd_stats()
    elif args.retry_failed:
        cmd_retry_failed(args.max_retries)
    elif args.cleanup_stale:
        cmd_cleanup_stale(args.stale_hours)
    elif args.reset_maxed_failed:
        cmd_reset_maxed_failed(args.max_retries)


if __name__ == '__main__':
    main()
