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
  python3 conv_admin.py --reset-task ID [ID ...]
  python3 conv_admin.py --add-file FILE [FILE ...]
  python3 conv_admin.py --kill-stale-ffmpeg [--dry-run]
"""
import os
import signal
import argparse
from pathlib import Path

from task_manager import TaskRepository
from converter import get_video_info
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
    task_repo = TaskRepository()
    s = task_repo.get_task_statistics()
    if not s:
        print("No data in conversion_tasks.")
        return
    print("=== Task Statistics ===")
    print(f"  Total      : {s['total']}")
    print(f"  Pending    : {s['pending']}")
    print(f"  Processing : {s['processing']}")
    print(f"  Completed  : {s['completed']}")
    print(f"  Failed     : {s['failed']}")
    print(f"  Retried    : {s['retried'] or 0}")
    if s['avg_duration']:
        print(f"  Avg time   : {float(s['avg_duration']) / 60:.1f} min")

    if s['failed'] and int(s['failed']) > 0:
        print("\n  Recent failed tasks (up to 5):")
        for t in task_repo.get_recent_failed_tasks(5):
            print(f"    [{t['id']}] {Path(t['input_path']).name}")
            print(f"          error={t['error_message']}  retries={t['retry_count']}  at={t['updated_at']}")

# ---------------------------------------------------------------------------
# Retry failed / cleanup stale  (one-shot maintenance)
# ---------------------------------------------------------------------------

def cmd_retry_failed(max_retries=3):
    task_repo = TaskRepository()
    count = task_repo.retry_failed_tasks(max_retries=max_retries)
    if count == 0:
        print("No failed tasks eligible for retry.")
    else:
        print(f"Retried {count} task(s) (max_retries={max_retries}).")

def cmd_reset_maxed_failed(max_retries=3):
    task_repo = TaskRepository()
    tasks = task_repo.get_maxed_failed_tasks(max_retries)
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
    task_repo.reset_tasks_to_pending([t['id'] for t in tasks], reason='reset-maxed-failed')
    print(f"Reset {len(tasks)} task(s) to pending.")


def cmd_reset_task(task_ids):
    task_repo = TaskRepository()
    found = []
    for tid in task_ids:
        t = task_repo.get_task_detail(tid)
        if t is None:
            print(f"  ⚠ Task {tid} not found, skipping.")
        else:
            found.append(tid)
            print(f"  [{t['id']}] status={t['status']}  retries={t['retry_count']}  "
                  f"{Path(t['input_path']).name}")
            if t.get('error_message'):
                print(f"        error: {t['error_message'][:100]}")

    if not found:
        print("No valid task IDs to reset.")
        return

    count = task_repo.reset_tasks_to_pending(found, reason='manual reset via --reset-task')
    print(f"\nReset {count} task(s) to pending (retry_count=0).")


def cmd_cleanup_stale(hours=24):
    task_repo = TaskRepository()
    count = task_repo.cleanup_stale_tasks(stale_hours=hours)
    if count == 0:
        print(f"No stale tasks (>{hours}h in processing).")
    else:
        print(f"Cleaned up {count} stale task(s) (>{hours}h in processing).")

# ---------------------------------------------------------------------------
# Kill orphaned ffmpeg processes
# ---------------------------------------------------------------------------

def _get_process_daemon_descendant_pids():
    """
    讀取 process daemon PID 檔，回傳其所有子孫 PID 集合（含自身）。
    若 daemon 未執行或 psutil 不可用，回傳空集合。
    """
    try:
        import psutil
    except ImportError:
        return set()

    pid_file = Path(os.getenv('PROCESS_DAEMON_PID_FILE',
                              '/var/run/video-converter/processor.pid'))
    if not pid_file.exists():
        return set()

    try:
        daemon_pid = int(pid_file.read_text().strip())
        proc = psutil.Process(daemon_pid)
        pids = {proc.pid}
        for child in proc.children(recursive=True):
            pids.add(child.pid)
        return pids
    except (ValueError, psutil.NoSuchProcess, psutil.AccessDenied):
        return set()


def cmd_kill_stale_ffmpeg(dry_run=False):
    try:
        import psutil
    except ImportError:
        print("psutil 未安裝，無法掃描 ffmpeg 程序。請執行: pip install psutil")
        return

    task_repo = TaskRepository()
    daemon_pids = _get_process_daemon_descendant_pids()
    if daemon_pids:
        print(f"Process daemon running (PID {next(iter(daemon_pids))}), "
              f"excluding {len(daemon_pids)} descendant PID(s) from kill list.")
    else:
        print("Process daemon not running (or PID file not found); all ffmpeg processes are candidates.")

    killed = 0
    skipped = 0

    for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
        try:
            if proc.info['name'] != 'ffmpeg':
                continue
            if proc.pid in daemon_pids:
                skipped += 1
                continue

            cmdline = proc.info['cmdline'] or []
            input_path = None
            for idx, arg in enumerate(cmdline):
                if arg == '-i' and idx + 1 < len(cmdline):
                    input_path = cmdline[idx + 1]
                    break

            if not input_path:
                continue

            task = task_repo.get_task_by_input_path(input_path)
            if task is None:
                continue

            # Only kill if the task is in an active state; skip completed/failed
            # to avoid killing unrelated ffmpeg processes using the same source file.
            if task.get('status') not in ('pending', 'processing'):
                continue

            # Double-check status right before kill to close the TOCTOU window
            # (task may have completed between the first check and now).
            task = task_repo.get_task_by_input_path(input_path)
            if task is None or task.get('status') not in ('pending', 'processing'):
                continue

            print(f"  {'[DRY-RUN] ' if dry_run else ''}Kill ffmpeg PID {proc.pid} "
                  f"(task_id={task['id']}, status={task.get('status','?')}, "
                  f"input={input_path})")
            if not dry_run:
                try:
                    os.kill(proc.pid, signal.SIGKILL)
                    killed += 1
                except ProcessLookupError:
                    print(f"    (already gone)")

        except (psutil.NoSuchProcess, psutil.AccessDenied):
            continue

    if dry_run:
        print("Dry-run complete. Use without --dry-run to actually kill.")
    else:
        if killed == 0:
            print("No orphaned ffmpeg processes found.")
        else:
            print(f"Killed {killed} orphaned ffmpeg process(es).")
    if skipped:
        print(f"Skipped {skipped} ffmpeg process(es) under process daemon.")

# ---------------------------------------------------------------------------
# Add specific file to conversion database
# ---------------------------------------------------------------------------

def cmd_add_file(file_paths):
    input_dir  = Path(os.getenv('INPUT_DIRECTORY', '')).resolve()
    output_dir = Path(os.getenv('OUTPUT_DIRECTORY', '')).resolve()
    supported  = set(e.strip().lower() for e in
                     os.getenv('SUPPORTED_EXTENSIONS',
                               '.mp4,.mkv,.avi,.mov,.mts,.mxf,.mpg').split(','))

    task_repo = TaskRepository()
    added = 0
    skipped = 0

    for raw_path in file_paths:
        file_path = Path(raw_path).resolve()

        if not file_path.exists():
            print(f"  ❌ File not found: {file_path}")
            skipped += 1
            continue

        if not file_path.is_file():
            print(f"  ❌ Not a file: {file_path}")
            skipped += 1
            continue

        if file_path.suffix.lower() not in supported:
            print(f"  ❌ Unsupported extension '{file_path.suffix}': {file_path.name}")
            skipped += 1
            continue

        # Compute output path the same way scan_daemon does
        orig_suffix = file_path.suffix[1:].lower()
        out_name = f"480p_{file_path.stem}.mp4" if orig_suffix == "mp4" else f"480p_{file_path.stem}_{orig_suffix}.mp4"
        try:
            relative = file_path.relative_to(input_dir)
            out_path = output_dir / relative.parent / out_name
        except ValueError:
            # File is outside INPUT_DIRECTORY — place output alongside input file
            out_path = file_path.parent / out_name

        # Probe resolution via ffprobe
        video_info = get_video_info(str(file_path))
        if not video_info:
            print(f"  ❌ Cannot probe video info: {file_path.name}")
            skipped += 1
            continue

        rows = task_repo.insert_task(str(file_path), str(out_path), video_info['resolution'])
        if rows > 0:
            print(f"  ✅ Added  [{video_info['resolution']}] {file_path.name}")
            print(f"           → {out_path}")
            added += 1
        else:
            # INSERT IGNORE silently skipped — already exists
            existing = task_repo.get_task_by_input_path(str(file_path))
            status = existing.get('status', '?') if existing else '?'
            print(f"  ⚠ Already in DB (status={status}): {file_path.name}")
            skipped += 1

    print(f"\nDone. Added {added}, skipped {skipped}.")


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
  python3 conv_admin.py --reset-task 123 456 789
  python3 conv_admin.py --add-file /BCVNAS/path/to/video.mp4
  python3 conv_admin.py --add-file /BCVNAS/a.mp4 /BCVNAS/b.mkv
  python3 conv_admin.py --kill-stale-ffmpeg --dry-run
  python3 conv_admin.py --kill-stale-ffmpeg
"""
    )

    action = parser.add_mutually_exclusive_group(required=True)
    action.add_argument('--show-dirs',          action='store_true', help='預覽目錄結構')
    action.add_argument('--stats',               action='store_true', help='顯示任務統計')
    action.add_argument('--retry-failed',        action='store_true', help='手動重試失敗任務')
    action.add_argument('--cleanup-stale',       action='store_true', help='手動清除過時任務')
    action.add_argument('--reset-maxed-failed',  action='store_true', help='重設已達重試上限的失敗任務為 pending（retry_count 歸零）')
    action.add_argument('--reset-task',          nargs='+', type=int, metavar='ID',
                        help='重設指定 task ID 為 pending（retry_count 歸零）')
    action.add_argument('--add-file',            nargs='+', metavar='FILE',
                        help='手動新增指定影片檔至轉檔資料庫')
    action.add_argument('--kill-stale-ffmpeg',   action='store_true', help='Kill 不在 process daemon 下且 source file 有 DB 記錄的孤兒 ffmpeg 程序')

    parser.add_argument('--stale-hours', type=float, default=24,
                        help='過時任務的時間閾值（小時，預設 24，僅用於 --cleanup-stale）')
    parser.add_argument('--max-retries', type=int, default=3,
                        help='最大重試次數（預設 3，僅用於 --retry-failed）')
    parser.add_argument('--dry-run', action='store_true',
                        help='僅列出會被 kill 的程序，不實際執行（僅用於 --kill-stale-ffmpeg）')
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
    elif args.reset_task:
        cmd_reset_task(args.reset_task)
    elif args.add_file:
        cmd_add_file(args.add_file)
    elif args.kill_stale_ffmpeg:
        cmd_kill_stale_ffmpeg(dry_run=args.dry_run)


if __name__ == '__main__':
    main()
