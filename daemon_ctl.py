#!/usr/bin/env python3
"""
Daemon 統一管理腳本

用法:
  python daemon_ctl.py <target> [command] [options]

target:
  scan      掃描 daemon
  process   處理 daemon
  api       API 伺服器
  all       同時操作 scan 和 process（不含 api）
            ※ status 指令會額外顯示 api 狀態

command（預設 start）:
  start     啟動
  stop      停止
  restart   重新啟動
  status    顯示狀態
  log       查閱 log 檔（預設開啟末尾，可往上捲；all 同時開啟所有）

選項:
  --foreground, -f   在前景執行（start/restart 用）
  --follow,    -f    持續追蹤新增內容（log 指令用，Ctrl+C 可切回捲動）
  --error,     -e    查閱 error log（log 指令用）

範例:
  python daemon_ctl.py scan
  python daemon_ctl.py process start --foreground
  python daemon_ctl.py scan log           # 查閱 scan log（可上下捲）
  python daemon_ctl.py process log -f     # 追蹤 process log
  python daemon_ctl.py api log -e         # 查閱 api error log
  python daemon_ctl.py all log            # 同時開啟所有 log（:n/:p 切換）
  python daemon_ctl.py status             # 等同 all status
  python daemon_ctl.py start              # 等同 all start
  python daemon_ctl.py stop               # 等同 all stop
  python daemon_ctl.py restart            # 等同 all restart
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
# API server 管理（不透過 BaseDaemon，用 PID 檔自行管理）
# ---------------------------------------------------------------------------

def _api_pid_file():
    run_dir = os.getenv('VIDEO_CONVERTER_RUN_DIR', './run')
    return Path(run_dir) / 'api.pid'


def _api_log_file():
    return Path(os.getenv('API_SERVER_LOG_FILE', './log/api.log'))


def _api_error_log_file():
    return Path(os.getenv('API_SERVER_ERROR_LOG_FILE', './log/api_error.log'))


def _read_api_pid():
    pid_file = _api_pid_file()
    try:
        pid = int(pid_file.read_text().strip())
        # 確認程序仍在執行
        os.kill(pid, 0)
        return pid
    except (FileNotFoundError, ValueError, ProcessLookupError, PermissionError):
        return None


def cmd_api_start(foreground=False):
    if _read_api_pid():
        print("api_server is already running.")
        return

    pid_file = _api_pid_file()
    log_file = _api_log_file()
    err_file = _api_error_log_file()

    print(f"Starting api_server in {'foreground' if foreground else 'background'} mode")
    print(f"PID file: {pid_file}")
    print(f"Log file: {log_file}")
    print(f"Error log file: {err_file}")

    if foreground:
        # 前景模式：寫入 PID 檔後直接啟動
        print(f"PID: {os.getpid()}")
        pid_file.parent.mkdir(parents=True, exist_ok=True)
        pid_file.write_text(str(os.getpid()))
        try:
            from api.server import start_api_server
            start_api_server()
        finally:
            pid_file.unlink(missing_ok=True)
        return

    # 背景啟動：用 subprocess.Popen 啟動 --foreground 子程序（子程序自己寫 PID 檔）
    import subprocess
    log_file.parent.mkdir(parents=True, exist_ok=True)
    err_file.parent.mkdir(parents=True, exist_ok=True)
    pid_file.parent.mkdir(parents=True, exist_ok=True)

    script = str(Path(__file__).resolve())
    with open(log_file, 'a') as out, open(err_file, 'a') as err:
        proc = subprocess.Popen(
            [sys.executable, script, 'api', 'start', '--foreground'],
            stdout=out,
            stderr=err,
            start_new_session=True,
        )

    # 等子程序寫入 PID 檔（最多等 5 秒）
    for _ in range(10):
        time.sleep(0.5)
        try:
            pid = pid_file.read_text().strip()
            print(f"PID: {pid}")
            return
        except FileNotFoundError:
            pass
    print("Warning: api_server may not have started correctly. Check log for details.")


def cmd_api_stop():
    pid = _read_api_pid()
    if not pid:
        print("api_server is not running.")
        return
    import signal as _signal
    print(f"Stopping api_server (PID: {pid})...")
    try:
        os.kill(pid, _signal.SIGTERM)
        for _ in range(10):
            time.sleep(1)
            try:
                os.kill(pid, 0)
            except ProcessLookupError:
                break
        else:
            os.kill(pid, _signal.SIGKILL)
            print("api_server force killed.")
        pid_file = _api_pid_file()
        pid_file.unlink(missing_ok=True)
        print("api_server stopped successfully.")
    except ProcessLookupError:
        print("api_server already stopped.")
        _api_pid_file().unlink(missing_ok=True)


def cmd_api_restart(foreground=False):
    cmd_api_stop()
    time.sleep(1)
    cmd_api_start(foreground)


def cmd_api_status():
    pid = _read_api_pid()
    host = os.getenv('API_SERVER_HOST', '0.0.0.0')
    port = os.getenv('API_SERVER_PORT', '5000')
    if pid:
        print(f"✅ api_server: running (PID: {pid})")
        print(f"   Endpoint   : http://{host}:{port}")
    else:
        print(f"❌ api_server: stopped")


# ---------------------------------------------------------------------------
# Log 查閱
# ---------------------------------------------------------------------------

def _log_files_for(target, error=False):
    """回傳 target 對應的 log 檔路徑列表（error=True 取 error log）"""
    files = []

    if target in ('scan', 'all'):
        d = make_scan_daemon()
        files.append(Path(d.stderr_log_file if error else d.log_file))

    if target in ('process', 'all'):
        d = make_process_daemon()
        files.append(Path(d.stderr_log_file if error else d.log_file))

    if target in ('api', 'all'):
        files.append(_api_error_log_file() if error else _api_log_file())

    return files


def cmd_log(target, follow=False, error=False):
    """用 less 開啟指定 daemon 的 log 檔。
    - 預設：less +G（跳至末尾，可自由上下捲）
    - follow=True：less +F（持續追蹤，Ctrl+C 可切回捲動模式，F 繼續追蹤）
    - error=True：開啟 error log
    - all：同時開啟多個檔案（:n / :p 切換）
    """
    import shutil
    import subprocess

    log_files = _log_files_for(target, error=error)
    if not log_files:
        print(f"Unknown target: {target!r}")
        sys.exit(1)

    # 確保檔案存在（尚未產生的建立空檔）
    existing = []
    for f in log_files:
        if not f.exists():
            print(f"⚠️  Log file not found: {f}")
        else:
            existing.append(str(f))

    if not existing:
        print("No log files available.")
        return

    less = shutil.which('less')
    if not less:
        # fallback：直接印出最後 50 行
        for path in existing:
            print(f"\n=== {path} ===")
            lines = Path(path).read_text(errors='replace').splitlines()
            print('\n'.join(lines[-50:]))
        return

    # +F：follow 模式（tail -f 風格，但可 Ctrl+C 切回捲動）
    # +G：跳至末尾（預設，方便往上查）
    # -R：支援 ANSI 顏色
    # -S：長行不折行（log 常有很長的一行）
    flag = '+F' if follow else '+G'
    cmd = [less, '-R', '-S', flag] + existing

    # 在 less +F 模式下，Ctrl+C 中斷可能導致 terminal 停在 raw mode（無回顯）。
    # 用 termios 事先備份 terminal 屬性，離開後無論正常或中斷都還原。
    import termios
    fd = sys.stdin.fileno() if sys.stdin.isatty() else None
    saved_tty = None
    if fd is not None:
        try:
            saved_tty = termios.tcgetattr(fd)
        except termios.error:
            pass

    try:
        subprocess.run(cmd)
    except KeyboardInterrupt:
        pass
    finally:
        if saved_tty is not None:
            try:
                termios.tcsetattr(fd, termios.TCSADRAIN, saved_tty)
            except termios.error:
                pass


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
Usage: python daemon_ctl.py <target> [command] [options]

  target:   scan | process | api | all (scan+process only)
  command:  start (default) | stop | restart | status | log
  -f / --foreground   run in foreground (start/restart)
  -f / --follow       follow log output (log)
  -e / --error        view error log (log)
"""


def main():
    args = sys.argv[1:]

    if not args or args[0] in ('-h', '--help'):
        print(__doc__)
        sys.exit(0)

    follow = '-f' in args or '--follow' in args
    error  = '-e' in args or '--error' in args
    foreground = '-f' in args or '--foreground' in args
    args = [a for a in args if a not in ('--foreground', '-f', '--follow', '--error', '-e')]

    # `daemon_ctl.py <command>` → 等同 `all <command>`
    if len(args) == 1 and args[0] in ('start', 'stop', 'restart', 'status', 'log'):
        args = ['all', args[0]]

    target = args[0]
    command = args[1] if len(args) > 1 else 'start'

    if target not in ('scan', 'process', 'api', 'all'):
        print(f"Unknown target: {target!r}\n{USAGE}")
        sys.exit(1)

    if command not in ('start', 'stop', 'restart', 'status', 'log'):
        print(f"Unknown command: {command!r}\n{USAGE}")
        sys.exit(1)

    # log 指令
    if command == 'log':
        cmd_log(target, follow=follow, error=error)
        return

    # 'all' + foreground 不合理（多個 foreground 無法同時跑）
    if target == 'all' and foreground and command in ('start', 'restart'):
        print("Error: --foreground cannot be used with target 'all'")
        sys.exit(1)

    # api target 直接處理
    if target == 'api':
        if command == 'start':
            cmd_api_start(foreground)
        elif command == 'stop':
            cmd_api_stop()
        elif command == 'restart':
            cmd_api_restart(foreground)
        elif command == 'status':
            cmd_api_status()
        return

    # all = scan + process（不含 api）
    targets = ['scan', 'process'] if target == 'all' else [target]

    # start/restart 用 subprocess 各自啟動，避免 DaemonContext double-fork 吃掉父程序
    if target == 'all' and command in ('start', 'restart'):
        import subprocess
        script = str(Path(__file__).resolve())
        for t in targets:
            proc = subprocess.Popen([sys.executable, script, t, command])
            proc.wait()
        return

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

    if target == 'all' and command == 'status':
        cmd_api_status()


if __name__ == '__main__':
    main()
