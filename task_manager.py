"""
task_manager — 任務資料庫操作的共用函式庫

process_daemon 與 conv_admin 共用的 DB 操作集中在此，
避免在多處維護相同的 SQL 邏輯。
"""
import logging
from datetime import datetime, timedelta

from db_manager import db_manager


class TaskRepository:
    """任務資料庫操作的統一入口。

    接受 optional logger；若未提供則使用模組預設 logger，
    方便 daemon（有 logger）與 CLI 工具（無 logger）共用。
    """

    def __init__(self, logger=None):
        """初始化 TaskRepository。

        Args:
            logger: 自訂 logger；若為 None 則使用模組預設 logger。
                    測試時可注入 mock logger 以捕捉 log 輸出。
        """
        self._logger = logger or logging.getLogger(__name__)

    # ------------------------------------------------------------------
    # Read operations
    # ------------------------------------------------------------------

    def get_pending_tasks(self, limit=100):
        """回傳待處理任務清單（status='pending' 且 is_processing=FALSE）"""
        try:
            query = '''
            SELECT id, input_path, output_path, source_resolution
            FROM conversion_tasks
            WHERE status = 'pending'
            AND is_processing = FALSE
            ORDER BY retry_count ASC, created_at ASC
            LIMIT %s
            '''
            return db_manager.execute_query(query, (limit,), fetch=True)
        except Exception as e:
            self._logger.error(f"Error getting pending tasks: {str(e)}")
            # 回傳空列表：check cycle 本輪跳過派工，下次輪詢自動重試
            return []

    def get_task_by_id(self, task_id):
        """以 task_id 取得單一任務詳細資訊；找不到時回傳 None"""
        try:
            result = db_manager.execute_query(
                "SELECT input_path, output_path FROM conversion_tasks WHERE id = %s",
                (task_id,), fetch=True
            )
            return result[0] if result else None
        except Exception as e:
            self._logger.error(f"Error getting task {task_id}: {str(e)}")
            return None

    def get_task_detail(self, task_id):
        """以 task_id 取得完整任務資訊（含 status, retry_count, error_message）；找不到時回傳 None"""
        try:
            result = db_manager.execute_query(
                "SELECT id, input_path, output_path, status, retry_count, error_message "
                "FROM conversion_tasks WHERE id = %s",
                (task_id,), fetch=True
            )
            return result[0] if result else None
        except Exception as e:
            self._logger.error(f"Error getting task detail {task_id}: {str(e)}")
            return None

    def get_full_task_info(self, task_id):
        """以 task_id 取得所有欄位的任務資訊；找不到時回傳 None"""
        try:
            result = db_manager.execute_query(
                "SELECT id, input_path, output_path, source_resolution, target_resolution, "
                "status, progress, is_processing, start_time, end_time, "
                "error_message, retry_count, created_at, updated_at "
                "FROM conversion_tasks WHERE id = %s",
                (task_id,), fetch=True
            )
            return result[0] if result else None
        except Exception as e:
            self._logger.error(f"Error getting full task info {task_id}: {str(e)}")
            return None

    def get_task_statistics(self):
        """查詢任務統計資訊，回傳 dict；查詢失敗時回傳 None"""
        try:
            avg_dur = db_manager.dialect.timestampdiff_seconds('start_time', 'end_time')
            query = f"""
            SELECT
                COUNT(*) AS total,
                SUM(status = 'pending')    AS pending,
                SUM(status = 'processing') AS processing,
                SUM(status = 'completed')  AS completed,
                SUM(status = 'failed')     AS failed,
                SUM(retry_count > 0)       AS retried,
                AVG(CASE WHEN status IN ('completed','failed')
                    THEN {avg_dur} END) AS avg_duration
            FROM conversion_tasks
            """
            rows = db_manager.execute_query(query, fetch=True)
            return rows[0] if rows else None
        except Exception as e:
            self._logger.error(f"Error getting task statistics: {str(e)}")
            return None

    def get_task_by_input_path(self, input_path):
        """以 input_path 查詢任務，回傳包含 id/status/output_path 的 dict；找不到時回傳 None"""
        try:
            result = db_manager.execute_query(
                "SELECT id, status, output_path FROM conversion_tasks WHERE input_path = %s LIMIT 1",
                (input_path,), fetch=True
            )
            return result[0] if result else None
        except Exception as e:
            self._logger.error(f"Error querying task by input_path: {str(e)}")
            return None

    def requeue_missing_output(self, input_path):
        """將 completed 但輸出檔遺失的任務重置為 pending"""
        try:
            db_manager.execute_query(
                "UPDATE conversion_tasks SET status='pending', is_processing=FALSE, "
                "error_message='Output file missing, re-queued by scanner' "
                "WHERE input_path=%s",
                (input_path,)
            )
        except Exception as e:
            self._logger.error(f"Error re-queuing task for {input_path}: {str(e)}")

    def insert_task(self, input_path, output_path, resolution):
        """INSERT IGNORE 新增轉檔任務，回傳 rows affected（0 表示已存在）"""
        try:
            return db_manager.execute_query(
                '''INSERT IGNORE INTO conversion_tasks
                   (input_path, output_path, source_resolution, status)
                   VALUES (%s, %s, %s, 'pending')''',
                (input_path, output_path, resolution)
            )
        except Exception as e:
            self._logger.error(f"Error inserting task for {input_path}: {str(e)}")
            return 0

    def get_maxed_failed_tasks(self, max_retries=3):
        """回傳 retry_count >= max_retries 的失敗任務清單"""
        try:
            return db_manager.execute_query(
                """SELECT id, input_path, retry_count, error_message
                   FROM conversion_tasks
                   WHERE status='failed' AND retry_count >= %s
                   ORDER BY updated_at DESC""",
                (max_retries,), fetch=True
            )
        except Exception as e:
            self._logger.error(f"Error querying maxed failed tasks: {str(e)}")
            return []

    def get_processing_tasks(self):
        """回傳目前正在轉檔（status='processing'）的任務清單，依 start_time 升冪排列。"""
        try:
            return db_manager.execute_query(
                """SELECT id, input_path, start_time, retry_count
                   FROM conversion_tasks
                   WHERE status='processing'
                   ORDER BY start_time ASC""",
                fetch=True
            )
        except Exception as e:
            self._logger.error(f"Error getting processing tasks: {str(e)}")
            return []

    def get_recent_failed_tasks(self, limit=5):
        """回傳最近失敗任務清單"""
        try:
            return db_manager.execute_query(
                """SELECT id, input_path, error_message, retry_count, updated_at
                   FROM conversion_tasks WHERE status='failed'
                   ORDER BY updated_at DESC LIMIT %s""",
                (limit,), fetch=True
            )
        except Exception as e:
            self._logger.error(f"Error getting recent failed tasks: {str(e)}")
            return []

    # ------------------------------------------------------------------
    # Write operations — task status
    # ------------------------------------------------------------------

    def update_task_status(self, task_id, status, progress=None, error_message=None):
        """更新任務狀態。

        - status='failed'    時自動遞增 retry_count 並清除 is_processing
        - status='completed' 時清除 is_processing
        - progress 限制在 [0, 100]；error_message 截斷至 1000 字元
        """
        try:
            updates = []
            params = []

            if status:
                updates.append('status = %s')
                params.append(status)

            if progress is not None:
                updates.append('progress = %s')
                params.append(min(100.0, max(0.0, progress)))

            if error_message:
                updates.append('error_message = %s')
                params.append(error_message[:1000])

            if status in ['completed', 'failed']:
                updates.append('end_time = CURRENT_TIMESTAMP')
                # 原子性清除 is_processing 旗標，縮小狀態更新與鎖釋放之間的視窗，
                # 避免程序崩潰時任務卡在 is_processing=TRUE
                updates.append('is_processing = FALSE')

            if status == 'failed':
                updates.append('retry_count = COALESCE(retry_count, 0) + 1')

            if not updates:
                # 呼叫者未傳入任何要更新的欄位，跳過 UPDATE
                return

            query = f"UPDATE conversion_tasks SET {', '.join(updates)} WHERE id = %s"
            params.append(task_id)
            db_manager.execute_query(query, tuple(params))

        except Exception as e:
            self._logger.error(f"Error updating task status: {str(e)}")
            # 靜默失敗：任務留在 processing 狀態，cleanup_stale_tasks() 最終會將其標為 failed

    # ------------------------------------------------------------------
    # Write operations — locking
    # ------------------------------------------------------------------

    def acquire_task_lock(self, task_id, worker_id):
        """原子性取得任務鎖（is_processing=TRUE），回傳是否成功取得。

        資料庫行級鎖保證同一時間只有一個 worker 能成功更新同一列，
        無需額外的應用層鎖。
        """
        try:
            rows_affected = db_manager.execute_query(
                '''UPDATE conversion_tasks
                   SET is_processing = TRUE, status = 'processing', start_time = CURRENT_TIMESTAMP
                   WHERE id = %s AND status = 'pending' AND is_processing = FALSE''',
                (task_id,)
            )
            if rows_affected > 0:
                # 寫入 processing_lock 供追蹤（非并發控制用途，失敗不影響主流程）
                try:
                    db_manager.execute_query(
                        "INSERT IGNORE INTO processing_lock (task_id, worker_id) VALUES (%s, %s)",
                        (task_id, worker_id)
                    )
                except Exception:
                    pass
                return True
            return False
        except Exception as e:
            self._logger.error(f"Error acquiring task lock: {str(e)}")
            # 回傳 False：worker 跳過此任務，任務留在 pending，下輪 check cycle 自動重試
            return False

    def release_task_lock(self, task_id, worker_id):
        """釋放任務鎖：原子性清除 is_processing 旗標並移除 processing_lock 紀錄"""
        try:
            db_manager.execute_transaction([
                ("UPDATE conversion_tasks SET is_processing = FALSE WHERE id = %s", (task_id,)),
                ("DELETE FROM processing_lock WHERE task_id = %s", (task_id,)),
            ])
            return True
        except Exception as e:
            self._logger.error(f"Error releasing task lock: {str(e)}")
            # 回傳 False：is_processing 可能留 TRUE，cleanup_stale_tasks() 超時後負責清理
            return False

    # ------------------------------------------------------------------
    # Maintenance operations
    # ------------------------------------------------------------------

    def retry_failed_tasks(self, max_retries=3, limit=100):
        """將 retry_count < max_retries 的失敗任務重置為 pending，回傳重置數量"""
        try:
            failed_tasks = db_manager.execute_query(
                '''SELECT id, COALESCE(retry_count, 0) AS retry_count
                   FROM conversion_tasks
                   WHERE status = 'failed'
                   AND COALESCE(retry_count, 0) < %s
                   ORDER BY retry_count ASC, created_at ASC
                   LIMIT %s''',
                (max_retries, limit), fetch=True
            )
            if not failed_tasks:
                return 0

            retried = 0
            for task in failed_tasks:
                concat_expr = db_manager.dialect.concat(
                    "'Retry #'", '%s', "': '", "COALESCE(error_message, '')",
                )
                rows = db_manager.execute_query(
                    f'''UPDATE conversion_tasks
                       SET status = 'pending',
                           is_processing = FALSE,
                           error_message = {concat_expr}
                       WHERE id = %s AND status = 'failed' ''',
                    (task['retry_count'], task['id'])
                )
                if rows:
                    retried += 1

            if retried:
                self._logger.info(f"Retried {retried} failed task(s) (max_retries={max_retries})")
            return retried

        except Exception as e:
            self._logger.error(f"Error retrying failed tasks: {str(e)}")
            return 0

    def cleanup_stale_tasks(self, stale_hours=1):
        """將卡在 processing 超過 stale_hours 的任務標記為 failed，回傳清理數量"""
        try:
            stale_time = datetime.now() - timedelta(hours=stale_hours)
            stale_tasks = db_manager.execute_query(
                '''SELECT id FROM conversion_tasks
                   WHERE status = 'processing'
                   AND is_processing = TRUE
                   AND COALESCE(start_time, updated_at, created_at) < %s''',
                (stale_time.strftime('%Y-%m-%d %H:%M:%S'),), fetch=True
            )
            if not stale_tasks:
                return 0

            cleaned = 0
            for task in stale_tasks:
                task_id = task['id']
                # execute_transaction() returns list of rowcounts; check the first
                # (UPDATE) to see if the task was still processing (TOCTOU guard).
                try:
                    rowcounts = db_manager.execute_transaction([
                        ('''UPDATE conversion_tasks
                            SET status = 'failed',
                                is_processing = FALSE,
                                error_message = %s,
                                end_time = CURRENT_TIMESTAMP
                            WHERE id = %s AND status = 'processing' AND is_processing = TRUE''',
                         (f"Task marked as stale after {stale_hours}h (was processing)", task_id)),
                        ("DELETE FROM processing_lock WHERE task_id = %s", (task_id,)),
                    ])
                    if rowcounts[0]:
                        cleaned += 1
                except Exception as e:
                    self._logger.error(f"Error cleaning stale task {task_id}: {str(e)}")
                    # 單一任務失敗不中斷迴圈，繼續清理其他 stale 任務

            if cleaned:
                self._logger.warning(f"Cleaned up {cleaned} stale task(s) (>{stale_hours}h in processing)")
            return cleaned

        except Exception as e:
            self._logger.error(f"Error cleaning up stale tasks: {str(e)}")
            return 0

    def reset_tasks_to_pending(self, task_ids, reason='manual reset'):
        """將指定 task_ids 重置為 pending（retry_count 歸零），回傳實際重置數量"""
        if not task_ids:
            return 0
        try:
            placeholders = ','.join(['%s'] * len(task_ids))
            concat_expr = db_manager.dialect.concat('%s', "COALESCE(error_message,'')")
            rows = db_manager.execute_query(
                f"""UPDATE conversion_tasks
                    SET status='pending', is_processing=FALSE,
                        retry_count=0,
                        error_message={concat_expr}
                    WHERE id IN ({placeholders})""",
                (f'[{reason}] ',) + tuple(task_ids)
            )
            return rows or 0
        except Exception as e:
            self._logger.error(f"Error resetting tasks to pending: {str(e)}")
            return 0

    def cleanup_orphaned_flags(self):
        """清理上次崩潰留下的孤兒任務，回傳清理數量。

        處理兩種殭屍狀態：
        1. status='processing', is_processing=TRUE
           → daemon 崩潰時正在處理，lock 未釋放。
             重設為 pending，讓任務重新排程。
        2. status='processing', is_processing=FALSE, updated_at 超過 5 分鐘
           → release_task_lock() 執行後 daemon 崩潰（update_task_status 未執行），
             lock 已釋放但 status 未更新，任何清理機制都不會觸及此狀態。
             5 分鐘閾值確保不誤殺 release_task_lock 與 update_task_status
             之間的正常毫秒級時間窗口。
        """
        try:
            # 殭屍類型 1：lock 未釋放
            cleaned_locked = db_manager.execute_query(
                "UPDATE conversion_tasks SET is_processing = FALSE, status = 'pending' "
                "WHERE status = 'processing' AND is_processing = TRUE"
            ) or 0

            # 殭屍類型 2：lock 已釋放但 status 未更新（超過 5 分鐘確保不誤殺正常流程）
            stale_expr = db_manager.dialect.interval_ago(5)
            cleaned_unlocked = db_manager.execute_query(
                f"UPDATE conversion_tasks SET status = 'pending' "
                f"WHERE status = 'processing' AND is_processing = FALSE "
                f"AND updated_at < {stale_expr}"
            ) or 0

            total = cleaned_locked + cleaned_unlocked
            if cleaned_unlocked > 0:
                self._logger.warning(
                    f"Cleaned up {cleaned_unlocked} zombie task(s) "
                    f"(status=processing, is_processing=FALSE)"
                )
            return total
        except Exception as e:
            self._logger.error(f"Error cleaning up orphaned flags: {str(e)}")
            return 0

    def get_tasks_for_source_cleanup(self, input_dir_prefix: str):
        """回傳 input_path 位於 input_dir_prefix 下、且非 processing 狀態的所有任務。

        用於 source 刪除清理：processing 任務由 process_daemon 自行處理（
        ffmpeg 失敗後會由 stale 清理機制介入），不在此清理範圍內。

        Args:
            input_dir_prefix: 輸入目錄絕對路徑字串（不含結尾 /）。

        Returns:
            list of dict with keys: id, input_path, output_path, status
        """
        try:
            # 在 LIKE 模式中 escape !、%、_ 三個特殊字元，避免目錄名稱含底線
            # （如 /mnt/nas_input/）誤匹配其他路徑。
            # 使用 ! 作為 ESCAPE 字元，避免 backslash 在 MariaDB 字串中的歧義。
            safe = (
                input_dir_prefix.rstrip('/')
                .replace('!', '!!')
                .replace('%', '!%')
                .replace('_', '!_')
            ) + '/'
            return db_manager.execute_query(
                """SELECT id, input_path, output_path, status
                   FROM conversion_tasks
                   WHERE status != 'processing'
                   AND input_path LIKE %s ESCAPE '!'
                   ORDER BY id ASC""",
                (safe + '%',), fetch=True
            )
        except Exception as e:
            self._logger.error(f"Error querying tasks for source cleanup: {str(e)}")
            return []

    def delete_task(self, task_id: int) -> bool:
        """永久刪除任務記錄（processing_lock 由 FK CASCADE 自動清除）。

        僅刪除非 processing 狀態的任務：在 get_tasks_for_source_cleanup() 查詢
        完成後到此方法執行之間，process_daemon 可能已搶先取得 lock 並將任務轉為
        processing。加上 AND status != 'processing' 守衛可防止刪除正在轉檔的任務。

        不顯式刪除 processing_lock：若在 race 視窗內任務已變為 processing，
        先刪 lock 後 DELETE conversion_tasks 是 no-op，反而留下無 lock row 的孤兒任務；
        使用 ON DELETE CASCADE 確保 lock 只在 task 本身被成功刪除時才一併清除。

        Args:
            task_id: 要刪除的任務 ID。

        Returns:
            True 表示成功刪除，False 表示任務已被 process_daemon 接管或發生錯誤。
        """
        try:
            rowcounts = db_manager.execute_transaction([
                ("DELETE FROM conversion_tasks WHERE id = %s AND status != 'processing'",
                 (task_id,)),
            ])
            # rowcounts[0] 為 conversion_tasks DELETE 影響的列數；
            # 0 表示任務已被 process_daemon 轉為 processing，不應刪除
            return bool(rowcounts[0])
        except Exception as e:
            self._logger.error(f"Error deleting task {task_id}: {str(e)}")
            return False


_ACTIVE_STATUSES = ('pending', 'processing')


def find_orphaned_ffmpeg_candidates(task_repo, excluded_pids):
    """掃描系統中的 ffmpeg 程序，找出應被 kill 的孤兒程序候選清單。

    條件：
    - 不在 excluded_pids（即非 process daemon 的子孫程序）
    - -i 參數指向的 source file 在 DB 中有 pending/processing 的任務
    - 含 TOCTOU 雙重查詢防護（確認 kill 前 status 仍為 active）

    回傳 list of dict：{'pid', 'task_id', 'status', 'input_path'}
    若 psutil 未安裝則回傳空 list。
    """
    try:
        import psutil
    except ImportError:
        return []

    candidates = []
    for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
        try:
            if proc.info['name'] != 'ffmpeg':
                continue
            if proc.pid in excluded_pids:
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
            if task is None or task.get('status') not in _ACTIVE_STATUSES:
                continue

            candidates.append({
                'pid': proc.pid,
                'task_id': task['id'],
                'status': task.get('status'),
                'input_path': input_path,
            })
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            continue

    return candidates
