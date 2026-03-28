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

    def get_task_statistics(self):
        """查詢任務統計資訊，回傳 dict；查詢失敗時回傳 None"""
        try:
            query = """
            SELECT
                COUNT(*) AS total,
                SUM(status = 'pending')    AS pending,
                SUM(status = 'processing') AS processing,
                SUM(status = 'completed')  AS completed,
                SUM(status = 'failed')     AS failed,
                SUM(retry_count > 0)       AS retried,
                AVG(CASE WHEN status IN ('completed','failed')
                    THEN TIMESTAMPDIFF(SECOND, start_time, end_time) END) AS avg_duration
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
                db_manager.execute_query(
                    '''UPDATE conversion_tasks
                       SET status = 'pending',
                           is_processing = FALSE,
                           error_message = CONCAT('Retry #', %s, ': ', COALESCE(error_message, ''))
                       WHERE id = %s''',
                    (task['retry_count'], task['id'])
                )
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
                db_manager.execute_query(
                    '''UPDATE conversion_tasks
                       SET status = 'failed',
                           is_processing = FALSE,
                           error_message = %s,
                           end_time = CURRENT_TIMESTAMP
                       WHERE id = %s''',
                    (f"Task marked as stale after {stale_hours}h (was processing)", task_id)
                )
                db_manager.execute_query(
                    "DELETE FROM processing_lock WHERE task_id = %s", (task_id,)
                )
                cleaned += 1

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
            rows = db_manager.execute_query(
                f"""UPDATE conversion_tasks
                    SET status='pending', is_processing=FALSE,
                        retry_count=0,
                        error_message=CONCAT(%s, COALESCE(error_message,''))
                    WHERE id IN ({placeholders})""",
                (f'[{reason}] ',) + tuple(task_ids)
            )
            return rows or 0
        except Exception as e:
            self._logger.error(f"Error resetting tasks to pending: {str(e)}")
            return 0

    def cleanup_orphaned_flags(self):
        """清理上次崩潰留下的孤兒 is_processing 旗標，回傳清理數量"""
        try:
            cleaned = db_manager.execute_query(
                "UPDATE conversion_tasks SET is_processing = FALSE "
                "WHERE status = 'pending' AND is_processing = TRUE"
            )
            return cleaned or 0
        except Exception as e:
            self._logger.error(f"Error cleaning up orphaned flags: {str(e)}")
            return 0


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

            # Double-check status to close the TOCTOU window
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
