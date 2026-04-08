"""
logging_utils — 共用的 RotatingFileHandler logger 工廠

scan_daemon、process_daemon、api/server 均呼叫此模組建立 logger，
確保輪替行為、格式與環境變數語義完全一致。
"""
import os
import sys
import logging
import logging.handlers
from pathlib import Path

_LOG_FORMATTER = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

_DEFAULT_MAX_BYTES    = 10 * 1024 * 1024  # 10 MB
_DEFAULT_BACKUP_COUNT = 5


def setup_rotating_logger(
    name: str,
    log_file: str,
    error_log_file: str | None = None,
    console: bool = False,
) -> logging.Logger:
    """建立並回傳一個設定好 RotatingFileHandler 的 logger。

    Args:
        name:            logger 名稱（logging.getLogger(name)）。
        log_file:        INFO+ 訊息寫入的檔案路徑；父目錄不存在時自動建立。
        error_log_file:  若提供，ERROR+ 訊息另外寫入此路徑（預設 None）。
        console:         是否加入 stdout handler。
                         daemon 模式下由呼叫端依 sys.stdout 特性決定。

    環境變數（全域生效）：
        LOG_LEVEL        日誌等級（預設 INFO）
        LOG_MAX_BYTES    單檔大小上限（預設 10MB）
        LOG_BACKUP_COUNT 保留舊檔數（預設 5）

    Returns:
        設定完成的 logging.Logger 實例。
    """
    logger = logging.getLogger(name)
    log_level = os.getenv('LOG_LEVEL', 'INFO').upper()
    logger.setLevel(getattr(logging, log_level, logging.INFO))

    # 關閉並移除現有 handler。
    # daemon 模式下 DaemonContext 進入後 fd 已被關閉，handler.close() 可能拋 OSError，
    # 屬於預期情況，直接忽略。
    for handler in logger.handlers[:]:
        try:
            handler.close()
        except OSError:
            pass
    logger.handlers.clear()

    max_bytes    = int(os.getenv('LOG_MAX_BYTES',    str(_DEFAULT_MAX_BYTES)))
    backup_count = int(os.getenv('LOG_BACKUP_COUNT', str(_DEFAULT_BACKUP_COUNT)))

    # 主 log handler（INFO+）
    try:
        Path(log_file).parent.mkdir(parents=True, exist_ok=True)
        file_handler = logging.handlers.RotatingFileHandler(
            log_file, maxBytes=max_bytes, backupCount=backup_count,
        )
        file_handler.setFormatter(_LOG_FORMATTER)
        logger.addHandler(file_handler)
    except Exception as e:
        print(f"Error setting up file handler for {log_file}: {e}")
        # 回退到標準輸出，確保 log 不完全消失
        _add_console_handler(logger)

    # Error log handler（ERROR+）
    if error_log_file:
        try:
            Path(error_log_file).parent.mkdir(parents=True, exist_ok=True)
            error_handler = logging.handlers.RotatingFileHandler(
                error_log_file, maxBytes=max_bytes, backupCount=backup_count,
            )
            error_handler.setLevel(logging.ERROR)
            error_handler.setFormatter(_LOG_FORMATTER)
            logger.addHandler(error_handler)
        except Exception as e:
            print(f"Error setting up error handler for {error_log_file}: {e}")

    # Console handler（呼叫端決定是否加入）
    if console:
        _add_console_handler(logger)

    return logger


def _add_console_handler(logger: logging.Logger) -> None:
    """在 logger 上加入 stdout StreamHandler（內部輔助函式）。

    若已存在 console StreamHandler 則跳過，避免重複呼叫累積 handler。
    """
    for h in logger.handlers:
        if isinstance(h, logging.StreamHandler) and not isinstance(h, logging.FileHandler):
            return  # 已有 console handler，不重複加入
    try:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(_LOG_FORMATTER)
        logger.addHandler(handler)
    except Exception as e:
        print(f"Error setting up console handler: {e}")
