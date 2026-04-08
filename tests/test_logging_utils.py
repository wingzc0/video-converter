"""
Unit tests for logging_utils.py
測試 setup_rotating_logger() 的各種參數與環境變數，
以及 error_log_file、console、LOG_LEVEL、fallback 行為。
"""
import os
import sys
import logging
import logging.handlers
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch, MagicMock

sys.path.insert(0, str(Path(__file__).parent.parent))

from logging_utils import setup_rotating_logger, _add_console_handler


def _tmp_log(tmp_dir, name='test.log'):
    return str(Path(tmp_dir) / name)


class TestSetupRotatingLoggerBasic(unittest.TestCase):
    """基本行為：RotatingFileHandler 建立與 env var 讀取"""

    def setUp(self):
        self.tmp = tempfile.mkdtemp()

    def tearDown(self):
        # 清理 logger，避免 handler 洩漏至其他測試
        logger = logging.getLogger('test_basic')
        for h in logger.handlers[:]:
            try:
                h.close()
            except OSError:
                pass
        logger.handlers.clear()

    def test_returns_rotating_file_handler(self):
        """應建立至少一個 RotatingFileHandler"""
        logger = setup_rotating_logger('test_basic', _tmp_log(self.tmp))
        rotating = [h for h in logger.handlers
                    if isinstance(h, logging.handlers.RotatingFileHandler)]
        self.assertGreater(len(rotating), 0)

    def test_default_max_bytes(self):
        """預設 LOG_MAX_BYTES 應為 10MB"""
        with patch.dict('os.environ', {}, clear=False):
            os.environ.pop('LOG_MAX_BYTES', None)
            logger = setup_rotating_logger('test_basic', _tmp_log(self.tmp))
        handler = next(h for h in logger.handlers
                       if isinstance(h, logging.handlers.RotatingFileHandler))
        self.assertEqual(handler.maxBytes, 10 * 1024 * 1024)

    def test_default_backup_count(self):
        """預設 LOG_BACKUP_COUNT 應為 5"""
        with patch.dict('os.environ', {}, clear=False):
            os.environ.pop('LOG_BACKUP_COUNT', None)
            logger = setup_rotating_logger('test_basic', _tmp_log(self.tmp))
        handler = next(h for h in logger.handlers
                       if isinstance(h, logging.handlers.RotatingFileHandler))
        self.assertEqual(handler.backupCount, 5)

    def test_custom_max_bytes_env(self):
        """LOG_MAX_BYTES 應正確套用"""
        with patch.dict('os.environ', {'LOG_MAX_BYTES': '5242880'}):
            logger = setup_rotating_logger('test_basic', _tmp_log(self.tmp))
        handler = next(h for h in logger.handlers
                       if isinstance(h, logging.handlers.RotatingFileHandler))
        self.assertEqual(handler.maxBytes, 5 * 1024 * 1024)

    def test_custom_backup_count_env(self):
        """LOG_BACKUP_COUNT 應正確套用"""
        with patch.dict('os.environ', {'LOG_BACKUP_COUNT': '3'}):
            logger = setup_rotating_logger('test_basic', _tmp_log(self.tmp))
        handler = next(h for h in logger.handlers
                       if isinstance(h, logging.handlers.RotatingFileHandler))
        self.assertEqual(handler.backupCount, 3)

    def test_clears_existing_handlers(self):
        """重複呼叫不應累積 handler"""
        log_file = _tmp_log(self.tmp)
        setup_rotating_logger('test_basic', log_file)
        setup_rotating_logger('test_basic', log_file)
        logger = logging.getLogger('test_basic')
        # 每次呼叫只保留一個 RotatingFileHandler
        rotating = [h for h in logger.handlers
                    if isinstance(h, logging.handlers.RotatingFileHandler)]
        self.assertEqual(len(rotating), 1)

    def test_creates_parent_directory(self):
        """不存在的父目錄應自動建立"""
        nested = str(Path(self.tmp) / 'deep' / 'nested' / 'test.log')
        setup_rotating_logger('test_basic', nested)
        self.assertTrue(Path(nested).parent.exists())


class TestSetupRotatingLoggerLogLevel(unittest.TestCase):
    """LOG_LEVEL 環境變數"""

    def setUp(self):
        self.tmp = tempfile.mkdtemp()

    def tearDown(self):
        for name in ('test_level_debug', 'test_level_warn', 'test_level_default'):
            logger = logging.getLogger(name)
            for h in logger.handlers[:]:
                try:
                    h.close()
                except OSError:
                    pass
            logger.handlers.clear()

    def test_log_level_debug(self):
        """LOG_LEVEL=DEBUG 應設定 logger level 為 DEBUG"""
        with patch.dict('os.environ', {'LOG_LEVEL': 'DEBUG'}):
            logger = setup_rotating_logger('test_level_debug', _tmp_log(self.tmp))
        self.assertEqual(logger.level, logging.DEBUG)

    def test_log_level_warning(self):
        """LOG_LEVEL=WARNING 應設定 logger level 為 WARNING"""
        with patch.dict('os.environ', {'LOG_LEVEL': 'WARNING'}):
            logger = setup_rotating_logger('test_level_warn', _tmp_log(self.tmp))
        self.assertEqual(logger.level, logging.WARNING)

    def test_default_log_level_info(self):
        """未設定 LOG_LEVEL 預設應為 INFO"""
        with patch.dict('os.environ', {}, clear=False):
            os.environ.pop('LOG_LEVEL', None)
            logger = setup_rotating_logger('test_level_default', _tmp_log(self.tmp))
        self.assertEqual(logger.level, logging.INFO)


class TestSetupRotatingLoggerErrorLog(unittest.TestCase):
    """error_log_file 參數：建立 ERROR+ 獨立 handler"""

    def setUp(self):
        self.tmp = tempfile.mkdtemp()

    def tearDown(self):
        logger = logging.getLogger('test_errlog')
        for h in logger.handlers[:]:
            try:
                h.close()
            except OSError:
                pass
        logger.handlers.clear()

    def test_error_handler_added(self):
        """提供 error_log_file 時應新增額外的 RotatingFileHandler"""
        logger = setup_rotating_logger(
            'test_errlog',
            _tmp_log(self.tmp, 'main.log'),
            error_log_file=_tmp_log(self.tmp, 'error.log'),
        )
        self.assertEqual(len([h for h in logger.handlers
                               if isinstance(h, logging.handlers.RotatingFileHandler)]), 2)

    def test_error_handler_level_is_error(self):
        """error_log_file handler 的 level 應為 ERROR"""
        logger = setup_rotating_logger(
            'test_errlog',
            _tmp_log(self.tmp, 'main.log'),
            error_log_file=_tmp_log(self.tmp, 'error.log'),
        )
        handlers = [h for h in logger.handlers
                    if isinstance(h, logging.handlers.RotatingFileHandler)]
        # error handler 是 level=ERROR，main handler 是 NOTSET
        error_handlers = [h for h in handlers if h.level == logging.ERROR]
        self.assertEqual(len(error_handlers), 1)

    def test_no_error_handler_when_not_provided(self):
        """未提供 error_log_file 時只應有一個 RotatingFileHandler"""
        logger = setup_rotating_logger('test_errlog', _tmp_log(self.tmp, 'main.log'))
        self.assertEqual(len([h for h in logger.handlers
                               if isinstance(h, logging.handlers.RotatingFileHandler)]), 1)


class TestSetupRotatingLoggerConsole(unittest.TestCase):
    """console 參數：是否加入 StreamHandler"""

    def setUp(self):
        self.tmp = tempfile.mkdtemp()

    def tearDown(self):
        for name in ('test_console_true', 'test_console_false'):
            logger = logging.getLogger(name)
            for h in logger.handlers[:]:
                try:
                    h.close()
                except OSError:
                    pass
            logger.handlers.clear()

    def test_console_true_adds_stream_handler(self):
        """console=True 應加入 StreamHandler"""
        logger = setup_rotating_logger(
            'test_console_true', _tmp_log(self.tmp), console=True,
        )
        stream_handlers = [h for h in logger.handlers
                           if isinstance(h, logging.StreamHandler)
                           and not isinstance(h, logging.handlers.RotatingFileHandler)]
        self.assertGreater(len(stream_handlers), 0)

    def test_console_false_no_stream_handler(self):
        """console=False（預設）不應加入 StreamHandler"""
        logger = setup_rotating_logger(
            'test_console_false', _tmp_log(self.tmp), console=False,
        )
        stream_handlers = [h for h in logger.handlers
                           if isinstance(h, logging.StreamHandler)
                           and not isinstance(h, logging.handlers.RotatingFileHandler)]
        self.assertEqual(len(stream_handlers), 0)


class TestSetupRotatingLoggerFallback(unittest.TestCase):
    """file handler 建立失敗時應回退至 console handler"""

    def tearDown(self):
        logger = logging.getLogger('test_fallback')
        for h in logger.handlers[:]:
            try:
                h.close()
            except OSError:
                pass
        logger.handlers.clear()

    def test_fallback_to_console_on_file_error(self):
        """RotatingFileHandler 建立失敗時，應回退加入 StreamHandler（非 FileHandler）"""
        with patch('logging_utils.Path') as mock_path_cls:
            mock_path_cls.return_value.parent.mkdir.side_effect = PermissionError('no access')
            with patch('logging_utils.logging.handlers.RotatingFileHandler',
                       side_effect=PermissionError('no access')):
                logger = setup_rotating_logger('test_fallback', '/no/such/path/test.log')

        # 回退時只有純 StreamHandler（stdout），不應有任何 FileHandler
        file_handlers = [h for h in logger.handlers
                         if isinstance(h, logging.FileHandler)]
        stream_only = [h for h in logger.handlers
                       if type(h) is logging.StreamHandler]
        self.assertEqual(len(file_handlers), 0, "fallback 不應有任何 FileHandler")
        self.assertGreater(len(stream_only), 0, "fallback 應有 StreamHandler")


class TestBaseDaemonSetupLoggerNoConsole(unittest.TestCase):
    """BaseDaemon.setup_logger() 不應加入 console handler（避免 status 指令雜訊）"""

    def test_no_console_handler_in_setup_logger(self):
        """setup_logger() 預設 console=False，不應有 StreamHandler"""
        import tempfile
        tmp = Path(tempfile.mkdtemp())
        with patch.dict('os.environ',
                        {'INPUT_DIRECTORY': '/tmp', 'OUTPUT_DIRECTORY': '/tmp'},
                        clear=False):
            from daemons.base_daemon import BaseDaemon

            class ConcreteDaemon(BaseDaemon):
                def run(self): pass
                def get_progress(self): return {}
                def get_current_status(self): return {}

            d = ConcreteDaemon(
                name='test_no_console',
                default_pid_file=str(tmp / 'test.pid'),
                default_log_file=str(tmp / 'test.log'),
                default_stderr_log_file=str(tmp / 'test_err.log'),
            )

        stream_handlers = [h for h in d.logger.handlers
                           if isinstance(h, logging.StreamHandler)
                           and not isinstance(h, logging.handlers.RotatingFileHandler)]
        self.assertEqual(len(stream_handlers), 0)


if __name__ == '__main__':
    unittest.main()
