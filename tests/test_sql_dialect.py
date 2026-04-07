"""
Unit tests for sql_dialect.py
測試 SqlDialect 抽象層、各後端實作，及 create_dialect() 工廠函式。
"""
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from sql_dialect import (
    MariaDBDialect,
    SQLiteDialect,
    SqlDialect,
    create_dialect,
    _validate_sql_identifier,
)


class TestValidateSqlIdentifier(unittest.TestCase):
    """_validate_sql_identifier 安全驗證函式測試"""

    def test_valid_simple_column(self):
        """單一欄位名稱應通過驗證"""
        _validate_sql_identifier('start_time', 'col')  # no exception

    def test_valid_table_dot_column(self):
        """table.column 形式應通過驗證"""
        _validate_sql_identifier('t.end_time', 'col')

    def test_valid_alphanumeric(self):
        _validate_sql_identifier('col1', 'col')

    def test_rejects_semicolon(self):
        """含分號的輸入應被拒絕（防止 SQL injection）"""
        with self.assertRaises(ValueError) as ctx:
            _validate_sql_identifier("end); DROP TABLE users; --", 'col')
        self.assertIn('SqlDialect', str(ctx.exception))

    def test_rejects_space(self):
        with self.assertRaises(ValueError):
            _validate_sql_identifier('col name', 'col')

    def test_rejects_single_quote(self):
        with self.assertRaises(ValueError):
            _validate_sql_identifier("col'injection", 'col')

    def test_rejects_empty_string(self):
        with self.assertRaises(ValueError):
            _validate_sql_identifier('', 'col')

    def test_rejects_starts_with_digit(self):
        with self.assertRaises(ValueError):
            _validate_sql_identifier('1col', 'col')

    def test_error_message_contains_param_name(self):
        """錯誤訊息應包含參數名稱，方便除錯"""
        with self.assertRaises(ValueError) as ctx:
            _validate_sql_identifier('bad col', 'start_col')
        self.assertIn('start_col', str(ctx.exception))


class TestTimestampdiffValidation(unittest.TestCase):
    """驗證 timestampdiff_seconds 在非法識別字時拒絕執行"""

    def test_mariadb_rejects_injection(self):
        d = MariaDBDialect()
        with self.assertRaises(ValueError):
            d.timestampdiff_seconds('start_time', "end); DROP TABLE t; --")

    def test_sqlite_rejects_injection(self):
        d = SQLiteDialect()
        with self.assertRaises(ValueError):
            d.timestampdiff_seconds("start'; DROP TABLE t; --", 'end_time')

    def test_mariadb_accepts_valid_columns(self):
        d = MariaDBDialect()
        result = d.timestampdiff_seconds('start_time', 'end_time')
        self.assertIn('start_time', result)
        self.assertIn('end_time', result)

    def test_sqlite_accepts_valid_columns(self):
        d = SQLiteDialect()
        result = d.timestampdiff_seconds('start_time', 'end_time')
        self.assertIn('start_time', result)
        self.assertIn('end_time', result)


class TestMariaDBDialect(unittest.TestCase):
    """MariaDBDialect 方法測試"""

    def setUp(self):
        self.d = MariaDBDialect()

    def test_is_sql_dialect(self):
        self.assertIsInstance(self.d, SqlDialect)

    def test_timestampdiff_seconds(self):
        result = self.d.timestampdiff_seconds('start_time', 'end_time')
        self.assertEqual(result, 'TIMESTAMPDIFF(SECOND, start_time, end_time)')

    def test_concat_two_parts(self):
        result = self.d.concat("'prefix'", 'col')
        self.assertEqual(result, "CONCAT('prefix', col)")

    def test_concat_many_parts(self):
        result = self.d.concat("'a'", '%s', "'b'", 'col')
        self.assertEqual(result, "CONCAT('a', %s, 'b', col)")

    def test_concat_single_part(self):
        result = self.d.concat('col')
        self.assertEqual(result, 'CONCAT(col)')

    def test_interval_ago(self):
        result = self.d.interval_ago(5)
        self.assertEqual(result, 'NOW() - INTERVAL 5 MINUTE')

    def test_interval_ago_custom_minutes(self):
        result = self.d.interval_ago(30)
        self.assertEqual(result, 'NOW() - INTERVAL 30 MINUTE')

    def test_translate_query_passthrough(self):
        """MariaDB 不轉換，直接回傳原始 SQL"""
        sql = "INSERT IGNORE INTO t (a) VALUES (%s)"
        self.assertEqual(self.d.translate_query(sql), sql)

    def test_translate_query_preserves_percent_s(self):
        sql = "SELECT * FROM t WHERE id=%s AND name=%s"
        self.assertEqual(self.d.translate_query(sql), sql)


class TestSQLiteDialect(unittest.TestCase):
    """SQLiteDialect 方法測試"""

    def setUp(self):
        self.d = SQLiteDialect()

    def test_is_sql_dialect(self):
        self.assertIsInstance(self.d, SqlDialect)

    def test_timestampdiff_seconds(self):
        result = self.d.timestampdiff_seconds('start_time', 'end_time')
        self.assertIn('julianday(end_time)', result)
        self.assertIn('julianday(start_time)', result)
        self.assertIn('86400', result)
        self.assertIn('CAST', result)

    def test_concat_two_parts(self):
        result = self.d.concat("'prefix'", 'col')
        self.assertEqual(result, "'prefix' || col")

    def test_concat_many_parts(self):
        result = self.d.concat("'a'", '%s', "'b'", 'col')
        self.assertEqual(result, "'a' || %s || 'b' || col")

    def test_concat_single_part(self):
        result = self.d.concat('col')
        self.assertEqual(result, 'col')

    def test_interval_ago(self):
        result = self.d.interval_ago(5)
        self.assertEqual(result, "datetime('now', '-5 minutes')")

    def test_interval_ago_custom_minutes(self):
        result = self.d.interval_ago(30)
        self.assertEqual(result, "datetime('now', '-30 minutes')")

    def test_translate_query_placeholder(self):
        sql = "SELECT * FROM t WHERE id=%s AND name=%s"
        result = self.d.translate_query(sql)
        self.assertEqual(result, "SELECT * FROM t WHERE id=? AND name=?")
        self.assertNotIn('%s', result)

    def test_translate_query_insert_ignore(self):
        sql = "INSERT IGNORE INTO t (a) VALUES (%s)"
        result = self.d.translate_query(sql)
        self.assertEqual(result, "INSERT OR IGNORE INTO t (a) VALUES (?)")

    def test_translate_query_insert_ignore_lowercase(self):
        sql = "insert ignore into t (a) values (%s)"
        result = self.d.translate_query(sql)
        self.assertIn('INSERT OR IGNORE', result.upper())
        self.assertNotIn('INSERT IGNORE', result.upper())

    def test_translate_query_insert_ignore_mixed_case(self):
        sql = "Insert Ignore Into t (a) Values (%s)"
        result = self.d.translate_query(sql)
        self.assertIn('INSERT OR IGNORE', result.upper())

    def test_translate_query_no_change_needed(self):
        """不含需要轉換的語法時，僅轉換佔位符"""
        sql = "SELECT 1"
        self.assertEqual(self.d.translate_query(sql), "SELECT 1")

    def test_translate_query_no_percent_s(self):
        """不含 %s 時不應產生 ?"""
        sql = "SELECT * FROM t"
        self.assertEqual(self.d.translate_query(sql), sql)


class TestCreateDialect(unittest.TestCase):
    """create_dialect() 工廠函式測試"""

    def test_mariadb_lowercase(self):
        d = create_dialect('mariadb')
        self.assertIsInstance(d, MariaDBDialect)

    def test_mariadb_uppercase(self):
        d = create_dialect('MARIADB')
        self.assertIsInstance(d, MariaDBDialect)

    def test_mariadb_mixed_case(self):
        d = create_dialect('MariaDB')
        self.assertIsInstance(d, MariaDBDialect)

    def test_sqlite_lowercase(self):
        d = create_dialect('sqlite')
        self.assertIsInstance(d, SQLiteDialect)

    def test_sqlite_uppercase(self):
        d = create_dialect('SQLITE')
        self.assertIsInstance(d, SQLiteDialect)

    def test_unsupported_backend_raises_value_error(self):
        with self.assertRaises(ValueError) as ctx:
            create_dialect('postgresql')
        self.assertIn('postgresql', str(ctx.exception))
        # 錯誤訊息應包含支援清單
        self.assertIn('mariadb', str(ctx.exception))
        self.assertIn('sqlite', str(ctx.exception))

    def test_unsupported_empty_string_raises_value_error(self):
        with self.assertRaises(ValueError):
            create_dialect('')

    def test_returns_new_instance_each_call(self):
        """每次呼叫應回傳獨立實例（非單例）"""
        d1 = create_dialect('mariadb')
        d2 = create_dialect('mariadb')
        self.assertIsNot(d1, d2)

    def test_dialect_is_sql_dialect_subclass(self):
        for db_type in ('mariadb', 'sqlite'):
            with self.subTest(db_type=db_type):
                d = create_dialect(db_type)
                self.assertIsInstance(d, SqlDialect)


class TestDialectConsistency(unittest.TestCase):
    """驗證所有 dialect 方法簽名一致（介面合規性）"""

    DIALECTS = [MariaDBDialect(), SQLiteDialect()]

    def test_all_dialects_have_timestampdiff_seconds(self):
        for d in self.DIALECTS:
            with self.subTest(dialect=type(d).__name__):
                result = d.timestampdiff_seconds('s', 'e')
                self.assertIsInstance(result, str)
                self.assertTrue(len(result) > 0)

    def test_all_dialects_have_concat(self):
        for d in self.DIALECTS:
            with self.subTest(dialect=type(d).__name__):
                result = d.concat("'a'", "'b'")
                self.assertIsInstance(result, str)
                self.assertIn('a', result)
                self.assertIn('b', result)

    def test_all_dialects_have_interval_ago(self):
        for d in self.DIALECTS:
            with self.subTest(dialect=type(d).__name__):
                result = d.interval_ago(10)
                self.assertIsInstance(result, str)
                self.assertIn('10', result)

    def test_all_dialects_have_translate_query(self):
        for d in self.DIALECTS:
            with self.subTest(dialect=type(d).__name__):
                result = d.translate_query("SELECT 1")
                self.assertIsInstance(result, str)


if __name__ == '__main__':
    unittest.main()
