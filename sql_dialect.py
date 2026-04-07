"""
sql_dialect — SQL 方言抽象層

以 Factory Method 模式封裝各資料庫後端的 SQL 語法差異。
新增後端只需：
1. 繼承 SqlDialect 並實作所有抽象方法
2. 在 _DIALECTS 字典中登錄

目前支援後端：mariadb、sqlite
"""
import re
from abc import ABC, abstractmethod

# 合法的 SQL 識別字：字母、數字、底線、句點（table.column），不允許空白或特殊字元
_SAFE_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_.]*$")


def _validate_sql_identifier(value: str, param_name: str = 'value') -> None:
    """驗證字串是否為合法的 SQL 識別字（欄位名稱或 table.column 形式）。

    SqlDialect 的 timestampdiff_seconds 等方法接受欄位名稱並直接嵌入 SQL，
    因此必須確保輸入只能是硬編碼的識別字，不含任何注入風險字元。

    Args:
        value:      要驗證的字串
        param_name: 參數名稱（出現在錯誤訊息中）

    Raises:
        ValueError: 若 value 不符合合法識別字格式
    """
    if not _SAFE_IDENTIFIER_RE.match(value):
        raise ValueError(
            f"SqlDialect: {param_name}={value!r} is not a valid SQL identifier. "
            "Only hardcoded column names (letters, digits, underscores, dots) are allowed. "
            "Never pass user input or dynamic values to this method."
        )


class SqlDialect(ABC):
    """SQL 方言抽象基底類別。

    每個抽象方法對應一種跨後端不相容的 SQL 表達式；
    子類別須實作所有方法，回傳該後端原生的 SQL 字串片段。
    """

    @abstractmethod
    def timestampdiff_seconds(self, start_col: str, end_col: str) -> str:
        """回傳計算兩個時間欄位差（秒）的 SQL 表達式。

        Args:
            start_col: 起始時間欄位名稱（如 'start_time'）。
                       **必須為硬編碼識別字，絕不可傳入使用者輸入。**
            end_col:   結束時間欄位名稱（如 'end_time'）。
                       **必須為硬編碼識別字，絕不可傳入使用者輸入。**
        """

    @abstractmethod
    def concat(self, *parts: str) -> str:
        """回傳字串連接的 SQL 表達式。

        Args:
            parts: 要連接的 SQL 值或欄位名稱（如 "'prefix'", '%s', 'col_name'）
        """

    @abstractmethod
    def interval_ago(self, minutes: int) -> str:
        """回傳「目前時間往前 N 分鐘」的 SQL 時間戳表達式。

        Args:
            minutes: 往前推算的分鐘數
        """

    @abstractmethod
    def translate_query(self, query: str) -> str:
        """將通用 SQL（以 MariaDB 語法為基準）轉換為此後端原生語法。

        每個後端自行決定轉換規則。MariaDB 後端直接回傳原始字串；
        SQLite 後端轉換佔位符與 INSERT IGNORE 語法。

        Args:
            query: 含 %s 佔位符的 SQL 字串
        """


class MariaDBDialect(SqlDialect):
    """MariaDB / MySQL SQL 方言。

    使用 MariaDB 原生語法，translate_query 不做任何轉換。
    """

    def timestampdiff_seconds(self, start_col: str, end_col: str) -> str:
        _validate_sql_identifier(start_col, 'start_col')
        _validate_sql_identifier(end_col, 'end_col')
        return f"TIMESTAMPDIFF(SECOND, {start_col}, {end_col})"

    def concat(self, *parts: str) -> str:
        return f"CONCAT({', '.join(parts)})"

    def interval_ago(self, minutes: int) -> str:
        return f"NOW() - INTERVAL {minutes} MINUTE"

    def translate_query(self, query: str) -> str:
        return query


class SQLiteDialect(SqlDialect):
    """SQLite SQL 方言。

    translate_query 自動轉換：
    - 參數佔位符 %s → ?
    - INSERT IGNORE … → INSERT OR IGNORE …
    """

    def timestampdiff_seconds(self, start_col: str, end_col: str) -> str:
        _validate_sql_identifier(start_col, 'start_col')
        _validate_sql_identifier(end_col, 'end_col')
        # julianday 回傳天數（浮點），乘 86400 轉秒，CAST 為整數
        return (
            f"CAST((julianday({end_col}) - julianday({start_col})) * 86400 AS INTEGER)"
        )

    def concat(self, *parts: str) -> str:
        return " || ".join(parts)

    def interval_ago(self, minutes: int) -> str:
        return f"datetime('now', '-{minutes} minutes')"

    def translate_query(self, query: str) -> str:
        q = query.replace('%s', '?')
        q = re.sub(r'\bINSERT\s+IGNORE\b', 'INSERT OR IGNORE', q, flags=re.IGNORECASE)
        return q


# 後端名稱 → 方言類別的登錄表；新增後端只需在此加一行
_DIALECTS: dict[str, type[SqlDialect]] = {
    'mariadb': MariaDBDialect,
    'sqlite':  SQLiteDialect,
}


def create_dialect(db_type: str) -> SqlDialect:
    """工廠函式：依 db_type 字串建立對應的 SqlDialect 實例。

    Args:
        db_type: 資料庫後端名稱（大小寫不敏感），如 'mariadb' 或 'sqlite'

    Returns:
        對應後端的 SqlDialect 實例

    Raises:
        ValueError: 若 db_type 不在支援清單中
    """
    cls = _DIALECTS.get(db_type.lower())
    if cls is None:
        supported = ', '.join(sorted(_DIALECTS))
        raise ValueError(
            f"Unsupported DB_TYPE: {db_type!r}. Supported backends: {supported}"
        )
    return cls()
