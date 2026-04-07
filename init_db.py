import os
import sqlite3
from dotenv import load_dotenv

load_dotenv()


def init_database():
    """初始化資料庫，依 DB_TYPE 環境變數選擇後端。

    - DB_TYPE=mariadb（預設）：建立 MariaDB 資料庫與資料表
    - DB_TYPE=sqlite：建立 SQLite 資料庫檔案與資料表
    """
    db_type = os.getenv('DB_TYPE', 'mariadb').lower()
    if db_type == 'sqlite':
        _init_sqlite()
    else:
        _init_mariadb()


def _init_mariadb():
    """初始化 MariaDB 資料庫和資料表"""
    import mysql.connector
    from mysql.connector import errorcode

    config = {
        'user': os.getenv('DB_USER'),
        'password': os.getenv('DB_PASSWORD'),
        'host': os.getenv('DB_HOST'),
        'port': int(os.getenv('DB_PORT', '3306')),
    }

    try:
        conn = mysql.connector.connect(**config)
        cursor = conn.cursor()

        db_name = os.getenv('DB_NAME')
        if not db_name:
            raise ValueError("DB_NAME environment variable is not set")
        # utf8mb4：完整支援 Unicode（含 emoji 與 4-byte 字元），
        # 與 utf8（最多 3-byte）不同，可正確儲存非 BMP 字元路徑
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {db_name} CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci")
        print(f"Database '{db_name}' created or exists")

        cursor.execute(f"USE {db_name}")

        cursor.execute('''
        CREATE TABLE IF NOT EXISTS conversion_tasks (
            id INT AUTO_INCREMENT PRIMARY KEY,
            -- UNIQUE 限制確保同一個來源檔案不會被重複插入，
            -- scan 掃描時可直接 INSERT IGNORE 而無需先 SELECT 檢查
            input_path VARCHAR(1024) NOT NULL UNIQUE,
            output_path VARCHAR(1024) NOT NULL,
            source_resolution VARCHAR(20),
            target_resolution VARCHAR(20) DEFAULT '480p',
            -- status ENUM 記錄業務狀態（pending/processing/completed/failed）；
            -- is_processing BOOLEAN 是獨立的鎖旗標，用於原子性地防止多 worker 競爭同一任務，
            -- 兩者各有職責：status 用於查詢與報告，is_processing 用於並發控制
            status ENUM('pending', 'processing', 'completed', 'failed') DEFAULT 'pending',
            progress DECIMAL(5,2) DEFAULT 0.00,
            is_processing BOOLEAN DEFAULT FALSE,
            start_time DATETIME,
            end_time DATETIME,
            error_message TEXT,
            retry_count INT DEFAULT 0,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            INDEX idx_status (status),
            INDEX idx_is_processing (is_processing),
            INDEX idx_created_at (created_at),
            INDEX idx_retry_count (retry_count),
            INDEX idx_updated_at (updated_at)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        ''')

        # ON DELETE CASCADE：當對應的 conversion_tasks 記錄被刪除時，鎖記錄自動清除，避免孤兒鎖；
        # 此表為補充性追蹤（記錄由哪個 worker 持鎖），真正的並發控制靠 conversion_tasks.is_processing
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS processing_lock (
            task_id INT PRIMARY KEY,
            worker_id VARCHAR(50) NOT NULL,
            locked_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (task_id) REFERENCES conversion_tasks(id) ON DELETE CASCADE,
            INDEX idx_worker_id (worker_id)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        ''')

        # 複合索引 (status, is_processing)：worker 取得待處理任務的查詢條件為
        # WHERE status='pending' AND is_processing=FALSE，複合索引可讓此查詢只掃描符合條件的列
        cursor.execute('''
        CREATE INDEX IF NOT EXISTS idx_conversion_tasks_status_processing
        ON conversion_tasks (status, is_processing)
        ''')

        print("Tables created successfully")
        conn.commit()

    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("Access denied: Check your username and password")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            print("Database does not exist")
        else:
            print(f"Database error: {err}")
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals() and conn.is_connected():
            conn.close()


def _init_sqlite():
    """初始化 SQLite 資料庫檔案與資料表。

    SQLite schema 與 MariaDB schema 功能等價，語法差異：
    - INTEGER PRIMARY KEY AUTOINCREMENT 取代 AUTO_INCREMENT
    - TEXT CHECK() 取代 ENUM
    - INTEGER 0/1 取代 BOOLEAN
    - datetime('now') trigger 取代 ON UPDATE CURRENT_TIMESTAMP
    - 移除 ENGINE / CHARSET / COLLATION（SQLite 預設即 UTF-8）
    """
    db_path = os.getenv('DB_PATH', './data/converter.db')
    if db_path != ':memory:':
        db_dir = os.path.dirname(os.path.abspath(db_path))
        os.makedirs(db_dir, exist_ok=True)

    try:
        conn = sqlite3.connect(db_path)
        conn.execute("PRAGMA foreign_keys = ON")
        conn.execute("PRAGMA journal_mode = WAL")

        conn.execute('''
        CREATE TABLE IF NOT EXISTS conversion_tasks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            input_path TEXT NOT NULL UNIQUE,
            output_path TEXT NOT NULL,
            source_resolution TEXT,
            target_resolution TEXT DEFAULT '480p',
            status TEXT DEFAULT 'pending'
                CHECK (status IN ('pending', 'processing', 'completed', 'failed')),
            progress REAL DEFAULT 0.00,
            is_processing INTEGER DEFAULT 0,
            start_time TEXT,
            end_time TEXT,
            error_message TEXT,
            retry_count INTEGER DEFAULT 0,
            created_at TEXT DEFAULT (datetime('now')),
            updated_at TEXT DEFAULT (datetime('now'))
        )
        ''')

        # SQLite 無 ON UPDATE CURRENT_TIMESTAMP，以 AFTER UPDATE trigger 模擬
        conn.execute('''
        CREATE TRIGGER IF NOT EXISTS conversion_tasks_update_timestamp
        AFTER UPDATE ON conversion_tasks
        BEGIN
            UPDATE conversion_tasks SET updated_at = datetime('now') WHERE id = NEW.id;
        END
        ''')

        conn.execute('''
        CREATE TABLE IF NOT EXISTS processing_lock (
            task_id INTEGER PRIMARY KEY,
            worker_id TEXT NOT NULL,
            locked_at TEXT DEFAULT (datetime('now')),
            FOREIGN KEY (task_id) REFERENCES conversion_tasks(id) ON DELETE CASCADE
        )
        ''')

        conn.execute("CREATE INDEX IF NOT EXISTS idx_status ON conversion_tasks(status)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_is_processing ON conversion_tasks(is_processing)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_created_at ON conversion_tasks(created_at)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_retry_count ON conversion_tasks(retry_count)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_updated_at ON conversion_tasks(updated_at)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_worker_id ON processing_lock(worker_id)")
        conn.execute('''
        CREATE INDEX IF NOT EXISTS idx_conversion_tasks_status_processing
        ON conversion_tasks(status, is_processing)
        ''')

        conn.commit()
        print(f"SQLite database initialized: {db_path}")

    except Exception as err:
        print(f"SQLite initialization error: {err}")
    finally:
        if 'conn' in locals():
            conn.close()


if __name__ == '__main__':
    init_database()
