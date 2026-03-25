import mysql.connector
from mysql.connector import errorcode
import os
from dotenv import load_dotenv

load_dotenv()

def init_database():
    """初始化 MariaDB 資料庫和表格"""
    config = {
        'user': os.getenv('DB_USER'),
        'password': os.getenv('DB_PASSWORD'),
        'host': os.getenv('DB_HOST'),
        'port': int(os.getenv('DB_PORT', '3306')),
    }
    
    try:
        # 連接到 MariaDB 伺服器
        conn = mysql.connector.connect(**config)
        cursor = conn.cursor()
        
        # 建立資料庫
        db_name = os.getenv('DB_NAME')
        # utf8mb4：完整支援 Unicode（含 emoji 與 4-byte 字元），
        # 與 utf8（最多 3-byte）不同，可正確儲存非 BMP 字元路徑
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {db_name} CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci")
        print(f"Database '{db_name}' created or exists")
        
        # 選擇資料庫
        cursor.execute(f"USE {db_name}")
        
        # 建立轉檔任務表（更新結構）
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
        
        # 建立處理鎖表
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS processing_lock (
            task_id INT PRIMARY KEY,
            worker_id VARCHAR(50) NOT NULL,
            locked_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            -- ON DELETE CASCADE：當對應的 conversion_tasks 記錄被刪除時，鎖記錄自動清除，避免孤兒鎖；
            -- 此表為補充性追蹤（記錄由哪個 worker 持鎖），真正的並發控制靠 conversion_tasks.is_processing
            FOREIGN KEY (task_id) REFERENCES conversion_tasks(id) ON DELETE CASCADE,
            INDEX idx_worker_id (worker_id)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        ''')
        
        # 建立索引以提高查詢效率
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

if __name__ == '__main__':
    init_database()
