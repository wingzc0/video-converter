# video-converter

## 程式庫總覽

這是一個**基於 Python 的自動化影片轉檔流水線**。其主要功能是遞迴掃描指定目錄中的影片檔案，並將所有解析度高於 480p 的影片轉換為 480p（H.264/AAC 編碼），同時使用 MariaDB 資料庫追蹤所有轉檔任務。

---

## 核心技術

| 技術 | 用途 |
|---|---|
| **Python 3** | 主要開發語言 |
| **FFmpeg / ffprobe** | 影片轉碼與元數據讀取 |
| **MariaDB（MySQL）** | 持久化任務佇列與狀態追蹤 |
| **Flask + Flask-SocketIO** | REST API 與即時 WebSocket 推送 |
| **python-daemon** | UNIX daemon 程序管理（PID 檔、背景化） |
| **psutil** | 可選的系統指標（CPU、記憶體、磁碟） |
| **python-dotenv** | 透過 `.env` 檔進行設定管理 |

---

## 程式庫結構

```
video-converter/
│
├── main.py                    # ⚠️ 舊版 CLI 腳本（已不再維護，請改用 daemon 版本）
│
├── converter.py               # 核心 FFmpeg 封裝模組
│                              #   get_video_info()     – 用 ffprobe 取得解析度與元數據
│                              #   convert_to_480p()    – ffmpeg 轉檔，支援進度回調
│                              #   get_video_duration() – 用 ffprobe 取得影片時長
│
├── db_manager.py              # MariaDB 連接池管理（mysql.connector）
│                              #   DatabaseManager 類別，維護 5 個連接的連接池
│                              #   execute_query()、execute_transaction()、health_check()
│                              #   以模組級別的單例形式匯出：db_manager
│
├── init_db.py                 # 一次性資料庫結構初始化工具
│                              #   建立：conversion_tasks 表 + processing_lock 表
│                              #   為 status、is_processing、時間戳等欄位建立索引
│
├── daemons/
│   ├── __init__.py
│   ├── base_daemon.py         # 所有 daemon 的抽象基礎類別（ABC）
│   │                          #   透過 python-daemon 實現背景化（PID 檔、stdout/stderr 重導向）
│   │                          #   將狀態寫入 JSON 檔（儲存於 /var/run/video-converter/）
│   │                          #   信號處理（SIGTERM/SIGINT → 優雅關閉）
│   │                          #   start() / stop() / restart() / status()
│   │
│   ├── scan_daemon.py         # 掃描 Daemon（繼承 BaseDaemon）
│   │                          #   遞迴遍歷 INPUT_DIRECTORY
│   │                          #   跳過已存在資料庫、低於 MIN_RESOLUTION 或副檔名不符的檔案
│   │                          #   將新發現的影片以 'pending' 狀態寫入 conversion_tasks
│   │                          #   可設定掃描間隔（預設：300 秒）
│   │
│   └── process_daemon.py      # 處理 Daemon（繼承 BaseDaemon）
│                              #   執行緒池工作模式（預設：1 個工作執行緒，使用 queue.Queue）
│                              #   每 CHECK_INTERVAL 秒輪詢一次資料庫的待處理任務（預設：300 秒）
│                              #   使用資料庫列鎖（is_processing 旗標）防止重複處理
│                              #   呼叫 converter.convert_to_480p() 並即時回報進度
│                              #   更新任務狀態：pending → processing → completed/failed
│                              #   自動重試失敗任務（每 RETRY_INTERVAL_CYCLES 次 check 執行一次）
│                              #   自動清除過時任務（每次 check 都執行，閾值 STALE_HOURS）
│
├── api/
│   └── server.py              # Flask REST + WebSocket API 伺服器
│                              #   讀取 daemon 的 JSON 狀態檔（含 60 秒過期檢查）
│                              #   REST 端點：/api/health、/api/status、/api/progress/{scan,process,system,stats}
│                              #   WebSocket：每 2 秒廣播 scan_progress、process_progress、system_status、task_stats
│                              #   從資料庫查詢彙總任務統計資訊
│
├── monitor_daemons.py         # 終端機監控儀表板（CLI 工具）
│                              #   輪詢 REST API 並以彩色 ASCII 格式呈現
│                              #   顯示 daemon PID/運行時間、掃描/處理進度、任務統計、進度條
│                              #   支援持續監控（-c）與單次顯示兩種模式
│
├── start_scan_daemon.py       # 啟動腳本：啟動 ScanDaemon（背景或 --foreground 前景模式）
├── start_process_daemon.py    # 啟動腳本：啟動 ProcessDaemon（背景或 --foreground 前景模式）
├── start_api_server.py        # 啟動腳本：啟動 Flask API 伺服器
│
├── scripts/
│   ├── install_daemons.sh     # Bash 安裝腳本：建立 /var/run、/var/log 目錄，安裝 systemd 服務單元
│   └── video-scanner.service  # 掃描 daemon 的 systemd 服務單元範本
│
└── README.md                  # 本文件
```

---

## 系統運作流程

```
[ 檔案系統 ]
      │  （INPUT_DIRECTORY 輸入目錄）
      ▼
[ 掃描 Daemon ]  ──── ffprobe（解析度檢查）────► [ MariaDB：conversion_tasks ]
 (scan_daemon.py)                                        │  status='pending'（待處理）
                                                          │
[ 處理 Daemon ] ◄─────────────── 每 60 秒輪詢 ──────────┘
 (process_daemon.py)
      │  工作執行緒（最多 MAX_WORKERS 個）
      │  取得列鎖（is_processing=TRUE）
      ▼
[ converter.py ] ──── ffmpeg ────► OUTPUT_DIRECTORY/480p_<檔名>
      │
      └─► 更新資料庫：status='processing'（含進度 %）→ 'completed'（完成）/'failed'（失敗）

[ API 伺服器 ] ──── 讀取狀態 JSON + 查詢資料庫 ────► REST/WebSocket 用戶端
[ 監控工具  ] ──── 輪詢 REST API ────► 終端機儀表板
```

---

## 資料庫結構

**`conversion_tasks`**（核心表）：

| 欄位 | 說明 |
|---|---|
| `input_path`（唯一鍵）、`output_path` | 檔案路徑 |
| `source_resolution`、`target_resolution` | 例如 `1920x1080` → `480p` |
| `status` | 列舉值：`pending`（待處理）\| `processing`（處理中）\| `completed`（完成）\| `failed`（失敗） |
| `progress` | 0.00 至 100.00（轉檔過程中即時更新） |
| `is_processing` | 布林鎖旗標，防止重複處理 |
| `retry_count`、`error_message` | 重試次數與錯誤訊息 |
| `start_time`、`end_time` | 任務起訖時間 |

**`processing_lock`**：輔助鎖表（以 `task_id` 為主鍵）

---

## 設定方式（透過 `.env`）

所有執行期設定均來自環境變數：

| 變數 | 說明 |
|---|---|
| `DB_HOST`、`DB_PORT`、`DB_USER`、`DB_PASSWORD`、`DB_NAME` | 資料庫連線設定 |
| `INPUT_DIRECTORY` | 輸入影片目錄 |
| `OUTPUT_DIRECTORY` | 輸出目錄 |
| `SUPPORTED_EXTENSIONS` | 支援的副檔名（預設：`.mp4,.mkv,.avi,.mov,.flv,.wmv,.m4v,.webm`） |
| `MIN_RESOLUTION` | 最低解析度（預設：`481`，即跳過 ≤ 480p 的檔案） |
| `MAX_WORKERS` | 最大工作執行緒數 |
| `SCAN_INTERVAL` | 掃描間隔（秒） |
| `CHECK_INTERVAL` | 任務輪詢間隔（秒） |
| `MAX_RETRIES` | 失敗任務最大重試次數（預設：`3`） |
| `RETRY_INTERVAL_CYCLES` | 每幾個 check cycle 執行一次重試（預設：`10`） |
| `STALE_HOURS` | 任務卡在 processing 超過幾小時視為過時（預設：`1`） |
| `API_SERVER_HOST`、`API_SERVER_PORT`、`API_SERVER_URL` | API 伺服器設定 |
| `LOG_LEVEL` | 日誌等級 |

---

## 部署方式

> ⚠️ **注意：`main.py` 為舊版 CLI 腳本，已不再維護。請使用以下 daemon 方式執行。**

### 方式一：長駐 Daemon 程序（建議方式）

以 `start_*.py` 啟動三個獨立程序：

```bash
python3 start_scan_daemon.py
python3 start_process_daemon.py
python3 start_api_server.py --foreground &
```

### 方式二：即時監控

```bash
python monitor_daemons.py -c
```

透過查詢 REST API 提供即時終端機儀表板。

---

## 指令參數說明（`main.py`）

> ⚠️ `main.py` 為舊版腳本，已不再維護。以下僅供參考，請改用 daemon 版本。

| 參數 | 說明 |
|---|---|
| `--process-pending` | 處理 pending 任務（預設啟用） |
| `--no-process-pending` | 不處理 pending 任務 |
| `--retry-failed` | 重試失敗的任務（最多 3 次） |
| `--cleanup-stale` | 清理過時的任務 |
| `--stale-hours N` | 設定過時任務的時間閾值（小時，預設 24） |
| `--scan-only` | 只掃描目錄並添加任務，不進行轉檔 |
| `--process-only` | 只處理現有任務，不掃描新檔案 |
| `--no-interactive` | 非互動模式，不顯示目錄結構也不等待確認 |
| `--force` | 強制執行，跳過設定驗證 |
| `--force-overwrite` | 覆蓋已存在的輸出檔案 |
| `--skip-low-resolution` | 跳過解析度已在 480p 或以下的檔案 |
| `--verbose` | 詳細輸出，顯示每個檔案的處理過程 |
| `--quiet` | 安靜模式，只顯示錯誤訊息 |
| `--max-workers N` | 覆蓋 `.env` 中的 `MAX_WORKERS` 設定 |

---

## 使用範例

```bash
# 掃描並轉檔，非互動模式
python main.py --no-interactive --verbose

# 只掃描新檔案
python main.py --scan-only --no-interactive

# 只處理現有任務
python main.py --process-only --no-interactive

# 強制執行，忽略時間限制
python main.py --force --no-interactive

# 強制覆蓋已存在的檔案
python main.py --force-overwrite --no-interactive

# 處理所有 pending 任務
python main.py --process-only --no-interactive --process-pending

# 重試失敗的任務並清理過時任務
python main.py --process-only --no-interactive --retry-failed --cleanup-stale

# 只處理新掃描的檔案，不處理 pending 任務
python main.py --no-process-pending --no-interactive
```

---

## Logrotate 設定

```
/var/log/video-converter*.log {
    daily
    rotate 30
    compress
    delaycompress
    missingok
    notifempty
    create 644 www-data www-data
}
```

---

## Systemd 服務設定

```ini
[Unit]
Description=Video Converter Service
After=network.target mariadb.service

[Service]
Type=simple
User=videoconverter
Group=videoconverter
WorkingDirectory=/path/to/video-converter
Environment=PYTHONUNBUFFERED=1
ExecStart=/usr/bin/python3 start_process_daemon.py --foreground
Restart=on-failure
RestartSec=30
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
```

啟用服務：

```bash
sudo systemctl daemon-reload
sudo systemctl enable video-converter.service
sudo systemctl start video-converter.service
```

---

## 總結

這是一個結構清晰、面向生產環境的批次影片處理系統。各功能模組分工明確：

- **掃描**（`scan_daemon.py`）：檔案探索與任務入列
- **處理**（`process_daemon.py`）：多執行緒轉碼執行
- **持久化**（`db_manager.py` + MariaDB）：任務佇列與狀態追蹤
- **可觀測性**（`api/server.py`）：REST API + WebSocket 即時推送
- **監控**（`monitor_daemons.py`）：終端機儀表板

資料庫列鎖機制（`is_processing` 旗標）確保多個工作執行緒同時運行時不會重複處理同一個檔案。
