# video-converter

## 程式庫總覽

這是一個**基於 Python 的自動化影片轉檔流水線**。其主要功能是遞迴掃描指定目錄中的影片檔案，並將所有解析度高於 480p 的影片轉換為 480p（H.264/AAC 編碼），同時使用資料庫追蹤所有轉檔任務。支援 **MariaDB**（預設，適合多機部署）與 **SQLite**（輕量本機部署）雙後端，透過環境變數 `DB_TYPE` 切換，無需修改任何程式碼。

---

## 核心技術

| 技術 | 用途 |
|---|---|
| **Python 3** | 主要開發語言 |
| **FFmpeg / ffprobe** | 影片轉碼與元數據讀取 |
| **MariaDB（MySQL）** | 持久化任務佇列與狀態追蹤（預設後端） |
| **SQLite** | 輕量單機後端（無需額外服務，`DB_TYPE=sqlite`） |
| **Flask + Flask-SocketIO** | REST API 與即時 WebSocket 推送 |
| **python-daemon** | UNIX daemon 程序管理（PID 檔、背景化） |
| **psutil** | 可選的系統指標（CPU、記憶體、磁碟） |
| **python-dotenv** | 透過 `.env` 檔進行設定管理 |

---

## 程式庫結構

```
video-converter/
│
├── conv_admin.py              # 資料庫診斷與維護工具：目錄預覽、任務統計、手動重試/清理/重設/新增/kill 孤兒 ffmpeg
│
├── converter.py               # 核心 FFmpeg 封裝模組
│                              #   get_video_info()     – 用 ffprobe 取得解析度與元數據
│                              #   convert_to_480p()    – ffmpeg 轉檔，支援進度回調與雙層超時保護
│                              #                          回傳 (success: bool, error: str | None)
│                              #                          失敗時 error 包含 ffmpeg stderr 最後幾行
│                              #   get_video_duration() – 用 ffprobe 取得影片時長
│
├── db_manager.py              # 資料庫管理器，支援 MariaDB 與 SQLite 雙後端
│                              #   DatabaseManager 類別，透過 DB_TYPE 環境變數選擇後端：
│                              #     mariadb：mysql.connector connection pool（5 連線）
│                              #     sqlite ：threading.local per-thread 連線 + WAL 模式
│                              #   dialect property：依後端回傳對應 SqlDialect 實例
│                              #   execute_query()、execute_transaction()、health_check()
│                              #   以模組級別的單例形式匯出：db_manager
│
├── sql_dialect.py             # SQL 方言抽象層（Factory Method 模式）
│                              #   SqlDialect 抽象基底類別：定義跨後端不相容的 SQL 表達式介面
│                              #     timestampdiff_seconds()、concat()、interval_ago()、translate_query()
│                              #   MariaDBDialect：MariaDB 原生語法，translate_query 直接回傳
│                              #   SQLiteDialect ：julianday()、|| 串接、datetime('now',...)，
│                              #                  translate_query 轉換 %s→? 與 INSERT IGNORE→INSERT OR IGNORE
│                              #   create_dialect(db_type)：工廠函式；新增後端只需新增子類別
│                              #                           並在 _DIALECTS 字典登錄一行
│
├── init_db.py                 # 一次性資料庫結構初始化工具
│                              #   依 DB_TYPE 路由至 _init_mariadb() 或 _init_sqlite()
│                              #   MariaDB：建立 conversion_tasks + processing_lock 表、索引
│                              #   SQLite ：同等 schema（AUTOINCREMENT、TEXT CHECK()、INTEGER bool）
│                              #            + AFTER UPDATE trigger 模擬 ON UPDATE CURRENT_TIMESTAMP
│
├── task_manager.py            # 任務資料庫操作的統一抽象層
│                              #   TaskRepository 類別：封裝所有 conversion_tasks DB 操作
│                              #   get_pending_tasks()、get_task_by_input_path()、get_task_by_id()
│                              #   get_task_detail()、get_task_statistics()
│                              #   get_recent_failed_tasks()、get_maxed_failed_tasks()
│                              #   update_task_status()、acquire/release_task_lock()
│                              #   retry_failed_tasks()、cleanup_stale_tasks()
│                              #   reset_tasks_to_pending()、cleanup_orphaned_flags()
│                              #   requeue_missing_output()、insert_task()
│                              #   所有 daemon 與 conv_admin 均透過此類別存取 DB，不直接呼叫 db_manager
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
│   │                          #   遞迴遍歷 INPUT_DIRECTORY，輸出路徑一律使用 .mp4 副檔名
│   │                          #   命名規則：.mp4 輸入 → 480p_{stem}.mp4；其他格式 → 480p_{stem}_{ext}.mp4
│   │                          #   掃描順序（NFS I/O 最小化）：
│   │                          #     1. DB 查詢 → pending/processing/failed 直接略過
│   │                          #     2. completed → 用 DB 儲存的 output_path 做 exist 檢查（無 ffprobe）
│   │                          #     3. output_path.exists()（單一 stat，跳過 ffprobe）
│   │                          #     4. ffprobe 僅用於全新且無輸出的檔案
│   │                          #   可設定掃描間隔（SCAN_INTERVAL，NFS 環境建議 1800 秒）
│   │                          #   所有 DB 操作透過 TaskRepository（task_manager.py）
│   │
│   └── process_daemon.py      # 處理 Daemon（繼承 BaseDaemon）
│                              #   執行緒池工作模式（預設：2 個工作執行緒，使用 queue.Queue）
│                              #   每 CHECK_INTERVAL 秒輪詢一次資料庫的待處理任務（預設：60 秒）
│                              #   任務排序：retry_count ASC, created_at ASC（全新任務優先）
│                              #   使用資料庫列鎖（is_processing 旗標）防止重複處理
│                              #   worker() 統一管理鎖生命週期（lock_acquired 旗標 + finally 釋放）
│                              #   呼叫 converter.convert_to_480p() 並即時回報進度
│                              #   ffmpeg 雙層超時保護：stall timeout（無進度）+ absolute timeout
│                              #     absolute timeout：FFMPEG_TIMEOUT=0（預設）時動態計算
│                              #       = max(FFMPEG_TIMEOUT_MIN, 影片時長 × FFMPEG_TIMEOUT_MULTIPLIER × bitrate_factor)
│                              #       bitrate_factor = log2(max(2, src_Mbps / BITRATE_BASELINE_MBPS))（高 bitrate 自動延長）
│                              #   轉檔完成後驗證輸出時長（abs 差值 > DURATION_THRESHOLD → failed）
│                              #   status='completed'/'failed' 時原子性清除 is_processing 旗標
│                              #   retry_count 在 update_task_status(failed) 時遞增（非重新排入時）
│                              #   更新任務狀態：pending → processing → completed/failed
│                              #   啟動時執行 cleanup_orphaned_flags()，清理兩種殭屍狀態：
│                              #     type-1：status=processing + is_processing=TRUE（crash 未釋放鎖）
│                              #     type-2：status=processing + is_processing=FALSE + >5min（鎖已釋放但 status 未更新）
│                              #   自動重試失敗任務（每 RETRY_INTERVAL_CYCLES 次 check 執行一次）
│                              #   自動清除過時任務（每次 check 都執行，閾值 STALE_HOURS）
│                              #   每次 stale cleanup 同時 kill 孤兒 ffmpeg：
│                              #     不在本 daemon 子孫樹下、且 source file 有 DB 記錄的 ffmpeg 程序
│                              #   所有 DB 操作透過 TaskRepository（task_manager.py）
│
├── api/
│   └── server.py              # Flask REST + WebSocket API 伺服器
│                              #   讀取 daemon 的 JSON 狀態檔（含 60 秒過期檢查）
│                              #   REST 端點：/api/health、/api/status、/api/progress/{scan,process,system,stats}
│                              #   WebSocket：每 2 秒廣播 scan_progress、process_progress、system_status、task_stats
│                              #   任務統計透過 TaskRepository（task_manager.py）查詢
│
├── monitor_daemons.py         # 終端機監控儀表板（CLI 工具）
│                              #   輪詢 REST API 並以彩色 ASCII 格式呈現
│                              #   顯示 daemon PID/運行時間、掃描/處理進度、任務統計、進度條
│                              #   支援持續監控（-c）與單次顯示兩種模式
│
├── daemon_ctl.py              # 統一管理腳本：scan/process/api 的 start/stop/restart/status/log
│                              #   all 指令同時操作 scan、process 和 api
│
├── scripts/
│   ├── install_daemons.sh     # 安裝腳本：將 service 模板替換後安裝至 /etc/systemd/system/
│   ├── logrotate.conf         # logrotate 設定範本（進階選用；預設使用 Python RotatingFileHandler）
│   ├── video-scanner.service  # scan_daemon 的 systemd 服務模板
│   ├── video-processor.service # process_daemon 的 systemd 服務模板
│   └── video-api.service      # API 伺服器的 systemd 服務模板
│
├── sql_dialect.py             # SQL 方言抽象層（Factory Method 模式）
├── .env.sample                # 設定範本（含所有可用變數與說明，含 DB_TYPE / DB_PATH）
└── README.md                  # 本文件
```

---

## 系統運作流程

```
[ 檔案系統 ]
      │  （INPUT_DIRECTORY 輸入目錄）
      ▼
[ 掃描 Daemon ]  ──── ffprobe（僅全新檔案）────► [ 資料庫：conversion_tasks ]
 (scan_daemon.py)   DB/stat 檢查已知檔案               │  status='pending'（待處理）
                    避免重複存取 NFS                     │  MariaDB 或 SQLite（DB_TYPE）
[ 處理 Daemon ] ◄──────── 每 CHECK_INTERVAL 秒輪詢 ────┘
 (process_daemon.py)  retry_count=0 優先取出
      │  工作執行緒（最多 MAX_WORKERS 個）
      │  取得列鎖（is_processing=TRUE）
      ▼
[ converter.py ] ──── ffmpeg ────► OUTPUT_DIRECTORY/480p_<stem>.mp4（mp4 輸入）
                                              或 480p_<stem>_<ext>.mp4（其他格式）
      │  watchdog thread：stall timeout（無進度 FFMPEG_STALL_TIMEOUT 秒）
      │             ：absolute timeout（動態 = 時長 × FFMPEG_TIMEOUT_MULTIPLIER × bitrate_factor，或固定 FFMPEG_TIMEOUT 秒）
      │               bitrate_factor = log2(max(2, src_Mbps / BITRATE_BASELINE_MBPS))；高 bitrate 來源（如 8K RAW）自動延長
      │  失敗時回傳 ffmpeg stderr 最後幾行供診斷
      │
      ├─► ffprobe 驗證輸出時長（abs 差 > DURATION_THRESHOLD → failed + retry）
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
| `is_processing` | 布林鎖旗標，防止重複處理；status 更新為 completed/failed 時原子性清除 |
| `retry_count`、`error_message` | 重試次數（每次標記 failed 時遞增）與錯誤訊息 |
| `start_time`、`end_time` | 任務起訖時間 |

**`processing_lock`**：輔助鎖表（以 `task_id` 為主鍵）

---

## 設定方式（透過 `.env`）

所有執行期設定均來自環境變數，透過專案根目錄的 `.env` 檔載入。

**快速開始**：複製範本並填入實際值：

```bash
cp .env.sample .env
# 編輯 .env，至少填入資料庫連線資訊與輸入/輸出目錄
```

**SQLite 快速試用**（無需安裝 MariaDB）：

```bash
cp .env.sample .env
# 在 .env 中設定：
#   DB_TYPE=sqlite
#   DB_PATH=./data/converter.db
python3 init_db.py          # 自動建立 ./data/converter.db 及所有資料表
```

> SQLite 適合單機輕量部署或開發測試；生產環境多機部署建議使用 MariaDB。

| 變數 | 說明 |
|---|---|
| `DB_TYPE` | 資料庫後端：`mariadb`（預設）或 `sqlite`。選 `sqlite` 時只需設定 `DB_PATH`，無需 DB_HOST 等連線設定 |
| `DB_HOST`、`DB_PORT`、`DB_USER`、`DB_PASSWORD`、`DB_NAME` | MariaDB 連線設定（`DB_TYPE=mariadb` 時必填） |
| `DB_PATH` | SQLite 資料庫檔案路徑（`DB_TYPE=sqlite` 時生效，預設：`./data/converter.db`；`:memory:` 表示純記憶體，僅供測試） |
| `INPUT_DIRECTORY` | 輸入影片目錄 |
| `OUTPUT_DIRECTORY` | 輸出目錄 |
| `SUPPORTED_EXTENSIONS` | 支援的副檔名（預設：`.mp4,.mkv,.avi,.mov,.flv,.wmv,.m4v,.webm`） |
| `MIN_RESOLUTION` | 最低解析度（預設：`481`，即跳過 ≤ 480p 的檔案） |
| `IGNORE_DIRECTORIES` | 掃描時略過的目錄，逗號分隔。支援兩種格式：<br>• **絕對路徑**（`/mnt/nas/archive`）：精確前綴比對，只忽略該路徑本身及其子目錄<br>• **相對路徑**（`@Recycle`、`temp/cache`）：比對路徑中連續的目錄名稱序列，忽略掃描樹任意位置的同名目錄（單層或多層皆支援） |
| `IGNORE_OUTPUT_DIR` | 設為 `true` 時自動忽略 `OUTPUT_DIRECTORY`，無需在 `IGNORE_DIRECTORIES` 重複列出（預設：`false`；`.env.sample` 預設為 `true`） |
| `MAX_WORKERS` | 最大工作執行緒數 |
| `SCAN_INTERVAL` | 掃描間隔（秒，預設 300；NFS 環境建議 1800） |
| `CHECK_INTERVAL` | 任務輪詢間隔（秒） |
| `MAX_RETRIES` | 失敗任務最大重試次數（預設：`3`） |
| `RETRY_INTERVAL_CYCLES` | 每幾個 check cycle 執行一次重試（預設：`10`） |
| `STALE_HOURS` | 任務卡在 processing 超過幾小時視為過時（預設：`1`，NFS 長時轉檔建議 `4` 以上） |
| `DURATION_THRESHOLD` | 輸出檔長度驗證閾值（秒）：輸出與來源時長差超過此值（abs）則視為不完整並重新加入佇列；設 `0` 停用驗證（預設：`2.0`） |
| `FFMPEG_TIMEOUT` | ffmpeg 整體轉檔絕對上限（秒）；`0`（預設）= 動態模式（依 `FFMPEG_TIMEOUT_MULTIPLIER × 影片時長 × bitrate_factor` 自動計算）；`> 0` = 固定秒數 |
| `FFMPEG_TIMEOUT_MULTIPLIER` | 動態 timeout 倍數（`FFMPEG_TIMEOUT=0` 時生效）；`timeout = max(FFMPEG_TIMEOUT_MIN, 時長 × 此值 × bitrate_factor)`（預設：`2.0`） |
| `FFMPEG_TIMEOUT_MIN` | 動態 timeout 最低保障秒數，避免極短影片 timeout 過短（預設：`300`） |
| `BITRATE_BASELINE_MBPS` | 動態 timeout 的 bitrate 修正基準（Mbps）；`bitrate_factor = log2(max(2, src_Mbps / 此值))`，高 bitrate 來源（如 8K 540 Mbps）自動延長 timeout；`0` 停用修正（預設：`10`） |
| `FFMPEG_STALL_TIMEOUT` | ffmpeg 無進度輸出超時（秒）；適用於 NFS I/O stall 導致 ffmpeg 停住但不退出的情況；設 `0` 停用（預設：`300`，即 5 分鐘） |
| `ENABLE_TIME_RESTRICTION` | 設為 `true` 時啟用轉檔時間限制，僅在指定時段內允許轉檔（預設：`false`） |
| `ALLOWED_START_TIME` | 允許轉檔的開始時間，`HH:MM` 格式（預設：`22:00`） |
| `ALLOWED_END_TIME` | 允許轉檔的結束時間，`HH:MM` 格式（預設：`06:00`）。支援跨日時段（如 22:00–06:00） |
| `API_SERVER_HOST`、`API_SERVER_PORT`、`API_SERVER_URL` | API 伺服器設定 |
| `LOG_LEVEL` | 日誌等級 |
| `LOG_MAX_BYTES` | log 單檔大小上限（位元組）；超過自動輪替（預設：`10485760`，即 10MB） |
| `LOG_BACKUP_COUNT` | 保留舊 log 檔份數（預設：`5`，總計最多 60MB） |

### 動態 Timeout 說明

`FFMPEG_TIMEOUT=0`（預設動態模式）時，timeout 依下列公式計算：

```
timeout = max(FFMPEG_TIMEOUT_MIN, 時長(s) × FFMPEG_TIMEOUT_MULTIPLIER × bitrate_factor)

bitrate_factor = log2(max(2, src_Mbps / BITRATE_BASELINE_MBPS))
```

`bitrate_factor` 以 log2 scale 平滑修正高 bitrate 來源的轉碼耗時：低 bitrate 影片不受影響（factor = 1.0），高 bitrate（如 8K RAW）則自動延長 timeout，避免轉碼中途被強制終止。

**情境範例**（`FFMPEG_TIMEOUT_MULTIPLIER=2.0`、`BITRATE_BASELINE_MBPS=10`、來源時長 6000s）：

| 來源規格 | src Mbps | bitrate_factor | timeout（6000s × 2.0 × factor） |
|---------|---------|---------------|--------------------------------|
| 低 bitrate（< 10 Mbps） | 2 | 1.0（下限保護） | 12,000s（3.3h） |
| 一般 1080p | 10 | 1.0 | 12,000s（3.3h） |
| 高品質 1080p | 30 | 1.6 | 19,200s（5.3h） |
| 8K 540 Mbps HEVC | 540 | 5.75 | 69,000s（19.2h） |

---

## 部署方式

> ⚠️ **注意：轉檔邏輯已完全移至 daemon。請使用以下 daemon 方式啟動轉檔。**

### 方式一：長駐 Daemon 程序（建議方式）

每個管理腳本均支援 `start`（預設）、`stop`、`restart`、`status` 四個子指令：

```bash
# 啟動
python3 daemon_ctl.py all start        # scan + process + api
python3 daemon_ctl.py scan start
python3 daemon_ctl.py process start
python3 daemon_ctl.py api start

# 停止
python3 daemon_ctl.py all stop
python3 daemon_ctl.py scan stop
python3 daemon_ctl.py process stop
python3 daemon_ctl.py api stop

# 重新啟動
python3 daemon_ctl.py all restart
python3 daemon_ctl.py scan restart
python3 daemon_ctl.py process restart
python3 daemon_ctl.py api restart

# 查看狀態
python3 daemon_ctl.py all status
python3 daemon_ctl.py scan status
python3 daemon_ctl.py process status
python3 daemon_ctl.py api status
```

**`status` 輸出範例：**

```
✅ scan_daemon: running (PID: 43003)
   Last scan  : 2026-03-25T18:07:14
   Files scan : 106091
   Tasks added: 0
   Errors     : 0

✅ process_daemon: running (PID: 43012)
   Last check : 2026-03-25T18:06:42
   Processing : 668  |  Queue: 664
   Completed  : 135  |  Failed: 0
   Workers    : 1/1  |  Errors: 0
   Time limit : 🚫 restricted  (window 22:00 – 06:00,  next in 215m 30s)
```

所有指令也支援 `--foreground`（或 `-f`）旗標，在前景執行（適合除錯或 systemd 管理）：

```bash
python3 daemon_ctl.py scan start --foreground
python3 daemon_ctl.py api start -f
python3 daemon_ctl.py process restart -f
```

### 查閱 Log（`log` 指令）

`log` 指令使用 `less` 開啟 log 檔，支援自由捲動：

```bash
# 查閱一般 log（預設跳至末尾，可上下捲動）
python3 daemon_ctl.py scan log
python3 daemon_ctl.py process log
python3 daemon_ctl.py api log
python3 daemon_ctl.py all log       # 同時開啟所有 log
python3 daemon_ctl.py log           # 同上（shortcut）

# 查閱 error log（-e / --error）
python3 daemon_ctl.py scan log -e
python3 daemon_ctl.py all log -e

# 持續追蹤新增內容（-f / --follow，類似 tail -f）
python3 daemon_ctl.py process log -f
python3 daemon_ctl.py api log -f -e
```

**less 操作快捷鍵：**

| 按鍵 | 動作 |
|------|------|
| `↑` / `↓` / 滑鼠滾輪 | 上下捲動 |
| `PgUp` / `PgDn` | 翻頁 |
| `g` | 跳至開頭 |
| `G` | 跳至末尾 |
| `/關鍵字` | 向下搜尋 |
| `?關鍵字` | 向上搜尋 |
| `n` | 下一個搜尋結果 |
| `N` | 上一個搜尋結果 |
| `F` | 開始追蹤（follow 模式） |
| `Ctrl+C` | 停止追蹤，切回捲動模式 |
| `:n` | 切換到下一個檔案（`all log` 時） |
| `:p` | 切換到上一個檔案（`all log` 時） |
| `q` | 退出 |

### 方式二：即時監控

```bash
python monitor_daemons.py -c
```

透過查詢 REST API 提供即時終端機儀表板。

---

## 指令參數說明（`conv_admin.py`）

`conv_admin.py` 是**資料庫診斷與維護工具**，轉檔邏輯完全移至 daemon。每次只能使用一個指令：

| 指令 | 說明 |
|---|---|
| `--show-dirs` | 預覽輸入目錄結構（含忽略目錄標示） |
| `--stats` | 顯示資料庫任務統計（現在時間、各狀態數量、目前轉檔中任務清單、平均耗時、最近失敗詳情）及時間限制狀態 |
| `--retry-failed` | 手動將失敗任務重置為 pending（僅 retry_count < max_retries） |
| `--reset-maxed-failed` | 手動重設已達重試上限的失敗任務為 pending（retry_count 歸零） |
| `--max-retries N` | 重試次數上限（預設 3，搭配 --retry-failed / --reset-maxed-failed） |
| `--cleanup-stale` | 手動將卡住的 processing 任務標為 failed |
| `--stale-hours N` | 過時閾值（小時，預設 24，搭配 --cleanup-stale 使用） |
| `--failed-limit N` | `--stats` 時印出最近失敗任務的數量（預設 5；設為 0 不印） |
| `--kill-stale-ffmpeg` | Kill 不在 process daemon 子孫樹下且 source file 有 DB 記錄的孤兒 ffmpeg 程序 |
| `--dry-run` | 僅顯示會執行的操作，不實際寫入（支援 `--kill-stale-ffmpeg`、`--reset-task`、`--add-file`） |
| `--reset-task ID [ID ...]` | 將指定任務重設為 pending（retry_count 歸零、清除錯誤訊息） |
| `--add-file FILE [FILE ...]` | 手動將指定影片檔加入轉檔佇列（跳過掃描 daemon） |

## 使用範例

```bash
# 預覽目錄結構（診斷忽略目錄設定）
python3 conv_admin.py --show-dirs

# 查看任務統計（含現在時間、目前轉檔中任務）
python3 conv_admin.py --stats

# 查看任務統計，顯示最近 10 筆失敗任務
python3 conv_admin.py --stats --failed-limit 10

# 查看任務統計，不顯示失敗任務清單
python3 conv_admin.py --stats --failed-limit 0

# 手動重試失敗任務（retry_count < 3）
python3 conv_admin.py --retry-failed

# 重設已達重試上限的失敗任務（retry_count 歸零，重新加入佇列）
python3 conv_admin.py --reset-maxed-failed

# 清除超過 2 小時未完成的過時任務
python3 conv_admin.py --cleanup-stale --stale-hours 2

# 預覽孤兒 ffmpeg（不實際 kill）
python3 conv_admin.py --kill-stale-ffmpeg --dry-run

# Kill 孤兒 ffmpeg（不在 process daemon 下且 source file 有 DB 記錄）
python3 conv_admin.py --kill-stale-ffmpeg

# 重設特定任務為 pending（適用於手動修正特定失敗任務）
python3 conv_admin.py --reset-task 42
python3 conv_admin.py --reset-task 42 43 44
python3 conv_admin.py --reset-task 42 43 44 --dry-run

# 手動將影片檔加入佇列（適用於 scan daemon 尚未掃描到的檔案）
python3 conv_admin.py --add-file /path/to/video.mp4
python3 conv_admin.py --add-file /path/to/video1.mp4 /path/to/video2.mkv
python3 conv_admin.py --add-file /path/to/video.mp4 --dry-run
```

---

## 日誌輪替

### 內建輪替（RotatingFileHandler，預設啟用）

daemon 使用 Python `RotatingFileHandler` 自動輪替，**無需任何系統設定**，開箱即用：

| 環境變數 | 預設 | 說明 |
|---|---|---|
| `LOG_MAX_BYTES` | `10485760`（10MB） | 單檔大小上限 |
| `LOG_BACKUP_COUNT` | `5` | 保留舊檔數（含當前檔共最多 6 × 10MB = 60MB） |

超過上限時自動重命名為 `.1`、`.2`…，無需重啟 daemon。

### 系統 logrotate（進階選用）

若偏好以 logrotate 集中管理，可使用 `scripts/logrotate.conf` 範本：

```bash
sudo cp scripts/logrotate.conf /etc/logrotate.d/video-converter
sudo sed -i 's|{{INSTALL_DIR}}|/opt/video-converter|g' /etc/logrotate.d/video-converter
```

> ⚠️ 同時使用兩套機制時，建議在 `.env` 設定 `LOG_MAX_BYTES=0` 停用 Python 層輪替，避免競爭。

---

## Systemd 服務設定

`scripts/` 目錄下提供三個服務模板（以 `{{SERVICE_USER}}` / `{{INSTALL_DIR}}` 作為佔位符），透過安裝腳本自動替換後部署。

### 快速安裝

```bash
# 使用目前使用者與目前目錄（預設）
bash scripts/install_daemons.sh

# 自訂使用者與安裝路徑
bash scripts/install_daemons.sh --user myuser --dir /opt/video-converter

# 解除安裝
bash scripts/install_daemons.sh --uninstall
```

安裝後啟動服務：

```bash
sudo systemctl start video-scanner video-processor video-api

# 查看狀態
sudo systemctl status video-scanner video-processor video-api

# 查看 log
journalctl -u video-scanner  -f
journalctl -u video-processor -f
journalctl -u video-api       -f
```

### 服務說明

| 服務 | 對應腳本 | 說明 |
|---|---|---|
| `video-scanner` | `daemon_ctl.py scan start` | 定期掃描目錄，發現新影片加入 DB |
| `video-processor` | `daemon_ctl.py process start` | 從 DB 取出 pending 任務，呼叫 ffmpeg 轉檔 |
| `video-api` | `daemon_ctl.py api` | REST API + WebSocket 即時狀態推送 |

> **注意**：`EnvironmentFile` 指向 `{{INSTALL_DIR}}/.env`，請確認 `.env` 已正確設定後再啟動服務。

---

## NFS 環境建議設定

本系統設計於 NFS 環境下運行，讀取來源影片與寫入輸出檔案均透過 NFS。以下為針對大檔（單檔 10GB+）影片轉檔的建議設定。

### NFS Mount 參數

建議於 `/etc/fstab` 加入以下參數（已驗證於 NFSv4.1）：

```
<server>:/path  <mountpoint>  nfs4  rw,noatime,vers=4.1,rsize=1048576,wsize=1048576,hard,proto=tcp,timeo=600,retrans=5,_netdev  0 0
```

| 參數 | 建議值 | 說明 |
|------|--------|------|
| `vers=4.1` | NFSv4.1 | 支援 session 復原，比 v3 更穩定 |
| `rsize=1048576` | 1MB | 最大讀取區塊，大檔效率最佳 |
| `wsize=1048576` | 1MB | 最大寫入區塊，大檔效率最佳 |
| `hard` | hard | I/O 錯誤時無限重試，避免轉檔中途中斷 |
| `proto=tcp` | tcp | 比 UDP 更可靠，適合長時間傳輸 |
| `timeo=600` | 60s | NFS 請求逾時（單位 0.1s），60s 為合理上限 |
| `retrans=5` | 5 | 逾時後重試次數 |
| `noatime` | — | 不更新存取時間，減少 NFS metadata 寫入 |

### Linux TCP Socket 緩衝（強烈建議）

預設 TCP socket 緩衝只有 ~200KB，在讀取大型 NFS 檔案時容易造成 TCP 窗口縮小、讀取停頓，進而導致 ffmpeg 輸出不完整。

在 `/etc/sysctl.conf` 加入：

```ini
net.core.rmem_max = 16777216
net.core.wmem_max = 16777216
net.ipv4.tcp_rmem = 4096 131072 16777216
net.ipv4.tcp_wmem = 4096 131072 16777216
net.ipv4.tcp_window_scaling = 1
```

套用：

```bash
sudo sysctl -p
```

### 為何需要這些設定

本系統在 NFS 環境下最常見的失敗原因是 **NFS I/O 中斷導致 ffmpeg 輸出不完整**：

1. ffmpeg 從 NFS 讀取大型來源檔案（如 4K MXF，單檔 60GB+）
2. TCP 緩衝不足或 NFS 短暫中斷，ffmpeg 停止讀取
3. ffmpeg 以 rc=0 退出，但輸出時長遠短於來源
4. 系統偵測到輸出時長差異，標記為 `failed`（`Incomplete output`）

調大 TCP 緩衝後，大型檔案的連續讀取更穩定，可顯著降低 `Incomplete output` 的發生率。

---

## 已修正的技術問題

### ffmpeg 在 Daemon 環境下誤讀 stdin 提早退出

**症狀：** 轉檔以 rc=0 正常結束、有完整的 `[libx264] kb/s:XXX` 統計，但輸出時長遠短於來源，且每個任務的停止點不固定。

**根本原因：**

`python-daemon` 的 `DaemonContext` 會將 daemon 的 stdin 設為 `/dev/null`。ffmpeg 子行程繼承此 `/dev/null` 作為 fd 0，但 ffmpeg 啟動時會內部關閉 fd 0（stdin），隨後開啟來源影片檔案，此時 `open()` 取得最小可用描述符——正好是 fd 0。沒有 `-nostdin` 的情況下，ffmpeg 會定期從 fd 0 讀取鍵盤指令；讀到來源檔案的二進位資料中的 `q`（0x71）位元組時，ffmpeg 以為使用者按下了 `q`，觸發優雅退出並以 rc=0 結束。

**修正（`e0e9057`）：** 在 ffmpeg 指令加入 `-nostdin` 旗標，完全停用鍵盤互動讀取。

---

### Stall Timeout 誤判導致長時間轉檔被強制終止

**症狀：** 轉檔時間超過 1200 秒（`FFMPEG_STALL_TIMEOUT`）的任務被標記為 `ffmpeg stall timeout`，即使 ffmpeg 仍在正常運作。

**根本原因：**

ffmpeg 的 progress 訊息以 `\r`（carriage return）結尾，而非 `\n`。Python 的 `readline()` 在二進位模式下只辨識 `\n` 作為行尾，因此所有 `\r` 結尾的 progress 資料都會累積在緩衝區，直到最終出現 `\n` 的統計行（如 `[libx264] kb/s:XXX\n`）才一次返回。對於 60 分鐘以上的轉檔，`readline()` 阻塞超過 1200 秒，stall 偵測機制誤判為卡住並強制終止。

**修正（`3e8e359`）：** 以 `select()` + `os.read(4096)` 非阻塞讀取取代 `readline()`，同時支援 `\r` 與 `\n` 分行。每次解析到 `time=` 即更新 `last_progress_time`，stall 偵測僅在 ffmpeg 真正停止輸出 progress 時才觸發。同時加入 progress callback 節流（每 1% 或每 5 秒才觸發），避免長時間轉檔產生大量不必要的 DB 寫入。

---

## 已知限制

### 輸出路徑命名衝突

輸出檔名規則為：
- `.mp4` 輸入：`480p_{stem}.mp4`（例：`video.mp4` → `480p_video.mp4`）
- 其他格式輸入：`480p_{stem}_{ext}.mp4`（例：`video.mpg` → `480p_video_mpg.mp4`）

若目錄中同時存在如 `clip.mxf` 與 `clip_mxf.mp4`，兩者會對應到相同輸出路徑 `480p_clip_mxf.mp4`，第二個轉檔將靜默覆蓋第一個的輸出檔。資料庫僅對 `input_path` 設有唯一鍵，`output_path` 不受保護。

**建議：** 避免在同一目錄放置 stem 加上副檔名後相同的混合格式檔案（如 `video.mpg` 與 `video_mpg.mp4`）。

---

## 總結

這是一個結構清晰、面向生產環境的批次影片處理系統。各功能模組分工明確：

- **掃描**（`scan_daemon.py`）：檔案探索與任務入列
- **處理**（`process_daemon.py`）：多執行緒轉碼執行
- **任務管理**（`task_manager.py`）：TaskRepository — 集中管理所有任務 DB 操作的單一入口
- **持久化**（`db_manager.py` + `sql_dialect.py`）：雙後端（MariaDB / SQLite）任務佇列與狀態追蹤；SQL 方言差異由 Factory Method 模式封裝，新增後端只需實作 `SqlDialect` 子類別並登錄一行
- **可觀測性**（`api/server.py`）：REST API + WebSocket 即時推送
- **監控**（`monitor_daemons.py`）：終端機儀表板

資料庫列鎖機制（`is_processing` 旗標 + `processing_lock` 表）確保多個工作執行緒同時運行時不會重複處理同一個檔案。鎖的生命週期統一由 `worker()` 管理；`status='completed'/'failed'` 的 UPDATE 同時原子性清除 `is_processing`，即使程序崩潰也能在下次啟動時由 `cleanup_orphaned_flags()` 清理。`retry_count` 代表已嘗試次數，每次標記 `failed` 時遞增，`MAX_RETRIES=N` 表示最多執行 N 次。

**殭屍任務清理**（`cleanup_orphaned_flags`，daemon 啟動時執行）：

| 殭屍類型 | 狀態 | 成因 | 處置 |
|---|---|---|---|
| Type-1 | `status=processing` + `is_processing=TRUE` | Daemon crash，鎖未釋放 | → `pending`（重設 is_processing + status） |
| Type-2 | `status=processing` + `is_processing=FALSE` + 超過 5 分鐘 | `release_task_lock()` 執行後 crash，`update_task_status` 未執行 | → `pending`（重設 status） |
