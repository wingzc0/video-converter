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
├── conv_admin.py              # 資料庫診斷與維護工具：目錄預覽、任務統計、手動重試/清理
│
├── converter.py               # 核心 FFmpeg 封裝模組
│                              #   get_video_info()     – 用 ffprobe 取得解析度與元數據
│                              #   convert_to_480p()    – ffmpeg 轉檔，支援進度回調與雙層超時保護
│                              #                          回傳 (success: bool, error: str | None)
│                              #                          失敗時 error 包含 ffmpeg stderr 最後幾行
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
├── task_manager.py            # 任務資料庫操作的統一抽象層
│                              #   TaskRepository 類別：封裝所有 conversion_tasks DB 操作
│                              #   get_pending_tasks()、get_task_by_input_path()、get_task_statistics()
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
│   │                          #   遞迴遍歷 INPUT_DIRECTORY
│   │                          #   掃描順序（NFS I/O 最小化）：
│   │                          #     1. DB 查詢 → pending/processing/failed 直接略過
│   │                          #     2. completed → 用 DB 儲存的 output_path 做 exist 檢查（無 ffprobe）
│   │                          #     3. output_path.exists()（單一 stat，跳過 ffprobe）
│   │                          #     4. ffprobe 僅用於全新且無輸出的檔案
│   │                          #   可設定掃描間隔（SCAN_INTERVAL，NFS 環境建議 1800 秒）
│   │                          #   所有 DB 操作透過 TaskRepository（task_manager.py）
│   │
│   └── process_daemon.py      # 處理 Daemon（繼承 BaseDaemon）
│                              #   執行緒池工作模式（預設：1 個工作執行緒，使用 queue.Queue）
│                              #   每 CHECK_INTERVAL 秒輪詢一次資料庫的待處理任務（預設：300 秒）
│                              #   任務排序：retry_count ASC, created_at ASC（全新任務優先）
│                              #   使用資料庫列鎖（is_processing 旗標）防止重複處理
│                              #   worker() 統一管理鎖生命週期（lock_acquired 旗標 + finally 釋放）
│                              #   呼叫 converter.convert_to_480p() 並即時回報進度
│                              #   ffmpeg 雙層超時保護：stall timeout（無進度）+ absolute timeout
│                              #   轉檔完成後驗證輸出時長（abs 差值 > DURATION_THRESHOLD → failed）
│                              #   status='completed'/'failed' 時原子性清除 is_processing 旗標
│                              #   retry_count 在 update_task_status(failed) 時遞增（非重新排入時）
│                              #   更新任務狀態：pending → processing → completed/failed
│                              #   自動重試失敗任務（每 RETRY_INTERVAL_CYCLES 次 check 執行一次）
│                              #   自動清除過時任務（每次 check 都執行，閾值 STALE_HOURS）
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
│                              #   all 指令同時操作 scan 和 process（不含 api）
│                              #   api target 獨立控制 Flask API 伺服器
│
├── scripts/
│   ├── install_daemons.sh     # 安裝腳本：將 service 模板替換後安裝至 /etc/systemd/system/
│   ├── video-scanner.service  # scan_daemon 的 systemd 服務模板
│   ├── video-processor.service # process_daemon 的 systemd 服務模板
│   └── video-api.service      # API 伺服器的 systemd 服務模板
│
├── .env.sample                # 設定範本（含所有可用變數與說明）
└── README.md                  # 本文件
```

---

## 系統運作流程

```
[ 檔案系統 ]
      │  （INPUT_DIRECTORY 輸入目錄）
      ▼
[ 掃描 Daemon ]  ──── ffprobe（僅全新檔案）────► [ MariaDB：conversion_tasks ]
 (scan_daemon.py)   DB/stat 檢查已知檔案               │  status='pending'（待處理）
                    避免重複存取 NFS                     │
[ 處理 Daemon ] ◄──────── 每 CHECK_INTERVAL 秒輪詢 ────┘
 (process_daemon.py)  retry_count=0 優先取出
      │  工作執行緒（最多 MAX_WORKERS 個）
      │  取得列鎖（is_processing=TRUE）
      ▼
[ converter.py ] ──── ffmpeg ────► OUTPUT_DIRECTORY/480p_<檔名>
      │  watchdog thread：stall timeout（無進度 FFMPEG_STALL_TIMEOUT 秒）
      │             ：absolute timeout（FFMPEG_TIMEOUT 秒上限）
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

| 變數 | 說明 |
|---|---|
| `DB_HOST`、`DB_PORT`、`DB_USER`、`DB_PASSWORD`、`DB_NAME` | 資料庫連線設定 |
| `INPUT_DIRECTORY` | 輸入影片目錄 |
| `OUTPUT_DIRECTORY` | 輸出目錄 |
| `SUPPORTED_EXTENSIONS` | 支援的副檔名（預設：`.mp4,.mkv,.avi,.mov,.flv,.wmv,.m4v,.webm`） |
| `MIN_RESOLUTION` | 最低解析度（預設：`481`，即跳過 ≤ 480p 的檔案） |
| `MAX_WORKERS` | 最大工作執行緒數 |
| `SCAN_INTERVAL` | 掃描間隔（秒，預設 300；NFS 環境建議 1800） |
| `CHECK_INTERVAL` | 任務輪詢間隔（秒） |
| `MAX_RETRIES` | 失敗任務最大重試次數（預設：`3`） |
| `RETRY_INTERVAL_CYCLES` | 每幾個 check cycle 執行一次重試（預設：`10`） |
| `STALE_HOURS` | 任務卡在 processing 超過幾小時視為過時（預設：`1`，NFS 長時轉檔建議 `4` 以上） |
| `DURATION_THRESHOLD` | 輸出檔長度驗證閾值（秒）：輸出與來源時長差超過此值（abs）則視為不完整並重新加入佇列；設 `0` 停用驗證（預設：`2.0`） |
| `FFMPEG_TIMEOUT` | ffmpeg 整體轉檔絕對上限（秒）；超過即強制終止並標記失敗；設 `0` 停用（預設：`7200`，即 2 小時） |
| `FFMPEG_STALL_TIMEOUT` | ffmpeg 無進度輸出超時（秒）；適用於 NFS I/O stall 導致 ffmpeg 停住但不退出的情況；設 `0` 停用（預設：`300`，即 5 分鐘） |
| `API_SERVER_HOST`、`API_SERVER_PORT`、`API_SERVER_URL` | API 伺服器設定 |
| `LOG_LEVEL` | 日誌等級 |

---

## 部署方式

> ⚠️ **注意：轉檔邏輯已完全移至 daemon。請使用以下 daemon 方式啟動轉檔。**

### 方式一：長駐 Daemon 程序（建議方式）

每個管理腳本均支援 `start`（預設）、`stop`、`restart`、`status` 四個子指令：

```bash
# 啟動
python3 daemon_ctl.py scan start
python3 daemon_ctl.py process start
python3 daemon_ctl.py api start
python3 daemon_ctl.py all start        # scan + process（不含 api）

# 停止
python3 daemon_ctl.py scan stop
python3 daemon_ctl.py process stop
python3 daemon_ctl.py api stop
python3 daemon_ctl.py all stop

# 重新啟動
python3 daemon_ctl.py scan restart
python3 daemon_ctl.py process restart
python3 daemon_ctl.py api restart

# 查看狀態
python3 daemon_ctl.py scan status
python3 daemon_ctl.py process status
python3 daemon_ctl.py api status
python3 daemon_ctl.py all status
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
| `--stats` | 顯示資料庫任務統計（總數、各狀態數量、平均耗時、失敗詳情） |
| `--retry-failed` | 手動將失敗任務重置為 pending（僅 retry_count < max_retries） |
| `--reset-maxed-failed` | 手動重設已達重試上限的失敗任務為 pending（retry_count 歸零） |
| `--max-retries N` | 重試次數上限（預設 3，搭配 --retry-failed / --reset-maxed-failed） |
| `--cleanup-stale` | 手動將卡住的 processing 任務標為 failed |
| `--stale-hours N` | 過時閾值（小時，預設 24，搭配 --cleanup-stale 使用） |
| `--kill-stale-ffmpeg` | Kill 不在 process daemon 子孫樹下且 source file 有 DB 記錄的孤兒 ffmpeg 程序 |
| `--dry-run` | 僅列出會被 kill 的程序，不實際執行（搭配 --kill-stale-ffmpeg 使用） |

## 使用範例

```bash
# 預覽目錄結構（診斷忽略目錄設定）
python3 conv_admin.py --show-dirs

# 查看任務統計
python3 conv_admin.py --stats

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
```

---

## Logrotate 設定

日誌檔位於 `{{INSTALL_DIR}}/log/`，建議加入 logrotate 以避免檔案無限增長。

建立 `/etc/logrotate.d/video-converter`，內容如下（請將路徑替換為實際安裝目錄）：

```
/opt/video-converter/log/scanner.log
/opt/video-converter/log/scanner_error.log
/opt/video-converter/log/processor.log
/opt/video-converter/log/processor_error.log
/opt/video-converter/log/api.log {
    daily
    rotate 30
    compress
    delaycompress
    missingok
    notifempty
}
```

> daemon 使用 `WatchedFileHandler`，logrotate 輪替後會自動偵測 inode 變化並重新開啟新檔，不需要 `copytruncate`。

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
| `video-scanner` | `start_scan_daemon.py` | 定期掃描目錄，發現新影片加入 DB |
| `video-processor` | `start_process_daemon.py` | 從 DB 取出 pending 任務，呼叫 ffmpeg 轉檔 |
| `video-api` | `daemon_ctl.py api` | REST API + WebSocket 即時狀態推送 |

> **注意**：`EnvironmentFile` 指向 `{{INSTALL_DIR}}/.env`，請確認 `.env` 已正確設定後再啟動服務。

---

## 總結

這是一個結構清晰、面向生產環境的批次影片處理系統。各功能模組分工明確：

- **掃描**（`scan_daemon.py`）：檔案探索與任務入列
- **處理**（`process_daemon.py`）：多執行緒轉碼執行
- **任務管理**（`task_manager.py`）：TaskRepository — 集中管理所有任務 DB 操作的單一入口
- **持久化**（`db_manager.py` + MariaDB）：任務佇列與狀態追蹤
- **可觀測性**（`api/server.py`）：REST API + WebSocket 即時推送
- **監控**（`monitor_daemons.py`）：終端機儀表板

資料庫列鎖機制（`is_processing` 旗標 + `processing_lock` 表）確保多個工作執行緒同時運行時不會重複處理同一個檔案。鎖的生命週期統一由 `worker()` 管理；`status='completed'/'failed'` 的 UPDATE 同時原子性清除 `is_processing`，即使程序在 finally 釋放前崩潰也不會造成任務永久卡死。`retry_count` 代表已嘗試次數，每次標記 `failed` 時遞增，`MAX_RETRIES=N` 表示最多執行 N 次。
