# SKILL.md — 開發思路與操作慣例

本文件記錄本專案的開發慣例、除錯思路、以及歷史決策，供日後維護和繼續開發時參考。

---

## 目錄

1. [隱私考量](#隱私考量)
2. [系統概覽](#系統概覽)
3. [除錯與調查方法論](#除錯與調查方法論)
4. [Code Review 慣例](#code-review-慣例)
5. [NFS 特有知識](#nfs-特有知識)
6. [Task 狀態管理](#task-狀態管理)
7. [ffmpeg 轉檔相關](#ffmpeg-轉檔相關)
8. [測試慣例](#測試慣例)
9. [文件一致性](#文件一致性)
10. [Commit 慣例](#commit-慣例)
11. [Daemon 管理](#daemon-管理)
12. [錯誤訊息設計](#錯誤訊息設計)
13. [常用 DB 查詢](#常用-db-查詢)

---

## 隱私考量

本文件是公開 repository 的一部分，**以下資訊不應出現在任何已提交的檔案中**：

| 類型 | 範例 | 正確處理方式 |
|------|------|------|
| NFS server hostname / IP | `nas.example.org` | 放在 `.env`（已 gitignore） |
| NFS export 路徑 | `/share/video` | 放在 `.env` 的 `INPUT_DIRECTORY` |
| 本機掛載點（若含組織識別） | `/ORGNAME` | 統一以 `$INPUT_DIRECTORY` 指稱 |
| DB 主機 / 帳號 / 密碼 | `DB_HOST`, `DB_USER`, `DB_PASS` | 放在 `.env` |
| 影片目錄或檔名（含組織、人名） | 頻道名稱、講師名稱 | 除錯時僅放在私人 session，不 commit |
| 任何 API key / token | — | 放在 `.env` |

### 操作慣例

- **log 檔**：`log/` 已加入 `.gitignore`，含有真實路徑，不要 commit
- **`.env` 檔**：包含所有機密設定，只 commit `.env.sample`（佔位值）
- **除錯輸出**：含真實路徑的 ffprobe/ffmpeg 輸出只保留在本機 terminal，不放入 commit message 或 SKILL.md
- **Session 筆記**：`~/.copilot/session-state/` 屬於私人工作空間，不 commit 至 repo

---

## 系統概覽

- **目的**：掃描 NFS 掛載的 NAS（掛載點見 `.env` 的 `INPUT_DIRECTORY`），將 1080p 影片批次轉換為 480p H.264
- **架構**：三個 daemon（`scan_daemon`、`process_daemon`、`api_server`）+ `conv_admin.py` 管理工具
- **資料庫**：MariaDB（主要）或 SQLite（透過 `sql_dialect.py` 抽象層支援兩者）
- **NFS 掛載**：NFSv4.1，`rsize/wsize=1MB`，`hard`（詳細 host/路徑見 `.env`，不放入版本控制）

---

## 除錯與調查方法論

### 原則：先看 log，再查 DB，再手動測試

1. **看 log** — 確認錯誤模式和發生頻率
   ```bash
   tail -n 100 log/processor.log
   grep "WARNING\|ERROR" log/processor.log | tail -50
   grep "Incomplete output" log/processor.log | wc -l
   ```

2. **查 DB** — 統計失敗原因分布
   ```python
   from db_manager import db_manager
   rows = db_manager.execute_query(
       "SELECT error_message, COUNT(*) c FROM conversion_tasks "
       "WHERE status='failed' GROUP BY error_message ORDER BY c DESC",
       fetch=True
   )
   ```

3. **手動測試** — 確認來源檔案是否可讀、手動 ffmpeg 能否成功
   ```bash
   ffprobe -v quiet -show_entries format=duration,bit_rate -of json "file.mp4"
   ffmpeg -nostdin -i "input.mp4" -t 10 -c:v libx264 /tmp/test_out.mp4
   ```

4. **確認環境** — 手動測試成功、daemon 仍失敗時，排查 daemon 層問題（env 變數、timeout、鎖狀態）

### 不要急著猜原因，先收集數據

- `completed` 數量減少 → 先查 requeue 紀錄，再找 `stale_hours` 設定
- 大量 `Incomplete output` → 先分析輸出時長分布，再推論是 ffmpeg timeout 還是 NFS truncation
- 少數特殊失敗 → `ffprobe` 檢查來源完整性，再查 `retry_count` 歷史

---

## Code Review 慣例

### 每次實作完後進行 code review

- 使用 `/review` 指令進行系統性 code review
- review 通常分多輪：每輪找到 bug → 修復 → 下一輪確認並找新問題
- review 可能找到的問題類型：
  - 邏輯錯誤（如 `n % 1 == 1` 永遠為 False）
  - 資源洩漏（pipe fd 未關閉）
  - 狀態不一致（`is_processing` 未原子清除）
  - 例外路徑缺漏（`task_done()` 在例外時未呼叫）

### review README 也是正式 review 的一部分

```
/review readme
/review readme & comment & .env.sample 一致性
```

### 常見 false positive（不需修改）

詳見 `SKILL-general.md §2`，本專案額外補充：
- SQL 注入疑慮：f-string 只包含 `%s` 佔位符，資料另外傳入 → 安全（`db_manager.execute_query` 的呼叫模式）

---

## NFS 特有知識

### 已知的 NFS 陷阱

| 問題 | 說明 | 解決方案 |
|------|------|----------|
| `-movflags +faststart` 靜默截斷 | ffmpeg 做兩次寫入（second pass rewrite），NFS 靜默截斷但 ffmpeg 回傳 rc=0 | 移除 `+faststart` |
| NFS 提早回傳 EOF | 高 bitrate 持續讀取時，NFS 中途回傳 EOF，ffmpeg 以為讀完，輸出截斷但 rc=0 | 調大 TCP socket buffer |
| inotify 不適用 NFS | inotify 只有本機 kernel 事件，NFS 上不會觸發 | 改用 `SCAN_INTERVAL` 定時掃描 |
| ffprobe 讀 AVI header | `format=duration` 只讀 header，AVI 截斷檔案 ffprobe 仍回傳 header 宣稱時長 | 需手動播放確認 |

### NFS TCP buffer 建議設定

```ini
# /etc/sysctl.conf
net.core.rmem_max = 16777216
net.core.wmem_max = 16777216
net.ipv4.tcp_rmem = 4096 131072 16777216
net.ipv4.tcp_wmem = 4096 131072 16777216
net.ipv4.tcp_window_scaling = 1
```

套用：`sudo sysctl -p`

### NFS 掃描 I/O 最小化原則

`scan_daemon` 掃描順序（效能最佳化）：
1. DB 查詢 → 2. `completed` task 直接用 `output_path` 檢查（不 ffprobe）→ 3. 檔案 `stat` 存在確認 → 4. 只對**全新**的檔案呼叫 ffprobe

---

## Task 狀態管理

### 狀態機

```
pending → processing → completed
                    ↘ failed → pending (retry)
```

- `retry_count` 在每次標記 `failed` 時 **+1**（在 `update_task_status()` 內）
- `MAX_RETRIES=3` → 最多共 4 次嘗試（0, 1, 2, 3 次失敗後放棄）
- `retry_failed_tasks()` 只重設 `status='pending'`，不碰 `retry_count`

### 關鍵設定

| 設定 | 說明 | 典型值 |
|------|------|--------|
| `STALE_HOURS` | 超過此時間標為 failed 重試 | `4`（小時） |
| `RETRY_INTERVAL_CYCLES` | 每幾個 check cycle 重試一次 failed tasks | `10` |
| `MAX_RETRIES` | 最多重試次數 | `3` |
| `DURATION_THRESHOLD` | 輸出與來源時長差異容忍值（秒） | `2.0` |
| `SCAN_INTERVAL` | scan_daemon 掃描間隔（秒） | `300`（NFS 建議 `1800`） |
| `CHECK_INTERVAL` | process_daemon 輪詢間隔（秒） | `300` |

### 手動操作

```bash
# 查看特定任務完整資訊（DB 欄位 + 磁碟狀態）
python3 conv_admin.py --task-info 17879
python3 conv_admin.py --task-info 17879 17500

# 列出 pending 任務（預設）
python3 conv_admin.py --list-tasks

# 列出 failed 任務（最多 50 筆）
python3 conv_admin.py --list-tasks --status failed --limit 50

# 重置所有 maxed-failed tasks
python3 conv_admin.py --reset-maxed-failed

# 查看統計
python3 conv_admin.py --stats

# 手動標記 completed（截斷檔案已接受）
python3 -c "
from db_manager import db_manager
db_manager.execute_query(
    \"UPDATE conversion_tasks SET status='completed' WHERE id=%s\",
    params=(17879,)
)
"
```

### 任務優先順序

`process_daemon` 取任務順序：`ORDER BY retry_count ASC, created_at ASC`
- 先處理**全新任務**（retry_count=0），再處理重試任務
- 不會讓舊的重試任務無限期阻塞新任務

### 任務鎖機制

`acquire_task_lock()` 使用兩階段設計，避免誤把「追蹤表」當成鎖：

1. **並發控制**：`UPDATE conversion_tasks SET is_processing=TRUE WHERE status='pending' AND is_processing=FALSE` — 行級鎖確保原子性，rowcount=0 表示已被搶走
2. **審計記錄**：`INSERT IGNORE INTO processing_lock` — 僅供除錯查詢，失敗不影響主流程

> `processing_lock` 表**不是**並發控制手段，只是輔助追蹤。

### RETRY_INTERVAL_CYCLES 的 modulo 陷阱

```python
# ❌ 錯誤：cycles=1 時永遠為 False（0 % 1 == 0，不是 1）
if current_cycle % retry_interval_cycles == 1:

# ✅ 正確
if current_cycle % retry_interval_cycles == 0:
```

歷史 bug：`== 1` 在設定值為 1 時導致 retry 邏輯完全不觸發。

---

## ffmpeg 轉檔相關

### Timeout 保護機制

兩層 timeout，在 `converter.py` 的 watchdog thread 實作：

| 參數 | 說明 | 預設 |
|------|------|------|
| `FFMPEG_STALL_TIMEOUT` | 多久無進度輸出即判定卡住 | `300s` |
| `FFMPEG_TIMEOUT` | 絕對時間上限（動態計算） | 依 bitrate 動態 |

### 動態 Timeout 公式

```
bitrate_factor = log2(max(2, src_Mbps / BITRATE_BASELINE_MBPS))
timeout = max(MIN_TIMEOUT, duration × TIMEOUT_MULTIPLIER × bitrate_factor)
```

- `BITRATE_BASELINE_MBPS=10`：低於此 bitrate 的 factor=1.0（不放大）
- 高 bitrate 來源（8K HEVC 540 Mbps）自動獲得更長的 timeout
- 邊界：`bitrate_factor` 永遠 ≥ 1.0（不會縮短 timeout）

### convert_to_480p() 回傳值

```python
success, conv_error = convert_to_480p(input_path, output_path, ...)
# success=True, conv_error=None → 成功
# success=False, conv_error="ffmpeg stall timeout..." → 失敗原因
```

### ffmpeg 必要參數

```python
"-nostdin"  # 必須加，避免 ffmpeg 嘗試讀取 stdin 造成 hang
```

### 轉檔完成後驗證

轉檔成功（rc=0）後仍需驗證輸出時長：
- `src_dur == 0` → 無法讀取來源時長，**保留輸出，跳過驗證**（避免誤刪完整輸出）
- `out_dur == 0` → 輸出損毀，標記 failed 重試
- `abs(src_dur - out_dur) > threshold` → Incomplete output，標記 failed

---

## 測試慣例

### 執行測試

```bash
.venv/bin/python -m pytest tests/ -q
```

### 重要測試模式

**mock `get_video_duration_and_bitrate`，不要 mock `get_video_duration`**（因為後者只是委派前者）：
```python
with patch('converter.get_video_duration_and_bitrate', return_value=(800.0, 36_000_000)):
    ...
```

**`open()` mock 用 path-based dispatcher**（不用 `side_effect` list，避免呼叫順序改變時脆化）：
```python
def open_side_effect(path, *args, **kwargs):
    path_str = str(path)
    if path_str.endswith('/stat'):
        return stat_mock.__enter__.return_value
    elif path_str.endswith('/uptime'):
        return uptime_mock.__enter__.return_value
    ...
```

**`__main__` block 測試使用 `runpy`**：
```python
import runpy
runpy.run_module('init_db', run_name='__main__')
```

**`load_dotenv()` 在 module import 時執行**，測試隔離需要 patch：
```python
with patch('dotenv.load_dotenv'):
    from init_db import init_database
```

### 測試現況

- 417 tests passing（pytest）
- pre-existing failures in `test_scan_daemon.TestScanDirectoryFiltering`（已知，與當前開發無關）

---

## 文件一致性

### README、.env.sample、程式碼、注釋 四者必須一致

review 時常見的不一致：
- 設定的預設值（README 寫錯 `MAX_WORKERS=1`，實際是 `2`）
- 行為說明（`IGNORE_OUTPUT_DIR` 預設值）
- `.env.sample` 缺少新增的環境變數（如 `BITRATE_BASELINE_MBPS`、`SCAN_INTERVAL`）
- `.env.sample` 的 `SUPPORTED_EXTENSIONS` 與 `scan_daemon` 實際掃描的不符

### README 結構

- 設定表格（env vars）與情境說明表格要**分開**，各自有標題和說明
- 動態 timeout 情境表在 `### 動態 Timeout 說明` 小節，獨立於設定表格

---

## Commit 慣例

- 使用**中文**撰寫 commit message
- 每個 commit 只做一件事（原子性）
- 格式範例：
  ```
  修正：PermissionError 處理與 retry_interval_cycles=1 無效問題
  fix: README 與程式碼四處不一致
  feat: bitrate-aware 動態 timeout（BITRATE_BASELINE_MBPS）
  docs: 動態 timeout 情境表獨立為說明小節
  ```
- 每個 commit 結尾加上 co-author trailer：
  ```
  Co-authored-by: Copilot <223556219+Copilot@users.noreply.github.com>
  ```

---

## Daemon 管理

### 控制指令

```bash
python3 daemon_ctl.py <all|scan|process|api> <start|stop|restart|status|log>

# 範例
python3 daemon_ctl.py all restart
python3 daemon_ctl.py process status
python3 daemon_ctl.py process log
```

### 重啟後確認

1. 確認三個 daemon 都有 PID
2. 查 `log/processor.log` 看是否有 `validate_settings` 的設定確認訊息
3. 確認 `processing_lock` 沒有殘留 stale entries

```bash
python3 -c "
from db_manager import db_manager
rows = db_manager.execute_query('SELECT * FROM processing_lock', fetch=True)
print(rows)
"
```

### SQLite vs MariaDB

- 透過 `sql_dialect.py` 的抽象層，`%s` 佔位符在兩個 DB 都可用
- SQLite 適合測試環境和輕量部署
- MariaDB 用於生產環境（支援高並發、deadlock 偵測）

### 時間限制（Time Restriction）

`process_daemon` 可設定只在特定時段轉檔（例如夜間避開上班時間）：

```bash
ENABLE_TIME_RESTRICTION=true
ALLOWED_START_TIME=22:00   # 開始時間（24 小時制）
ALLOWED_END_TIME=06:00     # 結束時間（可跨午夜）
```

- 停用時（`false`）：全天候轉檔
- 時間外收到任務：daemon 不取新任務，但不影響 scan_daemon 掃描入庫
- `conv_admin.py --stats` 會顯示目前是否在允許時段內

### 孤立 ffmpeg 處理

process_daemon 的 stale cleanup 會**自動**殺掉與 stale task 對應的孤立 ffmpeg 進程。手動操作：

```bash
python3 conv_admin.py --kill-stale-ffmpeg --dry-run  # 先確認
python3 conv_admin.py --kill-stale-ffmpeg             # 實際執行
```

> **注意**：若 daemon 以不同使用者身分執行（例如透過 systemd），`os.kill()` 可能拋出 `PermissionError`。程式會記錄 warning 並繼續處理，不中斷清理流程。

---

## 錯誤訊息設計

### 原則：error message 要能幫助判斷根因

| 情境 | 好的錯誤訊息 |
|------|------|
| ffmpeg 被 stall timeout 殺掉 | `ffmpeg stall timeout (300s without progress)` |
| 輸出不完整（ffmpeg rc=0） | `Incomplete output: src=1862.7s, out=52.9s, diff=1809.8s > threshold=2.0s` |
| 輸出無法讀取 | `Could not verify output duration (ffprobe returned 0); marked for retry` |
| 來源無法讀取 | `Could not read source duration (ffprobe returned 0), skipping validation` |

### 注意事項

- `error_message` DB 欄位限 1000 字元（在 `update_task_status()` 截斷）
- `Incomplete output` 目前無法自動區分「NFS 提早 EOF」與「source 本身截斷」
  - AVI 的 `ffprobe format=duration` 只讀 header，不掃實際內容
  - 需手動確認：請人工確認檔案能否正常播放到宣稱的時長

---

## 常用 DB 查詢

```python
from db_manager import db_manager

# 查看所有 failed tasks
db_manager.execute_query(
    "SELECT id, input_path, retry_count, error_message FROM conversion_tasks WHERE status='failed'",
    fetch=True
)

# 統計各狀態數量
db_manager.execute_query(
    "SELECT status, COUNT(*) c FROM conversion_tasks GROUP BY status",
    fetch=True
)

# 查看特定 task 完整資訊
db_manager.execute_query(
    "SELECT * FROM conversion_tasks WHERE id=%s",
    params=(17500,), fetch=True
)

# 重置單一 task
db_manager.execute_query(
    "UPDATE conversion_tasks SET status='pending', retry_count=0 WHERE id=%s",
    params=(17500,)
)

# 查詢 processing_lock 殘留
db_manager.execute_query("SELECT * FROM processing_lock", fetch=True)
```
