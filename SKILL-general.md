# Engineering Skill — 工程思路精要

個人工程慣例，適用於任何後端 / 系統維運專案。

---

## 1. 調查方法論

**先收集數據，不要猜**

```
log 分析 → DB 統計 → 手動重現 → 排查環境差異
```

- 看 log 前先確認時間範圍和 log level（不要全看）
- 統計錯誤分布再判斷主因（95% 同一類 → 先解決那類）
- 手動測試成功、自動化仍失敗 → 問題在環境、設定、timeout、鎖，不在邏輯

---

## 2. Code Review 紀律

- 每次功能完成後做 review，視為獨立 phase，不是最後一步
- 多輪 review：每輪找到問題 → 修復 → 下一輪確認並找新問題
- Review 範圍包含**文件**：README、設定範例檔、注釋需與程式碼同步
- 常見 false positive 記下來避免重複踩坑（見下方）

**常見 false positive：**
- Python `finally` 遇到 `continue`/`break`/`return` **仍然執行**
- f-string 裡只有 `%s`（資料另外傳入）→ 不是 SQL injection

---

## 3. 狀態機設計

- `status` 欄位轉換必須**原子**：同一個 UPDATE 同時清除相關 flag
- 計數器（如 `retry_count`）在**更新狀態時**遞增，而非在排程重試時
- 設計邊界值：`== 0`（初始）與 `>= max`（放棄）需要明確處理

**典型錯誤：**
- 計數器在 reset 邏輯和 increment 邏輯都修改 → off-by-one
- status 改了但 flag 沒清 → stale detection 誤判

---

## 4. 錯誤訊息設計

**錯誤訊息要能直接指向根因**

| ❌ 模糊 | ✅ 精確 |
|---------|---------|
| `Conversion failed` | `ffmpeg stall timeout (300s without progress)` |
| `Incomplete output` | `Incomplete output: src=1862s, out=52s — NFS/read error` |
| `Error processing task` | `Could not verify output (ffprobe=0); marked for retry` |

- 不同根因 → 不同訊息（即使表面症狀相同）
- 在可能的情況下說明「下一步是什麼」（retry? 人工確認? 接受?）
- DB 欄位有長度限制時，截斷應保留最有診斷價值的部分（通常是最後幾行 stderr）

---

## 5. Timeout 設計

兩層保護，職責不同：

| 層次 | 觸發條件 | 用途 |
|------|----------|------|
| **Stall timeout** | N 秒無進度輸出 | 偵測卡住（process 沒 crash 但不動） |
| **Absolute timeout** | 超過總時間上限 | 防止意外無限等待 |

- 動態 timeout：對高成本任務（大檔案、高 bitrate）放大倍數，避免誤殺
- Log 公式：`log2(x)` 平滑極端值，避免線性放大到荒謬數字
- Guard：timeout 永遠 ≥ 最小值（不因來源太小而縮到毫秒）

---

## 6. 防禦性編碼

- 任何可能回傳 `0` / `None` 的函式，**呼叫端必須明確處理**
- 測試 edge case 的優先順序：`0`、`None`、負數、空字串、超長字串
- `try/finally` 確保資源釋放（fd、lock、臨時檔）
- Process 啟動後才能 close pipe；close 前確認 process 已 spawn

---

## 7. 測試隔離

**副作用在 import 時執行 → 測試需提前 patch**

```python
# 模組 import 時呼叫 load_dotenv() 會污染測試環境
with patch('dotenv.load_dotenv'):
    from mymodule import func
```

**Mock 用 path-based dispatcher，不用有序 side_effect list**

```python
# ❌ 脆：call 順序改就壞
mock.side_effect = [mock1, mock2, mock3]

# ✅ 穩：依路徑分派
def dispatcher(path, *a, **kw):
    if path.endswith('/stat'): return stat_mock
    if path.endswith('.pid'):  return pid_mock
```

**`__main__` block 測試用 `runpy`**

```python
import runpy
runpy.run_module('mymodule', run_name='__main__')
```

---

## 8. 文件一致性

**有多份文件時，任何一份都是潛在的謊言**

每次改了程式碼，依序確認：
1. inline comment / docstring
2. README 的設定表格和說明
3. 設定範例檔（`.env.sample` 等）
4. 任何獨立的 CHANGELOG / SKILL.md

常見 drift：預設值、支援的選項列表、腳本名稱、設定 key 的拼字

---

## 9. Commit 紀律

- 每個 commit 只做**一件事**（邏輯上原子）
- Commit message 說明「做了什麼、為什麼」，不只是「改了哪個檔案」
- Fix / feat / docs / refactor 前綴有助於 changelog 生成
- 不把除錯用的 print、臨時測試、log 檔帶進 commit

---

## 10. 隱私紀律

**什麼不進版本控制：**

| 類型 | 處理方式 |
|------|----------|
| Credentials（host、帳密、token） | `.env`（gitignore） |
| 環境特定路徑 | `.env` 的設定變數 |
| 含組織/人名的檔案路徑或目錄名 | 只留在 terminal 或私人 session |
| Log 檔（含真實路徑） | `log/` 進 `.gitignore` |
| Session 工作筆記 | `~/.copilot/session-state/`（不 commit） |

**範例檔（`.env.sample`）只放佔位值：**
```
DB_HOST=localhost
DB_PASS=your_password_here
```

---

## 11. 作業系統 / 環境知識備忘

| 事項 | 說明 |
|------|------|
| Process uptime | 用 `/proc/PID/stat` field 22 + `/proc/uptime`，不用 `st_ctime` 或 `time.time()` |
| inotify | 只適用本機 filesystem，NFS/SMB 上不觸發 |
| NFS 大檔讀取 | TCP buffer 太小（rmem_max < 1MB）會造成 stall；建議 16MB |
| subprocess pipe | `stdout=PIPE` 但不讀 → fd leak；exception 路徑也要 close |
| `finally` + `continue` | Python 保證 `finally` 一定執行 |
| Container metadata（ffprobe 等） | 許多格式的 duration/bitrate 來自 header，截斷的檔案仍回傳 header 宣稱值；無法僅靠 metadata 判斷檔案完整性 |
