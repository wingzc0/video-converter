Crontab 設定
# 每天晚上 10:00 掃描新檔案並開始轉檔
0 22 * * * cd /path/to/video-converter && /usr/bin/python3 main.py --no-interactive --verbose >> /var/log/video-converter.log 2>&1

# 每天早上 6:00 檢查是否有未完成的任務
0 6 * * * cd /path/to/video-converter && /usr/bin/python3 main.py --process-only --no-interactive --verbose >> /var/log/video-converter.log 2>&1

# 每天中午 12:00 只掃描新檔案（不處理）
0 12 * * * cd /path/to/video-converter && /usr/bin/python3 main.py --scan-only --no-interactive --verbose >> /var/log/video-converter-scan.log 2>&1

# 每天晚上 10:00 處理 pending 任務
0 22 * * * cd /path/to/video-converter && /usr/bin/python3 main.py --process-only --no-interactive --verbose --process-pending >> /var/log/video-converter.log 2>&1

# 每天早上 6:00 清理過時任務並重試失敗任務
0 6 * * * cd /path/to/video-converter && /usr/bin/python3 main.py --process-only --no-interactive --verbose --cleanup-stale --retry-failed >> /var/log/video-converter-maintenance.log 2>&1

====================
參數說明
--process-pending     # 處理 pending 任務（預設啟用）
--no-process-pending  # 不處理 pending 任務
--retry-failed        # 重試失敗的任務（最多 3 次）
--cleanup-stale       # 清理過時的任務
--stale-hours N       # 設定過時任務的時間閾值（小時，預設 24）
--scan-only           # 只掃描目錄並添加任務，不進行轉檔
--process-only        # 只處理現有任務，不掃描新檔案
--no-interactive      # 非互動模式，不顯示目錄結構也不等待確認
--force               # 強制執行，跳過設定驗證
--force-overwrite     # 覆蓋已存在的輸出檔案
--skip-low-resolution # 跳過解析度已在 480p 或以下的檔案
--verbose             # 詳細輸出，顯示每個檔案的處理過程
--quiet               # 安靜模式，只顯示錯誤訊息
--max-workers N       # 覆蓋 .env 中的 MAX_WORKERS 設定

====================
範例
python main.py

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

====================
Logrotate 設定
/var/log/video-converter*.log {
    daily
    rotate 30
    compress
    delaycompress
    missingok
    notifempty
    create 644 www-data www-data
}

====================
Systemd Ini 設定
[Unit]
Description=Video Converter Service
After=network.target mariadb.service

[Service]
Type=simple
User=videoconverter
Group=videoconverter
WorkingDirectory=/path/to/video-converter
Environment=PYTHONUNBUFFERED=1
ExecStart=/usr/bin/python3 main.py --process-only --no-interactive --process-pending --retry-failed --cleanup-stale
Restart=on-failure
RestartSec=30
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target

====================
啟用 Systemd Ini 指令
sudo systemctl daemon-reload
sudo systemctl enable video-converter.service
sudo systemctl start video-converter.service
