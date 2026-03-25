#!/bin/bash

# 安裝 daemon 服務
set -e

echo "Installing video converter daemons..."

# 建立必要的目錄
sudo mkdir -p /var/log/video-converter
sudo mkdir -p /var/run/video-converter
sudo chown -R $USER:$USER /var/log/video-converter
sudo chown -R $USER:$USER /var/run/video-converter

# 複製 systemd 服務檔案
sudo cp scripts/video-scanner.service /etc/systemd/system/
sudo cp scripts/video-processor.service /etc/systemd/system/
sudo cp scripts/video-api.service /etc/systemd/system/

# 設定權限
sudo chmod 644 /etc/systemd/system/video-scanner.service
sudo chmod 644 /etc/systemd/system/video-processor.service
sudo chmod 644 /etc/systemd/system/video-api.service

# 重新載入 systemd
sudo systemctl daemon-reload

# 啟用服務
sudo systemctl enable video-scanner.service
sudo systemctl enable video-processor.service
sudo systemctl enable video-api.service

echo "Daemons installed successfully!"
echo ""
echo "To start the services:"
echo "  sudo systemctl start video-scanner"
echo "  sudo systemctl start video-processor"
echo "  sudo systemctl start video-api"
echo ""
echo "To check status:"
echo "  sudo systemctl status video-scanner"
echo "  sudo systemctl status video-processor"
echo "  sudo systemctl status video-api"
echo ""
echo "To view logs:"
echo "  journalctl -u video-scanner -f"
echo "  journalctl -u video-processor -f"
echo "  journalctl -u video-api -f"
