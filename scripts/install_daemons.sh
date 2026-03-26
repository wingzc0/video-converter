#!/bin/bash
# 安裝 video-converter systemd 服務
# 用法：bash scripts/install_daemons.sh [--user USER] [--dir DIR] [--uninstall]
#
# 預設值：
#   --user  目前登入的使用者（$USER）
#   --dir   此腳本所在目錄的上層（即專案根目錄）
#
# 範例：
#   bash scripts/install_daemons.sh
#   bash scripts/install_daemons.sh --user myuser --dir /opt/bcvnas-converter
#   bash scripts/install_daemons.sh --uninstall

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

SERVICE_USER="${USER}"
INSTALL_DIR="${PROJECT_DIR}"
UNINSTALL=false

# 解析參數
while [[ $# -gt 0 ]]; do
    case "$1" in
        --user)  SERVICE_USER="$2"; shift 2 ;;
        --dir)   INSTALL_DIR="$2";  shift 2 ;;
        --uninstall) UNINSTALL=true; shift ;;
        *) echo "Unknown option: $1"; exit 1 ;;
    esac
done

SERVICES=(video-scanner video-processor video-api)

# ------------------------------------------------------------------
# 解除安裝
# ------------------------------------------------------------------
if $UNINSTALL; then
    echo "Uninstalling video-converter services..."
    for svc in "${SERVICES[@]}"; do
        if systemctl is-active --quiet "${svc}.service" 2>/dev/null; then
            sudo systemctl stop "${svc}.service"
        fi
        if systemctl is-enabled --quiet "${svc}.service" 2>/dev/null; then
            sudo systemctl disable "${svc}.service"
        fi
        sudo rm -f "/etc/systemd/system/${svc}.service"
        echo "  Removed ${svc}.service"
    done
    sudo systemctl daemon-reload
    echo "Done."
    exit 0
fi

# ------------------------------------------------------------------
# 安裝前確認
# ------------------------------------------------------------------
echo "================================================"
echo "  Video Converter - Systemd Service Installer"
echo "================================================"
echo "  Service user : ${SERVICE_USER}"
echo "  Install dir  : ${INSTALL_DIR}"
echo ""

if [[ ! -d "${INSTALL_DIR}" ]]; then
    echo "ERROR: Install directory not found: ${INSTALL_DIR}"
    exit 1
fi
if [[ ! -f "${INSTALL_DIR}/.env" ]]; then
    echo "WARNING: .env file not found at ${INSTALL_DIR}/.env"
    echo "         Create it before starting the services."
fi
if ! id -u "${SERVICE_USER}" &>/dev/null; then
    echo "ERROR: User not found: ${SERVICE_USER}"
    exit 1
fi

read -r -p "Proceed with installation? [y/N] " confirm
[[ "${confirm,,}" == "y" ]] || { echo "Aborted."; exit 0; }

# ------------------------------------------------------------------
# 安裝服務
# ------------------------------------------------------------------
for svc in "${SERVICES[@]}"; do
    TEMPLATE="${SCRIPT_DIR}/${svc}.service"
    if [[ ! -f "${TEMPLATE}" ]]; then
        echo "  SKIP: template not found: ${TEMPLATE}"
        continue
    fi

    # 替換佔位符後寫入 /etc/systemd/system/
    sed \
        -e "s|{{SERVICE_USER}}|${SERVICE_USER}|g" \
        -e "s|{{INSTALL_DIR}}|${INSTALL_DIR}|g" \
        "${TEMPLATE}" \
        | sudo tee "/etc/systemd/system/${svc}.service" > /dev/null

    sudo chmod 644 "/etc/systemd/system/${svc}.service"
    echo "  Installed ${svc}.service"
done

sudo systemctl daemon-reload

for svc in "${SERVICES[@]}"; do
    if [[ -f "/etc/systemd/system/${svc}.service" ]]; then
        sudo systemctl enable "${svc}.service"
        echo "  Enabled  ${svc}.service"
    fi
done

echo ""
echo "Installation complete!"
echo ""
echo "Start services:"
echo "  sudo systemctl start video-scanner video-processor video-api"
echo ""
echo "Check status:"
echo "  sudo systemctl status video-scanner video-processor video-api"
echo "  python3 ${INSTALL_DIR}/daemon_ctl.py all status"
echo ""
echo "View logs:"
echo "  journalctl -u video-scanner  -f"
echo "  journalctl -u video-processor -f"
echo "  journalctl -u video-api       -f"

