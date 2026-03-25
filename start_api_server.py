#!/usr/bin/env python3
"""
啟動整合的 API 伺服器的腳本
"""
import sys
import os
from pathlib import Path

# 將專案根目錄加入 Python 路徑
project_root = Path(__file__).parent.resolve()
sys.path.insert(0, str(project_root))

from api.server import start_api_server

def main():
    print(f"Starting integrated API server in foreground mode (PID: {os.getpid()})")
    start_api_server()

if __name__ == '__main__':
    main()
