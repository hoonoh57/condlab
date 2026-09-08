@echo off
cd /d %~dp0
if exist "..\venv\Scripts\activate.bat" call ..\venv\Scripts\activate.bat
powershell -NoProfile -Command "$c=Get-NetTCPConnection -LocalPort 8765 -State Listen -ErrorAction SilentlyContinue; if($c){exit 1}else{exit 0}"
if errorlevel 1 (
  echo Server already running. Opening browser.
  start "" http://127.0.0.1:8765
  exit /b 0
)
start "" http://127.0.0.1:8765
python -m uvicorn condlab.server:app --host 127.0.0.1 --port 8765
