@echo off
cd /d "%~dp0"
echo KRX login setup. Your password will be stored in Windows Credential Manager.
python .\krx_etf_monitor.py setup-krx-login
echo.
echo Finished. You can close this window.
pause
