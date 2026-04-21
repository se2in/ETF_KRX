@echo off
cd /d "%~dp0"
echo This will register two Windows Scheduled Tasks:
echo - ETF KRX Publish 0835 at 08:35
echo - ETF KRX Publish 0905 at 09:05
echo.

taskscheduler /? >nul 2>nul

schtasks /Create /TN "ETF KRX Publish 0835" /TR "\"%~dp0run_collect_publish.bat\"" /SC DAILY /ST 08:35 /F
schtasks /Create /TN "ETF KRX Publish 0905" /TR "\"%~dp0run_collect_publish.bat\"" /SC DAILY /ST 09:05 /F

echo.
echo Scheduled tasks were created.
echo You can close this window.
pause
