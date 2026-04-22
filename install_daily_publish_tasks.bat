@echo off
cd /d "%~dp0"
echo This will register one Windows Scheduled Task:
echo - ETF KRX Publish 0925 at 09:25
echo.

echo Removing old 08:35 and 09:05 tasks if they exist...
schtasks /End /TN "ETF KRX Publish 0835" >nul 2>nul
schtasks /End /TN "ETF KRX Publish 0905" >nul 2>nul
schtasks /Delete /TN "ETF KRX Publish 0835" /F >nul 2>nul
schtasks /Delete /TN "ETF KRX Publish 0905" /F >nul 2>nul

echo Creating 09:25 task...
schtasks /Create /TN "ETF KRX Publish 0925" /TR "\"%~dp0run_collect_publish.bat\"" /SC DAILY /ST 09:25 /F
if errorlevel 1 (
  echo ERROR: Scheduled task creation failed.
  pause
  exit /b 1
)

echo.
echo Scheduled task was created at 09:25.
echo You can close this window.
pause
