@echo off
cd /d "%~dp0"
echo This will register one Windows Scheduled Task:
echo - ETF KRX Publish 0930 at 09:30
echo - ETF KRX Publish 1030 at 10:30
echo - ETF KRX Publish 1600 at 16:00
echo.

echo Removing old tasks if they exist...
schtasks /End /TN "ETF KRX Publish 0835" >nul 2>nul
schtasks /End /TN "ETF KRX Publish 0905" >nul 2>nul
schtasks /End /TN "ETF KRX Publish 0925" >nul 2>nul
schtasks /End /TN "ETF KRX Publish 0930" >nul 2>nul
schtasks /End /TN "ETF KRX Publish 1030" >nul 2>nul
schtasks /End /TN "ETF KRX Publish 1600" >nul 2>nul
schtasks /Delete /TN "ETF KRX Publish 0835" /F >nul 2>nul
schtasks /Delete /TN "ETF KRX Publish 0905" /F >nul 2>nul
schtasks /Delete /TN "ETF KRX Publish 0925" /F >nul 2>nul
schtasks /Delete /TN "ETF KRX Publish 0930" /F >nul 2>nul
schtasks /Delete /TN "ETF KRX Publish 1030" /F >nul 2>nul
schtasks /Delete /TN "ETF KRX Publish 1600" /F >nul 2>nul

echo Creating 09:30 task...
schtasks /Create /TN "ETF KRX Publish 0930" /TR "\"%~dp0run_collect_publish.bat\"" /SC DAILY /ST 09:30 /F
if errorlevel 1 (
  echo ERROR: Scheduled task creation failed.
  pause
  exit /b 1
)

echo Creating 10:30 task...
schtasks /Create /TN "ETF KRX Publish 1030" /TR "\"%~dp0run_collect_publish.bat\"" /SC DAILY /ST 10:30 /F
if errorlevel 1 (
  echo ERROR: Scheduled task creation failed.
  pause
  exit /b 1
)

echo Creating 16:00 task...
schtasks /Create /TN "ETF KRX Publish 1600" /TR "\"%~dp0run_collect_publish.bat\"" /SC DAILY /ST 16:00 /F
if errorlevel 1 (
  echo ERROR: Scheduled task creation failed.
  pause
  exit /b 1
)

echo.
echo Scheduled tasks were created at 09:30, 10:30, and 16:00.
echo You can close this window.
pause
