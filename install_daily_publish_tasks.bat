@echo off
setlocal enabledelayedexpansion
cd /d "%~dp0"

echo This will register Windows Scheduled Tasks every 30 minutes:
echo - 09:30, 10:00, 10:30, 11:00, 11:30, 12:00, 12:30
echo - 13:00, 13:30, 14:00, 14:30, 15:00, 15:30, 16:00
echo.

echo Removing old tasks if they exist...
for %%T in (
  0830 0835 0905 0925
  0930 1000 1030 1100 1130 1200 1230
  1300 1330 1400 1430 1500 1530 1600
) do (
  schtasks /End /TN "ETF KRX Publish %%T" >nul 2>nul
  schtasks /Delete /TN "ETF KRX Publish %%T" /F >nul 2>nul
)

echo Creating new tasks...
for %%T in (
  0930 1000 1030 1100 1130 1200 1230
  1300 1330 1400 1430 1500 1530 1600
) do (
  set TASK_TIME=%%T
  set TASK_CLOCK=!TASK_TIME:~0,2!:!TASK_TIME:~2,2!
  echo Creating !TASK_CLOCK! task...
  schtasks /Create /TN "ETF KRX Publish %%T" /TR "\"%~dp0run_collect_publish.bat\"" /SC DAILY /ST !TASK_CLOCK! /F
  if errorlevel 1 (
    echo ERROR: Scheduled task creation failed at !TASK_CLOCK!.
    pause
    exit /b 1
  )
)

echo.
echo Scheduled tasks were created every 30 minutes from 09:30 to 16:00.
echo You can close this window.
pause
