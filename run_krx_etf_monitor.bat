@echo off
cd /d "%~dp0"
set HOLIDAY_SKIP_EXIT_CODE=20
set DUPLICATE_SKIP_EXIT_CODE=21
echo KRX ETF monitor is running.
python .\krx_etf_monitor.py run --send-telegram --sleep 0.2
if errorlevel %DUPLICATE_SKIP_EXIT_CODE% (
  echo Another ETF collection run is already in progress. This run was skipped.
  echo.
  pause
  exit /b 0
)
if errorlevel %HOLIDAY_SKIP_EXIT_CODE% (
  echo KRX holiday/weekend detected. Collection skipped.
  echo.
  pause
  exit /b 0
)
echo.
echo Finished. You can close this window.
pause
