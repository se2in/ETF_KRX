@echo off
cd /d "%~dp0"
set HOLIDAY_SKIP_EXIT_CODE=20
echo KRX ETF monitor is running.
python .\krx_etf_monitor.py run --send-telegram --sleep 0.2
if errorlevel %HOLIDAY_SKIP_EXIT_CODE% (
  echo KRX holiday/weekend detected. Collection skipped.
  echo.
  pause
  exit /b 0
)
echo.
echo Finished. You can close this window.
pause
