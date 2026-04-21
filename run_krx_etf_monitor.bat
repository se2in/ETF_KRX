@echo off
cd /d "%~dp0"
echo KRX ETF monitor is running.
python .\krx_etf_monitor.py run --send-telegram --sleep 0.2
echo.
echo Finished. You can close this window.
pause
