@echo off
cd /d "%~dp0"
if exist ".\reports\latest_changes.html" (
  start "" ".\reports\latest_changes.html"
) else (
  echo HTML report does not exist yet. Run run_krx_etf_monitor.bat first.
  pause
)
