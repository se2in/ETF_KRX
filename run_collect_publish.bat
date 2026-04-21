@echo off
cd /d "%~dp0"
echo KRX ETF collect + Telegram + HTML + PPTX + GitHub publish is running.

echo [1/5] Collecting KRX data, updating DB, sending Telegram, and creating HTML/PPTX...
python .\krx_etf_monitor.py run --send-telegram --sleep 0.2
if errorlevel 1 (
  echo ERROR: KRX collection failed. GitHub publish skipped.
  pause
  exit /b 1
)

echo [2/5] Copying latest HTML/PPTX reports to docs...
if not exist ".\reports\latest_changes.html" (
  echo ERROR: reports\latest_changes.html was not created.
  pause
  exit /b 1
)
if not exist ".\reports\latest_changes.pptx" (
  echo ERROR: reports\latest_changes.pptx was not created.
  pause
  exit /b 1
)
copy /Y ".\reports\latest_changes.html" ".\docs\index.html" >nul
copy /Y ".\reports\latest_changes.pptx" ".\docs\latest_changes.pptx" >nul

echo [3/5] Checking Git repository...
git rev-parse --is-inside-work-tree >nul 2>nul
if errorlevel 1 (
  echo ERROR: This folder is not a Git repository yet.
  echo Run setup_github_publish.bat first after creating the GitHub repository.
  pause
  exit /b 1
)

echo [4/5] Committing public reports...
git add .\docs\index.html .\docs\latest_changes.pptx
git commit -m "Update ETF public reports" || echo No report changes to commit.

echo [5/5] Pushing to GitHub...
git push
if errorlevel 1 (
  echo ERROR: GitHub push failed. Check repository URL or login.
  pause
  exit /b 1
)

echo.
echo Finished. GitHub Pages will update shortly.
pause
