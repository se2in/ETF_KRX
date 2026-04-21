@echo off
cd /d "%~dp0"
echo KRX ETF collect + Telegram + HTML + GitHub publish is running.

echo [1/4] Collecting KRX data, updating DB, sending Telegram, and creating HTML...
python .\krx_etf_monitor.py run --send-telegram --sleep 0.2
if errorlevel 1 (
  echo ERROR: KRX collection failed. GitHub publish skipped.
  pause
  exit /b 1
)

echo [2/4] Copying latest HTML report to docs\index.html...
if not exist ".\reports\latest_changes.html" (
  echo ERROR: reports\latest_changes.html was not created.
  pause
  exit /b 1
)
copy /Y ".\reports\latest_changes.html" ".\docs\index.html" >nul

echo [3/4] Committing docs HTML report...
git rev-parse --is-inside-work-tree >nul 2>nul
if errorlevel 1 (
  echo ERROR: This folder is not a Git repository yet.
  echo Run setup_github_publish.bat first after creating the GitHub repository.
  pause
  exit /b 1
)

git add .\docs\index.html
for /f "tokens=1-4 delims=/.: " %%a in ("%date% %time%") do set NOW=%%a-%%b-%%c_%%d
git commit -m "Update ETF HTML report" || echo No HTML changes to commit.

echo [4/4] Pushing to GitHub...
git push
if errorlevel 1 (
  echo ERROR: GitHub push failed. Check repository URL or login.
  pause
  exit /b 1
)

echo.
echo Finished. GitHub Pages will update shortly.
pause

