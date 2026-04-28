@echo off
cd /d "%~dp0"
echo GoatCounter visitor analytics setup
echo.
echo 1. Open https://www.goatcounter.com/help/start
echo 2. Create a site and copy your GoatCounter endpoint
echo    Example: https://yourcode.goatcounter.com/count
echo.
set /p GC_ENDPOINT=Paste GoatCounter endpoint here: 

if "%GC_ENDPOINT%"=="" (
  echo No endpoint entered. Nothing changed.
  pause
  exit /b 1
)

powershell -NoProfile -ExecutionPolicy Bypass -Command ^
  "$path = Join-Path '%~dp0' 'config.json';" ^
  "$json = Get-Content -LiteralPath $path -Raw -Encoding UTF8 | ConvertFrom-Json;" ^
  "$json.goatcounter_endpoint = '%GC_ENDPOINT%';" ^
  "$json | ConvertTo-Json -Depth 10 | Set-Content -LiteralPath $path -Encoding UTF8"

if errorlevel 1 (
  echo Failed to update config.json
  pause
  exit /b 1
)

echo.
echo GoatCounter endpoint saved.
echo Now run run_collect_publish.bat once to publish the tracking script.
pause
