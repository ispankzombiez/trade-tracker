@echo off
echo ========================================
echo   trade-tracker
echo   Quick Setup Script
echo ========================================
echo.

echo Checking Python installation...
python --version
if errorlevel 1 (
    echo ERROR: Python not found. Please install Python first.
    pause
    exit /b 1
)

echo.
echo Installing required packages...
pip install requests

echo.
echo Creating necessary directories...
if not exist "scripts" mkdir scripts
if not exist "web" mkdir web
if not exist "web\data" mkdir "web\data"
if not exist ".github" mkdir .github
if not exist ".github\workflows" mkdir ".github\workflows"
if not exist "raw pull" mkdir "raw pull"
if not exist "marketplace history" mkdir "marketplace history"
if not exist "Trade Overview" mkdir "Trade Overview"

echo.
echo Testing master.py script...
python master.py

echo.
echo ========================================
echo Setup completed successfully!
echo.
echo Next steps:
echo 1. Push this code to GitHub
echo 2. Set up GitHub secrets (SFL_API_KEY, SFL_BEARER_TOKEN)
echo 3. Configure self-hosted runner
echo 4. Enable GitHub Pages
echo.
echo See SETUP.md for detailed instructions.
echo ========================================
pause