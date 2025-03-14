@echo off
echo Launching a text entity extraction system based on a large language model...

:: Check if conda is installed
where conda >nul 2>nul
if %errorlevel% neq 0 (
    echo Error: conda not found. Please install Anaconda or Miniconda first.
    exit /b 1
)

:: Activate the environment and run
call conda activate kg
python kg_app.py

pause