@echo off
echo Install the dependencies required for the text entity acquisition system based on the large language model...

:: Check if conda is installed
where conda >nul 2>nul
if %errorlevel% neq 0 (
    echo Error: conda not found. Please install Anaconda or Miniconda first.
    exit /b 1
)

:: Create and activate environment
echo Creating and activating virtual environment...
call conda create -n kg python=3.9 -y
call conda activate kg

:: Install necessary dependencies
echo Installing necessary packages...
call conda install -c conda-forge biopython pandas networkx matplotlib pyvis requests tqdm rich configparser -y
call pip install urllib3

echo Dependencies installation complete!
echo Please use the following commands to activate the environment and run the system:
echo conda activate kg
echo python kg_app.py
pause