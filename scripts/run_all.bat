@echo off
setlocal
REM === OPTIONAL: activate your venv (uncomment & fix path) ===
REM call "%~dp0venv\Scripts\activate.bat"

REM Run the ETL orchestrator
python "%~dp0scripts\run_etl.py"

REM Optionally skip backtest to speed up:
REM python "%~dp0scripts\run_etl.py" --skip-backtest

endlocal
