@echo off
REM Activate the virtual environment
call "%~dp0punters\Scripts\activate.bat"

REM Run the Python script with arguments
python "%~dp0punters.com.au_process.py" -from 20240721

REM Deactivate the virtual environment
deactivate
