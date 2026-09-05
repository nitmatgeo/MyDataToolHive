@echo off
cd /d "%~dp0"

echo Cleaning previous build...
rmdir /s /q dist 2>nul
rmdir /s /q build 2>nul
rmdir /s /q etl_monitor.egg-info 2>nul
rmdir /s /q databricks_etl_monitor.egg-info 2>nul

if exist dist (
    echo ERROR: Could not delete dist\ — close VS Code Explorer / File Explorer on that folder and retry.
    exit /b 1
)

echo Installing build dependencies (required for --no-isolation on Python 3.12.0)...
python -m pip install setuptools wheel build --quiet
if %errorlevel% neq 0 (
    echo ERROR: Could not install build dependencies.
    exit /b 1
)

echo Building package...
python -m build --no-isolation
if %errorlevel% neq 0 (
    echo ERROR: Build failed — fix the errors above before uploading.
    exit /b 1
)

echo Uploading to PyPI...
python -m twine upload dist/*
if %errorlevel% neq 0 (
    echo ERROR: Upload failed.
    exit /b 1
)

echo Done.
