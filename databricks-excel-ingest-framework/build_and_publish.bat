@echo off
cd /d "%~dp0"

echo Cleaning previous builds...
if exist dist rmdir /s /q dist
if exist *.egg-info rmdir /s /q *.egg-info
if exist excel_ingest.egg-info rmdir /s /q excel_ingest.egg-info

echo Building wheel...
python -m build
if errorlevel 1 (
    echo Build failed.
    exit /b 1
)

echo Publishing to PyPI...
python -m twine upload dist/*
if errorlevel 1 (
    echo Publish failed.
    exit /b 1
)

echo Done.
