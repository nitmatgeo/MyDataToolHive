@echo off
cd /d "%~dp0"
echo Cleaning previous build...
rmdir /s /q dist 2>nul
rmdir /s /q build 2>nul
rmdir /s /q etl_monitor.egg-info 2>nul
rmdir /s /q databricks_etl_monitor.egg-info 2>nul

echo Building package...
python -m build

echo Uploading to PyPI...
python -m twine upload dist/*

echo Done.
