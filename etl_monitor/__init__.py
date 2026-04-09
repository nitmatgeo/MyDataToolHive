"""
Databricks ETL Monitor Framework
=================================
Centralised ETL process monitoring and execution tracking for Databricks / Delta Lake.

Tracks ADF pipelines, Databricks notebooks, Databricks jobs, and Dataflows in a
single Unity Catalog schema without modifying how those jobs are triggered.

Quick start::

    from etl_monitor import ETLMonitorFramework

    monitor = ETLMonitorFramework(spark, catalog="main", schema="etl")
    monitor.setup()   # idempotent — safe on every cluster start

    exec_id = ETLMonitorFramework.generate_execution_id()
    monitor.generate_execution_steps(exec_id, "CORP", "HR_DAILY", "2026-04-09")

    with monitor.task(exec_id, "CORP", "HR_DAILY",
                      task_id=1, workflow_id=1, sequence_id=2,
                      processing_date="2026-04-09"):
        pass   # your notebook logic here
"""

from etl_monitor.framework import ETLMonitorFramework

__all__ = ["ETLMonitorFramework"]

try:
    from importlib.metadata import version as _pkg_version
    __version__ = _pkg_version("databricks-etl-monitor")
except Exception:
    __version__ = "unknown"
