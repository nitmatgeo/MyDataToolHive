"""
Data Quality Non-Functional Assessment Framework for Databricks / Delta Lake
============================================================================
Metadata-driven, dynamically-generated DQ assessment engine.

Equivalent to the SQL Server implementation but running natively on
Databricks with Delta tables, PySpark DataFrames, and Python UDFs.

Usage
-----
from dq_framework import DQFramework

dq = DQFramework(spark, catalog="main", schema="dq")
dq.setup()                              # create Delta tables + seed reference data
dq.generate_rule_functions()            # build field-level checker functions from config
dq.run_assessment(schema_name="Curated")  # run DQ assessment
"""

from dq_framework.framework import DQFramework
from dq_framework.config import ConfigManager

__all__ = ["DQFramework", "ConfigManager"]
try:
    from importlib.metadata import version as _pkg_version
    __version__ = _pkg_version("databricks-dq-framework")
except Exception:
    __version__ = "unknown"
