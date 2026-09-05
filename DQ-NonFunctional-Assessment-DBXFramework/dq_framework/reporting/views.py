"""
Reporting Views
================
Python / Spark SQL equivalents of the two SQL Server reporting views:

  v_auditDQChecks
      Row-level violation log joined to field mapping metadata.
      Use for: identifying exactly which records failed which rules.

  v_statDQChecks
      Aggregated pass/fail statistics with calculated PercentageQualified.
      Use for: DQ dashboards, trend reporting, SLA tracking.

These are created as Spark SQL views (persistent in the metastore) so they
can be queried directly with SQL just like the original SQL Server views.

Equivalent to Script_04_Reporting_Views.sql.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)


def _fqn(catalog: str, schema: str, name: str) -> str:
    if catalog:
        return f"`{catalog}`.`{schema}`.`{name}`"
    return f"`{schema}`.`{name}`"


def create_reporting_views(spark: "SparkSession", catalog: str, dq_schema: str) -> None:
    """
    Create (or replace) the two reporting views in the metastore.

    Equivalent to running Script_04_Reporting_Views.sql.

    Parameters
    ----------
    spark
        Active SparkSession.
    catalog
        Unity Catalog name (or empty for legacy Hive metastore).
    dq_schema
        The schema containing the framework tables (default: 'dq').
    """
    _create_audit_view(spark, catalog, dq_schema)
    _create_stat_view(spark, catalog, dq_schema)
    logger.info("Reporting views created in %s.%s", catalog or "<default>", dq_schema)


def _create_audit_view(spark: "SparkSession", catalog: str, dq_schema: str) -> None:
    """
    v_auditDQChecks
    ---------------
    Row-level violations joined to field mapping context.

    Columns:
        LogID            -- Unique log record ID (_ID from auditDQChecks)
        FullFieldName    -- Source field definition (Schema.Table.Field)
        GeneratedOn      -- Timestamp when the check was run
        ExecutionID      -- GUID grouping all checks in one assessment batch
        TargetSchemaName -- Curated schema being assessed
        TargetTableName  -- Curated table being assessed
        TargetFieldName  -- Curated column being assessed
        InputValue       -- Actual data value that failed
        Result           -- False = FAIL (only failures stored here)
        ViolationType    -- Category of failure
        LogMessage       -- Diagnostic detail

    SQL equivalent:
        SELECT A._ID AS LogID, M.FullFieldName, A.GeneratedOn, A.ExecutionID,
               M.TargetSchemaName, M.TargetTableName, M.TargetFieldName,
               A.InputValue, A.Result, A.ViolationType, A.LogMessage
        FROM [dq].[auditDQChecks] A
        JOIN [dq].[mapDQChecks] M ON M._ID = A.MappingID
    """
    audit_fqn = _fqn(catalog, dq_schema, "auditDQChecks")
    map_fqn   = _fqn(catalog, dq_schema, "mapDQChecks")
    view_fqn  = _fqn(catalog, dq_schema, "v_auditDQChecks")

    spark.sql(f"DROP VIEW IF EXISTS {view_fqn}")
    spark.sql(f"""
        CREATE VIEW {view_fqn} AS
        SELECT
            A._ID              AS LogID,
            M.FullFieldName,
            A.GeneratedOn,
            A.ExecutionID,
            M.TargetSchemaName,
            M.TargetTableName,
            M.TargetFieldName,
            A.InputValue,
            A.Result,
            A.ViolationType,
            A.LogMessage
        FROM {audit_fqn} A
        JOIN {map_fqn} M
            ON M._ID = A.MappingID
    """)
    logger.info("Created view: %s", view_fqn)


def _create_stat_view(spark: "SparkSession", catalog: str, dq_schema: str) -> None:
    """
    v_statDQChecks
    ---------------
    Aggregated pass/fail counts with calculated quality score.

    Columns:
        StatisticID          -- Unique stat record ID (_ID from statDQChecks)
        FullFieldName        -- Source field definition
        LoggedOn             -- Date the assessment was run
        ExecutionID          -- GUID grouping all checks in one assessment batch
        TargetSchemaName     -- Curated schema assessed
        TargetTableName      -- Curated table assessed
        TargetFieldName      -- Curated column assessed
        RowsQualified        -- Count of records that passed all checks
        RowsDisqualified     -- Count of records that failed at least one check
        PercentageQualified  -- ROUND((Qualified / Total) * 100, 0)

    SQL equivalent:
        SELECT S._ID AS StatisticID, M.FullFieldName, S.LoggedOn, S.ExecutionID,
               M.TargetSchemaName, M.TargetTableName, M.TargetFieldName,
               S.RowsQualified, S.RowsDisqualified,
               ROUND((CAST(S.RowsQualified AS FLOAT)
                      / (S.RowsQualified + S.RowsDisqualified)) * 100, 0) AS PercentageQualified
        FROM [dq].[statDQChecks] S
        JOIN [dq].[mapDQChecks] M ON M._ID = S.MappingID
    """
    stat_fqn  = _fqn(catalog, dq_schema, "statDQChecks")
    map_fqn   = _fqn(catalog, dq_schema, "mapDQChecks")
    view_fqn  = _fqn(catalog, dq_schema, "v_statDQChecks")

    spark.sql(f"DROP VIEW IF EXISTS {view_fqn}")
    spark.sql(f"""
        CREATE VIEW {view_fqn} AS
        SELECT
            S._ID              AS StatisticID,
            M.FullFieldName,
            S.LoggedOn,
            S.ExecutionID,
            M.TargetSchemaName,
            M.TargetTableName,
            M.TargetFieldName,
            S.RowsQualified,
            S.RowsDisqualified,
            ROUND(
                (CAST(S.RowsQualified AS DOUBLE)
                 / (S.RowsQualified + S.RowsDisqualified)) * 100,
                0
            ) AS PercentageQualified
        FROM {stat_fqn} S
        JOIN {map_fqn} M
            ON M._ID = S.MappingID
    """)
    logger.info("Created view: %s", view_fqn)


# ---------------------------------------------------------------------------
# Convenience query helpers (mirrors the sample queries in Script_04)
# ---------------------------------------------------------------------------

def query_violations(spark: "SparkSession", catalog: str, dq_schema: str,
                     execution_id: str = None):
    """
    Return all violations for a given execution (or all if execution_id is None).

    Equivalent to:
        SELECT * FROM [dq].[v_auditDQChecks]
        WHERE ExecutionID = @ExecutionID
        ORDER BY FullFieldName, LogID
    """
    from pyspark.sql import functions as F
    df = spark.table(_fqn(catalog, dq_schema, "v_auditDQChecks"))
    if execution_id:
        df = df.filter(F.col("ExecutionID") == execution_id)
    return df.orderBy("FullFieldName", "LogID")


def query_quality_scores(spark: "SparkSession", catalog: str, dq_schema: str,
                          execution_id: str = None):
    """
    Return quality scores for a given execution.

    Equivalent to:
        SELECT * FROM [dq].[v_statDQChecks]
        WHERE ExecutionID = @ExecutionID
        ORDER BY PercentageQualified ASC
    """
    from pyspark.sql import functions as F
    df = spark.table(_fqn(catalog, dq_schema, "v_statDQChecks"))
    if execution_id:
        df = df.filter(F.col("ExecutionID") == execution_id)
    return df.orderBy("PercentageQualified")


def query_fields_below_threshold(spark: "SparkSession", catalog: str, dq_schema: str,
                                   threshold: float = 80.0):
    """
    Return fields with PercentageQualified below the given threshold.

    Equivalent to:
        SELECT * FROM [dq].[v_statDQChecks]
        WHERE PercentageQualified < 80
        ORDER BY PercentageQualified ASC
    """
    from pyspark.sql import functions as F
    return (spark.table(_fqn(catalog, dq_schema, "v_statDQChecks"))
            .filter(F.col("PercentageQualified") < threshold)
            .orderBy("PercentageQualified"))


def query_summary_by_violation_type(spark: "SparkSession", catalog: str, dq_schema: str,
                                    execution_id: str = None):
    """
    Return violation record counts grouped by field and violation type.

    Equivalent to:
        SELECT FullFieldName, GeneratedOn, ExecutionID,
               TargetSchemaName, TargetTableName, TargetFieldName,
               ViolationType, COUNT(*) AS RecordCount
        FROM [dq].[v_auditDQChecks]
        [WHERE ExecutionID = @ExecutionID]
        GROUP BY FullFieldName, GeneratedOn, ExecutionID,
                 TargetSchemaName, TargetTableName, TargetFieldName, ViolationType
        ORDER BY TargetTableName, TargetFieldName, ViolationType
    """
    from pyspark.sql import functions as F
    df = spark.table(_fqn(catalog, dq_schema, "v_auditDQChecks"))
    if execution_id:
        df = df.filter(F.col("ExecutionID") == execution_id)
    return (
        df
        .groupBy(
            "FullFieldName", "GeneratedOn", "ExecutionID",
            "TargetSchemaName", "TargetTableName", "TargetFieldName",
            "ViolationType",
        )
        .agg(F.count("*").alias("RecordCount"))
        .orderBy("TargetTableName", "TargetFieldName", "ViolationType")
    )


def query_summary_by_table(spark: "SparkSession", catalog: str, dq_schema: str,
                           execution_id: str = None):
    """
    Return aggregated quality score per curated table.

    Equivalent to:
        SELECT TargetTableName,
               SUM(RowsQualified) AS TotalQualified,
               SUM(RowsDisqualified) AS TotalDisqualified,
               ROUND(CAST(SUM(RowsQualified) AS FLOAT)
                     / NULLIF(SUM(RowsQualified+RowsDisqualified),0) * 100, 1) AS TableQualityPct
        FROM [dq].[v_statDQChecks]
        [WHERE ExecutionID = @ExecutionID]
        GROUP BY TargetTableName
        ORDER BY TableQualityPct ASC
    """
    from pyspark.sql import functions as F
    df = spark.table(_fqn(catalog, dq_schema, "v_statDQChecks"))
    if execution_id:
        df = df.filter(F.col("ExecutionID") == execution_id)
    return (
        df
        .groupBy("TargetTableName")
        .agg(
            F.sum("RowsQualified").alias("TotalQualified"),
            F.sum("RowsDisqualified").alias("TotalDisqualified"),
        )
        .withColumn(
            "TableQualityPct",
            F.round(
                F.col("TotalQualified").cast("double") /
                F.nullif(F.col("TotalQualified") + F.col("TotalDisqualified"), F.lit(0)) * 100,
                1,
            )
        )
        .orderBy("TableQualityPct")
    )
