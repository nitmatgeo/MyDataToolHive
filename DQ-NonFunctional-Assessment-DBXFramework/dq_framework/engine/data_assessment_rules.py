"""
Assessment Runner
==================
Python / PySpark equivalent of ``[dq].[p_DQ_DataAssessmentRules]``.

Reproduces every aspect of the SQL stored procedure faithfully:

    * ``1=1`` base WHERE clause with optional SchemaName / TableName /
      FieldName scope filters — appended as ``AND`` conditions
    * PreQuery   — reset DQEligible/DQViolations/DQFields on previously
                   failed rows where DQEligible=0
    * AuditQuery — apply field-level TVF (Python checker) to every row via
                   CROSS APPLY equivalent; collect violations
    * UpdateQuery — write DQEligible / DQViolations / DQFields back to the
                   curated Delta table rows
    * StatQuery  — aggregate pass/fail counts into statDQChecks
    * Dual INSERT — only Result=False rows go into auditDQChecks; all stats
                   go into statDQChecks

DQ column semantics on curated tables (preserved exactly)
-----------------------------------------------------------
DQEligible
    None  (NULL)  — row not yet assessed in this execution
    True  (1)     — all configured checks passed
    False (0)     — at least one check failed
    Sticky logic: ``CASE WHEN A.DQEligible = 0 THEN A.DQEligible
                         ELSE B.Result END``
    → If a row already has DQEligible=False from a prior field check,
      it stays False even if subsequent fields pass.

DQViolations
    Accumulates ``[FieldName: ViolationType]`` entries across all field
    assessments for a row:
        None → first violation  → '[field: ViolationType]'
        existing string + ', '  → '[field: ViolationType]'
        no violation (pass)     → unchanged

DQFields
    Accumulates the names of ALL fields assessed on this row (pass or fail):
        None → first field  → '[field]'
        existing string + ', ' → '[field]'

ExecutionID
    If None / NULL passed, a new UUID is generated for the batch (mirrors
    ``IF @ExecutionID IS NULL SET @ExecutionID = CONVERT(VARCHAR(40), NEWID())``).
"""

from __future__ import annotations

import logging
import uuid
from datetime import datetime
from typing import Optional

logger = logging.getLogger(__name__)

try:
    from pyspark.sql import SparkSession, DataFrame
    from pyspark.sql import functions as F
    from pyspark.sql.types import (
        BooleanType, IntegerType, StringType, StructField, StructType, TimestampType
    )
    from delta.tables import DeltaTable
    _HAS_SPARK = True
except ImportError:
    _HAS_SPARK = False
    logger.warning(
        "PySpark / delta-spark not available. "
        "DQRunner will operate in offline / testing mode only."
    )


class DQRunner:
    """
    Executes the DQ assessment for one or more field mappings.

    Parameters
    ----------
    spark
        Active SparkSession.
    catalog
        Unity Catalog name (or empty string for legacy Hive metastore).
    schema
        The schema / database that contains the framework tables (default: 'dq').
    function_registry
        Populated ``FunctionRegistry`` from ``generate_rule_functions()``.
    """

    def __init__(self, spark, catalog: str, schema: str, function_registry):
        self.spark = spark
        self.catalog = catalog
        self.dq_schema = schema
        self.registry = function_registry

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _fqn(self, table_name: str) -> str:
        """Build a fully-qualified table name."""
        if self.catalog:
            return f"`{self.catalog}`.`{self.dq_schema}`.`{table_name}`"
        return f"`{self.dq_schema}`.`{table_name}`"

    def _curated_fqn(self, target_catalog: Optional[str], target_schema: str,
                     target_table: str) -> str:
        if target_catalog:
            return f"`{target_catalog}`.`{target_schema}`.`{target_table}`"
        if self.catalog:
            return f"`{self.catalog}`.`{target_schema}`.`{target_table}`"
        return f"`{target_schema}`.`{target_table}`"

    def _read_table(self, fqn: str) -> "DataFrame":
        return self.spark.table(fqn)

    def _load_mappings(
        self,
        schema_name: Optional[str],
        table_name: Optional[str],
        field_name: Optional[str],
    ) -> list[dict]:
        """
        Load active mapDQChecks rows, filtered by scope parameters.

        Mirrors the WHERE clause in #DynamicQuerySQL SELECT:
            WHERE 1 = 1
              AND IsActive = 1
              AND ((1 = 1 AND @SchemaName IS NULL) OR (@SchemaName IS NOT NULL AND TargetSchemaName = @SchemaName))
              AND ((1 = 1 AND @TableName  IS NULL) OR (@TableName  IS NOT NULL AND TargetTableName  = @TableName))
              AND ((1 = 1 AND @FieldName  IS NULL) OR (@FieldName  IS NOT NULL AND TargetFieldName  = @FieldName))

        The ``1=1`` base ensures the query is always syntactically valid and
        allows dynamic appending of AND conditions — a classic dynamic SQL
        pattern preserved here as Python conditional logic.
        """
        df = self._read_table(self._fqn("mapDQChecks"))

        # Base filter: WHERE 1=1 AND IsActive=1
        df = df.filter(F.col("IsActive") == True)  # noqa: E712

        # Dynamic AND conditions — each is optional, appended only if parameter IS NOT NULL
        if schema_name is not None:
            df = df.filter(F.col("TargetSchemaName") == schema_name)
        if table_name is not None:
            df = df.filter(F.col("TargetTableName") == table_name)
        if field_name is not None:
            df = df.filter(F.col("TargetFieldName") == field_name)

        return [row.asDict() for row in df.collect()]

    # ------------------------------------------------------------------
    # Core execution
    # ------------------------------------------------------------------

    def run(
        self,
        schema_name: Optional[str] = None,
        table_name: Optional[str] = None,
        field_name: Optional[str] = None,
        reset_eligible_flag: bool = False,
        enable_output: bool = True,
        execution_id: Optional[str] = None,
    ) -> Optional[str]:
        """
        Execute the DQ assessment.

        Equivalent to ``EXEC [dq].[p_DQ_DataAssessmentRules]``.

        Parameters
        ----------
        schema_name
            Curated schema to assess (NULL = all schemas).
        table_name
            Specific table to assess (NULL = all tables).
        field_name
            Specific field to assess (NULL = all fields).
        reset_eligible_flag
            True  = reset DQEligible/DQViolations/DQFields where DQEligible=0
                    (PreQuery only, no assessment).
            False = run full assessment.
        enable_output
            True = return summary DataFrames; False = silent.
        execution_id
            Supply a fixed UUID for grouped tracking; None generates a new one.

        Returns
        -------
        The ExecutionID used for this batch.
        """
        # IF @ExecutionID IS NULL SET @ExecutionID = NEWID()
        if execution_id is None:
            execution_id = str(uuid.uuid4())

        logger.info("DQ Assessment started. ExecutionID: %s", execution_id)

        # Load field mappings with scope filters (1=1 dynamic WHERE)
        mappings = self._load_mappings(schema_name, table_name, field_name)

        if not mappings:
            scope_parts = []
            if schema_name:
                scope_parts.append(f"schema='{schema_name}'")
            if table_name:
                scope_parts.append(f"table='{table_name}'")
            if field_name:
                scope_parts.append(f"field='{field_name}'")
            scope_info = ", ".join(scope_parts) if scope_parts else "no filter (all)"
            total_rows = self.spark.table(self._fqn("mapDQChecks")).count()
            print(
                f"No active mappings found for the given scope ({scope_info}). "
                f"mapDQChecks has {total_rows} row(s) total. "
                f"Check IsActive column or populate via dq.config.add_mapping()."
            )
            logger.warning(
                "No active mappings found. Scope: %s. mapDQChecks total rows: %d",
                scope_info, total_rows,
            )
            print(f"✓ Assessment complete — ExecutionID: {execution_id}")
            return execution_id

        # Iterate mappings — equivalent to WHILE @CurrentRow <= @RowCount
        for mapping in mappings:
            mapping_id = mapping["_ID"]
            target_catalog = mapping.get("TargetCatalogName")
            target_schema = mapping["TargetSchemaName"]
            target_table = mapping["TargetTableName"]
            target_field = mapping["TargetFieldName"]
            # DQFunctionSchemaName mirrors [dq] schema in SQL Server — informational in Python
            dq_fn_schema = mapping.get("DQFunctionSchemaName", self.dq_schema)
            dq_fn_name = mapping["DQFunctionName"]

            curated_fqn = self._curated_fqn(target_catalog, target_schema, target_table)

            logger.info(
                "Processing MappingID=%d  %s.%s.%s  →  [%s].[%s]",
                mapping_id, target_schema, target_table, target_field,
                dq_fn_schema, dq_fn_name,
            )

            if reset_eligible_flag:
                # PreQuery: UPDATE curated table SET DQEligible=NULL, DQViolations=NULL,
                #           DQFields=NULL WHERE DQEligible=0
                self._execute_pre_query(curated_fqn)
                continue

            # Get the field checker function from registry
            checker_fn = self.registry.get(dq_fn_name)
            if checker_fn is None:
                logger.error(
                    "Field checker '%s' not found in registry. "
                    "Run generate_rule_functions() first.",
                    dq_fn_name,
                )
                continue

            # ----------------------------------------------------------
            # Combined AuditQuery + UpdateQuery — single table scan.
            # Reads table once, applies checker UDF via Spark (distributed),
            # applies any SQL-type L02 expressions at DataFrame level, then
            # writes DQ columns back via merge.
            # Equivalent to: AuditQueryText + QueryText (UPDATE) in the SQL SP.
            # ----------------------------------------------------------
            sql_expressions = self.registry.get_sql_expressions(dq_fn_name)
            audit_rows, rows_qualified, rows_disqualified = self._execute_field_assessment(
                curated_fqn, target_field, checker_fn, mapping_id, execution_id,
                sql_expressions=sql_expressions,
            )

            # ----------------------------------------------------------
            # StatQuery + Persist
            # ----------------------------------------------------------
            self._persist_audit(audit_rows)
            self._persist_stat(
                mapping_id, execution_id, rows_qualified, rows_disqualified
            )

        if enable_output:
            self._print_output(execution_id)

        logger.info("DQ Assessment complete. ExecutionID: %s", execution_id)
        print(f"✓ Assessment complete — ExecutionID: {execution_id}")
        return execution_id

    # ------------------------------------------------------------------
    # SQL-type L02 — DataFrame-level evaluation
    # ------------------------------------------------------------------

    def _apply_sql_l02_checks(
        self,
        df: "DataFrame",
        target_field: str,
        sql_expressions: list[dict],
    ) -> "DataFrame":
        """
        Apply SQL-type L02 custom query expressions at the DataFrame level.

        SQL expressions store a Spark SQL fragment in ``CustomQuery`` with
        ``@InputValue`` as a placeholder for the field being validated.
        This mirrors the original SQL Server TVF generation where
        ``p_DQ_GenerateRuleFunctions`` injects ``@InputValue`` into the TVF
        body as a bound parameter — translated here to ``F.expr()`` with the
        actual column name substituted.

        The method updates the existing ``_dq_check`` column produced by the
        Python UDF:
          - If the Python UDF already returned False (L01/L03/L04 failure),
            the result is unchanged (early-exit — Python failure takes priority).
          - If the Python UDF returned True (passed) but an SQL expression
            fails, ``_dq_check`` is overridden with the SQL failure details.
          - If both Python and all SQL expressions pass, ``_dq_check`` is
            unchanged.

        Ordering: SQL expressions are evaluated in ascending ``_ID`` order
        (matching the SQL Server proc's deterministic INSERT order), with
        early-exit semantics — the first failure wins.

        Parameters
        ----------
        df
            DataFrame that already contains the ``_dq_check`` struct column.
        target_field
            Name of the field column being assessed (used to replace
            ``@InputValue`` in expressions and to build log messages).
        sql_expressions
            List of configCustomQuery row dicts with CustomQueryType='SQL'.
        """
        field_ref = f"`{target_field}`"
        is_null_col = F.col(field_ref).isNull()

        # Work columns track the first SQL expression failure across all rows.
        # Initialise as "all passing" (sql_failed=False, vtype/msg=NULL).
        df = df.withColumn("_sql_failed", F.lit(False))
        df = df.withColumn("_sql_vtype",  F.lit(None).cast(StringType()))
        df = df.withColumn("_sql_msg",    F.lit(None).cast(StringType()))

        # Sort by _ID to match SQL Server proc's deterministic processing order
        for expr_dict in sorted(sql_expressions, key=lambda x: x.get("_ID", 0)):
            cq_id    = expr_dict.get("_ID", "?")
            raw_expr = (expr_dict.get("CustomQuery") or "").strip()
            is_allowed = bool(expr_dict.get("IsConditionAllowed", True))

            if not raw_expr:
                continue

            # Validate via spark.sql() with a NULL substitute — F.expr().schema is
            # deferred in Spark Connect and will not surface PARSE_SYNTAX_ERROR
            # until the action fires outside this try/except.
            validate_sql = raw_expr.replace("@InputValue", "CAST(NULL AS STRING)")
            try:
                self.spark.sql(
                    f"SELECT CAST(({validate_sql}) AS BOOLEAN) AS _dq_validate"
                ).limit(0).count()
            except Exception as exc:
                print(
                    f"  ⚠  CustomQuery ID {cq_id} has an invalid Spark SQL expression"
                    f" — skipped.\n"
                    f"     Expression : {raw_expr}\n"
                    f"     Error      : {exc}\n"
                    f"     Common fix : CHARINDEX(s,str)→LOCATE(s,str)  LEN(str)→LENGTH(str)"
                    f"  String literals must be single-quoted: LIKE '%x%' not LIKE %x%\n"
                    f"     Update configCustomQuery and re-run generate_rule_functions()."
                )
                logger.warning(
                    "SQL expression for CustomQuery ID %s could not be parsed "
                    "— skipped. Expression: %r. Error: %s",
                    cq_id, raw_expr, exc,
                )
                continue

            # Replace @InputValue placeholder with the actual column reference.
            spark_sql = raw_expr.replace("@InputValue", field_ref)
            condition_col = F.expr(spark_sql).cast(BooleanType())

            # Determine the "this row fails this expression" predicate.
            # NULL values always pass (same semantics as Python closure).
            # is_condition_allowed=True:  condition must match → fails if NOT condition
            # is_condition_allowed=False: condition must NOT match → fails if condition
            if is_allowed:
                this_fails = (~is_null_col) & (~condition_col)
                fail_suffix = (
                    f"is/are NOT matched but is desired to be mandatorily :<Allowed>"
                )
            else:
                this_fails = (~is_null_col) & condition_col
                fail_suffix = (
                    f"is/are matched but this is :<NOT Allowed>"
                )

            fail_msg = F.concat(
                F.lit("Debug:: The value <"),
                F.col(field_ref).cast(StringType()),
                F.lit(
                    f"> has [FAILED]. Rule: Custom Query Condition(s) of ID "
                    f"[{cq_id}] {fail_suffix}"
                ),
            )

            # Accumulate: once a row has failed (_sql_failed=True), keep it
            # failed (first failure wins — early-exit semantics).
            df = df.withColumn(
                "_sql_failed",
                F.when(F.col("_sql_failed"), F.lit(True))
                 .when(this_fails, F.lit(True))
                 .otherwise(F.lit(False)),
            )
            # Capture violation type for the FIRST failure only
            df = df.withColumn(
                "_sql_vtype",
                F.when(F.col("_sql_failed") & F.col("_sql_vtype").isNull(),
                       F.lit("Custom Query"))
                 .when(F.col("_sql_failed"), F.col("_sql_vtype"))
                 .otherwise(F.lit(None).cast(StringType())),
            )
            # Capture log message for the FIRST failure only
            df = df.withColumn(
                "_sql_msg",
                F.when(F.col("_sql_failed") & F.col("_sql_msg").isNull(),
                       fail_msg)
                 .when(F.col("_sql_failed"), F.col("_sql_msg"))
                 .otherwise(F.lit(None).cast(StringType())),
            )

        # Combine with the Python UDF result:
        #   Python FAIL → keep Python result (L01 / L03 / L04 took priority)
        #   Python PASS + SQL FAIL → override with SQL Custom Query failure
        #   Both PASS → keep Python result (True)
        df = df.withColumn(
            "_dq_check",
            F.when(
                # Python UDF already failed — do not override
                F.col("_dq_check.result") == False,    # noqa: E712
                F.col("_dq_check"),
            ).when(
                # Python passed but at least one SQL expression failed
                F.col("_sql_failed"),
                F.struct(
                    F.lit(False).alias("result"),
                    F.col("_sql_vtype").alias("violation_type"),
                    F.col("_sql_msg").alias("log_message"),
                ),
            ).otherwise(
                # Both Python and SQL passed
                F.col("_dq_check"),
            ),
        )

        # Drop temporary work columns
        return df.drop("_sql_failed", "_sql_vtype", "_sql_msg")

    # ------------------------------------------------------------------
    # PreQuery
    # ------------------------------------------------------------------

    def _execute_pre_query(self, curated_fqn: str) -> None:
        """
        Reset DQ columns on rows that previously failed.

        SQL equivalent:
            UPDATE A
            SET DQEligible = NULL, DQViolations = NULL, DQFields = NULL
            FROM [Curated].[Table] A
            WHERE 1 = 1 AND A.DQEligible = 0
        """
        dt = DeltaTable.forName(self.spark, curated_fqn)
        dt.update(
            condition=F.col("DQEligible") == False,  # noqa: E712
            set={
                "DQEligible":   F.lit(None).cast(BooleanType()),
                "DQViolations": F.lit(None).cast(StringType()),
                "DQFields":     F.lit(None).cast(StringType()),
            }
        )
        logger.info("PreQuery complete: reset DQ columns on %s", curated_fqn)

    # ------------------------------------------------------------------
    # Combined AuditQuery + UpdateQuery
    # ------------------------------------------------------------------

    def _execute_field_assessment(
        self,
        curated_fqn: str,
        target_field: str,
        checker_fn,
        mapping_id: int,
        execution_id: str,
        sql_expressions: list[dict] | None = None,
    ) -> tuple[list[dict], int, int]:
        """
        Apply the field checker and write DQ columns back.

        Mirrors the SQL Server stored proc two-pass pattern:
          Pass 1 (AuditQueryText): read table, apply checker UDF, collect
            Result=False rows into auditDQChecks.
          Pass 2 (QueryText): MERGE on DQRowID (UUID set by prepare_curated_table())
            — equivalent to ``UPDATE A SET ... FROM Table A CROSS APPLY fn_DQ(field) B``.
            DQRowID guarantees 1:1 row matching; duplicate content rows are handled
            correctly since each has a distinct UUID.

        SQL equivalents (merged into one method)
        -----------------------------------------
        AuditQueryText:
            INSERT INTO #auditDQChecks (...)
            SELECT GETDATE(), @ExecutionID, MappingID, A.[field], B.Result,
                   B.ViolationType, B.LogMessage
            FROM [Curated].[Table] A CROSS APPLY [dq].[fn_DQ_...](field) B

        QueryText (UPDATE):
            UPDATE A SET
              DQEligible   = CASE WHEN A.DQEligible=0 THEN 0 ELSE B.Result END,
              DQViolations = CASE WHEN B.Result=0 AND A.DQViolations IS NULL
                                       THEN '[field: ViolationType]'
                                  WHEN B.Result=0
                                       THEN A.DQViolations + ', [field: ViolationType]'
                                  ELSE A.DQViolations END,
              DQFields     = CASE WHEN A.DQFields IS NULL THEN '[field]'
                                  ELSE A.DQFields + ', [field]' END
            FROM [Curated].[Table] A CROSS APPLY [dq].[fn_DQ_...](field) B

        Key behaviours preserved
        ------------------------
        - DQEligible STICKY: once False, stays False regardless of later results.
        - DQViolations accumulates: all failing fields joined with ', '.
        - DQFields accumulates ALL assessed fields (pass AND fail).
        - Only Result=False rows are written to auditDQChecks.
        """
        generated_on = datetime.utcnow()

        # ── Pre-flight: verify DQ columns exist on the curated table ─────────
        # These columns are a prerequisite for assessment. If missing, raise a
        # clear error pointing to dq.prepare_curated_table() rather than letting
        # Spark crash deep in query planning with an unresolved column error.
        curated_df = self._read_table(curated_fqn)
        existing_cols = {f.name for f in curated_df.schema.fields}
        missing_dq_cols = [c for c in ("DQRowID", "DQEligible", "DQViolations", "DQFields")
                           if c not in existing_cols]
        if missing_dq_cols:
            raise RuntimeError(
                f"Curated table {curated_fqn} is missing required DQ column(s): "
                f"{missing_dq_cols}. "
                f"Run dq.prepare_curated_table('{target_field.split('.')[0] if '.' in target_field else '?'}', "
                f"'<table_name>') to add them. "
                f"Or use dq.prepare_curated_tables() to add them to all mapped tables at once. "
                f"Requires ALTER privilege on the target table."
            )

        # Auto-populate any NULL DQRowIDs (rows added after prepare_curated_table()).
        # NULL DQRowID rows would silently skip the MERGE (NULL = NULL never matches).
        # Delta UPDATE rejects uuid() (INVALID_NON_DETERMINISTIC_EXPRESSIONS);
        # replaceWhere atomically replaces only NULL-DQRowID rows.
        null_row_ids = curated_df.filter(F.col("DQRowID").isNull()).count()
        if null_row_ids > 0:
            # Delta UPDATE rejects uuid() (non-deterministic). replaceWhere also
            # rejects written rows that don't satisfy the predicate — after UUID
            # assignment DQRowID IS NOT NULL, so the check always fails.
            # Disable the safety check just for this write; Delta still physically
            # replaces only the DQRowID IS NULL rows, all others are untouched.
            self.spark.conf.set(
                "spark.databricks.delta.replaceWhere.constraintCheck.enabled", "false"
            )
            try:
                (
                    self.spark.table(curated_fqn)
                    .filter(F.col("DQRowID").isNull())
                    .withColumn("DQRowID", F.expr("uuid()"))
                    .write.format("delta")
                    .option("replaceWhere", "DQRowID IS NULL")
                    .mode("overwrite")
                    .saveAsTable(curated_fqn)
                )
            finally:
                self.spark.conf.set(
                    "spark.databricks.delta.replaceWhere.constraintCheck.enabled", "true"
                )
            curated_df = self._read_table(curated_fqn)  # re-read with populated IDs

        # ── UDF: run the checker function distributedly on the cluster ──────
        @F.udf(returnType=StructType([
            StructField("result",         BooleanType()),
            StructField("violation_type", StringType()),
            StructField("log_message",    StringType()),
        ]))
        def apply_checker(value):
            v = str(value) if value is not None else None
            result, vtype, msg = checker_fn(v)
            return (result, vtype, msg)

        checked_df = curated_df.withColumn("_dq_check", apply_checker(F.col(target_field)))

        # SQL-type L02 expressions cannot run inside a UDF (no SparkSession on
        # executors).  Apply them here at the DataFrame level — equivalent to the
        # SQL proc injecting @InputValue into the TVF body dynamically — then
        # override _dq_check where Python passed but an SQL expression fails.
        if sql_expressions:
            checked_df = self._apply_sql_l02_checks(checked_df, target_field, sql_expressions)

        checked_df.cache()

        # ── Collect audit data (driver-side) from the cached DF ─────────────
        audit_rows = []
        rows_qualified = 0
        rows_disqualified = 0

        for row in checked_df.select(F.col(target_field), F.col("_dq_check")).collect():
            result = row["_dq_check"]["result"]
            raw_val = row[target_field]
            value_str = str(raw_val) if raw_val is not None else None
            if result:
                rows_qualified += 1
            else:
                rows_disqualified += 1
                # Only failures go to auditDQChecks (WHERE Result = 0)
                audit_rows.append({
                    "GeneratedOn":   generated_on,
                    "ExecutionID":   execution_id,
                    "MappingID":     mapping_id,
                    "InputValue":    value_str,
                    "Result":        result,
                    "ViolationType": row["_dq_check"]["violation_type"] or "",
                    "LogMessage":    row["_dq_check"]["log_message"],
                })

        # ── Write back via MERGE on DQRowID ─────────────────────────────────
        # Equivalent to SQL Server's QueryText:
        #   UPDATE A SET ... FROM [Curated].[Table] A CROSS APPLY [dq].[fn_DQ_...](field) B
        #
        # DQRowID is a UUID assigned per row by prepare_curated_table().
        # Guarantees 1:1 MERGE matching — no duplicate-row ambiguity,
        # no race conditions, works for parallel assessments.
        # checked_df already carries DQRowID from the same table scan used for audit.
        dt = DeltaTable.forName(self.spark, curated_fqn)
        (
            dt.alias("t")
              .merge(checked_df.alias("s"), "t.DQRowID = s.DQRowID")
              .whenMatchedUpdate(set={
                  # Sticky False: once disqualified, stays disqualified
                  "DQEligible": (
                      F.when(F.col("t.DQEligible") == False, F.lit(False))  # noqa: E712
                       .otherwise(F.col("s._dq_check.result"))
                  ),
                  # Accumulate all failing field/violation-type pairs
                  "DQViolations": (
                      F.when(
                          F.col("s._dq_check.result") == False,              # noqa: E712
                          F.when(F.col("t.DQViolations").isNull(),
                                 F.concat(F.lit(f"[{target_field}: "),
                                          F.col("s._dq_check.violation_type"),
                                          F.lit("]")))
                           .otherwise(F.concat(F.col("t.DQViolations"),
                                               F.lit(f", [{target_field}: "),
                                               F.col("s._dq_check.violation_type"),
                                               F.lit("]")))
                      ).otherwise(F.col("t.DQViolations"))
                  ),
                  # Accumulate every assessed field (pass and fail)
                  "DQFields": (
                      F.when(F.col("t.DQFields").isNull(), F.lit(f"[{target_field}]"))
                       .otherwise(F.concat(F.col("t.DQFields"),
                                           F.lit(f", [{target_field}]")))
                  ),
              })
              .execute()
        )

        checked_df.unpersist()
        return audit_rows, rows_qualified, rows_disqualified

    # ------------------------------------------------------------------
    # Persist helpers
    # ------------------------------------------------------------------

    def _persist_audit(self, audit_rows: list[dict]) -> None:
        """Write violation rows to auditDQChecks (only Result=False rows)."""
        if not audit_rows:
            return
        import pandas as pd
        # _ID is GENERATED ALWAYS AS IDENTITY — do not include it in the insert.
        # Delta assigns a unique sequential value automatically on each append.
        audit_schema = StructType([
            StructField("GeneratedOn",   TimestampType(), False),
            StructField("ExecutionID",   StringType(),    True),
            StructField("MappingID",     IntegerType(),   False),
            StructField("InputValue",    StringType(),    True),
            StructField("Result",        BooleanType(),   False),
            StructField("ViolationType", StringType(),    False),
            StructField("LogMessage",    StringType(),    True),
        ])
        # Use pandas — Spark Connect raises CANNOT_DETERMINE_TYPE for Python lists
        # when any column is all-None (e.g. InputValue when all failing rows are NULL).
        audit_pdf = pd.DataFrame([
            {
                "GeneratedOn":   pd.Timestamp(r["GeneratedOn"]),
                "ExecutionID":   r["ExecutionID"],
                "MappingID":     r["MappingID"],
                "InputValue":    r["InputValue"],
                "Result":        r["Result"],
                "ViolationType": r["ViolationType"],
                "LogMessage":    r["LogMessage"],
            }
            for r in audit_rows
        ])
        self.spark.createDataFrame(audit_pdf, schema=audit_schema).write \
            .format("delta").mode("append").saveAsTable(self._fqn("auditDQChecks"))

    def _persist_stat(
        self,
        mapping_id: int,
        execution_id: str,
        rows_qualified: int,
        rows_disqualified: int,
    ) -> None:
        """
        Write aggregated pass/fail statistics to statDQChecks.

        SQL equivalent (StatQueryText):
            INSERT INTO #statDQChecks (LoggedOn, ExecutionID, MappingID,
                                       RowsQualified, RowsDisqualified)
            SELECT CAST(GeneratedOn AS DATE), ExecutionID, MappingID,
                   (SELECT COUNT(*) FROM #auditDQChecks C
                    WHERE C.Result = 1 AND A.MappingID = C.MappingID) AS RowsQualified,
                   COUNT(CASE WHEN A.Result = 0 THEN 1 END) AS RowsDisqualified
            FROM #auditDQChecks A
            JOIN [dq].[mapDQChecks] B ON 1=1
                AND A.Result = 0
                AND A.MappingID = B._ID
                AND B.TargetSchemaName = '...'
                AND B.TargetTableName  = '...'
            GROUP BY CAST(GeneratedOn AS DATE), ExecutionID, MappingID

        Note: the StatQuery filters audit to Result=0 rows for the GROUP BY
        (to avoid counting eligible rows in the grouping), but RowsQualified
        is fetched via a correlated subcount of Result=1 rows.
        """
        import pandas as pd
        # _ID is GENERATED ALWAYS AS IDENTITY — do not include it in the insert.
        # Delta assigns a unique sequential value automatically on each append.
        stat_schema = StructType([
            StructField("LoggedOn",          TimestampType(), False),
            StructField("ExecutionID",       StringType(),    True),
            StructField("MappingID",         IntegerType(),   False),
            StructField("RowsQualified",     IntegerType(),   False),
            StructField("RowsDisqualified",  IntegerType(),   False),
        ])
        # Use pandas — avoids CANNOT_DETERMINE_TYPE in Spark Connect
        stat_pdf = pd.DataFrame([{
            "LoggedOn":        pd.Timestamp(datetime.utcnow()),
            "ExecutionID":     execution_id,
            "MappingID":       mapping_id,
            "RowsQualified":   rows_qualified,
            "RowsDisqualified": rows_disqualified,
        }])
        self.spark.createDataFrame(stat_pdf, schema=stat_schema).write \
            .format("delta").mode("append").saveAsTable(self._fqn("statDQChecks"))

    # ------------------------------------------------------------------
    # Output (enable_output=True)
    # ------------------------------------------------------------------

    def _print_output(self, execution_id: str) -> None:
        """
        Display assessment results — equivalent to the IF @EnableOutput=1 block.

        Shows:
            1. v_auditDQChecks filtered to this ExecutionID (violations)
            2. v_statDQChecks filtered to this ExecutionID (quality scores)
            3. Summary: violations grouped by field + violation type
        """
        exec_filter = (
            (F.col("ExecutionID") == execution_id)
            if execution_id
            else F.col("ExecutionID").isNull()
        )

        audit_view = self.spark.table(self._fqn("v_auditDQChecks"))
        stat_view  = self.spark.table(self._fqn("v_statDQChecks"))

        print(f"\n=== DQ Audit (violations) — ExecutionID: {execution_id} ===")
        audit_view.filter(exec_filter).orderBy("FullFieldName", "LogID").show(100, truncate=False)

        print(f"\n=== DQ Statistics (quality scores) — ExecutionID: {execution_id} ===")
        stat_view.filter(exec_filter).orderBy("FullFieldName", "StatisticID").show(100, truncate=False)

        print(f"\n=== DQ Summary (violations by field + type) ===")
        (audit_view
            .filter(exec_filter)
            .groupBy("FullFieldName", "TargetTableName", "TargetFieldName", "ViolationType")
            .count()
            .withColumnRenamed("count", "RecordCount")
            .orderBy(F.desc("RecordCount"))
            .show(100, truncate=False))
