"""
DQFramework — Top-level facade
================================
Single entry point for the Data Quality Assessment Framework on Databricks.

Orchestrates all steps in the equivalent order to the SQL Server scripts:

    dq = DQFramework(spark, catalog="main", schema="dq")
    dq.setup()                          # Script_00 + Script_01
    dq.generate_rule_functions()        # p_DQ_GenerateRuleFunctions
    dq.run_assessment(schema_name=...) # p_DQ_DataAssessmentRules
"""

from __future__ import annotations

import logging
import uuid
from typing import Optional

from pyspark.sql import functions as F

from dq_framework.ddl_framework_tables import DDL_STATEMENTS, TABLE_ORDER, FRAMEWORK_SEEDED_TABLES, USER_CONFIG_TABLES
from dq_framework.seed_master_data import seed_master_data, BUILTIN_VALIDATORS, add_invalid_keyword, add_custom_pattern
from dq_framework.engine.generate_rule_functions import (
    FunctionRegistry,
    generate_rule_functions,
    register_validator,
    custom_validator_registry,
)
from dq_framework.engine.resolve_pattern_rules import resolve_patterns
from dq_framework.engine.data_assessment_rules import DQRunner
from dq_framework.reporting.views import create_reporting_views
from dq_framework.config import ConfigManager

logger = logging.getLogger(__name__)


class DQFramework:
    """
    Data Quality Assessment Framework for Databricks.

    Parameters
    ----------
    spark
        Active SparkSession.
    catalog
        Unity Catalog name.  Pass ``""`` to use the legacy Hive metastore.
    schema
        Schema / database for framework tables.  Default: ``"dq"``.
    """

    def __init__(self, spark, catalog: str = "", schema: str = "dq"):
        self.spark = spark
        self.catalog = catalog
        self.dq_schema = schema
        self._registry = FunctionRegistry()
        self._runner: Optional[DQRunner] = None

        # Register all built-in custom validators (email rules etc.)
        for name, fn in BUILTIN_VALIDATORS.items():
            register_validator(name, fn)

    # ------------------------------------------------------------------
    # Setup (equivalent to Script_00 + Script_01)
    # ------------------------------------------------------------------

    def setup(self, seed_data: bool = True) -> None:
        """
        Create the framework schema and all Delta tables, then seed master
        reference data.

        Equivalent to running Script_00_DDL_Framework_Tables.sql followed by
        Script_01_Master_Reference_Data.sql.

        Idempotency
        -----------
        Safe to call multiple times on the same catalog/schema:
        - ``CREATE SCHEMA IF NOT EXISTS`` — no-op if schema exists
        - ``CREATE TABLE IF NOT EXISTS`` — no-op if table exists
        - Master data seeding uses INSERT-ONLY MERGE — never overwrites
          existing rows (user modifications and custom keywords are preserved)

        Two kinds of tables
        -------------------
        FRAMEWORK-MANAGED (auto-seeded):
            masterDataCategory  — 27 data type classifications
            masterPattern       — 118 built-in validation patterns
            Users MAY add custom patterns using _ID >= 1000.

        USER-MANAGED (empty after setup, populated by project team):
            masterField, configFieldValues, configFieldAllowedPattern,
            configCustomQuery, mapDQChecks
            → Use ``dq.config`` (ConfigManager) or raw Spark SQL to populate.

        RESULTS (auto-populated by ``run_assessment()``):
            auditDQChecks, statDQChecks

        Parameters
        ----------
        seed_data
            If True (default) seed the 27 categories and 118 patterns.
            Pass False on subsequent calls to skip reseeding (INSERT-ONLY
            MERGE means it is harmless either way, but False is faster).

        Raises
        ------
        RuntimeError
            If ``catalog`` is specified but does not exist in Unity Catalog.
        """
        self._verify_catalog()
        self._create_schema()
        self._create_tables()
        create_reporting_views(self.spark, self.catalog, self.dq_schema)
        if seed_data:
            seed_master_data(self.spark, self.catalog, self.dq_schema)
            logger.info(
                "Framework-managed master data seeded successfully. "
                "User-managed tables (masterField, configFieldValues, "
                "configFieldAllowedPattern, configCustomQuery, mapDQChecks) "
                "are empty — populate them via dq.config or Spark SQL."
            )

    def _verify_catalog(self) -> None:
        """
        Verify that the specified Unity Catalog exists before trying to use it.
        Provides a helpful error message rather than a cryptic Spark exception.
        """
        if not self.catalog:
            return  # Legacy Hive metastore — no catalog check needed
        try:
            catalogs = [r["catalog"] for r in
                        self.spark.sql("SHOW CATALOGS").collect()]
            if self.catalog not in catalogs:
                raise RuntimeError(
                    f"Unity Catalog '{self.catalog}' does not exist. "
                    f"Available catalogs: {catalogs}. "
                    f"Create it first with: CREATE CATALOG `{self.catalog}`"
                )
        except Exception as exc:
            if "does not exist" in str(exc):
                raise
            # SHOW CATALOGS may not be supported in all environments — skip check
            logger.debug("Could not verify catalog existence: %s", exc)

    def _create_schema(self) -> None:
        fqn = (f"`{self.catalog}`.`{self.dq_schema}`"
               if self.catalog else f"`{self.dq_schema}`")
        self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {fqn}")
        logger.info("Schema ready: %s", fqn)

    def _create_tables(self) -> None:
        for table_name in TABLE_ORDER:
            ddl = DDL_STATEMENTS[table_name]
            fqn = self._fqn(table_name)
            rendered = ddl.format(fqn=fqn)
            self.spark.sql(rendered)
            table_type = (
                "[FRAMEWORK-MANAGED]" if table_name in FRAMEWORK_SEEDED_TABLES
                else "[USER-MANAGED]"   if table_name in USER_CONFIG_TABLES
                else "[RESULTS]"
            )
            logger.info("Table ready: %s  %s", fqn, table_type)

    def _fqn(self, table_name: str) -> str:
        if self.catalog:
            return f"`{self.catalog}`.`{self.dq_schema}`.`{table_name}`"
        return f"`{self.dq_schema}`.`{table_name}`"

    # ------------------------------------------------------------------
    # ConfigManager — Python API for user configuration
    # ------------------------------------------------------------------

    @property
    def config(self) -> ConfigManager:
        """
        Python API for populating user-managed configuration tables.

        Equivalent to writing INSERT/MERGE statements in Script_02.

        Example
        -------
        dq.config.register_field(1, 'Source.SRC_Party.FIRST_NAME', data_category_type_id=2) \\
                 .set_field_values(1, 'Source.SRC_Party.FIRST_NAME', min_data_length=2, max_data_length=20) \\
                 .block_category(1, 'Source.SRC_Party.FIRST_NAME', 'DataEmptiness') \\
                 .allow_pattern(2, 'Source.SRC_Party.FIRST_NAME', 'Has Hyphen') \\
                 .add_mapping(1, 'Source.SRC_Party.FIRST_NAME',
                              target_schema_name='Curated',
                              target_table_name='Individual_Denorm',
                              target_field_name='FIRST_NAME')
        dq.config.show_config_summary()
        """
        return ConfigManager(self.spark, self.catalog, self.dq_schema)

    # ------------------------------------------------------------------
    # Custom keyword / pattern management
    # ------------------------------------------------------------------

    def add_invalid_keyword(
        self,
        keyword: str,
        pattern_id: int,
        priority: int = 30,
        description: str = None,
    ) -> "DQFramework":
        """
        Add a project-specific invalid keyword to ``masterPattern``.

        The InvalidKeyword check fires when the keyword comprises >= 50%
        of the value (case-insensitive) — same threshold as the built-in
        40 keywords.

        After calling this, re-run ``generate_rule_functions()`` for the
        new keyword to take effect.

        Parameters
        ----------
        keyword
            The keyword to detect (e.g. 'nodata', 'noemail', 'notset').
        pattern_id
            Must be >= 1000.  Framework reserves IDs 1–999.

        Example
        -------
        dq.add_invalid_keyword('nodata',  pattern_id=1000)
        dq.add_invalid_keyword('noemail', pattern_id=1001)
        dq.generate_rule_functions()
        """
        add_invalid_keyword(
            self.spark, self.catalog, self.dq_schema,
            keyword, pattern_id, priority, description
        )
        return self

    def add_custom_pattern(
        self,
        pattern_id: int,
        pattern_category: str,
        pattern_name: str,
        pattern_priority: int,
        pattern_value: str = None,
        pattern_subcategory: str = None,
        pattern_description: str = None,
    ) -> "DQFramework":
        """
        Add any custom validation pattern to ``masterPattern``.

        After adding:
        1. Add allow/block rules in ``configFieldAllowedPattern``.
        2. Register a Python check function if needed (for custom categories).
        3. Re-run ``generate_rule_functions()``.

        pattern_id must be >= 1000.
        """
        add_custom_pattern(
            self.spark, self.catalog, self.dq_schema,
            pattern_id, pattern_category, pattern_name, pattern_priority,
            pattern_value, pattern_subcategory, pattern_description
        )
        return self

    # ------------------------------------------------------------------
    # Custom validator registration
    # ------------------------------------------------------------------

    def register_validator(self, name: str, fn) -> "DQFramework":
        """
        Register a Python callable as a named custom validator for use in
        ``configCustomQuery.CustomQuery``.

        Parameters
        ----------
        name
            The string to store in ``configCustomQuery.CustomQueryPython``.
        fn
            A callable ``(value: str | None) -> bool`` that returns True when
            the condition is matched.

        Returns self for method chaining.
        """
        register_validator(name, fn)
        return self

    # ------------------------------------------------------------------
    # generate_rule_functions (equivalent to p_DQ_GenerateRuleFunctions)
    # ------------------------------------------------------------------

    def generate_rule_functions(self, execution_id: Optional[str] = None) -> None:
        """
        Build field-level checker functions from the configuration tables.

        Equivalent to ``EXEC [dq].[p_DQ_GenerateRuleFunctions]``.

        Must be called (or re-called) whenever any configuration table is
        modified: masterField, configFieldValues, configFieldAllowedPattern,
        or configCustomQuery.

        Parameters
        ----------
        execution_id
            Optional batch identifier embedded in log output.
        """
        if execution_id is None:
            execution_id = str(uuid.uuid4())

        # Pre-flight: abort if configuration has referential integrity issues
        result = self.config.verify_config()
        if not result["ok"]:
            lines = "\n  ".join(result["issues"])
            raise RuntimeError(
                f"generate_rule_functions() aborted — {len(result['issues'])} config issue(s) found:\n"
                f"  {lines}\n"
                f"Fix the issues above then re-run generate_rule_functions().\n"
                f"(Run dq.config.show_config_summary() for a full overview.)"
            )

        # Load config tables
        cfap = [row.asDict() for row in
                self.spark.table(self._fqn("configFieldAllowedPattern"))
                    .filter("IsActive = true").collect()]
        mp   = [row.asDict() for row in
                self.spark.table(self._fqn("masterPattern"))
                    .filter("IsActive = true").collect()]
        fv   = [row.asDict() for row in
                self.spark.table(self._fqn("configFieldValues"))
                    .filter("IsActive = true").collect()]
        cq   = [row.asDict() for row in
                self.spark.table(self._fqn("configCustomQuery"))
                    .filter("IsActive = true AND CustomQuery IS NOT NULL").collect()]

        # L0A: Resolve pattern precedence
        resolved = resolve_patterns(cfap, mp)

        # Generate and register field checkers
        generate_rule_functions(resolved, fv, cq, self._registry, execution_id)

        logger.info(
            "generate_rule_functions complete. %d functions in registry.",
            len(self._registry)
        )

        fn_list = sorted(self._registry.list_functions())
        print(f"✓ generate_rule_functions complete — {len(fn_list)} field checker(s) registered:")
        for fn in fn_list:
            sql_count = len(self._registry.get_sql_expressions(fn))
            suffix = f"  [{sql_count} SQL expr]" if sql_count else ""
            print(f"  • {fn}{suffix}")

        # Warn about DQ functions referenced in mapDQChecks that have no checker
        # in the registry — meaning no rules were configured for them in any
        # config table.  The assessment runner skips these silently; this warning
        # surfaces the gap so users can act on it.
        mapped_fn_names = {
            row["DQFunctionName"]
            for row in self.spark.table(self._fqn("mapDQChecks"))
                .filter("IsActive = true")
                .select("DQFunctionName")
                .distinct()
                .collect()
        }
        registered_names = set(self._registry.list_functions())
        no_rules = [fn for fn in sorted(mapped_fn_names) if fn not in registered_names]
        if no_rules:
            print(f"\n  ⚠  {len(no_rules)} mapped DQ function(s) have no rules configured"
                  f" — skipped by assessment (add rules to include them):")
            for fn in no_rules:
                print(f"     • {fn}")

    # ------------------------------------------------------------------
    # Curated table preparation — add DQ columns before assessment
    # ------------------------------------------------------------------

    def prepare_curated_table(
        self,
        schema_name: str,
        table_name: str,
        catalog_name: Optional[str] = None,
    ) -> None:
        """
        Add DQRowID, DQEligible, DQViolations, and DQFields columns to a curated table.

        These four columns must exist on every curated table before running
        run_assessment(). DQRowID is populated with a UUID per row and is used
        as the MERGE join key during assessment — ensuring correctness even when
        the table contains fully-duplicate rows. This method is idempotent —
        columns that already exist are left untouched, new NULL DQRowIDs are filled.

        Requires ALTER privilege on the target table.

        Parameters
        ----------
        schema_name
            Schema of the curated table (e.g. 'MyCuratedSchema').
        table_name
            Name of the curated table (e.g. 'mock_curated_contacts').
        catalog_name
            Catalog of the curated table. Defaults to the framework catalog.

        Example
        -------
        dq.prepare_curated_table("MyCuratedSchema", "mock_curated_contacts")
        """
        cat = catalog_name or self.catalog
        if cat:
            fqn = f"`{cat}`.`{schema_name}`.`{table_name}`"
        else:
            fqn = f"`{schema_name}`.`{table_name}`"

        existing_cols = {f.name for f in self.spark.table(fqn).schema.fields}
        dq_col_ddl = [
            ("DQRowID",      "STRING",
             "UUID — stable unique row identifier used as MERGE join key by the assessment engine"),
            ("DQEligible",   "BOOLEAN",
             "1=all checks passed, 0=at least one failed, NULL=not yet assessed"),
            ("DQViolations", "STRING",
             "[field: ViolationType], ... — accumulated DQ violations across assessed fields"),
            ("DQFields",     "STRING",
             "[field1], [field2] — all fields assessed on this row"),
        ]
        missing = [(col, dtype, comment)
                   for col, dtype, comment in dq_col_ddl
                   if col not in existing_cols]

        if not missing:
            print(f"✓ {fqn} — all 4 DQ columns already present.")
        else:
            cols_sql = ",\n    ".join(
                f"`{col}` {dtype} COMMENT '{comment}'"
                for col, dtype, comment in missing
            )
            self.spark.sql(f"ALTER TABLE {fqn} ADD COLUMNS (\n    {cols_sql}\n)")
            print(f"✓ {fqn} — added {len(missing)} DQ column(s):")
            for col, dtype, _ in missing:
                print(f"   + {col}  ({dtype})")

        # Always populate NULL DQRowIDs — covers both the initial ADD and any rows
        # inserted after the last prepare run.
        # Delta UPDATE rejects uuid() (INVALID_NON_DETERMINISTIC_EXPRESSIONS).
        # replaceWhere atomically replaces only NULL-DQRowID rows; all others untouched.
        null_count = self.spark.sql(
            f"SELECT COUNT(*) FROM {fqn} WHERE DQRowID IS NULL"
        ).first()[0]
        if null_count > 0:
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
                    self.spark.table(fqn)
                    .filter(F.col("DQRowID").isNull())
                    .withColumn("DQRowID", F.expr("uuid()"))
                    .write.format("delta")
                    .option("replaceWhere", "DQRowID IS NULL")
                    .mode("overwrite")
                    .saveAsTable(fqn)
                )
            finally:
                self.spark.conf.set(
                    "spark.databricks.delta.replaceWhere.constraintCheck.enabled", "true"
                )
            print(f"   ✓ DQRowID populated for {null_count} row(s)")

    def prepare_curated_tables(
        self,
        schema_name: Optional[str] = None,
    ) -> None:
        """
        Add DQ columns to all curated tables referenced in active mapDQChecks rows.

        Reads distinct (TargetCatalogName, TargetSchemaName, TargetTableName)
        from mapDQChecks and calls prepare_curated_table() for each.
        Idempotent — safe to re-run at any time.

        Requires ALTER privilege on each target table.

        Parameters
        ----------
        schema_name
            If provided, only tables in this schema are prepared.
            None = all schemas referenced in mapDQChecks.

        Example
        -------
        dq.prepare_curated_tables()                           # all mapped tables
        dq.prepare_curated_tables("MyCuratedSchema")          # one schema only
        """
        rows = (
            self.spark.table(self._fqn("mapDQChecks"))
                .filter("IsActive = true")
                .select("TargetCatalogName", "TargetSchemaName", "TargetTableName")
                .distinct()
                .collect()
        )
        if schema_name:
            rows = [r for r in rows if r["TargetSchemaName"] == schema_name]

        if not rows:
            print("No active mappings found in mapDQChecks. Nothing to prepare.")
            return

        print(f"Preparing {len(rows)} curated table(s) for DQ assessment...")
        for row in rows:
            self.prepare_curated_table(
                schema_name=row["TargetSchemaName"],
                table_name=row["TargetTableName"],
                catalog_name=row["TargetCatalogName"],
            )

    # ------------------------------------------------------------------
    # run_assessment (equivalent to p_DQ_DataAssessmentRules)
    # ------------------------------------------------------------------

    def run_assessment(
        self,
        schema_name: Optional[str] = None,
        table_name: Optional[str] = None,
        field_name: Optional[str] = None,
        reset_eligible_flag: bool = False,
        enable_output: bool = True,
        execution_id: Optional[str] = None,
    ) -> str:
        """
        Execute the DQ assessment against curated Delta tables.

        Equivalent to ``EXEC [dq].[p_DQ_DataAssessmentRules]``.

        Parameters
        ----------
        schema_name
            Curated schema to assess.  None = all schemas.
        table_name
            Specific table to assess.  None = all tables in schema.
        field_name
            Specific field to assess.  None = all fields in table.
        reset_eligible_flag
            True = reset DQEligible / DQViolations / DQFields on previously
            failed rows (WHERE DQEligible = 0) and exit.
            False = run full assessment.
        enable_output
            True = display violation and quality score results.
        execution_id
            Supply a fixed UUID for grouped tracking; None generates a new one.

        Returns
        -------
        The ExecutionID used for this batch (use to filter reporting views).

        Examples
        --------
        # Assess all fields in the Curated schema:
        exec_id = dq.run_assessment(schema_name="Curated")

        # Assess a single table:
        exec_id = dq.run_assessment(schema_name="Curated",
                                    table_name="Individual_Denorm")

        # Reset DQ flags before re-assessment:
        dq.run_assessment(schema_name="Curated", reset_eligible_flag=True,
                          enable_output=False)
        """
        if not self._registry:
            raise RuntimeError(
                "Function registry is empty. Run generate_rule_functions() first. "
                "If you just called setup(), you also need to populate the user-managed "
                "config tables (masterField, configFieldAllowedPattern, configCustomQuery, "
                "mapDQChecks) via dq.config or Spark SQL before generating functions."
            )

        runner = DQRunner(self.spark, self.catalog, self.dq_schema, self._registry)
        return runner.run(
            schema_name=schema_name,
            table_name=table_name,
            field_name=field_name,
            reset_eligible_flag=reset_eligible_flag,
            enable_output=enable_output,
            execution_id=execution_id,
        )

    # ------------------------------------------------------------------
    # Convenience reporting helpers
    # ------------------------------------------------------------------

    def violations(self, execution_id: str = None):
        """Return a DataFrame of all violations for the given execution."""
        from dq_framework.reporting.views import query_violations
        return query_violations(self.spark, self.catalog, self.dq_schema, execution_id)

    def quality_scores(self, execution_id: str = None):
        """Return a DataFrame of quality scores per field for the given execution."""
        from dq_framework.reporting.views import query_quality_scores
        return query_quality_scores(self.spark, self.catalog, self.dq_schema, execution_id)

    def fields_below_threshold(self, threshold: float = 80.0):
        """Return fields with PercentageQualified below the given threshold."""
        from dq_framework.reporting.views import query_fields_below_threshold
        return query_fields_below_threshold(self.spark, self.catalog, self.dq_schema, threshold)

    def field_rule_summary(self, field_name: str = None):
        """
        Return a flat DataFrame of all active rules for a field (or all fields).

        One row per rule — suitable for .display(), export to Excel, pivot, and filter.

        Parameters
        ----------
        field_name
            Logical FullFieldName (e.g. 'email_address'), physical Schema.Table.Column,
            or omit for all configured fields.

        Example
        -------
        dq.field_rule_summary("email_address").display()
        dq.field_rule_summary("silver.mock_curated_contacts.email").display()
        dq.field_rule_summary().display()
        """
        return self.config.field_rule_summary(field_name)

    def summary_by_violation_type(self, execution_id: str = None):
        """Return violation record counts grouped by field and violation type."""
        from dq_framework.reporting.views import query_summary_by_violation_type
        return query_summary_by_violation_type(self.spark, self.catalog, self.dq_schema, execution_id)

    def summary_by_table(self, execution_id: str = None):
        """Return aggregated quality score per curated table."""
        from dq_framework.reporting.views import query_summary_by_table
        return query_summary_by_table(self.spark, self.catalog, self.dq_schema, execution_id)

    # ------------------------------------------------------------------
    # Checker inspection
    # ------------------------------------------------------------------

    def inspect_checker(self, fn_name: str, show_all_patterns: bool = False) -> None:
        """
        Print a human-readable breakdown of all rules compiled into a field checker.

        Shows every L01/L02/L03/L04 rule that will be applied at assessment time,
        so you can verify the in-memory function matches your configuration without
        needing to read raw config table rows.

        Parameters
        ----------
        fn_name
            Registry key to inspect (e.g. 'fn_DQ_email_address').
        show_all_patterns
            False (default) — summarise L03 patterns grouped by check category.
            True            — list every individual pattern with its priority,
                              allow/block flag, and pattern value.

        Example
        -------
        dq.inspect_checker("fn_DQ_email_address")
        dq.inspect_checker("fn_DQ_email_address", show_all_patterns=True)
        """
        import inspect as _inspect

        fn = self._registry.get(fn_name)
        if fn is None:
            available = sorted(self._registry.list_functions())
            print(f"No checker '{fn_name}' in registry.")
            print(f"Registered: {available}")
            return

        try:
            cvars = _inspect.getclosurevars(fn).nonlocals
        except Exception as exc:
            print(f"Could not introspect closure for '{fn_name}': {exc}")
            return

        l01      = cvars.get("l01_check")
        l02_list = cvars.get("l02_checks", [])
        l03_list = cvars.get("l03_sorted", [])
        l04      = cvars.get("l04_check")
        sql_exprs = self._registry.get_sql_expressions(fn_name)

        W = 64
        print(f"\n{'─' * W}")
        print(f"  {fn_name}")
        print(f"{'─' * W}")

        # ── L01 ──────────────────────────────────────────────────────────
        if l01 is not None:
            try:
                v = _inspect.getclosurevars(l01).nonlocals
                print(f"  L01  Data Length    : {v.get('min_len','?')} – {v.get('max_len','?')} chars")
            except Exception:
                print(f"  L01  Data Length    : configured (cannot read bounds)")
        else:
            print(f"  L01  Data Length    : not configured")

        # ── L02 Python closures ───────────────────────────────────────────
        total_l02 = len(l02_list) + len(sql_exprs)
        if total_l02:
            print(f"  L02  Custom Queries : {total_l02} rule(s)")
            for i, l02_fn in enumerate(l02_list, 1):
                try:
                    v = _inspect.getclosurevars(l02_fn).nonlocals
                    cq_id      = v.get("cq_id", "?")
                    is_allowed = v.get("is_allowed", "?")
                    # expression and _cqtype live in _evaluate's closure, not check's —
                    # go one level deeper to retrieve them
                    _ev_fn = v.get("_evaluate")
                    ev: dict = {}
                    if _ev_fn is not None:
                        try:
                            ev = _inspect.getclosurevars(_ev_fn).nonlocals
                        except Exception:
                            pass
                    expr   = str(ev.get("expression", v.get("expression", "?")))
                    cqtype = ev.get("_cqtype") or v.get("_cqtype") or "auto"
                    flag   = "MUST MATCH" if is_allowed else "MUST NOT MATCH"
                    print(f"       [{i}] ID={cq_id}  type={cqtype}  {flag}")
                    print(f"            {expr[:80]}{'…' if len(expr) > 80 else ''}")
                except Exception:
                    print(f"       [{i}] (cannot introspect closure)")
            for i, row in enumerate(sql_exprs, len(l02_list) + 1):
                cq_id      = row.get("_ID", "?")
                expr       = str(row.get("CustomQuery", "?"))
                is_allowed = row.get("IsConditionAllowed", True)
                flag       = "MUST MATCH" if is_allowed else "MUST NOT MATCH"
                print(f"       [{i}] ID={cq_id}  type=SQL  {flag}")
                print(f"            {expr[:80]}{'…' if len(expr) > 80 else ''}")
        else:
            print(f"  L02  Custom Queries : not configured")

        # ── L03 patterns ──────────────────────────────────────────────────
        if l03_list:
            print(f"  L03  Pattern Checks : {len(l03_list)} pattern(s)")
            if show_all_patterns:
                for priority, check_fn, is_allowed, pattern_value in l03_list:
                    flag  = "ALLOW" if is_allowed else "BLOCK"
                    label = getattr(check_fn, "__name__", str(check_fn))
                    val   = f"  value={pattern_value!r}" if pattern_value else ""
                    print(f"       pri={priority:3d}  [{flag}]  {label}{val}")
            else:
                # Group by check function name (≈ pattern category) for a compact view
                from collections import Counter
                counts: Counter = Counter()
                flags: dict = {}
                for _, check_fn, is_allowed, _ in l03_list:
                    label = getattr(check_fn, "__name__", "unknown")
                    counts[label] += 1
                    flags.setdefault(label, set()).add("ALLOW" if is_allowed else "BLOCK")
                for label, count in sorted(counts.items()):
                    flag_str = "+".join(sorted(flags[label]))
                    print(f"       {label:<40}  {count:3d}  [{flag_str}]")
                print(f"       (pass show_all_patterns=True to list individually)")
        else:
            print(f"  L03  Pattern Checks : not configured")

        # ── L04 ──────────────────────────────────────────────────────────
        if l04 is not None:
            try:
                v = _inspect.getclosurevars(l04).nonlocals
                print(f"  L04  Value Range    : '{v.get('min_val','?')}' – '{v.get('max_val','?')}'")
            except Exception:
                print(f"  L04  Value Range    : configured (cannot read bounds)")
        else:
            print(f"  L04  Value Range    : not configured")

        print(f"{'─' * W}")

    def test_checker(self, fn_name: str, *values) -> None:
        """
        Run a field checker against one or more test values and print the full result.

        Each result prints the pass/fail outcome, the violation type, and the
        diagnostic log message produced by the exact code path that executed.
        The log messages are embedded in every check closure (L01, L02, L03, L04)
        and state precisely which rule ran, whether it passed or failed, and why —
        making this the primary tool for debugging unexpected assessment outcomes.

        Parameters
        ----------
        fn_name
            Registry key to test (e.g. 'fn_DQ_email_address').
        *values
            One or more values to run through the checker.
            Pass None to test NULL handling.

        Example
        -------
        dq.test_checker("fn_DQ_email_address",
                        "user@example.com",   # expected PASS
                        "not-an-email",        # expected FAIL
                        "",                    # empty — expected FAIL
                        None)                  # NULL — expected PASS (not applicable)
        """
        fn = self._registry.get(fn_name)
        if fn is None:
            print(f"No checker '{fn_name}' in registry. "
                  f"Available: {sorted(self._registry.list_functions())}")
            return

        print(f"\n{'─' * 66}")
        print(f"  {fn_name}  —  {len(values)} test value(s)")
        print(f"{'─' * 66}")
        for val in values:
            result, vtype, msg = fn(val)
            status = "✓ PASS" if result else "✗ FAIL"
            print(f"\n  input     : {val!r}")
            print(f"  outcome   : {status}  |  violation: {vtype or '—'}")
            if msg:
                print(f"  log       : {msg}")
        print(f"{'─' * 66}")

    def validate_custom_queries_sql(self) -> bool:
        """
        Validate all registered SQL-type custom queries against Spark SQL before
        running an assessment.

        Each SQL expression has its ``@InputValue`` placeholder substituted with
        ``CAST(NULL AS STRING)`` — identical to the substitution used inside
        ``_apply_sql_l02_checks`` at assessment time — then executed via
        ``spark.sql(...).limit(0).count()`` to trigger Spark's parse / analysis
        phase without scanning any data.

        The query result is always NULL (``CAST(NULL AS STRING) LIKE '...'``
        evaluates to NULL), but that is intentional: this step only checks that
        Spark can *parse* the expression without a syntax error.  The actual
        value comparison happens at assessment time when ``@InputValue`` is
        replaced with the real column reference and applied to every data row.

        Python-type and Regex-type custom queries do not need this validator —
        they are compiled into closures at ``generate_rule_functions()`` time and
        any errors surface immediately during that step.

        Returns
        -------
        bool
            True  — all SQL expressions parsed successfully.
            False — one or more expressions failed; details printed to stdout.

        Example
        -------
        dq.generate_rule_functions()
        ok = dq.validate_custom_queries_sql()
        if ok:
            dq.run_assessment(schema_name="curated")
        """
        all_exprs = self._registry._sql_expressions  # dict[fn_name, list[dict]]

        # Count REGEX/PYTHON CQs (in closures, not in _sql_expressions) for info
        all_fns = self._registry.list_functions()
        import inspect as _inspect
        regex_python_count = 0
        for fn_name in all_fns:
            fn = self._registry.get(fn_name)
            if fn:
                try:
                    l02_list = _inspect.getclosurevars(fn).nonlocals.get("l02_checks", [])
                    regex_python_count += len(l02_list)
                except Exception:
                    pass

        total = sum(len(v) for v in all_exprs.values())
        if total == 0:
            msg = "No SQL custom queries registered. Nothing to validate."
            if regex_python_count:
                msg += (f"\n  Note: {regex_python_count} REGEX/PYTHON custom query rule(s) exist "
                        f"but are not in scope — they compile at generate_rule_functions() time.")
            print(msg)
            return True

        W = 70
        print(f"\n{'─' * W}")
        print(f"  validate_custom_queries_sql  —  {total} SQL expression(s) across "
              f"{len(all_exprs)} checker(s)")
        if regex_python_count:
            print(f"  (REGEX/PYTHON: {regex_python_count} rule(s) not in scope — "
                  f"validated at generate_rule_functions() time)")
        print(f"{'─' * W}")

        passed = 0
        failed = 0
        invalid_rows: list[tuple[str, int | str, str, str]] = []

        for fn_name, exprs in sorted(all_exprs.items()):
            for row in sorted(exprs, key=lambda x: x.get("_ID", 0)):
                cq_id    = row.get("_ID", "?")
                raw_expr = (row.get("CustomQuery") or "").strip()
                if not raw_expr:
                    continue

                validate_sql = raw_expr.replace("@InputValue", "CAST(NULL AS STRING)")
                try:
                    self.spark.sql(
                        f"SELECT CAST(({validate_sql}) AS BOOLEAN) AS _dq_validate"
                    ).limit(0).count()
                    passed += 1
                except Exception as exc:
                    failed += 1
                    invalid_rows.append((fn_name, cq_id, raw_expr, str(exc)))

        print(f"\n  ✓ Valid   : {passed}")
        print(f"  ✗ Invalid : {failed}")

        if invalid_rows:
            print(f"\n{'─' * W}")
            print("  INVALID EXPRESSIONS")
            print("  Fix in configCustomQuery, then re-run generate_rule_functions().")
            print(f"{'─' * W}")
            print(
                "  Common T-SQL → Spark SQL translations:\n"
                "    LEN(x)                 →  LENGTH(x)\n"
                "    CHARINDEX(needle, str) →  LOCATE(needle, str)\n"
                "    ISNULL(x, y)           →  COALESCE(x, y)\n"
                "    GETDATE()              →  current_timestamp()\n"
                "    String literals must be single-quoted:  LIKE '%x%'\n"
                "    @ in string literals must be quoted:    LIKE '%@%'\n"
            )
            print(f"{'─' * W}")
            for fn_name, cq_id, raw_expr, err in invalid_rows:
                print(f"\n  Checker   : {fn_name}")
                print(f"  CQ ID     : {cq_id}")
                print(f"  Expression: {raw_expr}")
                print(f"  Error     : {err}")

        else:
            print(f"\n  All SQL expressions are valid Spark SQL.")

        print(f"\n{'─' * W}")
        return failed == 0

    # ------------------------------------------------------------------
    # Sample usage resources
    # ------------------------------------------------------------------

    def sample_usage(self, spark=None) -> str:
        """Extract the bundled sample_usage folder to the user's Workspace and list contents.

        The ``sample_usage/`` folder is bundled inside the installed package
        (``dq_framework/sample_usage/``).  On first call this method copies all
        files to ``/Workspace/Users/{current_user()}/databricks-dq-framework/sample_usage/``
        so they are private to each user, accessible from any compute type
        (serverless, classic, DBFS-less), and independent of any Repos clone.

        Subsequent calls are idempotent — files already present are not overwritten
        unless the bundled version is newer (compared by mtime).

        Parameters
        ----------
        spark : SparkSession, optional
            Active SparkSession.  When omitted the method attempts to import
            ``pyspark.sql.SparkSession`` and use the active session.

        Returns
        -------
        str
            The resolved ``/Workspace/Users/{user}/...`` folder path.

        Raises
        ------
        RuntimeError
            If ``current_user()`` cannot be resolved and no user path can be built.
        """
        import os
        import shutil

        # ------------------------------------------------------------------
        # Source: sample_usage/ bundled inside the installed package
        # ------------------------------------------------------------------
        pkg_dir    = os.path.dirname(os.path.abspath(__file__))
        bundled    = os.path.join(pkg_dir, "sample_usage")

        # ------------------------------------------------------------------
        # Resolve current user for the destination path
        # ------------------------------------------------------------------
        repo_user: str = ""
        if spark is None:
            try:
                from pyspark.sql import SparkSession as _SS
                spark = _SS.getActiveSession()
            except Exception:
                spark = None
        if spark is not None:
            try:
                repo_user = spark.sql("SELECT current_user()").first()[0] or ""
            except Exception:
                pass

        if not repo_user:
            raise RuntimeError(
                "dq.sample_usage(): could not resolve current_user() — "
                "pass an active SparkSession: dq.sample_usage(spark)"
            )

        # ------------------------------------------------------------------
        # Destination: private to each user, under /Workspace/Users/
        # ------------------------------------------------------------------
        dest = f"/Workspace/Users/{repo_user}/databricks-dq-framework/sample_usage"
        os.makedirs(dest, exist_ok=True)

        # Copy bundled files to destination (skip if dest file is already up-to-date)
        copied = []
        if os.path.isdir(bundled):
            for fname in os.listdir(bundled):
                if fname.startswith(".") or fname.startswith("~$"):
                    continue
                src_file  = os.path.join(bundled, fname)
                dest_file = os.path.join(dest, fname)
                if not os.path.isfile(src_file):
                    continue
                if (not os.path.exists(dest_file)
                        or os.path.getmtime(src_file) > os.path.getmtime(dest_file)):
                    shutil.copy2(src_file, dest_file)
                    copied.append(fname)

        if copied:
            print(f"  Extracted {len(copied)} file(s) → {dest}")

        # Scan folder contents
        base_path = dest
        notebooks, data, templates, other = [], [], [], []
        for fname in sorted(os.listdir(base_path)):
            if fname.startswith(".") or fname.startswith("~$"):
                continue
            ext = os.path.splitext(fname)[1].lower()
            if ext in (".py", ".ipynb"):
                notebooks.append(fname)
            elif ext in (".csv", ".parquet", ".json", ".delta"):
                data.append(fname)
            elif ext in (".xlsx", ".xls", ".xlsm"):
                templates.append(fname)
            else:
                other.append(fname)

        def _section(title, files):
            if not files:
                return ""
            lines = [f"\n  {title}"]
            for f in files:
                lines.append(f"    \u2022 {f}")
            return "\n".join(lines)

        body = _section("Notebooks", notebooks)
        body += _section("Data", data)
        body += _section("Templates", templates)
        body += _section("Other", other)

        print(f"\nSample resources are at:\n  {base_path}{body}\n")
        return base_path

    # ------------------------------------------------------------------
    # Guide / help
    # ------------------------------------------------------------------

    def guide(self) -> None:
        """Print a concise usage guide for the DQ Framework."""
        import dq_framework as _dqf
        print(f"""
╔══════════════════════════════════════════════════════════════════════╗
║       Databricks DQ Non-Functional Assessment Framework  v{_dqf.__version__:<9}║
╚══════════════════════════════════════════════════════════════════════╝

WORKFLOW  (run steps in order)
──────────────────────────────────────────────────────────────────────
 1. dq.setup()
      Creates 9 Delta tables + 2 views in your DQ schema.
      Seeds 27 data categories and 118 built-in validation patterns.
      Safe to re-run (idempotent).

 2. dq.config.<method>(...)           ← populate 5 user-managed tables
      Define what to check and where to check it (see CONFIG below).

 3. dq.generate_rule_functions()
      Reads your config tables and builds one in-memory Python checker
      per field definition.  Re-run whenever config changes.

 3b. dq.prepare_curated_tables()           ← REQUIRED before first assessment
      Adds DQRowID / DQEligible / DQViolations / DQFields to all curated tables
      in mapDQChecks.  DQRowID is populated with a UUID per row (MERGE join key).
      Idempotent — re-run after inserting new rows to fill any NULL DQRowIDs.
      Requires ALTER on each table.
      Or for a single table: dq.prepare_curated_table("Schema", "TableName")

 3c. dq.validate_custom_queries_sql()      ← optional pre-flight for SQL rules
      Validates all SQL-type custom queries against Spark SQL before assessment.
      Catches T-SQL syntax (LEN, CHARINDEX, unquoted literals) that would
      otherwise be silently skipped during run_assessment().
      Python/Regex custom queries do not need this — they compile at step 3.

 4. exec_id = dq.run_assessment(schema_name="YourSchema")
      Runs all checkers against your curated Delta tables.
      Writes DQEligible / DQViolations / DQFields back to each row.
      Returns an ExecutionID for filtering results.

 5. dq.violations(exec_id).display()
    dq.quality_scores(exec_id).display()
    dq.summary_by_violation_type(exec_id).display()
    dq.summary_by_table().display()
    dq.fields_below_threshold(threshold=80).display()
    dq.field_rule_summary("my_field").display()   # config audit — one row per rule
    dq.field_rule_summary().display()             # all fields

──────────────────────────────────────────────────────────────────────
CONFIG — two patterns for FullFieldName
──────────────────────────────────────────────────────────────────────
 Pattern A  — Reusable rule (recommended for generic checks)
   FullFieldName = 'email_address'
   Define the DQ rules once.  Map the same definition to as many
   physical columns and tables as needed via mapDQChecks.
   Use when: the same validation logic applies across multiple tables
   (e.g. every email column everywhere follows the same rules).

 Pattern B  — Column-specific rule
   FullFieldName = 'Schema.Table.ColumnName'
   Rules are tied to one exact column.
   Use when: a column has unique constraints not shared anywhere else.

 Both patterns can coexist.  Mix freely.

──────────────────────────────────────────────────────────────────────
CONFIG METHODS  (dq.config.<method>)
──────────────────────────────────────────────────────────────────────
 register_field(full_field_name, data_category_type_id)
      Register a field definition and its data category (1-27).
      Required before adding any rules for a field.

 set_field_values(full_field_name, ...)
      L01 check: min/max character length.
      L04 check: min/max lexicographic value range.

 add_pattern_rule(full_field_name, is_pattern_allowed, ...)
      L03 check: allow or block a named pattern, sub-category, or
      entire category from the 118 built-in patterns.
      block_category / allow_pattern / block_pattern are shortcuts.

 add_custom_query(full_field_name, expression, is_condition_allowed, ...)
      L02 check: custom validation expression.
      custom_query_type='REGEX'  — regex applied via re.search()
      custom_query_type='SQL'    — Spark SQL with @InputValue placeholder,
                                   applied at DataFrame level via F.expr()
      custom_query_type='PYTHON' — named validator (register_validator)

 add_mapping(full_field_name, target_schema, target_table, target_field)
      Links a field definition to a physical column in a curated table.
      One field definition can map to many physical columns.
      The assessment engine uses this table to know what to scan.

──────────────────────────────────────────────────────────────────────
VALIDATION LEVELS  (applied in order, early-exit on first failure)
──────────────────────────────────────────────────────────────────────
 L01  Data length range        set_field_values(min_data_length, max_data_length)
 L02  Custom expression        add_custom_query(expression, ...)
 L03  Built-in pattern check   add_pattern_rule(pattern_name / category, ...)
 L04  Data value range         set_field_values(min_data_value, max_data_value)
 L99  Default PASS             (reached only if all prior levels pass)

──────────────────────────────────────────────────────────────────────
OTHER METHODS
──────────────────────────────────────────────────────────────────────
 dq.prepare_curated_tables()         Add DQEligible/DQViolations/DQFields to all mapped tables
 dq.prepare_curated_table(s, t)      Add DQ columns to a single curated table
 dq.validate_custom_queries_sql()    Pre-flight: validate all SQL-type L02 expressions against Spark SQL
 dq.inspect_checker("fn_DQ_...")     Verify config: what rules are compiled into a checker
 dq.inspect_checker(..., show_all_patterns=True)  List every L03 pattern individually
 dq.test_checker("fn_DQ_...", val1, val2, None)  Debug: run checker + print exact log messages
 dq.config.show_config_summary()     Overview of row counts + config health
 dq.config.verify_config()           Pre-flight: duplicate _ID check + duplicate logical rule check +
                                   FK integrity check. Called automatically by generate_rule_functions()
                                   (raises RuntimeError on failure) and show_config_summary() (prints banner).
 dq.sample_usage(spark)              Get started: shows sample notebooks, data & config template to self-demo the framework
 dq.add_invalid_keyword(word)        Extend the InvalidKeyword pattern list
 dq.add_custom_pattern(...)          Add a custom regex to masterPattern
 dq.register_validator(name, fn)     Register a Python callable for L02 PYTHON
 dq.run_assessment(reset_eligible_flag=True)  Clear prior DQ flags and re-run
""".rstrip())
