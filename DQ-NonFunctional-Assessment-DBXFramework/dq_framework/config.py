"""
ConfigManager — Python API for User Configuration
===================================================
Provides a typed Python interface for populating the five user-managed
configuration tables without writing raw Spark SQL.

Equivalent to the guidance in Script_02_Field_Configuration_Template.sql,
but as a reusable Python API that project teams can call from notebooks or
pipelines.

Table ownership
---------------
FRAMEWORK-MANAGED (auto-seeded, do not populate manually):
    masterDataCategory      -- 27 types
    masterPattern           -- 118 built-in patterns + user extensions (_ID >= 1000)

USER-MANAGED (populated via ConfigManager or raw Spark SQL):
    masterField             -- register_field()
    configFieldValues       -- set_field_values()
    configFieldAllowedPattern -- add_pattern_rule(), block_category(), allow_pattern()
    configCustomQuery       -- add_custom_query_regex() / add_custom_query()
    mapDQChecks             -- add_mapping()

RESULTS (auto-populated by the assessment engine):
    auditDQChecks
    statDQChecks

Idempotency
-----------
All methods use MERGE (insert-only on _ID) so they are safe to re-run.
Existing rows with the same _ID are never overwritten.
To update an existing rule, delete the old row first via Spark SQL then
re-insert, or use the ``replace=True`` parameter where supported.

FullFieldName convention
------------------------
Must follow the format ``SourceSchema.SourceTable.ColumnName``, e.g.:
    ``Source.SRC_ContactPoint.EMAIL_ADDRESS``

DQFunctionName convention
--------------------------
Must be: ``fn_DQ_<SourceSchema>_<SourceTable>_<ColumnName>``
The ConfigManager derives this automatically from FullFieldName.
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Optional

logger = logging.getLogger(__name__)


class ConfigManager:
    """
    Python API for populating the user-managed DQ configuration tables.

    Parameters
    ----------
    spark
        Active SparkSession.
    catalog
        Unity Catalog name (or ``""`` for legacy Hive metastore).
    dq_schema
        Schema containing the framework tables (default: ``"dq"``).
    """

    def __init__(self, spark, catalog: str = "", dq_schema: str = "dq"):
        self.spark = spark
        self.catalog = catalog
        self.dq_schema = dq_schema

    def _fqn(self, table: str) -> str:
        if self.catalog:
            return f"`{self.catalog}`.`{self.dq_schema}`.`{table}`"
        return f"`{self.dq_schema}`.`{table}`"

    @staticmethod
    def _col_str(v: Optional[str], default_sql: str = "current_user()") -> str:
        """Return SQL literal for an optional string override, or the default SQL expression."""
        return f"'{v}'" if v is not None else default_sql

    def _next_id(self, table_name: str) -> int:
        """Return MAX(_ID) + 1 for the given table, or 1 if the table is empty."""
        fqn = self._fqn(table_name)
        row = self.spark.sql(f"SELECT COALESCE(MAX(_ID), 0) + 1 AS next_id FROM {fqn}").collect()[0]
        return int(row["next_id"])

    @staticmethod
    def derive_function_name(full_field_name: str) -> str:
        """
        Derive the DQFunctionName from a FullFieldName.

        ``Source.SRC_Party.FIRST_NAME``
            → ``fn_DQ_Source_SRC_Party_FIRST_NAME``
        """
        parts = full_field_name.split(".", 2)
        return "fn_DQ_" + "_".join(parts)

    # ------------------------------------------------------------------
    # masterField
    # ------------------------------------------------------------------

    def register_field(
        self,
        full_field_name: str,
        data_category_type_id: int,
        field_id: Optional[int] = None,
        is_active: bool = True,
        created_by: Optional[str] = None,
        last_updated_by: Optional[str] = None,
    ) -> "ConfigManager":
        """
        Register a source field for DQ assessment.

        Inserts a row into ``masterField``.  Idempotent — safe to re-run.

        Parameters
        ----------
        field_id
            Unique integer _ID for this field.
        full_field_name
            ``SourceSchema.SourceTable.ColumnName``
        data_category_type_id
            FK to ``masterDataCategory._ID`` (1–27).
            e.g. 4=Email, 10=PostalCode, 12=PhoneNumber, 2=Short Text.
        is_active
            Whether this field is active. Defaults to True.
        created_by
            CreatedBy value. Defaults to current_user().
            Pass ``'sys'`` to mark as system-loaded (e.g. bulk import).
        last_updated_by
            LastUpdatedBy value. Defaults to None.
            Pass current_user() explicitly if tracking who last edited.

        Returns self for method chaining.
        """
        if field_id is None:
            field_id = self._next_id("masterField")
        fqn = self._fqn("masterField")
        self.spark.sql(f"""
            MERGE INTO {fqn} AS t
            USING (SELECT {field_id} AS _ID) AS s ON t._ID = s._ID
            WHEN MATCHED AND (
                t.FullFieldName      <> '{full_field_name}' OR
                t.DataCategoryTypeID <> {data_category_type_id} OR
                t.IsActive           <> {str(is_active).lower()}
            ) THEN UPDATE SET
                t.FullFieldName      = '{full_field_name}',
                t.DataCategoryTypeID = {data_category_type_id},
                t.IsActive           = {str(is_active).lower()},
                t.LastUpdatedBy      = {self._col_str(last_updated_by)},
                t.LastUpdatedOn      = current_timestamp()
            WHEN NOT MATCHED THEN INSERT (
                _ID, FullFieldName, DataCategoryTypeID, IsActive,
                CreatedBy, CreatedOn, LastUpdatedBy, LastUpdatedOn
            ) VALUES (
                {field_id}, '{full_field_name}', {data_category_type_id},
                {str(is_active).lower()},
                {self._col_str(created_by)}, current_timestamp(),
                {self._col_str(last_updated_by, 'null')}, null
            )
        """)
        logger.info("register_field: %s (ID=%d)", full_field_name, field_id)
        return self

    # ------------------------------------------------------------------
    # configFieldValues
    # ------------------------------------------------------------------

    def set_field_values(
        self,
        full_field_name: str,
        config_id: Optional[int] = None,
        field_id: Optional[int] = None,
        min_data_length: Optional[int] = None,
        max_data_length: Optional[int] = None,
        min_data_value: Optional[str] = None,
        max_data_value: Optional[str] = None,
        is_active: bool = True,
        created_by: Optional[str] = None,
        last_updated_by: Optional[str] = None,
    ) -> "ConfigManager":
        """
        Set L01 (data length) and L04 (data value range) constraints for a field.

        Inserts or updates a row in ``configFieldValues``. Idempotent — safe to re-run.
        At least one of length or value range should be provided.

        Parameters
        ----------
        config_id
            Unique integer _ID for this row.
        full_field_name
            Must match a row in ``masterField``.
        field_id
            Optional FK to ``masterField._ID``.
        min_data_length / max_data_length
            Character count range. L01 check: ``LEN(value) BETWEEN min AND max``.
        min_data_value / max_data_value
            Lexicographic value range. L04 check: ``value BETWEEN 'min' AND 'max'``.
        is_active
            Whether this rule is active. Defaults to True.
        created_by
            CreatedBy value. Defaults to current_user().
        last_updated_by
            LastUpdatedBy value. Defaults to current_user() on update.
        """
        if config_id is None:
            config_id = self._next_id("configFieldValues")
        fqn = self._fqn("configFieldValues")
        fid_sql = str(field_id) if field_id is not None else "null"
        min_len = str(min_data_length) if min_data_length is not None else "null"
        max_len = str(max_data_length) if max_data_length is not None else "null"
        min_val = f"'{min_data_value}'" if min_data_value is not None else "null"
        max_val = f"'{max_data_value}'" if max_data_value is not None else "null"

        self.spark.sql(f"""
            MERGE INTO {fqn} AS t
            USING (SELECT {config_id} AS _ID) AS s ON t._ID = s._ID
            WHEN MATCHED AND (
                t.MinDataLength <> {min_len} OR t.MaxDataLength <> {max_len} OR
                t.MinDataValue  <> {min_val} OR t.MaxDataValue  <> {max_val} OR
                t.IsActive      <> {str(is_active).lower()}
            ) THEN UPDATE SET
                t.MinDataLength = {min_len},
                t.MaxDataLength = {max_len},
                t.MinDataValue  = {min_val},
                t.MaxDataValue  = {max_val},
                t.IsActive      = {str(is_active).lower()},
                t.LastUpdatedBy = {self._col_str(last_updated_by)},
                t.LastUpdatedOn = current_timestamp()
            WHEN NOT MATCHED THEN INSERT (
                _ID, FieldID, FullFieldName, MinDataLength, MaxDataLength,
                MinDataValue, MaxDataValue, IsActive,
                CreatedBy, CreatedOn, LastUpdatedBy, LastUpdatedOn
            ) VALUES (
                {config_id}, {fid_sql}, '{full_field_name}', {min_len}, {max_len},
                {min_val}, {max_val}, {str(is_active).lower()},
                {self._col_str(created_by)}, current_timestamp(),
                {self._col_str(last_updated_by, 'null')}, null
            )
        """)
        logger.info("set_field_values: %s field_id=%s length=[%s,%s] value=[%s,%s]",
                    full_field_name, fid_sql, min_len, max_len, min_val, max_val)
        return self

    # ------------------------------------------------------------------
    # configFieldAllowedPattern
    # ------------------------------------------------------------------

    def add_pattern_rule(
        self,
        full_field_name: str,
        is_pattern_allowed: bool,
        rule_id: Optional[int] = None,
        pattern_name: Optional[str] = None,
        pattern_category: Optional[str] = None,
        pattern_subcategory: Optional[str] = None,
        is_active: bool = True,
        created_by: Optional[str] = None,
        last_updated_by: Optional[str] = None,
    ) -> "ConfigManager":
        """
        Add a pattern allow/block rule for a field.

        Inserts or updates a row in ``configFieldAllowedPattern``. Idempotent.

        Exactly ONE of ``pattern_name``, ``pattern_category``, or
        ``pattern_subcategory`` should be populated:

        - ``pattern_name`` only        → pattern-level rule (most specific)
        - ``pattern_subcategory`` only → sub-category rule
        - ``pattern_category`` only    → category-level rule (broadest)

        Precedence: PatternName > PatternSubCategory > PatternCategory.
        A specific Allowed overrides a broad Not Allowed (and vice versa).

        Parameters
        ----------
        is_pattern_allowed
            True  = pattern is permitted (FAIL if absent when required).
            False = pattern is forbidden (FAIL if detected).
        created_by
            CreatedBy value. Defaults to current_user().
        last_updated_by
            LastUpdatedBy value. Defaults to current_user() on update.
        """
        if rule_id is None:
            rule_id = self._next_id("configFieldAllowedPattern")
        fqn = self._fqn("configFieldAllowedPattern")
        pn  = f"'{pattern_name}'"        if pattern_name        else "null"
        pc  = f"'{pattern_category}'"    if pattern_category    else "null"
        psc = f"'{pattern_subcategory}'" if pattern_subcategory else "null"

        self.spark.sql(f"""
            MERGE INTO {fqn} AS t
            USING (SELECT {rule_id} AS _ID) AS s ON t._ID = s._ID
            WHEN MATCHED AND (
                t.FullFieldName                    <> '{full_field_name}' OR
                COALESCE(t.PatternCategory,    '') <> COALESCE({pc},  '') OR
                COALESCE(t.PatternSubCategory, '') <> COALESCE({psc}, '') OR
                COALESCE(t.PatternName,        '') <> COALESCE({pn},  '') OR
                t.IsPatternAllowed                 <> {str(is_pattern_allowed).lower()} OR
                t.IsActive                         <> {str(is_active).lower()}
            ) THEN UPDATE SET
                t.FullFieldName      = '{full_field_name}',
                t.PatternCategory    = {pc},
                t.PatternSubCategory = {psc},
                t.PatternName        = {pn},
                t.IsPatternAllowed   = {str(is_pattern_allowed).lower()},
                t.IsActive           = {str(is_active).lower()},
                t.LastUpdatedBy      = {self._col_str(last_updated_by)},
                t.LastUpdatedOn      = current_timestamp()
            WHEN NOT MATCHED THEN INSERT (
                _ID, FullFieldName, PatternCategory, PatternSubCategory, PatternName,
                IsPatternAllowed, IsActive,
                CreatedBy, CreatedOn, LastUpdatedBy, LastUpdatedOn
            ) VALUES (
                {rule_id}, '{full_field_name}', {pc}, {psc}, {pn},
                {str(is_pattern_allowed).lower()}, {str(is_active).lower()},
                {self._col_str(created_by)}, current_timestamp(),
                {self._col_str(last_updated_by, 'null')}, null
            )
        """)
        logger.info("add_pattern_rule: %s  name=%s cat=%s allowed=%s",
                    full_field_name, pattern_name, pattern_category, is_pattern_allowed)
        return self

    def block_category(self, rule_id: int, full_field_name: str,
                       pattern_category: str) -> "ConfigManager":
        """
        Convenience: block ALL patterns in a category.

        Equivalent to: ``add_pattern_rule(..., is_pattern_allowed=False,
                                          pattern_category=pattern_category)``
        """
        return self.add_pattern_rule(
            rule_id, full_field_name, False, pattern_category=pattern_category
        )

    def allow_pattern(self, rule_id: int, full_field_name: str,
                      pattern_name: str) -> "ConfigManager":
        """
        Convenience: allow a specific pattern (override a broader block rule).

        Equivalent to: ``add_pattern_rule(..., is_pattern_allowed=True,
                                          pattern_name=pattern_name)``
        """
        return self.add_pattern_rule(
            rule_id, full_field_name, True, pattern_name=pattern_name
        )

    def block_pattern(self, rule_id: int, full_field_name: str,
                      pattern_name: str) -> "ConfigManager":
        """
        Convenience: block a specific pattern.

        Equivalent to: ``add_pattern_rule(..., is_pattern_allowed=False,
                                          pattern_name=pattern_name)``
        """
        return self.add_pattern_rule(
            rule_id, full_field_name, False, pattern_name=pattern_name
        )

    # ------------------------------------------------------------------
    # configCustomQuery
    # ------------------------------------------------------------------

    def add_custom_query(
        self,
        full_field_name: str,
        expression: str,
        is_condition_allowed: bool,
        query_id: Optional[int] = None,
        custom_query_type: str = None,
        description: str = None,
        is_active: bool = True,
        created_by: Optional[str] = None,
        last_updated_by: Optional[str] = None,
    ) -> "ConfigManager":
        """
        Add a custom L02 validation rule for a field.

        Inserts a row into ``configCustomQuery``.

        The ``expression`` is stored in the ``CustomQuery`` column and its
        interpretation is controlled by the ``custom_query_type`` parameter:

        **custom_query_type='REGEX' (recommended for non-technical users)**
            Write a plain regular expression applied via ``re.search(pattern, value)``.

            Examples::

                cfg.add_custom_query(1, EMAIL, r'^[^@]+@[^@]+\\.[^@]+$',
                                     is_condition_allowed=True,
                                     custom_query_type='REGEX',
                                     description='Basic email format')

                cfg.add_custom_query(2, MOBILE, r'^04[0-9]{8}$',
                                     is_condition_allowed=True,
                                     custom_query_type='REGEX',
                                     description='AU mobile: starts 04, 10 digits')

        **custom_query_type='SQL'**
            A Spark SQL expression with ``@InputValue`` as the column placeholder.
            Applied at the DataFrame level via ``F.expr()`` at assessment time —
            not evaluated inside a Python UDF. Use ``add_custom_query_sql()`` as
            a convenience wrapper.

            Example::

                cfg.add_custom_query(3, POSTCODE, "length(@InputValue) between 4 and 10",
                                     is_condition_allowed=True,
                                     custom_query_type='SQL')

        **custom_query_type='PYTHON'**
            A named validator registered via ``DQFramework.register_validator()``.
            Use for complex logic that cannot be expressed as a regex.

            Example::

                dq.register_validator('email_at_validation', my_fn)
                cfg.add_custom_query(4, EMAIL, 'email_at_validation',
                                     is_condition_allowed=True,
                                     custom_query_type='PYTHON')

        **custom_query_type=None (auto-detect)**
            The framework infers the type at evaluation time:
            named validator → regex metacharacter detection → Python eval fallback.

        **IsConditionAllowed semantics (same as SQL version)**::

            is_condition_allowed=True  AND matches     → PASS (value is acceptable)
            is_condition_allowed=True  AND NOT matches → FAIL (required pattern missing)
            is_condition_allowed=False AND matches     → FAIL (forbidden pattern found)
            is_condition_allowed=False AND NOT matches → PASS (forbidden pattern absent)

        Parameters
        ----------
        expression
            Regex pattern, Spark SQL expression, or registered validator name.
        is_condition_allowed
            True  = value must match the expression.
            False = value must NOT match the expression.
        custom_query_type
            ``'SQL'``, ``'REGEX'``, ``'PYTHON'``, or ``None`` (auto-detect).
        """
        if query_id is None:
            query_id = self._next_id("configCustomQuery")
        fqn = self._fqn("configCustomQuery")
        # Databricks runs in legacy Spark SQL escape mode: backslash is the escape
        # character inside SQL string literals (not ANSI ''). Escaping order matters:
        # 1. double all backslashes first  (\  →  \\)
        # 2. then escape single quotes     ('  →  \')
        # Doing step 2 before step 1 would double-escape the newly introduced backslashes.
        def _sql_str(s: str) -> str:
            return s.replace("\\", "\\\\").replace("'", "\\'")

        desc = f"'{_sql_str(description)}'" if description else "null"
        expr_sql = _sql_str(expression)
        cqtype_sql = f"'{custom_query_type}'" if custom_query_type else "null"

        self.spark.sql(f"""
            MERGE INTO {fqn} AS t
            USING (SELECT {query_id} AS _ID) AS s ON t._ID = s._ID
            WHEN MATCHED AND (
                t.FullFieldName      <> '{full_field_name}' OR
                t.CustomQuery        <> '{expr_sql}' OR
                t.CustomQueryType    IS DISTINCT FROM {cqtype_sql} OR
                t.CustomQueryDescription IS DISTINCT FROM {desc} OR
                t.IsConditionAllowed <> {str(is_condition_allowed).lower()} OR
                t.IsActive           <> {str(is_active).lower()}
            ) THEN UPDATE SET
                t.FullFieldName      = '{full_field_name}',
                t.CustomQuery        = '{expr_sql}',
                t.CustomQueryType    = {cqtype_sql},
                t.CustomQueryDescription = {desc},
                t.IsConditionAllowed = {str(is_condition_allowed).lower()},
                t.IsActive           = {str(is_active).lower()},
                t.LastUpdatedBy      = {self._col_str(last_updated_by)},
                t.LastUpdatedOn      = current_timestamp()
            WHEN NOT MATCHED THEN INSERT (
                _ID, FullFieldName, CustomQuery, CustomQueryType, CustomQueryDescription,
                IsConditionAllowed, IsActive,
                CreatedBy, CreatedOn, LastUpdatedBy, LastUpdatedOn
            ) VALUES (
                {query_id}, '{full_field_name}', '{expr_sql}', {cqtype_sql}, {desc},
                {str(is_condition_allowed).lower()}, {str(is_active).lower()},
                {self._col_str(created_by)}, current_timestamp(),
                {self._col_str(last_updated_by, 'null')}, null
            )
        """)
        logger.info("add_custom_query: %s → expression type=%s (allowed=%s)",
                    full_field_name, custom_query_type, is_condition_allowed)
        return self

    def add_custom_query_regex(
        self,
        query_id: int,
        full_field_name: str,
        regex_pattern: str,
        must_match: bool = True,
        description: str = None,
    ) -> "ConfigManager":
        """
        Shorthand: add a regex-based L02 rule. No Python knowledge required.

        Equivalent to ``add_custom_query(..., expression=regex_pattern, ...)``.

        Parameters
        ----------
        regex_pattern
            A regular expression string. The value is tested with
            ``re.search(regex_pattern, value)``.
        must_match
            True  = value MUST match the regex (FAIL if it does not).
            False = value must NOT match the regex (FAIL if it does).

        Examples
        --------
        # Australian mobile number: starts with 04, exactly 10 digits
        cfg.add_custom_query_regex(1, 'Source.SRC_Party.MOBILE',
                                   r'^04[0-9]{8}$', must_match=True,
                                   description='AU mobile format')

        # Basic email structure (quick check)
        cfg.add_custom_query_regex(2, 'Source.SRC_ContactPoint.EMAIL',
                                   r'^[^@\\s]+@[^@\\s]+\\.[^@\\s]+$', must_match=True,
                                   description='email must contain @ and domain')

        # Must NOT contain placeholder text
        cfg.add_custom_query_regex(3, 'Source.SRC_ContactPoint.EMAIL',
                                   r'noemaildress', must_match=False,
                                   description='reject placeholder email addresses')
        """
        return self.add_custom_query(
            query_id, full_field_name, regex_pattern,
            is_condition_allowed=must_match,
            custom_query_type="REGEX",
            description=description,
        )

    def add_custom_query_sql(
        self,
        query_id: int,
        full_field_name: str,
        spark_sql_expr: str,
        is_condition_allowed: bool = True,
        description: str = None,
    ) -> "ConfigManager":
        """
        Add a Spark SQL expression as an L02 rule. Use @InputValue as the placeholder.

        The expression is applied at the DataFrame level via F.expr(), not inside
        a Python UDF. @InputValue is replaced with the actual column reference at
        assessment time.

        Examples::

            # Email must contain @
            cfg.add_custom_query_sql(1, 'TCA.HZ_CONTACT_POINTS.EMAIL_ADDRESS',
                                     "instr(@InputValue, '@') > 0", must_match=True)
            # Length check (alternative to set_field_values)
            cfg.add_custom_query_sql(2, 'TCA.HZ_LOCATIONS.POSTAL_CODE',
                                     "length(@InputValue) between 4 and 10")
        """
        return self.add_custom_query(
            query_id, full_field_name, spark_sql_expr,
            is_condition_allowed=is_condition_allowed,
            custom_query_type="SQL",
            description=description,
        )

    # ------------------------------------------------------------------
    # mapDQChecks
    # ------------------------------------------------------------------

    def add_mapping(
        self,
        full_field_name: str,
        target_schema_name: str,
        target_table_name: str,
        target_field_name: str,
        mapping_id: Optional[int] = None,
        target_catalog_name: Optional[str] = None,
        dq_function_schema_name: str = "dq",
        dq_function_name: Optional[str] = None,
        is_active: bool = True,
        created_by: Optional[str] = None,
        last_updated_by: Optional[str] = None,
    ) -> "ConfigManager":
        """
        Map a source field definition to a curated Delta table column.

        Inserts or updates a row in ``mapDQChecks``. Idempotent — safe to re-run.

        The ``dq_function_name`` is auto-derived from ``full_field_name``
        if not supplied:
            ``Source.SRC_Party.FIRST_NAME``
            → ``fn_DQ_Source_SRC_Party_FIRST_NAME``

        Parameters
        ----------
        full_field_name
            Must match a row in ``masterField``.
        target_catalog_name
            Unity Catalog for the curated table. None uses the framework's default.
        dq_function_schema_name
            Mirrors the SQL Server DQFunctionSchemaName column (default: 'dq').
        is_active
            Whether this mapping is active. Defaults to True.
        created_by
            CreatedBy value. Defaults to current_user().
        last_updated_by
            LastUpdatedBy value. Defaults to current_user() on update.
        """
        if mapping_id is None:
            mapping_id = self._next_id("mapDQChecks")
        fn_name = dq_function_name or self.derive_function_name(full_field_name)
        fqn = self._fqn("mapDQChecks")
        cat = f"'{target_catalog_name}'" if target_catalog_name else "null"

        self.spark.sql(f"""
            MERGE INTO {fqn} AS t
            USING (SELECT {mapping_id} AS _ID) AS s ON t._ID = s._ID
            WHEN MATCHED AND (
                t.TargetSchemaName <> '{target_schema_name}' OR
                t.TargetTableName  <> '{target_table_name}'  OR
                t.TargetFieldName  <> '{target_field_name}'  OR
                t.IsActive         <> {str(is_active).lower()}
            ) THEN UPDATE SET
                t.TargetCatalogName    = {cat},
                t.TargetSchemaName     = '{target_schema_name}',
                t.TargetTableName      = '{target_table_name}',
                t.TargetFieldName      = '{target_field_name}',
                t.DQFunctionSchemaName = '{dq_function_schema_name}',
                t.DQFunctionName       = '{fn_name}',
                t.IsActive             = {str(is_active).lower()},
                t.LastUpdatedBy        = {self._col_str(last_updated_by)},
                t.LastUpdatedOn        = current_timestamp()
            WHEN NOT MATCHED THEN INSERT (
                _ID, FullFieldName, TargetCatalogName, TargetSchemaName,
                TargetTableName, TargetFieldName,
                DQFunctionSchemaName, DQFunctionName,
                IsActive, CreatedBy, CreatedOn, LastUpdatedBy, LastUpdatedOn
            ) VALUES (
                {mapping_id}, '{full_field_name}', {cat},
                '{target_schema_name}', '{target_table_name}', '{target_field_name}',
                '{dq_function_schema_name}', '{fn_name}',
                {str(is_active).lower()},
                {self._col_str(created_by)}, current_timestamp(),
                {self._col_str(last_updated_by, 'null')}, null
            )
        """)
        logger.info("add_mapping: %s → %s.%s.%s  fn=%s",
                    full_field_name, target_schema_name, target_table_name,
                    target_field_name, fn_name)
        return self

    # ------------------------------------------------------------------
    # Bulk helpers
    # ------------------------------------------------------------------

    def verify_config(self) -> dict:
        """
        Verify that the user configuration is consistent before running
        ``generate_rule_functions()``.

        Checks:
        - Duplicate _ID values within each user-managed config table (causes MERGE errors)
        - Duplicate logical rules in configFieldAllowedPattern (same FullFieldName+PatternCategory+PatternSubCategory+PatternName)
        - Every FullFieldName in mapDQChecks exists in masterField
        - Every FullFieldName in configFieldAllowedPattern exists in masterField
        - Every PatternName in configFieldAllowedPattern exists in masterPattern
        - Every FullFieldName in configCustomQuery exists in masterField
        - At least one active mapping exists

        Returns a dict with keys ``ok`` (bool) and ``issues`` (list of strings).
        """
        from pyspark.sql import functions as F

        issues = []

        # ------------------------------------------------------------------
        # 1. Duplicate _ID checks — any duplicate _ID in a user-managed table
        #    will cause DELTA_MULTIPLE_SOURCE_ROW_MATCHING_TARGET_ROW_IN_MERGE
        # ------------------------------------------------------------------
        _dup_id_tables = [
            "masterField",
            "configFieldValues",
            "configFieldAllowedPattern",
            "configCustomQuery",
            "mapDQChecks",
        ]
        for tbl in _dup_id_tables:
            dup_rows = (
                self.spark.table(self._fqn(tbl))
                    .groupBy("_ID")
                    .agg(F.count("*").alias("cnt"))
                    .filter("cnt > 1")
                    .collect()
            )
            for row in dup_rows:
                issues.append(
                    f"{tbl}._ID={row['_ID']}: appears {row['cnt']} times — "
                    f"causes MERGE errors; delete the extra row(s) and re-seed"
                )

        # ------------------------------------------------------------------
        # 2. Duplicate logical rule check for configFieldAllowedPattern
        #    Same (FullFieldName, PatternCategory, PatternSubCategory, PatternName)
        #    means contradictory IsPatternAllowed values are possible
        # ------------------------------------------------------------------
        dup_rules = (
            self.spark.table(self._fqn("configFieldAllowedPattern"))
                .groupBy("FullFieldName", "PatternCategory", "PatternSubCategory", "PatternName")
                .agg(F.count("*").alias("cnt"))
                .filter("cnt > 1")
                .collect()
        )
        for row in dup_rules:
            issues.append(
                f"configFieldAllowedPattern: duplicate rule for "
                f"FullFieldName='{row['FullFieldName']}', "
                f"PatternCategory={row['PatternCategory']!r}, "
                f"PatternSubCategory={row['PatternSubCategory']!r}, "
                f"PatternName={row['PatternName']!r} "
                f"({row['cnt']} rows) — contradictory IsPatternAllowed possible; "
                f"remove the duplicate row"
            )

        # ------------------------------------------------------------------
        # 3. Referential integrity checks
        # ------------------------------------------------------------------
        mf = {r["FullFieldName"] for r in
              self.spark.table(self._fqn("masterField"))
                  .filter("IsActive = true").select("FullFieldName").collect()}
        mp = {r["PatternName"] for r in
              self.spark.table(self._fqn("masterPattern"))
                  .filter("IsActive = true").select("PatternName").collect()}

        for row in (self.spark.table(self._fqn("mapDQChecks"))
                        .filter("IsActive = true").collect()):
            if row["FullFieldName"] not in mf:
                issues.append(f"mapDQChecks._ID={row['_ID']}: "
                               f"FullFieldName '{row['FullFieldName']}' not in masterField")

        for row in (self.spark.table(self._fqn("configFieldAllowedPattern"))
                        .filter("IsActive = true").collect()):
            if row["FullFieldName"] not in mf:
                issues.append(f"configFieldAllowedPattern._ID={row['_ID']}: "
                               f"FullFieldName '{row['FullFieldName']}' not in masterField")
            if row["PatternName"] and row["PatternName"] not in mp:
                issues.append(f"configFieldAllowedPattern._ID={row['_ID']}: "
                               f"PatternName '{row['PatternName']}' not in masterPattern")

        for row in (self.spark.table(self._fqn("configCustomQuery"))
                        .filter("IsActive = true AND CustomQuery IS NOT NULL").collect()):
            if row["FullFieldName"] not in mf:
                issues.append(f"configCustomQuery._ID={row['_ID']}: "
                               f"FullFieldName '{row['FullFieldName']}' not in masterField")

        mappings = self.spark.table(self._fqn("mapDQChecks")).filter("IsActive = true").count()
        if mappings == 0:
            issues.append("mapDQChecks has no active rows — nothing to assess")

        return {"ok": len(issues) == 0, "issues": issues}

    def field_rule_summary(self, field_name: str = None):
        """
        Return a flat DataFrame of all active rules configured for a field (or all fields).

        One row per rule — suitable for .display(), export to Excel, pivot, and filter.

        Covers all four rule types:
          - Data Length  (L01)  — from configFieldValues
          - Value Range  (L04)  — from configFieldValues
          - Pattern Rule (L03)  — from configFieldAllowedPattern + masterPattern, fully resolved
          - Custom Rule  (L02)  — from configCustomQuery

        Parameters
        ----------
        field_name
            Optional. Either:
              - Logical FullFieldName as stored in masterField (e.g. 'email_address')
              - Physical field in Schema.Table.Column format (e.g. 'silver.mock_curated_contacts.email')
                → resolved to logical FullFieldName via mapDQChecks
            Omit to return rules for all configured fields.

        Returns
        -------
        pyspark.sql.DataFrame with columns:
            FullFieldName, DataCategory, TargetSchemaName, TargetTableName, TargetFieldName,
            RuleType, PatternCategory, PatternPriority, PatternName, PatternDescription,
            PatternValue, Status

        Example
        -------
        dq.config.field_rule_summary("email_address").display()
        dq.config.field_rule_summary("silver.mock_curated_contacts.email").display()
        dq.config.field_rule_summary().display()   # all fields
        """
        from pyspark.sql import Row
        from dq_framework.engine.resolve_pattern_rules import resolve_patterns

        # ── Load all reference tables ────────────────────────────────────────
        mf_rows = (self.spark.table(self._fqn("masterField"))
                   .filter("IsActive = true").collect())
        mdc_rows = (self.spark.table(self._fqn("masterDataCategory"))
                    .filter("IsActive = true").collect())
        mp_rows = (self.spark.table(self._fqn("masterPattern"))
                   .filter("IsActive = true").collect())
        cfv_rows = (self.spark.table(self._fqn("configFieldValues"))
                    .filter("IsActive = true").collect())
        cq_rows = (self.spark.table(self._fqn("configCustomQuery"))
                   .filter("IsActive = true").collect())
        cfap_rows = (self.spark.table(self._fqn("configFieldAllowedPattern"))
                     .filter("IsActive = true").collect())
        map_rows = (self.spark.table(self._fqn("mapDQChecks"))
                    .filter("IsActive = true").collect())

        # ── Build lookup maps ────────────────────────────────────────────────
        # DataCategory label per masterField._ID
        dc_by_id = {r["_ID"]: r["DataCategoryShortDescription"] for r in mdc_rows}
        mf_by_ffn = {r["FullFieldName"]: r for r in mf_rows}

        # All mappings: FullFieldName → list of (TargetSchema, TargetTable, TargetField)
        mappings_by_ffn: dict = {}
        for r in map_rows:
            ffn = r["FullFieldName"]
            mappings_by_ffn.setdefault(ffn, []).append({
                "TargetSchemaName": r["TargetSchemaName"],
                "TargetTableName":  r["TargetTableName"],
                "TargetFieldName":  r["TargetFieldName"],
            })

        # ── Resolve the logical FullFieldName(s) from the input ──────────────
        if field_name is None:
            # All logical fields that have at least one active mapping
            target_ffns = sorted(mappings_by_ffn.keys())
        else:
            # Check if it matches a logical FullFieldName directly
            if field_name in mf_by_ffn:
                target_ffns = [field_name]
            else:
                # Try physical: Schema.Table.Column → look up via mapDQChecks
                parts = field_name.split(".", 2)
                matched = [
                    r["FullFieldName"] for r in map_rows
                    if (len(parts) == 3
                        and r["TargetSchemaName"] == parts[0]
                        and r["TargetTableName"] == parts[1]
                        and r["TargetFieldName"] == parts[2])
                    or (len(parts) == 2
                        and r["TargetTableName"] == parts[0]
                        and r["TargetFieldName"] == parts[1])
                ]
                target_ffns = sorted(set(matched))
                if not target_ffns:
                    raise ValueError(
                        f"'{field_name}' not found as a FullFieldName in masterField "
                        f"or as a physical column in mapDQChecks."
                    )

        # ── Resolve L03 patterns for all target fields ───────────────────────
        cfap_filtered = [r.asDict() for r in cfap_rows if r["FullFieldName"] in target_ffns]
        mp_dicts = [r.asDict() for r in mp_rows]
        resolved_patterns = resolve_patterns(cfap_filtered, mp_dicts)
        # Group by FullFieldName for easy lookup
        resolved_by_ffn: dict = {}
        for rp in resolved_patterns:
            resolved_by_ffn.setdefault(rp.full_field_name, []).append(rp)

        # ── Build flat rows ───────────────────────────────────────────────────
        output_rows = []

        for ffn in target_ffns:
            mf = mf_by_ffn.get(ffn)
            data_category = dc_by_id.get(mf["DataCategoryTypeID"], "") if mf else ""
            physical_cols = mappings_by_ffn.get(ffn, [{"TargetSchemaName": "", "TargetTableName": "", "TargetFieldName": ""}])

            def _add_rows(rule_type, pat_cat, priority, name, description, value, status):
                for phys in physical_cols:
                    output_rows.append(Row(
                        FullFieldName=ffn,
                        DataCategory=data_category,
                        TargetSchemaName=phys["TargetSchemaName"],
                        TargetTableName=phys["TargetTableName"],
                        TargetFieldName=phys["TargetFieldName"],
                        RuleType=rule_type,
                        PatternCategory=pat_cat,
                        PatternPriority=priority,
                        PatternName=name,
                        PatternDescription=description,
                        PatternValue=value,
                        Status=status,
                    ))

            # L01 — Data Length
            cfv = next((r for r in cfv_rows if r["FullFieldName"] == ffn), None)
            if cfv:
                _add_rows(
                    rule_type="Data Length",
                    pat_cat="DataLength",
                    priority=0,
                    name="Data Length Check",
                    description=f"Value must be between {cfv['MinDataLength']} and {cfv['MaxDataLength']} characters",
                    value=f"Min: {cfv['MinDataLength']}  |  Max: {cfv['MaxDataLength']}",
                    status="Enforced",
                )

            # L04 — Value Range
            if cfv and (cfv["MinDataValue"] is not None or cfv["MaxDataValue"] is not None):
                min_v = cfv["MinDataValue"] if cfv["MinDataValue"] is not None else "no lower bound"
                max_v = cfv["MaxDataValue"] if cfv["MaxDataValue"] is not None else "no upper bound"
                _add_rows(
                    rule_type="Value Range",
                    pat_cat="ValueRange",
                    priority=1,
                    name="Value Range Check",
                    description=f"Value must be within range: {min_v} to {max_v}",
                    value=f"Min: {min_v}  |  Max: {max_v}",
                    status="Enforced",
                )

            # L03 — Pattern Rules (fully resolved, one row per pattern)
            for rp in sorted(
                resolved_by_ffn.get(ffn, []),
                key=lambda x: (x.pattern_priority, x.pattern_name),
            ):
                _add_rows(
                    rule_type="Pattern Rule",
                    pat_cat=rp.pattern_category,
                    priority=rp.pattern_priority,
                    name=rp.pattern_name,
                    description=rp.pattern_description or "",
                    value=rp.pattern_value or "",
                    status="Allowed" if rp.is_pattern_allowed else "Not Allowed",
                )

            # L02 — Custom Rules
            for idx, cq in enumerate(
                [r for r in cq_rows if r["FullFieldName"] == ffn], start=1
            ):
                match_label = "Must Match" if cq["IsConditionAllowed"] else "Must NOT Match"
                qtype = cq["CustomQueryType"] or "SQL"
                _add_rows(
                    rule_type="Custom Rule",
                    pat_cat=f"CustomQuery ({qtype})",
                    priority=9999 + idx,
                    name=cq["CustomQueryDescription"],
                    description=f"{match_label}: {cq['CustomQuery']}",
                    value=cq["CustomQuery"],
                    status=match_label,
                )

        if not output_rows:
            # Return empty DataFrame with correct schema
            from pyspark.sql.types import StructType, StructField, StringType, IntegerType
            schema = StructType([
                StructField("FullFieldName",    StringType()),
                StructField("DataCategory",     StringType()),
                StructField("TargetSchemaName", StringType()),
                StructField("TargetTableName",  StringType()),
                StructField("TargetFieldName",  StringType()),
                StructField("RuleType",         StringType()),
                StructField("PatternCategory",  StringType()),
                StructField("PatternPriority",  IntegerType()),
                StructField("PatternName",      StringType()),
                StructField("PatternDescription", StringType()),
                StructField("PatternValue",     StringType()),
                StructField("Status",           StringType()),
            ])
            return self.spark.createDataFrame([], schema)

        return self.spark.createDataFrame(output_rows)

    def show_config_summary(self) -> None:
        """Print a summary of the current user configuration."""
        def count(table, condition="IsActive = true"):
            return self.spark.table(self._fqn(table)).filter(condition).count()

        import dq_framework as _dqf
        print(f"=== DQ Framework Configuration Summary  (v{_dqf.__version__}) ===")
        print(f"  masterDataCategory        : {count('masterDataCategory')} rows")
        print(f"  masterPattern             : {count('masterPattern')} rows")
        print(f"    └─ InvalidKeyword user  : "
              f"{self.spark.table(self._fqn('masterPattern')).filter('PatternCategory = \"InvalidKeyword\" AND _ID >= 1000 AND IsActive = true').count()} custom")
        print(f"  masterField               : {count('masterField')} rows  [USER]")
        print(f"  configFieldValues         : {count('configFieldValues')} rows  [USER]")
        print(f"  configFieldAllowedPattern : {count('configFieldAllowedPattern')} rows  [USER]")
        print(f"  configCustomQuery         : {count('configCustomQuery')} rows  [USER]")
        print(f"  mapDQChecks               : {count('mapDQChecks')} rows  [USER]")
        print(f"  auditDQChecks             : {count('auditDQChecks', '1=1')} rows  [RESULTS]")
        print(f"  statDQChecks              : {count('statDQChecks', '1=1')} rows  [RESULTS]")

        result = self.verify_config()
        if result["ok"]:
            print("\n  Config verification: OK — ready to run generate_rule_functions()")
        else:
            n = len(result["issues"])
            print(f"\n  {'!' * 60}")
            print(f"  CONFIG VERIFICATION FAILED — {n} issue(s) found")
            print(f"  generate_rule_functions() will raise RuntimeError until resolved")
            print(f"  {'!' * 60}")
            for issue in result["issues"]:
                print(f"    ✗ {issue}")
