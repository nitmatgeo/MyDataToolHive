"""
Rule Function Generator
=========================
Python equivalent of ``[dq].[p_DQ_GenerateRuleFunctions]``.

Instead of generating T-SQL Table-Valued Functions (TVFs) and creating them
as database objects, this module constructs Python callables — one per source
field — and stores them in an in-memory ``FunctionRegistry`` dict.

These callables are then used by the runner (equivalent to CROSS APPLY in SQL)
to evaluate each row of a curated table.

Generated function signature
-----------------------------
Each field checker has the signature:

    fn_DQ_<Schema>_<Table>_<Field>(input_value: str | None)
        -> tuple(result: bool, violation_type: str | None, log_message: str | None)

    result        True  = all checks passed (DQ eligible)
                  False = at least one check failed (DQ ineligible)
    violation_type  The category of the first failing rule.
    log_message     Diagnostic detail of the first failing rule.

Validation level hierarchy (mirrors SQL TVF levels)
-----------------------------------------------------
L00  Function header + variable declarations
L01  Data Length range check (min / max character count)
L02  Custom query validators (Python callables from registry)
L03  118-pattern checks (all from masterPattern, in PatternPriority order)
L04  Data Value range check (min / max value comparison — lexicographic)
L99  Default PASS — if all prior checks pass, value is DQ-eligible

Early-exit semantics
---------------------
The generated Python function returns immediately on the first failing check
(equivalent to ``INSERT INTO @ResultTable ... RETURN;`` in the SQL TVF).
This prevents masking multiple violations in a single pass — re-run after
fixing the first violation to surface the next.
"""

from __future__ import annotations

import logging
from typing import Callable, Optional

from dq_framework.engine.pattern_checks import resolve_pattern_check_fn
from dq_framework.engine.resolve_pattern_rules import ResolvedPattern, get_distinct_fields

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Type alias for a field checker function
# ---------------------------------------------------------------------------
FieldChecker = Callable[[Optional[str]], tuple]


class FunctionRegistry:
    """
    In-memory registry of generated field checker functions.

    Equivalent to the TVFs created in the [dq] schema by
    ``p_DQ_GenerateRuleFunctions``.  Registry is rebuilt on each call to
    ``generate_rule_functions()``.

    Keys follow the SQL TVF naming convention:
        fn_DQ_<SchemaName>_<TableName>_<FieldName>
    """

    def __init__(self):
        self._functions: dict[str, FieldChecker] = {}
        # SQL-type L02 expressions stored separately — applied at DataFrame
        # level via F.expr() in the runner (cannot run inside a PySpark UDF).
        # Maps fn_name → list of configCustomQuery row dicts (CustomQueryType='SQL').
        self._sql_expressions: dict[str, list[dict]] = {}

    def register(self, name: str, fn: FieldChecker) -> None:
        self._functions[name] = fn
        logger.debug("Registered field checker: %s", name)

    def register_sql_expressions(self, name: str, exprs: list[dict]) -> None:
        """
        Store SQL-type L02 expressions for a field checker.

        These are applied at the DataFrame level by the runner using
        ``F.expr()`` after the Python UDF step, because Spark SQL
        expressions cannot execute inside a UDF (no SparkSession on
        executor).
        """
        self._sql_expressions[name] = exprs
        logger.debug("Registered %d SQL expression(s) for: %s", len(exprs), name)

    def get_sql_expressions(self, name: str) -> list[dict]:
        """Return SQL-type L02 expression rows for a field checker (empty list if none)."""
        return self._sql_expressions.get(name, [])

    def get(self, name: str) -> Optional[FieldChecker]:
        return self._functions.get(name)

    def remove(self, name: str) -> None:
        """Remove a stale function (equivalent to DROP FUNCTION in CleanQueryText)."""
        self._functions.pop(name, None)
        self._sql_expressions.pop(name, None)
        logger.debug("Removed stale field checker: %s", name)

    def list_functions(self) -> list[str]:
        return list(self._functions.keys())

    def clear(self) -> None:
        self._functions.clear()
        self._sql_expressions.clear()

    def __contains__(self, name: str) -> bool:
        return name in self._functions

    def __len__(self) -> int:
        return len(self._functions)


# ---------------------------------------------------------------------------
# Custom validator registry
# ---------------------------------------------------------------------------
#: Maps custom validator name → Python callable
#: Populated via DQFramework.register_validator() or register_validator()
custom_validator_registry: dict[str, Callable[[Optional[str]], bool]] = {}


def register_validator(name: str, fn: Callable[[Optional[str]], bool]) -> None:
    """Register a custom validator function by name for use in configCustomQuery."""
    custom_validator_registry[name] = fn
    logger.debug("Registered custom validator: %s", name)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _parse_ffn(ffn: str) -> tuple:
    """Split a FullFieldName ('Schema.Table.Field' or 'FieldName') into a 3-tuple."""
    parts = ffn.split(".", 2)
    return (
        parts[0] if len(parts) > 0 else "",
        parts[1] if len(parts) > 1 else "",
        parts[2] if len(parts) > 2 else "",
    )


def _make_fn_name(schema: str, table: str, field: str) -> str:
    """Build the registry key / function name, mirroring SQL TVF name format.

    Empty segments (when FullFieldName has fewer than 3 dot-separated parts)
    are excluded, so 'email_address' produces 'fn_DQ_email_address' and
    'Schema.Table.Field' produces 'fn_DQ_Schema_Table_Field'.
    """
    return "fn_DQ_" + "_".join(p for p in [schema, table, field] if p)


def _build_l01_check(min_len: Optional[int], max_len: Optional[int]):
    """
    Build an L01 (Data Length) check closure.

    SQL:
        IF ((@TargetValue IS NOT NULL AND LEN(@TargetValue) BETWEEN min AND max)
            OR @TargetValue IS NULL)
        -> PASS (continue)
        ELSE -> FAIL + RETURN
    """
    if min_len is None or max_len is None:
        return None

    def check(current_value, input_value):
        val = current_value
        if val is None:
            return (False, "Data Length",
                    f"Debug:: The value <NULL> has [PASSED]. Rule: Data Length is NULL or Not Applicable or is within the desired range",
                    val)
        n = len(val)
        if min_len <= n <= max_len:
            return (False, "Data Length",
                    f"Debug:: The value <{val}> has [PASSED]. Rule: Data Length is NULL or Not Applicable or is within the desired range",
                    val)
        return (True, "Data Length",
                f"Debug:: The value <{val}> has [FAILED]. Rule: Data Length is NOT within the desired range",
                val)

    return check


def _build_l02_check(custom_query_id: int, expression: str,
                      is_condition_allowed: bool,
                      custom_query_type: Optional[str] = None):
    """
    Build an L02 (Custom Query) check closure.

    Mirrors the 4-branch logic in the generated SQL TVF:

        IF @TargetValue IS NULL                         → PASS (NULL not applicable)
        ELSE IF (is_allowed=0 AND condition matches)    → FAIL + RETURN
        ELSE IF (is_allowed=1 AND condition matches)    → PASS (continue)
        ELSE IF (is_allowed=0 AND NOT condition)        → PASS (continue)
        ELSE IF (is_allowed=1 AND NOT condition)        → FAIL + RETURN
        ELSE                                            → FAIL + RETURN (unknown state)

    Parameters
    ----------
    custom_query_id
        The _ID from configCustomQuery — used in log messages.
    expression
        The query/expression string. Interpretation depends on ``custom_query_type``:

        - ``'REGEX'`` — regex pattern applied via ``re.search(pattern, value)``.
          Example: ``'^04[0-9]{8}$'``
        - ``'PYTHON'`` — named validator key in ``custom_validator_registry``
          (registered via ``DQFramework.register_validator()``).
          Example: ``'email_at_validation'``
        - ``'SQL'`` — Spark SQL expression with ``@InputValue`` placeholder
          (applied at DataFrame level via ``F.expr()`` during assessment,
          not evaluated here in the Python closure path).
        - ``None`` / auto-detect — evaluated in priority order:
          1. Named validator if ``expression`` matches a registered name.
          2. Regex pattern if ``expression`` contains regex metacharacters.
          3. Python ``eval()`` fallback (legacy / power users).

    is_condition_allowed
        True  = condition must match (value is Allowed when it matches).
        False = condition must NOT match (value is NOT Allowed when it matches).
    custom_query_type
        Optional type hint: ``'SQL'``, ``'REGEX'``, ``'PYTHON'``, or ``None``
        for auto-detect (default).
    """
    import re as _re

    cq_id = custom_query_id
    is_allowed = is_condition_allowed

    # Determine evaluation mode based on CustomQueryType
    _cqtype = (custom_query_type or "").upper() if custom_query_type else None

    # Detect whether ``expression`` is a plain regex pattern (auto-detect fallback).
    # Heuristic: contains regex metacharacters and is not a registered name.
    _REGEX_METACHARACTERS = set(r'^$+*?[({|\\')
    _is_regex = any(c in _REGEX_METACHARACTERS for c in expression)

    def _evaluate(value: Optional[str]) -> Optional[bool]:
        """
        Invoke the validator; returns True=matches, False=not matches, None=error.

        When CustomQueryType is set, dispatch directly to the appropriate path.
        When None (auto-detect), priority order: named validator → regex → eval.
        SQL type is handled at the DataFrame level and returns None (not applicable
        in the Python closure path).
        """
        # SQL expressions are applied via F.expr() at the DataFrame level —
        # they are not evaluated inside this Python closure.
        if _cqtype == "SQL":
            logger.debug("CustomQuery ID %s is type SQL — skipped in Python closure path", cq_id)
            return None

        # PYTHON type: named validator (explicit)
        if _cqtype == "PYTHON":
            if expression in custom_validator_registry:
                try:
                    return bool(custom_validator_registry[expression](value))
                except Exception as exc:
                    logger.warning("Custom validator '%s' raised: %s", expression, exc)
                    return None
            logger.warning("CustomQuery ID %s: PYTHON validator '%s' not registered", cq_id, expression)
            return None

        # REGEX type: explicit regex (no auto-detect needed)
        if _cqtype == "REGEX":
            try:
                return bool(_re.search(expression, value or ""))
            except Exception as exc:
                logger.warning("Regex pattern failed for ID %s ('%s'): %s",
                               cq_id, expression, exc)
                return None

        # Auto-detect (CustomQueryType is None): priority order below.

        # 1. Named validator registered via register_validator()
        if expression in custom_validator_registry:
            try:
                return bool(custom_validator_registry[expression](value))
            except Exception as exc:
                logger.warning("Custom validator '%s' raised: %s", expression, exc)
                return None

        # 2. Regex pattern — heuristic: contains regex metacharacters
        if _is_regex:
            try:
                return bool(_re.search(expression, value or ""))
            except Exception as exc:
                logger.warning("Regex pattern failed for ID %s ('%s'): %s",
                               cq_id, expression, exc)
                return None

        # 3. Python expression (legacy / power user fallback)
        try:
            import re as _re_eval
            return bool(eval(expression,                           # noqa: S307
                             {"v": value, "re": _re_eval, "__builtins__": {}}))
        except Exception as exc:
            logger.warning("Custom expression eval failed for ID %s: %s", cq_id, exc)
            return None

    def check(current_value, input_value):
        val = current_value
        vtype = "Custom Query"

        # NULL values always pass (not applicable for custom queries)
        if val is None:
            return (False, vtype,
                    f"Debug:: The value <NULL> has [PASSED]. Rule: Custom Query Condition(s) of ID [{cq_id}] is/are NOT Applicable for NULL values!",
                    val)

        condition_matched = _evaluate(val)

        if condition_matched is None:
            # Unknown state → FAIL (mirrors SQL ELSE branch)
            return (True, vtype,
                    f"Debug:: The value <{val}> has [FAILED]. Rule: Custom Query Condition(s) of ID [{cq_id}] is/are NOT DEFINED and is :<Unknown>",
                    val)

        # Branch 01: is_allowed=False AND condition matches → FAIL
        if not is_allowed and condition_matched:
            return (True, vtype,
                    f"Debug:: The value <{val}> has [FAILED]. Rule: Custom Query Condition(s) of ID [{cq_id}] is/are matched but this is :<NOT Allowed>",
                    val)
        # Branch 11: is_allowed=True AND condition matches → PASS
        if is_allowed and condition_matched:
            return (False, vtype,
                    f"Debug:: The value <{val}> has [PASSED]. Rule: Custom Query Condition(s) of ID [{cq_id}] is/are matched and is :<Allowed>",
                    val)
        # Branch 00: is_allowed=False AND NOT condition matches → PASS
        if not is_allowed and not condition_matched:
            return (False, vtype,
                    f"Debug:: The value <{val}> has [PASSED]. Rule: Custom Query Condition(s) of ID [{cq_id}] is/are NOT matched and is :<NOT Allowed>",
                    val)
        # Branch 10: is_allowed=True AND NOT condition matches → FAIL
        if is_allowed and not condition_matched:
            return (True, vtype,
                    f"Debug:: The value <{val}> has [FAILED]. Rule: Custom Query Condition(s) of ID [{cq_id}] is/are NOT matched but is desired to be mandatorily :<Allowed>",
                    val)

        # Fallback ELSE → FAIL
        return (True, vtype,
                f"Debug:: The value <{val}> has [FAILED]. Rule: Custom Query Condition(s) of ID [{cq_id}] is/are NOT DEFINED and is :<Unknown>",
                val)

    return check


def _build_l04_check(min_val: Optional[str], max_val: Optional[str]):
    """
    Build an L04 (Data Value Range) check closure.

    SQL:
        SET @TargetValue = @InputValue   -- reset to original input
        IF ((@TargetValue IS NOT NULL AND @TargetValue BETWEEN 'min' AND 'max')
            OR @TargetValue IS NULL)
        → PASS
        ELSE → FAIL + RETURN

    Note: SQL BETWEEN with string literals is lexicographic — Python string
    comparison has the same semantics.
    """
    if min_val is None or max_val is None:
        return None

    def check(current_value, input_value):
        # L04 resets @TargetValue = @InputValue
        val = input_value
        if val is None or (min_val <= val <= max_val):
            return (False, "Data Value Range",
                    f"Debug:: The value <{val if val is not None else 'NULL'}> has [PASSED]. Rule: Data Value is NULL or Not Applicable or is within the desired range",
                    val)
        return (True, "Data Value Range",
                f"Debug:: The value <{val}> has [FAILED]. Rule: Data Value is NOT within the desired range",
                val)

    return check


def _build_field_checker(
    fn_name: str,
    l01_check,      # L01 closure or None
    l02_checks: list,   # list of L02 closures
    l03_checks: list,   # list of (priority, L03 closure)
    l04_check,      # L04 closure or None
) -> FieldChecker:
    """
    Assemble the ordered check sequence for one field into a single callable.

    Mirrors the SQL TVF body: L00 variables, L01, L02, L03 (ordered by priority),
    L04, then L99 (default PASS).

    The sequence of steps in priority order (same as SQL level + priority):
        L00: initialise target_value = input_value
        L01: data length  (priority = 1)
        L02: custom queries (priority = 2)
        L03: pattern checks (priority = PatternPriority + 2)
        L04: data value range (priority = 98)
        L99: default pass (priority = 99)

    On first failure → immediate return (early exit).
    """
    # Sort L03 checks by priority (already ordered, but make explicit)
    l03_sorted = sorted(l03_checks, key=lambda x: x[0])

    def field_checker(input_value: Optional[str]) -> tuple:
        """
        Evaluate all configured DQ rules for this field against ``input_value``.

        Returns
        -------
        (result, violation_type, log_message)
            result         : True = PASS, False = FAIL
            violation_type : None if PASS
            log_message    : None if PASS
        """
        # L00: initialise current_value = @TargetValue = @InputValue
        current_value = input_value

        # L01: Data Length
        if l01_check is not None:
            failed, vtype, msg, current_value = l01_check(current_value, input_value)
            if failed:
                return (False, vtype, msg)

        # L02: Custom Queries
        for l02_check in l02_checks:
            failed, vtype, msg, current_value = l02_check(current_value, input_value)
            if failed:
                return (False, vtype, msg)

        # L03: Pattern checks (ordered by PatternPriority)
        for _priority, l03_check_fn, is_allowed, pattern_value in l03_sorted:
            failed, vtype, msg, current_value = l03_check_fn(
                current_value, input_value, is_allowed, pattern_value
            )
            if failed:
                return (False, vtype, msg)

        # L04: Data Value Range
        if l04_check is not None:
            failed, vtype, msg, current_value = l04_check(current_value, input_value)
            if failed:
                return (False, vtype, msg)

        # L99: Default PASS
        return (True, None, None)

    field_checker.__name__ = fn_name
    field_checker.__qualname__ = fn_name
    return field_checker


# ---------------------------------------------------------------------------
# Main generation entry point
# ---------------------------------------------------------------------------

def generate_rule_functions(
    resolved_patterns: list[ResolvedPattern],
    config_field_values: list[dict],
    config_custom_queries: list[dict],
    registry: FunctionRegistry,
    execution_id: Optional[str] = None,
) -> None:
    """
    Build one Python field checker per distinct source field and register it
    in ``registry``.

    Equivalent to ``EXEC [dq].[p_DQ_GenerateRuleFunctions]``.

    Parameters
    ----------
    resolved_patterns
        Output of ``resolver.resolve_patterns()`` — already deduplicated and
        ranked (Rank=1 rows only).
    config_field_values
        Rows from ``configFieldValues`` (active only).
    config_custom_queries
        Rows from ``configCustomQuery`` (active only, CustomQuery NOT NULL).
    registry
        The ``FunctionRegistry`` to populate.  Existing entries are replaced
        (equivalent to DROP + CREATE in SQL).
    execution_id
        Optional execution batch ID — embedded in log messages (mirrors the
        ``@ExecutionID`` parameter of p_DQ_GenerateRuleFunctions).
    """
    exec_label = execution_id or "<Not Available>"

    # Index field values by FullFieldName
    fv_by_field: dict[str, dict] = {}
    for fv in config_field_values:
        if fv.get("IsActive", True):
            fv_by_field[fv["FullFieldName"]] = fv

    # Index custom queries by FullFieldName.
    # SQL-type queries are routed to a separate dict — they are applied at
    # the DataFrame level in the runner via F.expr(), not inside the Python
    # closure (PySpark UDFs run on executors without a SparkSession).
    cq_by_field: dict[str, list[dict]] = {}      # REGEX / PYTHON / auto-detect
    sql_cq_by_field: dict[str, list[dict]] = {}  # SQL only
    for cq in config_custom_queries:
        if cq.get("IsActive", True) and cq.get("CustomQuery"):
            cqtype = (cq.get("CustomQueryType") or "").strip().upper()
            if cqtype == "SQL":
                sql_cq_by_field.setdefault(cq["FullFieldName"], []).append(cq)
            else:
                cq_by_field.setdefault(cq["FullFieldName"], []).append(cq)

    # Group resolved patterns by (schema, table, field)
    patterns_by_field: dict[tuple, list[ResolvedPattern]] = {}
    for rp in resolved_patterns:
        key = (rp.schema_name, rp.table_name, rp.field_name)
        patterns_by_field.setdefault(key, []).append(rp)

    # Get distinct fields from all active config sources.
    # configFieldAllowedPattern drives L03; configFieldValues drives L01/L04;
    # configCustomQuery drives L02.  A field present in any one of these sources
    # gets a checker — even if only one level is configured.
    # Fields that are mapped (mapDQChecks) but have no rules in any config
    # table are intentionally excluded: generating a no-op function for them
    # would produce misleading 100% quality scores with zero checks performed.
    distinct_fields_set: set[tuple] = set(get_distinct_fields(resolved_patterns))
    for fv in config_field_values:
        if fv.get("IsActive", True) and fv.get("FullFieldName"):
            distinct_fields_set.add(_parse_ffn(fv["FullFieldName"]))
    for cq in config_custom_queries:
        if cq.get("IsActive", True) and cq.get("CustomQuery") and cq.get("FullFieldName"):
            distinct_fields_set.add(_parse_ffn(cq["FullFieldName"]))
    distinct_fields = sorted(distinct_fields_set)

    # Track which function names should exist after this run
    expected_names: set[str] = set()

    for schema, table, field in distinct_fields:
        fn_name = _make_fn_name(schema, table, field)
        expected_names.add(fn_name)
        full_field_name = ".".join(p for p in [schema, table, field] if p)

        logger.info("Generating field checker: %s  (ExecutionID: %s)", fn_name, exec_label)

        # --- L01: Data Length ---
        fv = fv_by_field.get(full_field_name)
        l01 = None
        if fv:
            l01 = _build_l01_check(fv.get("MinDataLength"), fv.get("MaxDataLength"))

        # --- L02: Custom Queries ---
        l02_list = []
        for cq in cq_by_field.get(full_field_name, []):
            l02 = _build_l02_check(
                cq["_ID"],
                cq["CustomQuery"],
                bool(cq.get("IsConditionAllowed", True)),
                custom_query_type=cq.get("CustomQueryType"),
            )
            l02_list.append(l02)

        # --- L03: Pattern checks ---
        l03_list = []
        field_patterns = patterns_by_field.get((schema, table, field), [])
        for rp in field_patterns:
            check_fn = resolve_pattern_check_fn(
                rp.pattern_category, rp.pattern_name, rp.pattern_value
            )
            if check_fn is None:
                logger.debug("No check function for pattern '%s' (%s) — skipping",
                             rp.pattern_name, rp.pattern_category)
                continue
            priority = rp.pattern_priority + 2  # mirrors L03: PatternPriority + 2 AS Priority
            l03_list.append((priority, check_fn, rp.is_pattern_allowed, rp.pattern_value))

        # --- L04: Data Value Range ---
        l04 = None
        if fv:
            l04 = _build_l04_check(fv.get("MinDataValue"), fv.get("MaxDataValue"))

        # Assemble and register Python closure (L01, REGEX/PYTHON L02, L03, L04)
        checker = _build_field_checker(fn_name, l01, l02_list, l03_list, l04)
        registry.register(fn_name, checker)

        # Register SQL-type L02 expressions separately for DataFrame-level execution.
        # Always called — even with an empty list — so that removing all SQL CQs
        # for a field (e.g. truncating configCustomQuery) clears the previous entries
        # rather than leaving stale expressions in the registry.
        sql_exprs = sql_cq_by_field.get(full_field_name, [])
        registry.register_sql_expressions(fn_name, sql_exprs)

    # --- CleanQueryText equivalent ---
    # Remove functions in the registry that are no longer in the current config
    for existing_name in registry.list_functions():
        if existing_name not in expected_names:
            logger.info("Removing stale field checker (no longer in config): %s", existing_name)
            registry.remove(existing_name)

    logger.info("generate_rule_functions complete. %d field checkers registered.", len(registry))
