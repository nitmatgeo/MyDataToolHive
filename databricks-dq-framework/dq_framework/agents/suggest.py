"""
Schema-Based Config Suggester
==============================
Reads a list of (column_name, spark_type) pairs and generates a ready-to-run
ConfigManager code snippet using heuristics.

No LLM or Spark required — pure Python heuristics based on column name patterns.
Output is a Python string that can be executed in a Databricks notebook or
returned to a user by an agent.

Usage
-----
from dq_framework.agents.suggest import suggest_config

schema = [
    ("EMAIL_ADDRESS",   "string"),
    ("FIRST_NAME",      "string"),
    ("LAST_NAME",       "string"),
    ("MOBILE_NUMBER",   "string"),
    ("DATE_OF_BIRTH",   "date"),
    ("POSTAL_CODE",     "string"),
    ("CUSTOMER_ID",     "int"),
]

code = suggest_config(
    schema,
    source_schema="Source",
    source_table="SRC_Customer",
    curated_schema="Curated",
    curated_table="Customer_Denorm",
    catalog="main",
)
print(code)
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Optional

# ---------------------------------------------------------------------------
# Column type heuristics
# ---------------------------------------------------------------------------

@dataclass
class _ColumnConfig:
    """Suggested configuration for one column."""
    column_name:        str
    data_category_id:   int
    data_category_name: str
    min_length:         Optional[int]
    max_length:         Optional[int]
    min_value:          Optional[str]
    max_value:          Optional[str]
    block_categories:   list[str]    = field(default_factory=list)
    block_patterns:     list[str]    = field(default_factory=list)
    allow_patterns:     list[str]    = field(default_factory=list)
    regex_rules:        list[tuple]  = field(default_factory=list)  # (regex, must_match, desc)
    skip:               bool         = False   # True = non-string type, skip L03 checks


def _classify(col: str, dtype: str) -> _ColumnConfig:
    """Apply heuristics to classify a column and suggest config."""
    lo = col.lower()
    dtype_lo = dtype.lower()

    # ---------- Non-string types — minimal config ----------
    if dtype_lo in ("int", "integer", "bigint", "long",
                    "double", "float", "decimal"):
        if any(k in lo for k in ("id", "key", "code", "ref")):
            return _ColumnConfig(col, 17, "Reference ID",
                                 min_length=1, max_length=20,
                                 min_value=None, max_value=None, skip=True)
        return _ColumnConfig(col, 13, "Short INT",
                             min_length=None, max_length=None,
                             min_value=None, max_value=None, skip=True)

    if dtype_lo in ("date",):
        return _ColumnConfig(col, 20, "Short Date",
                             min_length=None, max_length=None,
                             min_value=None, max_value=None, skip=True)

    if dtype_lo in ("timestamp", "datetime"):
        return _ColumnConfig(col, 21, "DateTime",
                             min_length=None, max_length=None,
                             min_value=None, max_value=None, skip=True)

    if dtype_lo in ("boolean", "bool"):
        return _ColumnConfig(col, 23, "Y/N Boolean",
                             min_length=1, max_length=5,
                             min_value=None, max_value=None, skip=True)

    # ---------- String types — classify by name ----------
    cfg = _ColumnConfig(col, 2, "Short Text",
                        min_length=None, max_length=None,
                        min_value=None, max_value=None)

    # Shared blocks for almost all string fields
    cfg.block_categories = ["DataEmptiness", "InvalidKeyword", "UnicodeCharacters"]

    # ── EMAIL ──────────────────────────────────────────────────────────────
    if re.search(r"email|e_mail|e-mail", lo):
        cfg.data_category_id   = 4
        cfg.data_category_name = "Email"
        cfg.min_length, cfg.max_length = 6, 255
        cfg.block_categories  += ["DataType3", "SpecialCharacter"]
        cfg.allow_patterns     = ["Has At Sign", "Has Full Stop", "Has Hyphen", "Has Underscore"]
        cfg.block_patterns     = ["Is Fully Numeric", "Has Space", "Has Fully Duplicated Character"]
        cfg.regex_rules        = [
            (r"^[^@\s]+@[^@\s]+\.[^@\s]+$", True,  "Basic email format: local@domain.ext"),
            (r"^[^@]+@[^@]+$",              True,  "Exactly one @ sign"),
            (r"noemaildress",               False, "Reject placeholder email addresses"),
            (r"[-._]$",                     False, "Must not end with . - or _"),
        ]
        return cfg

    # ── PHONE / MOBILE ─────────────────────────────────────────────────────
    if re.search(r"mobile|cell|phone|tel\b|telephone", lo):
        cfg.data_category_id   = 12
        cfg.data_category_name = "Phone Number"
        cfg.min_length, cfg.max_length = 8, 15
        cfg.block_categories  += ["SpecialCharacter"]
        cfg.allow_patterns     = ["Has Plus Sign"]
        cfg.block_patterns     = ["Has Space", "Has Fully Duplicated Character"]
        if re.search(r"mobile|cell", lo):
            cfg.regex_rules    = [
                (r"^(04[0-9]{8}|\+614[0-9]{8})$", True,
                 "AU mobile: 04XXXXXXXX or +614XXXXXXXX"),
            ]
        return cfg

    # ── POSTAL CODE / POSTCODE / ZIP ───────────────────────────────────────
    if re.search(r"post.?code|postal|zip.?code|zipcode", lo):
        cfg.data_category_id   = 10
        cfg.data_category_name = "Postal Code"
        cfg.min_length, cfg.max_length = 4, 10
        cfg.block_patterns     = ["Has Space", "Has Fully Duplicated Character"]
        cfg.regex_rules        = [
            (r"^[0-9]{4}$", True, "4-digit Australian postcode"),
        ]
        return cfg

    # ── NAME fields ────────────────────────────────────────────────────────
    if re.search(r"first.?name|forename|given.?name|fname", lo):
        cfg.data_category_id   = 2
        cfg.data_category_name = "Short Text"
        cfg.min_length, cfg.max_length = 1, 50
        cfg.block_categories  += ["DataType3", "SpecialCharacter"]
        cfg.allow_patterns     = ["Has Hyphen", "Has Single Quote", "Has Full Stop",
                                  "Has Unicode Characters"]
        cfg.block_patterns     = ["Is Fully Numeric", "Has Fully Duplicated Character"]
        return cfg

    if re.search(r"last.?name|surname|family.?name|lname", lo):
        cfg.data_category_id   = 2
        cfg.data_category_name = "Short Text"
        cfg.min_length, cfg.max_length = 1, 100
        cfg.block_categories  += ["DataType3", "SpecialCharacter"]
        cfg.allow_patterns     = ["Has Hyphen", "Has Single Quote", "Has Full Stop",
                                  "Has Space", "Has Unicode Characters"]
        cfg.block_patterns     = ["Is Fully Numeric", "Has Fully Duplicated Character"]
        return cfg

    if re.search(r"\bname\b|full.?name|display.?name", lo):
        cfg.data_category_id   = 2
        cfg.data_category_name = "Short Text"
        cfg.min_length, cfg.max_length = 2, 100
        cfg.block_categories  += ["DataType3"]
        cfg.block_patterns     = ["Has Fully Duplicated Character"]
        return cfg

    # ── DATE fields ────────────────────────────────────────────────────────
    if re.search(r"date|dob|birth|created|updated|timestamp", lo):
        cfg.data_category_id   = 20
        cfg.data_category_name = "Short Date"
        cfg.min_length, cfg.max_length = 8, 10
        cfg.block_patterns     = ["Is Fully Numeric", "Has Space"]
        cfg.regex_rules        = [
            (r"^\d{4}-\d{2}-\d{2}$", True, "ISO date format YYYY-MM-DD"),
        ]
        return cfg

    # ── ID / key fields ────────────────────────────────────────────────────
    if re.search(r"\b(id|key|code|ref|uuid|guid)\b", lo):
        cfg.data_category_id   = 17
        cfg.data_category_name = "Reference ID"
        cfg.min_length, cfg.max_length = 1, 50
        cfg.block_patterns     = ["Has Space"]
        return cfg

    # ── ADDRESS fields ─────────────────────────────────────────────────────
    if re.search(r"address|street|suburb|city|state|country", lo):
        cfg.data_category_id   = 6
        cfg.data_category_name = "Address"
        cfg.min_length, cfg.max_length = 2, 255
        cfg.block_patterns     = ["Has Fully Duplicated Character"]
        return cfg

    # ── URL ────────────────────────────────────────────────────────────────
    if re.search(r"url|link|website|web.?site|homepage", lo):
        cfg.data_category_id   = 5
        cfg.data_category_name = "URL"
        cfg.min_length, cfg.max_length = 7, 500
        cfg.block_categories  += ["DataEmptiness"]
        cfg.regex_rules        = [
            (r"^https?://", True, "Must start with http:// or https://"),
        ]
        return cfg

    # ── Generic short text (default) ───────────────────────────────────────
    cfg.min_length, cfg.max_length = 1, 255
    cfg.block_patterns = ["Has Fully Duplicated Character"]
    return cfg


# ---------------------------------------------------------------------------
# Code generator
# ---------------------------------------------------------------------------

def suggest_config(
    schema: list[tuple[str, str]],
    source_schema: str = "Source",
    source_table: str  = "SRC_Table",
    curated_schema: str = "Curated",
    curated_table: str  = "",
    catalog: str        = "main",
    start_field_id: int  = 1,
    start_rule_id: int   = 1,
    start_query_id: int  = 1,
) -> str:
    """
    Generate ConfigManager code from a table schema.

    Parameters
    ----------
    schema
        List of (column_name, spark_data_type) tuples.
    source_schema / source_table
        Used to build FullFieldName strings.
    curated_schema / curated_table
        Target location for add_mapping() calls.
    catalog
        Unity Catalog name.
    start_*
        Starting IDs for field, rule, and query registrations.

    Returns
    -------
    A multi-line Python string ready to run in a Databricks notebook.
    """
    curated_table = curated_table or source_table

    lines: list[str] = [
        "# ── Auto-generated DQ configuration — review and adjust before running ─────",
        f"# Source: {source_schema}.{source_table}",
        f"# Target: {catalog}.{curated_schema}.{curated_table}",
        "# Generated by dq_framework.agents.suggest.suggest_config()",
        "",
        "cfg = dq.config",
        "",
    ]

    field_id  = start_field_id
    rule_id   = start_rule_id
    query_id  = start_query_id

    for col_name, dtype in schema:
        cfg = _classify(col_name, dtype)
        ffn = f"{source_schema}.{source_table}.{col_name}"

        lines.append(f"# ── {col_name} ({cfg.data_category_name}) {'─'*(40 - len(col_name))}")

        # register_field
        lines.append(
            f"cfg.register_field({field_id}, '{ffn}', "
            f"data_category_type_id={cfg.data_category_id})  "
            f"# {cfg.data_category_name}"
        )

        # set_field_values
        if cfg.min_length is not None or cfg.min_value is not None:
            parts = []
            if cfg.min_length is not None:
                parts.append(f"min_data_length={cfg.min_length}, max_data_length={cfg.max_length}")
            if cfg.min_value is not None:
                parts.append(f"min_data_value='{cfg.min_value}', max_data_value='{cfg.max_value}'")
            lines.append(f"cfg.set_field_values({field_id}, '{ffn}', {', '.join(parts)})")

        if not cfg.skip:
            # block_category calls
            for i, cat in enumerate(cfg.block_categories):
                lines.append(f"cfg.block_category({rule_id}, '{ffn}', '{cat}')")
                rule_id += 1

            # block_pattern calls
            for pat in cfg.block_patterns:
                lines.append(f"cfg.block_pattern({rule_id}, '{ffn}', '{pat}')")
                rule_id += 1

            # allow_pattern calls
            for pat in cfg.allow_patterns:
                lines.append(f"cfg.allow_pattern({rule_id}, '{ffn}', '{pat}')")
                rule_id += 1

            # add_custom_query_regex calls
            for regex, must_match, desc in cfg.regex_rules:
                lines.append(
                    f"cfg.add_custom_query_regex({query_id}, '{ffn}', "
                    f"r'{regex}', must_match={must_match}, description='{desc}')"
                )
                query_id += 1

        # add_mapping
        lines.append(
            f"cfg.add_mapping({field_id}, '{ffn}', "
            f"target_schema_name='{curated_schema}', target_table_name='{curated_table}', "
            f"target_field_name='{col_name}', target_catalog_name='{catalog}')"
        )

        lines.append("")
        field_id += 1

    lines += [
        "# ── Verify and generate ──────────────────────────────────────────────────────",
        "cfg.show_config_summary()",
        "dq.generate_rule_functions()",
    ]

    return "\n".join(lines)
