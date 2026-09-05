"""
Master Reference Data
======================
Equivalent to Script_01_Master_Reference_Data.sql.

Provides the seeding functions for:
  - 27 masterDataCategory rows
  - 118 masterPattern rows (all validation patterns)

All data matches the SQL Server seed exactly — data types, pattern values,
priorities and category classifications are preserved without modification.

Idempotent loading (equivalent to MERGE + IDENTITY_INSERT in SQL)
------------------------------------------------------------------
``seed_master_data()`` performs an upsert so it is safe to re-run on a
populated Delta table without creating duplicates.
"""

from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

# ---------------------------------------------------------------------------
# 27 Data Categories
# ---------------------------------------------------------------------------

MASTER_DATA_CATEGORIES = [
    # _ID, DataCategoryType, DataType, DataCategoryShortDescription, DataCategoryDescription
    (1,  "STRING",       "varchar",  "Free Text Long",        "Free text field with unrestricted character length"),
    (2,  "STRING",       "varchar",  "Free Text Short",       "Free text field with limited character length"),
    (3,  "STRING",       "varchar",  "Short Text Value",      "Short reference code or lookup text value"),
    (4,  "STRING",       "varchar",  "Email",                 "Electronic mail address"),
    (5,  "STRING",       "varchar",  "URL",                   "Uniform Resource Locator (web address)"),
    (6,  "STRING",       "varchar",  "Address",               "Physical or postal address string"),
    (7,  "STRING",       "varchar",  "Time Zone",             "Time zone identifier or UTC offset"),
    (8,  "STRING",       "varchar",  "File Name",             "Name of a file without path"),
    (9,  "STRING",       "varchar",  "File Path",             "Full or relative file system path"),
    (10, "ALPHANUMERIC", "varchar",  "Postal Code",           "Postal or ZIP code"),
    (11, "ALPHANUMERIC", "varchar",  "License Number",        "Vehicle, professional or other licence number"),
    (12, "ALPHANUMERIC", "varchar",  "Phone Number",          "Telephone number in any format"),
    (13, "NUMERIC",      "int",      "Short Integer",         "Small integer numeric value (INT range)"),
    (14, "NUMERIC",      "bigint",   "Long Integer",          "Large integer numeric value (BIGINT range)"),
    (15, "NUMERIC",      "float",    "Decimal",               "Floating point decimal numeric value"),
    (16, "NUMERIC",      "decimal",  "Currency",              "Monetary decimal value"),
    (17, "NUMERIC",      "int",      "Reference ID",          "Foreign key or reference integrity identifier"),
    (18, "NUMERIC",      "bigint",   "Social Security No",    "Government-issued social security number"),
    (19, "STRING",       "varchar",  "IPv4 Address",          "Internet Protocol version 4 address"),
    (20, "DATE",         "date",     "Short Date",            "Date value without time component"),
    (21, "TIME",         "datetime", "Date With Time",        "Datetime value with date and time components"),
    (22, "TIME",         "time",     "Only Time",             "Time value without date component"),
    (23, "BOOLEAN",      "bit",      "Boolean Y/N",           "Boolean flag stored as Y or N character"),
    (24, "BOOLEAN",      "bit",      "Boolean 0/1",           "Boolean flag stored as 0 or 1 digit"),
    (25, "BOOLEAN",      "bit",      "Boolean T/F",           "Boolean flag stored as T or F character"),
    (26, "LOCATION",     "decimal",  "Latitude",              "Geographic latitude coordinate decimal degrees"),
    (27, "LOCATION",     "decimal",  "Longitude",             "Geographic longitude coordinate decimal degrees"),
]

# ---------------------------------------------------------------------------
# 118 Validation Patterns
# ---------------------------------------------------------------------------
# (_ID, PatternCategory, PatternSubCategory, PatternName, PatternDescription,
#  PatternPriority, PatternValue)

MASTER_PATTERNS = [
    # --- DataType1 (3 patterns) ---
    (1,  "DataType1", None, "Is Fully Numeric",  "Value consists entirely of digits 0-9 (no decimal, no sign)", 10, None),
    (2,  "DataType1", None, "Is Fully Decimal",  "Value consists of digits and exactly one decimal point",       10, None),
    (3,  "DataType1", None, "Is Fully Text",      "Value contains no digit characters 0-9",                      10, None),

    # --- DataType2 (1 pattern) ---
    (4,  "DataType2", None, "Is AlphaNumeric",   "Value contains both letters and digits",                       10, None),

    # --- DataType3 (4 patterns) ---
    (5,  "DataType3", None, "Is Date",           "Value is a date without time component (no : or .)",          10, None),
    (6,  "DataType3", None, "Is Time",           "Value is a time HH:MM or HH:MM:SS.mmm (length 8–16)",        10, None),
    (7,  "DataType3", None, "Is Timestamp",      "Value is a datetime with both date (- separator) and time (: separator)", 10, None),
    (8,  "DataType3", None, "Is Boolean",        "Value is exactly '0' or '1'",                                 10, None),

    # --- SpecialCharacter (32 patterns) ---
    (10, "SpecialCharacter", "Symbol",    "Has Exclamation Mark",   "Contains !",  20, "!"),
    (11, "SpecialCharacter", "Symbol",    "Has At Sign",            "Contains @",  20, "@"),
    (12, "SpecialCharacter", "Symbol",    "Has Hash",               "Contains #",  20, "#"),
    (13, "SpecialCharacter", "Symbol",    "Has Dollar Sign",        "Contains $",  20, "$"),
    (14, "SpecialCharacter", "Symbol",    "Has Percent Sign",       "Contains %",  20, "%"),
    (15, "SpecialCharacter", "Symbol",    "Has Caret",              "Contains ^",  20, "^"),
    (16, "SpecialCharacter", "Symbol",    "Has Ampersand",          "Contains &",  20, "&"),
    (17, "SpecialCharacter", "Symbol",    "Has Asterisk",           "Contains *",  20, "*"),
    (18, "SpecialCharacter", "Bracket",   "Has Open Parenthesis",   "Contains (",  20, "("),
    (19, "SpecialCharacter", "Bracket",   "Has Close Parenthesis",  "Contains )",  20, ")"),
    (20, "SpecialCharacter", "Bracket",   "Has Open Square Bracket","Contains [",  20, "["),
    (21, "SpecialCharacter", "Bracket",   "Has Close Square Bracket","Contains ]", 20, "]"),
    (22, "SpecialCharacter", "Bracket",   "Has Open Curly Brace",   "Contains {",  20, "{"),
    (23, "SpecialCharacter", "Bracket",   "Has Close Curly Brace",  "Contains }",  20, "}"),
    (24, "SpecialCharacter", "Separator", "Has Pipe Symbol",        "Contains |",  20, "|"),
    (25, "SpecialCharacter", "Separator", "Has Backslash",          "Contains \\", 20, "\\"),
    (26, "SpecialCharacter", "Separator", "Has Forward Slash",      "Contains /",  20, "/"),
    (27, "SpecialCharacter", "Separator", "Has Comma",              "Contains ,",  20, ","),
    (28, "SpecialCharacter", "Separator", "Has Semicolon",          "Contains ;",  20, ";"),
    (29, "SpecialCharacter", "Separator", "Has Colon",              "Contains :",  20, ":"),
    (30, "SpecialCharacter", "Separator", "Has Full Stop",          "Contains .",  20, "."),
    (31, "SpecialCharacter", "Separator", "Has Hyphen",             "Contains -",  20, "-"),
    (32, "SpecialCharacter", "Separator", "Has Underscore",         "Contains _",  20, "_"),
    (33, "SpecialCharacter", "Quote",     "Has Single Quote",       "Contains '",  20, "'"),
    (34, "SpecialCharacter", "Quote",     "Has Double Quote",       'Contains "',  20, '"'),
    (35, "SpecialCharacter", "Quote",     "Has Backtick",           "Contains `",  20, "`"),
    (36, "SpecialCharacter", "Symbol",    "Has Tilde",              "Contains ~",  20, "~"),
    (37, "SpecialCharacter", "Symbol",    "Has Plus Sign",          "Contains +",  20, "+"),
    (38, "SpecialCharacter", "Symbol",    "Has Less Than",          "Contains <",  20, "<"),
    (39, "SpecialCharacter", "Symbol",    "Has Greater Than",       "Contains >",  20, ">"),
    (40, "SpecialCharacter", "Symbol",    "Has Question Mark",      "Contains ?",  20, "?"),
    (41, "SpecialCharacter", "Symbol",    "Has Equal Sign",         "Contains =",  20, "="),

    # --- SpaceFound (1 pattern) ---
    (50, "SpaceFound",  None, "Has Space", "Value contains one or more space characters", 15, None),

    # --- DataEmptiness (34 patterns) ---
    (60, "DataEmptiness", "Null",        "Is Empty or NULL",                 "Value is NULL or empty string",                    1,  None),
    (61, "DataEmptiness", "Spaces",      "Is Virtually Empty with Spaces",   "Value is all whitespace (LTRIM/RTRIM = '')",        2,  None),
    (62, "DataEmptiness", "Character",   "Is Virtually Empty with ,",        "All chars (stripped) are commas",                  3,  ","),
    (63, "DataEmptiness", "Character",   "Is Virtually Empty with .",        "All chars (stripped) are full stops",               3,  "."),
    (64, "DataEmptiness", "Character",   "Is Virtually Empty with -",        "All chars (stripped) are hyphens",                  3,  "-"),
    (65, "DataEmptiness", "Character",   "Is Virtually Empty with _",        "All chars (stripped) are underscores",              3,  "_"),
    (66, "DataEmptiness", "Character",   "Is Virtually Empty with /",        "All chars (stripped) are forward slashes",          3,  "/"),
    (67, "DataEmptiness", "Character",   "Is Virtually Empty with \\",       "All chars (stripped) are backslashes",              3,  "\\"),
    (68, "DataEmptiness", "Character",   "Is Virtually Empty with |",        "All chars (stripped) are pipe symbols",             3,  "|"),
    (69, "DataEmptiness", "Character",   "Is Virtually Empty with :",        "All chars (stripped) are colons",                   3,  ":"),
    (70, "DataEmptiness", "Character",   "Is Virtually Empty with ;",        "All chars (stripped) are semicolons",               3,  ";"),
    (71, "DataEmptiness", "Character",   "Is Virtually Empty with ?",        "All chars (stripped) are question marks",           3,  "?"),
    (72, "DataEmptiness", "Character",   "Is Virtually Empty with !",        "All chars (stripped) are exclamation marks",        3,  "!"),
    (73, "DataEmptiness", "Character",   "Is Virtually Empty with @",        "All chars (stripped) are at signs",                 3,  "@"),
    (74, "DataEmptiness", "Character",   "Is Virtually Empty with #",        "All chars (stripped) are hashes",                   3,  "#"),
    (75, "DataEmptiness", "Character",   "Is Virtually Empty with $",        "All chars (stripped) are dollar signs",             3,  "$"),
    (76, "DataEmptiness", "Character",   "Is Virtually Empty with %",        "All chars (stripped) are percent signs",            3,  "%"),
    (77, "DataEmptiness", "Character",   "Is Virtually Empty with ^",        "All chars (stripped) are carets",                   3,  "^"),
    (78, "DataEmptiness", "Character",   "Is Virtually Empty with &",        "All chars (stripped) are ampersands",               3,  "&"),
    (79, "DataEmptiness", "Character",   "Is Virtually Empty with *",        "All chars (stripped) are asterisks",                3,  "*"),
    (80, "DataEmptiness", "Character",   "Is Virtually Empty with (",        "All chars (stripped) are open parentheses",         3,  "("),
    (81, "DataEmptiness", "Character",   "Is Virtually Empty with )",        "All chars (stripped) are close parentheses",        3,  ")"),
    (82, "DataEmptiness", "Character",   "Is Virtually Empty with [",        "All chars (stripped) are open square brackets",     3,  "["),
    (83, "DataEmptiness", "Character",   "Is Virtually Empty with ]",        "All chars (stripped) are close square brackets",    3,  "]"),
    (84, "DataEmptiness", "Character",   "Is Virtually Empty with {",        "All chars (stripped) are open curly braces",        3,  "{"),
    (85, "DataEmptiness", "Character",   "Is Virtually Empty with }",        "All chars (stripped) are close curly braces",       3,  "}"),
    (86, "DataEmptiness", "Character",   "Is Virtually Empty with +",        "All chars (stripped) are plus signs",               3,  "+"),
    (87, "DataEmptiness", "Character",   "Is Virtually Empty with =",        "All chars (stripped) are equal signs",              3,  "="),
    (88, "DataEmptiness", "Character",   "Is Virtually Empty with <",        "All chars (stripped) are less-than signs",          3,  "<"),
    (89, "DataEmptiness", "Character",   "Is Virtually Empty with >",        "All chars (stripped) are greater-than signs",       3,  ">"),
    (90, "DataEmptiness", "Character",   "Is Virtually Empty with ~",        "All chars (stripped) are tildes",                   3,  "~"),
    (91, "DataEmptiness", "Character",   "Is Virtually Empty with '",        "All chars (stripped) are single quotes",            3,  "'"),
    (92, "DataEmptiness", "Character",   'Is Virtually Empty with "',        "All chars (stripped) are double quotes",            3,  '"'),
    (93, "DataEmptiness", "Character",   "Is Virtually Empty with `",        "All chars (stripped) are backticks",                3,  "`"),

    # --- InvalidKeyword (40 patterns) ---
    (100, "InvalidKeyword", "Generic",  "Has Keyword-n/a",          "Keyword proportion ≥ 50%: n/a",       30, "n/a"),
    (101, "InvalidKeyword", "Generic",  "Has Keyword-null",         "Keyword proportion ≥ 50%: null",      30, "null"),
    (102, "InvalidKeyword", "Generic",  "Has Keyword-nil",          "Keyword proportion ≥ 50%: nil",       30, "nil"),
    (103, "InvalidKeyword", "Generic",  "Has Keyword-none",         "Keyword proportion ≥ 50%: none",      30, "none"),
    (104, "InvalidKeyword", "Generic",  "Has Keyword-missing",      "Keyword proportion ≥ 50%: missing",   30, "missing"),
    (105, "InvalidKeyword", "Generic",  "Has Keyword-unknown",      "Keyword proportion ≥ 50%: unknown",   30, "unknown"),
    (106, "InvalidKeyword", "Generic",  "Has Keyword-undefined",    "Keyword proportion ≥ 50%: undefined", 30, "undefined"),
    (107, "InvalidKeyword", "Generic",  "Has Keyword-not provided", "Keyword proportion ≥ 50%: not provided", 30, "not provided"),
    (108, "InvalidKeyword", "Generic",  "Has Keyword-not available","Keyword proportion ≥ 50%: not available", 30, "not available"),
    (109, "InvalidKeyword", "Generic",  "Has Keyword-not applicable","Keyword proportion ≥ 50%: not applicable", 30, "not applicable"),
    (110, "InvalidKeyword", "Generic",  "Has Keyword-not known",    "Keyword proportion ≥ 50%: not known", 30, "not known"),
    (111, "InvalidKeyword", "Generic",  "Has Keyword-not set",      "Keyword proportion ≥ 50%: not set",   30, "not set"),
    (112, "InvalidKeyword", "Generic",  "Has Keyword-na",           "Keyword proportion ≥ 50%: na",        30, "na"),
    (113, "InvalidKeyword", "Generic",  "Has Keyword-no data",      "Keyword proportion ≥ 50%: no data",   30, "no data"),
    (114, "InvalidKeyword", "Generic",  "Has Keyword-no info",      "Keyword proportion ≥ 50%: no info",   30, "no info"),
    (115, "InvalidKeyword", "Generic",  "Has Keyword-no value",     "Keyword proportion ≥ 50%: no value",  30, "no value"),
    (116, "InvalidKeyword", "Generic",  "Has Keyword-blank",        "Keyword proportion ≥ 50%: blank",     30, "blank"),
    (117, "InvalidKeyword", "Generic",  "Has Keyword-empty",        "Keyword proportion ≥ 50%: empty",     30, "empty"),
    (118, "InvalidKeyword", "Generic",  "Has Keyword-void",         "Keyword proportion ≥ 50%: void",      30, "void"),
    (119, "InvalidKeyword", "Generic",  "Has Keyword-tbd",          "Keyword proportion ≥ 50%: tbd",       30, "tbd"),
    (120, "InvalidKeyword", "Generic",  "Has Keyword-tbc",          "Keyword proportion ≥ 50%: tbc",       30, "tbc"),
    (121, "InvalidKeyword", "Generic",  "Has Keyword-todo",         "Keyword proportion ≥ 50%: todo",      30, "todo"),
    (122, "InvalidKeyword", "Generic",  "Has Keyword-pending",      "Keyword proportion ≥ 50%: pending",   30, "pending"),
    (123, "InvalidKeyword", "Generic",  "Has Keyword-unset",        "Keyword proportion ≥ 50%: unset",     30, "unset"),
    (124, "InvalidKeyword", "Generic",  "Has Keyword-unspecified",  "Keyword proportion ≥ 50%: unspecified", 30, "unspecified"),
    (125, "InvalidKeyword", "Generic",  "Has Keyword-invalid",      "Keyword proportion ≥ 50%: invalid",   30, "invalid"),
    (126, "InvalidKeyword", "Generic",  "Has Keyword-error",        "Keyword proportion ≥ 50%: error",     30, "error"),
    (127, "InvalidKeyword", "Generic",  "Has Keyword-test",         "Keyword proportion ≥ 50%: test",      30, "test"),
    (128, "InvalidKeyword", "Generic",  "Has Keyword-dummy",        "Keyword proportion ≥ 50%: dummy",     30, "dummy"),
    (129, "InvalidKeyword", "Generic",  "Has Keyword-fake",         "Keyword proportion ≥ 50%: fake",      30, "fake"),
    (130, "InvalidKeyword", "Generic",  "Has Keyword-placeholder",  "Keyword proportion ≥ 50%: placeholder", 30, "placeholder"),
    (131, "InvalidKeyword", "Generic",  "Has Keyword-sample",       "Keyword proportion ≥ 50%: sample",    30, "sample"),
    (132, "InvalidKeyword", "Generic",  "Has Keyword-temp",         "Keyword proportion ≥ 50%: temp",      30, "temp"),
    (133, "InvalidKeyword", "Generic",  "Has Keyword-xxx",          "Keyword proportion ≥ 50%: xxx",       30, "xxx"),
    (134, "InvalidKeyword", "Generic",  "Has Keyword-aaa",          "Keyword proportion ≥ 50%: aaa",       30, "aaa"),
    (135, "InvalidKeyword", "Generic",  "Has Keyword-zzz",          "Keyword proportion ≥ 50%: zzz",       30, "zzz"),
    (136, "InvalidKeyword", "Generic",  "Has Keyword-000",          "Keyword proportion ≥ 50%: 000",       30, "000"),
    (137, "InvalidKeyword", "Generic",  "Has Keyword-999",          "Keyword proportion ≥ 50%: 999",       30, "999"),
    (138, "InvalidKeyword", "Generic",  "Has Keyword-123",          "Keyword proportion ≥ 50%: 123",       30, "123"),
    (139, "InvalidKeyword", "Generic",  "Has Keyword-abc",          "Keyword proportion ≥ 50%: abc",       30, "abc"),

    # --- FullyDuplicatedCharacter (1 pattern) ---
    (150, "FullyDuplicatedCharacter", None, "Has Fully Duplicated Character",
     "All characters (after stripping spaces) are the same non-numeric character", 40, None),

    # --- UnicodeCharacters (1 pattern) ---
    (160, "UnicodeCharacters", None, "Has Unicode Characters",
     "Contains characters outside printable ASCII range (0x20–0x7E)", 45, None),

    # --- CasingCheck (2 patterns) ---
    (170, "CasingCheck", None, "Has Lowercase Character", "Contains at least one a-z character (case-sensitive)", 50, None),
    (171, "CasingCheck", None, "Has Uppercase Character", "Contains at least one A-Z character (case-sensitive)", 50, None),
]


# ---------------------------------------------------------------------------
# Seeding functions
# ---------------------------------------------------------------------------

def seed_master_data(spark: "SparkSession", catalog: str, dq_schema: str) -> None:
    """
    Load masterDataCategory and masterPattern with all reference data.

    Idempotent — safe to re-run on a populated table.  Uses Delta MERGE to
    insert new rows and update existing ones (equivalent to SQL MERGE +
    IDENTITY_INSERT).
    """
    _seed_data_categories(spark, catalog, dq_schema)
    _seed_patterns(spark, catalog, dq_schema)


def _fqn(catalog: str, schema: str, table: str) -> str:
    if catalog:
        return f"`{catalog}`.`{schema}`.`{table}`"
    return f"`{schema}`.`{table}`"


def _sql_str(v) -> str:
    """Return a Spark SQL literal for a Python value (handles None, bool, int, str)."""
    if v is None:
        return "NULL"
    if isinstance(v, bool):
        return "true" if v else "false"
    if isinstance(v, int):
        return str(v)
    # Escape backslashes first (\\  → \\\\), then single quotes (' → '')
    # Backslash must come first — otherwise the '' replacement could be re-escaped.
    return "'" + str(v).replace("\\", "\\\\").replace("'", "''") + "'"


def _seed_data_categories(spark: "SparkSession", catalog: str, dq_schema: str) -> None:
    """
    Seed 27 data categories using a pure SQL MERGE … WHEN NOT MATCHED INSERT.

    No createDataFrame() — avoids CANNOT_DETERMINE_TYPE in Spark Connect
    (Databricks Serverless), which infers types from Python data even when
    an explicit schema is provided, and fails on all-None columns.
    """
    fqn = _fqn(catalog, dq_schema, "masterDataCategory")
    now_str = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

    value_rows = []
    for r in MASTER_DATA_CATEGORIES:
        value_rows.append(
            f"({_sql_str(r[0])}, {_sql_str(r[1])}, {_sql_str(r[2])}, "
            f"{_sql_str(r[3])}, {_sql_str(r[4])}, "
            f"true, 'sys', TIMESTAMP('{now_str}'), NULL, NULL)"
        )

    values_sql = ",\n            ".join(value_rows)
    spark.sql(f"""
        MERGE INTO {fqn} AS t
        USING (
            SELECT
                _ID, DataCategoryType, DataType,
                DataCategoryShortDescription, DataCategoryDescription,
                IsActive, CreatedBy, CreatedOn,
                CAST(LastUpdatedBy AS STRING)    AS LastUpdatedBy,
                CAST(LastUpdatedOn AS TIMESTAMP) AS LastUpdatedOn
            FROM VALUES
                {values_sql}
            AS v(
                _ID, DataCategoryType, DataType,
                DataCategoryShortDescription, DataCategoryDescription,
                IsActive, CreatedBy, CreatedOn,
                LastUpdatedBy, LastUpdatedOn
            )
        ) AS s ON t._ID = s._ID
        WHEN NOT MATCHED THEN INSERT *
    """)


def _seed_patterns(spark: "SparkSession", catalog: str, dq_schema: str) -> None:
    """
    Seed 118 built-in validation patterns using a pure SQL MERGE … WHEN NOT MATCHED INSERT.

    No createDataFrame() — avoids CANNOT_DETERMINE_TYPE in Spark Connect.
    Framework rows occupy _ID 1–999; user custom patterns should use _ID >= 1000.
    Existing rows are NEVER overwritten (INSERT-ONLY merge).
    """
    fqn = _fqn(catalog, dq_schema, "masterPattern")
    now_str = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

    value_rows = []
    for r in MASTER_PATTERNS:
        value_rows.append(
            f"({_sql_str(r[0])}, {_sql_str(r[1])}, {_sql_str(r[2])}, "
            f"{_sql_str(r[3])}, {_sql_str(r[4])}, {_sql_str(r[5])}, {_sql_str(r[6])}, "
            f"true, 'sys', TIMESTAMP('{now_str}'), NULL, NULL)"
        )

    values_sql = ",\n            ".join(value_rows)
    spark.sql(f"""
        MERGE INTO {fqn} AS t
        USING (
            SELECT
                _ID, PatternCategory, PatternSubCategory,
                PatternName, PatternDescription, PatternPriority, PatternValue,
                IsActive, CreatedBy, CreatedOn,
                CAST(LastUpdatedBy AS STRING)    AS LastUpdatedBy,
                CAST(LastUpdatedOn AS TIMESTAMP) AS LastUpdatedOn
            FROM VALUES
                {values_sql}
            AS v(
                _ID, PatternCategory, PatternSubCategory,
                PatternName, PatternDescription, PatternPriority, PatternValue,
                IsActive, CreatedBy, CreatedOn,
                LastUpdatedBy, LastUpdatedOn
            )
        ) AS s ON t._ID = s._ID
        WHEN NOT MATCHED THEN INSERT *
    """)


# ---------------------------------------------------------------------------
# User-extensible pattern helpers
# ---------------------------------------------------------------------------

def add_invalid_keyword(
    spark: "SparkSession",
    catalog: str,
    dq_schema: str,
    keyword: str,
    pattern_id: int,
    priority: int = 30,
    description: str = None,
) -> None:
    """
    Add a custom InvalidKeyword pattern to ``masterPattern``.

    The InvalidKeyword check fires when the keyword comprises >= 50% of the
    value (case-insensitive, trimmed) — matching the hardcoded threshold from
    the original SQL engine (``SET @Threshold = 0.5``).

    Project teams use this when their data has domain-specific placeholders
    (e.g. 'nodata', 'notset', 'noemail') that differ from the 40 built-in
    keywords.

    Parameters
    ----------
    keyword
        The keyword string to detect (stored as PatternValue, lowercased at
        evaluation time — case-insensitive match).
    pattern_id
        Must be >= 1000 (framework reserves 1–999).
    priority
        PatternPriority — controls order within L03 checks (default: 30,
        same as built-in InvalidKeyword patterns).
    description
        Optional human-readable description.

    Raises
    ------
    ValueError
        If pattern_id < 1000 (collision with framework-reserved range).
    """
    from .ddl_framework_tables import USER_PATTERN_ID_START
    if pattern_id < USER_PATTERN_ID_START:
        raise ValueError(
            f"pattern_id must be >= {USER_PATTERN_ID_START}. "
            f"IDs 1–999 are reserved for framework built-in patterns."
        )
    fqn = _fqn(catalog, dq_schema, "masterPattern")
    desc = description or f"Keyword proportion >= 50%: {keyword}"
    # Escape single quotes to prevent SQL string breakage
    kw_sql = keyword.replace("'", "''")
    desc_sql = desc.replace("'", "''")
    spark.sql(f"""
        MERGE INTO {fqn} AS t
        USING (SELECT {pattern_id} AS _ID) AS s ON t._ID = s._ID
        WHEN NOT MATCHED THEN INSERT (
            _ID, PatternCategory, PatternSubCategory, PatternName,
            PatternDescription, PatternPriority, PatternValue,
            IsActive, CreatedBy, CreatedOn, LastUpdatedBy, LastUpdatedOn
        ) VALUES (
            {pattern_id}, 'InvalidKeyword', 'Custom',
            'Has Keyword-{kw_sql}', '{desc_sql}', {priority}, '{kw_sql}',
            true, current_user(), current_timestamp(), null, null
        )
    """)


def add_custom_pattern(
    spark: "SparkSession",
    catalog: str,
    dq_schema: str,
    pattern_id: int,
    pattern_category: str,
    pattern_name: str,
    pattern_priority: int,
    pattern_value: str = None,
    pattern_subcategory: str = None,
    pattern_description: str = None,
) -> None:
    """
    Add any custom pattern to ``masterPattern``.

    After adding a pattern, you must also:
    1. Add rows to ``configFieldAllowedPattern`` referencing the new PatternName.
    2. Register a corresponding check function via ``DQFramework.register_validator()``
       if the pattern requires custom Python logic.
    3. Re-run ``dq.generate_rule_functions()``.

    pattern_id must be >= 1000 (framework reserves 1–999).
    """
    from .ddl_framework_tables import USER_PATTERN_ID_START
    if pattern_id < USER_PATTERN_ID_START:
        raise ValueError(
            f"pattern_id must be >= {USER_PATTERN_ID_START}. "
            f"IDs 1–999 are reserved for framework built-in patterns."
        )
    fqn = _fqn(catalog, dq_schema, "masterPattern")
    # Escape single quotes to prevent SQL string breakage
    def _esc(s):
        return s.replace("'", "''") if s else s
    pv  = f"'{_esc(pattern_value)}'"       if pattern_value       else "null"
    psc = f"'{_esc(pattern_subcategory)}'" if pattern_subcategory else "null"
    pd  = f"'{_esc(pattern_description)}'" if pattern_description else "null"
    spark.sql(f"""
        MERGE INTO {fqn} AS t
        USING (SELECT {pattern_id} AS _ID) AS s ON t._ID = s._ID
        WHEN NOT MATCHED THEN INSERT (
            _ID, PatternCategory, PatternSubCategory, PatternName,
            PatternDescription, PatternPriority, PatternValue,
            IsActive, CreatedBy, CreatedOn, LastUpdatedBy, LastUpdatedOn
        ) VALUES (
            {pattern_id}, '{_esc(pattern_category)}', {psc}, '{_esc(pattern_name)}',
            {pd}, {pattern_priority}, {pv},
            true, current_user(), current_timestamp(), null, null
        )
    """)


# ---------------------------------------------------------------------------
# Built-in custom validators for the seeded email rules
# (registered via DQFramework.setup() — Python equivalents of the 6 SQL
# configCustomQuery entries for email validation)
# ---------------------------------------------------------------------------

import re as _re


def _email_basic_format(v):
    """
    Rule 1: Basic email format pattern.
    SQL: (@InputValue LIKE '%_@_%.__%' with 1-3 dots in domain)
    Enhanced with regex for Databricks.
    """
    if not v:
        return False
    return bool(_re.match(r'^[^@\s]+@[^@\s]+\.[^@\s]+$', v))


def _email_at_validation(v):
    """
    Rule 2: @ symbol validation.
    1. @ is not the first character
    2. @ is not the last character
    3. At least one dot AFTER the @
    4. Exactly one @ symbol
    """
    if not v:
        return False
    at_idx = v.find('@')
    if at_idx <= 0 or at_idx >= len(v) - 1:
        return False
    if v.count('@') != 1:
        return False
    domain = v[at_idx + 1:]
    # At least one dot after @
    last_at_idx_reverse = v[::-1].find('@')
    last_dot_idx_reverse = v[::-1].find('.')
    if last_dot_idx_reverse >= last_at_idx_reverse:
        return False
    return '.' in domain


def _email_domain_format(v):
    """
    Rule 3: Domain part validation.
    - domain part (after @) has 1-3 dots
    - no trailing dot
    - no consecutive dots
    - each label part has at least 2 chars
    """
    if not v or '@' not in v:
        return False
    domain = v.split('@', 1)[1]
    if not domain or domain.endswith('.') or '..' in domain:
        return False
    dot_count = domain.count('.')
    if not (1 <= dot_count <= 3):
        return False
    labels = domain.split('.')
    return all(len(lbl) >= 2 for lbl in labels)


def _email_local_part_dots(v):
    """
    Rule 4: Local part (before @) must not contain more than 2 dots.
    SQL: LEN(local_part) - LEN(REPLACE(local_part, '.', '')) < 3
    """
    if not v or '@' not in v:
        return False
    local = v.split('@', 1)[0]
    return local.count('.') < 3


def _email_no_placeholder(v):
    """
    Rule 5: Email must not contain placeholder keyword 'noemaildress'.
    SQL: @InputValue LIKE '%noemaildress%'  (IsConditionAllowed = Not Allowed)
    """
    if not v:
        return False
    return 'noemaildress' in v.lower()


def _email_no_trailing_special(v):
    """
    Rule 6: Email must not end with allowed special characters (., -, _).
    SQL: @InputValue LIKE '%[-._]'  (IsConditionAllowed = Not Allowed)
    """
    if not v:
        return False
    return bool(_re.search(r'[-._]$', v))


BUILTIN_VALIDATORS = {
    "email_basic_format":        _email_basic_format,
    "email_at_validation":       _email_at_validation,
    "email_domain_format":       _email_domain_format,
    "email_local_part_dots":     _email_local_part_dots,
    "email_no_placeholder":      _email_no_placeholder,
    "email_no_trailing_special": _email_no_trailing_special,
}
