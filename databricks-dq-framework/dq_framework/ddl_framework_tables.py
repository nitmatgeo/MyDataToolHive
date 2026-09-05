"""
Framework Table Definitions
============================
Delta table schemas and DDL for all 9 framework tables.

Equivalent to Script_00_DDL_Framework_Tables.sql (Databricks Delta Lake version).

Column names align exactly to the SQL Server originals.  Databricks-specific
type mappings applied:
  BIT        -> BOOLEAN
  DATETIME   -> TIMESTAMP
  NVARCHAR   -> STRING
  VARCHAR(MAX) -> STRING
  IDENTITY(1,1) -> INT NOT NULL  (user-managed tables; user specifies IDs)
  BIGINT IDENTITY -> BIGINT NOT NULL  (results tables; framework generates IDs)
  SMALLINT, TINYINT remain as-is (supported natively in Databricks)

Constraint notes
----------------
Databricks Delta Lake does not support inline PRIMARY KEY, UNIQUE, or FOREIGN KEY
enforcement via CREATE TABLE syntax on all runtimes.  All constraint intent is
preserved as SQL comments below each table definition.  Data integrity is enforced
at the application layer through INSERT-ONLY MERGE statements (see config.py and
seed_master_data.py) which key on _ID or FullFieldName, providing equivalent
uniqueness guarantees to SQL Server PK/UQ constraints.

Table Classification
--------------------
FRAMEWORK-MANAGED (seeded automatically by ``setup()`` / ``seed_master_data()``)
  masterDataCategory        -- 27 field type classifications  [auto-seeded]
  masterPattern             -- 118 built-in validation patterns [auto-seeded]
                               Users MAY add extra rows (e.g. custom keywords)
                               using _ID >= 1000 to avoid collision with
                               framework rows (_ID 1-999).

USER-MANAGED (populated by project team via ConfigManager or Spark SQL)
  masterField               -- Source fields registered for DQ assessment
  configFieldValues         -- Data length (L01) and value range (L04) boundaries per field
  configFieldAllowedPattern -- Pattern allow/block rules (L03) per field
  configCustomQuery         -- Custom Spark SQL / regex / Python validators per field (L02 rules)
  mapDQChecks               -- Maps source field definitions to target curated table columns

RESULTS (auto-populated by ``run_assessment()``)
  auditDQChecks             -- Row-level violation audit log (Result=False rows only)
  statDQChecks              -- Aggregated pass/fail statistics per field per execution

Prerequisite Columns on Curated Tables
---------------------------------------
Each curated Delta table being assessed must have these four columns added
BEFORE running the assessment.  Use dq.prepare_curated_tables() — it adds the
columns AND populates DQRowID with UUIDs.  Do not ALTER manually.

    ALTER TABLE <catalog>.<schema>.<table>
    ADD COLUMNS (
        DQRowID      STRING  COMMENT 'UUID — stable unique row identifier, MERGE join key for DQ write-back',
        DQEligible   BOOLEAN COMMENT '1=all checks passed, 0=at least one failed, NULL=not assessed',
        DQViolations STRING  COMMENT '[field: ViolationType], [field: ViolationType]',
        DQFields     STRING  COMMENT '[field1], [field2] (all fields assessed on this row)'
    );
"""

# ---------------------------------------------------------------------------
# DDL statements (Spark SQL) for each framework table
# ---------------------------------------------------------------------------

DDL_STATEMENTS = {

    # -------------------------------------------------------------------------
    # masterDataCategory
    # PK: _ID  |  UNIQUE: DataCategoryShortDescription  (enforced via MERGE)
    # -------------------------------------------------------------------------
    "masterDataCategory": """
        CREATE TABLE IF NOT EXISTS {fqn} (
            _ID                          INT        NOT NULL
                COMMENT 'Primary key — running identity value incremented by 1',
            DataCategoryType             STRING     NOT NULL
                COMMENT 'High-level data category type (e.g. STRING, NUMERIC, DATE, BOOLEAN, LOCATION)',
            DataType                     STRING
                COMMENT 'Equivalent SQL/Spark data type for this category (e.g. varchar, int, date)',
            DataCategoryShortDescription STRING     NOT NULL
                COMMENT 'Short unique label for this data category (max 30 chars; must be unique)',
            DataCategoryDescription      STRING
                COMMENT 'Human-readable description of what this data category represents',
            IsActive                     BOOLEAN    NOT NULL
                COMMENT 'TRUE = active; FALSE = inactive/soft-deleted',
            CreatedBy                    STRING     NOT NULL
                COMMENT 'User or system that created this row',
            CreatedOn                    TIMESTAMP  NOT NULL
                COMMENT 'Timestamp when this row was created',
            LastUpdatedBy                STRING
                COMMENT 'User or system that last updated this row',
            LastUpdatedOn                TIMESTAMP
                COMMENT 'Timestamp when this row was last updated'
        )
        USING DELTA
        COMMENT 'Master reference table of 27 field-type classifications used to categorise source fields for DQ assessment'
    """,

    # -------------------------------------------------------------------------
    # masterPattern
    # PK: _ID  |  UNIQUE: PatternName  (enforced via MERGE)
    # -------------------------------------------------------------------------
    "masterPattern": """
        CREATE TABLE IF NOT EXISTS {fqn} (
            _ID                 INT        NOT NULL
                COMMENT 'Primary key — running identity value incremented by 1',
            PatternName         STRING     NOT NULL
                COMMENT 'Unique name identifying this validation pattern',
            PatternCategory     STRING     NOT NULL
                COMMENT 'Top-level grouping for the pattern (e.g. DataType1, SpecialCharacter, InvalidKeyword)',
            PatternSubCategory  STRING
                COMMENT 'Optional sub-grouping within the category (e.g. Symbol, Bracket, Generic)',
            PatternPriority     INT        NOT NULL
                COMMENT 'Execution priority order within a category; lower number runs first',
            PatternDescription  STRING     NOT NULL
                COMMENT 'Human-readable description of what this pattern detects',
            PatternValue        STRING
                COMMENT 'Literal character or keyword value used by the pattern engine at evaluation time',
            IsActive            BOOLEAN    NOT NULL
                COMMENT 'TRUE = active; FALSE = inactive/soft-deleted',
            CreatedBy           STRING     NOT NULL
                COMMENT 'User or system that created this row',
            CreatedOn           TIMESTAMP  NOT NULL
                COMMENT 'Timestamp when this row was created',
            LastUpdatedBy       STRING
                COMMENT 'User or system that last updated this row',
            LastUpdatedOn       TIMESTAMP
                COMMENT 'Timestamp when this row was last updated'
        )
        USING DELTA
        COMMENT 'Master reference table of 118 built-in validation patterns across 10 categories; framework rows occupy _ID 1-999'
    """,

    # -------------------------------------------------------------------------
    # masterField
    # PK: _ID
    # FK: DataCategoryTypeID -> masterDataCategory(_ID)  (enforced via app logic)
    # -------------------------------------------------------------------------
    "masterField": """
        CREATE TABLE IF NOT EXISTS {fqn} (
            _ID                 INT        NOT NULL
                COMMENT 'Primary key — running identity value incremented by 1',
            FullFieldName       STRING     NOT NULL
                COMMENT 'Fully qualified source field name in Schema.Table.Column format',
            DataCategoryTypeID  INT        NOT NULL
                COMMENT 'FK -> masterDataCategory._ID; identifies the data category classification for this field',
            IsActive            BOOLEAN    NOT NULL
                COMMENT 'TRUE = active; FALSE = inactive/soft-deleted',
            CreatedBy           STRING     NOT NULL
                COMMENT 'User or system that created this row',
            CreatedOn           TIMESTAMP  NOT NULL
                COMMENT 'Timestamp when this row was created',
            LastUpdatedBy       STRING
                COMMENT 'User or system that last updated this row',
            LastUpdatedOn       TIMESTAMP
                COMMENT 'Timestamp when this row was last updated'
        )
        USING DELTA
        COMMENT 'Source fields registered for DQ assessment; FullFieldName = Schema.Table.Column'
    """,

    # -------------------------------------------------------------------------
    # configFieldValues
    # PK: _ID  |  UNIQUE: FullFieldName  (enforced via MERGE)
    # FK: FieldID -> masterField(_ID)  (enforced via app logic)
    # -------------------------------------------------------------------------
    "configFieldValues": """
        CREATE TABLE IF NOT EXISTS {fqn} (
            _ID             INT        NOT NULL
                COMMENT 'Primary key — running identity value incremented by 1',
            FieldID         INT        NOT NULL
                COMMENT 'FK -> masterField._ID; links this config row to its registered source field',
            FullFieldName   STRING     NOT NULL
                COMMENT 'Fully qualified source field name in Schema.Table.Column format',
            MinDataLength   INT        NOT NULL
                COMMENT 'Minimum permissible character length for this field value (L01 check)',
            MaxDataLength   INT        NOT NULL
                COMMENT 'Maximum permissible character length for this field value (L01 check)',
            MinDataValue    STRING
                COMMENT 'Minimum permissible data value for range checking (L04 check); NULL means no lower bound',
            MaxDataValue    STRING
                COMMENT 'Maximum permissible data value for range checking (L04 check); NULL means no upper bound',
            IsActive        BOOLEAN    NOT NULL
                COMMENT 'TRUE = active; FALSE = inactive/soft-deleted',
            CreatedBy       STRING     NOT NULL
                COMMENT 'User or system that created this row',
            CreatedOn       TIMESTAMP  NOT NULL
                COMMENT 'Timestamp when this row was created',
            LastUpdatedBy   STRING
                COMMENT 'User or system that last updated this row',
            LastUpdatedOn   TIMESTAMP
                COMMENT 'Timestamp when this row was last updated'
        )
        USING DELTA
        COMMENT 'Data length (L01) and value range (L04) boundaries per source field'
    """,

    # -------------------------------------------------------------------------
    # configFieldAllowedPattern
    # PK: _ID  |  UNIQUE: (FullFieldName, PatternCategory, PatternSubCategory, PatternName)  (enforced via MERGE)
    # -------------------------------------------------------------------------
    "configFieldAllowedPattern": """
        CREATE TABLE IF NOT EXISTS {fqn} (
            _ID                 INT        NOT NULL
                COMMENT 'Primary key — running identity value incremented by 1',
            FullFieldName       STRING     NOT NULL
                COMMENT 'Fully qualified source field name in Schema.Table.Column format',
            PatternCategory     STRING
                COMMENT 'Pattern category scope for this rule; NULL applies rule at field level only',
            PatternSubCategory  STRING
                COMMENT 'Pattern sub-category scope for this rule; NULL applies rule at category level',
            PatternName         STRING
                COMMENT 'Specific pattern name this rule applies to; NULL applies rule at sub-category level',
            IsPatternAllowed    BOOLEAN    NOT NULL
                COMMENT 'TRUE = pattern is permitted; FALSE = pattern is blocked',
            IsActive            BOOLEAN    NOT NULL
                COMMENT 'TRUE = active; FALSE = inactive/soft-deleted',
            CreatedBy           STRING     NOT NULL
                COMMENT 'User or system that created this row',
            CreatedOn           TIMESTAMP  NOT NULL
                COMMENT 'Timestamp when this row was created',
            LastUpdatedBy       STRING
                COMMENT 'User or system that last updated this row',
            LastUpdatedOn       TIMESTAMP
                COMMENT 'Timestamp when this row was last updated'
        )
        USING DELTA
        COMMENT 'Pattern allow/block rules (L03) per source field'
    """,

    # -------------------------------------------------------------------------
    # configCustomQuery
    # PK: _ID
    # CustomQueryType is a Databricks-specific addition (not in SQL Server original)
    # -------------------------------------------------------------------------
    "configCustomQuery": """
        CREATE TABLE IF NOT EXISTS {fqn} (
            _ID                     INT        NOT NULL
                COMMENT 'Primary key — running identity value incremented by 1',
            FullFieldName           STRING     NOT NULL
                COMMENT 'Fully qualified source field name in Schema.Table.Column format',
            CustomQuery             STRING     NOT NULL
                COMMENT 'The condition to evaluate. Use @InputValue as placeholder. Supports Spark SQL (CustomQueryType=SQL), regex (CustomQueryType=REGEX), or registered Python validator name (CustomQueryType=PYTHON).',
            CustomQueryType         STRING
                COMMENT 'Expression type: SQL, REGEX, PYTHON, or NULL for auto-detect',
            CustomQueryDescription  STRING     NOT NULL
                COMMENT 'Human-readable description of what this custom query validates',
            IsConditionAllowed      BOOLEAN    NOT NULL
                COMMENT 'TRUE = value must match (PASS); FALSE = value must NOT match (PASS)',
            IsActive                BOOLEAN    NOT NULL
                COMMENT 'TRUE = active; FALSE = inactive/soft-deleted',
            CreatedBy               STRING     NOT NULL
                COMMENT 'User or system that created this row',
            CreatedOn               TIMESTAMP  NOT NULL
                COMMENT 'Timestamp when this row was created',
            LastUpdatedBy           STRING
                COMMENT 'User or system that last updated this row',
            LastUpdatedOn           TIMESTAMP
                COMMENT 'Timestamp when this row was last updated'
        )
        USING DELTA
        COMMENT 'Custom validation rules (L02) per source field; supports Spark SQL expressions, regex patterns, and registered Python validators'
    """,

    # -------------------------------------------------------------------------
    # mapDQChecks
    # PK: _ID  |  UNIQUE: (TargetSchemaName, TargetTableName, TargetFieldName, DQFunctionSchemaName, DQFunctionName)  (enforced via MERGE)
    # TargetCatalogName is a Databricks-specific addition (not in SQL Server original)
    # -------------------------------------------------------------------------
    "mapDQChecks": """
        CREATE TABLE IF NOT EXISTS {fqn} (
            _ID                   INT        NOT NULL
                COMMENT 'Primary key — running identity value incremented by 1',
            FullFieldName         STRING     NOT NULL
                COMMENT 'Fully qualified source field name in Schema.Table.Column format',
            TargetCatalogName     STRING
                COMMENT 'Unity Catalog name of the target curated table (Databricks-specific)',
            TargetSchemaName      STRING     NOT NULL
                COMMENT 'Schema name of the target curated table',
            TargetTableName       STRING     NOT NULL
                COMMENT 'Table name of the target curated table to assess',
            TargetFieldName       STRING     NOT NULL
                COMMENT 'Column name in the target curated table to assess',
            DQFunctionSchemaName  STRING     NOT NULL
                COMMENT 'Namespace for the DQ function; mirrors the [dq] schema in SQL Server',
            DQFunctionName        STRING     NOT NULL
                COMMENT 'Name of the DQ check function to execute against TargetFieldName',
            IsActive              BOOLEAN    NOT NULL
                COMMENT 'TRUE = active; FALSE = inactive/soft-deleted',
            CreatedBy             STRING     NOT NULL
                COMMENT 'User or system that created this row',
            CreatedOn             TIMESTAMP  NOT NULL
                COMMENT 'Timestamp when this row was created',
            LastUpdatedBy         STRING
                COMMENT 'User or system that last updated this row',
            LastUpdatedOn         TIMESTAMP
                COMMENT 'Timestamp when this row was last updated'
        )
        USING DELTA
        COMMENT 'Maps source field definitions to target curated table columns and DQ check functions'
    """,

    # -------------------------------------------------------------------------
    # auditDQChecks  (append-only results table)
    # PK: _ID  GENERATED ALWAYS AS IDENTITY — Delta handles uniqueness automatically
    # FK: MappingID -> mapDQChecks(_ID)  (enforced via app logic)
    # -------------------------------------------------------------------------
    "auditDQChecks": """
        CREATE TABLE IF NOT EXISTS {fqn} (
            _ID             BIGINT     GENERATED ALWAYS AS IDENTITY
                COMMENT 'Primary key — auto-incremented by Delta; unique across all rows',
            GeneratedOn     TIMESTAMP  NOT NULL
                COMMENT 'Timestamp when this audit record was generated',
            ExecutionID     STRING
                COMMENT 'Batch/run identifier to group related audit records (UUID)',
            MappingID       INT        NOT NULL
                COMMENT 'FK -> mapDQChecks._ID; identifies which DQ mapping rule produced this record',
            InputValue      STRING
                COMMENT 'The actual field value that was assessed (NULL if value was NULL)',
            Result          BOOLEAN    NOT NULL
                COMMENT 'DQ check outcome: TRUE = passed, FALSE = failed (violation)',
            ViolationType   STRING     NOT NULL
                COMMENT 'Classification code for the type of DQ violation detected',
            LogMessage      STRING
                COMMENT 'Detailed message describing the violation or check outcome'
        )
        USING DELTA
        COMMENT 'Row-level violation audit log; only Result=FALSE (failing) rows are stored'
    """,

    # -------------------------------------------------------------------------
    # statDQChecks  (append-only results table)
    # PK: _ID  GENERATED ALWAYS AS IDENTITY — Delta handles uniqueness automatically
    # FK: MappingID -> mapDQChecks(_ID)  (enforced via app logic)
    # -------------------------------------------------------------------------
    "statDQChecks": """
        CREATE TABLE IF NOT EXISTS {fqn} (
            _ID                 BIGINT     GENERATED ALWAYS AS IDENTITY
                COMMENT 'Primary key — auto-incremented by Delta; unique across all rows',
            LoggedOn            TIMESTAMP  NOT NULL
                COMMENT 'Timestamp when this statistics record was logged',
            ExecutionID         STRING
                COMMENT 'Batch/run identifier to group related statistics records (UUID)',
            MappingID           INT        NOT NULL
                COMMENT 'FK -> mapDQChecks._ID; identifies which DQ mapping rule these statistics relate to',
            RowsQualified       INT        NOT NULL
                COMMENT 'Count of rows that passed the DQ check in this execution',
            RowsDisqualified    INT        NOT NULL
                COMMENT 'Count of rows that failed the DQ check in this execution'
        )
        USING DELTA
        COMMENT 'Aggregated pass/fail statistics per DQ mapping rule per execution batch'
    """,
}

TABLE_ORDER = [
    "masterDataCategory",
    "masterPattern",
    "masterField",
    "configFieldValues",
    "configFieldAllowedPattern",
    "configCustomQuery",
    "mapDQChecks",
    "auditDQChecks",
    "statDQChecks",
]

# Tables seeded automatically by the framework (categories + patterns).
# Users MUST NOT delete these rows; they MAY add rows with _ID >= 1000.
FRAMEWORK_SEEDED_TABLES = {"masterDataCategory", "masterPattern"}

# Tables the user (project team) must populate for their data model.
# The framework creates the table structure but inserts NO data.
USER_CONFIG_TABLES = {
    "masterField",
    "configFieldValues",
    "configFieldAllowedPattern",
    "configCustomQuery",
    "mapDQChecks",
}

# Tables written by the assessment engine — not touched by user directly.
RESULTS_TABLES = {"auditDQChecks", "statDQChecks"}

# Framework-reserved _ID range for masterPattern (do not use in user rows)
FRAMEWORK_PATTERN_ID_RANGE = range(1, 1000)   # 1-999 reserved
# Users should add custom patterns with _ID >= 1000
USER_PATTERN_ID_START = 1000
