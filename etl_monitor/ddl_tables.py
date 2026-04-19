"""
ETL Monitor — Delta Table DDL
==============================
All table DDL statements for the ETL Monitoring Framework.

Column names follow the original SQL Server framework naming exactly:
  TaskID, WorkFlowID, Attempts (plural), TaskMandatory, SequenceID

Constraint notes
----------------
Databricks Delta Lake does not enforce PK/FK at DDL level.
Uniqueness is enforced application-side via INSERT-ONLY MERGE statements
which key on composite natural keys — equivalent to SQL Server PK constraints.

DEFAULT values
--------------
Column DEFAULT expressions (DEFAULT current_timestamp(), DEFAULT TRUE, etc.) are NOT
used in the DDL — Databricks requires the delta.feature.allowColumnDefaults table
property to be explicitly enabled before column defaults work, which is not guaranteed
across all DBR versions and cluster configs.
All default values are passed explicitly by the Python INSERT/MERGE statements instead.

Table Classification
--------------------
FRAMEWORK-MANAGED (seeded automatically by setup() / seed_sequence_data())
  ETLconfigSequence   — 7 workflow stage definitions (LOAD_GO → PROCESS_DATA)
                        Custom stages may be added with SequenceID >= 10.

USER-MANAGED (empty after setup — populated by project team)
  ETLconfigProcess    — process / domain registry  (new vs original SQL Server)
  ETLconfigTasks      — task catalogue per process
  ETLconfigParameters — delta watermarks and config flags

RESULTS (written by instrumented notebooks/jobs at runtime)
  ETLProcessingSteps  — per-task live execution log (mutable)
  ETLsysLogs          — raw run receipts (append-only)
"""

# ---------------------------------------------------------------------------
# DDL statements — {fqn} placeholder substituted at runtime by _create_tables
# ---------------------------------------------------------------------------

DDL_STATEMENTS: dict[str, str] = {

    # -----------------------------------------------------------------------
    # ETLOrganisation — USER-MANAGED  (ported from original SQL Server framework)
    # PK: OrganisationCode  (enforced via INSERT-ONLY MERGE)
    # Top-level grouper — enterprise / division / business unit.
    # In Unity Catalog deployments the catalog is the primary isolation boundary;
    # ETLOrganisation provides an additional logical grouping layer within a catalog.
    # -----------------------------------------------------------------------
    "ETLOrganisation": """
        CREATE TABLE IF NOT EXISTS {fqn} (
            OrganisationCode        STRING     NOT NULL
                COMMENT 'Natural primary key — short code identifying the organisation or division (e.g. CORP, UK, EMEA)',
            OrganisationName        STRING     NOT NULL
                COMMENT 'Human-readable organisation name',
            OrganisationDescription STRING     NOT NULL
                COMMENT 'Full description of the organisation or business unit',
            IsActive                BOOLEAN
                COMMENT 'Soft-delete — FALSE removes organisation from new registrations without affecting history',
            CreatedOn               TIMESTAMP
                COMMENT 'Row creation timestamp',
            CreatedBy               STRING
                COMMENT 'User who created the row',
            LastUpdatedOn           TIMESTAMP
                COMMENT 'Last modification timestamp',
            LastUpdatedBy           STRING
                COMMENT 'User who last modified the row'
        )
        USING DELTA
        COMMENT 'Organisation registry — user-managed. Top-level grouper for enterprise deployments. PK: OrganisationCode.'
    """,

    # -----------------------------------------------------------------------
    # ETLconfigProject — USER-MANAGED  (ported from original SQL Server framework)
    # PK: ProjectCode  (enforced via INSERT-ONLY MERGE)
    # Mid-level grouper — project / portfolio / department within an organisation.
    # Links to ETLOrganisation via OrganisationCode (natural key, no surrogate ID).
    # Note: FactoryGUID from the original SQL Server framework is not ported —
    #       ADF factory binding is not required for the Databricks monitoring layer.
    # -----------------------------------------------------------------------
    "ETLconfigProject": """
        CREATE TABLE IF NOT EXISTS {fqn} (
            ProjectCode             STRING     NOT NULL
                COMMENT 'Natural primary key — short code for the project or department (e.g. HR, FINANCE, SUPPLY_CHAIN). FK to ETLconfigProcess.ProjectCode.',
            OrganisationCode        STRING
                COMMENT 'FK to ETLOrganisation.OrganisationCode — which org this project belongs to',
            ProjectName             STRING     NOT NULL
                COMMENT 'Human-readable project or department name',
            ProjectDescription      STRING     NOT NULL
                COMMENT 'Full description of what this project covers',
            IsActive                BOOLEAN
                COMMENT 'Soft-delete — FALSE removes project from new registrations without affecting history',
            CreatedOn               TIMESTAMP
                COMMENT 'Row creation timestamp',
            CreatedBy               STRING
                COMMENT 'User who created the row',
            LastUpdatedOn           TIMESTAMP
                COMMENT 'Last modification timestamp',
            LastUpdatedBy           STRING
                COMMENT 'User who last modified the row'
        )
        USING DELTA
        COMMENT 'Project registry — user-managed. Mid-level grouper linking an organisation to its ETL processes. PK: ProjectCode. FK: OrganisationCode → ETLOrganisation.'
    """,

    # -----------------------------------------------------------------------
    # ETLconfigSequence — FRAMEWORK-MANAGED
    # PK: SequenceID  (enforced via INSERT-ONLY MERGE)
    # 7 built-in stages seeded by setup() / seed_sequence_data()
    # All tasks sharing a SequenceID are intended to run in parallel.
    # Custom stages: SequenceID >= 10
    # -----------------------------------------------------------------------
    "ETLconfigSequence": """
        CREATE TABLE IF NOT EXISTS {fqn} (
            SequenceID          INT        NOT NULL
                COMMENT 'Primary key — 0=LOAD_GO through 6=PROCESS_DATA; custom stages use SequenceID >= 10',
            SequenceCode        STRING     NOT NULL
                COMMENT 'Short code (LOAD_GO, LOAD_DB_CONFIG, PROCESS_DATA, etc.)',
            SequenceName        STRING
                COMMENT 'Human-readable stage name',
            SequenceDescription STRING
                COMMENT 'Full description of what this stage covers',
            SortOrder           INT
                COMMENT 'Display ordering for dashboards (mirrors SequenceID by default)',
            IsActive            BOOLEAN
                COMMENT 'Soft-delete — FALSE removes stage from new runs without affecting history',
            CreatedOn           TIMESTAMP
                COMMENT 'Row creation timestamp',
            CreatedBy           STRING
                COMMENT 'User who created the row',
            LastUpdatedOn       TIMESTAMP
                COMMENT 'Last modification timestamp',
            LastUpdatedBy       STRING
                COMMENT 'User who last modified the row'
        )
        USING DELTA
        COMMENT 'Workflow stage definitions — framework-managed, seeded by setup(). Tasks sharing a SequenceID run in parallel. Custom stages: SequenceID >= 10.'
    """,

    # -----------------------------------------------------------------------
    # ETLconfigProcess — USER-MANAGED  (new vs original SQL Server framework)
    # PK: (ProjectCode, ProcessLoad)  (enforced via INSERT-ONLY MERGE)
    # -----------------------------------------------------------------------
    "ETLconfigProcess": """
        CREATE TABLE IF NOT EXISTS {fqn} (
            ProjectCode        STRING     NOT NULL
                COMMENT 'Project / portfolio code (e.g. CORP, HR, FIN)',
            ProcessLoad        STRING     NOT NULL
                COMMENT 'Process identifier within the project (e.g. HR_DAILY, FIN_MONTHLY)',
            ProcessName        STRING
                COMMENT 'Human-readable display name',
            ProcessDescription STRING
                COMMENT 'What this process loads or transforms',
            ProcessOwner       STRING
                COMMENT 'Team or individual responsible for this process',
            LoadFrequency      STRING
                COMMENT 'D=Daily  W=Weekly  M=Monthly  Y=Yearly  A=Ad-hoc',
            IsActive           BOOLEAN
                COMMENT 'Soft-delete',
            CreatedOn          TIMESTAMP
                COMMENT 'Row creation timestamp',
            CreatedBy          STRING
                COMMENT 'User who created the row',
            LastUpdatedOn      TIMESTAMP
                COMMENT 'Last modification timestamp',
            LastUpdatedBy      STRING
                COMMENT 'User who last modified the row'
        )
        USING DELTA
        COMMENT 'Central process registry — user-managed. Register each domain process here before creating tasks. PK: (ProjectCode, ProcessLoad).'
    """,

    # -----------------------------------------------------------------------
    # ETLconfigTasks — USER-MANAGED
    # PK: (TaskID, WorkFlowID, ProjectCode, ProcessLoad)  — user-assigned TaskID
    #
    # WorkFlowID semantics (identical to original SQL Server):
    #   0 = Initiation task — always TaskID=0, SequenceID=0, one per process
    #   1 = First workflow pass (main load)
    #   2 = Second pass (enrichment / second data iteration)
    #   N = Nth iteration over same data with different scope
    #
    # SequenceID — implicit parallelism:
    #   All active tasks sharing (WorkFlowID, SequenceID) for a process run in parallel.
    #   Developer designs the ADF ForEach / DBX Workflow fan-out accordingly.
    #
    # SourceSystemCode → ETLconfigParameters.ParameterName:
    #   Links a task to its delta watermark.
    #   NULL = full-load task with no delta watermark.
    # -----------------------------------------------------------------------
    "ETLconfigTasks": """
        CREATE TABLE IF NOT EXISTS {fqn} (
            TaskID                  INT        NOT NULL
                COMMENT 'User-assigned task identifier — unique within (ProjectCode, ProcessLoad, WorkFlowID). MERGE enforces uniqueness.',
            ProjectCode             STRING     NOT NULL
                COMMENT 'FK to ETLconfigProcess.ProjectCode',
            ProcessLoad             STRING     NOT NULL
                COMMENT 'FK to ETLconfigProcess.ProcessLoad',
            WorkFlowID              INT        NOT NULL
                COMMENT '0=Initiation  1=First pass  2=Second pass  N=Nth iteration',
            SequenceID              INT        NOT NULL
                COMMENT 'FK to ETLconfigSequence.SequenceID — tasks sharing SequenceID run in parallel',
            TaskName                STRING     NOT NULL
                COMMENT 'Short descriptive name for the task',
            TaskDescription         STRING
                COMMENT 'Longer description of what the task loads or transforms',
            SourceType              STRING
                COMMENT 'ADF_PIPELINE | DBX_JOB | DBX_NOTEBOOK | DATAFLOW',
            SourceIdentifier        STRING
                COMMENT 'ADF pipeline name, Databricks job_id, or notebook workspace path',
            SourceSystemCode        STRING
                COMMENT 'Links to ETLconfigParameters.ParameterName — which watermark to advance on DONE. NULL for full-load tasks.',
            FileNameMask            STRING
                COMMENT 'Base filename without date suffix for file-based source tasks (e.g. payroll_uk). NULL for non-file tasks. Combined with LoadFrequency at generate_execution_steps() time: D→_YYYYMMDD, M→_YYYYMM, Y→_YYYY.',
            FileExtension           STRING
                COMMENT 'File extension including the dot (e.g. .csv, .xlsx). Appended after the date suffix. NULL for non-file tasks.',
            InFilePath              STRING
                COMMENT 'ADLS/storage base folder path where the input file is expected (e.g. abfss://raw@store.dfs.core.windows.net/payroll/uk/). NULL for non-file tasks.',
            OutFilePath             STRING
                COMMENT 'ADLS/storage base folder path for processed or output files. NULL if no separate output location.',
            LoadFrequency           STRING
                COMMENT 'D=Daily  W=Weekly  M=Monthly  Y=Yearly  A=Ad-hoc',
            TaskMandatory           BOOLEAN
                COMMENT 'TRUE = processing must not advance past this SequenceID stage if this task FAILs. FALSE or NULL = non-mandatory (pipeline continues even if this task FAILs).',
            IsActive                BOOLEAN
                COMMENT 'FALSE = skip from new runs without deleting history',
            CreatedOn               TIMESTAMP
                COMMENT 'Row creation timestamp',
            CreatedBy               STRING
                COMMENT 'User who created the row',
            LastUpdatedOn           TIMESTAMP
                COMMENT 'Last modification timestamp',
            LastUpdatedBy           STRING
                COMMENT 'User who last modified the row'
        )
        USING DELTA
        COMMENT 'Task catalogue — user-managed. PK: (TaskID, WorkFlowID, ProjectCode, ProcessLoad). Tasks sharing SequenceID run in parallel.'
    """,

    # -----------------------------------------------------------------------
    # ETLconfigParameters — USER-MANAGED
    # PK: (ProjectCode, ProcessLoad, ParameterName)
    #
    # ParameterType enum (replaces ParameterDescription LIKE matching):
    #   DELTA_DATE  ValueDateTime — auto-advanced to task StartTime on DONE
    #   DELTA_ID    ValueINT      — NOT auto-advanced; call advance_watermark()
    #   FLAG        ValueBIT      — boolean config; read freely
    #   SYSTEM      ValueDateTime — reserved for SYSDT; via set_processing_mode() only
    #
    # ProcessLoad scoping (enhancement over original SQL Server):
    #   Allows HR_DAILY and FIN_MONTHLY to have independent watermarks under same project.
    # -----------------------------------------------------------------------
    "ETLconfigParameters": """
        CREATE TABLE IF NOT EXISTS {fqn} (
            ProjectCode          STRING     NOT NULL
                COMMENT 'FK to ETLconfigProcess.ProjectCode',
            ProcessLoad          STRING     NOT NULL
                COMMENT 'FK to ETLconfigProcess.ProcessLoad — scopes parameter to a specific process',
            ParameterName        STRING     NOT NULL
                COMMENT 'Parameter identifier — matched by ETLconfigTasks.SourceSystemCode. SYSDT reserved for system date.',
            ParameterDescription STRING
                COMMENT 'Human-readable description',
            ParameterType        STRING     NOT NULL
                COMMENT 'DELTA_DATE | DELTA_ID | FLAG | SYSTEM',
            ValueINT             BIGINT
                COMMENT 'Active for DELTA_ID. 0 = bulk mode.',
            ValueDateTime        TIMESTAMP
                COMMENT 'Active for DELTA_DATE and SYSTEM. NULL = bulk mode (DELTA_DATE) or live date (SYSTEM).',
            ValueBIT             BOOLEAN
                COMMENT 'Active for FLAG.',
            CreatedOn            TIMESTAMP
                COMMENT 'Row creation timestamp',
            CreatedBy            STRING
                COMMENT 'User who created the row',
            LastUpdatedOn        TIMESTAMP
                COMMENT 'Last modification timestamp',
            LastUpdatedBy        STRING
                COMMENT 'User who last modified the row'
        )
        USING DELTA
        COMMENT 'Delta watermarks and config flags — user-managed. ParameterType drives behaviour: DELTA_DATE auto-advances on DONE; DELTA_ID requires advance_watermark(); FLAG is free config; SYSTEM is SYSDT only.'
    """,

    # -----------------------------------------------------------------------
    # ETLProcessingSteps — RESULTS (mutable)
    # Natural key: (ProcessingDate, ProjectCode, ProcessLoad, ExecutionID,
    #               WorkFlowID, TaskID, SequenceID, Attempts)
    #
    # Snapshot columns (TaskName, SequenceCode, TaskMandatory, SourceSystemCode):
    #   Copied at generate_execution_steps() time — history accurate even if catalogue changes.
    #
    # Attempts (plural — original column name):
    #   0 = first attempt, 1 = first retry, N = Nth retry
    #
    # Initiation task (WorkFlowID=0, SequenceID=0):
    #   Overall run status indicator. Reset to NQUE on any mandatory task FAIL.
    # -----------------------------------------------------------------------
    "ETLProcessingSteps": """
        CREATE TABLE IF NOT EXISTS {fqn} (
            ProcessingDate   DATE       NOT NULL
                COMMENT 'Logical processing date (partition key)',
            ProjectCode      STRING     NOT NULL
                COMMENT 'FK to ETLconfigProcess.ProjectCode',
            ProcessLoad      STRING     NOT NULL
                COMMENT 'FK to ETLconfigProcess.ProcessLoad',
            ExecutionID      STRING     NOT NULL
                COMMENT 'UUID identifying this logical run — groups all task rows in one batch',
            WorkFlowID       INT        NOT NULL
                COMMENT 'Pass/iteration number',
            TaskID           INT        NOT NULL
                COMMENT 'FK to ETLconfigTasks.TaskID',
            SequenceID       INT        NOT NULL
                COMMENT 'FK to ETLconfigSequence.SequenceID',
            Attempts         INT        NOT NULL
                COMMENT '0=first attempt, 1=first retry, N=Nth retry',
            Status           STRING     NOT NULL
                COMMENT 'NQUE | RQUE | DONE | FAIL',
            TaskName         STRING     COMMENT 'Snapshot of ETLconfigTasks.TaskName at generate time',
            SequenceCode     STRING     COMMENT 'Snapshot of ETLconfigSequence.SequenceCode at generate time',
            TaskMandatory    BOOLEAN    COMMENT 'Snapshot of ETLconfigTasks.TaskMandatory at generate time',
            SourceSystemCode STRING     COMMENT 'Snapshot of ETLconfigTasks.SourceSystemCode at generate time',
            FullFileName     STRING     COMMENT 'Computed at generate_execution_steps() — FileNameMask + date suffix by LoadFrequency (D→_yyyyMMdd, M→_yyyyMM, Y→_yyyy) + FileExtension. NULL for non-file tasks. Mirrors FullFileName from original SQL Server framework.',
            InFilePath       STRING     COMMENT 'Snapshot of ETLconfigTasks.InFilePath — ADLS/storage base folder path for input files. NULL for non-file tasks.',
            OutFilePath      STRING     COMMENT 'Snapshot of ETLconfigTasks.OutFilePath — ADLS/storage output path. NULL if not applicable.',
            ForceSkip        BOOLEAN    COMMENT 'Run-level skip flag — TRUE = skip this task for this specific run without deactivating the task config. Default FALSE. Cleared by status_reset(). Does not carry forward to retry runs (new ExecutionID). Use IsActive in ETLconfigTasks for permanent deactivation.',
            StartTime        TIMESTAMP  COMMENT 'When this task began executing',
            EndTime          TIMESTAMP  COMMENT 'When this task finished (NULL while running)',
            DurationSeconds  INT        COMMENT 'DATEDIFF(SECOND, StartTime, EndTime)',
            LogMessage       STRING     COMMENT 'Free-text log or error message (truncated to 2000 chars)',
            LogType          STRING     COMMENT 'INFO | WARN | ERROR',
            LogCode          STRING     COMMENT 'Short error code for programmatic filtering',
            SourceType       STRING     COMMENT 'ADF_PIPELINE | DBX_JOB | DBX_NOTEBOOK | DATAFLOW',
            SourceRunID      STRING     COMMENT 'ADF ActivityRunId or Databricks run_id',
            ClusterID        STRING     COMMENT 'Databricks cluster ID — cost attribution',
            LastUpdatedOn    TIMESTAMP  COMMENT 'Last modification timestamp',
            LastUpdatedBy    STRING     COMMENT 'User who last modified the row'
        )
        USING DELTA
        PARTITIONED BY (ProcessingDate)
        COMMENT 'Per-task live execution log — mutable. One row per task per attempt. Snapshot columns preserve config at run time.'
    """,

    # -----------------------------------------------------------------------
    # ETLsysLogs — RESULTS (append-only)
    # PK: _ID GENERATED ALWAYS AS IDENTITY
    # -----------------------------------------------------------------------
    "ETLsysLogs": """
        CREATE TABLE IF NOT EXISTS {fqn} (
            _ID            BIGINT     NOT NULL  GENERATED ALWAYS AS IDENTITY
                COMMENT 'Surrogate key — auto-generated',
            ProcessingDate DATE       COMMENT 'Logical processing date (partition key)',
            ProjectCode    STRING     COMMENT 'FK to ETLconfigProcess.ProjectCode',
            ProcessLoad    STRING     COMMENT 'FK to ETLconfigProcess.ProcessLoad',
            SourceType     STRING     COMMENT 'ADF | DBX',
            RunID          STRING     COMMENT 'ADF ActivityRunId or Databricks run_id',
            ExecutionID    STRING     COMMENT 'FK to ETLProcessingSteps.ExecutionID',
            CorrelationID  STRING     COMMENT 'ADF correlation ID for multi-activity pipeline tracing',
            PipelineName   STRING     COMMENT 'ADF pipeline name or Databricks job name',
            ActivityName   STRING     COMMENT 'ADF activity name or Databricks task name',
            OperationName  STRING     COMMENT 'PipelineRun | ActivityRun | etc.',
            Level          INT        COMMENT '0=DEBUG  1=INFO  2=WARN  3=ERROR',
            LogTimeUTC     TIMESTAMP  COMMENT 'UTC timestamp of the log event',
            Message        STRING     COMMENT 'Raw log message or JSON payload'
        )
        USING DELTA
        PARTITIONED BY (ProcessingDate)
        COMMENT 'Raw run receipts — append-only.'
    """,
}

TABLE_ORDER = [
    "ETLOrganisation",      # USER-MANAGED      — top-level org / division registry
    "ETLconfigProject",     # USER-MANAGED      — mid-level project / department registry
    "ETLconfigSequence",    # FRAMEWORK-MANAGED — 7 built-in stages (auto-seeded)
    "ETLconfigProcess",     # USER-MANAGED      — process / domain registry
    "ETLconfigTasks",       # USER-MANAGED      — task catalogue per process
    "ETLconfigParameters",  # USER-MANAGED      — delta watermarks + config flags
    "ETLProcessingSteps",   # RESULTS           — per-task live execution log (mutable)
    "ETLsysLogs",           # RESULTS           — raw run receipts (append-only)
]

FRAMEWORK_SEEDED_TABLES = {"ETLconfigSequence"}
USER_CONFIG_TABLES      = {
    "ETLOrganisation",
    "ETLconfigProject",
    "ETLconfigProcess",
    "ETLconfigTasks",
    "ETLconfigParameters",
}
RESULTS_TABLES          = {"ETLProcessingSteps", "ETLsysLogs"}

# Framework-reserved SequenceID range for built-in stages
FRAMEWORK_SEQUENCE_ID_RANGE = range(0, 10)   # 0-9 reserved
# Custom/project stages should use SequenceID >= 10
USER_SEQUENCE_ID_START = 10
