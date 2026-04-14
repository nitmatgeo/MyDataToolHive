"""
ETLMonitorFramework — Databricks ETL monitoring & tracking
===========================================================
Centralised, enterprise-grade process orchestration metadata registry with
near-live execution tracking.  Consumed by any tool:

    ADF pipelines         → SQL Lookup / ForEach / Notebook activities
    Databricks notebooks  → Python class methods
    Databricks jobs       → Python class methods
    SQL Server consumers  → JDBC queries against Delta views
    Dataflows             → webhook write-back or notebook shim

The framework does NOT trigger or schedule jobs.  It only registers,
tracks, and reports — making it generic across any ETL technology stack.

Architecture mirrors DQFramework (dq_framework/framework.py):
  setup()              → _verify_catalog → _create_schema
                         → _create_tables → _create_reporting_views
                         → seed_sequence_data
  _fqn(name)           → backtick-quoted fully-qualified name
  DDL_STATEMENTS dict  → all table DDL (in ddl_tables.py)
  TABLE_ORDER list     → creation order respecting dependencies
  INSERT-ONLY MERGE    → all config writes (idempotent, never overwrites)

PK / FK enforcement:
  Delta Lake does not enforce PK/FK at DDL level.
  Uniqueness is enforced application-side via MERGE ON composite keys.

ParameterType in ETLconfigParameters:
  DELTA_DATE  date/timestamp watermark — ValueDateTime
              auto-advanced to task StartTime on DONE via end_task()
  DELTA_ID    numeric ID watermark — ValueINT
              NOT auto-advanced — developer calls advance_watermark()
              *** KNOWN LIMITATION — framework cannot detect source delta column ***
  FLAG        boolean config flag — ValueBIT
  SYSTEM      system params (SYSDT) — ValueDateTime
              NULL = live date, past date = historic mode

Audit columns on all user-managed tables:
  CreatedOn, CreatedBy    — set once at INSERT via INSERT-ONLY MERGE
  LastUpdatedOn, LastUpdatedBy — updated on every MERGE/UPDATE

Enterprise / ADF consumption:
  v_watermarks.ActiveValue → ADF Lookup activity bridge (single STRING)
  ADF expression: @activity('LookupWatermark').output.firstRow.ActiveValue
"""

from __future__ import annotations

import logging
import os
import shutil
import uuid
from contextlib import contextmanager
from typing import Optional, Union

from etl_monitor.ddl_tables import (
    DDL_STATEMENTS,
    FRAMEWORK_SEEDED_TABLES,
    RESULTS_TABLES,
    TABLE_ORDER,
    USER_CONFIG_TABLES,
)
from etl_monitor.seed_data import SEQUENCE_SEED

logger = logging.getLogger(__name__)


class ETLMonitorFramework:
    """
    Centralised ETL process orchestration metadata registry and execution tracker.

    Self-creating, idempotent, enterprise-generic.  Consumed by ADF, Databricks
    notebooks, Databricks jobs, SQL Server (via JDBC), or any ODBC-capable tool.

    Parameters
    ----------
    spark
        Active SparkSession.
    catalog
        Unity Catalog name.  Pass ``""`` for legacy Hive metastore.
    schema
        Schema name for all framework tables.  Default: ``"etl"``.
        Kept separate from the DQ framework's ``"dq"`` schema.
    """

    def __init__(self, spark, catalog: str = "", schema: str = "etl"):
        self.spark      = spark
        self.catalog    = catalog
        self.etl_schema = schema

    # ------------------------------------------------------------------
    # Public — setup  (mirrors DQFramework.setup())
    # ------------------------------------------------------------------

    def setup(self, seed_data: bool = True) -> None:
        """
        Create schema, Delta tables, reporting views, and seed sequence data.
        Fully idempotent — safe to call on every cluster start.

        Table classification after setup
        ---------------------------------
        FRAMEWORK-MANAGED (auto-seeded):
            ETLconfigSequence   — 7 workflow stage definitions

        USER-MANAGED (empty, populate via register_* helpers or Spark SQL):
            ETLOrganisation     — top-level org / division registry
            ETLconfigProject    — mid-level project / department registry
            ETLconfigProcess    — process registry
            ETLconfigTasks      — task catalogue per process
            ETLconfigParameters — delta watermarks and config flags

        RESULTS (written by instrumented jobs/notebooks at runtime):
            ETLProcessingSteps  — mutable per-task execution log
            ETLsysLogs          — append-only raw run receipts

        Raises
        ------
        RuntimeError
            If ``catalog`` is specified but does not exist in Unity Catalog.
        """
        self._verify_catalog()
        self._create_schema()
        self._create_tables()
        self._create_reporting_views()
        if seed_data:
            self.seed_sequence_data()
            logger.info(
                "ETL Monitor setup complete. ETLconfigSequence seeded (%d stages). "
                "Populate ETLOrganisation via register_organisation(), ETLconfigProject "
                "via register_project(), ETLconfigProcess via register_process(), "
                "ETLconfigTasks via register_task(), ETLconfigParameters via register_parameter().",
                len(SEQUENCE_SEED),
            )

    def seed_sequence_data(self) -> None:
        """
        Seed 7 built-in workflow stages into ETLconfigSequence.
        INSERT-ONLY MERGE — never overwrites. Custom stages: SequenceID >= 10.
        """
        fqn      = self._fqn("ETLconfigSequence")
        rows_sql = ",\n            ".join(
            f"({sid}, '{code}', '{name}', '{desc}', {sort})"
            for sid, code, name, desc, sort in SEQUENCE_SEED
        )
        self.spark.sql(f"""
            MERGE INTO {fqn} AS tgt
            USING (
                SELECT col1 AS SequenceID, col2 AS SequenceCode,
                       col3 AS SequenceName, col4 AS SequenceDescription,
                       col5 AS SortOrder
                FROM VALUES {rows_sql}
            ) AS src
            ON tgt.SequenceID = src.SequenceID
            WHEN NOT MATCHED THEN INSERT (
                SequenceID, SequenceCode, SequenceName, SequenceDescription,
                SortOrder, IsActive, CreatedOn, CreatedBy, LastUpdatedOn, LastUpdatedBy
            ) VALUES (
                src.SequenceID, src.SequenceCode, src.SequenceName,
                src.SequenceDescription, src.SortOrder, TRUE,
                current_timestamp(), current_user(),
                current_timestamp(), current_user()
            )
        """)
        logger.info("ETLconfigSequence seeded: %d stages.", len(SEQUENCE_SEED))

    # ------------------------------------------------------------------
    # Public — organisation / project / process / task / parameter registration
    # ------------------------------------------------------------------

    def register_organisation(
        self,
        organisation_code: str,
        organisation_name: str,
        organisation_description: str = "",
    ) -> "ETLMonitorFramework":
        """
        Register (or update) an organisation in ETLOrganisation.

        **Mandatory parameters**
        - ``organisation_code``        — natural PK, short code e.g. ``"CORP"``, ``"UK"``, ``"EMEA"``
        - ``organisation_name``        — human-readable name e.g. ``"Corporate Division"``

        **Optional parameters**
        - ``organisation_description`` — full description (default: ``""``)

        MERGE behaviour:
          - INSERT on first call (new OrganisationCode).
          - UPDATE OrganisationName and OrganisationDescription on subsequent calls if changed.
          - CreatedOn / CreatedBy are never overwritten.

        Returns self for optional method chaining.
        """
        fqn = self._fqn("ETLOrganisation")
        self.spark.sql(f"""
            MERGE INTO {fqn} AS tgt
            USING (SELECT
                '{organisation_code}'        AS OrganisationCode,
                '{organisation_name}'        AS OrganisationName,
                '{organisation_description}' AS OrganisationDescription
            ) AS src
            ON COALESCE(tgt.OrganisationCode,'') = COALESCE(src.OrganisationCode,'')
            WHEN MATCHED AND (
                COALESCE(tgt.OrganisationName,        '') <> COALESCE(src.OrganisationName,        '') OR
                COALESCE(tgt.OrganisationDescription, '') <> COALESCE(src.OrganisationDescription, '')
            ) THEN UPDATE SET
                tgt.OrganisationName        = src.OrganisationName,
                tgt.OrganisationDescription = src.OrganisationDescription,
                tgt.LastUpdatedOn           = current_timestamp(),
                tgt.LastUpdatedBy           = current_user()
            WHEN NOT MATCHED THEN INSERT (
                OrganisationCode, OrganisationName, OrganisationDescription,
                IsActive, CreatedOn, CreatedBy, LastUpdatedOn, LastUpdatedBy
            ) VALUES (
                src.OrganisationCode, src.OrganisationName, src.OrganisationDescription,
                TRUE, current_timestamp(), current_user(),
                current_timestamp(), current_user()
            )
        """)
        logger.info("Organisation registered: %s", organisation_code)
        return self

    def register_project(
        self,
        project_code: str,
        project_name: str,
        project_description: str = "",
        organisation_code: str = "",
    ) -> "ETLMonitorFramework":
        """
        Register (or update) a project in ETLconfigProject.

        **Mandatory parameters**
        - ``project_code``        — natural PK, short code e.g. ``"HR"``, ``"FINANCE"``
        - ``project_name``        — human-readable name e.g. ``"HR Data Platform"``

        **Optional parameters**
        - ``project_description`` — full description (default: ``""``)
        - ``organisation_code``   — FK to ETLOrganisation.OrganisationCode (default: ``""``)

        MERGE behaviour:
          - INSERT on first call (new ProjectCode).
          - UPDATE ProjectName, ProjectDescription, OrganisationCode on subsequent calls if changed.
          - CreatedOn / CreatedBy are never overwritten.

        Returns self for optional method chaining.
        """
        fqn     = self._fqn("ETLconfigProject")
        org_sql = f"'{organisation_code}'" if organisation_code else "NULL"
        self.spark.sql(f"""
            MERGE INTO {fqn} AS tgt
            USING (SELECT
                '{project_code}'        AS ProjectCode,
                {org_sql}               AS OrganisationCode,
                '{project_name}'        AS ProjectName,
                '{project_description}' AS ProjectDescription
            ) AS src
            ON COALESCE(tgt.ProjectCode,'') = COALESCE(src.ProjectCode,'')
            WHEN MATCHED AND (
                COALESCE(tgt.OrganisationCode,   '') <> COALESCE(src.OrganisationCode,   '') OR
                COALESCE(tgt.ProjectName,        '') <> COALESCE(src.ProjectName,        '') OR
                COALESCE(tgt.ProjectDescription, '') <> COALESCE(src.ProjectDescription, '')
            ) THEN UPDATE SET
                tgt.OrganisationCode   = src.OrganisationCode,
                tgt.ProjectName        = src.ProjectName,
                tgt.ProjectDescription = src.ProjectDescription,
                tgt.LastUpdatedOn      = current_timestamp(),
                tgt.LastUpdatedBy      = current_user()
            WHEN NOT MATCHED THEN INSERT (
                ProjectCode, OrganisationCode, ProjectName, ProjectDescription,
                IsActive, CreatedOn, CreatedBy, LastUpdatedOn, LastUpdatedBy
            ) VALUES (
                src.ProjectCode, src.OrganisationCode, src.ProjectName,
                src.ProjectDescription, TRUE, current_timestamp(), current_user(),
                current_timestamp(), current_user()
            )
        """)
        logger.info("Project registered: %s (org: %s)", project_code, organisation_code or "—")
        return self

    def register_process(
        self,
        project_code: str,
        process_load: str,
        name: str = "",
        description: str = "",
        owner: str = "",
        load_frequency: str = "D",
    ) -> "ETLMonitorFramework":
        """
        Register (or update) a domain process in ETLconfigProcess.

        **Mandatory parameters**
        - ``project_code``  — project / portfolio code, e.g. ``"CORP"``
        - ``process_load``  — process identifier, e.g. ``"HR_DAILY"``

        **Optional parameters**
        - ``name``           — human-readable display name (default: ``""``)
        - ``description``    — what this process loads or transforms (default: ``""``)
        - ``owner``          — team or individual responsible (default: ``""``)
        - ``load_frequency`` — ``"D"`` Daily, ``"W"`` Weekly, ``"M"`` Monthly, ``"A"`` Ad-hoc (default: ``"D"``)

        MERGE behaviour:
          - INSERT on first call (new ProjectCode/ProcessLoad).
          - UPDATE mutable fields (ProcessName, ProcessDescription, ProcessOwner,
            LoadFrequency) on subsequent calls if any value changed.
          - CreatedOn / CreatedBy are never overwritten.

        Returns self for optional method chaining.
        """
        fqn = self._fqn("ETLconfigProcess")
        self.spark.sql(f"""
            MERGE INTO {fqn} AS tgt
            USING (SELECT
                '{project_code}'   AS ProjectCode,
                '{process_load}'   AS ProcessLoad,
                '{name}'           AS ProcessName,
                '{description}'    AS ProcessDescription,
                '{owner}'          AS ProcessOwner,
                '{load_frequency}' AS LoadFrequency
            ) AS src
            ON  COALESCE(tgt.ProjectCode,'') = COALESCE(src.ProjectCode,'')
            AND COALESCE(tgt.ProcessLoad, '') = COALESCE(src.ProcessLoad, '')
            WHEN MATCHED AND (
                COALESCE(tgt.ProcessName,        '') <> COALESCE(src.ProcessName,        '') OR
                COALESCE(tgt.ProcessDescription, '') <> COALESCE(src.ProcessDescription, '') OR
                COALESCE(tgt.ProcessOwner,       '') <> COALESCE(src.ProcessOwner,       '') OR
                COALESCE(tgt.LoadFrequency,      '') <> COALESCE(src.LoadFrequency,      '')
            ) THEN UPDATE SET
                tgt.ProcessName        = src.ProcessName,
                tgt.ProcessDescription = src.ProcessDescription,
                tgt.ProcessOwner       = src.ProcessOwner,
                tgt.LoadFrequency      = src.LoadFrequency,
                tgt.LastUpdatedOn      = current_timestamp(),
                tgt.LastUpdatedBy      = current_user()
            WHEN NOT MATCHED THEN INSERT (
                ProjectCode, ProcessLoad, ProcessName, ProcessDescription,
                ProcessOwner, LoadFrequency, IsActive,
                CreatedOn, CreatedBy, LastUpdatedOn, LastUpdatedBy
            ) VALUES (
                src.ProjectCode, src.ProcessLoad, src.ProcessName,
                src.ProcessDescription, src.ProcessOwner, src.LoadFrequency,
                TRUE, current_timestamp(), current_user(),
                current_timestamp(), current_user()
            )
        """)
        logger.info("Process registered: %s / %s", project_code, process_load)
        return self

    def register_task(
        self,
        project_code: str,
        process_load: str,
        task_id: int,
        workflow_id: int,
        sequence_id: int,
        task_name: str,
        source_type: str = "",
        source_identifier: str = "",
        source_system_code: str = "",
        task_description: str = "",
        load_frequency: str = "D",
        task_mandatory: bool = True,
    ) -> "ETLMonitorFramework":
        """
        Register (or update) a task in ETLconfigTasks.

        **Mandatory parameters**
        - ``project_code``  — FK to ETLconfigProcess.ProjectCode, e.g. ``"CORP"``
        - ``process_load``  — FK to ETLconfigProcess.ProcessLoad, e.g. ``"HR_DAILY"``
        - ``task_id``       — **user-assigned integer** — you control the ID value.
                              Unique within (ProjectCode, ProcessLoad, WorkFlowID).
                              Initiation task is always task_id=0.
        - ``workflow_id``   — 0=Initiation, 1=First pass, 2=Second pass, N=Nth pass
        - ``sequence_id``   — FK to ETLconfigSequence.SequenceID.
                              Tasks sharing the same SequenceID run in parallel.
                              0=LOAD_GO, 1=LOAD_DB_CONFIG, 2=LOAD_DB_TRAN,
                              3=LOAD_DIM, 4=LOAD_TRAN, 5=PRE_PROCESS, 6=PROCESS_DATA.
                              Custom stages: SequenceID >= 10.
        - ``task_name``     — short descriptive name

        **Optional parameters**
        - ``source_type``               — ``"DBX_NOTEBOOK"`` | ``"DBX_JOB"`` | ``"ADF_PIPELINE"`` | ``"DATAFLOW"`` (default: ``""``)
        - ``source_identifier``         — notebook path, job ID, or ADF pipeline name (default: ``""``)
        - ``source_system_code``        — matches ETLconfigParameters.ParameterName for watermark advance.
                                          Leave empty (``""``) for full-load tasks with no delta watermark. (default: ``""``)
        - ``task_description``          — longer description (default: ``""``)
        - ``load_frequency``            — ``"D"`` | ``"W"`` | ``"M"`` | ``"Y"`` | ``"A"`` (default: ``"D"``)
        - ``task_mandatory``            — if True, a FAIL blocks downstream SequenceID stages (default: ``True``)

        MERGE behaviour:
          - INSERT on first call (new composite key).
          - UPDATE mutable fields on subsequent calls if any value changed.
          - TaskID, WorkFlowID, ProjectCode, ProcessLoad, CreatedOn, CreatedBy never overwritten.

        Returns self for optional method chaining.
        """
        fqn     = self._fqn("ETLconfigTasks")
        ssc_sql = f"'{source_system_code}'" if source_system_code else "NULL"

        self.spark.sql(f"""
            MERGE INTO {fqn} AS tgt
            USING (SELECT
                {task_id}              AS TaskID,
                '{project_code}'       AS ProjectCode,
                '{process_load}'       AS ProcessLoad,
                {workflow_id}          AS WorkFlowID,
                {sequence_id}          AS SequenceID,
                '{task_name}'          AS TaskName,
                '{task_description}'   AS TaskDescription,
                '{source_type}'        AS SourceType,
                '{source_identifier}'  AS SourceIdentifier,
                {ssc_sql}              AS SourceSystemCode,
                '{load_frequency}'     AS LoadFrequency,
                {str(task_mandatory).upper()}  AS TaskMandatory
            ) AS src
            ON  tgt.TaskID       = src.TaskID
            AND tgt.WorkFlowID   = src.WorkFlowID
            AND COALESCE(tgt.ProjectCode,'') = COALESCE(src.ProjectCode,'')
            AND COALESCE(tgt.ProcessLoad, '') = COALESCE(src.ProcessLoad, '')
            WHEN MATCHED AND (
                COALESCE(tgt.SequenceID,              -1) <> COALESCE(src.SequenceID,              -1) OR
                COALESCE(tgt.TaskName,                '') <> COALESCE(src.TaskName,                '') OR
                COALESCE(tgt.TaskDescription,         '') <> COALESCE(src.TaskDescription,         '') OR
                COALESCE(tgt.SourceType,              '') <> COALESCE(src.SourceType,              '') OR
                COALESCE(tgt.SourceIdentifier,        '') <> COALESCE(src.SourceIdentifier,        '') OR
                COALESCE(tgt.SourceSystemCode,        '') <> COALESCE(src.SourceSystemCode,        '') OR
                COALESCE(tgt.LoadFrequency,           '') <> COALESCE(src.LoadFrequency,           '') OR
                COALESCE(tgt.TaskMandatory,        FALSE) <> COALESCE(src.TaskMandatory,        FALSE)
            ) THEN UPDATE SET
                tgt.SequenceID              = src.SequenceID,
                tgt.TaskName                = src.TaskName,
                tgt.TaskDescription         = src.TaskDescription,
                tgt.SourceType              = src.SourceType,
                tgt.SourceIdentifier        = src.SourceIdentifier,
                tgt.SourceSystemCode        = src.SourceSystemCode,
                tgt.LoadFrequency           = src.LoadFrequency,
                tgt.TaskMandatory           = src.TaskMandatory,
                tgt.LastUpdatedOn           = current_timestamp(),
                tgt.LastUpdatedBy           = current_user()
            WHEN NOT MATCHED THEN INSERT (
                TaskID, ProjectCode, ProcessLoad, WorkFlowID, SequenceID,
                TaskName, TaskDescription, SourceType, SourceIdentifier,
                SourceSystemCode, LoadFrequency, TaskMandatory, IsActive,
                CreatedOn, CreatedBy, LastUpdatedOn, LastUpdatedBy
            ) VALUES (
                src.TaskID, src.ProjectCode, src.ProcessLoad, src.WorkFlowID,
                src.SequenceID, src.TaskName, src.TaskDescription,
                src.SourceType, src.SourceIdentifier, src.SourceSystemCode,
                src.LoadFrequency, src.TaskMandatory, TRUE,
                current_timestamp(), current_user(),
                current_timestamp(), current_user()
            )
        """)
        logger.info("Task registered: %s/%s WF=%d T=%d — %s",
                    project_code, process_load, workflow_id, task_id, task_name)
        return self

    def register_parameter(
        self,
        project_code: str,
        process_load: str,
        parameter_name: str,
        parameter_type: str,
        description: str = "",
        value_datetime: Optional[str] = None,
        value_int: Optional[int] = None,
        value_bit: Optional[bool] = None,
    ) -> "ETLMonitorFramework":
        """
        Register (or update) a delta watermark or config parameter in ETLconfigParameters.

        **Mandatory parameters**
        - ``project_code``    — FK to ETLconfigProcess.ProjectCode
        - ``process_load``    — FK to ETLconfigProcess.ProcessLoad
        - ``parameter_name``  — parameter identifier; must match ETLconfigTasks.SourceSystemCode
                                for DELTA_DATE/DELTA_ID. Use ``"SYSDT"`` for the system date parameter.
        - ``parameter_type``  — one of:

            ``"DELTA_DATE"``  ValueDateTime watermark — auto-advanced to task StartTime on DONE.
                              Pass ``value_datetime="2026-01-01"`` to set an initial watermark,
                              or omit (None) to start in bulk mode (full load).

            ``"DELTA_ID"``    ValueINT watermark — NOT auto-advanced. Developer must call
                              advance_watermark() after load. Pass ``value_int=0`` for bulk start.
                              KNOWN LIMITATION: framework cannot detect source max ID automatically.

            ``"FLAG"``        ValueBIT boolean config — read freely by notebooks.
                              Pass ``value_bit=True`` or ``value_bit=False``.

            ``"SYSTEM"``      Reserved for SYSDT. Controlled via set_processing_mode() only.
                              Do not set value directly — leave all value params as None.

        **Optional parameters**
        - ``description``     — human-readable description (default: ``""``)
        - ``value_datetime``  — initial TIMESTAMP value as string, e.g. ``"2026-01-01"`` (default: ``None``)
        - ``value_int``       — initial BIGINT value, e.g. ``0`` for bulk start (default: ``None``)
        - ``value_bit``       — initial BOOLEAN value (default: ``None``)

        MERGE behaviour:
          - INSERT on first call (new ProjectCode/ProcessLoad/ParameterName).
          - UPDATE ParameterDescription on subsequent calls if changed.
          - Value columns (ValueDateTime, ValueINT, ValueBIT) are NOT overwritten on update
            — use advance_watermark() or set_processing_mode() to change watermark values.
          - ParameterType, CreatedOn, CreatedBy never overwritten.

        Returns self for optional method chaining.
        """
        valid_types = {"DELTA_DATE", "DELTA_ID", "FLAG", "SYSTEM"}
        if parameter_type not in valid_types:
            raise ValueError(
                f"parameter_type must be one of {valid_types}, got '{parameter_type}'"
            )

        fqn     = self._fqn("ETLconfigParameters")
        dt_sql  = f"'{value_datetime}'"   if value_datetime is not None else "NULL"
        int_sql = str(value_int)          if value_int      is not None else "NULL"
        bit_sql = str(value_bit).upper()  if value_bit      is not None else "NULL"

        self.spark.sql(f"""
            MERGE INTO {fqn} AS tgt
            USING (SELECT
                '{project_code}'    AS ProjectCode,
                '{process_load}'    AS ProcessLoad,
                '{parameter_name}'  AS ParameterName,
                '{description}'     AS ParameterDescription,
                '{parameter_type}'  AS ParameterType,
                {dt_sql}            AS ValueDateTime,
                {int_sql}           AS ValueINT,
                {bit_sql}           AS ValueBIT
            ) AS src
            ON  COALESCE(tgt.ProjectCode,  '') = COALESCE(src.ProjectCode,  '')
            AND COALESCE(tgt.ProcessLoad,  '') = COALESCE(src.ProcessLoad,  '')
            AND COALESCE(tgt.ParameterName,'') = COALESCE(src.ParameterName,'')
            WHEN MATCHED AND (
                COALESCE(tgt.ParameterDescription,'') <> COALESCE(src.ParameterDescription,'')
            ) THEN UPDATE SET
                tgt.ParameterDescription = src.ParameterDescription,
                tgt.LastUpdatedOn        = current_timestamp(),
                tgt.LastUpdatedBy        = current_user()
            WHEN NOT MATCHED THEN INSERT (
                ProjectCode, ProcessLoad, ParameterName, ParameterDescription,
                ParameterType, ValueDateTime, ValueINT, ValueBIT,
                CreatedOn, CreatedBy, LastUpdatedOn, LastUpdatedBy
            ) VALUES (
                src.ProjectCode, src.ProcessLoad, src.ParameterName,
                src.ParameterDescription, src.ParameterType,
                src.ValueDateTime, src.ValueINT, src.ValueBIT,
                current_timestamp(), current_user(),
                current_timestamp(), current_user()
            )
        """)
        logger.info("Parameter registered: %s/%s — %s (%s)",
                    project_code, process_load, parameter_name, parameter_type)
        return self

    # ------------------------------------------------------------------
    # Public — execution step generation
    # Equivalent to p_ETLProcessingSteps (GenerateMode=1)
    # ------------------------------------------------------------------

    def generate_execution_steps(
        self,
        execution_id: str,
        project_code: str,
        process_load: str,
        processing_date: str,
    ) -> None:
        """
        Generate NQUE rows in ETLProcessingSteps for all active tasks.
        Mirrors ``p_ETLProcessingSteps`` from the original SQL Server framework.

        **First run (no rows for this date+process):**
        Inserts all active tasks as NQUE with Attempts=0.

        **Same ExecutionID called again:**
        Idempotent — rows already exist for this ExecutionID, returns immediately.

        **New ExecutionID on the same processing date (retry run — e.g. ADF re-trigger):**
        Increments Attempts = MAX(Attempts across all rows for this date+process) + 1.
        Inserts ONLY tasks that do NOT already have a DONE row at a lower Attempts level.
        Tasks completed (DONE) on a previous attempt are carried forward without re-insertion.
        FAIL/NQUE tasks get new NQUE rows at the higher Attempts level.

        This mirrors the ADF pattern where each pipeline re-trigger produces a new RunID
        (ExecutionID). The Attempts counter is a date-level retry counter, not per-execution.
        """
        steps = self._fqn("ETLProcessingSteps")
        tasks = self._fqn("ETLconfigTasks")
        seqs  = self._fqn("ETLconfigSequence")

        # Idempotency — if rows already exist for THIS ExecutionID, skip entirely
        same_exec = self.spark.sql(f"""
            SELECT COUNT(*) AS n FROM {steps}
            WHERE ExecutionID    = '{execution_id}'
              AND ProcessingDate = '{processing_date}'
              AND ProjectCode    = '{project_code}'
              AND ProcessLoad    = '{process_load}'
        """).collect()[0]["n"]

        if same_exec > 0:
            logger.info("Steps already exist for exec=%s — skipping generation", execution_id)
            return

        # Iteration = MAX(Attempts for this date+process) + 1.
        # First run → no rows exist → COALESCE(-1)+1 = 0.
        # Retry run  → previous rows exist → next Attempts level.
        row = self.spark.sql(f"""
            SELECT COALESCE(MAX(Attempts), -1) + 1 AS NextAttempts
            FROM {steps}
            WHERE ProcessingDate = '{processing_date}'
              AND ProjectCode    = '{project_code}'
              AND ProcessLoad    = '{process_load}'
        """).collect()[0]
        iteration = int(row["NextAttempts"])

        # Insert rows for tasks that have NOT already succeeded (DONE at a previous Attempts).
        # For Attempts=0: Attempts < 0 is always false → all tasks inserted.
        # For Attempts=N (retry): tasks with DONE at Attempts < N are skipped.
        self.spark.sql(f"""
            INSERT INTO {steps}
            (ProcessingDate, ProjectCode, ProcessLoad, ExecutionID,
             WorkFlowID, TaskID, SequenceID, Attempts, Status,
             TaskName, SequenceCode, TaskMandatory, SourceSystemCode,
             StartTime, LastUpdatedOn, LastUpdatedBy)
            SELECT
                '{processing_date}', t.ProjectCode, t.ProcessLoad, '{execution_id}',
                t.WorkFlowID, t.TaskID, t.SequenceID, {iteration}, 'NQUE',
                t.TaskName, s.SequenceCode, t.TaskMandatory, t.SourceSystemCode,
                current_timestamp(), current_timestamp(), current_user()
            FROM {tasks} t
            LEFT JOIN {seqs} s ON t.SequenceID = s.SequenceID
            WHERE t.ProjectCode = '{project_code}'
              AND t.ProcessLoad = '{process_load}'
              AND t.IsActive    = TRUE
              AND NOT EXISTS (
                SELECT 1 FROM {steps} e
                WHERE e.Status        = 'DONE'
                  AND e.Attempts      < {iteration}
                  AND e.ProcessingDate = '{processing_date}'
                  AND e.ProjectCode   = '{project_code}'
                  AND e.ProcessLoad   = '{process_load}'
                  AND e.WorkFlowID    = t.WorkFlowID
                  AND e.TaskID        = t.TaskID
                  AND e.SequenceID    = t.SequenceID
              )
        """)
        logger.info("Execution steps generated for %s/%s exec=%s date=%s attempts=%d",
                    project_code, process_load, execution_id, processing_date, iteration)

    # ------------------------------------------------------------------
    # Public — get pending tasks
    # Equivalent to p_ETLOrchestrationSteps
    # ------------------------------------------------------------------

    def get_pending_tasks(
        self,
        execution_id: str,
        project_code: str,
        process_load: str,
        processing_date: Optional[str] = None,
        sequence_id: Optional[int] = None,
        workflow_id: Optional[int] = None,
    ):
        """
        Return all non-DONE tasks for this run ordered by WorkFlowID / SequenceID / TaskID.
        Auto-generates steps if none exist yet (mirrors p_ETLOrchestrationSteps first-call behaviour).
        Tasks sharing a SequenceID should be dispatched in parallel by the orchestrator.

        **execution_id** options:
          - ADF pipeline run ID:  pass ``pipeline().RunId`` from ADF expression via widget.
            ``execution_id = dbutils.widgets.get("execution_id")``
          - Databricks-generated: use ``ETLMonitorFramework.generate_execution_id()`` when
            ADF is not orchestrating (e.g. direct DBX Workflow runs or ad-hoc notebook runs).

        **processing_date** — defaults to today's date (``current_date()``) if not supplied.
        """
        from datetime import date as _date
        if processing_date is None:
            processing_date = str(_date.today())

        steps = self._fqn("ETLProcessingSteps")

        count = self.spark.sql(f"""
            SELECT COUNT(*) AS n FROM {steps}
            WHERE ExecutionID = '{execution_id}'
        """).collect()[0]["n"]

        if count == 0:
            self.generate_execution_steps(
                execution_id, project_code, process_load, processing_date
            )

        seq_filt = f"AND SequenceID = {sequence_id}" if sequence_id is not None else ""
        wf_filt  = f"AND WorkFlowID = {workflow_id}"  if workflow_id  is not None else ""

        return self.spark.sql(f"""
            SELECT
                Status, ExecutionID, ProjectCode, ProcessLoad,
                WorkFlowID, SequenceID, TaskID,
                TaskName, SequenceCode, TaskMandatory, SourceSystemCode,
                SourceType, Attempts, StartTime
            FROM {steps}
            WHERE ExecutionID    = '{execution_id}'
              AND ProcessingDate = '{processing_date}'
              AND ProjectCode    = '{project_code}'
              AND ProcessLoad    = '{process_load}'
              AND Status        != 'DONE'
              {seq_filt} {wf_filt}
            ORDER BY WorkFlowID, SequenceID, TaskID
        """)

    # ------------------------------------------------------------------
    # Public — task write-back
    # Equivalent to p_ETLProcessingStatusUpdate
    # ------------------------------------------------------------------

    def start_task(
        self,
        execution_id: str,
        project_code: str,
        process_load: str,
        task_id: int,
        sequence_id: int,
        workflow_id: int,
        processing_date: str,
        attempts: Optional[int] = None,
        source_type: str = "DBX_NOTEBOOK",
        source_run_id: Optional[str] = None,
    ) -> None:
        """Record task start — MERGE into ETLProcessingSteps.

        ``attempts`` is auto-detected from the existing NQUE/RQUE row for this task when
        not supplied. This handles retry runs (new ExecutionID, Attempts > 0) transparently
        — callers never need to track the Attempts level themselves.
        """
        steps = self._fqn("ETLProcessingSteps")

        if attempts is None:
            # Look up the Attempts level from the NQUE/RQUE row generated by generate_execution_steps.
            # For the first run this will be 0; for retry runs (new ExecutionID, same date) it will be 1+.
            rows = self.spark.sql(f"""
                SELECT Attempts FROM {steps}
                WHERE ExecutionID    = '{execution_id}'
                  AND ProcessingDate = '{processing_date}'
                  AND ProjectCode    = '{project_code}'
                  AND ProcessLoad    = '{process_load}'
                  AND WorkFlowID     = {workflow_id}
                  AND TaskID         = {task_id}
                  AND SequenceID     = {sequence_id}
                  AND Status IN ('NQUE', 'RQUE')
                LIMIT 1
            """).collect()
            attempts = int(rows[0]["Attempts"]) if rows else 0

        status     = "RQUE" if attempts > 0 else "NQUE"
        cluster_id = self._get_cluster_id()
        run_id     = source_run_id or self._get_run_id()
        tasks      = self._fqn("ETLconfigTasks")
        seqs       = self._fqn("ETLconfigSequence")

        self.spark.sql(f"""
            MERGE INTO {steps} AS tgt
            USING (
                SELECT
                    '{processing_date}' AS ProcessingDate,
                    '{project_code}'    AS ProjectCode,
                    '{process_load}'    AS ProcessLoad,
                    '{execution_id}'    AS ExecutionID,
                    {workflow_id}       AS WorkFlowID,
                    {task_id}           AS TaskID,
                    {sequence_id}       AS SequenceID,
                    {attempts}          AS Attempts,
                    '{status}'          AS Status,
                    t.TaskName          AS TaskName,
                    s.SequenceCode      AS SequenceCode,
                    t.TaskMandatory     AS TaskMandatory,
                    t.SourceSystemCode  AS SourceSystemCode,
                    '{source_type}'     AS SourceType,
                    {f"'{run_id}'"      if run_id     else 'NULL'} AS SourceRunID,
                    {f"'{cluster_id}'"  if cluster_id else 'NULL'} AS ClusterID
                FROM {tasks} t
                LEFT JOIN {seqs} s ON t.SequenceID = s.SequenceID
                WHERE t.TaskID      = {task_id}
                  AND t.ProjectCode = '{project_code}'
                  AND t.ProcessLoad = '{process_load}'
                  AND t.WorkFlowID  = {workflow_id}
            ) AS src
            ON  tgt.ExecutionID    = src.ExecutionID
            AND tgt.ProcessingDate = src.ProcessingDate
            AND tgt.ProjectCode    = src.ProjectCode
            AND tgt.ProcessLoad    = src.ProcessLoad
            AND tgt.WorkFlowID     = src.WorkFlowID
            AND tgt.TaskID         = src.TaskID
            AND tgt.SequenceID     = src.SequenceID
            AND tgt.Attempts       = src.Attempts
            WHEN MATCHED THEN UPDATE SET
                tgt.Status        = src.Status,
                tgt.SourceType    = src.SourceType,
                tgt.SourceRunID   = src.SourceRunID,
                tgt.ClusterID     = src.ClusterID,
                tgt.StartTime     = current_timestamp(),
                tgt.LastUpdatedOn = current_timestamp()
            WHEN NOT MATCHED THEN INSERT (
                ProcessingDate, ProjectCode, ProcessLoad, ExecutionID,
                WorkFlowID, TaskID, SequenceID, Attempts, Status,
                TaskName, SequenceCode, TaskMandatory, SourceSystemCode,
                SourceType, SourceRunID, ClusterID,
                StartTime, LastUpdatedOn, LastUpdatedBy
            ) VALUES (
                src.ProcessingDate, src.ProjectCode, src.ProcessLoad, src.ExecutionID,
                src.WorkFlowID, src.TaskID, src.SequenceID, src.Attempts, src.Status,
                src.TaskName, src.SequenceCode, src.TaskMandatory, src.SourceSystemCode,
                src.SourceType, src.SourceRunID, src.ClusterID,
                current_timestamp(), current_timestamp(), current_user()
            )
        """)

    def end_task(
        self,
        execution_id: str,
        project_code: str,
        process_load: str,
        task_id: int,
        sequence_id: int,
        workflow_id: int,
        processing_date: str,
        attempts: int = 0,
        status: str = "DONE",
        log_message: Optional[str] = None,
        log_type: Optional[str] = None,
        log_code: Optional[str] = None,
    ) -> None:
        """
        Write DONE or FAIL — equivalent to p_ETLProcessingStatusUpdate.

        On DONE: auto-advances DELTA_DATE watermark to task StartTime.
        On FAIL: resets initiation task (WF0/SEQ0) back to NQUE.
        DELTA_ID watermarks are NOT auto-advanced — call advance_watermark() manually.
        """
        steps  = self._fqn("ETLProcessingSteps")
        params = self._fqn("ETLconfigParameters")
        msg_sql   = f"'{log_message}'" if log_message else "NULL"
        ltype_sql = f"'{log_type}'"   if log_type    else "NULL"
        lcode_sql = f"'{log_code}'"   if log_code    else "NULL"

        self.spark.sql(f"""
            UPDATE {steps}
            SET
                Status          = '{status}',
                EndTime         = current_timestamp(),
                DurationSeconds = DATEDIFF(SECOND, StartTime, current_timestamp()),
                LogMessage      = {msg_sql},
                LogType         = {ltype_sql},
                LogCode         = {lcode_sql},
                LastUpdatedOn   = current_timestamp()
            WHERE ExecutionID    = '{execution_id}'
              AND ProjectCode    = '{project_code}'
              AND ProcessLoad    = '{process_load}'
              AND WorkFlowID     = {workflow_id}
              AND TaskID         = {task_id}
              AND SequenceID     = {sequence_id}
              AND Attempts       = {attempts}
        """)

        if status == "DONE":
            self.spark.sql(f"""
                UPDATE {params} AS p
                SET
                    p.ValueDateTime = (
                        SELECT e.StartTime FROM {steps} e
                        WHERE e.ExecutionID    = '{execution_id}'
                          AND e.ProcessingDate = '{processing_date}'
                          AND e.ProjectCode    = '{project_code}'
                          AND e.ProcessLoad    = '{process_load}'
                          AND e.WorkFlowID     = {workflow_id}
                          AND e.TaskID         = {task_id}
                          AND e.SequenceID     = {sequence_id}
                          AND e.Attempts       = {attempts}
                    ),
                    p.LastUpdatedOn = current_timestamp(),
                    p.LastUpdatedBy = current_user()
                WHERE p.ProjectCode   = '{project_code}'
                  AND p.ProcessLoad   = '{process_load}'
                  AND p.ParameterType = 'DELTA_DATE'
                  AND p.ParameterName = (
                    SELECT e.SourceSystemCode FROM {steps} e
                    WHERE e.ExecutionID    = '{execution_id}'
                      AND e.ProcessingDate = '{processing_date}'
                      AND e.ProjectCode    = '{project_code}'
                      AND e.ProcessLoad    = '{process_load}'
                      AND e.WorkFlowID     = {workflow_id}
                      AND e.TaskID         = {task_id}
                      AND e.SequenceID     = {sequence_id}
                      AND e.Attempts       = {attempts}
                  )
            """)

        elif status == "FAIL":
            self.spark.sql(f"""
                UPDATE {steps}
                SET
                    Status        = 'NQUE',
                    EndTime       = current_timestamp(),
                    LastUpdatedOn = current_timestamp()
                WHERE ExecutionID    = '{execution_id}'
                  AND ProjectCode    = '{project_code}'
                  AND ProcessLoad    = '{process_load}'
                  AND WorkFlowID     = 0
                  AND SequenceID     = 0
            """)

    def fail_task(
        self,
        execution_id: str,
        project_code: str,
        process_load: str,
        task_id: int,
        sequence_id: int,
        workflow_id: int,
        processing_date: str,
        attempts: int = 0,
        error_message: str = "",
        log_code: Optional[str] = None,
    ) -> None:
        """Convenience wrapper: record FAIL with truncated error message."""
        self.end_task(
            execution_id, project_code, process_load,
            task_id, sequence_id, workflow_id, processing_date,
            attempts=attempts, status="FAIL",
            log_message=error_message[:2000],
            log_type="ERROR",
            log_code=log_code,
        )

    @contextmanager
    def task(
        self,
        execution_id: str,
        project_code: str,
        process_load: str,
        task_id: int,
        sequence_id: int,
        workflow_id: int,
        processing_date: Optional[str] = None,
        attempts: int = 0,
        source_type: str = "DBX_NOTEBOOK",
        log_message: Optional[str] = None,
        log_type: Optional[str] = None,
        log_code: Optional[str] = None,
    ):
        """
        Context manager: start_task() at entry, end_task(DONE) on clean exit,
        fail_task() on exception.

        ``log_message`` / ``log_type`` / ``log_code`` are written on DONE.
        On exception, the exception string is captured as the error log_message automatically.

        ``processing_date`` defaults to today's date when not supplied.

        Example::

            exec_id = ETLMonitorFramework.generate_execution_id()
            monitor.generate_execution_steps(exec_id, "CORP", "HR_DAILY", "2026-04-09")

            with monitor.task(exec_id, "CORP", "HR_DAILY",
                              task_id=1, sequence_id=2, workflow_id=1,
                              log_message="Loaded 1,243 rows"):
                # notebook logic here
                pass
        """
        from datetime import date as _date
        if processing_date is None:
            processing_date = str(_date.today())

        self.start_task(execution_id, project_code, process_load,
                        task_id, sequence_id, workflow_id,
                        processing_date, attempts, source_type)
        try:
            yield
            self.end_task(execution_id, project_code, process_load,
                          task_id, sequence_id, workflow_id,
                          processing_date, attempts, status="DONE",
                          log_message=log_message, log_type=log_type, log_code=log_code)
        except Exception as exc:
            self.fail_task(execution_id, project_code, process_load,
                           task_id, sequence_id, workflow_id,
                           processing_date, attempts, error_message=str(exc))
            raise

    # ------------------------------------------------------------------
    # Public — watermark helpers
    # ------------------------------------------------------------------

    def advance_watermark(
        self,
        project_code: str,
        process_load: str,
        parameter_name: str,
        new_datetime_value: Optional[str] = None,
        new_int_value: Optional[int] = None,
    ) -> None:
        """
        Advance a delta watermark without direct DML on ETLconfigParameters.

        DELTA_DATE: pass new_datetime_value (ISO timestamp string).
        DELTA_ID:   pass new_int_value (max ID successfully loaded).

        DELTA_ID watermarks cannot be auto-advanced — the framework has no
        knowledge of which column in the source dataset is the delta field.
        Call this method before or inside the task context manager.

        DELTA_DATE watermarks are also auto-advanced by end_task(DONE).
        Calling advance_watermark() for DELTA_DATE overrides the default
        (task StartTime) with a more precise value from the source data.
        """
        params   = self._fqn("ETLconfigParameters")
        dt_sql   = f"'{new_datetime_value}'" if new_datetime_value is not None else "ValueDateTime"
        int_sql  = str(new_int_value)        if new_int_value      is not None else "ValueINT"

        self.spark.sql(f"""
            UPDATE {params}
            SET
                ValueDateTime = {dt_sql},
                ValueINT      = {int_sql},
                LastUpdatedOn = current_timestamp(),
                LastUpdatedBy = current_user()
            WHERE COALESCE(ProjectCode,  '') = '{project_code}'
              AND COALESCE(ProcessLoad,  '') = '{process_load}'
              AND COALESCE(ParameterName,'') = '{parameter_name}'
              AND ParameterType IN ('DELTA_DATE', 'DELTA_ID')
        """)

    def get_active_watermark(
        self,
        project_code: str,
        process_load: str,
        parameter_name: str,
    ) -> Optional[Union[str, int, bool]]:
        """
        Return the active watermark value resolved by ParameterType.

        DELTA_DATE / SYSTEM → ValueDateTime  (python datetime or None)
        DELTA_ID            → ValueINT        (int or None)
        FLAG                → ValueBIT        (bool or None)

        For ADF consumption use v_watermarks.ActiveValue (STRING) instead.
        """
        params = self._fqn("ETLconfigParameters")
        rows = self.spark.sql(f"""
            SELECT ParameterType, ValueINT, ValueDateTime, ValueBIT
            FROM   {params}
            WHERE  COALESCE(ProjectCode,  '') = '{project_code}'
              AND  COALESCE(ProcessLoad,  '') = '{process_load}'
              AND  COALESCE(ParameterName,'') = '{parameter_name}'
        """).collect()

        if not rows:
            return None
        r = rows[0]
        ptype = r["ParameterType"]
        if ptype in ("DELTA_DATE", "SYSTEM"):
            return r["ValueDateTime"]
        if ptype == "DELTA_ID":
            return r["ValueINT"]
        if ptype == "FLAG":
            return r["ValueBIT"]
        return None

    # ------------------------------------------------------------------
    # Public — status get / reset / processing mode
    # ------------------------------------------------------------------

    def get_status(
        self,
        project_code: str,
        process_load: str,
        processing_date: Optional[str] = None,
        execution_id: Optional[str] = None,
        workflow_id: Optional[int] = None,
        sequence_id: Optional[int] = None,
        task_id: Optional[int] = None,
        attempts: Optional[int] = None,
        summary_mode: bool = False,
    ):
        """
        Retrieve execution status — equivalent to p_ETLProcessingStatusGet.

        summary_mode=True  → one row per ProcessingDate/Attempts with OverallStatus.
        summary_mode=False → task-level detail filtered by supplied parameters.
        """
        steps = self._fqn("ETLProcessingSteps")

        if not processing_date:
            processing_date = self._resolve_processing_date(project_code, process_load)

        if summary_mode:
            date_filt = f"AND ProcessingDate = '{processing_date}'" if processing_date else ""
            return self.spark.sql(f"""
                SELECT
                    x.ProjectCode, x.ProcessLoad, x.ProcessingDate, x.Attempts,
                    MIN(x.StartTime)                                        AS StartTime,
                    MAX(x.EndTime)                                          AS EndTime,
                    DATEDIFF(SECOND, MIN(x.StartTime),
                             MAX(COALESCE(x.EndTime, current_timestamp()))) AS DurationSeconds,
                    (SELECT y.Status FROM {steps} y
                     WHERE y.ProjectCode=x.ProjectCode AND y.ProcessLoad=x.ProcessLoad
                       AND y.ProcessingDate=x.ProcessingDate AND y.Attempts=x.Attempts
                       AND y.WorkFlowID=0 AND y.SequenceID=0 LIMIT 1)      AS OverallStatus,
                    CASE WHEN MAX(x.EndTime) IS NULL
                         THEN 'Some tasks are not yet complete; run may still be active.'
                    END                                                     AS Remark
                FROM {steps} x
                WHERE ProjectCode = '{project_code}' AND ProcessLoad = '{process_load}'
                  {date_filt}
                GROUP BY x.ProjectCode, x.ProcessLoad, x.ProcessingDate, x.Attempts
                ORDER BY x.ProcessingDate DESC, x.Attempts DESC
            """)

        resolved_attempts = attempts
        if execution_id and attempts is None:
            rows = self.spark.sql(f"""
                SELECT MAX(Attempts) AS MaxA FROM {steps}
                WHERE ProjectCode='{project_code}' AND ProcessLoad='{process_load}'
                  AND ProcessingDate='{processing_date}' AND ExecutionID='{execution_id}'
            """).collect()
            resolved_attempts = rows[0]["MaxA"] if rows else 0

        filters = [
            f"ProjectCode = '{project_code}'",
            f"ProcessLoad = '{process_load}'",
            f"ProcessingDate = '{processing_date}'",
        ]
        if execution_id:                    filters.append(f"ExecutionID = '{execution_id}'")
        if resolved_attempts is not None:   filters.append(f"Attempts = {resolved_attempts}")
        if workflow_id is not None:         filters.append(f"WorkFlowID = {workflow_id}")
        if sequence_id is not None:         filters.append(f"SequenceID = {sequence_id}")
        if task_id     is not None:         filters.append(f"TaskID = {task_id}")

        where = " AND ".join(filters)
        return self.spark.sql(f"""
            SELECT
                ProcessingDate, ProjectCode, ProcessLoad, ExecutionID, Attempts,
                TaskName, Status, WorkFlowID, SequenceID, TaskID,
                TaskMandatory, SourceSystemCode, SourceType,
                StartTime, EndTime, DurationSeconds,
                LogMessage, LogType, LogCode, LastUpdatedOn
            FROM {steps} WHERE {where}
            ORDER BY Attempts DESC, WorkFlowID, SequenceID, TaskID
        """)

    def status_reset(
        self,
        project_code: str,
        process_load: str,
        processing_date: Optional[str] = None,
        execution_id: Optional[str] = None,
        workflow_id: Optional[int] = None,
        sequence_id: Optional[int] = None,
        task_id: Optional[int] = None,
        attempts: Optional[int] = None,
        remark: Optional[str] = None,
    ) -> None:
        """
        Reset execution steps for replay — equivalent to p_ETLProcessingStatusReset.

        **Purpose: day replay, not failure retry.**
        Resets DONE tasks back to RQUE so a date that already completed can be re-processed
        (e.g. data quality issue found after the fact, or a historic date re-run).

        **Failure retry is handled differently** — use a new ExecutionID and call
        ``generate_execution_steps()`` again. It will auto-detect Attempts, skip already-DONE
        tasks, and create new NQUE rows only for the FAIL/NQUE tasks at Attempts+1.

        **Bulk (no execution_id):** all DONE tasks for the ProcessingDate → RQUE.
        **Specific (execution_id given):** DONE tasks for that execution → RQUE;
        also resets the initiation task (WF0/SEQ0) DONE → RQUE.
        """
        steps = self._fqn("ETLProcessingSteps")

        if not processing_date:
            processing_date = self._resolve_processing_date(project_code, process_load)

        remark_sql = f"'{remark}'" if remark else "NULL"

        if not execution_id and attempts is None:
            # ── Full day replay ─────────────────────────────────────────────
            # Reset all DONE tasks for this date back to RQUE.
            self.spark.sql(f"""
                UPDATE {steps}
                SET Status='RQUE', LogMessage={remark_sql}, LastUpdatedOn=current_timestamp()
                WHERE Status='DONE'
                  AND ProcessingDate='{processing_date}'
                  AND ProjectCode='{project_code}'
                  AND ProcessLoad='{process_load}'
            """)
        else:
            # ── Specific execution replay ────────────────────────────────────
            resolved = attempts
            if execution_id and resolved is None:
                rows = self.spark.sql(f"""
                    SELECT MAX(Attempts) AS MaxA FROM {steps}
                    WHERE ProcessingDate='{processing_date}'
                      AND ProjectCode='{project_code}' AND ProcessLoad='{process_load}'
                      AND ExecutionID='{execution_id}'
                """).collect()
                resolved = rows[0]["MaxA"] if rows and rows[0]["MaxA"] is not None else 0

            wf_filt   = f"AND WorkFlowID={workflow_id}" if workflow_id  is not None else ""
            seq_filt  = f"AND SequenceID={sequence_id}" if sequence_id  is not None else ""
            tsk_filt  = f"AND TaskID={task_id}"         if task_id      is not None else ""
            exec_filt = f"AND ExecutionID='{execution_id}'" if execution_id else ""

            # Reset DONE tasks → RQUE (for replay)
            self.spark.sql(f"""
                UPDATE {steps}
                SET Status='RQUE', LogMessage={remark_sql}, LastUpdatedOn=current_timestamp()
                WHERE Status='DONE'
                  AND ProcessingDate='{processing_date}'
                  AND ProjectCode='{project_code}'
                  AND ProcessLoad='{process_load}'
                  AND Attempts<={resolved}
                  {exec_filt} {wf_filt} {seq_filt} {tsk_filt}
            """)
            # Always also reset initiation task (WF0/SEQ0) DONE → RQUE
            self.spark.sql(f"""
                UPDATE {steps}
                SET Status='RQUE', LogMessage={remark_sql}, LastUpdatedOn=current_timestamp()
                WHERE Status='DONE'
                  AND ProcessingDate='{processing_date}'
                  AND ProjectCode='{project_code}'
                  AND ProcessLoad='{process_load}'
                  AND Attempts<={resolved}
                  AND WorkFlowID=0 AND SequenceID=0
                  {exec_filt}
            """)

    def set_processing_mode(
        self,
        project_code: str,
        process_load: str,
        is_bulk_mode: bool = False,
        is_historic_mode: bool = False,
        processing_date: Optional[str] = None,
        parameter_name: Optional[str] = None,
        value_datetime: Optional[str] = None,
        value_int: Optional[int] = None,
        value_bit: Optional[bool] = None,
    ) -> None:
        """
        Configure processing mode — equivalent to p_ETLconfigProcessingMode.

        Historic mode (is_historic_mode=True, processing_date=<past>):
            Sets SYSDT.ValueDateTime to the given date.

        Live mode (default):
            Sets SYSDT.ValueDateTime = NULL (use current date).

        Bulk mode (is_bulk_mode=True):
            DELTA_DATE → ValueDateTime=NULL, ValueINT=-1
            DELTA_ID   → ValueINT=0, ValueDateTime=NULL

        Specific (parameter_name given):
            Updates only that parameter.
        """
        params = self._fqn("ETLconfigParameters")

        if is_historic_mode:
            if not processing_date:
                raise ValueError("processing_date is required in historic mode.")
            self.spark.sql(f"""
                UPDATE {params}
                SET ValueDateTime='{processing_date}',
                    LastUpdatedOn=current_timestamp(), LastUpdatedBy=current_user()
                WHERE ProjectCode='{project_code}' AND ParameterName='SYSDT'
                  AND ParameterType='SYSTEM'
            """)
        else:
            self.spark.sql(f"""
                UPDATE {params}
                SET ValueDateTime=NULL,
                    LastUpdatedOn=current_timestamp(), LastUpdatedBy=current_user()
                WHERE ProjectCode='{project_code}' AND ParameterName='SYSDT'
                  AND ParameterType='SYSTEM'
            """)

        if is_bulk_mode:
            self.spark.sql(f"""
                UPDATE {params}
                SET ValueINT=-1, ValueDateTime=NULL,
                    LastUpdatedOn=current_timestamp(), LastUpdatedBy=current_user()
                WHERE ProjectCode='{project_code}' AND ProcessLoad='{process_load}'
                  AND ParameterType='DELTA_DATE'
            """)
            self.spark.sql(f"""
                UPDATE {params}
                SET ValueINT=0, ValueDateTime=NULL,
                    LastUpdatedOn=current_timestamp(), LastUpdatedBy=current_user()
                WHERE ProjectCode='{project_code}' AND ProcessLoad='{process_load}'
                  AND ParameterType='DELTA_ID'
            """)
        elif parameter_name:
            dt_sql  = f"'{value_datetime}'"   if value_datetime is not None else "ValueDateTime"
            int_sql = str(value_int)          if value_int      is not None else "ValueINT"
            bit_sql = str(value_bit).upper()  if value_bit      is not None else "ValueBIT"
            self.spark.sql(f"""
                UPDATE {params}
                SET ValueDateTime={dt_sql}, ValueINT={int_sql}, ValueBIT={bit_sql},
                    LastUpdatedOn=current_timestamp(), LastUpdatedBy=current_user()
                WHERE ProjectCode='{project_code}' AND ProcessLoad='{process_load}'
                  AND ParameterName='{parameter_name}'
            """)

        self.spark.sql(f"""
            SELECT * FROM {params}
            WHERE ProjectCode='{project_code}' AND ProcessLoad='{process_load}'
            ORDER BY ParameterType, ParameterName
        """).show(truncate=False)

    # ------------------------------------------------------------------
    # Public — static helpers
    # ------------------------------------------------------------------

    @staticmethod
    def generate_execution_id() -> str:
        """Generate a new UUID execution ID for a logical run."""
        return str(uuid.uuid4())

    def guide(self) -> None:
        """Print a concise step-by-step usage guide to stdout."""
        W = 70
        print("─" * W)
        print("  Databricks ETL Monitor Framework — Usage Guide")
        print("─" * W)
        print("""
STEP 1 — Setup (idempotent, run every cluster start)
──────────────────────────────────────────────────────
  monitor = ETLMonitorFramework(spark, catalog="<catalog>", schema="etl")
  monitor.setup()
    Creates: 8 Delta tables + 6 reporting views + seeds sequence stages.

STEP 2 — Register organisation and project (once per org/project)
──────────────────────────────────────────────────────────────────
  monitor.register_organisation(
      organisation_code        = "CORP",             # MANDATORY — short code
      organisation_name        = "Corporate Group",  # MANDATORY — display name
      organisation_description = "...",              # optional
  )
  monitor.register_project(
      project_code        = "HR",                    # MANDATORY — short code, FK to ETLconfigProcess
      project_name        = "HR Data Platform",      # MANDATORY — display name
      project_description = "...",                   # optional
      organisation_code   = "CORP",                  # optional — FK to ETLOrganisation
  )

STEP 3 — Register your process (once per domain)
──────────────────────────────────────────────────
  monitor.register_process(
      project_code   = "HR",            # MANDATORY — must match ETLconfigProject.ProjectCode
      process_load   = "HR_DAILY",      # MANDATORY — process identifier
      name           = "HR Daily Load", # optional
      description    = "...",           # optional
      owner          = "HR Team",       # optional
      load_frequency = "D",             # optional  D/W/M/Y/A  (default: D)
  )

STEP 5 — Register tasks (once per task)
─────────────────────────────────────────
  monitor.register_task(
      project_code             = "HR",              # MANDATORY — must match ETLconfigProject.ProjectCode
      process_load             = "HR_DAILY",         # MANDATORY
      task_id                  = 1,                  # MANDATORY — YOUR integer, you assign it
      workflow_id              = 1,                  # MANDATORY — 0=Init 1=First pass 2=Second...
      sequence_id              = 2,                  # MANDATORY — tasks sharing SequenceID run in PARALLEL
      task_name                = "Load Employees",   # MANDATORY
      source_type              = "DBX_NOTEBOOK",     # optional  DBX_NOTEBOOK/DBX_JOB/ADF_PIPELINE/DATAFLOW
      source_identifier        = "/Repos/.../nb",    # optional  notebook path / job id / pipeline name
      source_system_code       = "LoadEmployees",    # optional  links to watermark ParameterName; None = full load
      task_description         = "...",              # optional
      load_frequency           = "D",                # optional  D/W/M/Y/A  (default: D)
      task_mandatory           = True,               # optional  FAIL blocks downstream stages (default: True)
  )
  # Initiation task is ALWAYS: task_id=0, workflow_id=0, sequence_id=0

STEP 6 — Register watermark parameters (once per watermark)
─────────────────────────────────────────────────────────────
  monitor.register_parameter(
      project_code   = "HR",              # MANDATORY
      process_load   = "HR_DAILY",        # MANDATORY
      parameter_name = "LoadEmployees",   # MANDATORY — must match source_system_code above
      parameter_type = "DELTA_DATE",      # MANDATORY — DELTA_DATE / DELTA_ID / FLAG / SYSTEM
      description    = "...",             # optional
      value_datetime = None,              # optional  None = bulk/full load start
      value_int      = None,              # optional  for DELTA_ID; 0 = bulk start
      value_bit      = None,              # optional  for FLAG
  )
  # Always register SYSDT:
  monitor.register_parameter("HR", "HR_DAILY", "SYSDT", "SYSTEM")

  ParameterType reference:
    DELTA_DATE  — auto-advanced to task StartTime on DONE
    DELTA_ID    — NOT auto-advanced; call advance_watermark() after load
    FLAG        — boolean config; read freely
    SYSTEM      — SYSDT only; controlled via set_processing_mode()

STEP 7 — Each run: generate steps and instrument tasks
────────────────────────────────────────────────────────
  exec_id = ETLMonitorFramework.generate_execution_id()
  monitor.generate_execution_steps(exec_id, "HR", "HR_DAILY", "2026-04-10")

  with monitor.task(exec_id, "HR", "HR_DAILY",
                    task_id=1, workflow_id=1, sequence_id=2,
                    processing_date="2026-04-10"):
      pass  # your notebook logic here

STEP 8 — Query status
───────────────────────
  monitor.get_status("HR", "HR_DAILY", execution_id=exec_id)   # task detail
  monitor.get_status("HR", "HR_DAILY", summary_mode=True)       # run rollup

  SQL views:
    v_processStatus    — cross-process live dashboard
    v_runSummary       — per execution rollup
    v_taskDetail       — per-task with SLA breach flag
    v_mandatoryBlockers— tasks blocking downstream progress
    v_currentFailures  — all failures today
    v_watermarks       — watermark values + ActiveValue (ADF bridge)

STEP 9 — Retry after failure
──────────────────────────────
  monitor.status_reset("HR", "HR_DAILY", execution_id=exec_id)        # all failures
  monitor.status_reset("HR", "HR_DAILY", execution_id=exec_id,
                        task_id=1, workflow_id=1)                      # specific task

OTHER METHODS
─────────────
  monitor.get_active_watermark("CORP", "HR_DAILY", "LoadEmployees")     # read watermark value
  monitor.advance_watermark("CORP", "HR_DAILY", "LoadByID", new_int_value=99999)  # DELTA_ID only
  monitor.set_processing_mode("CORP", "HR_DAILY", is_bulk_mode=True)    # full reload
  monitor.set_processing_mode("CORP", "HR_DAILY", is_historic_mode=True,
                               historic_date="2026-01-01")               # historic rerun
  monitor.set_processing_mode("CORP", "HR_DAILY")                       # restore live mode
  monitor.sample_usage(spark)                                            # extract sample notebooks
""".rstrip())
        print("─" * W)

    def sample_usage(self, spark) -> str:
        """
        Extract bundled sample notebooks to the current user's Workspace folder.

        Returns the path where the notebooks were extracted.
        Safe to re-run — skips files already up-to-date (compared by mtime).

        Usage::

            path = monitor.sample_usage(spark)
            print(f"Sample notebooks extracted to: {path}")
        """
        try:
            repo_user = spark.sql("SELECT current_user()").first()[0]
        except Exception:
            repo_user = os.environ.get("USER", "unknown")

        dest = f"/Workspace/Users/{repo_user}/databricks-etl-monitor/sample_usage"
        os.makedirs(dest, exist_ok=True)

        pkg_dir = os.path.dirname(os.path.abspath(__file__))
        bundled = os.path.join(pkg_dir, "sample_usage")

        copied = []
        if os.path.isdir(bundled):
            for fname in os.listdir(bundled):
                if fname.startswith(".") or fname.startswith("~$"):
                    continue
                src_file  = os.path.join(bundled, fname)
                dest_file = os.path.join(dest, fname)
                if not os.path.isfile(src_file):
                    continue   # skip __pycache__ and any subdirectories
                if (not os.path.exists(dest_file)
                        or os.path.getmtime(src_file) > os.path.getmtime(dest_file)):
                    shutil.copy2(src_file, dest_file)
                    copied.append(fname)

        logger.info("Sample notebooks extracted to: %s (%d files)", dest, len(copied))
        print(f"Sample notebooks extracted to: {dest}")
        return dest

    # ------------------------------------------------------------------
    # Private — setup internals  (mirrors DQFramework pattern)
    # ------------------------------------------------------------------

    def _verify_catalog(self) -> None:
        if not self.catalog:
            return
        try:
            catalogs = [r["catalog"] for r in
                        self.spark.sql("SHOW CATALOGS").collect()]
            if self.catalog not in catalogs:
                raise RuntimeError(
                    f"Unity Catalog '{self.catalog}' does not exist. "
                    f"Available: {catalogs}. "
                    f"Create it with: CREATE CATALOG `{self.catalog}`"
                )
        except Exception as exc:
            if "does not exist" in str(exc):
                raise
            logger.debug("Could not verify catalog: %s", exc)

    def _create_schema(self) -> None:
        fqn = (f"`{self.catalog}`.`{self.etl_schema}`"
               if self.catalog else f"`{self.etl_schema}`")
        self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {fqn}")
        logger.info("Schema ready: %s", fqn)

    def _create_tables(self) -> None:
        for name in TABLE_ORDER:
            fqn      = self._fqn(name)
            rendered = DDL_STATEMENTS[name].format(fqn=fqn)
            self.spark.sql(rendered)
            cls = (
                "[FRAMEWORK-MANAGED]" if name in FRAMEWORK_SEEDED_TABLES
                else "[USER-MANAGED]"   if name in USER_CONFIG_TABLES
                else "[RESULTS]"
            )
            logger.info("Table ready: %s  %s", fqn, cls)

    def _create_reporting_views(self) -> None:
        """Create or replace all 6 monitoring views."""
        steps  = self._fqn("ETLProcessingSteps")
        tasks  = self._fqn("ETLconfigTasks")
        params = self._fqn("ETLconfigParameters")
        procs  = self._fqn("ETLconfigProcess")

        self.spark.sql(f"""
            CREATE OR REPLACE VIEW {self._fqn("v_processStatus")} AS
            SELECT
                e.ProcessingDate, e.ProjectCode, e.ProcessLoad,
                p.ProcessName, p.ProcessOwner,
                MAX(e.Attempts)                                              AS LatestAttempt,
                COUNT(DISTINCT e.ExecutionID)                                AS TotalRuns,
                SUM(CASE WHEN e.Status='DONE' THEN 1 ELSE 0 END)            AS Done,
                SUM(CASE WHEN e.Status='FAIL' THEN 1 ELSE 0 END)            AS Failed,
                SUM(CASE WHEN e.Status IN ('NQUE','RQUE') THEN 1 ELSE 0 END) AS Pending,
                MIN(e.StartTime)                                             AS RunStart,
                MAX(e.EndTime)                                               AS RunEnd,
                DATEDIFF(SECOND, MIN(e.StartTime), MAX(e.EndTime))           AS WallClockSeconds,
                MAX(CASE WHEN e.WorkFlowID=0 AND e.SequenceID=0
                         THEN e.Status END)                                  AS OverallStatus
            FROM {steps} e
            LEFT JOIN {procs} p
              ON e.ProjectCode=p.ProjectCode AND e.ProcessLoad=p.ProcessLoad
            GROUP BY e.ProcessingDate, e.ProjectCode, e.ProcessLoad,
                     p.ProcessName, p.ProcessOwner, e.Attempts
        """)

        self.spark.sql(f"""
            CREATE OR REPLACE VIEW {self._fqn("v_runSummary")} AS
            SELECT
                ProcessingDate, ProjectCode, ProcessLoad, ExecutionID, Attempts,
                COUNT(*)                                                     AS TotalTasks,
                SUM(CASE WHEN Status='DONE' THEN 1 ELSE 0 END)              AS Done,
                SUM(CASE WHEN Status='FAIL' THEN 1 ELSE 0 END)              AS Failed,
                SUM(CASE WHEN Status IN ('NQUE','RQUE') THEN 1 ELSE 0 END)  AS Pending,
                MIN(StartTime) AS RunStart, MAX(EndTime) AS RunEnd,
                SUM(DurationSeconds) AS TotalDurationSeconds,
                DATEDIFF(SECOND, MIN(StartTime), MAX(EndTime))               AS WallClockSeconds,
                CASE
                    WHEN SUM(CASE WHEN Status IN ('NQUE','RQUE') THEN 1 ELSE 0 END)>0 THEN 'RUNNING'
                    WHEN SUM(CASE WHEN Status='FAIL' THEN 1 ELSE 0 END)>0             THEN 'FAILED'
                    ELSE 'COMPLETE'
                END                                                          AS OverallStatus,
                MAX(LastUpdatedOn) AS LastActivity
            FROM {steps}
            GROUP BY ProcessingDate, ProjectCode, ProcessLoad, ExecutionID, Attempts
        """)

        self.spark.sql(f"""
            CREATE OR REPLACE VIEW {self._fqn("v_taskDetail")} AS
            SELECT
                e.ProcessingDate, e.ProjectCode, e.ProcessLoad,
                e.ExecutionID, e.Attempts, e.WorkFlowID,
                e.SequenceCode, e.SequenceID,
                e.TaskName, e.TaskID, e.TaskMandatory, e.SourceSystemCode,
                e.Status, e.StartTime, e.EndTime, e.DurationSeconds,
                e.SourceType, e.SourceRunID,
                e.LogType, e.LogMessage, e.LastUpdatedBy
            FROM {steps} e
        """)

        self.spark.sql(f"""
            CREATE OR REPLACE VIEW {self._fqn("v_mandatoryBlockers")} AS
            SELECT
                e.ProcessingDate, e.ProjectCode, e.ProcessLoad,
                e.ExecutionID, e.WorkFlowID, e.SequenceCode, e.TaskName,
                e.TaskID, e.Attempts, e.Status, e.StartTime,
                e.LogType, e.LogMessage, e.SourceType, e.SourceSystemCode
            FROM {steps} e
            WHERE e.TaskMandatory=TRUE AND e.Status IN ('FAIL','NQUE','RQUE')
              AND e.Attempts=(
                SELECT MAX(e2.Attempts) FROM {steps} e2
                WHERE e2.ExecutionID=e.ExecutionID AND e2.ProjectCode=e.ProjectCode
                  AND e2.ProcessLoad=e.ProcessLoad AND e2.WorkFlowID=e.WorkFlowID
                  AND e2.TaskID=e.TaskID AND e2.SequenceID=e.SequenceID
              )
        """)

        self.spark.sql(f"""
            CREATE OR REPLACE VIEW {self._fqn("v_currentFailures")} AS
            SELECT
                e.ProcessingDate, e.ProjectCode, e.ProcessLoad,
                e.ExecutionID, e.WorkFlowID, e.SequenceCode, e.TaskName,
                e.TaskID, e.TaskMandatory, e.Attempts, e.Status, e.StartTime,
                e.LogType, e.LogMessage, e.SourceType, e.SourceRunID
            FROM {steps} e
            WHERE e.Status='FAIL'
              AND e.Attempts=(
                SELECT MAX(e2.Attempts) FROM {steps} e2
                WHERE e2.ExecutionID=e.ExecutionID AND e2.ProjectCode=e.ProjectCode
                  AND e2.ProcessLoad=e.ProcessLoad AND e2.WorkFlowID=e.WorkFlowID
                  AND e2.TaskID=e.TaskID AND e2.SequenceID=e.SequenceID
              )
        """)

        self.spark.sql(f"""
            CREATE OR REPLACE VIEW {self._fqn("v_watermarks")} AS
            SELECT
                p.ProjectCode, p.ProcessLoad, p.ParameterName,
                p.ParameterType, p.ParameterDescription,
                p.ValueINT, p.ValueDateTime, p.ValueBIT,
                CASE p.ParameterType
                    WHEN 'DELTA_DATE' THEN CAST(p.ValueDateTime AS STRING)
                    WHEN 'DELTA_ID'   THEN CAST(p.ValueINT      AS STRING)
                    WHEN 'FLAG'       THEN CAST(p.ValueBIT       AS STRING)
                    WHEN 'SYSTEM'     THEN CAST(p.ValueDateTime  AS STRING)
                    ELSE NULL
                END AS ActiveValue,
                CASE p.ParameterType
                    WHEN 'DELTA_DATE' THEN
                        CASE WHEN p.ValueDateTime IS NULL THEN 'BULK MODE'
                             ELSE 'DELTA: '||CAST(p.ValueDateTime AS STRING) END
                    WHEN 'DELTA_ID' THEN
                        CASE WHEN p.ValueINT IN (-1,0) OR p.ValueINT IS NULL THEN 'BULK MODE'
                             ELSE 'DELTA: '||CAST(p.ValueINT AS STRING) END
                    WHEN 'FLAG'   THEN CAST(p.ValueBIT AS STRING)
                    WHEN 'SYSTEM' THEN
                        CASE WHEN p.ValueDateTime IS NULL THEN 'LIVE DATE'
                             ELSE 'HISTORIC: '||CAST(p.ValueDateTime AS STRING) END
                    ELSE 'UNKNOWN'
                END AS WatermarkState,
                t.TaskName AS LinkedTaskName,
                p.LastUpdatedOn, p.LastUpdatedBy
            FROM {params} p
            LEFT JOIN {tasks} t
              ON  COALESCE(t.ProjectCode,     '')=COALESCE(p.ProjectCode,    '')
              AND COALESCE(t.ProcessLoad,     '')=COALESCE(p.ProcessLoad,    '')
              AND COALESCE(t.SourceSystemCode,'')=COALESCE(p.ParameterName,  '')
        """)

        logger.info(
            "Views ready: v_processStatus, v_runSummary, v_taskDetail, "
            "v_mandatoryBlockers, v_currentFailures, v_watermarks"
        )

    def _fqn(self, name: str) -> str:
        """Backtick-quoted fully-qualified name — mirrors DQFramework._fqn()."""
        if self.catalog:
            return f"`{self.catalog}`.`{self.etl_schema}`.`{name}`"
        return f"`{self.etl_schema}`.`{name}`"

    def _resolve_processing_date(self, project_code: str, process_load: str) -> Optional[str]:
        """Resolve ProcessingDate from SYSDT parameter (NULL = current date)."""
        params = self._fqn("ETLconfigParameters")
        rows = self.spark.sql(f"""
            SELECT COALESCE(ValueDateTime, current_date()) AS ProcessingDate
            FROM   {params}
            WHERE  ProjectCode='{project_code}' AND ProcessLoad='{process_load}'
              AND  ParameterName='SYSDT' AND ParameterType='SYSTEM'
        """).collect()
        return str(rows[0]["ProcessingDate"]) if rows else None

    def _get_cluster_id(self) -> Optional[str]:
        try:
            return self.spark.conf.get(
                "spark.databricks.clusterUsageTags.clusterId"
            )
        except Exception:
            return None

    def _get_run_id(self) -> Optional[str]:
        try:
            ctx = (self.spark._jvm
                   .com.databricks.dbutils
                   .DBUtils(self.spark._jsc.sc())
                   .notebook().getContext())
            return ctx.currentRunId().toString()
        except Exception:
            return None
