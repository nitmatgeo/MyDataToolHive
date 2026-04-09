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

import importlib.resources
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
                "Populate ETLconfigProcess and ETLconfigTasks via register_process() "
                "/ register_task(), then ETLconfigParameters via register_parameter().",
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
    # Public — process / task / parameter registration
    # ------------------------------------------------------------------

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
        Register a named process in ETLconfigProcess via INSERT-ONLY MERGE.
        Will not overwrite an existing row.  Returns self for chaining.
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
        expected_duration_seconds: Optional[int] = None,
    ) -> "ETLMonitorFramework":
        """
        Register a task in ETLconfigTasks via INSERT-ONLY MERGE.

        ``task_id`` is user-assigned — developer controls ID values, matching
        the SQL Server IDENTITY INSERT OFF equivalent.  The MERGE ON composite
        key enforces uniqueness.

        ``source_system_code`` must match a ParameterName in ETLconfigParameters
        for the same (ProjectCode, ProcessLoad).  Leave empty for full-load tasks.

        Returns self for optional method chaining.
        """
        fqn     = self._fqn("ETLconfigTasks")
        dur_sql = str(expected_duration_seconds) if expected_duration_seconds is not None else "NULL"
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
                {str(task_mandatory).upper()}  AS TaskMandatory,
                {dur_sql}              AS ExpectedDurationSeconds
            ) AS src
            ON  tgt.TaskID       = src.TaskID
            AND tgt.WorkFlowID   = src.WorkFlowID
            AND COALESCE(tgt.ProjectCode,'') = COALESCE(src.ProjectCode,'')
            AND COALESCE(tgt.ProcessLoad, '') = COALESCE(src.ProcessLoad, '')
            WHEN NOT MATCHED THEN INSERT (
                TaskID, ProjectCode, ProcessLoad, WorkFlowID, SequenceID,
                TaskName, TaskDescription, SourceType, SourceIdentifier,
                SourceSystemCode, LoadFrequency, TaskMandatory,
                ExpectedDurationSeconds, IsActive,
                CreatedOn, CreatedBy, LastUpdatedOn, LastUpdatedBy
            ) VALUES (
                src.TaskID, src.ProjectCode, src.ProcessLoad, src.WorkFlowID,
                src.SequenceID, src.TaskName, src.TaskDescription,
                src.SourceType, src.SourceIdentifier, src.SourceSystemCode,
                src.LoadFrequency, src.TaskMandatory, src.ExpectedDurationSeconds,
                TRUE, current_timestamp(), current_user(),
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
        Register a parameter in ETLconfigParameters via INSERT-ONLY MERGE.

        parameter_type must be one of:
            DELTA_DATE  — date/timestamp watermark; auto-advanced on DONE
            DELTA_ID    — numeric ID watermark; developer must call advance_watermark()
            FLAG        — boolean config flag; read freely by developers
            SYSTEM      — reserved for SYSDT; controlled via set_processing_mode()

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
        Idempotent: skips if rows already exist for this ExecutionID.
        Snapshots TaskName, SequenceCode, TaskMandatory, SourceSystemCode at generation time.
        """
        steps = self._fqn("ETLProcessingSteps")
        tasks = self._fqn("ETLconfigTasks")
        seqs  = self._fqn("ETLconfigSequence")

        self.spark.sql(f"""
            INSERT INTO {steps}
            (ProcessingDate, ProjectCode, ProcessLoad, ExecutionID,
             WorkFlowID, TaskID, SequenceID, Attempts, Status,
             TaskName, SequenceCode, TaskMandatory, SourceSystemCode,
             StartTime, LastUpdatedOn, LastUpdatedBy)
            SELECT
                '{processing_date}', t.ProjectCode, t.ProcessLoad, '{execution_id}',
                t.WorkFlowID, t.TaskID, t.SequenceID, 0, 'NQUE',
                t.TaskName, s.SequenceCode, t.TaskMandatory, t.SourceSystemCode,
                current_timestamp(), current_timestamp(), current_user()
            FROM {tasks} t
            LEFT JOIN {seqs} s ON t.SequenceID = s.SequenceID
            WHERE t.ProjectCode = '{project_code}'
              AND t.ProcessLoad = '{process_load}'
              AND t.IsActive    = TRUE
              AND NOT EXISTS (
                SELECT 1 FROM {steps} e
                WHERE e.ExecutionID    = '{execution_id}'
                  AND e.ProcessingDate = '{processing_date}'
                  AND e.ProjectCode    = '{project_code}'
                  AND e.ProcessLoad    = '{process_load}'
              )
        """)
        logger.info("Execution steps generated for %s/%s exec=%s date=%s",
                    project_code, process_load, execution_id, processing_date)

    # ------------------------------------------------------------------
    # Public — get pending tasks
    # Equivalent to p_ETLOrchestrationSteps
    # ------------------------------------------------------------------

    def get_pending_tasks(
        self,
        execution_id: str,
        project_code: str,
        process_load: str,
        processing_date: str,
        sequence_id: Optional[int] = None,
        workflow_id: Optional[int] = None,
    ):
        """
        Return all non-DONE tasks for this run ordered by WorkFlowID / SequenceID / TaskID.
        Auto-generates steps if none exist yet (mirrors p_ETLOrchestrationSteps first-call behaviour).
        Tasks sharing a SequenceID should be dispatched in parallel by the orchestrator.
        """
        steps = self._fqn("ETLProcessingSteps")

        count = self.spark.sql(f"""
            SELECT COUNT(*) AS n FROM {steps}
            WHERE ExecutionID    = '{execution_id}'
              AND ProcessingDate = '{processing_date}'
              AND ProjectCode    = '{project_code}'
              AND ProcessLoad    = '{process_load}'
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
        attempts: int = 0,
        source_type: str = "DBX_NOTEBOOK",
        source_run_id: Optional[str] = None,
    ) -> None:
        """Record task start — MERGE into ETLProcessingSteps."""
        status     = "RQUE" if attempts > 0 else "NQUE"
        cluster_id = self._get_cluster_id()
        run_id     = source_run_id or self._get_run_id()
        steps      = self._fqn("ETLProcessingSteps")
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
            ON  tgt.ExecutionID = src.ExecutionID
            AND tgt.ProjectCode = src.ProjectCode
            AND tgt.ProcessLoad = src.ProcessLoad
            AND tgt.WorkFlowID  = src.WorkFlowID
            AND tgt.TaskID      = src.TaskID
            AND tgt.SequenceID  = src.SequenceID
            AND tgt.Attempts    = src.Attempts
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
                        WHERE e.ExecutionID = '{execution_id}'
                          AND e.ProjectCode = '{project_code}'
                          AND e.ProcessLoad = '{process_load}'
                          AND e.WorkFlowID  = {workflow_id}
                          AND e.TaskID      = {task_id}
                          AND e.SequenceID  = {sequence_id}
                          AND e.Attempts    = {attempts}
                    ),
                    p.LastUpdatedOn = current_timestamp(),
                    p.LastUpdatedBy = current_user()
                WHERE p.ProjectCode   = '{project_code}'
                  AND p.ProcessLoad   = '{process_load}'
                  AND p.ParameterType = 'DELTA_DATE'
                  AND p.ParameterName = (
                    SELECT e.SourceSystemCode FROM {steps} e
                    WHERE e.ExecutionID = '{execution_id}'
                      AND e.ProjectCode = '{project_code}'
                      AND e.ProcessLoad = '{process_load}'
                      AND e.WorkFlowID  = {workflow_id}
                      AND e.TaskID      = {task_id}
                      AND e.SequenceID  = {sequence_id}
                      AND e.Attempts    = {attempts}
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
        processing_date: str,
        attempts: int = 0,
        source_type: str = "DBX_NOTEBOOK",
    ):
        """
        Context manager: start_task() at entry, end_task(DONE) on clean exit,
        fail_task() on exception.

        Example::

            exec_id = ETLMonitorFramework.generate_execution_id()
            monitor.generate_execution_steps(exec_id, "CORP", "HR_DAILY", "2026-04-09")

            with monitor.task(exec_id, "CORP", "HR_DAILY",
                              task_id=1, sequence_id=2, workflow_id=1,
                              processing_date="2026-04-09"):
                # notebook logic here
                pass
        """
        self.start_task(execution_id, project_code, process_load,
                        task_id, sequence_id, workflow_id,
                        processing_date, attempts, source_type)
        try:
            yield
            self.end_task(execution_id, project_code, process_load,
                          task_id, sequence_id, workflow_id,
                          processing_date, attempts, status="DONE")
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
        Reset execution steps to RQUE for retry — equivalent to p_ETLProcessingStatusReset.

        Bulk (no execution_id): all DONE tasks for the ProcessingDate → RQUE.
        Specific (execution_id given): resolves max Attempts, resets DONE rows,
        always also resets the initiation task (WF0/SEQ0).
        """
        steps = self._fqn("ETLProcessingSteps")

        if not processing_date:
            processing_date = self._resolve_processing_date(project_code, process_load)

        remark_sql = f"'{remark}'" if remark else "NULL"

        if not execution_id and attempts is None:
            self.spark.sql(f"""
                UPDATE {steps}
                SET Status='RQUE', LogMessage={remark_sql}, LastUpdatedOn=current_timestamp()
                WHERE Status='DONE'
                  AND ProcessingDate='{processing_date}'
                  AND ProjectCode='{project_code}'
                  AND ProcessLoad='{process_load}'
            """)
        else:
            resolved = attempts
            if execution_id:
                rows = self.spark.sql(f"""
                    SELECT MAX(Attempts) AS MaxA FROM {steps}
                    WHERE ProcessingDate='{processing_date}'
                      AND ProjectCode='{project_code}' AND ProcessLoad='{process_load}'
                      AND ExecutionID='{execution_id}'
                """).collect()
                resolved = rows[0]["MaxA"] if rows else 0

            wf_filt   = f"AND WorkFlowID={workflow_id}" if workflow_id  is not None else ""
            seq_filt  = f"AND SequenceID={sequence_id}" if sequence_id  is not None else ""
            tsk_filt  = f"AND TaskID={task_id}"         if task_id      is not None else ""
            exec_filt = f"AND ExecutionID='{execution_id}'" if execution_id else ""

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
            # Always reset initiation task (mirrors original SP)
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

    def sample_usage(self, spark) -> str:
        """
        Extract bundled sample notebooks to the current user's Workspace folder.

        Returns the path where the notebooks were extracted.
        Safe to re-run — overwrites any existing files.

        Usage::

            path = monitor.sample_usage(spark)
            print(f"Sample notebooks extracted to: {path}")
        """
        try:
            repo_user = spark.sql("SELECT current_user()").first()[0]
        except Exception:
            repo_user = os.environ.get("USER", "unknown")

        dest = f"/Workspace/Users/{repo_user}/databricks-etl-monitor/sample_usage"

        try:
            pkg_path = importlib.resources.files("etl_monitor") / "sample_usage"
            os.makedirs(dest, exist_ok=True)
            for item in pkg_path.iterdir():
                src = str(item)
                dst = os.path.join(dest, item.name)
                shutil.copy2(src, dst)
            logger.info("Sample notebooks extracted to: %s", dest)
            print(f"Sample notebooks extracted to: {dest}")
            return dest
        except Exception as exc:
            logger.warning("Could not extract sample notebooks: %s", exc)
            print(f"Warning: Could not extract samples — {exc}")
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
                t.ExpectedDurationSeconds,
                CASE WHEN e.DurationSeconds > t.ExpectedDurationSeconds
                     THEN TRUE ELSE FALSE END AS SLABreached,
                e.SourceType, e.SourceRunID,
                e.LogType, e.LogMessage, e.LastUpdatedBy
            FROM {steps} e
            LEFT JOIN {tasks} t
              ON e.TaskID=t.TaskID AND e.ProjectCode=t.ProjectCode
             AND e.ProcessLoad=t.ProcessLoad AND e.WorkFlowID=t.WorkFlowID
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
