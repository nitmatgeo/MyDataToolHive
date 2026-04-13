# Databricks notebook source
# MAGIC %md
# MAGIC # 02 — Register Config: Organisations, Projects, Processes, Tasks & Parameters
# MAGIC Seeds all user-managed config tables using the ETL Monitor Python API.
# MAGIC All writes use INSERT-ONLY MERGE — idempotent, safe to re-run.
# MAGIC
# MAGIC **Scenario:** HR and Finance departments across two projects
# MAGIC
# MAGIC | Organisation | Project | ProcessLoad | Frequency | Source Systems |
# MAGIC |---|---|---|---|---|
# MAGIC | HR_DIV | HR | EMPLOYEE_MASTER | Daily | UK SAP HR (ADF), US Workday (ADLS), India PeopleSoft (network share) |
# MAGIC | HR_DIV | HR | PAYROLL_MONTHLY | Monthly | Payroll bureau SFTP/ADLS |
# MAGIC | FIN_DIV | FINANCE | GL_DAILY | Daily | ERP via ADF + Dataflow |
# MAGIC | FIN_DIV | FINANCE | REGULATORY_ANNUAL | Yearly | ERP archive + ADLS |
# MAGIC
# MAGIC Requires `01-install.py` to have been run in this session first.

# COMMAND ----------

# DBTITLE 1,Inherit Framework and Variables from 01-install
# MAGIC %run ./01-install

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 — Register Organisations
# MAGIC Top-level grouper. One row per org / division.

# COMMAND ----------

# DBTITLE 1,Register Organisations
(monitor
    .register_organisation(
        organisation_code        = "CORP",
        organisation_name        = "Corporate Group",
        organisation_description = "Top-level corporate holding group — parent organisation for all divisions.",
    )
    .register_organisation(
        organisation_code        = "HR_DIV",
        organisation_name        = "Human Resources Division",
        organisation_description = "HR division covering employee lifecycle, payroll and workforce analytics across all regions.",
    )
    .register_organisation(
        organisation_code        = "FIN_DIV",
        organisation_name        = "Finance Division",
        organisation_description = "Finance division covering general ledger, regulatory reporting and management accounts.",
    )
)
print("✓ Organisations registered")

# COMMAND ----------

# DBTITLE 1,Verify Organisations
spark.sql(f"""
    SELECT OrganisationCode, OrganisationName, IsActive
    FROM `{MY_CATALOG}`.`{ETL_SCHEMA}`.`ETLOrganisation`
    ORDER BY OrganisationCode
""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 — Register Projects
# MAGIC Mid-level grouper. Links an organisation to its ETL processes.
# MAGIC `ProjectCode` is reused as FK in ETLconfigProcess, ETLconfigTasks, and ETLconfigParameters.

# COMMAND ----------

# DBTITLE 1,Register Projects
(monitor
    .register_project(
        project_code        = "HR",
        project_name        = "HR Data Platform",
        project_description = "Data engineering platform for all HR source system ingestion, employee master data and payroll analytics.",
        organisation_code   = "HR_DIV",
    )
    .register_project(
        project_code        = "FINANCE",
        project_name        = "Finance Data Platform",
        project_description = "Data engineering platform for general ledger, account balances, regulatory reporting and management accounts.",
        organisation_code   = "FIN_DIV",
    )
)
print("✓ Projects registered")

# COMMAND ----------

# DBTITLE 1,Verify Projects
spark.sql(f"""
    SELECT p.ProjectCode, p.OrganisationCode, o.OrganisationName, p.ProjectName, p.IsActive
    FROM `{MY_CATALOG}`.`{ETL_SCHEMA}`.`ETLconfigProject` p
    LEFT JOIN `{MY_CATALOG}`.`{ETL_SCHEMA}`.`ETLOrganisation` o
      ON p.OrganisationCode = o.OrganisationCode
    ORDER BY p.ProjectCode
""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 — Register Processes
# MAGIC One row per domain load within a project.
# MAGIC `LoadFrequency`: D=Daily  W=Weekly  M=Monthly  Y=Yearly  A=Ad-hoc

# COMMAND ----------

# DBTITLE 1,Register All Processes
(monitor
    .register_process(
        project_code   = "HR",
        process_load   = "EMPLOYEE_MASTER",
        name           = "HR Employee Master Data Load",
        description    = "Daily ingestion of employee personal information, contract data and org structure "
                         "from three source systems: UK SAP HR, US Workday, India PeopleSoft.",
        owner          = "HR Data Engineering",
        load_frequency = "D",
    )
    .register_process(
        project_code   = "HR",
        process_load   = "PAYROLL_MONTHLY",
        name           = "HR Payroll Monthly Processing",
        description    = "Month-end payroll data ingestion and processing. Receives payroll export files "
                         "from the payroll bureau via SFTP/network share.",
        owner          = "HR Payroll Team",
        load_frequency = "M",
    )
    .register_process(
        project_code   = "FINANCE",
        process_load   = "GL_DAILY",
        name           = "Finance General Ledger Daily Feed",
        description    = "Daily incremental extract of general ledger journal entries and account balances "
                         "from the ERP system via ADF pipeline.",
        owner          = "Finance Data Engineering",
        load_frequency = "D",
    )
    .register_process(
        project_code   = "FINANCE",
        process_load   = "REGULATORY_ANNUAL",
        name           = "Finance Regulatory Annual Reporting Load",
        description    = "Annual load of regulatory reporting data — consolidates full-year financial "
                         "positions, disclosures and statutory tables. Runs once per financial year.",
        owner          = "Finance Regulatory Team",
        load_frequency = "Y",
    )
)
print("✓ Processes registered")

# COMMAND ----------

# DBTITLE 1,Verify Processes
spark.sql(f"""
    SELECT ProjectCode, ProcessLoad, ProcessName, LoadFrequency, IsActive
    FROM `{MY_CATALOG}`.`{ETL_SCHEMA}`.`ETLconfigProcess`
    ORDER BY ProjectCode, ProcessLoad
""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4 — Register Tasks
# MAGIC
# MAGIC Rules:
# MAGIC - Initiation task is **always** `TaskID=0, WorkFlowID=0, SequenceID=0` — one per process
# MAGIC - Tasks sharing the same `SequenceID` within a `WorkFlowID` run **in parallel**
# MAGIC - `SourceSystemCode` must match a `ParameterName` in ETLconfigParameters (for delta tasks)
# MAGIC - `TaskMandatory=True` → FAIL blocks all downstream SequenceID stages
# MAGIC - `TaskMandatory=False` or `None` → pipeline continues even if this task FAILs

# COMMAND ----------

# MAGIC %md
# MAGIC ### HR / EMPLOYEE_MASTER Tasks
# MAGIC
# MAGIC | TaskID | WF | SeqID | SequenceCode | Task | Mandatory | Source |
# MAGIC |---|---|---|---|---|---|---|
# MAGIC | 0 | 0 | 0 | LOAD_GO | Initiation | Y | DBX_NOTEBOOK |
# MAGIC | 1 | 1 | 1 | LOAD_DB_CONFIG | Load HR Reference Config | Y | DBX_NOTEBOOK |
# MAGIC | 2 | 1 | 2 | LOAD_DB_TRAN | Load UK Employees (SAP HR) | Y | ADF_PIPELINE |
# MAGIC | 3 | 1 | 2 | LOAD_DB_TRAN | Load US Employees (Workday) | Y | DBX_NOTEBOOK |
# MAGIC | 4 | 1 | 2 | LOAD_DB_TRAN | Load India Employees (PeopleSoft) | N | DBX_NOTEBOOK |
# MAGIC | 5 | 1 | 2 | LOAD_DB_TRAN | Load Org Structure Feed | N | DBX_NOTEBOOK |
# MAGIC | 6 | 2 | 3 | LOAD_DIM | Process Employee Dimensions | Y | DBX_NOTEBOOK |
# MAGIC | 7 | 2 | 5 | PRE_PROCESS | Apply HR Business Rules | N | DBX_NOTEBOOK |
# MAGIC | 8 | 2 | 6 | PROCESS_DATA | Build Employee Analytics Mart | Y | DBX_NOTEBOOK |
# MAGIC
# MAGIC Tasks 2, 3, 4, 5 share SequenceID=2 → run **in parallel** in WorkFlowID=1.

# COMMAND ----------

# DBTITLE 1,Register HR / EMPLOYEE_MASTER Tasks
(monitor
    .register_task(
        project_code              = "HR",
        process_load              = "EMPLOYEE_MASTER",
        task_id                   = 0,
        workflow_id               = 0,
        sequence_id               = 0,           # LOAD_GO
        task_name                 = "Initiation",
        task_description          = "Overall run initiation marker — resets to NQUE on any mandatory task FAIL.",
        source_type               = "DBX_NOTEBOOK",
        source_identifier         = "/Workspace/Repos/hr-platform/etl/00-initiation",
        task_mandatory            = True,
        load_frequency            = "D",
        expected_duration_seconds = 30,
    )
    .register_task(
        project_code              = "HR",
        process_load              = "EMPLOYEE_MASTER",
        task_id                   = 1,
        workflow_id               = 1,
        sequence_id               = 1,           # LOAD_DB_CONFIG — runs before SequenceID=2
        task_name                 = "Load HR Reference Config",
        task_description          = "Ingest department hierarchy, job grades, cost-centre codes and country "
                                    "reference data. Mandatory — all employee loads depend on this.",
        source_type               = "DBX_NOTEBOOK",
        source_identifier         = "/Workspace/Repos/hr-platform/employee/01-load-hr-config",
        task_mandatory            = True,
        load_frequency            = "D",
        expected_duration_seconds = 90,
    )
    .register_task(
        project_code              = "HR",
        process_load              = "EMPLOYEE_MASTER",
        task_id                   = 2,
        workflow_id               = 1,
        sequence_id               = 2,           # LOAD_DB_TRAN — parallel with tasks 3, 4, 5
        task_name                 = "Load UK Employees (SAP HR)",
        task_description          = "Incremental ingest of UK employee personal info and contract records "
                                    "via ADF pipeline from SAP HR. Delta watermark: LoadEmployeesUK.",
        source_type               = "ADF_PIPELINE",
        source_identifier         = "pl_hr_ingest_employees_uk",
        source_system_code        = "LoadEmployeesUK",
        task_mandatory            = True,
        load_frequency            = "D",
        expected_duration_seconds = 360,
    )
    .register_task(
        project_code              = "HR",
        process_load              = "EMPLOYEE_MASTER",
        task_id                   = 3,
        workflow_id               = 1,
        sequence_id               = 2,           # LOAD_DB_TRAN — parallel with tasks 2, 4, 5
        task_name                 = "Load US Employees (Workday)",
        task_description          = "Incremental ingest of US employee records from Workday ADLS delta drop path. "
                                    "Delta watermark: LoadEmployeesUS.",
        source_type               = "DBX_NOTEBOOK",
        source_identifier         = "abfss://raw@hrdatalake.dfs.core.windows.net/workday/employees/us/incremental/",
        source_system_code        = "LoadEmployeesUS",
        task_mandatory            = True,
        load_frequency            = "D",
        expected_duration_seconds = 420,
    )
    .register_task(
        project_code              = "HR",
        process_load              = "EMPLOYEE_MASTER",
        task_id                   = 4,
        workflow_id               = 1,
        sequence_id               = 2,           # LOAD_DB_TRAN — parallel, non-mandatory
        task_name                 = "Load India Employees (PeopleSoft)",
        task_description          = "Daily employee file from PeopleSoft via SFTP/network share. "
                                    "Non-mandatory — file may not arrive on bank holidays.",
        source_type               = "DBX_NOTEBOOK",
        source_identifier         = "\\\\hr-fileserver\\exports\\peoplesoft\\india\\employees\\daily",
        source_system_code        = "LoadEmployeesIN",
        task_mandatory            = False,
        load_frequency            = "D",
        expected_duration_seconds = 300,
    )
    .register_task(
        project_code              = "HR",
        process_load              = "EMPLOYEE_MASTER",
        task_id                   = 5,
        workflow_id               = 1,
        sequence_id               = 2,           # LOAD_DB_TRAN — parallel, non-mandatory
        task_name                 = "Load Org Structure Feed",
        task_description          = "Flat-file feed of reporting hierarchy and org chart changes. "
                                    "Non-mandatory — absence does not block employee processing.",
        source_type               = "DBX_NOTEBOOK",
        source_identifier         = "abfss://raw@hrdatalake.dfs.core.windows.net/org-structure/daily/",
        source_system_code        = "LoadOrgStructure",
        task_mandatory            = False,
        load_frequency            = "D",
        expected_duration_seconds = 180,
    )
    .register_task(
        project_code              = "HR",
        process_load              = "EMPLOYEE_MASTER",
        task_id                   = 6,
        workflow_id               = 2,
        sequence_id               = 3,           # LOAD_DIM — validate and merge dimensions
        task_name                 = "Process Employee Dimensions",
        task_description          = "Validate and merge staged employee records into the employee dimension table. "
                                    "Deduplication and country consolidation applied here.",
        source_type               = "DBX_NOTEBOOK",
        source_identifier         = "/Workspace/Repos/hr-platform/employee/06-process-dim-employees",
        task_mandatory            = True,
        load_frequency            = "D",
        expected_duration_seconds = 480,
    )
    .register_task(
        project_code              = "HR",
        process_load              = "EMPLOYEE_MASTER",
        task_id                   = 7,
        workflow_id               = 2,
        sequence_id               = 5,           # PRE_PROCESS — business logic, non-mandatory
        task_name                 = "Apply HR Business Rules",
        task_description          = "Derive FTE classification, band mapping and headcount eligibility flags. "
                                    "Non-mandatory — downstream reporting works without this enrichment.",
        source_type               = "DBX_NOTEBOOK",
        source_identifier         = "/Workspace/Repos/hr-platform/employee/07-apply-hr-rules",
        task_mandatory            = False,
        load_frequency            = "D",
        expected_duration_seconds = 240,
    )
    .register_task(
        project_code              = "HR",
        process_load              = "EMPLOYEE_MASTER",
        task_id                   = 8,
        workflow_id               = 2,
        sequence_id               = 6,           # PROCESS_DATA — analytics mart
        task_name                 = "Build Employee Analytics Mart",
        task_description          = "Aggregate employee data into headcount, attrition and diversity analytics mart.",
        source_type               = "DBX_NOTEBOOK",
        source_identifier         = "/Workspace/Repos/hr-platform/employee/08-employee-analytics-mart",
        task_mandatory            = True,
        load_frequency            = "D",
        expected_duration_seconds = 600,
    )
)
print("✓ HR / EMPLOYEE_MASTER tasks registered")

# COMMAND ----------

# MAGIC %md
# MAGIC ### HR / PAYROLL_MONTHLY Tasks
# MAGIC
# MAGIC | TaskID | WF | SeqID | Task | Mandatory |
# MAGIC |---|---|---|---|---|
# MAGIC | 0 | 0 | 0 | Initiation | Y |
# MAGIC | 1 | 1 | 2 | Load Payroll File UK | Y |
# MAGIC | 2 | 1 | 2 | Load Payroll File US | N |
# MAGIC | 3 | 2 | 5 | Apply Payroll Derivations | Y |
# MAGIC | 4 | 2 | 6 | Build Payroll Analytics Mart | Y |

# COMMAND ----------

# DBTITLE 1,Register HR / PAYROLL_MONTHLY Tasks
(monitor
    .register_task(
        project_code              = "HR",
        process_load              = "PAYROLL_MONTHLY",
        task_id                   = 0,
        workflow_id               = 0,
        sequence_id               = 0,
        task_name                 = "Initiation",
        task_description          = "Monthly payroll run initiation marker.",
        source_type               = "DBX_NOTEBOOK",
        source_identifier         = "/Workspace/Repos/hr-platform/etl/00-initiation",
        task_mandatory            = True,
        load_frequency            = "M",
        expected_duration_seconds = 30,
    )
    .register_task(
        project_code              = "HR",
        process_load              = "PAYROLL_MONTHLY",
        task_id                   = 1,
        workflow_id               = 1,
        sequence_id               = 2,           # parallel with task 2
        task_name                 = "Load Payroll File UK",
        task_description          = "Monthly payroll export from bureau via SFTP network share — UK entities.",
        source_type               = "DBX_NOTEBOOK",
        source_identifier         = "\\\\payroll-bureau\\exports\\uk\\monthly",
        source_system_code        = "LoadPayrollUK",
        task_mandatory            = True,
        load_frequency            = "M",
        expected_duration_seconds = 600,
    )
    .register_task(
        project_code              = "HR",
        process_load              = "PAYROLL_MONTHLY",
        task_id                   = 2,
        workflow_id               = 1,
        sequence_id               = 2,           # parallel with task 1, non-mandatory
        task_name                 = "Load Payroll File US",
        task_description          = "Monthly payroll export from bureau ADLS drop path — US entities. "
                                    "Non-mandatory — US payroll may be delayed without blocking UK close.",
        source_type               = "DBX_NOTEBOOK",
        source_identifier         = "abfss://raw@hrdatalake.dfs.core.windows.net/payroll/us/monthly/",
        source_system_code        = "LoadPayrollUS",
        task_mandatory            = False,
        load_frequency            = "M",
        expected_duration_seconds = 480,
    )
    .register_task(
        project_code              = "HR",
        process_load              = "PAYROLL_MONTHLY",
        task_id                   = 3,
        workflow_id               = 2,
        sequence_id               = 5,
        task_name                 = "Apply Payroll Derivations",
        task_description          = "Derive net pay, tax bands, pension deductions and employer cost allocations.",
        source_type               = "DBX_NOTEBOOK",
        source_identifier         = "/Workspace/Repos/hr-platform/payroll/03-apply-payroll-derivations",
        task_mandatory            = True,
        load_frequency            = "M",
        expected_duration_seconds = 900,
    )
    .register_task(
        project_code              = "HR",
        process_load              = "PAYROLL_MONTHLY",
        task_id                   = 4,
        workflow_id               = 2,
        sequence_id               = 6,
        task_name                 = "Build Payroll Analytics Mart",
        task_description          = "Persist monthly payroll totals and cost-centre allocations into payroll analytics mart.",
        source_type               = "DBX_NOTEBOOK",
        source_identifier         = "/Workspace/Repos/hr-platform/payroll/04-payroll-analytics-mart",
        task_mandatory            = True,
        load_frequency            = "M",
        expected_duration_seconds = 480,
    )
)
print("✓ HR / PAYROLL_MONTHLY tasks registered")

# COMMAND ----------

# MAGIC %md
# MAGIC ### FINANCE / GL_DAILY Tasks
# MAGIC
# MAGIC | TaskID | WF | SeqID | Task | Mandatory | SourceType |
# MAGIC |---|---|---|---|---|---|
# MAGIC | 0 | 0 | 0 | Initiation | Y | DBX_NOTEBOOK |
# MAGIC | 1 | 1 | 2 | Load GL Journal Entries | Y | ADF_PIPELINE |
# MAGIC | 2 | 1 | 2 | Load Account Balances | N | DATAFLOW |
# MAGIC | 3 | 2 | 6 | Build Finance Reporting Mart | Y | DBX_NOTEBOOK |

# COMMAND ----------

# DBTITLE 1,Register FINANCE / GL_DAILY Tasks
(monitor
    .register_task(
        project_code              = "FINANCE",
        process_load              = "GL_DAILY",
        task_id                   = 0,
        workflow_id               = 0,
        sequence_id               = 0,
        task_name                 = "Initiation",
        task_description          = "GL daily run initiation marker.",
        source_type               = "DBX_NOTEBOOK",
        source_identifier         = "/Workspace/Repos/finance-platform/etl/00-initiation",
        task_mandatory            = True,
        load_frequency            = "D",
        expected_duration_seconds = 30,
    )
    .register_task(
        project_code              = "FINANCE",
        process_load              = "GL_DAILY",
        task_id                   = 1,
        workflow_id               = 1,
        sequence_id               = 2,           # parallel with task 2
        task_name                 = "Load GL Journal Entries",
        task_description          = "Incremental ingest of general ledger journal entries via ADF pipeline. "
                                    "Delta watermark: LoadGLJournals.",
        source_type               = "ADF_PIPELINE",
        source_identifier         = "pl_finance_ingest_gl_journals",
        source_system_code        = "LoadGLJournals",
        task_mandatory            = True,
        load_frequency            = "D",
        expected_duration_seconds = 720,
    )
    .register_task(
        project_code              = "FINANCE",
        process_load              = "GL_DAILY",
        task_id                   = 2,
        workflow_id               = 1,
        sequence_id               = 2,           # parallel with task 1, non-mandatory
        task_name                 = "Load Account Balances",
        task_description          = "Daily account balance snapshot via Dataflow from ERP reporting views. "
                                    "Non-mandatory — runs in parallel with journal load.",
        source_type               = "DATAFLOW",
        source_identifier         = "df_finance_account_balances",
        source_system_code        = "LoadAccountBalances",
        task_mandatory            = False,
        load_frequency            = "D",
        expected_duration_seconds = 360,
    )
    .register_task(
        project_code              = "FINANCE",
        process_load              = "GL_DAILY",
        task_id                   = 3,
        workflow_id               = 2,
        sequence_id               = 6,
        task_name                 = "Build Finance Reporting Mart",
        task_description          = "Aggregate journal entries and account balances into P&L, "
                                    "balance sheet and cost-centre mart.",
        source_type               = "DBX_NOTEBOOK",
        source_identifier         = "/Workspace/Repos/finance-platform/gl/03-finance-reporting-mart",
        task_mandatory            = True,
        load_frequency            = "D",
        expected_duration_seconds = 900,
    )
)
print("✓ FINANCE / GL_DAILY tasks registered")

# COMMAND ----------

# MAGIC %md
# MAGIC ### FINANCE / REGULATORY_ANNUAL Tasks
# MAGIC
# MAGIC | TaskID | WF | SeqID | Task | Mandatory | Freq |
# MAGIC |---|---|---|---|---|---|
# MAGIC | 0 | 0 | 0 | Initiation | Y | Y |
# MAGIC | 1 | 1 | 2 | Load Full-Year Financial Positions | Y | Y |
# MAGIC | 2 | 1 | 2 | Load Statutory Disclosure Tables | N | Y |
# MAGIC | 3 | 2 | 6 | Build Regulatory Reporting Pack | Y | Y |

# COMMAND ----------

# DBTITLE 1,Register FINANCE / REGULATORY_ANNUAL Tasks
(monitor
    .register_task(
        project_code              = "FINANCE",
        process_load              = "REGULATORY_ANNUAL",
        task_id                   = 0,
        workflow_id               = 0,
        sequence_id               = 0,
        task_name                 = "Initiation",
        task_description          = "Annual regulatory run initiation marker.",
        source_type               = "DBX_NOTEBOOK",
        source_identifier         = "/Workspace/Repos/finance-platform/etl/00-initiation",
        task_mandatory            = True,
        load_frequency            = "Y",
        expected_duration_seconds = 30,
    )
    .register_task(
        project_code              = "FINANCE",
        process_load              = "REGULATORY_ANNUAL",
        task_id                   = 1,
        workflow_id               = 1,
        sequence_id               = 2,           # parallel with task 2, full-load (no watermark)
        task_name                 = "Load Full-Year Financial Positions",
        task_description          = "Full reload of all financial positions for the closed financial year from ERP archive.",
        source_type               = "ADF_PIPELINE",
        source_identifier         = "pl_finance_ingest_annual_positions",
        task_mandatory            = True,
        load_frequency            = "Y",
        expected_duration_seconds = 3600,
    )
    .register_task(
        project_code              = "FINANCE",
        process_load              = "REGULATORY_ANNUAL",
        task_id                   = 2,
        workflow_id               = 1,
        sequence_id               = 2,           # parallel with task 1, non-mandatory
        task_name                 = "Load Statutory Disclosure Tables",
        task_description          = "Load statutory disclosure reference data from ADLS annual drop. "
                                    "Non-mandatory — regulatory team may supply late.",
        source_type               = "DBX_NOTEBOOK",
        source_identifier         = "abfss://raw@financedatalake.dfs.core.windows.net/regulatory/annual/disclosures/",
        task_mandatory            = False,
        load_frequency            = "Y",
        expected_duration_seconds = 1800,
    )
    .register_task(
        project_code              = "FINANCE",
        process_load              = "REGULATORY_ANNUAL",
        task_id                   = 3,
        workflow_id               = 2,
        sequence_id               = 6,
        task_name                 = "Build Regulatory Reporting Pack",
        task_description          = "Consolidate and format annual financial data into regulatory submission tables.",
        source_type               = "DBX_NOTEBOOK",
        source_identifier         = "/Workspace/Repos/finance-platform/regulatory/03-regulatory-pack",
        task_mandatory            = True,
        load_frequency            = "Y",
        expected_duration_seconds = 7200,
    )
)
print("✓ FINANCE / REGULATORY_ANNUAL tasks registered")

# COMMAND ----------

# DBTITLE 1,Verify All Tasks
spark.sql(f"""
    SELECT t.ProjectCode, t.ProcessLoad, t.WorkFlowID, t.SequenceID,
           s.SequenceCode, t.TaskID, t.TaskName, t.SourceType,
           t.SourceSystemCode, t.TaskMandatory, t.LoadFrequency,
           t.ExpectedDurationSeconds
    FROM `{MY_CATALOG}`.`{ETL_SCHEMA}`.`ETLconfigTasks` t
    JOIN `{MY_CATALOG}`.`{ETL_SCHEMA}`.`ETLconfigSequence` s
      ON t.SequenceID = s.SequenceID
    ORDER BY t.ProjectCode, t.ProcessLoad, t.WorkFlowID, t.SequenceID, t.TaskID
""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5 — Register Watermark Parameters
# MAGIC
# MAGIC | ParameterType | Active column | Auto-advanced on DONE? | Bulk mode |
# MAGIC |---|---|---|---|
# MAGIC | `DELTA_DATE` | `ValueDateTime` | **Yes** — set to task StartTime | `ValueDateTime = NULL` |
# MAGIC | `DELTA_ID` | `ValueINT` | **No** — call `advance_watermark()` | `ValueINT = 0` |
# MAGIC | `FLAG` | `ValueBIT` | No | not applicable |
# MAGIC | `SYSTEM` | `ValueDateTime` | No — `set_processing_mode()` only | `NULL` = live date |
# MAGIC
# MAGIC Always register `SYSDT` as `SYSTEM` type for every process.

# COMMAND ----------

# MAGIC %md
# MAGIC ### HR / EMPLOYEE_MASTER Parameters

# COMMAND ----------

# DBTITLE 1,Register HR / EMPLOYEE_MASTER Parameters
(monitor
    .register_parameter(
        project_code   = "HR",
        process_load   = "EMPLOYEE_MASTER",
        parameter_name = "SYSDT",
        parameter_type = "SYSTEM",
        description    = "System processing date — NULL = live (current_date). Set to a past date for historic reruns.",
    )
    .register_parameter(
        project_code   = "HR",
        process_load   = "EMPLOYEE_MASTER",
        parameter_name = "LoadEmployeesUK",
        parameter_type = "DELTA_DATE",
        description    = "Last successfully loaded UK employee timestamp from SAP HR. NULL = bulk (full) reload.",
    )
    .register_parameter(
        project_code   = "HR",
        process_load   = "EMPLOYEE_MASTER",
        parameter_name = "LoadEmployeesUS",
        parameter_type = "DELTA_DATE",
        description    = "Last successfully loaded US employee timestamp from Workday ADLS path. NULL = bulk (full) reload.",
    )
    .register_parameter(
        project_code   = "HR",
        process_load   = "EMPLOYEE_MASTER",
        parameter_name = "LoadEmployeesIN",
        parameter_type = "DELTA_DATE",
        description    = "Last successfully loaded India employee file date from PeopleSoft network share. NULL = bulk reload.",
    )
    .register_parameter(
        project_code   = "HR",
        process_load   = "EMPLOYEE_MASTER",
        parameter_name = "LoadOrgStructure",
        parameter_type = "DELTA_DATE",
        description    = "Last successfully loaded org structure file date. NULL = bulk (full) reload.",
    )
    .register_parameter(
        project_code   = "HR",
        process_load   = "EMPLOYEE_MASTER",
        parameter_name = "IsFullReloadFlag",
        parameter_type = "FLAG",
        description    = "When TRUE, forces full reload for all delta tasks regardless of watermark values.",
        value_bit      = False,
    )
)
print("✓ HR / EMPLOYEE_MASTER parameters registered")

# COMMAND ----------

# MAGIC %md
# MAGIC ### HR / PAYROLL_MONTHLY Parameters

# COMMAND ----------

# DBTITLE 1,Register HR / PAYROLL_MONTHLY Parameters
(monitor
    .register_parameter(
        project_code   = "HR",
        process_load   = "PAYROLL_MONTHLY",
        parameter_name = "SYSDT",
        parameter_type = "SYSTEM",
        description    = "System processing date — NULL = live (current_date). Set to a past date for historic reruns.",
    )
    .register_parameter(
        project_code   = "HR",
        process_load   = "PAYROLL_MONTHLY",
        parameter_name = "LoadPayrollUK",
        parameter_type = "DELTA_DATE",
        description    = "Last successfully processed UK payroll month-end date. NULL = bulk (full) reload.",
    )
    .register_parameter(
        project_code   = "HR",
        process_load   = "PAYROLL_MONTHLY",
        parameter_name = "LoadPayrollUS",
        parameter_type = "DELTA_DATE",
        description    = "Last successfully processed US payroll month-end date. NULL = bulk (full) reload.",
    )
)
print("✓ HR / PAYROLL_MONTHLY parameters registered")

# COMMAND ----------

# MAGIC %md
# MAGIC ### FINANCE / GL_DAILY Parameters

# COMMAND ----------

# DBTITLE 1,Register FINANCE / GL_DAILY Parameters
(monitor
    .register_parameter(
        project_code   = "FINANCE",
        process_load   = "GL_DAILY",
        parameter_name = "SYSDT",
        parameter_type = "SYSTEM",
        description    = "System processing date — NULL = live (current_date). Set to a past date for historic reruns.",
    )
    .register_parameter(
        project_code   = "FINANCE",
        process_load   = "GL_DAILY",
        parameter_name = "LoadGLJournals",
        parameter_type = "DELTA_DATE",
        description    = "Last successfully loaded GL journal entry timestamp from ERP. NULL = bulk (full) reload.",
    )
    .register_parameter(
        project_code   = "FINANCE",
        process_load   = "GL_DAILY",
        parameter_name = "LoadAccountBalances",
        parameter_type = "DELTA_DATE",
        description    = "Last successfully loaded account balance snapshot timestamp. NULL = bulk (full) reload.",
    )
    .register_parameter(
        project_code   = "FINANCE",
        process_load   = "GL_DAILY",
        parameter_name = "IsPartialPeriodLoad",
        parameter_type = "FLAG",
        description    = "When TRUE, restricts GL journal load to the current open accounting period only.",
        value_bit      = False,
    )
)
print("✓ FINANCE / GL_DAILY parameters registered")

# COMMAND ----------

# MAGIC %md
# MAGIC ### FINANCE / REGULATORY_ANNUAL Parameters

# COMMAND ----------

# DBTITLE 1,Register FINANCE / REGULATORY_ANNUAL Parameters
(monitor
    .register_parameter(
        project_code   = "FINANCE",
        process_load   = "REGULATORY_ANNUAL",
        parameter_name = "SYSDT",
        parameter_type = "SYSTEM",
        description    = "System processing date — NULL = live (current_date). Set to a past date for historic reruns.",
    )
)
print("✓ FINANCE / REGULATORY_ANNUAL parameters registered")

# COMMAND ----------

# DBTITLE 1,Verify All Watermarks (v_watermarks view)
spark.sql(f"""
    SELECT ProjectCode, ProcessLoad, ParameterName, ParameterType,
           ActiveValue, ValueDateTime, ValueINT, ValueBIT
    FROM `{MY_CATALOG}`.`{ETL_SCHEMA}`.`v_watermarks`
    ORDER BY ProjectCode, ProcessLoad, ParameterName
""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration Complete
# MAGIC
# MAGIC All config tables are now seeded:
# MAGIC
# MAGIC | Table | Rows |
# MAGIC |---|---|
# MAGIC | `ETLOrganisation` | 3 organisations |
# MAGIC | `ETLconfigProject` | 2 projects |
# MAGIC | `ETLconfigProcess` | 4 processes |
# MAGIC | `ETLconfigTasks` | 22 tasks across 4 processes |
# MAGIC | `ETLconfigParameters` | 14 parameters (SYSDT × 4 + DELTA_DATE × 8 + FLAG × 2) |
# MAGIC
# MAGIC **Next step:** run `03-run.py` to simulate a full execution run across one of these processes,
# MAGIC including task start/end/fail/retry events and queries across all 6 reporting views.
# MAGIC
# MAGIC **Note on DELTA_DATE watermarks:** auto-advanced by the framework to task `StartTime` on DONE.
# MAGIC Read via `monitor.get_active_watermark()` or `v_watermarks.ActiveValue` (ADF Lookup bridge).
# MAGIC
# MAGIC **Note on NULL watermarks:** `ValueDateTime = NULL` = bulk/full reload mode for that parameter.
# MAGIC Use `monitor.set_processing_mode(..., is_bulk_mode=True)` to reset a watermark to NULL.
