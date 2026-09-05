"""
ETL Monitor — Seed Data
========================
Built-in workflow stage definitions seeded into ETLconfigSequence by setup().

INSERT-ONLY MERGE — never overwrites existing rows.
Custom stages: use SequenceID >= 10 to avoid collision with framework rows (0-9).

Stage overview:
  0  LOAD_GO        Initiation — overall run marker (always WorkFlowID=0)
  1  LOAD_DB_CONFIG Load configuration / reference data from source
  2  LOAD_DB_TRAN   Load transactional data from source (staging)
  3  LOAD_DIM       Validate and process staged dimension / master data
  4  LOAD_TRAN      Validate and process staged transactional data
  5  PRE_PROCESS    Apply business logic and derivations
  6  PROCESS_DATA   Core transformation into output / data mart tables
"""

# (SequenceID, SequenceCode, SequenceName, SequenceDescription, SortOrder)
SEQUENCE_SEED = [
    (
        0, "LOAD_GO",
        "Initiating ETL Processing",
        "Triggering of ETL activities — initiation task, always WorkFlowID=0.",
        0,
    ),
    (
        1, "LOAD_DB_CONFIG",
        "Load Configuration Data",
        "Ingest configuration/reference data from source into staging area.",
        1,
    ),
    (
        2, "LOAD_DB_TRAN",
        "Load Transactional Data",
        "Ingest transactional data from source into staging area.",
        2,
    ),
    (
        3, "LOAD_DIM",
        "Process Master Data",
        "Validate and process staged dimension/master data.",
        3,
    ),
    (
        4, "LOAD_TRAN",
        "Process Transactional Data",
        "Validate and process staged transactional data.",
        4,
    ),
    (
        5, "PRE_PROCESS",
        "Functional Logic",
        "Apply business logic and derivations to ingested data.",
        5,
    ),
    (
        6, "PROCESS_DATA",
        "Core Data Transformation",
        "Transform data from fact or master tables into output/data mart tables.",
        6,
    ),
]
