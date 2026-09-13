---
applyTo: "SQL Server Schema Craft Studio/**"
---

# SQL Server Schema Craft Studio — Copilot Instructions

You are working on the **SQL Server Schema Craft Studio** in `SQL Server Schema Craft Studio/`.

This is a modular PowerShell toolkit that extracts SQL Server schema metadata, compares two schema versions at column level, and produces timestamped audit CSVs.

---

## What this tool does

Takes two raw SQL Server schema metadata dumps (version n and version n−1), processes them through four modules, and outputs three CSV files classifying every column as Added, Deleted, Changed, or Unchanged — with the exact properties that changed.

---

## Module structure

```
SQL Server Schema Craft Studio/
├── msql.PowerShell.SCS.Main.ps1          # Orchestrates the full pipeline
├── Modules/
│   ├── processMetadata/                  # Extract + split + merge raw dumps
│   │   └── msql.PowerShell.SCS.processMetadata.psm1
│   ├── metadataComparator/               # Column-level diff (n vs n-1)
│   │   └── msql.PowerShell.SCS.metadataComparator.psm1
│   ├── comparatorSummary/                # Table + column summary generation
│   │   └── msql.PowerShell.SCS.comparatorSummary.psm1
│   └── Utils/                            # Logging utilities
│       └── msql.PowerShell.SCS.Utils.psm1
```

---

## Data flow

```
Data\Inputs\version (n)\    00.rawSchemaMetadataOutput.dat
                            00.rawHierarchyOutput.dat
Data\Inputs\version (n-1)\  00.rawSchemaMetadataOutput.dat
                            00.rawHierarchyOutput.dat
        │
        ▼ Extract-ValidJsonContent  (trim noise, isolate valid JSON)
        ▼ Process-JSON              (split bulk JSON → Schema.Table.json per table)
        ▼ Merge-JsonFiles           (merge schema + hierarchy into one JSON per table)
        │
        ▼ Load-JsonData             (load per-table JSONs → hashtable keyed by FullFieldName|FileName)
        ▼ Compare-Data              (full outer join n vs n-1; SHA256 + property diff per column)
        │
        ├── ComparisonLog_YYYYMMDD_HHMMSS.csv      (column-level, full detail)
        ├── Summary_YYYYMMDD_HHMMSS.csv            (table counts + per-table column counts)
        └── DetailedSummary_YYYYMMDD_HHMMSS.csv   (table classification + hierarchy + timestamps)
```

---

## Key functions

### processMetadata module

| Function | Purpose |
|----------|---------|
| `Extract-ValidJsonContent` | Reads a raw `.dat` file, finds valid JSON between start/end patterns, returns cleaned JSON string |
| `Process-JSON` | Parses JSON array, writes one `Schema.Table.json` file per table to output folder |
| `Merge-JsonFiles` | Merges schema JSON + hierarchy JSON per table (adds FullTableName, schema_priority, section_level, table_level, Hierarchy) |

### metadataComparator module

| Function | Purpose |
|----------|---------|
| `Load-JsonData` | Loads all per-table JSONs; returns hashtable keyed by `FullFieldName\|FileName` |
| `Compare-Data` | Full outer join of n vs n-1; classifies each column as Added / Deleted / Changed / Unchanged; records changed property names |

### comparatorSummary module

| Function | Purpose |
|----------|---------|
| `Generate-Summary` | Table-level counts (Added/Deleted/Changed/Unchanged) + column-level breakdown for each Changed table |
| `Generate-DetailedSummary` | Table classification with ChangeType, SchemaName, TableName, TableHierarchy, timestamps, ChangedColumns list |
| `Classify-Tables` | Determines table-level ChangeType by inspecting all column actions for that table |

---

## Output columns

### ComparisonLog CSV
| Column | Description |
|--------|-------------|
| `FullFieldName` | `schema.table.column` |
| `FileName` | `Schema.Table.json` |
| `Action` | Added / Deleted / Changed / Unchanged |
| `SHA256Hash_from_n` | Hash of column metadata in current version |
| `SHA256Hash_from_n_minus_1` | Hash of column metadata in previous version |
| `AdditionalRemark` | Human-readable change description |
| `ChangedColumns` | Bracket-delimited list of changed property names |
| `TableHierarchy` | Hierarchy label from hierarchy metadata |
| `TableCreatedOn` | Table creation timestamp |
| `TableUpdatedOn` | Table last-updated timestamp |
| `MetadataExtractedOn` | When the metadata was extracted from SQL Server |

### DetailedSummary CSV
| Column | Description |
|--------|-------------|
| `FullTableName` | `schema.table` |
| `SchemaName` | Schema part |
| `TableName` | Table part |
| `ChangeType` | Added / Deleted / Changed / Unchanged (table level) |
| `TableHierarchy` | Hierarchy label |
| `ChangedColumns` | Comma-separated list of changed column names |
| `TableCreatedOn` / `TableUpdatedOn` / `MetadataExtractedOn` | Timestamps |

---

## Conventions

- **PowerShell 5.1+** — no PowerShell 7-only syntax
- **Newtonsoft.Json** required for JSON serialisation — check `Check-Modules` passes before running
- **Input folder structure is fixed** — `Data\Inputs\version (n)\` and `Data\Inputs\version (n-1)\` must exist with the correct raw files
- **All outputs are timestamped** — never overwrite previous runs; each execution produces new files
- **`-debugMode $true`** enables verbose logging to file and console; always test with debug on first
- **`Log-Message`** is the only logging function — use it consistently, never `Write-Host` or `Write-Output` for operational messages
- **Do not modify raw `.dat` files** — the tool handles messy/partial JSON gracefully via `Extract-ValidJsonContent`
- **`FullFieldName` format is `schema.table.column`** — used as the primary comparison key throughout
