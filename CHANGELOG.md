# Changelog

## [0.1.0a15] — 2026-04-21

### Added
- **`ValidationStatus.description`** (`validation.py`): plain-English property on the enum,
  consistent with `FileStatus.description` and `MappingStatus.description`. Values: PASSED,
  WARNING, FAILED each carry a self-explanatory sentence.
- **`FileValidationResult.summary_record(label="")`** (`validation.py`): flat dict for Spark
  DataFrame display — one row per file. Fields: label, file, status, status_description,
  file_exists, format_type, file_size_bytes, is_readable, is_password_protected, total_sheets,
  visible_sheets, warnings, errors.
- **`FileStructureMetadata.summary_record(file_path="", label="")`** (`structure.py`): flat
  dict for Spark DataFrame display — one row per file/sheet. Fields: label, file, sheet_name,
  status, status_description, is_actionable, total_rows, total_cols, data_row_count,
  header_rows, header_range, data_range, merged_regions, blank_columns, hidden_columns.

### Changed
- **`02-validate.py`**: fully converted from `print()` to `display(spark.createDataFrame(...))`.
  Added "Validation Status Reference" cell (status enum table). "Validate Each File" and
  "Negative Examples" now use `summary_record()`. "Summary by Status" is a grouped DataFrame.
- **`03-structure.py`**: "Detect Structure for Each File" converted to DataFrame using
  `summary_record()`. Added "Files Needing Action" filter cell (`is_actionable = true`).
  Removed unused `FileStatus` import.
- **`04-metadata.py`**: "Extract Metadata for Each File" print loop replaced with DataFrame
  (`file_metadata.to_dict()` + `total_loadable_cols`). "Bronze Schema for a Single File"
  enriched to show `column_letter` and `hierarchical_header` alongside
  `db_canonical_bronze_column_name` so Excel origin is never ambiguous. "Superset Schema"
  and "Multi-Sheet Iteration" print calls removed. "Verify — List Files in Volume"
  (`01-install.py`) converted to DataFrame.

## [0.1.0a14] — 2026-04-20

### Fixed
- **SyntaxError in `guide()`** (`framework.py`): `spark.sql(f"""...""")` inside the
  triple-quoted guide string closed the outer string early. Replaced with a single-line
  `ddl = f"..."` assignment.
- **Domain references removed** from all live code and docs — replaced with
  FreshMart retail context (`order_id`, `product_name`, `store_name`, `orders_bronze`).
  Affected: `framework.py` (docstring + guide), `metadata.py` (two docstrings),
  `CLAUDE.md`, `README.md` (problem description + quick start).

## [0.1.0a13] — 2026-04-20

### Added
- **`MetadataExtractionResult.bronze_schema()`** (`metadata.py`): returns
  `{db_canonical_bronze_column_name: column_index}` for all non-blank columns. Use to
  build the NULL-safe per-file SELECT when loading into a superset bronze table.
- **`build_superset_schema(results)`** (`metadata.py`, exported from `excel_ingest`):
  returns sorted list of all distinct `db_canonical_bronze_column_name` values across a
  list of `MetadataExtractionResult`. Designed for multi-file consolidation (e.g. 60 country
  files) where some columns are GDPR-suppressed or country-specific — the superset table
  holds all columns; missing ones load as NULL.
- **`04-metadata.py` Bronze Schema cells**: three new cells — single-file `bronze_schema()`
  walkthrough, `build_superset_schema()` across all 12 sample files, and DDL generation.
- **Updated `guide()`** (`framework.py`): added Step 5 (bronze vs silver distinction),
  Step 6 (superset bronze table pattern with `build_superset_schema()` + `bronze_schema()`);
  updated all examples to use DataFrame output.
- **`/excel-ingest` skill** (`.claude/commands/excel-ingest.md`): fully rewritten to
  reflect current architecture, patterns, and rules — including bronze/silver split,
  `db_canonical_bronze_column_name`, `requires_action` filter pattern, and what to update
  on each change type.

## [0.1.0a12] — 2026-04-20

### Added
- **`ColumnMetadata.db_canonical_bronze_column_name`** (`metadata.py`): unique, SQL-safe
  Delta column name generated from the hierarchical header during Stage 3. Name reflects
  its full role: database-level, canonical, bronze-layer column identifier.
  Strategy: leaf segment only when unique across the sheet; escalates to full path
  (levels joined with `__`) for duplicates; appends `_N` as a last-resort fallback for
  verbatim-repeated headers.
- **`_sanitise_level()`** (`metadata.py`): sanitises one hierarchy level to an identifier
  fragment. Rules: `&`→`and`, `%`→`pct`, all other non-alphanumeric→`_`, squeeze/strip
  underscores, prefix `col_` if result starts with a digit.
- **`_assign_bronze_names()`** (`metadata.py`): two-pass function that resolves uniqueness
  across all columns in a sheet before assigning final names.
- **`db_canonical_bronze_column_name` in `column_records()` and `to_delta_records()`**
  (`metadata.py`): both output methods now include the field.
- **`MetadataExtractionResult.bronze_schema()`** (`metadata.py`): returns
  `{db_canonical_bronze_column_name: column_index}` for all non-blank columns in a file.
  Use to build the NULL-safe per-file SELECT when loading into a superset bronze table.
- **`build_superset_schema(results)`** (`metadata.py`, exported from `excel_ingest`):
  returns sorted list of all distinct `db_canonical_bronze_column_name` values across a
  list of results. Designed for multi-file consolidation (e.g. 60 country HR files) where
  some columns are GDPR-suppressed or country-specific — missing columns load as NULL.
  `all_cols = build_superset_schema(all_sheet1_metadata)` → DDL-ready column list.
- **`CanonicalMapping.column_letter`** (`mapping/confidence.py`): Excel column letter added
  to the mapping dataclass and `to_dict()` output.
- **`CanonicalMapping.db_canonical_bronze_column_name`** (`mapping/confidence.py`): the
  bronze column name flows through to the mapping output so `mapping_records()` is the
  single table a developer needs: Excel column → bronze name → canonical field → status.

### Architecture clarification
- **Bronze = as-is file structure**: the framework produces the bronze schema
  (`db_canonical_bronze_column_name`) and the mapping spec
  (`db_canonical_bronze_column_name → canonical_field`). Silver-layer concerns
  (unpivoting, renaming, merging duplicate section columns) are out of scope.
- **`canonical_field` name kept**: not renamed to `db_canonical_field` — the field name
  is not database-specific; the framework does not own persistence.

## [0.1.0a11] — 2026-04-20

### Added
- **`MetadataExtractionResult.signature_record()`** (`metadata.py`): minimal dict — `file_id`,
  `file_name`, `sheet_name`, `total_cols`, `header_signature` — for schema-change tracking.
  Persist to `excel_schema_signatures` after each run; compare on the next run to detect drift.
- **`combine_column_records(results)`** (`metadata.py`, exported from `excel_ingest`): flattens
  `column_records()` across a list of results into one call.
  `display(spark.createDataFrame(combine_column_records(all_metadata)))`
- **`MappingStatus.description`** (`mapping/confidence.py`): plain-English explanation of each
  status value including the confidence threshold and required action. Surfaces in the notebook
  status reference table so users never need to look up internal enum values.
- **`MappingStatus.requires_action`** (`mapping/confidence.py`): `True` for `NEEDS_REVIEW`,
  `REQUIRES_HUMAN`, and `UNMAPPED` — any status where a human must act before the column can
  be safely loaded. `False` for `AUTO_APPROVED` only.
- **`status_description` and `requires_action` in `CanonicalMapping.to_dict()`**
  (`mapping/confidence.py`): both fields are now included in the `mapping_records()` DataFrame
  output — callers never need to import or switch on `MappingStatus` to act on results.
- **`IngestResult.summary_record()`** (`framework.py`): one-dict aggregate per file —
  `file_id`, `success`, `total_cols`, `auto_approved`, `needs_review`, `requires_human`,
  `unmapped`. Enables a one-liner cross-file summary:
  `display(spark.createDataFrame([r.summary_record() for r in all_results]))`.
- **`05-mapping.py` Mapping Status Reference cell**: displays all `MappingStatus` values with
  `description` and `requires_action` as a Spark DataFrame — users understand the output
  without consulting external docs.
- **`05-mapping.py` Summary cells**: each file now shows `result.summary_record()` as a
  DataFrame before the per-column detail — `MappingStatus` no longer imported in the notebook.
- **`05-mapping.py` All Files Summary cell**: `display(spark.createDataFrame([r.summary_record() for r in all_results]))` — one row per file, all five files.
- **`05-mapping.py` Combined mapping detail cell**: all mapping records across all 5 files in
  one scrollable DataFrame — filterable by `file_id`, `mapping_status`, or `requires_action`.

### Changed
- **`column_group`** (`metadata.py`): `column_records()` field renamed from `header_section`
  to `column_group` — partitions columns by blank separator columns in the sheet.
- **`password` removed from `extract_metadata()`** (`metadata.py`): parameter was never used
  at Stage 3 — all data is already in the `structure` object. Safe for password-protected
  files; password is only needed at Stage 2 (`detect_structure`).
- **Deferred imports promoted to module level** (`metadata.py`, `framework.py`): `import os`,
  `get_column_letter`, `FileStatus`, `import shutil` moved from inside function bodies to top.
- **Type hints on internal helpers** (`metadata.py`): `List[MergedCellInfo]` added to
  `_merge_span_for_col` and `_get_horizontal_merge_value`.
- **`guide()` domain updated** (`framework.py`): example canonical_dict and file_id updated
  from HR to FreshMart retail.
- **`04-metadata.py` Full Column Listing**: replaced manual loop with
  `display(spark.createDataFrame(combine_column_records(all_metadata)))`.
- **`04-metadata.py` Multi-Sheet Iteration**: rewritten to output two Spark DataFrames —
  sheet summary (with `schema_group` and `is_header_unique`) and full column listing.
  `schema_group` = first sheet in workbook tab order sharing that signature;
  `is_header_unique` = True for that anchor sheet only.
- **`04-metadata.py` Signature Comparison**: replaced print loop with
  `display(spark.createDataFrame([m.signature_record() for m in all_metadata]))`.
- **`05-mapping.py`**: removed `_print_result()` print helper; all output is now Spark
  DataFrames. `MappingStatus` import removed from the notebook — status semantics are
  fully self-contained in the DataFrame columns (`mapping_status`, `status_description`,
  `requires_action`).

### Fixed
- **Rule-based alias matching** (`mapping/confidence.py`): matching previously ran against
  the full flattened hierarchical path (e.g. `"transaction & customer  customer identity
  customer name"`), causing false positives — `"customer id"` is a substring of
  `"customer identity"` and incorrectly won over the correct `"customer name"` match.
  New helper `_extract_leaf()` extracts the last `[…]` segment; matching now runs only
  against the leaf (most specific column label). Exact alias match weight raised from
  `0.4` to `0.6` so it outscores partial substring matches (e.g. `"customer"` in
  `"customer segment"`) and the correct canonical field wins in tie situations.

## [0.1.0a10] — 2026-04-20

### Added
- **`MetadataExtractionResult.signature_record()`** (`metadata.py`): returns a minimal dict
  — `file_id`, `file_name`, `sheet_name`, `total_cols`, `header_signature` — designed for
  schema-change tracking. Persist to a `excel_schema_signatures` reference table after each
  ingest run and compare on subsequent runs to detect column layout drift without re-mapping.
- **`MetadataExtractionResult.column_records()`** (`metadata.py`): returns column metadata as
  a flat list of dicts with human-readable keys (`file_id`, `file_name`, `col_index`,
  `col_letter`, `hierarchical_header`, `header_section`, `is_blank`, `is_hidden`,
  `is_merged`, `merge_span`). Intended for display and inspection; use `to_delta_records()`
  for Delta persistence.
- **`combine_column_records(results)`** (`metadata.py`, exported from `excel_ingest`): flattens
  `column_records()` across a list of `MetadataExtractionResult` into one list. Enables a
  one-liner for displaying all files: `display(spark.createDataFrame(combine_column_records(all_metadata)))`.

### Changed
- **`header_section` column name**: the section field in `column_records()` is named
  `header_section` (was `section` in the notebook-level code) — clarifies it partitions
  columns by blank-column separators within the header structure.
- **`04-metadata.py` Full Column Listing cell**: replaced manual loop with single
  `display(spark.createDataFrame(combine_column_records(all_metadata)))` call.
- **`password` removed from `extract_metadata()`** (`metadata.py`): parameter was accepted
  but never used — the workbook is not re-opened at Stage 3; all data comes from the
  already-extracted `structure`. Callers on `framework.extract_metadata()` are unaffected
  (the method-level `password` is still accepted for the `structure=None` auto-detect path).
- **Deferred imports promoted to module level** (`metadata.py`, `framework.py`): `import os`,
  `get_column_letter`, `FileStatus`, and `import shutil` moved from inside function/method
  bodies to top-level imports.
- **Type hints added to internal helpers** (`metadata.py`): `_merge_span_for_col` and
  `_get_horizontal_merge_value` now declare `List[MergedCellInfo]` for `merged_cells`.
- **`guide()` domain updated** (`framework.py`): canonical_dict example changed from HR
  (`employee_id`, `first_name`) to FreshMart retail (`order_id`, `product_name`,
  `store_name`, `quantity`). `file_id` example updated to `ORDERS_2026_Q1`.

## [0.1.0a9] — 2026-04-20

### Fixed
- **Merged parent header propagation** (`metadata.py`): sibling columns within a horizontally
  merged parent header now carry the parent prefix in their `hierarchical_header`. Previously
  only the first column received `[Parent].[Child]`; all others showed `[Child]` alone.
  New helper `_get_horizontal_merge_value()` looks up the top-left cell value for any column
  sitting inside a same-row merge. Vertical merges are excluded to avoid duplicating values
  across header levels.
- **`file_name` column in full column listing** (`04-metadata.py`): renamed `"file"` →
  `"file_name"` in the `display()` DataFrame for clarity.

## [0.1.0a8] — 2026-04-20

### Added
- **`FileStructureMetadata.header_range`** (`structure.py`): new property returning the header rows
  as an Excel A1-notation range string, e.g. `"A1:L1"` (single header) or `"A1:N2"` (multi-row).
  Returns `None` when no headers are detected.
- **`FileStructureMetadata.data_range`** (`structure.py`): new property returning the data rows as
  an Excel A1-notation range string, e.g. `"A2:L21"`. Returns `None` when there are no data rows.
- **`FileStatus.description`** (`structure.py`): new property on each enum member returning a
  plain-English explanation of the status and what action (if any) is required. Surfaces inline in
  notebook output so users never need to look up internal status codes.
- **`FileStatus.is_actionable`** (`structure.py`): new property returning `True` for statuses that
  require caller action before the pipeline can proceed (`EMPTY_FILE`, `INVALID_STRUCTURE`,
  `SHEET_NOT_SPECIFIED`). Used by the notebook icon logic and available to any calling code.

### Fixed
- **Header auto-detection rewritten** (`structure.py`): previous algorithm stopped only on
  population gaps — failing on dense data where both header and data rows are >30 % full. New
  algorithm adds a data-type check (`_row_is_data`): a row containing >35 % numeric, date, or
  structured-ID cells (e.g. `ORD-xxxx`) is classified as a data row and stops the header scan.
  Single-header files no longer require `static_header_rows=[1]` hints.
- **Low-confidence warning**: when auto-detection exhausts `max_rows_to_scan` without finding a
  clear data boundary, a warning is added to `FileStructureMetadata.messages` advising the user to
  set `static_header_rows` in `FileProcessingConfig`.
- **`dbutils.fs.ls` silent swallow** (`validation.py`): failure now appended to messages instead
  of silently caught.
- **`_get_dbutils()` exception scope** (`validation.py`): narrowed from `except Exception` to
  `except ImportError` — only IPython-not-installed is expected; other errors now propagate.
- **LLM `_parse_llm_json` error detail** (all three adapters): `"Could not parse LLM response."`
  now includes `str(exc)` so the specific parse error (missing key, wrong type, etc.) is visible.
- **Explicit ImportError for missing `msoffcrypto-tool`** (`validation.py`, `structure.py`): if the
  package is somehow absent, the error now reads
  `"msoffcrypto-tool is required to open AES-encrypted Excel files. Run: pip install msoffcrypto-tool"`
  instead of a cryptic openpyxl fallback failure.
- **`ImportError` routed separately in `_check_password_and_sheets`**: previously a missing-package
  error could be misclassified as a wrong-password error. Now surfaces its own message.
- **Status icon mapping** (`03-structure.py`): icon now driven by `FileStatus.is_actionable`
  instead of a hardcoded enum check. `EMPTY_FILE` and `SHEET_NOT_SPECIFIED` correctly show
  `[WARN]`; `NO_HEADERS` and `NO_DATA` show `[INFO]`.
- **Status description shown inline** (`03-structure.py`): each file's status line now includes
  `FileStatus.description` so the meaning and next step are visible without consulting docs.
- **Status and Status Description on separate lines** (`03-structure.py`): `Status` carries the
  machine-readable enum value; `Status Description` carries the plain-English explanation —
  allowing orchestration code to consume the status independently of the description.
- **Notebook output labels standardised to Title Case** (`03-structure.py`): all output labels
  (e.g. `Header Rows`, `Data Starts`, `Merged Regions`, `Header Range`) now use consistent
  Title Case formatting.

## [0.1.0a4] — 2026-04-20

### Fixed
- **AES password decryption** (`validation.py`, `structure.py`): openpyxl's `password` kwarg
  handles worksheet-level protection only. File-level AES encryption (created by Office/msoffcrypto)
  is now decrypted with `msoffcrypto-tool` to an `io.BytesIO` buffer before passing to openpyxl.
  Falls back to openpyxl `password` kwarg when msoffcrypto is not installed.
- **`if password is not None:` guard** (`structure.py`): changed from `if password:` so an empty
  string `""` is not treated as "no password".
- **`sample_usage()` copies `samples/` subfolder** (`framework.py`): previously only top-level
  notebook files were extracted; now the `samples/` subdirectory (bundled Excel files) is also
  copied to the Workspace destination.
- **`pyproject.toml` package-data** now includes `"sample_usage/samples/*"` so the 12 sample
  Excel files are bundled in the wheel.
- **`openpyxl` dependency**: removed `--no-deps` install flag; `openpyxl` is now correctly
  installed as a declared dependency.
- **`msoffcrypto-tool` added as a base dependency**: required for file-level AES decryption.
  Without it, `password=` on AES-encrypted files fails silently with openpyxl's fallback.

### Changed
- **Sample notebooks (01–05)** rebuilt as FreshMart retail data demo:
  - `01-install.py`: consolidated setup — pip install, variable definitions, schema/volume creation,
    `sample_usage()`, copy Excel files to Volume, verify listing.
  - `02-validate.py`: validates all 12 sample files + 6 negative cases (missing file, wrong catalog,
    wrong extension, no password, wrong password, correct password).
  - `03-structure.py`: detects structure for all 12 files with correct sheet/header hints.
  - `04-metadata.py`: extracts hierarchical metadata + signature comparison across all 12 files.
  - `05-mapping.py`: FreshMart canonical dict (33 fields), runs `ingest()` on S01/S02/S07/S11/S12.
- **Sample Excel files** (`sample_usage/samples/`): replaced HR domain with FreshMart supermarket
  retail data (orders, products, stores, financials). Includes 2 wide files (s07: 55 cols, s12: 45
  cols with 3-level merged headers). Password on s11 changed to `Password1234`.

## [0.1.0a1] — 2026-04-19

### Added
- Initial pre-release alpha.
- `ExcelIngestFramework` — main class with full 4-stage pipeline.
- Stage 1: `validation.py` — file existence, format, password detection, sheet listing.
- Stage 2: `structure.py` — auto-detection of merged cells, multi-row headers, hidden/blank columns.
- Stage 3: `metadata.py` — hierarchical header reconstruction (`[Parent].[Child]`), SHA-256 signature, section detection.
- Stage 4: `mapping/engine.py` — hybrid confidence mapping (70 % rule-based + 30 % LLM).
- Pluggable LLM adapters: Databricks Foundation Models, OpenAI, Anthropic (all optional extras).
- `utils/paths.py` — `FileLocationType` enum + `detect_location_type()` supporting Unity Catalog Volumes, DBFS, Azure Storage, local paths.
- Numbered `sample_usage/` notebooks (01–05).
