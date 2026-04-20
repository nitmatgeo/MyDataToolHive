# Changelog

## [0.1.0a7] — 2026-04-20

### Added
- **`FileStructureMetadata.header_range`** (`structure.py`): new property returning the header rows
  as an Excel A1-notation range string, e.g. `"A1:L1"` (single header) or `"A1:N2"` (multi-row).
  Returns `None` when no headers are detected.
- **`FileStructureMetadata.data_range`** (`structure.py`): new property returning the data rows as
  an Excel A1-notation range string, e.g. `"A2:L21"`. Returns `None` when there are no data rows.

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
