# Sample Excel Files — FreshMart Retail Dataset

Synthetic FreshMart supermarket data covering every structural edge case the framework handles.
All data is fully fabricated.

| File | Structural Scenario | Sheets | Sheet / Column Status |
|------|--------------------|---------|-----------------------|
| `s01_simple_single_sheet.xlsx` | Baseline — simplest possible structure | 1 — `Sales Orders` | 12 cols · 20 data rows · no special features |
| `s02_multi_row_merged_headers.xlsx` | Section labels merged in row 1, column names in row 2 | 1 — `Product Catalogue` | 14 cols · 3 merged ranges (row 1 section headers) · data from row 3 |
| `s03_no_headers.xlsx` | Raw dump — no header row | 1 — `RawTransactions` | 6 cols · data starts row 1 · no column labels |
| `s04_headers_only_no_data.xlsx` | Blank template — headers present, zero data | 1 — `Sales Submission` | 13 cols · header row only · no data rows |
| `s05_multi_sheet_diff_structure.xlsx` | Each sheet has a different schema | 3 — `Orders` / `Products` / `Stores` | Orders: 9 cols · Products: 13 cols · Stores: 10 cols + 3 merged ranges |
| `s06_multi_sheet_same_structure.xlsx` | Same columns across all regional sheets | 4 — `UK` / `US` / `DE` / `AU` | All sheets: 10 cols · identical structure · regional currency per sheet |
| `s07_wide_standard_vs_extended.xlsx` | Partner-facing (lean) vs internal (full commercial detail) | 2 — `Standard` / `Extended` | Standard: 15 cols, 2 hidden (G, O) · Extended: 65 cols, 7 merged section headers, 1 hidden (M) |
| `s08_hidden_sheet.xlsx` | Hidden sheet with sensitive margin data | 3 — `Sales Report` (visible) · `Summary` (visible) · `_Margins` (**hidden**) | Visible sheets: 9 cols + 2 cols · Hidden sheet: 3 cols (cost/margin data) |
| `s09_hidden_columns.xlsx` | Internal pricing columns hidden from recipients | 1 — `Orders` | 13 cols total · 2 hidden — col F (Cost Price), col I (Margin Code) |
| `s10_blank_column_sections.xlsx` | Blank columns used as visual section separators | 1 — `Weekly Summary` | 13 col positions · 3 merged section headers (row 1) · blank cols 5 and 10 as separators |
| `s11_password_protected.xlsx` | File-level AES encryption | 1 — `Confidential Pricing` | 12 cols · **password: `Password1234`** · requires decryption before openpyxl can read |
| `s12_wide_complex_3level_headers.xlsx` | 3-level merged header hierarchy — mirrors complex ERP exports | 2 — `UK` / `US` | UK: 45 cols · US: 40 cols · 3 header rows (category → sub-section → column name) · 16 merged ranges per sheet |

---

**Password for s11:** `Password1234`
