# SQL Server Schema Craft Studio

[![LinkedIn](https://img.shields.io/badge/LinkedIn-nitmatgeo-0A66C2?logo=linkedin)](https://linkedin.com/in/nitmatgeo)
[![Article](https://img.shields.io/badge/Article-SQL%20Server%20Schema%20Craft%20Studio-0A66C2?logo=linkedin)](https://www.linkedin.com/pulse/datatoolhive-sql-server-schema-craft-studio-nitmatgeo-mathew-george-dlrwc)

A modular PowerShell toolkit for SQL Server schema change tracking. Compares two schema versions at column level — every field checked property by property, SHA256-verified — and outputs timestamped CSVs classifying what was Added, Deleted, Changed, or Unchanged.

---

## How it works (overview)

```
Step 0  →  Run SQL scripts (SQLCMD) against SQL Server  →  generates raw .dat dumps
Step 1  →  Extract valid JSON from raw dumps             →  per-table JSON files
Step 2  →  Merge schema + hierarchy JSON per table       →  enriched JSON files
Step 3  →  Compare version (n) vs version (n-1)          →  ComparisonLog CSV
Step 4  →  Summarise by table and column                 →  Summary + DetailedSummary CSVs
```

---

## Step 0 — Generate raw metadata dumps from SQL Server

Before running the PowerShell scripts, you must extract schema metadata from your SQL Server database using the two SQLCMD scripts in `Scripts/`.

Both scripts use SQLCMD output directives (`:SETVAR` and `:OUT`) to write query results directly to `.dat` files. These directives are commented out by default — uncomment them before running.

**Run in SQLCMD mode** (SSMS: Query → SQLCMD Mode, or `sqlcmd` from terminal).

### SQL.01 — Schema Metadata Extract

Extracts all column-level metadata: data types, nullability, FK relationships, extended properties, and a SHA256 hash per column.

1. Open `Scripts\SQL.01.Schema Metadata Extract.sql`
2. Uncomment and set the output path:
   ```sql
   :SETVAR OutputPath "C:\...\SQL Server Schema Craft Studio\Data\Inputs\version (n)"
   ```
3. Uncomment the output directive near the end:
   ```sql
   :OUT $(OutputPath)\00.rawSchemaMetadataOutput.dat
   SELECT table_metadata FROM #MetadataSQLDB_JSON
   ```
4. Run against your target database → produces `00.rawSchemaMetadataOutput.dat`

### SQL.02 — Generate Table Sequences (Hierarchy)

Computes the table dependency hierarchy across the schema — section level, table level, schema priority — based on FK relationships.

1. Open `Scripts\SQL.02.Generate Table Sequences.sql`
2. Uncomment and set the output path:
   ```sql
   :SETVAR OutputPath "C:\...\SQL Server Schema Craft Studio\Data\Inputs\version (n)"
   ```
3. Uncomment the output directive near the end:
   ```sql
   :OUT $(OutputPath)\00.rawHierarchyOutput.dat
   SELECT table_hierarchy FROM ...
   ```
4. Run against the same database → produces `00.rawHierarchyOutput.dat`

**Repeat both scripts for `version (n-1)`**, pointing `OutputPath` to the `version (n-1)` folder.

---

## Prerequisites

1. **PowerShell Version**: Ensure you are using PowerShell 5.1 or later.
2. **NuGet Provider**: The script attempts to install the `NuGet` provider if it is not already installed.
3. **Internet Access**: Required to download the `Newtonsoft.Json` package from NuGet (used for JSON serialisation).
4. **SQL Server + SQLCMD**: Required to run Step 0. Use SSMS with SQLCMD Mode enabled, or the `sqlcmd` command-line tool.

## Modular PowerShell Scripts

### What Are Modular PowerShell Scripts?

Modular PowerShell scripts are scripts that are broken down into smaller, reusable components. Each component is responsible for a specific task and can be used independently or as part of a larger workflow. This approach improves maintainability, readability, and scalability.

### Benefits of Modular PowerShell Scripts

1. **Separation of Concerns**:
   - Each script focuses on a specific functionality, making it easier to understand and debug.

2. **Reusability**:
   - Functions and logic can be reused across multiple workflows or projects.

3. **Scalability**:
   - New features can be added without modifying existing scripts, reducing the risk of introducing bugs.

4. **Maintainability**:
   - Smaller, focused scripts are easier to maintain and update.

5. **Collaboration**:
   - Modular scripts make it easier for teams to work on different parts of the project simultaneously.

---

### Modular Structure of This Project

```
SQL Server Schema Craft Studio/
├── msql.PowerShell.SCS.Main.ps1          # Orchestrates the full pipeline
├── Scripts/
│   ├── SQL.01.Schema Metadata Extract.sql    # SQLCMD: extracts column metadata → rawSchemaMetadataOutput.dat
│   └── SQL.02.Generate Table Sequences.sql   # SQLCMD: computes table hierarchy → rawHierarchyOutput.dat
└── Modules/
    ├── processMetadata/                  # Extract valid JSON + split + merge raw dumps
    │   └── msql.PowerShell.SCS.processMetadata.psm1
    ├── metadataComparator/               # Column-level diff (n vs n-1), SHA256-verified
    │   └── msql.PowerShell.SCS.metadataComparator.psm1
    ├── comparatorSummary/                # Table + column summary generation
    │   └── msql.PowerShell.SCS.comparatorSummary.psm1
    └── Utils/                            # Logging (Log-Message)
        └── msql.PowerShell.SCS.Utils.psm1
```

1. **`SchemaMetadataExtract.ps1`**:
   - Handles JSON extraction and processing for schema metadata.
   - Functions:
     - `Initialize-Script`: Initializes paths and folders for processing.
     - `Process-JSON`: Extracts and processes JSON files.

2. **`SchemaMetadataComparator.ps1`**:
   - Compares schema metadata between two versions (`n` and `n-1`).
   - Functions:
     - `Load-JsonData`: Loads JSON data for comparison.
     - `Compare-Data`: Compares JSON data and generates a detailed comparison log.

3. **`SchemaMetadataSummary.ps1`**:
   - Analyzes the comparison log and generates a summary of changes.
   - Functions:
     - `Load-CSV`: Loads the comparison log as a CSV file.
     - `Classify-Tables`: Classifies tables into categories (e.g., `Newly Added`, `Deleted`, `Unchanged`, `Changed`).
     - `Output-Results`: Outputs the summary to a CSV file.

4. **`Utils.ps1`**:
   - Contains shared utility functions used across the project.
   - Functions:
     - `Log-Error`: Logs errors with timestamps for debugging.

5. **`Main.ps1`**:
   - Orchestrates the entire workflow by calling functions from the modular scripts.
   - Steps:
     1. Initializes paths and dependencies.
     2. Processes JSON files for both versions (`n` and `n-1`).
     3. Compares the processed JSON files.
     4. Generates a summary of the comparison.

---

### How to Use the Modular Scripts

1. **Run the Main Script**:
   - Use `Main.ps1` to execute the entire workflow:
     ```powershell
     .\Main.ps1 -rootPath "C:\Path\To\Project" -versionN "version (n)" -versionNMinus1 "version (n-1)" -rawFileName "00.rawDumpOutput.json" -comparisonLogFile "DetailedComparisonLog.csv" -summaryOutputFile "TableClassificationOutput.csv" -debugMode $true
     ```

2. **Use Individual Scripts**:
   - You can also run individual scripts for specific tasks:
     - Extract schema metadata:
       ```powershell
       .\SchemaMetadataExtract.ps1 -rootPath "C:\Path\To\Project" -version "version (n)" -rawFileName "00.rawDumpOutput.json" -debugMode $true
       ```
     - Compare schema metadata:
       ```powershell
       .\SchemaMetadataComparator.ps1 -versionNFolder "C:\Path\To\Project\version (n)" -versionNMinus1Folder "C:\Path\To\Project\version (n-1)" -logFile "DetailedComparisonLog.csv" -debugMode $true
       ```
     - Generate summary:
       ```powershell
       .\SchemaMetadataSummary.ps1 -logFile "DetailedComparisonLog.csv" -outputFile "TableClassificationOutput.csv" -debugMode $true
       ```

---

### Example Workflow

1. **Extract Schema Metadata**:
   - Extract JSON files for both `version (n)` and `version (n-1)` using `SchemaMetadataExtract.ps1`.

2. **Compare Versions**:
   - Compare the extracted JSON files using `SchemaMetadataComparator.ps1` to generate a detailed comparison log.

3. **Generate Summary**:
   - Analyze the comparison log using `SchemaMetadataSummary.ps1` to classify tables and output the results.

---

### Debugging and Logging

- Enable debugging by setting the `-debugMode` parameter to `$true`.
- Errors are logged with timestamps using the `Log-Error` function in `Utils.ps1`.

---

## Dependencies

- **Newtonsoft.Json**:
  - Required for JSON serialization and deserialization.
  - Refer to the Manual Installation of `Newtonsoft.Json` section for installation instructions.


### Manual Installation of `Newtonsoft.Json`

If the script fails to install the `Newtonsoft.Json` module automatically, follow these steps to manually install and configure it:

#### Step 1: Download the `Newtonsoft.Json` Package
1. Visit the [Newtonsoft.Json NuGet page](https://www.nuget.org/packages/Newtonsoft.Json/).
2. Download the `.nupkg` file for the latest version of `Newtonsoft.Json`.

#### Step 2: Extract the Package
1. Rename the downloaded `.nupkg` file to `.zip`.
2. Extract the contents of the `.zip` file using an archive tool (e.g., WinRAR, 7-Zip).

#### Step 3: Locate the DLL
1. Navigate to the extracted folder.
2. Locate the `Newtonsoft.Json.dll` file in the `lib/net45/` directory (or the appropriate folder for your environment).

#### Step 4: Update the Script
1. Open the `Schema Metadata Extract.ps1` script in a text editor.
2. Locate the following section:
   ```powershell
   Add-Type -Path "C:\Users\...\WindowsPowerShell\Modules\newtonsoft.json.13.0.3.nupkg\lib\net45\Newtonsoft.Json.dll"
   Install-Module -Name Newtonsoft.Json -Scope CurrentUser -Force
   Get-Package -Name Newtonsoft.Json 
   
   <!-- # Load the Newtonsoft.Json assembly
   $packagePath = (Get-Package -Name Newtonsoft.Json).Source
   Add-Type -Path (Join-Path -Path $packagePath -ChildPath "lib/net45/Newtonsoft.Json.dll") -->

---

### Conclusion

This modular approach ensures that the project is easy to maintain, extend, and debug. Each script can be used independently or as part of the larger workflow, making it flexible for different use cases.
