"""
databricks-excel-ingest-framework
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Databricks-native Excel ingestion: validate → detect structure → extract metadata → map to canonical.

Primary target: Databricks (Unity Catalog Volumes, DBFS, Foundation Models).
Also works outside Databricks with openpyxl only (no LLM mapping).

Install::

    pip install databricks-excel-ingest-framework                  # core only
    pip install databricks-excel-ingest-framework[databricks]      # + Databricks LLM adapter
    pip install databricks-excel-ingest-framework[openai]          # + OpenAI adapter
    pip install databricks-excel-ingest-framework[anthropic]       # + Anthropic adapter
    pip install databricks-excel-ingest-framework[all]             # all adapters

Quick start::

    from excel_ingest import ExcelIngestFramework

    framework = ExcelIngestFramework(spark=spark)
    result = framework.ingest(
        file_path="/Volumes/catalog/schema/volume/data.xlsx",
        canonical_dict={"employee_id": ["emp id", "staff no"], ...},
    )
"""

from excel_ingest.framework import ExcelIngestFramework, IngestResult
from excel_ingest.validation import FileValidationResult, ValidationStatus
from excel_ingest.structure import FileStructureMetadata, FileProcessingConfig, FileStatus
from excel_ingest.metadata import MetadataExtractionResult
from excel_ingest.mapping import map_to_canonical, CanonicalMapping, MappingStatus, MappingMethod

__all__ = [
    "ExcelIngestFramework",
    "IngestResult",
    "FileValidationResult",
    "ValidationStatus",
    "FileStructureMetadata",
    "FileProcessingConfig",
    "FileStatus",
    "MetadataExtractionResult",
    "map_to_canonical",
    "CanonicalMapping",
    "MappingStatus",
    "MappingMethod",
]

try:
    from importlib.metadata import version as _pkg_version
    __version__ = _pkg_version("databricks-excel-ingest-framework")
except Exception:
    __version__ = "0.1.0a1"
