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
        canonical_dict={
            "order_id":       ["order id", "order no", "transaction id"],
            "product_name":   ["product name", "item name", "description"],
            "store_name":     ["store name", "store", "retail unit"],
        },
    )
"""

from excel_ingest.framework import ExcelIngestFramework, IngestResult
from excel_ingest.validation import FileValidationResult, ValidationStatus, VALIDATION_RECORD_FIELDS
from excel_ingest.structure import FileStructureMetadata, FileProcessingConfig, FileStatus, STRUCTURE_RECORD_FIELDS
from excel_ingest.metadata import MetadataExtractionResult, combine_column_records, build_superset_schema, COLUMN_RECORD_FIELDS, SIGNATURE_RECORD_FIELDS
from excel_ingest.loader import LoadResult
from excel_ingest.mapping import map_to_canonical, CanonicalMapping, MappingStatus, MappingMethod

__all__ = [
    "ExcelIngestFramework",
    "IngestResult",
    "FileValidationResult",
    "ValidationStatus",
    "VALIDATION_RECORD_FIELDS",
    "FileStructureMetadata",
    "FileProcessingConfig",
    "FileStatus",
    "STRUCTURE_RECORD_FIELDS",
    "MetadataExtractionResult",
    "combine_column_records",
    "build_superset_schema",
    "COLUMN_RECORD_FIELDS",
    "SIGNATURE_RECORD_FIELDS",
    "LoadResult",
    "map_to_canonical",
    "CanonicalMapping",
    "MappingStatus",
    "MappingMethod",
]

try:
    from importlib.metadata import version as _pkg_version
    __version__ = _pkg_version("databricks-excel-ingest-framework")
except Exception:
    __version__ = "unknown"
