from __future__ import annotations

import os
import shutil
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from excel_ingest.mapping.adapters.base import LLMAdapter
from excel_ingest.mapping.confidence import CanonicalMapping, MappingStatus
from excel_ingest.mapping.engine import map_to_canonical
from excel_ingest.metadata import MetadataExtractionResult, extract_metadata
from excel_ingest.structure import (
    FileProcessingConfig, FileStatus, FileStructureMetadata, analyze_excel_structure,
)
from excel_ingest.validation import FileValidationResult, ValidationStatus, validate_excel_file


@dataclass
class IngestResult:
    """Full pipeline result returned by ExcelIngestFramework.ingest()."""

    file_path: str
    validation: FileValidationResult
    structure: Optional[FileStructureMetadata]
    metadata: Optional[MetadataExtractionResult]
    mappings: List[CanonicalMapping]
    success: bool
    errors: List[str] = field(default_factory=list)

    def mapping_records(self) -> List[Dict[str, Any]]:
        return [m.to_dict() for m in self.mappings]

    def summary_record(self) -> Dict[str, Any]:
        """Aggregate mapping counts for this file — one dict, ready for display() or logging.

        Columns map cleanly to the four MappingStatus values so callers never need
        to import or reference MappingStatus directly::

            display(spark.createDataFrame([result.summary_record()]))
        """
        file_id = self.metadata.file_metadata.file_id if self.metadata else os.path.basename(self.file_path)
        counts: Dict[str, int] = {s.value: 0 for s in MappingStatus}
        for m in self.mappings:
            counts[m.mapping_status.value] += 1
        return {
            "file_id":        file_id,
            "success":        self.success,
            "total_cols":     len(self.mappings),
            "auto_approved":  counts[MappingStatus.AUTO_APPROVED.value],
            "needs_review":   counts[MappingStatus.NEEDS_REVIEW.value],
            "requires_human": counts[MappingStatus.REQUIRES_HUMAN.value],
            "unmapped":       counts[MappingStatus.UNMAPPED.value],
        }

    def metadata_records(self) -> List[Dict[str, Any]]:
        return self.metadata.to_delta_records() if self.metadata else []

    def file_record(self) -> Optional[Dict[str, Any]]:
        return self.metadata.file_metadata.to_dict() if self.metadata else None


class ExcelIngestFramework:
    """Databricks-native Excel ingestion framework.

    Orchestrates a 4-stage pipeline:
      1. validate()         — file existence, format, password, sheets
      2. detect_structure() — merged cells, multi-row headers, blank/hidden columns
      3. extract_metadata() — hierarchical headers, SHA-256 signature, section IDs
      4. map_to_canonical() — hybrid rule + optional LLM confidence mapping

    The framework works on any Python 3.9+ environment with openpyxl.
    Databricks-specific features (Volume/DBFS path detection, Foundation Models)
    are enabled automatically when running inside a Databricks cluster.

    Args:
        spark:   Optional SparkSession — not used by the framework itself but
                 available to callers who want to write Delta tables from results.
        adapter: Optional LLMAdapter instance (DatabricksAdapter / OpenAIAdapter /
                 AnthropicAdapter). If None, the mapping stage uses rules only.

    Examples::

        from excel_ingest import ExcelIngestFramework

        framework = ExcelIngestFramework(spark=spark)
        result = framework.ingest(
            file_path="/Volumes/catalog/schema/vol/data.xlsx",
            canonical_dict={"employee_id": ["emp id", "staff no"], ...},
        )

        # With a Databricks LLM adapter
        from excel_ingest.mapping.adapters.databricks import DatabricksAdapter
        adapter = DatabricksAdapter(model="databricks-llama-3-70b-instruct")
        framework = ExcelIngestFramework(spark=spark, adapter=adapter)
    """

    def __init__(
        self,
        spark=None,
        adapter: Optional[LLMAdapter] = None,
    ) -> None:
        self.spark = spark
        self.adapter = adapter

    # ------------------------------------------------------------------
    # Stage 1
    # ------------------------------------------------------------------

    def validate(
        self,
        file_path: str,
        password: Optional[str] = None,
    ) -> FileValidationResult:
        """Validate file existence, format, password protection, and sheet list."""
        return validate_excel_file(file_path, password)

    # ------------------------------------------------------------------
    # Stage 2
    # ------------------------------------------------------------------

    def detect_structure(
        self,
        file_path: str,
        config: Optional[FileProcessingConfig] = None,
        password: Optional[str] = None,
    ) -> FileStructureMetadata:
        """Detect header rows, merged cells, blank/hidden columns, and data boundaries."""
        return analyze_excel_structure(file_path, config, password)

    # ------------------------------------------------------------------
    # Stage 3
    # ------------------------------------------------------------------

    def extract_metadata(
        self,
        file_path: str,
        structure: Optional[FileStructureMetadata] = None,
        file_id: Optional[str] = None,
        password: Optional[str] = None,
        config: Optional[FileProcessingConfig] = None,
    ) -> MetadataExtractionResult:
        """Extract hierarchical column metadata and generate a header signature.

        If structure is not supplied, detect_structure() is called automatically.
        """
        if structure is None:
            structure = self.detect_structure(file_path, config, password)
        return extract_metadata(file_path, structure, file_id)

    # ------------------------------------------------------------------
    # Stage 4
    # ------------------------------------------------------------------

    def map_to_canonical(
        self,
        metadata: MetadataExtractionResult,
        canonical_dict: Dict[str, List[str]],
        country_code: Optional[str] = None,
        prior_mappings: Optional[Dict[str, str]] = None,
        adapter: Optional[LLMAdapter] = None,
        skip_blank_columns: bool = True,
    ) -> List[CanonicalMapping]:
        """Map column headers to canonical field names.

        Args:
            metadata:        Output of extract_metadata().
            canonical_dict:  {canonical_field: [alias1, alias2, ...]}.
                             Fully caller-supplied — no hardcoded domain.
            country_code:    ISO-2 country hint (e.g. "UK", "US", "DE").
            prior_mappings:  {hierarchical_header: canonical_field} from previous
                             runs — boosts confidence for already-seen headers.
            adapter:         Override the framework-level adapter for this call.
            skip_blank_columns: Skip blank columns (default True).
        """
        effective_adapter = adapter if adapter is not None else self.adapter
        return map_to_canonical(
            metadata_result=metadata,
            canonical_dict=canonical_dict,
            adapter=effective_adapter,
            country_code=country_code,
            prior_mappings=prior_mappings,
            skip_blank_columns=skip_blank_columns,
        )

    # ------------------------------------------------------------------
    # Full pipeline
    # ------------------------------------------------------------------

    def ingest(
        self,
        file_path: str,
        canonical_dict: Dict[str, List[str]],
        config: Optional[FileProcessingConfig] = None,
        password: Optional[str] = None,
        file_id: Optional[str] = None,
        country_code: Optional[str] = None,
        prior_mappings: Optional[Dict[str, str]] = None,
        adapter: Optional[LLMAdapter] = None,
        skip_blank_columns: bool = True,
    ) -> IngestResult:
        """Run all 4 stages in sequence and return a consolidated IngestResult.

        Stops early if validation or structure detection fails.
        """
        errors: List[str] = []

        # Stage 1
        validation = self.validate(file_path, password)
        if validation.status == ValidationStatus.FAILED:
            return IngestResult(
                file_path=file_path,
                validation=validation,
                structure=None,
                metadata=None,
                mappings=[],
                success=False,
                errors=validation.errors,
            )

        # Stage 2
        structure = self.detect_structure(file_path, config, password)
        if structure.status in (FileStatus.EMPTY_FILE, FileStatus.INVALID_STRUCTURE,
                                FileStatus.SHEET_NOT_SPECIFIED):
            errors.append(f"Structure detection failed: {structure.status.value}")
            return IngestResult(
                file_path=file_path,
                validation=validation,
                structure=structure,
                metadata=None,
                mappings=[],
                success=False,
                errors=errors,
            )

        # Stage 3
        metadata = extract_metadata(file_path, structure, file_id)

        # Stage 4
        mappings = self.map_to_canonical(
            metadata=metadata,
            canonical_dict=canonical_dict,
            country_code=country_code,
            prior_mappings=prior_mappings,
            adapter=adapter,
            skip_blank_columns=skip_blank_columns,
        )

        return IngestResult(
            file_path=file_path,
            validation=validation,
            structure=structure,
            metadata=metadata,
            mappings=mappings,
            success=True,
            errors=errors,
        )

    # ------------------------------------------------------------------
    # Developer helpers
    # ------------------------------------------------------------------

    def guide(self) -> None:
        """Print a concise step-by-step usage guide to stdout."""
        W = 70
        print("─" * W)
        print("  databricks-excel-ingest-framework — Usage Guide")
        print("─" * W)
        print("""
STEP 1 — Instantiate the framework
────────────────────────────────────
  from excel_ingest import ExcelIngestFramework

  # Rule-only (no LLM — zero extra dependencies beyond openpyxl)
  framework = ExcelIngestFramework(spark=spark)

  # With Databricks Foundation Models LLM adapter (no endpoint setup needed)
  from excel_ingest.mapping.adapters.databricks import DatabricksAdapter
  adapter = DatabricksAdapter(model="databricks-llama-3-70b-instruct")
  framework = ExcelIngestFramework(spark=spark, adapter=adapter)

  # With OpenAI or Anthropic adapter
  from excel_ingest.mapping.adapters.openai import OpenAIAdapter
  from excel_ingest.mapping.adapters.anthropic import AnthropicAdapter
  adapter = OpenAIAdapter(model="gpt-4o-mini")            # reads OPENAI_API_KEY
  adapter = AnthropicAdapter(model="claude-haiku-4-5-20251001")  # reads ANTHROPIC_API_KEY

STEP 2 — Define your canonical dictionary (any domain)
────────────────────────────────────────────────────────
  # Fully caller-supplied — no hardcoded fields in the package.
  # Keys = canonical field names.  Values = known aliases for that field.
  canonical_dict = {
      "order_id":     ["order id", "order no", "transaction id"],
      "product_name": ["product name", "item name", "description"],
      "store_name":   ["store name", "store", "retail unit"],
      "quantity":     ["qty", "quantity", "units"],
      # ... add as many fields as needed for your domain
  }

STEP 3 — Run the full pipeline (recommended)
──────────────────────────────────────────────
  result = framework.ingest(
      file_path      = "/Volumes/<catalog>/<schema>/<volume>/data.xlsx",
      canonical_dict = canonical_dict,
      country_code   = "UK",          # optional ISO-2 hint for LLM adapter
      file_id        = "ORDERS_2026_Q1",  # optional — defaults to filename
      # config       = FileProcessingConfig(sheet_name="Sheet1"),  # multi-sheet files
      # password     = "secret",      # password-protected files
      # prior_mappings = {...},        # boost confidence for previously-seen headers
  )
  print(result.success)
  for m in result.mappings:
      print(m.hierarchical_header, "→", m.canonical_field, f"({m.final_confidence:.2f})")

  NOTE: sheet_name is MANDATORY for files with more than one visible sheet.

STEP 4 — Inspect confidence buckets
──────────────────────────────────────
  from excel_ingest import MappingStatus

  auto     = [m for m in result.mappings if m.mapping_status == MappingStatus.AUTO_APPROVED]
  review   = [m for m in result.mappings if m.mapping_status == MappingStatus.NEEDS_REVIEW]
  manual   = [m for m in result.mappings if m.mapping_status == MappingStatus.REQUIRES_HUMAN]
  unmapped = [m for m in result.mappings if m.mapping_status == MappingStatus.UNMAPPED]

  Confidence formula:
    With LLM:    final = 0.7 × rule_score + 0.3 × llm_confidence
    Without LLM: final = rule_score
    > 0.9  → AUTO_APPROVED | 0.7–0.9 → NEEDS_REVIEW | < 0.7 → REQUIRES_HUMAN

STEP 5 — Persist results to Delta (optional)
──────────────────────────────────────────────
  spark.createDataFrame([result.file_record()]).write \\
      .mode("append").saveAsTable("`<catalog>`.`<schema>`.`excel_file_metadata`")

  spark.createDataFrame(result.metadata_records()).write \\
      .mode("append").saveAsTable("`<catalog>`.`<schema>`.`excel_column_metadata`")

  spark.createDataFrame(result.mapping_records()).write \\
      .mode("append").saveAsTable("`<catalog>`.`<schema>`.`excel_canonical_mappings`")

STAGE-BY-STAGE (for debugging or inspection)
──────────────────────────────────────────────
  from excel_ingest.structure import FileProcessingConfig

  v = framework.validate(file_path)                          # Stage 1
  s = framework.detect_structure(file_path, config)          # Stage 2
  m = framework.extract_metadata(file_path, s, file_id="x") # Stage 3
  mappings = framework.map_to_canonical(m, canonical_dict)   # Stage 4

OTHER METHODS
─────────────
  framework.guide()                       # this guide
  framework.sample_usage(spark)           # extract sample notebooks to Workspace
""".rstrip())
        print("─" * W)

    def sample_usage(self, spark) -> str:
        """Extract bundled sample notebooks to the current user's Workspace folder.

        Returns the path where the notebooks were extracted.
        Safe to re-run — skips files already up-to-date.

        Usage::

            path = framework.sample_usage(spark)
            print(f"Sample notebooks extracted to: {path}")
        """
        try:
            repo_user = spark.sql("SELECT current_user()").first()[0]
        except Exception:
            repo_user = os.environ.get("USER", "unknown")

        dest = f"/Workspace/Users/{repo_user}/databricks-excel-ingest-framework/sample_usage"
        os.makedirs(dest, exist_ok=True)

        pkg_dir = os.path.dirname(os.path.abspath(__file__))
        bundled = os.path.join(pkg_dir, "sample_usage")

        def _copy_if_newer(src: str, dst: str) -> bool:
            src_mtime = os.path.getmtime(src)
            dst_mtime = os.path.getmtime(dst) if os.path.exists(dst) else 0
            if src_mtime > dst_mtime:
                shutil.copy2(src, dst)
                return True
            return False

        copied: List[str] = []
        if os.path.isdir(bundled):
            # Copy top-level notebook files
            for fname in sorted(os.listdir(bundled)):
                src = os.path.join(bundled, fname)
                if not os.path.isfile(src):
                    continue
                if _copy_if_newer(src, os.path.join(dest, fname)):
                    copied.append(fname)

            # Copy samples/ subfolder (Excel files)
            samples_src = os.path.join(bundled, "samples")
            samples_dst = os.path.join(dest, "samples")
            if os.path.isdir(samples_src):
                os.makedirs(samples_dst, exist_ok=True)
                for fname in sorted(os.listdir(samples_src)):
                    src = os.path.join(samples_src, fname)
                    if not os.path.isfile(src):
                        continue
                    if _copy_if_newer(src, os.path.join(samples_dst, fname)):
                        copied.append(f"samples/{fname}")

        if copied:
            print(f"Extracted {len(copied)} file(s) to: {dest}")
        else:
            print(f"Sample files already up-to-date at: {dest}")

        return dest
