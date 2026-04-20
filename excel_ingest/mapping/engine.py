from __future__ import annotations

from typing import Dict, List, Optional

from excel_ingest.mapping.adapters.base import LLMAdapter
from excel_ingest.mapping.confidence import (
    CanonicalMapping,
    MappingMethod,
    MappingStatus,
    calculate_rule_score,
    determine_status,
    hybrid_confidence,
)
from excel_ingest.metadata import ColumnMetadata, MetadataExtractionResult


def map_to_canonical(
    metadata_result: MetadataExtractionResult,
    canonical_dict: Dict[str, List[str]],
    adapter: Optional[LLMAdapter] = None,
    country_code: Optional[str] = None,
    prior_mappings: Optional[Dict[str, str]] = None,
    skip_blank_columns: bool = True,
) -> List[CanonicalMapping]:
    """Map all columns in a file to caller-supplied canonical field names.

    Args:
        metadata_result:  Output of extract_metadata().
        canonical_dict:   {canonical_field: [alias1, alias2, ...]}.
                          Fully caller-supplied — no hardcoded domain.
        adapter:          Optional LLMAdapter instance. If None, rule-only mode.
        country_code:     ISO-2 country hint passed through to the adapter.
        prior_mappings:   {hierarchical_header: canonical_field} from previous
                          files — boosts confidence for known mappings.
        skip_blank_columns: Skip columns flagged as blank (default True).

    Returns:
        List[CanonicalMapping], one entry per column (excl. blanks if skipped).
    """
    file_id = metadata_result.file_metadata.file_id
    results: List[CanonicalMapping] = []

    for col in metadata_result.column_metadata:
        if skip_blank_columns and col.is_blank_column:
            continue

        header = col.hierarchical_header
        section_hint = f"section_{col.section_id}"

        # Rule-based scoring
        best_field, rule = calculate_rule_score(
            header, canonical_dict, prior_mappings, section_hint
        )

        # LLM scoring (optional)
        llm_field: Optional[str] = None
        llm_conf = 0.0
        llm_reasoning: Optional[str] = None
        method = MappingMethod.RULE_BASED

        if adapter is not None:
            llm_resp = adapter.map_column(
                header=header,
                canonical_dict=canonical_dict,
                section_hint=section_hint,
                country_code=country_code,
            )
            llm_field = llm_resp.canonical_field
            llm_conf = llm_resp.confidence
            llm_reasoning = llm_resp.reasoning
            method = MappingMethod.LLM_ASSISTED

            # Prefer LLM field if its confidence is non-trivial and differs
            final_conf = hybrid_confidence(rule.total, llm_conf)
            canonical_field = llm_field if llm_conf >= 0.5 else best_field
        else:
            final_conf = rule.total
            canonical_field = best_field

        if rule.exact_alias_match > 0 and method == MappingMethod.RULE_BASED:
            method = MappingMethod.EXACT_MATCH

        status = determine_status(final_conf, canonical_field)

        results.append(
            CanonicalMapping(
                file_id=file_id,
                column_index=col.column_index,
                column_letter=col.column_letter,
                hierarchical_header=header,
                bronze_column_name=col.bronze_column_name,
                canonical_field=canonical_field,
                mapping_status=status,
                mapping_method=method,
                final_confidence=final_conf,
                rule_score=rule.total,
                llm_confidence=llm_conf,
                llm_reasoning=llm_reasoning,
            )
        )

    return results
