from __future__ import annotations

import re
from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, List, Optional


class MappingStatus(Enum):
    AUTO_APPROVED = "AUTO_APPROVED"     # confidence > 0.9
    NEEDS_REVIEW = "NEEDS_REVIEW"       # 0.7 – 0.9
    REQUIRES_HUMAN = "REQUIRES_HUMAN"   # < 0.7
    UNMAPPED = "UNMAPPED"               # no candidate found

    @property
    def description(self) -> str:
        return {
            MappingStatus.AUTO_APPROVED:  "Confidence > 0.90 — mapping accepted automatically; safe to load.",
            MappingStatus.NEEDS_REVIEW:   "Confidence 0.70–0.90 — likely correct but a human should confirm before loading.",
            MappingStatus.REQUIRES_HUMAN: "Confidence < 0.70 — low confidence; must be manually reviewed and corrected.",
            MappingStatus.UNMAPPED:       "No matching canonical field found — column will be excluded unless manually mapped.",
        }[self]

    @property
    def requires_action(self) -> bool:
        """True for statuses that need human attention before the mapping can be trusted."""
        return self in (MappingStatus.NEEDS_REVIEW, MappingStatus.REQUIRES_HUMAN, MappingStatus.UNMAPPED)


class MappingMethod(Enum):
    EXACT_MATCH = "EXACT_MATCH"
    RULE_BASED = "RULE_BASED"
    LLM_ASSISTED = "LLM_ASSISTED"
    MANUAL = "MANUAL"


THRESHOLD_AUTO = 0.9
THRESHOLD_REVIEW = 0.7
WEIGHT_RULE = 0.7
WEIGHT_LLM = 0.3


@dataclass
class RuleScore:
    exact_alias_match: float = 0.0      # +0.40 exact alias hit
    section_match: float = 0.0          # +0.20 section keyword hit
    prior_mapping: float = 0.0          # +0.30 seen before with same mapping
    total: float = 0.0


@dataclass
class CanonicalMapping:
    file_id: str
    column_index: int
    column_letter: str
    hierarchical_header: str
    db_canonical_bronze_column_name: str
    canonical_field: Optional[str]
    mapping_status: MappingStatus
    mapping_method: MappingMethod
    final_confidence: float
    rule_score: float
    llm_confidence: float
    llm_reasoning: Optional[str]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "file_id":             self.file_id,
            "column_index":        self.column_index,
            "column_letter":       self.column_letter,
            "hierarchical_header": self.hierarchical_header,
            "db_canonical_bronze_column_name":  self.db_canonical_bronze_column_name,
            "canonical_field":     self.canonical_field or "",
            "mapping_status":      self.mapping_status.value,
            "status_description":  self.mapping_status.description,
            "requires_action":     self.mapping_status.requires_action,
            "mapping_method":      self.mapping_method.value,
            "final_confidence":    round(self.final_confidence, 4),
            "rule_score":          round(self.rule_score, 4),
            "llm_confidence":      round(self.llm_confidence, 4),
            "llm_reasoning":       self.llm_reasoning or "",
        }


def determine_status(confidence: float, canonical_field: Optional[str]) -> MappingStatus:
    if canonical_field is None:
        return MappingStatus.UNMAPPED
    if confidence >= THRESHOLD_AUTO:
        return MappingStatus.AUTO_APPROVED
    if confidence >= THRESHOLD_REVIEW:
        return MappingStatus.NEEDS_REVIEW
    return MappingStatus.REQUIRES_HUMAN


def _extract_leaf(header: str) -> str:
    """Return the most specific (leaf) segment of a hierarchical header.

    For "[Parent].[Mid].[Leaf]" returns "Leaf"; for "[Header]" returns "Header".
    Matching on the leaf avoids false positives from parent labels — e.g. the alias
    "customer id" is a substring of "Customer Identity" in a flattened path but
    should never match a column whose leaf is "Customer Name".
    """
    parts = re.findall(r"\[([^\]]+)\]", header)
    return parts[-1] if parts else header


def calculate_rule_score(
    header: str,
    canonical_dict: Dict[str, List[str]],
    prior_mappings: Optional[Dict[str, str]] = None,
    section_hint: Optional[str] = None,
) -> tuple[Optional[str], RuleScore]:
    """Return (best_canonical_field, RuleScore)."""
    leaf_lower = _extract_leaf(header).lower()
    best_field: Optional[str] = None
    best_score = RuleScore()

    for canonical_field, aliases in canonical_dict.items():
        score = RuleScore()

        # Exact match scores higher (0.6) than substring match (0.4) to break ties
        # where a short alias (e.g. "customer") is a substring of a more specific
        # leaf that has an exact alias in another canonical field.
        for alias in aliases:
            alias_l = alias.lower()
            if alias_l == leaf_lower:
                score.exact_alias_match = 0.6
                break
            if alias_l in leaf_lower or leaf_lower in alias_l:
                score.exact_alias_match = max(score.exact_alias_match, 0.4)

        # Section hint match
        if section_hint:
            section_lower = section_hint.lower()
            for alias in aliases:
                if section_lower in alias.lower() or alias.lower() in section_lower:
                    score.section_match = 0.2
                    break

        # Prior mapping match
        if prior_mappings and header in prior_mappings:
            if prior_mappings[header] == canonical_field:
                score.prior_mapping = 0.3

        score.total = min(1.0, score.exact_alias_match + score.section_match + score.prior_mapping)

        if score.total > best_score.total:
            best_score = score
            best_field = canonical_field

    return best_field, best_score


def hybrid_confidence(rule_total: float, llm_confidence: float) -> float:
    return round(WEIGHT_RULE * rule_total + WEIGHT_LLM * llm_confidence, 4)
