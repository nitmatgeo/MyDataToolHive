from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, List, Optional


class MappingStatus(Enum):
    AUTO_APPROVED = "AUTO_APPROVED"     # confidence > 0.9
    NEEDS_REVIEW = "NEEDS_REVIEW"       # 0.7 – 0.9
    REQUIRES_HUMAN = "REQUIRES_HUMAN"   # < 0.7
    UNMAPPED = "UNMAPPED"               # no candidate found


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
    hierarchical_header: str
    canonical_field: Optional[str]
    mapping_status: MappingStatus
    mapping_method: MappingMethod
    final_confidence: float
    rule_score: float
    llm_confidence: float
    llm_reasoning: Optional[str]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "file_id": self.file_id,
            "column_index": self.column_index,
            "hierarchical_header": self.hierarchical_header,
            "canonical_field": self.canonical_field,
            "mapping_status": self.mapping_status.value,
            "mapping_method": self.mapping_method.value,
            "final_confidence": round(self.final_confidence, 4),
            "rule_score": round(self.rule_score, 4),
            "llm_confidence": round(self.llm_confidence, 4),
            "llm_reasoning": self.llm_reasoning,
        }


def determine_status(confidence: float, canonical_field: Optional[str]) -> MappingStatus:
    if canonical_field is None:
        return MappingStatus.UNMAPPED
    if confidence >= THRESHOLD_AUTO:
        return MappingStatus.AUTO_APPROVED
    if confidence >= THRESHOLD_REVIEW:
        return MappingStatus.NEEDS_REVIEW
    return MappingStatus.REQUIRES_HUMAN


def calculate_rule_score(
    header: str,
    canonical_dict: Dict[str, List[str]],
    prior_mappings: Optional[Dict[str, str]] = None,
    section_hint: Optional[str] = None,
) -> tuple[Optional[str], RuleScore]:
    """Return (best_canonical_field, RuleScore)."""
    header_lower = header.lower().strip("[]").replace(".", " ")
    best_field: Optional[str] = None
    best_score = RuleScore()

    for canonical_field, aliases in canonical_dict.items():
        score = RuleScore()

        # Exact alias match
        for alias in aliases:
            if alias.lower() == header_lower or alias.lower() in header_lower:
                score.exact_alias_match = 0.4
                break

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
