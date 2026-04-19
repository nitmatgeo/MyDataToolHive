from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Dict, List, Optional


@dataclass
class LLMResponse:
    canonical_field: Optional[str]
    confidence: float                # 0.0 – 1.0
    reasoning: str
    raw_response: Optional[str] = None


class LLMAdapter(ABC):
    """Abstract base for all LLM mapping adapters.

    Subclasses must implement map_column().  The engine calls this once per
    column; all inputs are plain text — no PII, only header names and the
    caller-supplied canonical dictionary keys/aliases.
    """

    @abstractmethod
    def map_column(
        self,
        header: str,
        canonical_dict: Dict[str, List[str]],
        section_hint: Optional[str] = None,
        country_code: Optional[str] = None,
    ) -> LLMResponse:
        """Map a single column header to a canonical field name.

        Args:
            header:         Hierarchical header string, e.g. "[Contact].[Email]".
            canonical_dict: {canonical_field_name: [alias1, alias2, ...]}.
                            Caller-supplied; domain-agnostic.
            section_hint:   Optional section keyword extracted from the file.
            country_code:   Optional ISO-2 country code for locale hints.

        Returns:
            LLMResponse with canonical_field and confidence (0–1).
        """

    def _build_prompt(
        self,
        header: str,
        canonical_dict: Dict[str, List[str]],
        section_hint: Optional[str],
        country_code: Optional[str],
    ) -> str:
        fields_summary = "\n".join(
            f"  - {k}: {', '.join(v[:5])}" for k, v in list(canonical_dict.items())[:30]
        )
        context_parts = []
        if section_hint:
            context_parts.append(f"Section context: {section_hint}")
        if country_code:
            context_parts.append(f"Country: {country_code}")
        context = "  " + "\n  ".join(context_parts) if context_parts else "  None"

        return (
            f"You are mapping Excel column headers to canonical field names.\n\n"
            f"Column header: \"{header}\"\n\n"
            f"Additional context:\n{context}\n\n"
            f"Canonical fields and known aliases:\n{fields_summary}\n\n"
            f"Return ONLY a JSON object with keys:\n"
            f"  \"canonical_field\": string or null\n"
            f"  \"confidence\": float 0.0-1.0\n"
            f"  \"reasoning\": string (one sentence)\n"
        )
