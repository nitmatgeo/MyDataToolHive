from __future__ import annotations

import json
from typing import Dict, List, Optional

from excel_ingest.mapping.adapters.base import LLMAdapter, LLMResponse

_DEFAULT_MODEL = "claude-haiku-4-5-20251001"


class AnthropicAdapter(LLMAdapter):
    """LLM adapter using Anthropic Messages API.

    Requires: pip install databricks-excel-ingest-framework[anthropic]

    Args:
        model:      Anthropic model ID. Defaults to "claude-haiku-4-5-20251001".
        api_key:    Anthropic API key. If None, reads ANTHROPIC_API_KEY env var.
        max_tokens: Maximum tokens for the response.
    """

    def __init__(
        self,
        model: str = _DEFAULT_MODEL,
        api_key: Optional[str] = None,
        max_tokens: int = 200,
    ) -> None:
        self.model = model
        self.api_key = api_key
        self.max_tokens = max_tokens
        self._client = None

    def _get_client(self):
        if self._client is None:
            try:
                import anthropic
            except ImportError as exc:
                raise ImportError(
                    "anthropic is required for AnthropicAdapter. "
                    "Install with: pip install databricks-excel-ingest-framework[anthropic]"
                ) from exc
            kwargs = {}
            if self.api_key:
                kwargs["api_key"] = self.api_key
            self._client = anthropic.Anthropic(**kwargs)
        return self._client

    def map_column(
        self,
        header: str,
        canonical_dict: Dict[str, List[str]],
        section_hint: Optional[str] = None,
        country_code: Optional[str] = None,
    ) -> LLMResponse:
        prompt = self._build_prompt(header, canonical_dict, section_hint, country_code)
        client = self._get_client()
        try:
            message = client.messages.create(
                model=self.model,
                max_tokens=self.max_tokens,
                messages=[{"role": "user", "content": prompt}],
            )
            raw = message.content[0].text.strip()
            return _parse_llm_json(raw)
        except Exception as exc:
            return LLMResponse(
                canonical_field=None, confidence=0.0,
                reasoning=f"Anthropic LLM call failed: {exc}",
            )


def _parse_llm_json(raw: str) -> LLMResponse:
    try:
        start = raw.find("{")
        end = raw.rfind("}") + 1
        data = json.loads(raw[start:end])
        return LLMResponse(
            canonical_field=data.get("canonical_field"),
            confidence=float(data.get("confidence", 0.0)),
            reasoning=data.get("reasoning", ""),
            raw_response=raw,
        )
    except Exception as exc:
        return LLMResponse(
            canonical_field=None, confidence=0.0,
            reasoning=f"Could not parse LLM response: {exc}",
            raw_response=raw,
        )
