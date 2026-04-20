from __future__ import annotations

import json
from typing import Dict, List, Optional

from excel_ingest.mapping.adapters.base import LLMAdapter, LLMResponse

_DEFAULT_MODEL = "gpt-4o-mini"


class OpenAIAdapter(LLMAdapter):
    """LLM adapter using OpenAI Chat Completions API.

    Requires: pip install databricks-excel-ingest-framework[openai]

    Args:
        model:      OpenAI model name. Defaults to "gpt-4o-mini".
        api_key:    OpenAI API key. If None, reads OPENAI_API_KEY env var.
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
                from openai import OpenAI
            except ImportError as exc:
                raise ImportError(
                    "openai is required for OpenAIAdapter. "
                    "Install with: pip install databricks-excel-ingest-framework[openai]"
                ) from exc
            kwargs = {}
            if self.api_key:
                kwargs["api_key"] = self.api_key
            self._client = OpenAI(**kwargs)
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
            response = client.chat.completions.create(
                model=self.model,
                messages=[{"role": "user", "content": prompt}],
                max_tokens=self.max_tokens,
                response_format={"type": "json_object"},
            )
            raw = response.choices[0].message.content.strip()
            return _parse_llm_json(raw)
        except Exception as exc:
            return LLMResponse(
                canonical_field=None, confidence=0.0,
                reasoning=f"OpenAI LLM call failed: {exc}",
            )


def _parse_llm_json(raw: str) -> LLMResponse:
    try:
        data = json.loads(raw)
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
