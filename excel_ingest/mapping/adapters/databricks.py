from __future__ import annotations

import json
from typing import Any, Dict, List, Optional

from excel_ingest.mapping.adapters.base import LLMAdapter, LLMResponse

_DEFAULT_MODEL = "databricks-llama-3-70b-instruct"


class DatabricksAdapter(LLMAdapter):
    """LLM adapter using Databricks Foundation Models API (pay-per-token, no endpoint setup).

    Requires: pip install databricks-excel-ingest-framework[databricks]

    Args:
        model:  Any Databricks Foundation Models serving endpoint name.
                Defaults to "databricks-llama-3-70b-instruct".
        host:   Databricks workspace URL. If None, auto-detected from environment
                (works when running inside a Databricks cluster).
        token:  PAT or service-principal token. If None, auto-detected.
        max_tokens: Maximum tokens for the LLM response.
    """

    def __init__(
        self,
        model: str = _DEFAULT_MODEL,
        host: Optional[str] = None,
        token: Optional[str] = None,
        max_tokens: int = 200,
    ) -> None:
        self.model = model
        self.host = host
        self.token = token
        self.max_tokens = max_tokens
        self._client = None

    def _get_client(self):
        if self._client is None:
            try:
                from databricks.sdk import WorkspaceClient
            except ImportError as exc:
                raise ImportError(
                    "databricks-sdk is required for DatabricksAdapter. "
                    "Install with: pip install databricks-excel-ingest-framework[databricks]"
                ) from exc
            kwargs: Dict[str, Any] = {}
            if self.host:
                kwargs["host"] = self.host
            if self.token:
                kwargs["token"] = self.token
            self._client = WorkspaceClient(**kwargs)
        return self._client

    def map_column(
        self,
        header: str,
        canonical_dict: Dict[str, List[str]],
        section_hint: Optional[str] = None,
        country_code: Optional[str] = None,
    ) -> LLMResponse:
        from databricks.sdk.service.serving import ChatMessage, ChatMessageRole

        prompt = self._build_prompt(header, canonical_dict, section_hint, country_code)
        client = self._get_client()

        try:
            response = client.serving_endpoints.query(
                name=self.model,
                messages=[ChatMessage(role=ChatMessageRole.USER, content=prompt)],
                max_tokens=self.max_tokens,
            )
            raw = response.choices[0].message.content.strip()
            return _parse_llm_json(raw)
        except Exception as exc:
            return LLMResponse(
                canonical_field=None, confidence=0.0,
                reasoning=f"Databricks LLM call failed: {exc}",
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
    except Exception:
        return LLMResponse(
            canonical_field=None, confidence=0.0,
            reasoning="Could not parse LLM response.",
            raw_response=raw,
        )
