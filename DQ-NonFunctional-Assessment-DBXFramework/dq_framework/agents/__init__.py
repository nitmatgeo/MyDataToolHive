"""
DQ Framework — Agent Support
=============================
Framework-agnostic building blocks for LLM / AI-agent integration.

No dependency on any specific agent framework (Semantic Kernel, AutoGen,
LangChain, etc.).  The three modules here produce plain Python objects
(strings, dicts) that plug into any LLM platform:

    context.py   — System prompt / instructions for an LLM assistant.
                   Gives the model enough knowledge to help users configure
                   the DQ framework conversationally.

    tools.py     — Tool / function definitions in JSON Schema format
                   (OpenAI-compatible; also accepted by Anthropic, Semantic
                   Kernel, AutoGen, and LangChain).

    suggest.py   — Reads a Delta table schema (column names + Spark types)
                   and returns a ready-to-run ConfigManager code snippet.
                   Use this as the output of an "auto-configure" agent step.

Quick start (any LLM SDK)
--------------------------
from dq_framework.agents import SYSTEM_PROMPT, DQ_TOOLS, suggest_config

# 1. Give the LLM framework knowledge
system = SYSTEM_PROMPT

# 2. Register tools so the agent can call framework operations
tools = DQ_TOOLS  # list of OpenAI-style tool dicts

# 3. Auto-suggest config from a table schema
schema = [("EMAIL_ADDRESS", "string"), ("FIRST_NAME", "string"), ("POSTCODE", "string")]
code   = suggest_config(schema, source_schema="Source", source_table="SRC_Customer")
print(code)
"""

from dq_framework.agents.context import SYSTEM_PROMPT
from dq_framework.agents.tools import DQ_TOOLS
from dq_framework.agents.suggest import suggest_config

__all__ = ["SYSTEM_PROMPT", "DQ_TOOLS", "suggest_config"]
