"""
Tool / Function Definitions for LLM Agent Integration
======================================================
OpenAI-compatible JSON Schema tool definitions for all key DQ framework
operations.  These are accepted by every major LLM platform:

  Platform            How to use
  -----------------   -------------------------------------------------------
  OpenAI / Azure OAI  Pass ``DQ_TOOLS`` as the ``tools=`` parameter
  Anthropic Claude    Pass ``DQ_TOOLS`` as the ``tools=`` parameter
  Microsoft Semantic  Convert via SemanticKernel.from_openai_function_spec()
  Kernel
  AutoGen             Pass to AssistantAgent(functions=DQ_TOOLS)
  LangChain           Convert via JsonSchemaFunction(spec) or StructuredTool

No framework dependency — this module is plain Python.

Usage example (OpenAI SDK)
--------------------------
from openai import AzureOpenAI
from dq_framework.agents import SYSTEM_PROMPT, DQ_TOOLS
from dq_framework.agents.tools import execute_tool_call

client = AzureOpenAI(...)
response = client.chat.completions.create(
    model="gpt-4o",
    messages=[{"role": "system", "content": SYSTEM_PROMPT},
              {"role": "user",   "content": "Configure email DQ for my email field"}],
    tools=DQ_TOOLS,
)
# Execute whichever tool the model called:
for call in response.choices[0].message.tool_calls or []:
    result = execute_tool_call(call.function.name, call.function.arguments, dq, cfg)
    print(result)
"""

from __future__ import annotations

import json
import logging
from typing import Any

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Tool definitions (OpenAI-compatible JSON Schema)
# ---------------------------------------------------------------------------

DQ_TOOLS: list[dict] = [

    # ── Framework setup ────────────────────────────────────────────────────

    {
        "type": "function",
        "function": {
            "name": "dq_setup",
            "description": (
                "Initialise the DQ framework: create the schema, all 9 Delta tables, "
                "reporting views, and seed 27 data categories + 118 validation patterns. "
                "Safe to call multiple times (idempotent)."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "catalog": {
                        "type": "string",
                        "description": "Unity Catalog name (e.g. 'main'). Pass '' for legacy Hive metastore.",
                        "default": "main",
                    },
                    "schema": {
                        "type": "string",
                        "description": "Schema that will contain all framework tables.",
                        "default": "dq",
                    },
                },
                "required": [],
            },
        },
    },

    # ── Field registration ─────────────────────────────────────────────────

    {
        "type": "function",
        "function": {
            "name": "register_field",
            "description": (
                "Register a source field for DQ assessment. "
                "FullFieldName must follow the format Schema.Table.Column. "
                "data_category_type_id: 4=Email, 10=PostalCode, 12=Phone, 2=ShortText, "
                "1=LongText, 20=Date, 21=DateTime, 13=INT, 15=Decimal."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "field_id": {
                        "type": "integer",
                        "description": "Unique integer ID for this field.",
                    },
                    "full_field_name": {
                        "type": "string",
                        "description": "Schema.Table.Column (e.g. 'Source.SRC_Party.EMAIL_ADDRESS').",
                    },
                    "data_category_type_id": {
                        "type": "integer",
                        "description": "Data category from masterDataCategory (1–27).",
                    },
                },
                "required": ["field_id", "full_field_name", "data_category_type_id"],
            },
        },
    },

    # ── Length / value boundaries ──────────────────────────────────────────

    {
        "type": "function",
        "function": {
            "name": "set_field_values",
            "description": (
                "Set minimum/maximum character length (L01 check) and/or "
                "lexicographic value range (L04 check) for a field."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "config_id": {"type": "integer", "description": "Unique integer ID."},
                    "full_field_name": {"type": "string"},
                    "min_data_length": {"type": "integer", "description": "Minimum character count (L01)."},
                    "max_data_length": {"type": "integer", "description": "Maximum character count (L01)."},
                    "min_data_value":  {"type": "string",  "description": "Minimum lexicographic value (L04)."},
                    "max_data_value":  {"type": "string",  "description": "Maximum lexicographic value (L04)."},
                },
                "required": ["config_id", "full_field_name"],
            },
        },
    },

    # ── Pattern rules ──────────────────────────────────────────────────────

    {
        "type": "function",
        "function": {
            "name": "block_category",
            "description": (
                "Block all patterns in a given category for this field (L03 check). "
                "Common categories: DataEmptiness, InvalidKeyword, SpecialCharacter, "
                "DataType3, SpaceFound, UnicodeCharacters."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "rule_id":          {"type": "integer"},
                    "full_field_name":  {"type": "string"},
                    "pattern_category": {"type": "string"},
                },
                "required": ["rule_id", "full_field_name", "pattern_category"],
            },
        },
    },

    {
        "type": "function",
        "function": {
            "name": "allow_pattern",
            "description": (
                "Allow a specific pattern, overriding a broader block rule. "
                "E.g. block all SpecialCharacter, then allow 'Has At Sign' and 'Has Full Stop'."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "rule_id":         {"type": "integer"},
                    "full_field_name": {"type": "string"},
                    "pattern_name":    {"type": "string",
                                        "description": "Exact pattern name from masterPattern."},
                },
                "required": ["rule_id", "full_field_name", "pattern_name"],
            },
        },
    },

    {
        "type": "function",
        "function": {
            "name": "block_pattern",
            "description": "Block a specific individual pattern for this field.",
            "parameters": {
                "type": "object",
                "properties": {
                    "rule_id":         {"type": "integer"},
                    "full_field_name": {"type": "string"},
                    "pattern_name":    {"type": "string"},
                },
                "required": ["rule_id", "full_field_name", "pattern_name"],
            },
        },
    },

    # ── Custom expression rules (L02) ──────────────────────────────────────

    {
        "type": "function",
        "function": {
            "name": "add_custom_query_regex",
            "description": (
                "Add a custom L02 validation rule using a regex pattern. "
                "No Python knowledge required — write a regular expression. "
                "The framework automatically applies re.search(pattern, value). "
                "must_match=true: value MUST match (fails if it doesn't). "
                "must_match=false: value must NOT match (fails if it does). "
                "Examples: '^04[0-9]{8}$' for AU mobile, '^[^@]+@[^@]+\\.[^@]+$' for email."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "query_id":        {"type": "integer", "description": "Unique ID."},
                    "full_field_name": {"type": "string"},
                    "regex_pattern":   {"type": "string",
                                        "description": "Regular expression to match against the value."},
                    "must_match":      {"type": "boolean",
                                        "description": "true=value must match; false=value must NOT match."},
                    "description":     {"type": "string", "description": "Human-readable rule description."},
                },
                "required": ["query_id", "full_field_name", "regex_pattern", "must_match"],
            },
        },
    },

    {
        "type": "function",
        "function": {
            "name": "add_custom_query_sql",
            "description": (
                "Add a custom L02 validation rule using a Spark SQL expression. "
                "Use @InputValue as the placeholder for the field value — this mirrors "
                "the original SQL Server @InputValue convention in p_DQ_GenerateRuleFunctions. "
                "The expression is applied via F.expr() at the DataFrame level (not inside a UDF). "
                "must_match=true: expression must evaluate to true to PASS. "
                "must_match=false: expression must evaluate to false to PASS. "
                "Examples: 'LENGTH(@InputValue) > 5', 'LOCATE(\"@\", @InputValue) > 0', "
                "'@InputValue RLIKE \"^[0-9]+$\"'."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "query_id":        {"type": "integer", "description": "Unique ID."},
                    "full_field_name": {"type": "string"},
                    "sql_expression":  {
                        "type": "string",
                        "description": (
                            "Spark SQL expression with @InputValue as the placeholder for the field. "
                            "Example: 'LENGTH(@InputValue) BETWEEN 6 AND 255'"
                        ),
                    },
                    "must_match":  {"type": "boolean",
                                    "description": "true=expression must be true; false=must be false."},
                    "description": {"type": "string", "description": "Human-readable rule description."},
                },
                "required": ["query_id", "full_field_name", "sql_expression", "must_match"],
            },
        },
    },

    # ── Mapping ────────────────────────────────────────────────────────────

    {
        "type": "function",
        "function": {
            "name": "add_mapping",
            "description": (
                "Map a registered source field to a column in a curated Delta table. "
                "This tells the engine which table and column to assess at runtime."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "mapping_id":          {"type": "integer"},
                    "full_field_name":     {"type": "string"},
                    "target_catalog_name": {"type": "string",
                                           "description": "Unity Catalog (e.g. 'main')."},
                    "target_schema_name":  {"type": "string",
                                           "description": "Curated schema (e.g. 'Curated')."},
                    "target_table_name":   {"type": "string"},
                    "target_field_name":   {"type": "string",
                                           "description": "Column name in the curated table."},
                },
                "required": ["mapping_id", "full_field_name",
                             "target_schema_name", "target_table_name", "target_field_name"],
            },
        },
    },

    # ── Engine operations ──────────────────────────────────────────────────

    {
        "type": "function",
        "function": {
            "name": "generate_rule_functions",
            "description": (
                "Build one field-checker function per registered field from the "
                "configuration tables. Must be called after any configuration change "
                "and before running the assessment."
            ),
            "parameters": {"type": "object", "properties": {}, "required": []},
        },
    },

    {
        "type": "function",
        "function": {
            "name": "run_assessment",
            "description": (
                "Run the DQ assessment on curated Delta tables. "
                "Applies all configured checks and writes DQEligible, DQViolations, "
                "DQFields back to each row. Returns an ExecutionID for filtering results."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "schema_name": {"type": "string",
                                   "description": "Curated schema to assess (null = all schemas)."},
                    "table_name":  {"type": "string",
                                   "description": "Specific table (null = all tables in schema)."},
                    "field_name":  {"type": "string",
                                   "description": "Specific field (null = all fields)."},
                    "reset_eligible_flag": {
                        "type": "boolean",
                        "description": "Reset DQ flags on previously failed rows before re-assessing.",
                        "default": False,
                    },
                },
                "required": [],
            },
        },
    },

    # ── Results ────────────────────────────────────────────────────────────

    {
        "type": "function",
        "function": {
            "name": "get_violations",
            "description": "Return row-level violations for a given execution. Only failing rows included.",
            "parameters": {
                "type": "object",
                "properties": {
                    "execution_id": {"type": "string",
                                    "description": "ExecutionID from run_assessment."},
                    "limit":        {"type": "integer", "default": 100,
                                    "description": "Maximum rows to return."},
                },
                "required": [],
            },
        },
    },

    {
        "type": "function",
        "function": {
            "name": "get_quality_scores",
            "description": (
                "Return quality scores (RowsQualified, RowsDisqualified, PercentageQualified) "
                "per field for a given execution."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "execution_id": {"type": "string"},
                    "threshold":    {"type": "number", "default": 0,
                                    "description": "Only return fields below this quality %."},
                },
                "required": [],
            },
        },
    },

    {
        "type": "function",
        "function": {
            "name": "get_config_summary",
            "description": (
                "Return a summary of the current configuration: row counts for all "
                "framework tables and a verification of FK integrity."
            ),
            "parameters": {"type": "object", "properties": {}, "required": []},
        },
    },

    {
        "type": "function",
        "function": {
            "name": "verify_config",
            "description": (
                "Pre-flight config check. Verifies: (1) no duplicate _ID values in any "
                "user-managed table, (2) no duplicate logical rules in configFieldAllowedPattern, "
                "(3) referential integrity (FullFieldName → masterField; PatternName → masterPattern). "
                "Returns ok=true/false and a list of issues. Also called automatically by "
                "generate_rule_functions()."
            ),
            "parameters": {"type": "object", "properties": {}, "required": []},
        },
    },

    {
        "type": "function",
        "function": {
            "name": "prepare_curated_tables",
            "description": (
                "Add DQRowID, DQEligible, DQViolations, DQFields columns to all curated "
                "Delta tables that are mapped in mapDQChecks. Idempotent — safe to re-run. "
                "Must be called before run_assessment."
            ),
            "parameters": {"type": "object", "properties": {}, "required": []},
        },
    },

    {
        "type": "function",
        "function": {
            "name": "validate_custom_queries_sql",
            "description": (
                "Dry-run all SQL-type custom queries against a real table row to catch "
                "syntax errors before the full assessment. Optional but recommended."
            ),
            "parameters": {"type": "object", "properties": {}, "required": []},
        },
    },

    {
        "type": "function",
        "function": {
            "name": "summary_by_violation_type",
            "description": (
                "Return a summary DataFrame grouped by FullFieldName, ViolationType, "
                "TargetTableName, TargetFieldName, GeneratedOn, and ExecutionID with a "
                "RecordCount per group. Useful for pivot/analysis of which violation types "
                "are most common per field."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "execution_id": {"type": "string",
                                    "description": "ExecutionID from run_assessment. Omit for all executions."},
                },
                "required": [],
            },
        },
    },

    {
        "type": "function",
        "function": {
            "name": "get_field_rule_summary",
            "description": (
                "Return a flat DataFrame of all active DQ rules for a field (or all fields). "
                "One row per rule, covering all 4 rule types (Data Length, Value Range, "
                "Pattern Rule, Custom Rule). Suitable for Excel export, pivot, and filter. "
                "Columns: FullFieldName, DataCategory, TargetSchemaName, TargetTableName, "
                "TargetFieldName, RuleType, PatternCategory, PatternPriority, PatternName, "
                "PatternDescription, PatternValue, Status."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "field_name": {
                        "type": "string",
                        "description": (
                            "Logical FullFieldName (Schema.Table.Column) or physical "
                            "Schema.Table.Column. Omit to return rules for all configured fields."
                        ),
                    },
                },
                "required": [],
            },
        },
    },

    # ── Extensibility ──────────────────────────────────────────────────────

    {
        "type": "function",
        "function": {
            "name": "add_invalid_keyword",
            "description": (
                "Add a project-specific invalid keyword to the framework. "
                "IDs must be >= 1000 (framework reserves 1–999). "
                "After adding, call generate_rule_functions() for it to take effect."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "keyword":     {"type": "string",
                                   "description": "The keyword to detect (e.g. 'nodata')."},
                    "pattern_id":  {"type": "integer",
                                   "description": "Must be >= 1000."},
                    "description": {"type": "string"},
                },
                "required": ["keyword", "pattern_id"],
            },
        },
    },

    {
        "type": "function",
        "function": {
            "name": "suggest_config",
            "description": (
                "Auto-suggest DQ configuration code for a list of column names and types. "
                "Returns ready-to-run Python ConfigManager code. "
                "Use this as a starting point — the user should review and adjust."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "columns": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "name": {"type": "string"},
                                "type": {"type": "string",
                                         "description": "Spark data type (string, int, date, etc.)"},
                            },
                        },
                        "description": "List of {name, type} dicts.",
                    },
                    "source_schema": {"type": "string", "description": "e.g. 'Source'"},
                    "source_table":  {"type": "string", "description": "e.g. 'SRC_Customer'"},
                    "curated_schema": {"type": "string", "default": "Curated"},
                    "curated_table":  {"type": "string"},
                    "catalog":        {"type": "string", "default": "main"},
                },
                "required": ["columns", "source_schema", "source_table"],
            },
        },
    },
]


# ---------------------------------------------------------------------------
# Tool executor — bridge between LLM tool calls and the live framework
# ---------------------------------------------------------------------------

def execute_tool_call(
    tool_name: str,
    arguments_json: str,
    dq=None,
    cfg=None,
) -> str:
    """
    Execute a tool call returned by an LLM and return a JSON-serialisable result string.

    Parameters
    ----------
    tool_name
        The function name from the LLM's tool_call response.
    arguments_json
        JSON string of arguments from the LLM.
    dq
        Live ``DQFramework`` instance.
    cfg
        Live ``ConfigManager`` instance (``dq.config``).

    Returns a string result that should be fed back to the LLM as a tool message.
    """
    args: dict[str, Any] = json.loads(arguments_json) if arguments_json else {}

    try:
        if tool_name == "dq_setup":
            if dq is None:
                return "Error: DQFramework instance not provided."
            dq.setup()
            return "Framework setup complete: schema, tables, views, and reference data ready."

        if tool_name == "register_field":
            cfg.register_field(args["field_id"], args["full_field_name"],
                               args["data_category_type_id"])
            return f"Registered field: {args['full_field_name']} (ID={args['field_id']})"

        if tool_name == "set_field_values":
            cfg.set_field_values(
                args["config_id"], args["full_field_name"],
                min_data_length=args.get("min_data_length"),
                max_data_length=args.get("max_data_length"),
                min_data_value=args.get("min_data_value"),
                max_data_value=args.get("max_data_value"),
            )
            return f"Field values set for {args['full_field_name']}"

        if tool_name == "block_category":
            cfg.block_category(args["rule_id"], args["full_field_name"], args["pattern_category"])
            return f"Blocked category '{args['pattern_category']}' for {args['full_field_name']}"

        if tool_name == "allow_pattern":
            cfg.allow_pattern(args["rule_id"], args["full_field_name"], args["pattern_name"])
            return f"Allowed pattern '{args['pattern_name']}' for {args['full_field_name']}"

        if tool_name == "block_pattern":
            cfg.block_pattern(args["rule_id"], args["full_field_name"], args["pattern_name"])
            return f"Blocked pattern '{args['pattern_name']}' for {args['full_field_name']}"

        if tool_name == "add_custom_query_regex":
            cfg.add_custom_query_regex(
                args["query_id"], args["full_field_name"], args["regex_pattern"],
                must_match=args.get("must_match", True),
                description=args.get("description"),
            )
            return (f"Added regex rule for {args['full_field_name']}: "
                    f"'{args['regex_pattern']}' (must_match={args.get('must_match', True)})")

        if tool_name == "add_custom_query_sql":
            cfg.add_custom_query_sql(
                args["query_id"], args["full_field_name"], args["sql_expression"],
                must_match=args.get("must_match", True),
                description=args.get("description"),
            )
            return (f"Added SQL rule for {args['full_field_name']}: "
                    f"'{args['sql_expression']}' (must_match={args.get('must_match', True)})")

        if tool_name == "add_mapping":
            cfg.add_mapping(
                args["mapping_id"], args["full_field_name"],
                target_schema_name=args["target_schema_name"],
                target_table_name=args["target_table_name"],
                target_field_name=args["target_field_name"],
                target_catalog_name=args.get("target_catalog_name"),
            )
            return (f"Mapped {args['full_field_name']} → "
                    f"{args.get('target_catalog_name','')}.{args['target_schema_name']}."
                    f"{args['target_table_name']}.{args['target_field_name']}")

        if tool_name == "generate_rule_functions":
            dq.generate_rule_functions()
            return f"Rule functions generated. Registry size: {len(dq._registry)}"

        if tool_name == "run_assessment":
            exec_id = dq.run_assessment(
                schema_name=args.get("schema_name"),
                table_name=args.get("table_name"),
                field_name=args.get("field_name"),
                reset_eligible_flag=args.get("reset_eligible_flag", False),
            )
            return f"Assessment complete. ExecutionID: {exec_id}"

        if tool_name == "get_violations":
            df = dq.violations(args.get("execution_id"))
            rows = df.limit(args.get("limit", 100)).collect()
            return json.dumps([r.asDict() for r in rows], default=str)

        if tool_name == "get_quality_scores":
            df = dq.quality_scores(args.get("execution_id"))
            if args.get("threshold", 0):
                from pyspark.sql import functions as F
                df = df.filter(F.col("PercentageQualified") < args["threshold"])
            rows = df.collect()
            return json.dumps([r.asDict() for r in rows], default=str)

        if tool_name == "get_config_summary":
            cfg.show_config_summary()
            return "Config summary printed above."

        if tool_name == "verify_config":
            result = cfg.verify_config()
            if result["ok"]:
                return "Config verification passed — no issues found."
            return "Config issues found:\n" + "\n".join(f"  • {i}" for i in result["issues"])

        if tool_name == "prepare_curated_tables":
            dq.prepare_curated_tables()
            return "Curated tables prepared: DQRowID, DQEligible, DQViolations, DQFields columns added."

        if tool_name == "validate_custom_queries_sql":
            ok = dq.validate_custom_queries_sql()
            return "All SQL custom queries validated successfully." if ok else "Some SQL custom queries have errors — check output above."

        if tool_name == "summary_by_violation_type":
            df = dq.summary_by_violation_type(args.get("execution_id"))
            rows = df.collect()
            return json.dumps([r.asDict() for r in rows], default=str)

        if tool_name == "get_field_rule_summary":
            df = dq.field_rule_summary(args.get("field_name"))
            rows = df.collect()
            return json.dumps([r.asDict() for r in rows], default=str)

        if tool_name == "add_invalid_keyword":
            dq.add_invalid_keyword(
                args["keyword"], pattern_id=args["pattern_id"],
                description=args.get("description"),
            )
            return f"Added invalid keyword '{args['keyword']}' (ID={args['pattern_id']})"

        if tool_name == "suggest_config":
            from dq_framework.agents.suggest import suggest_config
            cols = [(c["name"], c["type"]) for c in args["columns"]]
            code = suggest_config(
                cols,
                source_schema=args["source_schema"],
                source_table=args["source_table"],
                curated_schema=args.get("curated_schema", "Curated"),
                curated_table=args.get("curated_table", args["source_table"]),
                catalog=args.get("catalog", "main"),
            )
            return code

        return f"Unknown tool: {tool_name}"

    except Exception as exc:
        logger.exception("Tool call failed: %s", tool_name)
        return f"Error executing {tool_name}: {exc}"
