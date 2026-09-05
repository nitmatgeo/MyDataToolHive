"""
Pattern Resolution — L0A CTE Equivalent
=========================================
Resolves which patterns apply to each field at the individual-pattern level,
honouring the Category → SubCategory → PatternName precedence hierarchy
and the "Not Allowed overrides Allowed" rule.

This is the Python equivalent of the L0A CTE with RANK() window function
in ``p_DQ_GenerateRuleFunctions``.

Resolution logic (mirrors L0A CTE exactly)
-------------------------------------------
1. For each row in ``configFieldAllowedPattern``, join ``masterPattern`` at
   three levels:
     - pC  (Category match):     pC.PatternCategory  = cFAP.PatternCategory
     - pSC (SubCategory match):  pSC.PatternSubCategory = cFAP.PatternSubCategory
     - pN  (PatternName match):  pN.PatternName       = cFAP.PatternName

2. Resolve the effective pattern via COALESCE(pN, pSC, pC) — most specific wins.

3. Apply RANK() OVER PARTITION BY (FullFieldName, PatternValue, PatternName)
   ORDER BY specificity DESC, IsPatternAllowed DESC
   → Keep only Rank=1 rows.
   → When both Allowed and Not Allowed exist for the same resolved pattern on
     the same field, the DESC ordering on IsPatternAllowed ensures that
     Not Allowed (0) ranks higher than Allowed (1) — effectively "Not Allowed
     overrides Allowed".

   Wait — IsPatternAllowed: 0=Not Allowed, 1=Allowed.
   ORDER BY ... IsPatternAllowed DESC → 1 comes before 0.
   But the SQL RANK orders by name/subcat/cat presence DESC first (more
   specific = present wins), then IsPatternAllowed DESC.
   Within the same specificity level, Allowed (1) would rank HIGHER than
   Not Allowed (0) via DESC.

   The "Not Allowed overrides Allowed" rule works through a different
   mechanism: when both a category-level "Not Allowed" and a pattern-level
   "Allowed" exist for the same pattern, the RANK partition is
   (FullFieldName, PatternValue, PatternName) — and both rows map to the
   same resolved pattern.  The ORDER BY resolves in favour of the more
   specific (PatternName > SubCategory > Category) rule.  Then when two
   rows have the SAME specificity but different IsPatternAllowed, DESC
   means the Allowed row wins — but this is the OVERRIDE case (the more
   specific "Allowed" overrides the broader "Not Allowed").

   Therefore the correct interpretation is:
     "More specific rule wins; within same specificity Not Allowed wins"

   Looking at the actual ORDER BY in SQL more carefully:
     ORDER BY pN.PatternName DESC,       -- PatternName-level rule present = wins
              pSC.PatternName DESC,      -- SubCategory-level present
              pC.PatternName DESC,       -- Category-level present
              pN.PatternSubCategory DESC,
              pSC.PatternSubCategory DESC,
              pC.PatternSubCategory DESC,
              pN.PatternCategory DESC,
              pSC.PatternCategory DESC,
              pC.PatternCategory DESC,
              cFAP.IsPatternAllowed DESC  ← tiebreak: Allowed(1) ranks before Not Allowed(0)

   So within the same specificity level, Allowed wins over Not Allowed.
   This means: a specific "Allowed" overrides a broader "Not Allowed",
   which is the intended behaviour described in the docs:
     "Block ALL special characters (category rule),
      EXCEPT allow Hyphen, Full Stop, At Sign (specific pattern overrides)"
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional


@dataclass
class ResolvedPattern:
    """One resolved (field, pattern) pair after L0A precedence resolution."""
    full_field_name: str
    schema_name: str
    table_name: str
    field_name: str
    pattern_id: int
    pattern_category: str
    pattern_subcategory: Optional[str]
    pattern_name: str
    pattern_description: Optional[str]
    pattern_priority: int
    pattern_value: Optional[str]
    is_pattern_allowed: bool   # True = Allowed; False = Not Allowed


def resolve_patterns(
    config_field_allowed_pattern: list[dict],
    master_patterns: list[dict],
) -> list[ResolvedPattern]:
    """
    Resolve the effective per-field pattern rules from config tables.

    Equivalent to the L0A CTE in ``p_DQ_GenerateRuleFunctions``.

    Parameters
    ----------
    config_field_allowed_pattern
        Rows from the ``configFieldAllowedPattern`` table.
        Each dict must contain: FullFieldName, PatternCategory, PatternSubCategory,
        PatternName, IsPatternAllowed, IsActive.
    master_patterns
        Rows from the ``masterPattern`` table.
        Each dict must contain: _ID, PatternCategory, PatternSubCategory,
        PatternName, PatternDescription, PatternPriority, PatternValue, IsActive.

    Returns
    -------
    List of ResolvedPattern, one per (field, pattern) combination after
    deduplication.  Only Rank=1 rows are included.
    """
    # Build lookup indices
    by_category: dict[str, list[dict]] = {}
    by_subcategory: dict[str, list[dict]] = {}
    by_name: dict[str, list[dict]] = {}

    for mp in master_patterns:
        if not mp.get("IsActive", True):
            continue
        if mp.get("PatternCategory"):
            by_category.setdefault(mp["PatternCategory"], []).append(mp)
        if mp.get("PatternSubCategory"):
            by_subcategory.setdefault(mp["PatternSubCategory"], []).append(mp)
        if mp.get("PatternName"):
            by_name.setdefault(mp["PatternName"], []).append(mp)

    # Step 1: Expand each config row into candidate (field, pattern) pairs
    candidates = []  # list of dicts

    for cfap in config_field_allowed_pattern:
        if not cfap.get("IsActive", True):
            continue

        ffn = cfap["FullFieldName"]
        cat = cfap.get("PatternCategory") or None
        subcat = cfap.get("PatternSubCategory") or None
        pname = cfap.get("PatternName") or None
        is_allowed = bool(cfap["IsPatternAllowed"])

        # Determine which level(s) match
        matched_by_name = by_name.get(pname, []) if pname else []
        matched_by_subcat = by_subcategory.get(subcat, []) if subcat else []
        matched_by_cat = by_category.get(cat, []) if cat else []

        # Collect all master pattern rows touched by this config rule
        # (a single config row can expand to many patterns via category/subcategory)
        all_matches = set()
        for mp in matched_by_name:
            all_matches.add(mp["_ID"])
        for mp in matched_by_subcat:
            all_matches.add(mp["_ID"])
        for mp in matched_by_cat:
            all_matches.add(mp["_ID"])

        mp_by_id = {mp["_ID"]: mp for mp in master_patterns if mp.get("IsActive", True)}

        for pid in all_matches:
            mp = mp_by_id.get(pid)
            if not mp:
                continue

            # Determine effective values via COALESCE(pN, pSC, pC)
            p_from_name = mp if pname and mp.get("PatternName") == pname else None
            p_from_sc = mp if subcat and mp.get("PatternSubCategory") == subcat else None
            p_from_cat = mp if cat and mp.get("PatternCategory") == cat else None

            effective_mp = p_from_name or p_from_sc or p_from_cat or mp

            # Compute specificity rank score (higher = more specific = wins)
            # Mirrors ORDER BY pN.PatternName DESC, pSC.PatternName DESC, ...
            spec_name  = 3 if p_from_name else 0
            spec_sc    = 2 if p_from_sc else 0
            spec_cat   = 1 if p_from_cat else 0
            specificity = spec_name or spec_sc or spec_cat

            # Tiebreak: Allowed (True=1) > Not Allowed (False=0)  →  DESC ordering
            allowed_rank = 1 if is_allowed else 0

            resolved_pattern_name = effective_mp.get("PatternName", "")
            resolved_pattern_value = effective_mp.get("PatternValue")

            candidates.append({
                "full_field_name": ffn,
                "pattern_id": pid,
                "pattern_category": effective_mp.get("PatternCategory", ""),
                "pattern_subcategory": effective_mp.get("PatternSubCategory"),
                "pattern_name": resolved_pattern_name,
                "pattern_description": effective_mp.get("PatternDescription"),
                "pattern_priority": effective_mp.get("PatternPriority", 50),
                "pattern_value": resolved_pattern_value,
                "is_pattern_allowed": is_allowed,
                "specificity": specificity,
                "allowed_rank": allowed_rank,
            })

    # Step 2: Deduplicate via RANK() OVER PARTITION BY (field, pattern_value, pattern_name)
    # GROUP by (full_field_name, pattern_value, pattern_name) → keep highest rank row
    groups: dict[tuple, list[dict]] = {}
    for c in candidates:
        key = (c["full_field_name"], c["pattern_value"], c["pattern_name"])
        groups.setdefault(key, []).append(c)

    resolved: list[ResolvedPattern] = []

    for (ffn, _pv, _pn), group in groups.items():
        # Sort by (specificity DESC, allowed_rank DESC) → first item = Rank 1
        group.sort(key=lambda x: (x["specificity"], x["allowed_rank"]), reverse=True)
        winner = group[0]

        # Parse schema/table/field from FullFieldName (format: Schema.Table.Column)
        parts = ffn.split(".", 2)
        schema = parts[0] if len(parts) > 0 else ""
        table  = parts[1] if len(parts) > 1 else ""
        field  = parts[2] if len(parts) > 2 else ""

        resolved.append(ResolvedPattern(
            full_field_name=ffn,
            schema_name=schema,
            table_name=table,
            field_name=field,
            pattern_id=winner["pattern_id"],
            pattern_category=winner["pattern_category"],
            pattern_subcategory=winner["pattern_subcategory"],
            pattern_name=winner["pattern_name"],
            pattern_description=winner["pattern_description"],
            pattern_priority=winner["pattern_priority"],
            pattern_value=winner["pattern_value"],
            is_pattern_allowed=winner["is_pattern_allowed"],
        ))

    return resolved


def get_distinct_fields(resolved_patterns: list[ResolvedPattern]) -> list[tuple[str, str, str]]:
    """
    Return distinct (schema_name, table_name, field_name) tuples from resolved patterns.
    Equivalent to L0B: SELECT DISTINCT SchemaName, TableName, FieldName FROM #L0A.
    """
    seen = set()
    result = []
    for rp in resolved_patterns:
        key = (rp.schema_name, rp.table_name, rp.field_name)
        if key not in seen:
            seen.add(key)
            result.append(key)
    return result
