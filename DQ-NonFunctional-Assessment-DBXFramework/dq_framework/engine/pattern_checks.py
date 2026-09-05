"""
Pattern Check Implementations
===============================
Python equivalents of the 118 built-in validation patterns defined in the
SQL Server ``p_DQ_GenerateRuleFunctions`` CASE block (L03 level).

Each check function follows the same contract:

    check_<pattern>(current_value, input_value, is_allowed, pattern_value=None)
        -> (failed: bool, violation_type: str, log_message: str, next_target_value: str | None)

Parameters
----------
current_value
    The current @TargetValue (may have been mutated by a prior pattern in the
    same field's check sequence — mirrors the shared @TargetValue variable in
    the SQL TVF).
input_value
    The original @InputValue (never mutated — used by patterns that reset
    @TargetValue from @InputValue via REPLACE(TRIM(@InputValue),...)).
is_allowed
    0 / False = pattern is NOT allowed (fail if detected).
    1 / True  = pattern IS required (fail if NOT detected) — only applicable
                to DataType patterns that have bidirectional semantics.
pattern_value
    The raw PatternValue from masterPattern (e.g. the special char, keyword,
    or char for "Virtually Empty" checks). None when not applicable.

Returns
-------
failed
    True  = this check failed → caller must stop and report violation.
    False = this check passed → caller continues to the next check.
violation_type
    The violation category string written to auditDQChecks.ViolationType.
log_message
    Diagnostic detail string written to auditDQChecks.LogMessage.
next_target_value
    The (possibly mutated) @TargetValue to pass into the next check.
    Returned so the caller can thread mutations faithfully.

Early-exit semantics (mirrors INSERT ... RETURN in SQL TVF)
-----------------------------------------------------------
When ``failed=True`` the caller must immediately return — no further checks
are evaluated for that row / field combination.  This is the Python equivalent
of the ``INSERT INTO @ResultTable ... RETURN;`` statement that terminates each
failing branch in the generated SQL TVF.
"""

from __future__ import annotations

import re
from decimal import Decimal, InvalidOperation
from typing import Optional, Tuple

try:
    from dateutil import parser as _dateutil_parser
    _HAS_DATEUTIL = True
except ImportError:
    _HAS_DATEUTIL = False


# ---------------------------------------------------------------------------
# Return type alias
# ---------------------------------------------------------------------------
CheckResult = Tuple[bool, str, str, Optional[str]]


def _pass(violation_type: str, rule_desc: str, value: Optional[str],
          next_val: Optional[str]) -> CheckResult:
    msg = f"Debug:: The value <{value if value is not None else 'NULL'}> has [PASSED]. Rule: {rule_desc}"
    return (False, violation_type, msg, next_val)


def _fail(violation_type: str, rule_desc: str, value: Optional[str],
          next_val: Optional[str]) -> CheckResult:
    msg = f"Debug:: The value <{value if value is not None else 'NULL'}> has [FAILED]. Rule: {rule_desc}"
    return (True, violation_type, msg, next_val)


def _allowed_label(is_allowed: bool) -> str:
    return ":<Allowed>" if is_allowed else ":<NOT Allowed>"


# ===========================================================================
# DataEmptiness patterns
# ===========================================================================

def check_is_empty_or_null(current_value: Optional[str], input_value: Optional[str],
                            is_allowed: bool, pattern_value: Optional[str] = None) -> CheckResult:
    """
    'Is Empty or NULL' — DataEmptiness category.

    SQL: IF (is_allowed = 0) AND (@TargetValue IS NULL OR @TargetValue = '')
         -> FAIL + RETURN
         ELSE -> PASS (no RETURN, continues to next check)

    When is_allowed=True (1): the IF condition is always False (1=0 AND ...) so the
    ELSE branch always executes → sets result=1 but does NOT return → continues.
    """
    label = f"Is Empty or NULL {_allowed_label(is_allowed)}"
    vtype = "Data Emptiness"
    is_empty = (current_value is None or current_value == "")
    if not is_allowed and is_empty:
        return _fail(vtype, label, current_value, current_value)
    return _pass(vtype, label, current_value, current_value)


def check_is_virtually_empty_with_spaces(current_value: Optional[str],
                                          input_value: Optional[str],
                                          is_allowed: bool,
                                          pattern_value: Optional[str] = None) -> CheckResult:
    """
    'Is Virtually Empty with Spaces' — DataEmptiness category.

    SQL: IF (is_allowed = 0)
             AND (LTRIM(RTRIM(@TargetValue)) = '' OR LEN(@TargetValue) = CHARINDEX(' ', @TargetValue))
    """
    label = f"Is Virtually Empty with Spaces {_allowed_label(is_allowed)}"
    vtype = "Data Emptiness"
    if not is_allowed and current_value is not None:
        stripped = current_value.strip()
        # LTRIM(RTRIM()) = '' → all whitespace
        # LEN = CHARINDEX(' ') → first space is at position = total length (single trailing space)
        first_space_pos = (current_value.find(' ') + 1) if ' ' in current_value else 0
        if stripped == '' or (first_space_pos > 0 and len(current_value) == first_space_pos):
            return _fail(vtype, label, current_value, current_value)
    return _pass(vtype, label, current_value, current_value)


def check_virtually_empty_with_char(current_value: Optional[str], input_value: Optional[str],
                                     is_allowed: bool, pattern_value: Optional[str] = None) -> CheckResult:
    """
    'Is Virtually Empty with <char>' — DataEmptiness category (34 patterns with a PatternValue).

    SQL: SET @TargetValue = REPLACE(TRIM(@InputValue), ' ', '')
         IF (is_allowed = 0)
             AND (LEN(@TargetValue) > 0 AND @TargetValue LIKE '%$char%' ESCAPE '$')
             AND (LEN(@TargetValue) % LEN(char) = 0
                  AND LEN(@TargetValue) = LEN(REPLACE(@TargetValue, char, ''))
                              + LEN(char) * (LEN(@TargetValue) / LEN(char)))

    The string (after stripping spaces) must consist entirely of repetitions
    of the pattern character/string.
    """
    # Reset @TargetValue from @InputValue (as SQL does)
    target = input_value.strip().replace(' ', '') if input_value else ''
    label = f"Is Virtually Empty with '{pattern_value}' {_allowed_label(is_allowed)}"
    vtype = "Data Emptiness"
    if not is_allowed and target and pattern_value:
        char = pattern_value
        char_len = len(char)
        val_len = len(target)
        if (char in target
                and val_len % char_len == 0
                and len(target.replace(char, '')) + char_len * (val_len // char_len) == val_len):
            return _fail(vtype, label, target, target)
    return _pass(vtype, label, target, target)


# ===========================================================================
# SpaceFound patterns
# ===========================================================================

def check_has_space(current_value: Optional[str], input_value: Optional[str],
                    is_allowed: bool, pattern_value: Optional[str] = None) -> CheckResult:
    """
    'Has Space' — SpaceFound category.

    SQL: IF (is_allowed = 0)
             AND ((@TargetValue LIKE '% %') OR (CHARINDEX(' ', @TargetValue) > 0))
         -> FAIL + RETURN

    Note: does NOT reset @TargetValue from @InputValue — uses current value as-is.
    """
    label = f"Has Space {_allowed_label(is_allowed)}"
    vtype = "Space Found"
    if not is_allowed and current_value and ' ' in current_value:
        return _fail(vtype, label, current_value, current_value)
    return _pass(vtype, label, current_value, current_value)


# ===========================================================================
# DataType patterns — bidirectional semantics
#
# For DataType patterns, is_allowed has bidirectional meaning:
#   is_allowed=True (1):  value MUST match format  → fail if NOT matching
#   is_allowed=False (0): value must NOT be in format → fail if IS matching
# ===========================================================================

def check_is_fully_numeric(current_value: Optional[str], input_value: Optional[str],
                            is_allowed: bool, pattern_value: Optional[str] = None) -> CheckResult:
    """
    'Is Fully Numeric' — DataType1 category.

    SQL: SET @TargetValue = REPLACE(TRIM(@InputValue), ' ', '')
         Fully numeric = ISNUMERIC=1 AND NOT LIKE '%[^0-9]%' AND TRY_CAST AS BIGINT NOT NULL
         → Only pure digit strings (no sign, no decimal point).

         IF (is_allowed=1 AND value is NOT fully numeric) → FAIL
         IF (is_allowed=0 AND value IS fully numeric)     → FAIL
    """
    label = f"Is Fully Numeric {_allowed_label(is_allowed)}"
    vtype = "Data Type"
    # SQL: @TargetValue IS NOT NULL gates all fail branches — NULL always passes
    if input_value is None:
        return _pass(vtype, label, None, None)
    target = input_value.strip().replace(' ', '')

    def _is_fully_numeric(s: str) -> bool:
        if not s:
            return False
        try:
            int(s)
            return s.isdigit()  # Excludes negative sign
        except (ValueError, OverflowError):
            return False

    is_numeric = _is_fully_numeric(target)
    failed = (is_allowed and target is not None and not is_numeric) or \
             (not is_allowed and target is not None and is_numeric)
    if failed:
        return _fail(vtype, label, target, target)
    return _pass(vtype, label, target, target)


def check_is_fully_decimal(current_value: Optional[str], input_value: Optional[str],
                            is_allowed: bool, pattern_value: Optional[str] = None) -> CheckResult:
    """
    'Is Fully Decimal' — DataType1 category.

    SQL: SET @TargetValue = REPLACE(TRIM(@InputValue), ' ', '')
         Fully decimal = ISNUMERIC=1 AND NOT LIKE '%[^.0-9]%' AND LIKE '%[.]%'
                         AND TRY_CAST AS DECIMAL(18,10) NOT NULL
         → Digits and exactly one decimal point, castable as DECIMAL(18,10).

         IF (is_allowed=1 AND value is NOT fully decimal) → FAIL
         IF (is_allowed=0 AND value IS fully decimal)     → FAIL
    """
    label = f"Is Fully Decimal {_allowed_label(is_allowed)}"
    vtype = "Data Type"
    if input_value is None:
        return _pass(vtype, label, None, None)
    target = input_value.strip().replace(' ', '')

    def _is_fully_decimal(s: str) -> bool:
        if not s or '.' not in s:
            return False
        if not re.fullmatch(r'[0-9]+\.[0-9]+', s):
            return False
        try:
            Decimal(s)
            return True
        except InvalidOperation:
            return False

    is_decimal = _is_fully_decimal(target)
    failed = (is_allowed and target is not None and not is_decimal) or \
             (not is_allowed and target is not None and is_decimal)
    if failed:
        return _fail(vtype, label, target, target)
    return _pass(vtype, label, target, target)


def check_is_boolean(current_value: Optional[str], input_value: Optional[str],
                     is_allowed: bool, pattern_value: Optional[str] = None) -> CheckResult:
    """
    'Is Boolean' — DataType category.

    SQL: SET @TargetValue = REPLACE(TRIM(@InputValue), ' ', '')
         Valid boolean = value IN ('0', '1') AND ISNUMERIC=1 AND TRY_CAST AS BIT NOT NULL

         IF (is_allowed=0 AND IS boolean) → FAIL
         IF (is_allowed=1 AND NOT boolean) → FAIL
    """
    label = f"Is Boolean {_allowed_label(is_allowed)}"
    vtype = "Data Type"
    if input_value is None:
        return _pass(vtype, label, None, None)
    target = input_value.strip().replace(' ', '')

    is_bool = (target is not None and target in ('0', '1'))
    failed = (not is_allowed and target is not None and is_bool) or \
             (is_allowed and target is not None and not is_bool)
    if failed:
        return _fail(vtype, label, target, target)
    return _pass(vtype, label, target, target)


def check_is_time(current_value: Optional[str], input_value: Optional[str],
                  is_allowed: bool, pattern_value: Optional[str] = None) -> CheckResult:
    """
    'Is Time' — DataType3 category.

    SQL: SET @TargetValue = REPLACE(TRIM(@InputValue), ' ', '')
         Valid time = LIKE '%[0-9:.]%' AND TRY_CAST AS DECIMAL IS NULL
                      AND ISNUMERIC=0 AND no second decimal point
                      AND TRY_CONVERT(TIME) NOT NULL AND LIKE '%:%' AND LEN BETWEEN 8 AND 16

         IF (is_allowed=1 AND NOT valid time) → FAIL
         IF (is_allowed=0 AND IS valid time)  → FAIL
    """
    label = f"Is Time {_allowed_label(is_allowed)}"
    vtype = "Data Type"
    if input_value is None:
        return _pass(vtype, label, None, None)
    target = input_value.strip().replace(' ', '')

    def _is_valid_time(s: str) -> bool:
        if not s or ':' not in s:
            return False
        if not (8 <= len(s) <= 16):
            return False
        # Must not be purely numeric / decimal
        try:
            float(s)
            return False
        except ValueError:
            pass
        # Must not have more than one decimal point
        if s.count('.') > 1:
            return False
        # Must parse as a time
        time_pattern = re.compile(r'^\d{1,2}:\d{2}(:\d{2}(\.\d+)?)?$')
        if not time_pattern.match(s):
            return False
        # Additional: re-validate hours/minutes/seconds are in range
        parts = re.split(r'[:.]', s)
        try:
            h, m = int(parts[0]), int(parts[1])
            return 0 <= h <= 23 and 0 <= m <= 59
        except (IndexError, ValueError):
            return False

    is_time = _is_valid_time(target)
    failed = (is_allowed and target is not None and not is_time) or \
             (not is_allowed and target is not None and is_time)
    if failed:
        return _fail(vtype, label, target, target)
    return _pass(vtype, label, target, target)


def check_is_date(current_value: Optional[str], input_value: Optional[str],
                  is_allowed: bool, pattern_value: Optional[str] = None) -> CheckResult:
    """
    'Is Date' — DataType3 category.

    SQL: SET @TargetValue = TRIM(@InputValue)   -- note: keeps spaces, only trims ends
         Valid date = ISDATE=1 AND NOT LIKE '%[:.]%' AND TRY_CAST AS BIGINT IS NULL
                      AND TRY_CONVERT(DATETIME) NOT NULL

         IF (is_allowed=1 AND NOT valid date) → FAIL
         IF (is_allowed=0 AND IS valid date)  → FAIL
    """
    label = f"Is Date {_allowed_label(is_allowed)}"
    vtype = "Data Type"
    if input_value is None:
        return _pass(vtype, label, None, None)
    target = input_value.strip()

    # Recognised date format patterns — mirrors SQL Server ISDATE() scope.
    # Uses explicit structural patterns rather than dateutil, which is far too
    # permissive (it parses "3982 2nd St" as year 3982, day 2 — but SQL
    # ISDATE("3982 2nd St") = 0 because the string is not a recognised format).
    _DATE_PATTERNS = [
        r'^\d{4}-\d{2}-\d{2}$',                               # 2024-01-15  (ISO)
        r'^\d{2}/\d{2}/\d{4}$',                               # 01/15/2024  (US) or 15/01/2024 (UK)
        r'^\d{2}-\d{2}-\d{4}$',                               # 15-01-2024
        r'^\d{1,2}\s+(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\s+\d{4}$',
        # 15 Jan 2024  /  15 January 2024
        r'^(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\s+\d{1,2},?\s+\d{4}$',
        # Jan 15, 2024  /  January 15 2024
        r'^\d{4}\d{2}\d{2}$',                                 # 20240115  (yyyymmdd compact)
        r'^\d{2}\.\d{2}\.\d{4}$',                             # 15.01.2024 (some EU locales)
    ]

    def _is_valid_date(s: str) -> bool:
        if not s or ':' in s:
            return False
        # Must not be castable as integer
        try:
            int(s)
            return False
        except ValueError:
            pass
        # Match against explicit date format patterns.
        # Using dateutil here would be incorrect: dateutil's parser accepts
        # arbitrary natural-language strings like "3982 2nd St" as valid dates,
        # whereas SQL Server ISDATE() only recognises structured date formats.
        return any(re.match(p, s, re.IGNORECASE) for p in _DATE_PATTERNS)

    is_date = _is_valid_date(target)
    failed = (is_allowed and target is not None and not is_date) or \
             (not is_allowed and target is not None and is_date)
    if failed:
        return _fail(vtype, label, target, target)
    return _pass(vtype, label, target, target)


def check_is_timestamp(current_value: Optional[str], input_value: Optional[str],
                        is_allowed: bool, pattern_value: Optional[str] = None) -> CheckResult:
    """
    'Is Timestamp' — DataType3 category.

    SQL: SET @TargetValue = TRIM(@InputValue)
         Valid timestamp = ISDATE=1 AND LIKE '%[:.]%' AND LIKE '%[-]%'
                           AND TRY_CAST AS BIGINT IS NULL
                           AND TRY_CONVERT(DATETIME) NOT NULL

         IF (is_allowed=1 AND NOT valid timestamp) → FAIL
         IF (is_allowed=0 AND IS valid timestamp)  → FAIL
    """
    label = f"Is Timestamp {_allowed_label(is_allowed)}"
    vtype = "Data Type"
    if input_value is None:
        return _pass(vtype, label, None, None)
    target = input_value.strip()

    def _is_valid_timestamp(s: str) -> bool:
        if not s or ':' not in s or '-' not in s:
            return False
        try:
            int(s)
            return False
        except ValueError:
            pass
        if _HAS_DATEUTIL:
            try:
                dt = _dateutil_parser.parse(s)
                # Must have a time component (not just a date)
                return dt.hour != 0 or dt.minute != 0 or dt.second != 0 or '.' in s
            except (ValueError, OverflowError):
                return False
        ts_pattern = re.compile(
            r'^\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}(:\d{2}(\.\d+)?)?$'
        )
        return bool(ts_pattern.match(s))

    is_ts = _is_valid_timestamp(target)
    failed = (is_allowed and target is not None and not is_ts) or \
             (not is_allowed and target is not None and is_ts)
    if failed:
        return _fail(vtype, label, target, target)
    return _pass(vtype, label, target, target)


def check_is_fully_text(current_value: Optional[str], input_value: Optional[str],
                         is_allowed: bool, pattern_value: Optional[str] = None) -> CheckResult:
    """
    'Is Fully Text' — DataType1 category.

    SQL: IF (is_allowed=1 AND value has ANY digit 0-9)  → FAIL
         IF (is_allowed=0 AND value has NO digits)       → FAIL

    Uses CHARINDEX for each digit 0-9 individually — all must be absent for
    the value to be considered "Fully Text".  Does NOT reset @TargetValue.
    """
    label = f"Is Fully Text {_allowed_label(is_allowed)}"
    vtype = "Data Type"

    def _is_fully_text(s: str) -> bool:
        """True when string contains no digit characters 0-9."""
        return s is not None and not any(c in s for c in '0123456789')

    is_text = _is_fully_text(current_value) if current_value is not None else False
    failed = (is_allowed and current_value is not None and not is_text) or \
             (not is_allowed and current_value is not None and is_text)
    if failed:
        return _fail(vtype, label, current_value, current_value)
    return _pass(vtype, label, current_value, current_value)


def check_is_alphanumeric(current_value: Optional[str], input_value: Optional[str],
                           is_allowed: bool, pattern_value: Optional[str] = None) -> CheckResult:
    """
    'Is AlphaNumeric' — DataType2 category.

    SQL: PATINDEX('%[0-9]%', @TargetValue) > 0  (has digits)
         AND PATINDEX('%[a-zA-Z]%', @TargetValue) > 0  (has letters)
         → Both must be present.

         IF (is_allowed=1 AND NOT alphanumeric) → FAIL
         IF (is_allowed=0 AND IS alphanumeric)  → FAIL
    """
    label = f"Is AlphaNumeric {_allowed_label(is_allowed)}"
    vtype = "Data Type"

    def _is_alphanumeric(s: str) -> bool:
        return (s is not None
                and any(c.isdigit() for c in s)
                and any(c.isalpha() for c in s))

    is_alpha = _is_alphanumeric(current_value)
    failed = (is_allowed and current_value is not None and not is_alpha) or \
             (not is_allowed and current_value is not None and is_alpha)
    if failed:
        return _fail(vtype, label, current_value, current_value)
    return _pass(vtype, label, current_value, current_value)


# ===========================================================================
# CasingCheck patterns
# ===========================================================================

def check_has_lowercase(current_value: Optional[str], input_value: Optional[str],
                         is_allowed: bool, pattern_value: Optional[str] = None) -> CheckResult:
    """
    'Has Lowercase Character' — CasingCheck category.

    SQL: IF (is_allowed = 0) AND (@TargetValue COLLATE Latin1_General_BIN LIKE '%[a-z]%')
         → Only fails when NOT allowed and lowercase is present.

    Uses case-sensitive regex to match a-z (mirrors SQL binary collation).
    """
    label = f"Has Lowercase Character {_allowed_label(is_allowed)}"
    vtype = "Casing Issue"
    if not is_allowed and current_value and re.search(r'[a-z]', current_value):
        return _fail(vtype, label, current_value, current_value)
    return _pass(vtype, label, current_value, current_value)


def check_has_uppercase(current_value: Optional[str], input_value: Optional[str],
                         is_allowed: bool, pattern_value: Optional[str] = None) -> CheckResult:
    """
    'Has Uppercase Character' — CasingCheck category.

    SQL: IF (is_allowed = 0) AND (@TargetValue COLLATE Latin1_General_BIN LIKE '%[A-Z]%')
         → Only fails when NOT allowed and uppercase is present.
    """
    label = f"Has Uppercase Character {_allowed_label(is_allowed)}"
    vtype = "Casing Issue"
    if not is_allowed and current_value and re.search(r'[A-Z]', current_value):
        return _fail(vtype, label, current_value, current_value)
    return _pass(vtype, label, current_value, current_value)


# ===========================================================================
# SpecialCharacter patterns (32 patterns, one per character/symbol)
# ===========================================================================

def check_special_character(current_value: Optional[str], input_value: Optional[str],
                              is_allowed: bool, pattern_value: Optional[str] = None) -> CheckResult:
    """
    SpecialCharacter category — any pattern with a non-null PatternValue.

    SQL: IF (is_allowed = 0) AND (@TargetValue LIKE '%$<char>%' ESCAPE '$')
         → FAIL + RETURN only when NOT allowed AND the character is present.
         When is_allowed=1, the IF condition is always False (1=0 AND ...) →
         the ELSE always executes → PASS without returning.

    ESCAPE '$' in LIKE means the $ is the escape character (not the char itself).
    In Python we use a simple `in` test since the char is taken literally.
    """
    label = f"Has '{pattern_value}' {_allowed_label(is_allowed)}"
    vtype = "Special Character"
    if not is_allowed and current_value is not None and pattern_value and pattern_value in current_value:
        return _fail(vtype, label, current_value, current_value)
    return _pass(vtype, label, current_value, current_value)


# ===========================================================================
# InvalidKeyword patterns (40 patterns)
# ===========================================================================

def check_invalid_keyword(current_value: Optional[str], input_value: Optional[str],
                           is_allowed: bool, pattern_value: Optional[str] = None) -> CheckResult:
    """
    'Has Keyword-<word>' — InvalidKeyword category.

    SQL:
        SET @TargetValue = LOWER(TRIM(@InputValue))
        SET @Threshold = 0.5   -- hardcoded proportion threshold
        SET @KeyWord = '<pattern_value>'
        SET @keyWordLength = LEN(@KeyWord)
        SET @inputValueLength = LEN(@TargetValue)
        IF @inputValueLength > 0
        BEGIN
            SET @Occurrences = (LEN(@TargetValue) - LEN(REPLACE(@TargetValue, @KeyWord, ''))) / LEN(@KeyWord)
            SET @Proportion  = CAST(@Occurrences * LEN(@KeyWord) AS FLOAT) / @inputValueLength
        END
        IF (is_allowed = 0) AND NULLIF(@TargetValue, '') IS NOT NULL
           AND (LEN(@TargetValue) > 0 AND @Proportion >= @Threshold)
        → FAIL

    The keyword must comprise at least 50% of the lowercased value to trigger.
    """
    target = input_value.lower().strip() if input_value else ''
    label = f"Has Keyword-'{pattern_value}' {_allowed_label(is_allowed)}"
    vtype = "Invalid Keyword"
    threshold = 0.5  # Hardcoded in SQL: SET @Threshold = 0.5

    if not is_allowed and target and pattern_value:
        keyword = pattern_value.lower()
        keyword_len = len(keyword)
        input_len = len(target)
        if input_len > 0 and keyword_len > 0:
            occurrences = (len(target) - len(target.replace(keyword, ''))) // keyword_len
            proportion = (occurrences * keyword_len) / input_len
            if proportion >= threshold:
                return _fail(vtype, label, target, target)
    return _pass(vtype, label, target, target)


# ===========================================================================
# FullyDuplicatedCharacter patterns
# ===========================================================================

def check_fully_duplicated_character(current_value: Optional[str], input_value: Optional[str],
                                      is_allowed: bool,
                                      pattern_value: Optional[str] = None) -> CheckResult:
    """
    'Has Fully Duplicated Character' — FullyDuplicatedCharacter category.

    SQL:
        SET @TargetValue = REPLACE(TRIM(@InputValue), ' ', '')
        SET @FirstChar = LEFT(@TargetValue, 1)
        SET @RepeatedString = REPLICATE(@FirstChar, LEN(@TargetValue))
        IF (is_allowed = 0) AND (LEN(@TargetValue) > 1
                AND TRY_CAST(@TargetValue AS FLOAT) IS NULL)  -- numeric values excluded
                AND @TargetValue = @RepeatedString
        → FAIL

    Numeric values (castable as FLOAT) are excluded intentionally to avoid
    false positives on legitimate integers like '111' or '1000'.
    """
    target = input_value.strip().replace(' ', '') if input_value else ''
    label = f"Has Fully Duplicated Character {_allowed_label(is_allowed)}"
    vtype = "Fully Duplicated Character"

    if not is_allowed and len(target) > 1:
        # Exclude numerics (TRY_CAST AS FLOAT IS NULL means NOT castable as float)
        try:
            float(target)
            is_numeric_castable = True
        except ValueError:
            is_numeric_castable = False

        if not is_numeric_castable and target == target[0] * len(target):
            return _fail(vtype, label, target, target)
    return _pass(vtype, label, target, target)


# ===========================================================================
# UnicodeCharacters patterns
# ===========================================================================

def check_unicode_characters(current_value: Optional[str], input_value: Optional[str],
                               is_allowed: bool,
                               pattern_value: Optional[str] = None) -> CheckResult:
    """
    'Has Unicode Characters' — UnicodeCharacters category.

    SQL:
        SET @TargetValue = REPLACE(TRIM(@InputValue), ' ', '')
        IF (is_allowed = 0) AND (LEN(@TargetValue) > 1
                AND @TargetValue COLLATE Latin1_General_BIN LIKE '%[^ -~]%')
        → FAIL

    LIKE '%[^ -~]%' with Latin1_General_BIN collation detects any character
    outside the printable ASCII range (0x20 = SPACE to 0x7E = TILDE).
    Hidden control characters (CR, LF, TAB) also trigger this check.
    """
    target = input_value.strip().replace(' ', '') if input_value else ''
    label = f"Has Unicode Characters {_allowed_label(is_allowed)}"
    vtype = "Unicode Characters"

    if not is_allowed and len(target) > 1:
        # Detect chars outside printable ASCII (space 0x20 to tilde 0x7E)
        if any(ord(c) < 0x20 or ord(c) > 0x7E for c in target):
            return _fail(vtype, label, target, target)
    return _pass(vtype, label, target, target)


# ===========================================================================
# Pattern name → check function dispatch table
#
# Used by the generator to look up the correct Python check function for each
# pattern name from masterPattern, mirroring the CASE block in L03.
# ===========================================================================

#: Maps (PatternCategory, PatternName) → check function
#: For category-level dispatch (when PatternName is None), only PatternCategory is used.
PATTERN_DISPATCH: dict = {
    # DataEmptiness
    ("DataEmptiness", "Is Empty or NULL"):
        check_is_empty_or_null,
    ("DataEmptiness", "Is Virtually Empty with Spaces"):
        check_is_virtually_empty_with_spaces,
    # DataEmptiness "Is Virtually Empty with X" — dispatched by category + name prefix
    # All other DataEmptiness patterns with a PatternValue use check_virtually_empty_with_char
    ("DataEmptiness", "_virtually_empty_with_char"):
        check_virtually_empty_with_char,

    # SpaceFound
    ("SpaceFound", "Has Space"):
        check_has_space,
    # Some configs use "Space" as category (older naming)
    ("Space", "Has Space"):
        check_has_space,

    # DataType1
    ("DataType1", "Is Fully Numeric"):
        check_is_fully_numeric,
    ("DataType1", "Is Fully Decimal"):
        check_is_fully_decimal,
    ("DataType1", "Is Fully Text"):
        check_is_fully_text,

    # DataType2
    ("DataType2", "Is AlphaNumeric"):
        check_is_alphanumeric,

    # DataType3
    ("DataType3", "Is Date"):
        check_is_date,
    ("DataType3", "Is Time"):
        check_is_time,
    ("DataType3", "Is Timestamp"):
        check_is_timestamp,
    ("DataType3", "Is Boolean"):
        check_is_boolean,

    # CasingCheck
    ("CasingCheck", "Has Lowercase Character"):
        check_has_lowercase,
    ("CasingCheck", "Has Uppercase Character"):
        check_has_uppercase,

    # SpecialCharacter — all dispatched to check_special_character
    ("SpecialCharacter", "_dispatch"):
        check_special_character,

    # DataEmptiness with PatternValue — dispatched to check_virtually_empty_with_char
    # (handled explicitly in resolve step)

    # InvalidKeyword — all dispatched to check_invalid_keyword
    ("InvalidKeyword", "_dispatch"):
        check_invalid_keyword,

    # FullyDuplicatedCharacter
    ("FullyDuplicatedCharacter", "Has Fully Duplicated Character"):
        check_fully_duplicated_character,

    # UnicodeCharacters
    ("UnicodeCharacters", "Has Unicode Characters"):
        check_unicode_characters,
}


def resolve_pattern_check_fn(pattern_category: str, pattern_name: str,
                              pattern_value: Optional[str]):
    """
    Return the appropriate check function for a given pattern.

    Mirrors the CASE block in p_DQ_GenerateRuleFunctions (L03):
      WHEN PatternCategory = 'DataEmptiness' AND PatternName = 'Is Empty or NULL' THEN ...
      WHEN PatternCategory = 'SpecialCharacter' AND NULLIF(PatternValue,'') IS NOT NULL THEN ...
      etc.
    """
    key = (pattern_category, pattern_name)

    # Direct lookup
    if key in PATTERN_DISPATCH:
        return PATTERN_DISPATCH[key]

    # SpecialCharacter: any pattern in this category with a non-null PatternValue
    if pattern_category == "SpecialCharacter" and pattern_value:
        return PATTERN_DISPATCH[("SpecialCharacter", "_dispatch")]

    # DataEmptiness with a PatternValue and name contains "Virtually Empty"
    if (pattern_category == "DataEmptiness"
            and pattern_value
            and "Virtually Empty" in (pattern_name or "")):
        return check_virtually_empty_with_char

    # InvalidKeyword: any pattern with PatternValue and name starts with "Has Keyword-"
    if (pattern_category == "InvalidKeyword"
            and pattern_value
            and (pattern_name or "").startswith("Has Keyword-")):
        return PATTERN_DISPATCH[("InvalidKeyword", "_dispatch")]

    # DataType categories — try by category prefix
    for cat_prefix in ("DataType1", "DataType2", "DataType3"):
        if pattern_category == cat_prefix:
            cat_key = (cat_prefix, pattern_name)
            if cat_key in PATTERN_DISPATCH:
                return PATTERN_DISPATCH[cat_key]

    return None  # Unrecognised pattern — generator will skip (ELSE NULL in SQL)
