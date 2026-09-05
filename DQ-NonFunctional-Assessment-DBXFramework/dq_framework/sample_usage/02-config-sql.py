# Databricks notebook source
# MAGIC %md
# MAGIC # 02 — Config Seed: SQL MERGE
# MAGIC Seeds all 5 config tables using SQL MERGE statements.
# MAGIC Use this notebook **OR** `02-config-pyspark.py` — not both.
# MAGIC Requires `01-install.py` to have been run in this session first.
# MAGIC
# MAGIC > **Note on configCustomQuery:** The expression values use `\'` escaping (Databricks
# MAGIC > legacy mode). The rows are placed in a `r"""..."""` raw string so Python does not
# MAGIC > process the backslashes before Spark SQL sees them. The outer MERGE wrapper uses
# MAGIC > `f"""..."""` only for catalog/schema interpolation.

# COMMAND ----------

# DBTITLE 1,Load Install (variables + dq instance)
# MAGIC %run ./01-install

# COMMAND ----------

# MAGIC %md
# MAGIC ## masterField

# COMMAND ----------

# DBTITLE 1,Upsert masterField -SQL
spark.sql(f"""
MERGE INTO `{MY_CATALOG}`.`{DQ_SCHEMA}`.`masterField` AS t
USING (
    SELECT * FROM (
        SELECT NULL AS _ID, NULL AS FullFieldName, NULL AS DataCategoryTypeID, NULL AS IsActive, NULL AS CreatedBy, NULL AS CreatedOn, NULL AS LastUpdatedBy, NULL AS LastUpdatedOn UNION ALL
        SELECT 1,  'postal_code_all_country',      10, true, current_user(), current_timestamp(), NULL, NULL UNION ALL
        SELECT 2,  'province',                      3,  true, current_user(), current_timestamp(), NULL, NULL UNION ALL
        SELECT 3,  'state',                         3,  true, current_user(), current_timestamp(), NULL, NULL UNION ALL
        SELECT 4,  'city',                          3,  true, current_user(), current_timestamp(), NULL, NULL UNION ALL
        SELECT 5,  'address_line_1',                2,  true, current_user(), current_timestamp(), NULL, NULL UNION ALL
        SELECT 6,  'address_line_2',                2,  true, current_user(), current_timestamp(), NULL, NULL UNION ALL
        SELECT 7,  'address_general',               2,  true, current_user(), current_timestamp(), NULL, NULL UNION ALL
        SELECT 8,  'address_short',                 3,  true, current_user(), current_timestamp(), NULL, NULL UNION ALL
        SELECT 9,  'country',                       3,  true, current_user(), current_timestamp(), NULL, NULL UNION ALL
        SELECT 10, 'county',                        10, true, current_user(), current_timestamp(), NULL, NULL UNION ALL
        SELECT 11, 'email_address',                 4,  true, current_user(), current_timestamp(), NULL, NULL UNION ALL
        SELECT 12, 'phone_number',                  12, true, current_user(), current_timestamp(), NULL, NULL UNION ALL
        SELECT 13, 'mobile_number_without_country', 14, true, current_user(), current_timestamp(), NULL, NULL UNION ALL
        SELECT 14, 'vendor_name',                   2,  true, current_user(), current_timestamp(), NULL, NULL UNION ALL
        SELECT 15, 'first_name',                    2,  true, current_user(), current_timestamp(), NULL, NULL UNION ALL
        SELECT 16, 'name_general',                  2,  true, 'sys', current_timestamp(), NULL, NULL UNION ALL
        SELECT 17, 'trade_type',                    1,  true, 'sys', current_timestamp(), NULL, NULL UNION ALL
        SELECT 18, 'check_vendor_license_number',   11, true, 'sys', current_timestamp(), NULL, NULL
    ) WHERE _ID IS NOT NULL
) AS s ON t._ID = s._ID
WHEN MATCHED AND (
    t.FullFieldName      <> s.FullFieldName OR
    t.DataCategoryTypeID <> s.DataCategoryTypeID OR
    t.IsActive           <> s.IsActive
) THEN UPDATE SET
    t.FullFieldName      = s.FullFieldName,
    t.DataCategoryTypeID = s.DataCategoryTypeID,
    t.IsActive           = s.IsActive,
    t.LastUpdatedBy      = current_user(),
    t.LastUpdatedOn      = current_timestamp()
WHEN NOT MATCHED THEN INSERT (
    _ID, FullFieldName, DataCategoryTypeID, IsActive,
    CreatedBy, CreatedOn, LastUpdatedBy, LastUpdatedOn
) VALUES (
    s._ID, s.FullFieldName, s.DataCategoryTypeID, s.IsActive,
    s.CreatedBy, s.CreatedOn, s.LastUpdatedBy, s.LastUpdatedOn
)
""")

# COMMAND ----------

# DBTITLE 1,masterField -Data
display(spark.sql(f"SELECT * FROM `{MY_CATALOG}`.`{DQ_SCHEMA}`.`masterField`"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## configFieldValues

# COMMAND ----------

# DBTITLE 1,Upsert configFieldValues -SQL
spark.sql(f"""
MERGE INTO `{MY_CATALOG}`.`{DQ_SCHEMA}`.`configFieldValues` AS t
USING (
    SELECT * FROM (
        SELECT NULL AS _ID, NULL AS FieldID, NULL AS FullFieldName, NULL AS MinDataLength, NULL AS MaxDataLength, NULL AS MinDataValue, NULL AS MaxDataValue, NULL AS IsActive, NULL AS CreatedBy, NULL AS CreatedOn, NULL AS LastUpdatedBy, NULL AS LastUpdatedOn UNION ALL
        SELECT 1,  1,  'postal_code_all_country',       5,   10,  '00001',        'ZZZ 9ZZ',               true, 'sys', current_timestamp(), NULL, NULL UNION ALL
        SELECT 2,  2,  'province',                       4,   20,  'aaaa',          'ZZZZZZZZZZZZZZZZZZZZ', true, 'sys', current_timestamp(), NULL, NULL UNION ALL
        SELECT 3,  3,  'state',                          3,   20,  'aaaa',          'ZZZZZZZZZZZZZZZZZZZZ', true, 'sys', current_timestamp(), NULL, NULL UNION ALL
        SELECT 4,  4,  'city',                           3,   20,  NULL,            NULL,                   true, 'sys', current_timestamp(), NULL, NULL UNION ALL
        SELECT 5,  5,  'address_line_1',                 5,   100, NULL,            NULL,                   true, 'sys', current_timestamp(), NULL, NULL UNION ALL
        SELECT 6,  6,  'address_line_2',                 5,   100, NULL,            NULL,                   true, 'sys', current_timestamp(), NULL, NULL UNION ALL
        SELECT 7,  7,  'address_general',                5,   100, NULL,            NULL,                   true, 'sys', current_timestamp(), NULL, NULL UNION ALL
        SELECT 8,  8,  'address_short',                  5,   15,  NULL,            NULL,                   true, 'sys', current_timestamp(), NULL, NULL UNION ALL
        SELECT 9,  9,  'country',                        2,   2,   NULL,            NULL,                   true, 'sys', current_timestamp(), NULL, NULL UNION ALL
        SELECT 10, 10, 'county',                         3,   20,  NULL,            NULL,                   true, 'sys', current_timestamp(), NULL, NULL UNION ALL
        SELECT 11, 11, 'email_address',                  6,   255, NULL,            NULL,                   true, 'sys', current_timestamp(), NULL, NULL UNION ALL
        SELECT 12, 12, 'phone_number',                   6,   16,  '100000',        '999999999999999',       true, 'sys', current_timestamp(), NULL, NULL UNION ALL
        SELECT 13, 13, 'mobile_number_without_country',  6,   16,  '60000000000',   '9999999999',            true, 'sys', current_timestamp(), NULL, NULL UNION ALL
        SELECT 14, 14, 'vendor_name',                    5,   50,  NULL,            NULL,                   true, 'sys', current_timestamp(), NULL, NULL UNION ALL
        SELECT 15, 15, 'first_name',                     2,   20,  NULL,            NULL,                   true, 'sys', current_timestamp(), NULL, NULL UNION ALL
        SELECT 16, 16, 'name_general',                   2,   20,  NULL,            NULL,                   true, 'sys', current_timestamp(), NULL, NULL UNION ALL
        SELECT 17, 17, 'trade_type',                     5,   10,  NULL,            NULL,                   true, 'sys', current_timestamp(), NULL, NULL
    ) WHERE _ID IS NOT NULL
) AS s ON t._ID = s._ID
WHEN MATCHED AND (
    t.MinDataLength <> s.MinDataLength OR t.MaxDataLength <> s.MaxDataLength OR
    t.MinDataValue  <> s.MinDataValue  OR t.MaxDataValue  <> s.MaxDataValue  OR
    t.IsActive      <> s.IsActive
) THEN UPDATE SET
    t.MinDataLength = s.MinDataLength,
    t.MaxDataLength = s.MaxDataLength,
    t.MinDataValue  = s.MinDataValue,
    t.MaxDataValue  = s.MaxDataValue,
    t.IsActive      = s.IsActive,
    t.LastUpdatedBy = current_user(),
    t.LastUpdatedOn = current_timestamp()
WHEN NOT MATCHED THEN INSERT (
    _ID, FieldID, FullFieldName, MinDataLength, MaxDataLength,
    MinDataValue, MaxDataValue, IsActive,
    CreatedBy, CreatedOn, LastUpdatedBy, LastUpdatedOn
) VALUES (
    s._ID, s.FieldID, s.FullFieldName, s.MinDataLength, s.MaxDataLength,
    s.MinDataValue, s.MaxDataValue, s.IsActive,
    s.CreatedBy, s.CreatedOn, s.LastUpdatedBy, s.LastUpdatedOn
)
""")

# COMMAND ----------

# DBTITLE 1,configFieldValues -Data
display(spark.sql(f"SELECT * FROM `{MY_CATALOG}`.`{DQ_SCHEMA}`.`configFieldValues`"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## configFieldAllowedPattern

# COMMAND ----------

# DBTITLE 1,Upsert configFieldAllowedPattern -SQL
spark.sql(f"""
MERGE INTO `{MY_CATALOG}`.`{DQ_SCHEMA}`.`configFieldAllowedPattern` AS t
USING (
    SELECT * FROM (
        SELECT NULL AS _ID, NULL AS FullFieldName, NULL AS PatternCategory, NULL AS PatternSubCategory, NULL AS PatternName, NULL AS IsPatternAllowed, NULL AS IsActive, NULL AS CreatedBy, NULL AS CreatedOn, NULL AS LastUpdatedBy, NULL AS LastUpdatedOn UNION ALL 
        SELECT 1, 'email_address', NULL, NULL, 'Is Fully Numeric', false, true, current_user(), current_timestamp(), NULL, NULL UNION ALL 
        SELECT 2, 'email_address', NULL, NULL, 'Is Fully Decimal', false, true, current_user(), current_timestamp(), NULL, NULL UNION ALL 
        SELECT 3, 'email_address', 'DataType3', NULL, NULL, false, true, current_user(), current_timestamp(), NULL, NULL UNION ALL 
        SELECT 4, 'email_address', 'SpecialCharacter', NULL, NULL, false, true, current_user(), current_timestamp(), NULL, NULL UNION ALL 
        SELECT 5, 'email_address', NULL, NULL, 'Has Full Stop', true, true, current_user(), current_timestamp(), NULL, NULL UNION ALL 
        SELECT 6, 'email_address', NULL, NULL, 'Has Hyphen', true, true, current_user(), current_timestamp(), NULL, NULL UNION ALL 
        SELECT 7, 'email_address', NULL, NULL, 'Has Underscore', true, true, current_user(), current_timestamp(), NULL, NULL UNION ALL 
        SELECT 8, 'email_address', NULL, NULL, 'Has At Sign', true, true, current_user(), current_timestamp(), NULL, NULL UNION ALL 
        SELECT 9, 'email_address', NULL, 'Emptiness', NULL, false, true, current_user(), current_timestamp(), NULL, NULL UNION ALL 
        SELECT 10, 'email_address', 'InvalidKeyword', NULL, NULL, false, true, current_user(), current_timestamp(), NULL, NULL UNION ALL 
        SELECT 11, 'email_address', 'FullyDuplicatedCharacter', NULL, NULL, false, true, current_user(), current_timestamp(), NULL, NULL UNION ALL 
        SELECT 12, 'email_address', 'UnicodeCharacters', NULL, NULL, false, true, current_user(), current_timestamp(), NULL, NULL UNION ALL 
        SELECT 13, 'email_address', NULL, NULL, 'Is Empty or NULL', true, true, current_user(), current_timestamp(), NULL, NULL UNION ALL 
        SELECT 14, 'email_address', NULL, NULL, 'Has Space', false, true, current_user(), current_timestamp(), NULL, NULL UNION ALL 
        SELECT 15, 'postal_code_all_country', 'DataType3', NULL, NULL, false, true, current_user(), current_timestamp(), NULL, NULL UNION ALL 
        SELECT 16, 'postal_code_all_country', 'InvalidKeyword', NULL, NULL, false, true, current_user(), current_timestamp(), NULL, NULL UNION ALL 
        SELECT 17, 'postal_code_all_country', 'FullyDuplicatedCharacter', NULL, NULL, false, true, current_user(), current_timestamp(), NULL, NULL UNION ALL 
        SELECT 18, 'postal_code_all_country', 'UnicodeCharacters', NULL, NULL, false, true, current_user(), current_timestamp(), NULL, NULL UNION ALL 
        SELECT 19, 'postal_code_all_country', 'SpecialCharacter', NULL, NULL, false, true, current_user(), current_timestamp(), NULL, NULL UNION ALL 
        SELECT 20, 'postal_code_all_country', NULL, 'Emptiness', NULL, false, true, current_user(), current_timestamp(), NULL, NULL UNION ALL 
        SELECT 21, 'postal_code_all_country', NULL, NULL, 'Has Space', true, true, current_user(), current_timestamp(), NULL, NULL UNION ALL 
        SELECT 22, 'postal_code_all_country', NULL, NULL, 'Is Fully Decimal', false, true, current_user(), current_timestamp(), NULL, NULL UNION ALL 
        SELECT 23, 'postal_code_all_country', NULL, NULL, 'Has Lowercase Character', false, true, current_user(), current_timestamp(), NULL, NULL UNION ALL 
        SELECT 24, 'state', 'DataType1', NULL, NULL, false, true, current_user(), current_timestamp(), NULL, NULL UNION ALL 
        SELECT 25, 'state', 'DataType2', NULL, NULL, false, true, current_user(), current_timestamp(), NULL, NULL UNION ALL 
        SELECT 26, 'state', 'DataType3', NULL, NULL, false, true, current_user(), current_timestamp(), NULL, NULL UNION ALL 
        SELECT 27, 'state', NULL, NULL, 'Is Fully Text', true, true, current_user(), current_timestamp(), NULL, NULL UNION ALL 
        SELECT 28, 'state', 'InvalidKeyword', NULL, NULL, false, true, current_user(), current_timestamp(), NULL, NULL UNION ALL 
        SELECT 29, 'state', 'FullyDuplicatedCharacter', NULL, NULL, false, true, current_user(), current_timestamp(), NULL, NULL UNION ALL 
        SELECT 30, 'state', 'UnicodeCharacters', NULL, NULL, false, true, current_user(), current_timestamp(), NULL, NULL UNION ALL 
        SELECT 31, 'state', 'SpecialCharacter', NULL, NULL, false, true, current_user(), current_timestamp(), NULL, NULL UNION ALL 
        SELECT 32, 'state', NULL, 'Emptiness', NULL, false, true, current_user(), current_timestamp(), NULL, NULL UNION ALL 
        SELECT 33, 'state', NULL, NULL, 'Has Space', true, true, current_user(), current_timestamp(), NULL, NULL UNION ALL 
        SELECT 34, 'city', 'DataType1', NULL, NULL, false, true, current_user(), current_timestamp(), NULL, NULL UNION ALL 
        SELECT 35, 'city', 'DataType2', NULL, NULL, false, true, current_user(), current_timestamp(), NULL, NULL UNION ALL 
        SELECT 36, 'city', 'DataType3', NULL, NULL, false, true, current_user(), current_timestamp(), NULL, NULL UNION ALL 
        SELECT 37, 'city', NULL, NULL, 'Is Fully Text', true, true, current_user(), current_timestamp(), NULL, NULL UNION ALL 
        SELECT 38, 'city', 'InvalidKeyword', NULL, NULL, false, true, current_user(), current_timestamp(), NULL, NULL UNION ALL 
        SELECT 39, 'city', 'FullyDuplicatedCharacter', NULL, NULL, false, true, current_user(), current_timestamp(), NULL, NULL UNION ALL 
        SELECT 40, 'city', 'UnicodeCharacters', NULL, NULL, false, true, current_user(), current_timestamp(), NULL, NULL UNION ALL 
        SELECT 41, 'city', 'SpecialCharacter', NULL, NULL, false, true, current_user(), current_timestamp(), NULL, NULL UNION ALL 
        SELECT 42, 'city', NULL, 'Emptiness', NULL, false, true, current_user(), current_timestamp(), NULL, NULL UNION ALL 
        SELECT 43, 'city', NULL, NULL, 'Has Space', true, true, current_user(), current_timestamp(), NULL, NULL UNION ALL 
        SELECT 44, 'address_line_1', 'DataType3', NULL, NULL, false, true, current_user(), current_timestamp(), NULL, NULL UNION ALL 
        SELECT 45, 'address_line_1', 'InvalidKeyword', NULL, NULL, false, true, current_user(), current_timestamp(), NULL, NULL UNION ALL 
        SELECT 46, 'address_line_1', 'FullyDuplicatedCharacter', NULL, NULL, false, true, current_user(), current_timestamp(), NULL, NULL UNION ALL 
        SELECT 47, 'address_line_1', 'UnicodeCharacters', NULL, NULL, false, true, current_user(), current_timestamp(), NULL, NULL UNION ALL 
        SELECT 48, 'address_line_1', 'SpecialCharacter', NULL, NULL, false, true, current_user(), current_timestamp(), NULL, NULL UNION ALL 
        SELECT 49, 'address_line_1', NULL, 'Emptiness', NULL, false, true, current_user(), current_timestamp(), NULL, NULL UNION ALL 
        SELECT 50, 'address_line_1', NULL, 'SpecialCharacter-L1', NULL, true, true, current_user(), current_timestamp(), NULL, NULL UNION ALL 
        SELECT 51, 'address_line_1', NULL, 'SpecialCharacter-L2', NULL, true, true, current_user(), current_timestamp(), NULL, NULL UNION ALL 
        SELECT 52, 'address_line_1', NULL, 'SpecialCharacter-L3', NULL, true, true, current_user(), current_timestamp(), NULL, NULL UNION ALL 
        SELECT 53, 'address_line_1', NULL, NULL, 'Has Exclamation Mark', false, true, current_user(), current_timestamp(), NULL, NULL UNION ALL 
        SELECT 54, 'address_line_1', NULL, NULL, 'Has Question Mark', false, true, current_user(), current_timestamp(), NULL, NULL UNION ALL 
        SELECT 55, 'address_line_1', NULL, NULL, 'Has Space', true, true, current_user(), current_timestamp(), NULL, NULL UNION ALL 
        SELECT 56, 'address_line_1', NULL, NULL, 'Is Fully Decimal', false, true, current_user(), current_timestamp(), NULL, NULL UNION ALL 
        SELECT 57, 'address_line_1', NULL, NULL, 'Is Fully Numeric', false, true, current_user(), current_timestamp(), NULL, NULL UNION ALL 
        SELECT 58, 'country', 'DataType1', NULL, NULL, false, true, current_user(), current_timestamp(), NULL, NULL UNION ALL 
        SELECT 59, 'country', 'DataType2', NULL, NULL, false, true, current_user(), current_timestamp(), NULL, NULL UNION ALL 
        SELECT 60, 'country', 'DataType3', NULL, NULL, false, true, current_user(), current_timestamp(), NULL, NULL UNION ALL 
        SELECT 61, 'country', NULL, NULL, 'Is Fully Text', true, true, current_user(), current_timestamp(), NULL, NULL UNION ALL 
        SELECT 62, 'country', 'InvalidKeyword', NULL, NULL, false, true, current_user(), current_timestamp(), NULL, NULL UNION ALL 
        SELECT 63, 'country', 'FullyDuplicatedCharacter', NULL, NULL, false, true, current_user(), current_timestamp(), NULL, NULL UNION ALL 
        SELECT 64, 'country', 'UnicodeCharacters', NULL, NULL, false, true, current_user(), current_timestamp(), NULL, NULL UNION ALL 
        SELECT 65, 'country', 'SpecialCharacter', NULL, NULL, false, true, current_user(), current_timestamp(), NULL, NULL UNION ALL 
        SELECT 66, 'country', NULL, 'Emptiness', NULL, false, true, current_user(), current_timestamp(), NULL, NULL UNION ALL 
        SELECT 67, 'country', NULL, NULL, 'Has Space', false, true, current_user(), current_timestamp(), NULL, NULL UNION ALL 
        SELECT 68, 'country', NULL, NULL, 'Has Lowercase Character', false, true, current_user(), current_timestamp(), NULL, NULL UNION ALL 
        SELECT 69, 'vendor_name', 'DataType3', NULL, NULL, false, true, current_user(), current_timestamp(), NULL, NULL UNION ALL 
        SELECT 70, 'vendor_name', 'InvalidKeyword', NULL, NULL, false, true, current_user(), current_timestamp(), NULL, NULL UNION ALL 
        SELECT 71, 'vendor_name', 'FullyDuplicatedCharacter', NULL, NULL, false, true, current_user(), current_timestamp(), NULL, NULL UNION ALL 
        SELECT 72, 'vendor_name', 'UnicodeCharacters', NULL, NULL, false, true, current_user(), current_timestamp(), NULL, NULL UNION ALL 
        SELECT 73, 'vendor_name', 'SpecialCharacter', NULL, NULL, false, true, current_user(), current_timestamp(), NULL, NULL UNION ALL 
        SELECT 74, 'vendor_name', NULL, 'Emptiness', NULL, false, true, current_user(), current_timestamp(), NULL, NULL UNION ALL 
        SELECT 75, 'vendor_name', NULL, NULL, 'Has Space', true, true, current_user(), current_timestamp(), NULL, NULL UNION ALL 
        SELECT 76, 'vendor_name', NULL, NULL, 'Is Fully Decimal', false, true, current_user(), current_timestamp(), NULL, NULL UNION ALL 
        SELECT 77, 'vendor_name', NULL, NULL, 'Is Fully Numeric', false, true, current_user(), current_timestamp(), NULL, NULL UNION ALL 
        SELECT 78, 'vendor_name', NULL, NULL, 'Has Lowercase Character', false, true, current_user(), current_timestamp(), NULL, NULL UNION ALL 
        SELECT 79, 'first_name', 'DataType2', NULL, NULL, false, true, current_user(), current_timestamp(), NULL, NULL UNION ALL 
        SELECT 80, 'first_name', 'DataType3', NULL, NULL, false, true, current_user(), current_timestamp(), NULL, NULL UNION ALL 
        SELECT 81, 'first_name', 'InvalidKeyword', NULL, NULL, false, true, current_user(), current_timestamp(), NULL, NULL UNION ALL 
        SELECT 82, 'first_name', 'FullyDuplicatedCharacter', NULL, NULL, false, true, current_user(), current_timestamp(), NULL, NULL UNION ALL 
        SELECT 83, 'first_name', 'UnicodeCharacters', NULL, NULL, false, true, current_user(), current_timestamp(), NULL, NULL UNION ALL 
        SELECT 84, 'first_name', 'SpecialCharacter', NULL, NULL, false, true, current_user(), current_timestamp(), NULL, NULL UNION ALL 
        SELECT 85, 'first_name', NULL, 'Emptiness', NULL, false, true, current_user(), current_timestamp(), NULL, NULL UNION ALL 
        SELECT 86, 'first_name', NULL, NULL, 'Has Space', true, true, current_user(), current_timestamp(), NULL, NULL UNION ALL 
        SELECT 87, 'first_name', NULL, NULL, 'Is Fully Decimal', false, true, current_user(), current_timestamp(), NULL, NULL UNION ALL 
        SELECT 88, 'first_name', NULL, NULL, 'Is Fully Numeric', false, true, current_user(), current_timestamp(), NULL, NULL UNION ALL 
        SELECT 89, 'first_name', 'DataType1', NULL, NULL, false, true, current_user(), current_timestamp(), NULL, NULL UNION ALL 
        SELECT 90, 'name_general', 'DataType2', NULL, NULL, false, true, current_user(), current_timestamp(), NULL, NULL UNION ALL 
        SELECT 91, 'name_general', 'DataType3', NULL, NULL, false, true, current_user(), current_timestamp(), NULL, NULL UNION ALL 
        SELECT 92, 'name_general', 'InvalidKeyword', NULL, NULL, false, true, current_user(), current_timestamp(), NULL, NULL UNION ALL 
        SELECT 93, 'name_general', 'FullyDuplicatedCharacter', NULL, NULL, false, true, current_user(), current_timestamp(), NULL, NULL UNION ALL 
        SELECT 94, 'name_general', 'UnicodeCharacters', NULL, NULL, true, true, current_user(), current_timestamp(), NULL, NULL UNION ALL 
        SELECT 95, 'name_general', 'SpecialCharacter', NULL, NULL, false, true, current_user(), current_timestamp(), NULL, NULL UNION ALL 
        SELECT 96, 'name_general', NULL, 'Emptiness', NULL, false, true, current_user(), current_timestamp(), NULL, NULL UNION ALL 
        SELECT 97, 'name_general', NULL, NULL, 'Has Space', false, true, current_user(), current_timestamp(), NULL, NULL UNION ALL 
        SELECT 98, 'name_general', NULL, NULL, 'Is Fully Text', true, true, current_user(), current_timestamp(), NULL, NULL UNION ALL 
        SELECT 99, 'name_general', NULL, NULL, 'Has Lowercase Character', true, true, current_user(), current_timestamp(), NULL, NULL UNION ALL 
        SELECT 100, 'name_general', NULL, NULL, 'Has Hyphen', true, true, current_user(), current_timestamp(), NULL, NULL UNION ALL 
        SELECT 101, 'name_general', NULL, NULL, 'Has Full Stop', true, true, current_user(), current_timestamp(), NULL, NULL UNION ALL 
        SELECT 102, 'name_general', NULL, NULL, 'Has Single Quote', true, true, current_user(), current_timestamp(), NULL, NULL UNION ALL 
        SELECT 103, 'name_general', NULL, NULL, NULL, false, false, current_user(), current_timestamp(), NULL, NULL UNION ALL 
        SELECT 104, 'mobile_number_without_country', 'DataType1', NULL, NULL, false, true, current_user(), current_timestamp(), NULL, NULL UNION ALL 
        SELECT 105, 'mobile_number_without_country', 'DataType2', NULL, NULL, false, true, current_user(), current_timestamp(), NULL, NULL UNION ALL 
        SELECT 106, 'mobile_number_without_country', 'DataType3', NULL, NULL, false, true, current_user(), current_timestamp(), NULL, NULL UNION ALL 
        SELECT 107, 'mobile_number_without_country', 'SpecialCharacter', NULL, NULL, false, true, current_user(), current_timestamp(), NULL, NULL UNION ALL 
        SELECT 108, 'mobile_number_without_country', 'InvalidKeyword', NULL, NULL, false, true, current_user(), current_timestamp(), NULL, NULL UNION ALL 
        SELECT 109, 'mobile_number_without_country', 'FullyDuplicatedCharacter', NULL, NULL, false, true, current_user(), current_timestamp(), NULL, NULL UNION ALL 
        SELECT 110, 'mobile_number_without_country', 'UnicodeCharacters', NULL, NULL, false, true, current_user(), current_timestamp(), NULL, NULL UNION ALL 
        SELECT 111, 'mobile_number_without_country', NULL, 'Emptiness', NULL, false, true, current_user(), current_timestamp(), NULL, NULL UNION ALL 
        SELECT 112, 'mobile_number_without_country', NULL, NULL, 'Has Space', false, true, current_user(), current_timestamp(), NULL, NULL UNION ALL 
        SELECT 113, 'mobile_number_without_country', NULL, NULL, 'Is Fully Numeric', true, true, current_user(), current_timestamp(), NULL, NULL UNION ALL 
        SELECT 114, 'mobile_number_without_country', NULL, NULL, 'Is Empty or NULL', true, true, current_user(), current_timestamp(), NULL, NULL UNION ALL 
        SELECT 115, 'trade_type', 'InvalidKeyword', NULL, NULL, false, false, current_user(), current_timestamp(), NULL, NULL UNION ALL 
        SELECT 116, 'trade_type', 'FullyDuplicatedCharacter', NULL, NULL, false, true, current_user(), current_timestamp(), NULL, NULL UNION ALL 
        SELECT 117, 'trade_type', 'UnicodeCharacters', NULL, NULL, false, true, current_user(), current_timestamp(), NULL, NULL UNION ALL 
        SELECT 118, 'trade_type', NULL, 'Emptiness', NULL, false, true, current_user(), current_timestamp(), NULL, NULL UNION ALL 
        SELECT 119, 'trade_type', NULL, '100% Numeric', NULL, false, true, current_user(), current_timestamp(), NULL, NULL UNION ALL 
        SELECT 120, 'trade_type', NULL, 'SpecialCharacter-L1', NULL, true, true, current_user(), current_timestamp(), NULL, NULL UNION ALL 
        SELECT 121, 'trade_type', NULL, 'SpecialCharacter-L2', NULL, true, true, current_user(), current_timestamp(), NULL, NULL UNION ALL 
        SELECT 122, 'trade_type', NULL, NULL, 'Has Semicolon', true, true, current_user(), current_timestamp(), NULL, NULL UNION ALL 
        SELECT 123, 'trade_type', NULL, NULL, 'Has Space', true, true, current_user(), current_timestamp(), NULL, NULL UNION ALL 
        SELECT 124, 'address_general', 'DataType3', NULL, NULL, false, true, current_user(), current_timestamp(), NULL, NULL UNION ALL 
        SELECT 125, 'address_general', 'InvalidKeyword', NULL, NULL, false, true, current_user(), current_timestamp(), NULL, NULL UNION ALL 
        SELECT 126, 'address_general', 'FullyDuplicatedCharacter', NULL, NULL, false, true, current_user(), current_timestamp(), NULL, NULL UNION ALL 
        SELECT 127, 'address_general', 'UnicodeCharacters', NULL, NULL, false, true, current_user(), current_timestamp(), NULL, NULL UNION ALL 
        SELECT 128, 'address_general', 'SpecialCharacter', NULL, NULL, false, true, current_user(), current_timestamp(), NULL, NULL UNION ALL 
        SELECT 129, 'address_general', NULL, 'Emptiness', NULL, false, true, current_user(), current_timestamp(), NULL, NULL UNION ALL 
        SELECT 130, 'address_general', NULL, 'SpecialCharacter-L1', NULL, true, true, current_user(), current_timestamp(), NULL, NULL UNION ALL 
        SELECT 131, 'address_general', NULL, 'SpecialCharacter-L2', NULL, true, true, current_user(), current_timestamp(), NULL, NULL UNION ALL 
        SELECT 132, 'address_general', NULL, 'SpecialCharacter-L3', NULL, true, true, current_user(), current_timestamp(), NULL, NULL UNION ALL 
        SELECT 133, 'address_general', NULL, NULL, 'Has Exclamation Mark', false, true, current_user(), current_timestamp(), NULL, NULL UNION ALL 
        SELECT 134, 'address_general', NULL, NULL, 'Has Question Mark', false, true, current_user(), current_timestamp(), NULL, NULL UNION ALL 
        SELECT 135, 'address_general', NULL, NULL, 'Has Space', true, true, current_user(), current_timestamp(), NULL, NULL UNION ALL 
        SELECT 136, 'address_general', NULL, NULL, 'Is Fully Decimal', false, true, current_user(), current_timestamp(), NULL, NULL UNION ALL 
        SELECT 137, 'address_general', NULL, NULL, 'Is Fully Numeric', false, true, current_user(), current_timestamp(), NULL, NULL 
    ) WHERE _ID IS NOT NULL
) AS s ON t._ID = s._ID
WHEN MATCHED AND (
    t.FullFieldName                         <> s.FullFieldName OR
    COALESCE(t.PatternCategory,    '')      <> COALESCE(s.PatternCategory,    '') OR
    COALESCE(t.PatternSubCategory, '')      <> COALESCE(s.PatternSubCategory, '') OR
    COALESCE(t.PatternName,        '')      <> COALESCE(s.PatternName,        '') OR
    t.IsPatternAllowed                      <> s.IsPatternAllowed OR
    t.IsActive                              <> s.IsActive
) THEN UPDATE SET
    t.FullFieldName      = s.FullFieldName,
    t.PatternCategory    = s.PatternCategory,
    t.PatternSubCategory = s.PatternSubCategory,
    t.PatternName        = s.PatternName,
    t.IsPatternAllowed   = s.IsPatternAllowed,
    t.IsActive           = s.IsActive,
    t.LastUpdatedBy      = current_user(),
    t.LastUpdatedOn      = current_timestamp()
WHEN NOT MATCHED THEN INSERT (
    _ID, FullFieldName, PatternCategory, PatternSubCategory, PatternName,
    IsPatternAllowed, IsActive,
    CreatedBy, CreatedOn, LastUpdatedBy, LastUpdatedOn
) VALUES (
    s._ID, s.FullFieldName, s.PatternCategory, s.PatternSubCategory, s.PatternName,
    s.IsPatternAllowed, s.IsActive,
    s.CreatedBy, s.CreatedOn, s.LastUpdatedBy, s.LastUpdatedOn
)
""")

# COMMAND ----------

# DBTITLE 1,configFieldAllowedPattern -Data
display(spark.sql(f"SELECT * FROM `{MY_CATALOG}`.`{DQ_SCHEMA}`.`configFieldAllowedPattern`"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## configCustomQuery
# MAGIC
# MAGIC Expression rows use `\'` escaping (Databricks legacy SQL mode).
# MAGIC They are placed in `r"""..."""` so Python passes them verbatim to Spark SQL.
# MAGIC Only the outer MERGE wrapper uses `f"""..."""` for catalog/schema substitution.

# COMMAND ----------

# DBTITLE 1,Upsert configCustomQuery -SQL
_rows = r"""
SELECT NULL AS _ID, NULL AS FullFieldName, NULL AS CustomQueryType, NULL AS CustomQuery, NULL AS CustomQueryDescription, NULL AS IsConditionAllowed, NULL AS IsActive, NULL AS CreatedBy, NULL AS CreatedOn, NULL AS LastUpdatedBy, NULL AS LastUpdatedOn UNION ALL
SELECT 1, 'email_address', 'SQL', '(  (@InputValue LIKE \'%_@_%.__%\' AND LENGTH(SUBSTRING(@InputValue, LOCATE(\'@\', @InputValue) + 1, LENGTH(@InputValue))) - LENGTH(REPLACE(SUBSTRING(@InputValue, LOCATE(\'@\', @InputValue) + 1, LENGTH(@InputValue)), \'.\', \'\')) = 1)  OR (@InputValue LIKE \'%_@_%.__%.__%\' AND LENGTH(SUBSTRING(@InputValue, LOCATE(\'@\', @InputValue) + 1, LENGTH(@InputValue))) - LENGTH(REPLACE(SUBSTRING(@InputValue, LOCATE(\'@\', @InputValue) + 1, LENGTH(@InputValue)), \'.\', \'\')) = 2)  OR (@InputValue LIKE \'%_@_%.__%._%._%\' AND LENGTH(SUBSTRING(@InputValue, LOCATE(\'@\', @InputValue) + 1, LENGTH(@InputValue))) - LENGTH(REPLACE(SUBSTRING(@InputValue, LOCATE(\'@\', @InputValue) + 1, LENGTH(@InputValue)), \'.\', \'\')) > 2) )', 'Basic validations to ensure Pattern of valid email address.', true, true, current_user(), current_timestamp(), NULL, NULL UNION ALL
SELECT 2, 'email_address', 'SQL', '( LOCATE(\'@\', @InputValue) > 1 AND LOCATE(\'@\', @InputValue) < LENGTH(@InputValue) AND LOCATE(\'@\', REVERSE(@InputValue)) > LOCATE(\'.\', REVERSE(@InputValue)) AND (LENGTH(@InputValue) - LENGTH(REPLACE(@InputValue, \'@\', \'\'))) = 1 )', 'Additional validations: @ not first/last char, at least one dot after @, only one @ symbol', true, true, current_user(), current_timestamp(), NULL, NULL UNION ALL
SELECT 3, 'email_address', 'SQL', 'LENGTH(SUBSTRING(@InputValue, LOCATE(\'@\', @InputValue) + 1, LENGTH(@InputValue))) > 0 AND (LENGTH(SUBSTRING(@InputValue, LOCATE(\'@\', @InputValue) + 1, LENGTH(@InputValue)))  - LENGTH(REPLACE(SUBSTRING(@InputValue, LOCATE(\'@\', @InputValue) + 1, LENGTH(@InputValue)), \'.\', \'\'))  BETWEEN 1 AND 3 ) AND @InputValue NOT LIKE \'%.\' AND (LOCATE(\'..\', SUBSTRING(@InputValue, LOCATE(\'@\', @InputValue) + 1, LENGTH(@InputValue))) = 0   AND LENGTH(SUBSTRING(SUBSTRING(@InputValue, LOCATE(\'@\', @InputValue) + 1, LENGTH(@InputValue)), LOCATE(\'.\', SUBSTRING(@InputValue, LOCATE(\'@\', @InputValue) + 1, LENGTH(@InputValue))) + 1, LENGTH(SUBSTRING(@InputValue, LOCATE(\'@\', @InputValue) + 1, LENGTH(@InputValue))))) >= 2   AND (LOCATE(\'.\', SUBSTRING(SUBSTRING(@InputValue, LOCATE(\'@\', @InputValue) + 1, LENGTH(@InputValue)), LOCATE(\'.\', SUBSTRING(@InputValue, LOCATE(\'@\', @InputValue) + 1, LENGTH(@InputValue))) + 1, LENGTH(SUBSTRING(@InputValue, LOCATE(\'@\', @InputValue) + 1, LENGTH(@InputValue))))) = 0    OR LENGTH(SUBSTRING(SUBSTRING(@InputValue, LOCATE(\'@\', @InputValue) + 1, LENGTH(@InputValue)), LOCATE(\'.\', SUBSTRING(@InputValue, LOCATE(\'@\', @InputValue) + 1, LENGTH(@InputValue)), LOCATE(\'.\', SUBSTRING(@InputValue, LOCATE(\'@\', @InputValue) + 1, LENGTH(@InputValue))) + 1) + 1, LENGTH(SUBSTRING(@InputValue, LOCATE(\'@\', @InputValue) + 1, LENGTH(@InputValue))))) >= 2) )', 'Domain part: 1-3 dots, no consecutive dots, each label >= 1 char', true, true, current_user(), current_timestamp(), NULL, NULL UNION ALL
SELECT 4, 'email_address', 'SQL', 'LENGTH(SUBSTRING(@InputValue, 1, LOCATE(\'@\', @InputValue) - 1)) - LENGTH(REPLACE(SUBSTRING(@InputValue, 1, LOCATE(\'@\', @InputValue) - 1), \'.\', \'\')) < 3', 'Ensure local part does not contain more than 2 dots', true, true, current_user(), current_timestamp(), NULL, NULL UNION ALL
SELECT 5, 'email_address', 'SQL', '@InputValue LIKE \'%noemaildress%\'', 'Ensure email does not contain placeholder keyword noemaildress', false, true, current_user(), current_timestamp(), NULL, NULL UNION ALL
SELECT 6, 'email_address', 'SQL', '@InputValue RLIKE \'.*[-._]$\'', 'Ensure email does not end with special characters', false, true, current_user(), current_timestamp(), NULL, NULL UNION ALL
SELECT 7, 'email_address', 'REGEX', '^[a-zA-Z0-9._%+\\-]+@[a-zA-Z0-9]([a-zA-Z0-9\\-]{0,61}[a-zA-Z0-9])?(\\.[a-zA-Z0-9]([a-zA-Z0-9\\-]{0,61}[a-zA-Z0-9])?)*\\.[a-zA-Z]{2,}$', 'RFC-compliant email format: alphanumeric local part, domain labels max 63 chars, TLD min 2 chars', true, true, current_user(), current_timestamp(), NULL, NULL UNION ALL
SELECT 8, 'check_vendor_license_number', 'REGEX', '^[A-Z]{2,4}/[A-Z]{2}[0-9]{3}-[0-9]{3}$', 'Vendor code format: 2-4 uppercase prefix / 2 uppercase letters + 3 digits - 3 digits (e.g. ACME/WH001-003)', true, true, current_user(), current_timestamp(), NULL, NULL
"""

spark.sql(f"""
MERGE INTO `{MY_CATALOG}`.`{DQ_SCHEMA}`.`configCustomQuery` AS t
USING (
    SELECT * FROM (
""" + _rows + f"""
    ) WHERE _ID IS NOT NULL
) AS s ON t._ID = s._ID
WHEN MATCHED AND (
    t.CustomQuery           IS DISTINCT FROM s.CustomQuery OR
    t.CustomQueryType       IS DISTINCT FROM s.CustomQueryType OR
    t.CustomQueryDescription IS DISTINCT FROM s.CustomQueryDescription OR
    t.IsConditionAllowed    <> s.IsConditionAllowed OR
    t.IsActive              <> s.IsActive
) THEN UPDATE SET
    t.FullFieldName          = s.FullFieldName,
    t.CustomQuery            = s.CustomQuery,
    t.CustomQueryType        = s.CustomQueryType,
    t.CustomQueryDescription = s.CustomQueryDescription,
    t.IsConditionAllowed     = s.IsConditionAllowed,
    t.IsActive               = s.IsActive,
    t.LastUpdatedBy          = current_user(),
    t.LastUpdatedOn          = current_timestamp()
WHEN NOT MATCHED THEN INSERT (
    _ID, FullFieldName, CustomQueryType, CustomQuery, CustomQueryDescription,
    IsConditionAllowed, IsActive,
    CreatedBy, CreatedOn, LastUpdatedBy, LastUpdatedOn
) VALUES (
    s._ID, s.FullFieldName, s.CustomQueryType, s.CustomQuery, s.CustomQueryDescription,
    s.IsConditionAllowed, s.IsActive,
    s.CreatedBy, s.CreatedOn, s.LastUpdatedBy, s.LastUpdatedOn
)
""")

# COMMAND ----------

# DBTITLE 1,configCustomQuery -Data
display(spark.sql(f"SELECT * FROM `{MY_CATALOG}`.`{DQ_SCHEMA}`.`configCustomQuery`"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## mapDQChecks

# COMMAND ----------

# DBTITLE 1,Upsert mapDQChecks -SQL
spark.sql(f"""
MERGE INTO `{MY_CATALOG}`.`{DQ_SCHEMA}`.`mapDQChecks` AS t
USING (
    SELECT * FROM (
        SELECT NULL AS _ID, NULL AS FullFieldName, NULL AS TargetCatalogName, NULL AS TargetSchemaName, NULL AS TargetTableName, NULL AS TargetFieldName, NULL AS DQFunctionSchemaName, NULL AS DQFunctionName, NULL AS IsActive, NULL AS CreatedBy, NULL AS CreatedOn, NULL AS LastUpdatedBy, NULL AS LastUpdatedOn UNION ALL
        SELECT 1,  'email_address',                 NULL, '{CURATED_SCHEMA}', 'mock_curated_contacts',  'email_address',    '{DQ_SCHEMA}', 'fn_DQ_email_address',                 true, current_user(), current_timestamp(), NULL, NULL UNION ALL
        SELECT 2,  'postal_code_all_country',       NULL, '{CURATED_SCHEMA}', 'mock_curated_locations', 'postal_code',      '{DQ_SCHEMA}', 'fn_DQ_postal_code_all_country',       true, current_user(), current_timestamp(), NULL, NULL UNION ALL
        SELECT 3,  'trade_type',                    NULL, '{CURATED_SCHEMA}', 'mock_curated_vendors',   'trade_type',       '{DQ_SCHEMA}', 'fn_DQ_trade_type',                    true, current_user(), current_timestamp(), NULL, NULL UNION ALL
        SELECT 4,  'state',                         NULL, '{CURATED_SCHEMA}', 'mock_curated_locations', 'state',            '{DQ_SCHEMA}', 'fn_DQ_state',                         true, current_user(), current_timestamp(), NULL, NULL UNION ALL
        SELECT 5,  'city',                          NULL, '{CURATED_SCHEMA}', 'mock_curated_locations', 'city',             '{DQ_SCHEMA}', 'fn_DQ_city',                          true, current_user(), current_timestamp(), NULL, NULL UNION ALL
        SELECT 6,  'address_line_1',                NULL, '{CURATED_SCHEMA}', 'mock_curated_locations', 'address1',         '{DQ_SCHEMA}', 'fn_DQ_address_line_1',                true, current_user(), current_timestamp(), NULL, NULL UNION ALL
        SELECT 7,  'address_line_1',                NULL, '{CURATED_SCHEMA}', 'mock_curated_locations', 'address2',         '{DQ_SCHEMA}', 'fn_DQ_address_line_1',                true, current_user(), current_timestamp(), NULL, NULL UNION ALL
        SELECT 8,  'address_general',               NULL, '{CURATED_SCHEMA}', 'mock_curated_locations', 'address4',         '{DQ_SCHEMA}', 'fn_DQ_address_general',               true, current_user(), current_timestamp(), NULL, NULL UNION ALL
        SELECT 9,  'country',                       NULL, '{CURATED_SCHEMA}', 'mock_curated_locations', 'country',          '{DQ_SCHEMA}', 'fn_DQ_country',                       true, current_user(), current_timestamp(), NULL, NULL UNION ALL
        SELECT 10, 'address_line_2',                NULL, '{CURATED_SCHEMA}', 'mock_curated_locations', 'address3',         '{DQ_SCHEMA}', 'fn_DQ_address_line_2',                true, current_user(), current_timestamp(), NULL, NULL UNION ALL
        SELECT 11, 'mobile_number_without_country', NULL, '{CURATED_SCHEMA}', 'mock_curated_contacts',  'raw_phone_number', '{DQ_SCHEMA}', 'fn_DQ_mobile_number_without_country', true, current_user(), current_timestamp(), NULL, NULL UNION ALL
        SELECT 12, 'name_general',                  NULL, '{CURATED_SCHEMA}', 'mock_curated_vendors',   'last_name',        '{DQ_SCHEMA}', 'fn_DQ_name_general',                  true, current_user(), current_timestamp(), NULL, NULL UNION ALL
        SELECT 13, 'vendor_name',                   NULL, '{CURATED_SCHEMA}', 'mock_curated_vendors',   'vendor_name',      '{DQ_SCHEMA}', 'fn_DQ_vendor_name',                   true, current_user(), current_timestamp(), NULL, NULL UNION ALL
        SELECT 14, 'first_name',                    NULL, '{CURATED_SCHEMA}', 'mock_curated_vendors',   'first_name',       '{DQ_SCHEMA}', 'fn_DQ_first_name',                    true, current_user(), current_timestamp(), NULL, NULL UNION ALL
        SELECT 15, 'phone_number',                  NULL, '{CURATED_SCHEMA}', 'mock_curated_contacts',  'phone_number',     '{DQ_SCHEMA}', 'fn_DQ_phone_number',                  true, current_user(), current_timestamp(), NULL, NULL UNION ALL
        SELECT 16, 'check_vendor_license_number',   NULL, '{CURATED_SCHEMA}', 'mock_curated_vendors',   'vendor_code',      '{DQ_SCHEMA}', 'fn_DQ_check_vendor_license_number',   true, current_user(), current_timestamp(), NULL, NULL
    ) WHERE _ID IS NOT NULL
) AS s ON t._ID = s._ID
WHEN MATCHED AND (
    t.TargetSchemaName <> s.TargetSchemaName OR
    t.TargetTableName  <> s.TargetTableName  OR
    t.TargetFieldName  <> s.TargetFieldName  OR
    t.IsActive         <> s.IsActive
) THEN UPDATE SET
    t.TargetCatalogName    = s.TargetCatalogName,
    t.TargetSchemaName     = s.TargetSchemaName,
    t.TargetTableName      = s.TargetTableName,
    t.TargetFieldName      = s.TargetFieldName,
    t.DQFunctionSchemaName = s.DQFunctionSchemaName,
    t.DQFunctionName       = s.DQFunctionName,
    t.IsActive             = s.IsActive,
    t.LastUpdatedBy        = current_user(),
    t.LastUpdatedOn        = current_timestamp()
WHEN NOT MATCHED THEN INSERT (
    _ID, FullFieldName, TargetCatalogName, TargetSchemaName, TargetTableName,
    TargetFieldName, DQFunctionSchemaName, DQFunctionName,
    IsActive, CreatedBy, CreatedOn, LastUpdatedBy, LastUpdatedOn
) VALUES (
    s._ID, s.FullFieldName, s.TargetCatalogName, s.TargetSchemaName, s.TargetTableName,
    s.TargetFieldName, s.DQFunctionSchemaName, s.DQFunctionName,
    s.IsActive, s.CreatedBy, s.CreatedOn, s.LastUpdatedBy, s.LastUpdatedOn
)
""")

# COMMAND ----------

# DBTITLE 1,mapDQChecks -Data
display(spark.sql(f"SELECT * FROM `{MY_CATALOG}`.`{DQ_SCHEMA}`.`mapDQChecks`"))

# COMMAND ----------

# DBTITLE 1,Config Summary
dq.config.show_config_summary()
