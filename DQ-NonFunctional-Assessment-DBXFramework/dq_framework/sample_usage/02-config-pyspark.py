# Databricks notebook source
# MAGIC %md
# MAGIC # 02 — Config Seed: PySpark Methods
# MAGIC Seeds all 5 config tables using the `ConfigManager` Python API.
# MAGIC Use this notebook **OR** `02-config-sql.py` — not both.
# MAGIC Requires `01-install.py` to have been run in this session first.

# COMMAND ----------

# DBTITLE 1,Load Install (variables + dq instance)
# MAGIC %run ./01-install

# COMMAND ----------

# MAGIC %md
# MAGIC ## masterField

# COMMAND ----------

# DBTITLE 1,Upsert masterField -PySpark
(dq.config
    .register_field(field_id=1,  full_field_name='postal_code_all_country',       data_category_type_id=10, is_active=True, created_by=None, last_updated_by=None)
    .register_field(field_id=2,  full_field_name='province',                       data_category_type_id=3,  is_active=True, created_by=None, last_updated_by=None)
    .register_field(field_id=3,  full_field_name='state',                          data_category_type_id=3,  is_active=True, created_by=None, last_updated_by=None)
    .register_field(field_id=4,  full_field_name='city',                           data_category_type_id=3,  is_active=True, created_by=None, last_updated_by=None)
    .register_field(field_id=5,  full_field_name='address_line_1',                 data_category_type_id=2,  is_active=True, created_by=None, last_updated_by=None)
    .register_field(field_id=6,  full_field_name='address_line_2',                 data_category_type_id=2,  is_active=True, created_by=None, last_updated_by=None)
    .register_field(field_id=7,  full_field_name='address_general',                data_category_type_id=2,  is_active=True, created_by=None, last_updated_by=None)
    .register_field(field_id=8,  full_field_name='address_short',                  data_category_type_id=3,  is_active=True, created_by=None, last_updated_by=None)
    .register_field(field_id=9,  full_field_name='country',                        data_category_type_id=3,  is_active=True, created_by=None, last_updated_by=None)
    .register_field(field_id=10, full_field_name='county',                         data_category_type_id=10, is_active=True, created_by=None, last_updated_by=None)
    .register_field(field_id=11, full_field_name='email_address',                  data_category_type_id=4,  is_active=True, created_by=None, last_updated_by=None)
    .register_field(field_id=12, full_field_name='phone_number',                   data_category_type_id=12, is_active=True, created_by=None, last_updated_by=None)
    .register_field(field_id=13, full_field_name='mobile_number_without_country',  data_category_type_id=14, is_active=True, created_by=None, last_updated_by=None)
    .register_field(field_id=14, full_field_name='vendor_name',                    data_category_type_id=2,  is_active=True, created_by=None, last_updated_by=None)
    .register_field(field_id=15, full_field_name='first_name',                     data_category_type_id=2,  is_active=True, created_by=None, last_updated_by=None)
    .register_field(field_id=16, full_field_name='name_general',                   data_category_type_id=2,  is_active=True, created_by='sys', last_updated_by=None)
    .register_field(field_id=17, full_field_name='trade_type',                     data_category_type_id=1,  is_active=True, created_by='sys', last_updated_by=None)
    .register_field(field_id=18, full_field_name='check_vendor_license_number',    data_category_type_id=11, is_active=True, created_by='sys', last_updated_by=None)
)

# COMMAND ----------

# DBTITLE 1,masterField -Data
display(spark.sql(f"SELECT * FROM `{MY_CATALOG}`.`{DQ_SCHEMA}`.`masterField`"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## configFieldValues

# COMMAND ----------

# DBTITLE 1,Upsert configFieldValues -PySpark
(dq.config
    .set_field_values(config_id=1,  full_field_name='postal_code_all_country',      field_id=1,  min_data_length=5,  max_data_length=10,  min_data_value='00001',        max_data_value='ZZZ 9ZZ',               is_active=True, created_by=None, last_updated_by=None)
    .set_field_values(config_id=2,  full_field_name='province',                      field_id=2,  min_data_length=4,  max_data_length=20,  min_data_value='aaaa',         max_data_value='ZZZZZZZZZZZZZZZZZZZZ', is_active=True, created_by=None, last_updated_by=None)
    .set_field_values(config_id=3,  full_field_name='state',                         field_id=3,  min_data_length=3,  max_data_length=20,  min_data_value='aaaa',         max_data_value='ZZZZZZZZZZZZZZZZZZZZ', is_active=True, created_by=None, last_updated_by=None)
    .set_field_values(config_id=4,  full_field_name='city',                          field_id=4,  min_data_length=3,  max_data_length=20,  min_data_value=None,           max_data_value=None,                   is_active=True, created_by=None, last_updated_by=None)
    .set_field_values(config_id=5,  full_field_name='address_line_1',                field_id=5,  min_data_length=5,  max_data_length=100, min_data_value=None,           max_data_value=None,                   is_active=True, created_by=None, last_updated_by=None)
    .set_field_values(config_id=6,  full_field_name='address_line_2',                field_id=6,  min_data_length=5,  max_data_length=100, min_data_value=None,           max_data_value=None,                   is_active=True, created_by=None, last_updated_by=None)
    .set_field_values(config_id=7,  full_field_name='address_general',               field_id=7,  min_data_length=5,  max_data_length=100, min_data_value=None,           max_data_value=None,                   is_active=True, created_by=None, last_updated_by=None)
    .set_field_values(config_id=8,  full_field_name='address_short',                 field_id=8,  min_data_length=5,  max_data_length=15,  min_data_value=None,           max_data_value=None,                   is_active=True, created_by=None, last_updated_by=None)
    .set_field_values(config_id=9,  full_field_name='country',                       field_id=9,  min_data_length=2,  max_data_length=2,   min_data_value=None,           max_data_value=None,                   is_active=True, created_by=None, last_updated_by=None)
    .set_field_values(config_id=10, full_field_name='county',                        field_id=10, min_data_length=3,  max_data_length=20,  min_data_value=None,           max_data_value=None,                   is_active=True, created_by=None, last_updated_by=None)
    .set_field_values(config_id=11, full_field_name='email_address',                 field_id=11, min_data_length=6,  max_data_length=255, min_data_value=None,           max_data_value=None,                   is_active=True, created_by=None, last_updated_by=None)
    .set_field_values(config_id=12, full_field_name='phone_number',                  field_id=12, min_data_length=6,  max_data_length=16,  min_data_value='100000',       max_data_value='999999999999999',       is_active=True, created_by=None, last_updated_by=None)
    .set_field_values(config_id=13, full_field_name='mobile_number_without_country', field_id=13, min_data_length=6,  max_data_length=16,  min_data_value='60000000000',  max_data_value='9999999999',            is_active=True, created_by=None, last_updated_by=None)
    .set_field_values(config_id=14, full_field_name='vendor_name',                   field_id=14, min_data_length=5,  max_data_length=50,  min_data_value=None,           max_data_value=None,                   is_active=True, created_by=None, last_updated_by=None)
    .set_field_values(config_id=15, full_field_name='first_name',                    field_id=15, min_data_length=2,  max_data_length=20,  min_data_value=None,           max_data_value=None,                   is_active=True, created_by=None, last_updated_by=None)
    .set_field_values(config_id=16, full_field_name='name_general',                  field_id=16, min_data_length=2,  max_data_length=20,  min_data_value=None,           max_data_value=None,                   is_active=True, created_by=None, last_updated_by=None)
    .set_field_values(config_id=17, full_field_name='trade_type',                    field_id=17, min_data_length=5,  max_data_length=10,  min_data_value=None,           max_data_value=None,                   is_active=True, created_by=None, last_updated_by=None)
)

# COMMAND ----------

# DBTITLE 1,configFieldValues -Data
display(spark.sql(f"SELECT * FROM `{MY_CATALOG}`.`{DQ_SCHEMA}`.`configFieldValues`"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## configFieldAllowedPattern

# COMMAND ----------

# DBTITLE 1,Upsert configFieldAllowedPattern -PySpark
(dq.config
    .add_pattern_rule(rule_id=1, full_field_name='email_address', is_pattern_allowed=False, pattern_category=None, pattern_subcategory=None, pattern_name='Is Fully Numeric', is_active=True, created_by=None, last_updated_by=None)
    .add_pattern_rule(rule_id=2, full_field_name='email_address', is_pattern_allowed=False, pattern_category=None, pattern_subcategory=None, pattern_name='Is Fully Decimal', is_active=True, created_by=None, last_updated_by=None)
    .add_pattern_rule(rule_id=3, full_field_name='email_address', is_pattern_allowed=False, pattern_category='DataType3', pattern_subcategory=None, pattern_name=None, is_active=True, created_by=None, last_updated_by=None)
    .add_pattern_rule(rule_id=4, full_field_name='email_address', is_pattern_allowed=False, pattern_category='SpecialCharacter', pattern_subcategory=None, pattern_name=None, is_active=True, created_by=None, last_updated_by=None)
    .add_pattern_rule(rule_id=5, full_field_name='email_address', is_pattern_allowed=True, pattern_category=None, pattern_subcategory=None, pattern_name='Has Full Stop', is_active=True, created_by=None, last_updated_by=None)
    .add_pattern_rule(rule_id=6, full_field_name='email_address', is_pattern_allowed=True, pattern_category=None, pattern_subcategory=None, pattern_name='Has Hyphen', is_active=True, created_by=None, last_updated_by=None)
    .add_pattern_rule(rule_id=7, full_field_name='email_address', is_pattern_allowed=True, pattern_category=None, pattern_subcategory=None, pattern_name='Has Underscore', is_active=True, created_by=None, last_updated_by=None)
    .add_pattern_rule(rule_id=8, full_field_name='email_address', is_pattern_allowed=True, pattern_category=None, pattern_subcategory=None, pattern_name='Has At Sign', is_active=True, created_by=None, last_updated_by=None)
    .add_pattern_rule(rule_id=9, full_field_name='email_address', is_pattern_allowed=False, pattern_category=None, pattern_subcategory='Emptiness', pattern_name=None, is_active=True, created_by=None, last_updated_by=None)
    .add_pattern_rule(rule_id=10, full_field_name='email_address', is_pattern_allowed=False, pattern_category='InvalidKeyword', pattern_subcategory=None, pattern_name=None, is_active=True, created_by=None, last_updated_by=None)
    .add_pattern_rule(rule_id=11, full_field_name='email_address', is_pattern_allowed=False, pattern_category='FullyDuplicatedCharacter', pattern_subcategory=None, pattern_name=None, is_active=True, created_by=None, last_updated_by=None)
    .add_pattern_rule(rule_id=12, full_field_name='email_address', is_pattern_allowed=False, pattern_category='UnicodeCharacters', pattern_subcategory=None, pattern_name=None, is_active=True, created_by=None, last_updated_by=None)
    .add_pattern_rule(rule_id=13, full_field_name='email_address', is_pattern_allowed=True, pattern_category=None, pattern_subcategory=None, pattern_name='Is Empty or NULL', is_active=True, created_by=None, last_updated_by=None)
    .add_pattern_rule(rule_id=14, full_field_name='email_address', is_pattern_allowed=False, pattern_category=None, pattern_subcategory=None, pattern_name='Has Space', is_active=True, created_by=None, last_updated_by=None)
    .add_pattern_rule(rule_id=15, full_field_name='postal_code_all_country', is_pattern_allowed=False, pattern_category='DataType3', pattern_subcategory=None, pattern_name=None, is_active=True, created_by=None, last_updated_by=None)
    .add_pattern_rule(rule_id=16, full_field_name='postal_code_all_country', is_pattern_allowed=False, pattern_category='InvalidKeyword', pattern_subcategory=None, pattern_name=None, is_active=True, created_by=None, last_updated_by=None)
    .add_pattern_rule(rule_id=17, full_field_name='postal_code_all_country', is_pattern_allowed=False, pattern_category='FullyDuplicatedCharacter', pattern_subcategory=None, pattern_name=None, is_active=True, created_by=None, last_updated_by=None)
    .add_pattern_rule(rule_id=18, full_field_name='postal_code_all_country', is_pattern_allowed=False, pattern_category='UnicodeCharacters', pattern_subcategory=None, pattern_name=None, is_active=True, created_by=None, last_updated_by=None)
    .add_pattern_rule(rule_id=19, full_field_name='postal_code_all_country', is_pattern_allowed=False, pattern_category='SpecialCharacter', pattern_subcategory=None, pattern_name=None, is_active=True, created_by=None, last_updated_by=None)
    .add_pattern_rule(rule_id=20, full_field_name='postal_code_all_country', is_pattern_allowed=False, pattern_category=None, pattern_subcategory='Emptiness', pattern_name=None, is_active=True, created_by=None, last_updated_by=None)
    .add_pattern_rule(rule_id=21, full_field_name='postal_code_all_country', is_pattern_allowed=True, pattern_category=None, pattern_subcategory=None, pattern_name='Has Space', is_active=True, created_by=None, last_updated_by=None)
    .add_pattern_rule(rule_id=22, full_field_name='postal_code_all_country', is_pattern_allowed=False, pattern_category=None, pattern_subcategory=None, pattern_name='Is Fully Decimal', is_active=True, created_by=None, last_updated_by=None)
    .add_pattern_rule(rule_id=23, full_field_name='postal_code_all_country', is_pattern_allowed=False, pattern_category=None, pattern_subcategory=None, pattern_name='Has Lowercase Character', is_active=True, created_by=None, last_updated_by=None)
    .add_pattern_rule(rule_id=24, full_field_name='state', is_pattern_allowed=False, pattern_category='DataType1', pattern_subcategory=None, pattern_name=None, is_active=True, created_by=None, last_updated_by=None)
    .add_pattern_rule(rule_id=25, full_field_name='state', is_pattern_allowed=False, pattern_category='DataType2', pattern_subcategory=None, pattern_name=None, is_active=True, created_by=None, last_updated_by=None)
    .add_pattern_rule(rule_id=26, full_field_name='state', is_pattern_allowed=False, pattern_category='DataType3', pattern_subcategory=None, pattern_name=None, is_active=True, created_by=None, last_updated_by=None)
    .add_pattern_rule(rule_id=27, full_field_name='state', is_pattern_allowed=True, pattern_category=None, pattern_subcategory=None, pattern_name='Is Fully Text', is_active=True, created_by=None, last_updated_by=None)
    .add_pattern_rule(rule_id=28, full_field_name='state', is_pattern_allowed=False, pattern_category='InvalidKeyword', pattern_subcategory=None, pattern_name=None, is_active=True, created_by=None, last_updated_by=None)
    .add_pattern_rule(rule_id=29, full_field_name='state', is_pattern_allowed=False, pattern_category='FullyDuplicatedCharacter', pattern_subcategory=None, pattern_name=None, is_active=True, created_by=None, last_updated_by=None)
    .add_pattern_rule(rule_id=30, full_field_name='state', is_pattern_allowed=False, pattern_category='UnicodeCharacters', pattern_subcategory=None, pattern_name=None, is_active=True, created_by=None, last_updated_by=None)
    .add_pattern_rule(rule_id=31, full_field_name='state', is_pattern_allowed=False, pattern_category='SpecialCharacter', pattern_subcategory=None, pattern_name=None, is_active=True, created_by=None, last_updated_by=None)
    .add_pattern_rule(rule_id=32, full_field_name='state', is_pattern_allowed=False, pattern_category=None, pattern_subcategory='Emptiness', pattern_name=None, is_active=True, created_by=None, last_updated_by=None)
    .add_pattern_rule(rule_id=33, full_field_name='state', is_pattern_allowed=True, pattern_category=None, pattern_subcategory=None, pattern_name='Has Space', is_active=True, created_by=None, last_updated_by=None)
    .add_pattern_rule(rule_id=34, full_field_name='city', is_pattern_allowed=False, pattern_category='DataType1', pattern_subcategory=None, pattern_name=None, is_active=True, created_by=None, last_updated_by=None)
    .add_pattern_rule(rule_id=35, full_field_name='city', is_pattern_allowed=False, pattern_category='DataType2', pattern_subcategory=None, pattern_name=None, is_active=True, created_by=None, last_updated_by=None)
    .add_pattern_rule(rule_id=36, full_field_name='city', is_pattern_allowed=False, pattern_category='DataType3', pattern_subcategory=None, pattern_name=None, is_active=True, created_by=None, last_updated_by=None)
    .add_pattern_rule(rule_id=37, full_field_name='city', is_pattern_allowed=True, pattern_category=None, pattern_subcategory=None, pattern_name='Is Fully Text', is_active=True, created_by=None, last_updated_by=None)
    .add_pattern_rule(rule_id=38, full_field_name='city', is_pattern_allowed=False, pattern_category='InvalidKeyword', pattern_subcategory=None, pattern_name=None, is_active=True, created_by=None, last_updated_by=None)
    .add_pattern_rule(rule_id=39, full_field_name='city', is_pattern_allowed=False, pattern_category='FullyDuplicatedCharacter', pattern_subcategory=None, pattern_name=None, is_active=True, created_by=None, last_updated_by=None)
    .add_pattern_rule(rule_id=40, full_field_name='city', is_pattern_allowed=False, pattern_category='UnicodeCharacters', pattern_subcategory=None, pattern_name=None, is_active=True, created_by=None, last_updated_by=None)
    .add_pattern_rule(rule_id=41, full_field_name='city', is_pattern_allowed=False, pattern_category='SpecialCharacter', pattern_subcategory=None, pattern_name=None, is_active=True, created_by=None, last_updated_by=None)
    .add_pattern_rule(rule_id=42, full_field_name='city', is_pattern_allowed=False, pattern_category=None, pattern_subcategory='Emptiness', pattern_name=None, is_active=True, created_by=None, last_updated_by=None)
    .add_pattern_rule(rule_id=43, full_field_name='city', is_pattern_allowed=True, pattern_category=None, pattern_subcategory=None, pattern_name='Has Space', is_active=True, created_by=None, last_updated_by=None)
    .add_pattern_rule(rule_id=44, full_field_name='address_line_1', is_pattern_allowed=False, pattern_category='DataType3', pattern_subcategory=None, pattern_name=None, is_active=True, created_by=None, last_updated_by=None)
    .add_pattern_rule(rule_id=45, full_field_name='address_line_1', is_pattern_allowed=False, pattern_category='InvalidKeyword', pattern_subcategory=None, pattern_name=None, is_active=True, created_by=None, last_updated_by=None)
    .add_pattern_rule(rule_id=46, full_field_name='address_line_1', is_pattern_allowed=False, pattern_category='FullyDuplicatedCharacter', pattern_subcategory=None, pattern_name=None, is_active=True, created_by=None, last_updated_by=None)
    .add_pattern_rule(rule_id=47, full_field_name='address_line_1', is_pattern_allowed=False, pattern_category='UnicodeCharacters', pattern_subcategory=None, pattern_name=None, is_active=True, created_by=None, last_updated_by=None)
    .add_pattern_rule(rule_id=48, full_field_name='address_line_1', is_pattern_allowed=False, pattern_category='SpecialCharacter', pattern_subcategory=None, pattern_name=None, is_active=True, created_by=None, last_updated_by=None)
    .add_pattern_rule(rule_id=49, full_field_name='address_line_1', is_pattern_allowed=False, pattern_category=None, pattern_subcategory='Emptiness', pattern_name=None, is_active=True, created_by=None, last_updated_by=None)
    .add_pattern_rule(rule_id=50, full_field_name='address_line_1', is_pattern_allowed=True, pattern_category=None, pattern_subcategory='SpecialCharacter-L1', pattern_name=None, is_active=True, created_by=None, last_updated_by=None)
    .add_pattern_rule(rule_id=51, full_field_name='address_line_1', is_pattern_allowed=True, pattern_category=None, pattern_subcategory='SpecialCharacter-L2', pattern_name=None, is_active=True, created_by=None, last_updated_by=None)
    .add_pattern_rule(rule_id=52, full_field_name='address_line_1', is_pattern_allowed=True, pattern_category=None, pattern_subcategory='SpecialCharacter-L3', pattern_name=None, is_active=True, created_by=None, last_updated_by=None)
    .add_pattern_rule(rule_id=53, full_field_name='address_line_1', is_pattern_allowed=False, pattern_category=None, pattern_subcategory=None, pattern_name='Has Exclamation Mark', is_active=True, created_by=None, last_updated_by=None)
    .add_pattern_rule(rule_id=54, full_field_name='address_line_1', is_pattern_allowed=False, pattern_category=None, pattern_subcategory=None, pattern_name='Has Question Mark', is_active=True, created_by=None, last_updated_by=None)
    .add_pattern_rule(rule_id=55, full_field_name='address_line_1', is_pattern_allowed=True, pattern_category=None, pattern_subcategory=None, pattern_name='Has Space', is_active=True, created_by=None, last_updated_by=None)
    .add_pattern_rule(rule_id=56, full_field_name='address_line_1', is_pattern_allowed=False, pattern_category=None, pattern_subcategory=None, pattern_name='Is Fully Decimal', is_active=True, created_by=None, last_updated_by=None)
    .add_pattern_rule(rule_id=57, full_field_name='address_line_1', is_pattern_allowed=False, pattern_category=None, pattern_subcategory=None, pattern_name='Is Fully Numeric', is_active=True, created_by=None, last_updated_by=None)
    .add_pattern_rule(rule_id=58, full_field_name='country', is_pattern_allowed=False, pattern_category='DataType1', pattern_subcategory=None, pattern_name=None, is_active=True, created_by=None, last_updated_by=None)
    .add_pattern_rule(rule_id=59, full_field_name='country', is_pattern_allowed=False, pattern_category='DataType2', pattern_subcategory=None, pattern_name=None, is_active=True, created_by=None, last_updated_by=None)
    .add_pattern_rule(rule_id=60, full_field_name='country', is_pattern_allowed=False, pattern_category='DataType3', pattern_subcategory=None, pattern_name=None, is_active=True, created_by=None, last_updated_by=None)
    .add_pattern_rule(rule_id=61, full_field_name='country', is_pattern_allowed=True, pattern_category=None, pattern_subcategory=None, pattern_name='Is Fully Text', is_active=True, created_by=None, last_updated_by=None)
    .add_pattern_rule(rule_id=62, full_field_name='country', is_pattern_allowed=False, pattern_category='InvalidKeyword', pattern_subcategory=None, pattern_name=None, is_active=True, created_by=None, last_updated_by=None)
    .add_pattern_rule(rule_id=63, full_field_name='country', is_pattern_allowed=False, pattern_category='FullyDuplicatedCharacter', pattern_subcategory=None, pattern_name=None, is_active=True, created_by=None, last_updated_by=None)
    .add_pattern_rule(rule_id=64, full_field_name='country', is_pattern_allowed=False, pattern_category='UnicodeCharacters', pattern_subcategory=None, pattern_name=None, is_active=True, created_by=None, last_updated_by=None)
    .add_pattern_rule(rule_id=65, full_field_name='country', is_pattern_allowed=False, pattern_category='SpecialCharacter', pattern_subcategory=None, pattern_name=None, is_active=True, created_by=None, last_updated_by=None)
    .add_pattern_rule(rule_id=66, full_field_name='country', is_pattern_allowed=False, pattern_category=None, pattern_subcategory='Emptiness', pattern_name=None, is_active=True, created_by=None, last_updated_by=None)
    .add_pattern_rule(rule_id=67, full_field_name='country', is_pattern_allowed=False, pattern_category=None, pattern_subcategory=None, pattern_name='Has Space', is_active=True, created_by=None, last_updated_by=None)
    .add_pattern_rule(rule_id=68, full_field_name='country', is_pattern_allowed=False, pattern_category=None, pattern_subcategory=None, pattern_name='Has Lowercase Character', is_active=True, created_by=None, last_updated_by=None)
    .add_pattern_rule(rule_id=69, full_field_name='vendor_name', is_pattern_allowed=False, pattern_category='DataType3', pattern_subcategory=None, pattern_name=None, is_active=True, created_by=None, last_updated_by=None)
    .add_pattern_rule(rule_id=70, full_field_name='vendor_name', is_pattern_allowed=False, pattern_category='InvalidKeyword', pattern_subcategory=None, pattern_name=None, is_active=True, created_by=None, last_updated_by=None)
    .add_pattern_rule(rule_id=71, full_field_name='vendor_name', is_pattern_allowed=False, pattern_category='FullyDuplicatedCharacter', pattern_subcategory=None, pattern_name=None, is_active=True, created_by=None, last_updated_by=None)
    .add_pattern_rule(rule_id=72, full_field_name='vendor_name', is_pattern_allowed=False, pattern_category='UnicodeCharacters', pattern_subcategory=None, pattern_name=None, is_active=True, created_by=None, last_updated_by=None)
    .add_pattern_rule(rule_id=73, full_field_name='vendor_name', is_pattern_allowed=False, pattern_category='SpecialCharacter', pattern_subcategory=None, pattern_name=None, is_active=True, created_by=None, last_updated_by=None)
    .add_pattern_rule(rule_id=74, full_field_name='vendor_name', is_pattern_allowed=False, pattern_category=None, pattern_subcategory='Emptiness', pattern_name=None, is_active=True, created_by=None, last_updated_by=None)
    .add_pattern_rule(rule_id=75, full_field_name='vendor_name', is_pattern_allowed=True, pattern_category=None, pattern_subcategory=None, pattern_name='Has Space', is_active=True, created_by=None, last_updated_by=None)
    .add_pattern_rule(rule_id=76, full_field_name='vendor_name', is_pattern_allowed=False, pattern_category=None, pattern_subcategory=None, pattern_name='Is Fully Decimal', is_active=True, created_by=None, last_updated_by=None)
    .add_pattern_rule(rule_id=77, full_field_name='vendor_name', is_pattern_allowed=False, pattern_category=None, pattern_subcategory=None, pattern_name='Is Fully Numeric', is_active=True, created_by=None, last_updated_by=None)
    .add_pattern_rule(rule_id=78, full_field_name='vendor_name', is_pattern_allowed=False, pattern_category=None, pattern_subcategory=None, pattern_name='Has Lowercase Character', is_active=True, created_by=None, last_updated_by=None)
    .add_pattern_rule(rule_id=79, full_field_name='first_name', is_pattern_allowed=False, pattern_category='DataType2', pattern_subcategory=None, pattern_name=None, is_active=True, created_by=None, last_updated_by=None)
    .add_pattern_rule(rule_id=80, full_field_name='first_name', is_pattern_allowed=False, pattern_category='DataType3', pattern_subcategory=None, pattern_name=None, is_active=True, created_by=None, last_updated_by=None)
    .add_pattern_rule(rule_id=81, full_field_name='first_name', is_pattern_allowed=False, pattern_category='InvalidKeyword', pattern_subcategory=None, pattern_name=None, is_active=True, created_by=None, last_updated_by=None)
    .add_pattern_rule(rule_id=82, full_field_name='first_name', is_pattern_allowed=False, pattern_category='FullyDuplicatedCharacter', pattern_subcategory=None, pattern_name=None, is_active=True, created_by=None, last_updated_by=None)
    .add_pattern_rule(rule_id=83, full_field_name='first_name', is_pattern_allowed=False, pattern_category='UnicodeCharacters', pattern_subcategory=None, pattern_name=None, is_active=True, created_by=None, last_updated_by=None)
    .add_pattern_rule(rule_id=84, full_field_name='first_name', is_pattern_allowed=False, pattern_category='SpecialCharacter', pattern_subcategory=None, pattern_name=None, is_active=True, created_by=None, last_updated_by=None)
    .add_pattern_rule(rule_id=85, full_field_name='first_name', is_pattern_allowed=False, pattern_category=None, pattern_subcategory='Emptiness', pattern_name=None, is_active=True, created_by=None, last_updated_by=None)
    .add_pattern_rule(rule_id=86, full_field_name='first_name', is_pattern_allowed=True, pattern_category=None, pattern_subcategory=None, pattern_name='Has Space', is_active=True, created_by=None, last_updated_by=None)
    .add_pattern_rule(rule_id=87, full_field_name='first_name', is_pattern_allowed=False, pattern_category=None, pattern_subcategory=None, pattern_name='Is Fully Decimal', is_active=True, created_by=None, last_updated_by=None)
    .add_pattern_rule(rule_id=88, full_field_name='first_name', is_pattern_allowed=False, pattern_category=None, pattern_subcategory=None, pattern_name='Is Fully Numeric', is_active=True, created_by=None, last_updated_by=None)
    .add_pattern_rule(rule_id=89, full_field_name='first_name', is_pattern_allowed=False, pattern_category='DataType1', pattern_subcategory=None, pattern_name=None, is_active=True, created_by=None, last_updated_by=None)
    .add_pattern_rule(rule_id=90, full_field_name='name_general', is_pattern_allowed=False, pattern_category='DataType2', pattern_subcategory=None, pattern_name=None, is_active=True, created_by=None, last_updated_by=None)
    .add_pattern_rule(rule_id=91, full_field_name='name_general', is_pattern_allowed=False, pattern_category='DataType3', pattern_subcategory=None, pattern_name=None, is_active=True, created_by=None, last_updated_by=None)
    .add_pattern_rule(rule_id=92, full_field_name='name_general', is_pattern_allowed=False, pattern_category='InvalidKeyword', pattern_subcategory=None, pattern_name=None, is_active=True, created_by=None, last_updated_by=None)
    .add_pattern_rule(rule_id=93, full_field_name='name_general', is_pattern_allowed=False, pattern_category='FullyDuplicatedCharacter', pattern_subcategory=None, pattern_name=None, is_active=True, created_by=None, last_updated_by=None)
    .add_pattern_rule(rule_id=94, full_field_name='name_general', is_pattern_allowed=True, pattern_category='UnicodeCharacters', pattern_subcategory=None, pattern_name=None, is_active=True, created_by=None, last_updated_by=None)
    .add_pattern_rule(rule_id=95, full_field_name='name_general', is_pattern_allowed=False, pattern_category='SpecialCharacter', pattern_subcategory=None, pattern_name=None, is_active=True, created_by=None, last_updated_by=None)
    .add_pattern_rule(rule_id=96, full_field_name='name_general', is_pattern_allowed=False, pattern_category=None, pattern_subcategory='Emptiness', pattern_name=None, is_active=True, created_by=None, last_updated_by=None)
    .add_pattern_rule(rule_id=97, full_field_name='name_general', is_pattern_allowed=False, pattern_category=None, pattern_subcategory=None, pattern_name='Has Space', is_active=True, created_by=None, last_updated_by=None)
    .add_pattern_rule(rule_id=98, full_field_name='name_general', is_pattern_allowed=True, pattern_category=None, pattern_subcategory=None, pattern_name='Is Fully Text', is_active=True, created_by=None, last_updated_by=None)
    .add_pattern_rule(rule_id=99, full_field_name='name_general', is_pattern_allowed=True, pattern_category=None, pattern_subcategory=None, pattern_name='Has Lowercase Character', is_active=True, created_by=None, last_updated_by=None)
    .add_pattern_rule(rule_id=100, full_field_name='name_general', is_pattern_allowed=True, pattern_category=None, pattern_subcategory=None, pattern_name='Has Hyphen', is_active=True, created_by=None, last_updated_by=None)
    .add_pattern_rule(rule_id=101, full_field_name='name_general', is_pattern_allowed=True, pattern_category=None, pattern_subcategory=None, pattern_name='Has Full Stop', is_active=True, created_by=None, last_updated_by=None)
    .add_pattern_rule(rule_id=102, full_field_name='name_general', is_pattern_allowed=True, pattern_category=None, pattern_subcategory=None, pattern_name='Has Single Quote', is_active=True, created_by=None, last_updated_by=None)
    .add_pattern_rule(rule_id=103, full_field_name='name_general', is_pattern_allowed=False, pattern_category=None, pattern_subcategory=None, pattern_name=None, is_active=False, created_by=None, last_updated_by=None)
    .add_pattern_rule(rule_id=104, full_field_name='mobile_number_without_country', is_pattern_allowed=False, pattern_category='DataType1', pattern_subcategory=None, pattern_name=None, is_active=True, created_by=None, last_updated_by=None)
    .add_pattern_rule(rule_id=105, full_field_name='mobile_number_without_country', is_pattern_allowed=False, pattern_category='DataType2', pattern_subcategory=None, pattern_name=None, is_active=True, created_by=None, last_updated_by=None)
    .add_pattern_rule(rule_id=106, full_field_name='mobile_number_without_country', is_pattern_allowed=False, pattern_category='DataType3', pattern_subcategory=None, pattern_name=None, is_active=True, created_by=None, last_updated_by=None)
    .add_pattern_rule(rule_id=107, full_field_name='mobile_number_without_country', is_pattern_allowed=False, pattern_category='SpecialCharacter', pattern_subcategory=None, pattern_name=None, is_active=True, created_by=None, last_updated_by=None)
    .add_pattern_rule(rule_id=108, full_field_name='mobile_number_without_country', is_pattern_allowed=False, pattern_category='InvalidKeyword', pattern_subcategory=None, pattern_name=None, is_active=True, created_by=None, last_updated_by=None)
    .add_pattern_rule(rule_id=109, full_field_name='mobile_number_without_country', is_pattern_allowed=False, pattern_category='FullyDuplicatedCharacter', pattern_subcategory=None, pattern_name=None, is_active=True, created_by=None, last_updated_by=None)
    .add_pattern_rule(rule_id=110, full_field_name='mobile_number_without_country', is_pattern_allowed=False, pattern_category='UnicodeCharacters', pattern_subcategory=None, pattern_name=None, is_active=True, created_by=None, last_updated_by=None)
    .add_pattern_rule(rule_id=111, full_field_name='mobile_number_without_country', is_pattern_allowed=False, pattern_category=None, pattern_subcategory='Emptiness', pattern_name=None, is_active=True, created_by=None, last_updated_by=None)
    .add_pattern_rule(rule_id=112, full_field_name='mobile_number_without_country', is_pattern_allowed=False, pattern_category=None, pattern_subcategory=None, pattern_name='Has Space', is_active=True, created_by=None, last_updated_by=None)
    .add_pattern_rule(rule_id=113, full_field_name='mobile_number_without_country', is_pattern_allowed=True, pattern_category=None, pattern_subcategory=None, pattern_name='Is Fully Numeric', is_active=True, created_by=None, last_updated_by=None)
    .add_pattern_rule(rule_id=114, full_field_name='mobile_number_without_country', is_pattern_allowed=True, pattern_category=None, pattern_subcategory=None, pattern_name='Is Empty or NULL', is_active=True, created_by=None, last_updated_by=None)
    .add_pattern_rule(rule_id=115, full_field_name='trade_type', is_pattern_allowed=False, pattern_category='InvalidKeyword', pattern_subcategory=None, pattern_name=None, is_active=False, created_by=None, last_updated_by=None)
    .add_pattern_rule(rule_id=116, full_field_name='trade_type', is_pattern_allowed=False, pattern_category='FullyDuplicatedCharacter', pattern_subcategory=None, pattern_name=None, is_active=True, created_by=None, last_updated_by=None)
    .add_pattern_rule(rule_id=117, full_field_name='trade_type', is_pattern_allowed=False, pattern_category='UnicodeCharacters', pattern_subcategory=None, pattern_name=None, is_active=True, created_by=None, last_updated_by=None)
    .add_pattern_rule(rule_id=118, full_field_name='trade_type', is_pattern_allowed=False, pattern_category=None, pattern_subcategory='Emptiness', pattern_name=None, is_active=True, created_by=None, last_updated_by=None)
    .add_pattern_rule(rule_id=119, full_field_name='trade_type', is_pattern_allowed=False, pattern_category=None, pattern_subcategory='100% Numeric', pattern_name=None, is_active=True, created_by=None, last_updated_by=None)
    .add_pattern_rule(rule_id=120, full_field_name='trade_type', is_pattern_allowed=True, pattern_category=None, pattern_subcategory='SpecialCharacter-L1', pattern_name=None, is_active=True, created_by=None, last_updated_by=None)
    .add_pattern_rule(rule_id=121, full_field_name='trade_type', is_pattern_allowed=True, pattern_category=None, pattern_subcategory='SpecialCharacter-L2', pattern_name=None, is_active=True, created_by=None, last_updated_by=None)
    .add_pattern_rule(rule_id=122, full_field_name='trade_type', is_pattern_allowed=True, pattern_category=None, pattern_subcategory=None, pattern_name='Has Semicolon', is_active=True, created_by=None, last_updated_by=None)
    .add_pattern_rule(rule_id=123, full_field_name='trade_type', is_pattern_allowed=True, pattern_category=None, pattern_subcategory=None, pattern_name='Has Space', is_active=True, created_by=None, last_updated_by=None)
    .add_pattern_rule(rule_id=124, full_field_name='address_general', is_pattern_allowed=False, pattern_category='DataType3', pattern_subcategory=None, pattern_name=None, is_active=True, created_by=None, last_updated_by=None)
    .add_pattern_rule(rule_id=125, full_field_name='address_general', is_pattern_allowed=False, pattern_category='InvalidKeyword', pattern_subcategory=None, pattern_name=None, is_active=True, created_by=None, last_updated_by=None)
    .add_pattern_rule(rule_id=126, full_field_name='address_general', is_pattern_allowed=False, pattern_category='FullyDuplicatedCharacter', pattern_subcategory=None, pattern_name=None, is_active=True, created_by=None, last_updated_by=None)
    .add_pattern_rule(rule_id=127, full_field_name='address_general', is_pattern_allowed=False, pattern_category='UnicodeCharacters', pattern_subcategory=None, pattern_name=None, is_active=True, created_by=None, last_updated_by=None)
    .add_pattern_rule(rule_id=128, full_field_name='address_general', is_pattern_allowed=False, pattern_category='SpecialCharacter', pattern_subcategory=None, pattern_name=None, is_active=True, created_by=None, last_updated_by=None)
    .add_pattern_rule(rule_id=129, full_field_name='address_general', is_pattern_allowed=False, pattern_category=None, pattern_subcategory='Emptiness', pattern_name=None, is_active=True, created_by=None, last_updated_by=None)
    .add_pattern_rule(rule_id=130, full_field_name='address_general', is_pattern_allowed=True, pattern_category=None, pattern_subcategory='SpecialCharacter-L1', pattern_name=None, is_active=True, created_by=None, last_updated_by=None)
    .add_pattern_rule(rule_id=131, full_field_name='address_general', is_pattern_allowed=True, pattern_category=None, pattern_subcategory='SpecialCharacter-L2', pattern_name=None, is_active=True, created_by=None, last_updated_by=None)
    .add_pattern_rule(rule_id=132, full_field_name='address_general', is_pattern_allowed=True, pattern_category=None, pattern_subcategory='SpecialCharacter-L3', pattern_name=None, is_active=True, created_by=None, last_updated_by=None)
    .add_pattern_rule(rule_id=133, full_field_name='address_general', is_pattern_allowed=False, pattern_category=None, pattern_subcategory=None, pattern_name='Has Exclamation Mark', is_active=True, created_by=None, last_updated_by=None)
    .add_pattern_rule(rule_id=134, full_field_name='address_general', is_pattern_allowed=False, pattern_category=None, pattern_subcategory=None, pattern_name='Has Question Mark', is_active=True, created_by=None, last_updated_by=None)
    .add_pattern_rule(rule_id=135, full_field_name='address_general', is_pattern_allowed=True, pattern_category=None, pattern_subcategory=None, pattern_name='Has Space', is_active=True, created_by=None, last_updated_by=None)
    .add_pattern_rule(rule_id=136, full_field_name='address_general', is_pattern_allowed=False, pattern_category=None, pattern_subcategory=None, pattern_name='Is Fully Decimal', is_active=True, created_by=None, last_updated_by=None)
    .add_pattern_rule(rule_id=137, full_field_name='address_general', is_pattern_allowed=False, pattern_category=None, pattern_subcategory=None, pattern_name='Is Fully Numeric', is_active=True, created_by=None, last_updated_by=None)
)

# COMMAND ----------

# DBTITLE 1,configFieldAllowedPattern -Data
display(spark.sql(f"SELECT * FROM `{MY_CATALOG}`.`{DQ_SCHEMA}`.`configFieldAllowedPattern`"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## configCustomQuery

# COMMAND ----------

# DBTITLE 1,Upsert configCustomQuery -PySpark
(dq.config
    .add_custom_query(full_field_name='email_address', expression='(  (@InputValue LIKE \'%_@_%.__%\' AND LENGTH(SUBSTRING(@InputValue, LOCATE(\'@\', @InputValue) + 1, LENGTH(@InputValue))) - LENGTH(REPLACE(SUBSTRING(@InputValue, LOCATE(\'@\', @InputValue) + 1, LENGTH(@InputValue)), \'.\', \'\')) = 1)  OR (@InputValue LIKE \'%_@_%.__%.__%\' AND LENGTH(SUBSTRING(@InputValue, LOCATE(\'@\', @InputValue) + 1, LENGTH(@InputValue))) - LENGTH(REPLACE(SUBSTRING(@InputValue, LOCATE(\'@\', @InputValue) + 1, LENGTH(@InputValue)), \'.\', \'\')) = 2)  OR (@InputValue LIKE \'%_@_%.__%._%._%\' AND LENGTH(SUBSTRING(@InputValue, LOCATE(\'@\', @InputValue) + 1, LENGTH(@InputValue))) - LENGTH(REPLACE(SUBSTRING(@InputValue, LOCATE(\'@\', @InputValue) + 1, LENGTH(@InputValue)), \'.\', \'\')) > 2) )', is_condition_allowed=True, query_id=1, custom_query_type='SQL', description='Basic validations to ensure Pattern of valid email address.', is_active=True, created_by=None, last_updated_by=None)
    .add_custom_query(full_field_name='email_address', expression='( LOCATE(\'@\', @InputValue) > 1 AND LOCATE(\'@\', @InputValue) < LENGTH(@InputValue) AND LOCATE(\'@\', REVERSE(@InputValue)) > LOCATE(\'.\', REVERSE(@InputValue)) AND (LENGTH(@InputValue) - LENGTH(REPLACE(@InputValue, \'@\', \'\'))) = 1 )', is_condition_allowed=True, query_id=2, custom_query_type='SQL', description='Additional validations to ensure Pattern of valid email address- 1. Ensure \'@\' is not the first character 2. Ensure \'@\' is not the last character 3. Ensure at least one dot after \'@\' 4. Ensure only one \'@\' symbol', is_active=True, created_by=None, last_updated_by=None)
    .add_custom_query(full_field_name='email_address', expression='LENGTH(SUBSTRING(@InputValue, LOCATE(\'@\', @InputValue) + 1, LENGTH(@InputValue))) > 0 AND (LENGTH(SUBSTRING(@InputValue, LOCATE(\'@\', @InputValue) + 1, LENGTH(@InputValue)))  - LENGTH(REPLACE(SUBSTRING(@InputValue, LOCATE(\'@\', @InputValue) + 1, LENGTH(@InputValue)), \'.\', \'\'))  BETWEEN 1 AND 3 ) AND @InputValue NOT LIKE \'%.\' AND (LOCATE(\'..\', SUBSTRING(@InputValue, LOCATE(\'@\', @InputValue) + 1, LENGTH(@InputValue))) = 0   AND LENGTH(SUBSTRING(SUBSTRING(@InputValue, LOCATE(\'@\', @InputValue) + 1, LENGTH(@InputValue)), LOCATE(\'.\', SUBSTRING(@InputValue, LOCATE(\'@\', @InputValue) + 1, LENGTH(@InputValue))) + 1, LENGTH(SUBSTRING(@InputValue, LOCATE(\'@\', @InputValue) + 1, LENGTH(@InputValue))))) >= 2   AND (LOCATE(\'.\', SUBSTRING(SUBSTRING(@InputValue, LOCATE(\'@\', @InputValue) + 1, LENGTH(@InputValue)), LOCATE(\'.\', SUBSTRING(@InputValue, LOCATE(\'@\', @InputValue) + 1, LENGTH(@InputValue))) + 1, LENGTH(SUBSTRING(@InputValue, LOCATE(\'@\', @InputValue) + 1, LENGTH(@InputValue))))) = 0    OR LENGTH(SUBSTRING(SUBSTRING(@InputValue, LOCATE(\'@\', @InputValue) + 1, LENGTH(@InputValue)), LOCATE(\'.\', SUBSTRING(@InputValue, LOCATE(\'@\', @InputValue) + 1, LENGTH(@InputValue)), LOCATE(\'.\', SUBSTRING(@InputValue, LOCATE(\'@\', @InputValue) + 1, LENGTH(@InputValue))) + 1) + 1, LENGTH(SUBSTRING(@InputValue, LOCATE(\'@\', @InputValue) + 1, LENGTH(@InputValue))))) >= 2) )', is_condition_allowed=True, query_id=3, custom_query_type='SQL', description='1. Extract the domain part after \'@\' and ensure  \'.\' (periods) atleast 1 & atmost 3 2. Validate that domain labels are not empty and ensure there are no consecutive periods and each part is at least one character long', is_active=True, created_by=None, last_updated_by=None)
    .add_custom_query(full_field_name='email_address', expression='LENGTH(SUBSTRING(@InputValue, 1, LOCATE(\'@\', @InputValue) - 1)) - LENGTH(REPLACE(SUBSTRING(@InputValue, 1, LOCATE(\'@\', @InputValue) - 1), \'.\', \'\')) < 3', is_condition_allowed=True, query_id=4, custom_query_type='SQL', description='Ensure that main part does not contain more than 2 \'.\' (periods)', is_active=True, created_by=None, last_updated_by=None)
    .add_custom_query(full_field_name='email_address', expression='@InputValue LIKE \'%noemaildress%\'', is_condition_allowed=False, query_id=5, custom_query_type='SQL', description='Ensure that email does not contain a keyword like \'noemaildress\'', is_active=True, created_by=None, last_updated_by=None)
    .add_custom_query(full_field_name='email_address', expression='@InputValue RLIKE \'.*[-._]$\'', is_condition_allowed=False, query_id=6, custom_query_type='SQL', description='Ensure that email does not end with allowed special characters', is_active=True, created_by=None, last_updated_by=None)
    .add_custom_query(full_field_name='email_address', expression='^[a-zA-Z0-9._%+\\-]+@[a-zA-Z0-9]([a-zA-Z0-9\\-]{0,61}[a-zA-Z0-9])?(\\.[a-zA-Z0-9]([a-zA-Z0-9\\-]{0,61}[a-zA-Z0-9])?)*\\.[a-zA-Z]{2,}$', is_condition_allowed=True, query_id=7, custom_query_type='REGEX', description='Validates email address format: RFC-compliant structure with alphanumeric/special-char local part, domain labels max 63 chars with no leading/trailing hyphens, no consecutive dots, and TLD minimum 2 alpha characters.', is_active=True, created_by=None, last_updated_by=None)
    .add_custom_query(full_field_name='check_vendor_license_number', expression='^[A-Z]{2,4}/[A-Z]{2}[0-9]{3}-[0-9]{3}$', is_condition_allowed=True, query_id=8, custom_query_type='REGEX', description='Validates vendor code format: 2-4 uppercase org prefix, forward-slash separator, 2 uppercase type code letters, 3-digit sequence, hyphen, 3-digit version (e.g. ACME/WH001-003).', is_active=True, created_by=None, last_updated_by=None)
)

# COMMAND ----------

# DBTITLE 1,configCustomQuery -Data
display(spark.sql(f"SELECT * FROM `{MY_CATALOG}`.`{DQ_SCHEMA}`.`configCustomQuery`"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## mapDQChecks

# COMMAND ----------

# DBTITLE 1,Upsert mapDQChecks -PySpark
(dq.config
    .add_mapping(full_field_name='email_address',                 target_schema_name=CURATED_SCHEMA, target_table_name='mock_curated_contacts',  target_field_name='email_address',    mapping_id=1,  dq_function_schema_name=DQ_SCHEMA, dq_function_name='fn_DQ_email_address',                 is_active=True, created_by=None, last_updated_by=None)
    .add_mapping(full_field_name='postal_code_all_country',       target_schema_name=CURATED_SCHEMA, target_table_name='mock_curated_locations', target_field_name='postal_code',       mapping_id=2,  dq_function_schema_name=DQ_SCHEMA, dq_function_name='fn_DQ_postal_code_all_country',       is_active=True, created_by=None, last_updated_by=None)
    .add_mapping(full_field_name='trade_type',                    target_schema_name=CURATED_SCHEMA, target_table_name='mock_curated_vendors',   target_field_name='trade_type',        mapping_id=3,  dq_function_schema_name=DQ_SCHEMA, dq_function_name='fn_DQ_trade_type',                    is_active=True, created_by=None, last_updated_by=None)
    .add_mapping(full_field_name='state',                         target_schema_name=CURATED_SCHEMA, target_table_name='mock_curated_locations', target_field_name='state',             mapping_id=4,  dq_function_schema_name=DQ_SCHEMA, dq_function_name='fn_DQ_state',                         is_active=True, created_by=None, last_updated_by=None)
    .add_mapping(full_field_name='city',                          target_schema_name=CURATED_SCHEMA, target_table_name='mock_curated_locations', target_field_name='city',              mapping_id=5,  dq_function_schema_name=DQ_SCHEMA, dq_function_name='fn_DQ_city',                          is_active=True, created_by=None, last_updated_by=None)
    .add_mapping(full_field_name='address_line_1',                target_schema_name=CURATED_SCHEMA, target_table_name='mock_curated_locations', target_field_name='address1',          mapping_id=6,  dq_function_schema_name=DQ_SCHEMA, dq_function_name='fn_DQ_address_line_1',                is_active=True, created_by=None, last_updated_by=None)
    .add_mapping(full_field_name='address_line_1',                target_schema_name=CURATED_SCHEMA, target_table_name='mock_curated_locations', target_field_name='address2',          mapping_id=7,  dq_function_schema_name=DQ_SCHEMA, dq_function_name='fn_DQ_address_line_1',                is_active=True, created_by=None, last_updated_by=None)
    .add_mapping(full_field_name='address_general',               target_schema_name=CURATED_SCHEMA, target_table_name='mock_curated_locations', target_field_name='address4',          mapping_id=8,  dq_function_schema_name=DQ_SCHEMA, dq_function_name='fn_DQ_address_general',               is_active=True, created_by=None, last_updated_by=None)
    .add_mapping(full_field_name='country',                       target_schema_name=CURATED_SCHEMA, target_table_name='mock_curated_locations', target_field_name='country',           mapping_id=9,  dq_function_schema_name=DQ_SCHEMA, dq_function_name='fn_DQ_country',                       is_active=True, created_by=None, last_updated_by=None)
    .add_mapping(full_field_name='address_line_2',                target_schema_name=CURATED_SCHEMA, target_table_name='mock_curated_locations', target_field_name='address3',          mapping_id=10, dq_function_schema_name=DQ_SCHEMA, dq_function_name='fn_DQ_address_line_2',                is_active=True, created_by=None, last_updated_by=None)
    .add_mapping(full_field_name='mobile_number_without_country', target_schema_name=CURATED_SCHEMA, target_table_name='mock_curated_contacts',  target_field_name='raw_phone_number',  mapping_id=11, dq_function_schema_name=DQ_SCHEMA, dq_function_name='fn_DQ_mobile_number_without_country', is_active=True, created_by=None, last_updated_by=None)
    .add_mapping(full_field_name='name_general',                  target_schema_name=CURATED_SCHEMA, target_table_name='mock_curated_vendors',   target_field_name='last_name',         mapping_id=12, dq_function_schema_name=DQ_SCHEMA, dq_function_name='fn_DQ_name_general',                  is_active=True, created_by=None, last_updated_by=None)
    .add_mapping(full_field_name='vendor_name',                   target_schema_name=CURATED_SCHEMA, target_table_name='mock_curated_vendors',   target_field_name='vendor_name',       mapping_id=13, dq_function_schema_name=DQ_SCHEMA, dq_function_name='fn_DQ_vendor_name',                   is_active=True, created_by=None, last_updated_by=None)
    .add_mapping(full_field_name='first_name',                    target_schema_name=CURATED_SCHEMA, target_table_name='mock_curated_vendors',   target_field_name='first_name',        mapping_id=14, dq_function_schema_name=DQ_SCHEMA, dq_function_name='fn_DQ_first_name',                    is_active=True, created_by=None, last_updated_by=None)
    .add_mapping(full_field_name='phone_number',                  target_schema_name=CURATED_SCHEMA, target_table_name='mock_curated_contacts',  target_field_name='phone_number',      mapping_id=15, dq_function_schema_name=DQ_SCHEMA, dq_function_name='fn_DQ_phone_number',                  is_active=True, created_by=None, last_updated_by=None)
    .add_mapping(full_field_name='check_vendor_license_number',   target_schema_name=CURATED_SCHEMA, target_table_name='mock_curated_vendors',   target_field_name='vendor_code',       mapping_id=16, dq_function_schema_name=DQ_SCHEMA, dq_function_name='fn_DQ_check_vendor_license_number',   is_active=True, created_by=None, last_updated_by=None)
)

# COMMAND ----------

# DBTITLE 1,mapDQChecks -Data
display(spark.sql(f"SELECT * FROM `{MY_CATALOG}`.`{DQ_SCHEMA}`.`mapDQChecks`"))

# COMMAND ----------

# DBTITLE 1,Config Summary
dq.config.show_config_summary()
