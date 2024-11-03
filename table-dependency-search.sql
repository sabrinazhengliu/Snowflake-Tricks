USE ROLE <your-DBA-role>;
USE WAREHOUSE <larger-warehouse>;
USE DATABASE SNOWFLAKE;
USE SCHEMA ACCOUNT_USAGE;

SET table_name = <your-table>;  -- spell out DATABASE.SCHEMA.TABLE_NAME
SET start_date = NULL;  -- manually enter date range
SET end_date = NULL;    -- manually enter date range

BEGIN
  IF ($start_date IS NULL) THEN SET START_DATE = CURRENT_DATE() - 1; END IF;
  IF ($end_date IS NULL) THEN SET END_DATE = CURRENT_DATE(); END IF;
END;

-- find upstream source tables and queries used
-- use wild card serach as needed to include variations or backup copies

WITH access_history_source_tables AS (
  SELECT
    QUERY_ID
  , LISTAGG(DISTINCT PARSE_JSON(FJ.VALUE):objectName::STRING, ', ') as SOURCE_TABLES
  , TRIM(OBJECTS_MODIFIED[0].objectName, '"') AS TARGET_TABLE
  FROM SNOWFLAKE.ACCOUNT_USAGE.ACCESS_HISTORY
  , TABLE(FLATTEN(DIRECT_OBJECTS_ACCESSED)) FJ
  WHERE 
    OBJECTS_MODIFIED[0].objectName = $table_name
  -- OBJECTS_MODIFIED[0].objectName ilike '%'||$table_name||'%'
  AND QUERY_START_TIME BETWEEN $start_date AND $end_date
  GROUP BY ALL
)
-- select * from access_history_source_tables
SELECT
  AH.QUERY_ID
, QH.USER_NAME
, QH.ROLE_NAME
, QH.START_TIME
, QH.END_TIME
, QH.EXECUTION_TIME
, QH.EXECUTION_STATUS
, QH.ROWS_PRODUCED
, QH.DATABASE_NAME
, QH.SCHEMA_NAME
, QH.WAREHOUSE_SIZE
, QH.QUERY_TYPE
, AH.SOURCE_TABLES
, AH.TARGET_TABLE
, QH.QUERY_TEXT
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY QH
JOIN access_history_source_tables          AH ON QH.QUERY_ID = AH.QUERY_ID
WHERE START_TIME BETWEEN $start_date AND $end_date
ORDER BY START_TIME
;

-- find downstream target tables and queries used
-- use wild card search as needed to include variations or backup copies

WITH access_history_target_tables AS (
  SELECT
    QUERY_ID
  , LISTAGG(DISTINCT PARSE_JSON(FJ.VALUE):objectName::STRING, ', ') as TARGET_TABLES
  , TRIM(DIRECT_OBJECTS_ACCESSED[0].objectName, '"') AS SOURCE_TABLE
  FROM SNOWFLAKE.ACCOUNT_USAGE.ACCESS_HISTORY
  , TABLE(FLATTEN(OBJECTS_MODIFIED)) FJ
  WHERE
    DIRECT_OBJECTS_ACCESSED[0].objectName = $table_name
  -- DIRECT_OBJECTS_ACCESSED[0].objectName ilike '%'||$table_name||'%'
  AND QUERY_START_TIME BETWEEN $start_date AND $end_date
  GROUP BY ALL
)
-- select * from access_history_target_tables
SELECT
  AH.QUERY_ID
, QH.USER_NAME
, QH.ROLE_NAME
, QH.START_TIME
, QH.END_TIME
, QH.EXECUTION_TIME
, QH.EXECUTION_STATUS
, QH.ROWS_PRODUCED
, QH.DATABASE_NAME
, QH.SCHEMA_NAME
, QH.WAREHOUSE_SIZE
, QH.QUERY_TYPE
, AH.SOURCE_TABLE
, AH.TARGET_TABLES
, QH.QUERY_TEXT
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY QH
JOIN access_history_target_tables          AH ON QH.QUERY_ID = AH.QUERY_ID
WHERE START_TIME BETWEEN $start_date AND $end_date
ORDER BY START_TIME
;
