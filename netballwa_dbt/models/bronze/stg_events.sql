{{ config(
    materialized='view',
    database=var('nbwa_bronze_database'),
    schema=var('nbwa_bronze_schema'),
    alias='EVENTS'
) }}

SELECT
    RECORD_ID AS EVENT_ID,
    DATA:sellerId::VARCHAR AS SELLER_ID,
    DATA:name::VARCHAR AS EVENT_NAME,
    TRY_TO_TIMESTAMP_TZ(DATA:start::VARCHAR) AS START_AT,
    TRY_TO_TIMESTAMP_TZ(DATA:end::VARCHAR) AS END_AT,
    TRY_TO_TIMESTAMP_TZ(DATA:createdAt::VARCHAR) AS CREATED_AT,
    SOURCE_UPDATED_AT AS UPDATED_AT,
    FIRST_LOADED_AT,
    LAST_CHANGED_AT
FROM {{ source('vivenu_native', 'vivenu_raw_current') }}
WHERE STREAM_NAME = 'events'