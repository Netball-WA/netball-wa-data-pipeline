{{ config(
    materialized='view',
    database=var('nbwa_bronze_database'),
    schema=var('nbwa_bronze_schema'),
    alias='SALES_CHANNELS'
) }}

SELECT
    RECORD_ID AS SALES_CHANNEL_ID,
    DATA:sellerId::VARCHAR AS SELLER_ID,
    DATA:name::VARCHAR AS CHANNEL_NAME,
    DATA:type::VARCHAR AS CHANNEL_TYPE,
    DATA:groupId::VARCHAR AS GROUP_ID,
    DATA:status::VARCHAR AS STATUS,
    TRY_TO_TIMESTAMP_TZ(DATA:createdAt::VARCHAR) AS CREATED_AT,
    SOURCE_UPDATED_AT AS UPDATED_AT,
    FIRST_LOADED_AT,
    LAST_CHANGED_AT
FROM {{ source('vivenu_native', 'vivenu_raw_current') }}
WHERE STREAM_NAME = 'sales_channels'