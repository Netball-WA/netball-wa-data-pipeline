{{ config(
    materialized='view',
    alias='SALES_CHANNEL_GROUPS'
) }}

SELECT
    RECORD_ID AS SALES_CHANNEL_GROUP_ID,
    DATA:sellerId::VARCHAR AS SELLER_ID,
    DATA:name::VARCHAR AS GROUP_NAME,
    DATA:status::VARCHAR AS STATUS,
    DATA:restrictEventAccess::BOOLEAN AS RESTRICT_EVENT_ACCESS,
    TRY_TO_TIMESTAMP_TZ(DATA:createdAt::VARCHAR) AS CREATED_AT,
    SOURCE_UPDATED_AT AS UPDATED_AT,
    FIRST_LOADED_AT,
    LAST_CHANGED_AT
FROM {{ source('vivenu_native', 'vivenu_raw_current') }}
WHERE STREAM_NAME = 'sales_channel_groups'