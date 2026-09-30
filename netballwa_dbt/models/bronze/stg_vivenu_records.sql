{{ config(
    materialized='view',
    alias='VIVENU_RECORDS'
) }}

SELECT
    STREAM_NAME,
    RECORD_ID,
    SOURCE_UPDATED_AT,
    DATA,
    SOURCE_CONTEXT,
    RECORD_HASH,
    FIRST_LOADED_AT,
    LAST_CHANGED_AT,
    LAST_RUN_ID
FROM {{ source('vivenu_native', 'vivenu_raw_current') }}