{{ config(
    materialized='view',
    database=var('nbwa_bronze_database'),
    schema=var('nbwa_bronze_schema'),
    alias='SCANS'
) }}

SELECT
    RECORD_ID AS SCAN_ID,
    DATA:sellerId::VARCHAR AS SELLER_ID,
    DATA:eventId::VARCHAR AS EVENT_ID,
    DATA:ticketId::VARCHAR AS TICKET_ID,
    DATA:deviceId::VARCHAR AS DEVICE_ID,
    TRY_TO_TIMESTAMP_TZ(DATA:time::VARCHAR) AS SCANNED_AT,
    DATA:type::VARCHAR AS SCAN_TYPE,
    DATA:scanResult::VARCHAR AS SCAN_RESULT,
    FIRST_LOADED_AT,
    LAST_CHANGED_AT
FROM {{ source('vivenu_native', 'vivenu_raw_current') }}
WHERE STREAM_NAME = 'scans'