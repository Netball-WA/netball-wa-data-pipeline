{{ config(
    materialized='view',
    database=var('nbwa_bronze_database'),
    schema=var('nbwa_bronze_schema'),
    alias='SEATING_EVENTS'
) }}

SELECT
    RECORD_ID AS SEATING_EVENT_ID,
    DATA:_owner::VARCHAR AS EVENT_ID,
    DATA:seatMapId::VARCHAR AS SEAT_MAP_ID,
    DATA:revisionId::VARCHAR AS REVISION_ID,
    TRY_TO_TIMESTAMP_TZ(DATA:createdAt::VARCHAR) AS CREATED_AT,
    SOURCE_UPDATED_AT AS UPDATED_AT,
    FIRST_LOADED_AT,
    LAST_CHANGED_AT
FROM {{ source('vivenu_native', 'vivenu_raw_current') }}
WHERE STREAM_NAME = 'seating_events'