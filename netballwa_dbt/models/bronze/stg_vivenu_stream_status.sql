{{ config(
    materialized='view',
    database=var('nbwa_bronze_database'),
    schema=var('nbwa_bronze_schema'),
    alias='VIVENU_STREAM_STATUS'
) }}

SELECT
    C.STREAM_NAME,
    C.ENABLED,
    C.API_SPEC:mode::VARCHAR AS SYNC_MODE,
    COALESCE(C.API_SPEC:service::VARCHAR, 'core') AS SERVICE,
    C.API_SPEC:path::VARCHAR AS ENDPOINT,
    C.API_SPEC:cursor::VARCHAR AS CURSOR_FIELD,
    C.API_SPEC:parent:stream::VARCHAR AS PARENT_STREAM,
    COALESCE(C.API_SPEC:lookup::BOOLEAN, FALSE) AS USES_LOOKUP_IDS,
    C.API_SPEC:blocked::VARCHAR AS BLOCKED_REASON,
    C.MIN_SYNC_INTERVAL_MINUTES,
    C.LOOKBACK_SECONDS,
    C.RECONCILE_HOURS,
    S.LAST_SUCCESS_TO_TS,
    S.LAST_SUCCESS_AT,
    S.LAST_FULL_SYNC_AT,
    S.LAST_RUN_ID
FROM {{ source('vivenu_native', 'vivenu_stream_config') }} C
LEFT JOIN {{ source('vivenu_native', 'vivenu_sync_control') }} S
    ON S.STREAM_NAME = C.STREAM_NAME