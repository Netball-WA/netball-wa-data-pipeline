{{ config(
    materialized='view',
    alias='DATA_FIELDS'
) }}

SELECT
    RECORD_ID AS DATA_FIELD_ID,
    DATA:sellerId::VARCHAR AS SELLER_ID,
    DATA:name::VARCHAR AS FIELD_NAME,
    DATA:title::VARCHAR AS TITLE,
    DATA:slug::VARCHAR AS SLUG,
    DATA:type::VARCHAR AS FIELD_TYPE,
    DATA:isPersonalData::BOOLEAN AS IS_PERSONAL_DATA,
    DATA:options AS OPTIONS,
    DATA:settings AS SETTINGS,
    FIRST_LOADED_AT,
    LAST_CHANGED_AT
FROM {{ source('vivenu_native', 'vivenu_raw_current') }}
WHERE STREAM_NAME = 'data_fields'