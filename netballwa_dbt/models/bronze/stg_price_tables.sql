{{ config(
    materialized='view',
    alias='PRICE_TABLES'
) }}

SELECT
    RECORD_ID AS PRICE_TABLE_ID,
    DATA:sellerId::VARCHAR AS SELLER_ID,
    DATA:name::VARCHAR AS TABLE_NAME,
    DATA:categories AS CATEGORIES,
    DATA:tiers AS TIERS,
    DATA:types AS TYPES,
    FIRST_LOADED_AT,
    LAST_CHANGED_AT
FROM {{ source('vivenu_native', 'vivenu_raw_current') }}
WHERE STREAM_NAME = 'price_tables'