{{ config(
    materialized='view',
    database=var('nbwa_bronze_database'),
    schema=var('nbwa_bronze_schema'),
    alias='CUSTOMERS'
) }}

SELECT
    RECORD_ID AS CUSTOMER_ID,
    DATA:sellerId::VARCHAR AS SELLER_ID,
    DATA:primaryEmail::VARCHAR AS EMAIL,
    DATA:name::VARCHAR AS CUSTOMER_NAME,
    DATA:prename::VARCHAR AS FIRST_NAME,
    DATA:lastname::VARCHAR AS LAST_NAME,
    DATA:phone::VARCHAR AS PHONE,
    DATA:tags AS TAGS,
    DATA:segments AS SEGMENTS,
    DATA:location.street::VARCHAR AS LOCATION_STREET,
    DATA:location.city::VARCHAR AS LOCATION_CITY,
    DATA:location.state::VARCHAR AS LOCATION_STATE,
    DATA:location.postal::VARCHAR AS LOCATION_POSTAL,
    DATA:location.country::VARCHAR AS LOCATION_COUNTRY,
    TRY_TO_TIMESTAMP_TZ(DATA:createdAt::VARCHAR) AS CREATED_AT,
    SOURCE_UPDATED_AT AS UPDATED_AT,
    FIRST_LOADED_AT,
    LAST_CHANGED_AT
FROM {{ source('vivenu_native', 'vivenu_raw_current') }}
WHERE STREAM_NAME = 'customers'