{{ config(
    materialized='view',
    alias='CUSTOMER_PAYMENT_METHODS'
) }}

SELECT
    RECORD_ID AS PAYMENT_METHOD_ID,
    DATA:sellerId::VARCHAR AS SELLER_ID,
    DATA:customerId::VARCHAR AS CUSTOMER_ID,
    DATA:gatewayId::VARCHAR AS GATEWAY_ID,
    DATA:gatewayType::VARCHAR AS GATEWAY_TYPE,
    DATA:status::VARCHAR AS STATUS,
    DATA:active::BOOLEAN AS IS_ACTIVE,
    DATA:"primary"::BOOLEAN AS IS_PRIMARY,
    DATA:channels AS CHANNELS,
    TRY_TO_TIMESTAMP_TZ(DATA:createdAt::VARCHAR) AS CREATED_AT,
    SOURCE_UPDATED_AT AS UPDATED_AT,
    FIRST_LOADED_AT,
    LAST_CHANGED_AT
FROM {{ source('vivenu_native', 'vivenu_raw_current') }}
WHERE STREAM_NAME = 'customer_payment_methods'