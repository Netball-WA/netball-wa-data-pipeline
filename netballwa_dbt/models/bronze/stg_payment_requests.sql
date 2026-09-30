{{ config(
    materialized='view',
    alias='PAYMENT_REQUESTS'
) }}

SELECT
    RECORD_ID AS PAYMENT_REQUEST_ID,
    DATA:sellerId::VARCHAR AS SELLER_ID,
    DATA:checkoutId::VARCHAR AS CHECKOUT_ID,
    DATA:customer.customerId::VARCHAR AS CUSTOMER_ID,
    DATA:status::VARCHAR AS STATUS,
    DATA:currency::VARCHAR AS CURRENCY,
    TRY_TO_DECIMAL(DATA:amount::VARCHAR, 18, 4) AS AMOUNT,
    TRY_TO_DECIMAL(DATA:originalAmount::VARCHAR, 18, 4) AS ORIGINAL_AMOUNT,
    DATA:origin::VARCHAR AS ORIGIN,
    DATA:applications AS APPLICATIONS,
    TRY_TO_TIMESTAMP_TZ(DATA:createdAt::VARCHAR) AS CREATED_AT,
    TRY_TO_TIMESTAMP_TZ(DATA:expiresAt::VARCHAR) AS EXPIRES_AT,
    SOURCE_UPDATED_AT AS UPDATED_AT,
    FIRST_LOADED_AT,
    LAST_CHANGED_AT
FROM {{ source('vivenu_native', 'vivenu_raw_current') }}
WHERE STREAM_NAME = 'payment_requests'