{{ config(
    materialized='view',
    alias='SUBSCRIPTIONS'
) }}

SELECT
    RECORD_ID AS SUBSCRIPTION_ID,
    DATA:sellerId::VARCHAR AS SELLER_ID,
    DATA:customerId::VARCHAR AS CUSTOMER_ID,
    DATA:planId::VARCHAR AS PLAN_ID,
    DATA:status::VARCHAR AS STATUS,
    DATA:currency::VARCHAR AS CURRENCY,
    DATA:items AS ITEMS,
    TRY_TO_TIMESTAMP_TZ(DATA:startAt::VARCHAR) AS START_AT,
    TRY_TO_TIMESTAMP_TZ(DATA:canceledAt::VARCHAR) AS CANCELED_AT,
    SOURCE_UPDATED_AT AS UPDATED_AT,
    FIRST_LOADED_AT,
    LAST_CHANGED_AT
FROM {{ source('vivenu_native', 'vivenu_raw_current') }}
WHERE STREAM_NAME = 'subscriptions'