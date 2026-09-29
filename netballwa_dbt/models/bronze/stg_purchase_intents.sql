{{ config(
    materialized='view',
    database=var('nbwa_bronze_database'),
    schema=var('nbwa_bronze_schema'),
    alias='PURCHASE_INTENTS'
) }}

SELECT
    RECORD_ID AS PURCHASE_INTENT_ID,
    DATA:sellerId::VARCHAR AS SELLER_ID,
    DATA:eventId::VARCHAR AS EVENT_ID,
    DATA:customerId::VARCHAR AS CUSTOMER_ID,
    DATA:email::VARCHAR AS EMAIL,
    DATA:firstname::VARCHAR AS FIRST_NAME,
    DATA:lastname::VARCHAR AS LAST_NAME,
    DATA:status::VARCHAR AS STATUS,
    DATA:approvalStatus::VARCHAR AS APPROVAL_STATUS,
    DATA:outcome::VARCHAR AS OUTCOME,
    DATA:currency::VARCHAR AS CURRENCY,
    TRY_TO_DECIMAL(DATA:realPrice::VARCHAR, 18, 4) AS REAL_PRICE,
    TRY_TO_DECIMAL(DATA:regularPrice::VARCHAR, 18, 4) AS REGULAR_PRICE,
    DATA:salesChannelId::VARCHAR AS SALES_CHANNEL_ID,
    DATA:strategyId::VARCHAR AS STRATEGY_ID,
    DATA:tickets AS TICKETS,
    DATA:ticketsCreated::BOOLEAN AS TICKETS_CREATED,
    TRY_TO_TIMESTAMP_TZ(DATA:createdAt::VARCHAR) AS CREATED_AT,
    SOURCE_UPDATED_AT AS UPDATED_AT,
    FIRST_LOADED_AT,
    LAST_CHANGED_AT
FROM {{ source('vivenu_native', 'vivenu_raw_current') }}
WHERE STREAM_NAME = 'purchase_intents'