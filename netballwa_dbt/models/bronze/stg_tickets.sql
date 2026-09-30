{{ config(
    materialized='view',
    alias='TICKETS'
) }}

SELECT
    RECORD_ID AS TICKET_ID,
    DATA:sellerId::VARCHAR AS SELLER_ID,
    DATA:eventId::VARCHAR AS EVENT_ID,
    DATA:transactionId::VARCHAR AS TRANSACTION_ID,
    DATA:customerId::VARCHAR AS CUSTOMER_ID,
    DATA:ticketTypeId::VARCHAR AS TICKET_TYPE_ID,
    DATA:ticketName::VARCHAR AS TICKET_NAME,
    DATA:name::VARCHAR AS ATTENDEE_NAME,
    DATA:email::VARCHAR AS EMAIL,
    DATA:status::VARCHAR AS STATUS,
    DATA:seatingInfo AS SEATING_INFO,
    DATA:seatingInfo.sectionName::VARCHAR AS SEAT_SECTION,
    DATA:seatingInfo.rowName::VARCHAR AS SEAT_ROW,
    DATA:seatingInfo.seatName::VARCHAR AS SEAT_NAME,
    DATA:seatingInfo.gate::VARCHAR AS SEAT_GATE,
    TRY_TO_TIMESTAMP_TZ(DATA:createdAt::VARCHAR) AS CREATED_AT,
    SOURCE_UPDATED_AT AS UPDATED_AT,
    FIRST_LOADED_AT,
    LAST_CHANGED_AT
FROM {{ source('vivenu_native', 'vivenu_raw_current') }}
WHERE STREAM_NAME = 'tickets'