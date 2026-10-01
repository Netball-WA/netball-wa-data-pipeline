{{ config(
    materialized='view',
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
    LAST_CHANGED_AT,
    TRY_TO_DATE(DATA:extraFields.birth_date::VARCHAR) AS DOB,
    NULLIF(TRIM(DATA:extraFields.gender::VARCHAR), '') AS GENDER,
    DATA:extraFields.member_number::VARCHAR AS MEMBER_NUMBER,
    DATA:extraFields.member_since::VARCHAR AS MEMBER_SINCE,
    TRY_TO_DATE(DATA:extraFields.member_since::VARCHAR) AS MEMBER_SINCE_DATE,
    DATA:extraFields.ttk_customer_id::VARCHAR AS LEGACY_TICKETEK_CUSTOMER_ID,
    DATA:externalId::VARCHAR AS SOURCE_EXTERNAL_ID,
    DATA:number::VARCHAR AS CUSTOMER_NUMBER,
    TRY_TO_BOOLEAN(DATA:extraFields.marketing_consent::VARCHAR) AS MARKETING_CONSENT,
    DATA:consents AS CONSENTS,
    DATA:extraFields AS EXTRA_FIELDS,
    DATA:memberships AS MEMBERSHIPS,
    SOURCE_CONTEXT AS SOURCE_CONTEXT
FROM {{ source('vivenu_native', 'vivenu_raw_current') }}
WHERE STREAM_NAME = 'customers'