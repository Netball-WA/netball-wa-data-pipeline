{ % macro data_feed_vivenu_config(databse, schema) % }

-- =====================================================================
-- 3. GLOBAL DEFAULTS
-- =====================================================================

-- INITIAL_FROM_TS is the lower bound for:
--   * the first automatic read of a date-filtered stream;
--   * forced full reads;
--   * periodic retained-history reconciliation.
--
-- It is applied to the stream's configured cursor, not universally to
-- createdAt. For an UPDATED stream, an old record changed recently can
-- still qualify.
--
-- 1970 requests all available dated history.
-- Change this BEFORE the first load when a narrower history is sufficient.
--
-- Existing configuration values are not overwritten by this seed.

MERGE INTO {{database}}.{{schema}}.VIVENU_CONFIG T
USING (
    SELECT
        1 AS CONFIG_ID,
        '1970-01-01 00:00:00 +00:00'::TIMESTAMP_TZ AS INITIAL_FROM_TS,
        100 AS PAGE_SIZE,
        60 AS REQUEST_TIMEOUT_SECONDS,
        50 AS REQUEST_DELAY_MS,
        100000 AS MAX_PAGES_PER_STREAM,
        120 AS SAFETY_LAG_SECONDS
) S
ON T.CONFIG_ID = S.CONFIG_ID
WHEN NOT MATCHED THEN INSERT (
    CONFIG_ID,
    INITIAL_FROM_TS,
    PAGE_SIZE,
    REQUEST_TIMEOUT_SECONDS,
    REQUEST_DELAY_MS,
    MAX_PAGES_PER_STREAM,
    SAFETY_LAG_SECONDS
) VALUES (
    S.CONFIG_ID,
    S.INITIAL_FROM_TS,
    S.PAGE_SIZE,
    S.REQUEST_TIMEOUT_SECONDS,
    S.REQUEST_DELAY_MS,
    S.MAX_PAGES_PER_STREAM,
    S.SAFETY_LAG_SECONDS
);

{% endmacro %}