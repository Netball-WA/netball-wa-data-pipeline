{% macro vivenu_sync_control(database, schema) %}

{% set sql %}
    MERGE INTO {{ database }}.{{ schema }}.VIVENU_SYNC_CONTROL T
    USING (
        SELECT STREAM_NAME
        FROM {{ database }}.{{ schema }}.VIVENU_STREAM_CONFIG
    ) S
    ON T.STREAM_NAME = S.STREAM_NAME
    WHEN NOT MATCHED THEN INSERT (STREAM_NAME)
    VALUES (S.STREAM_NAME);

    MERGE INTO {{ database }}.{{ schema }}.VIVENU_SYNC_LOCK T
    USING (SELECT 1 AS LOCK_ID) S
    ON T.LOCK_ID = S.LOCK_ID
    WHEN NOT MATCHED THEN INSERT (LOCK_ID)
    VALUES (S.LOCK_ID);
{% endset %}

{% do run_query(sql) %}
{% do log("Sync control and sync lock initialized in " ~ database ~ "." ~ schema, info=True) %}

{% endmacro %}