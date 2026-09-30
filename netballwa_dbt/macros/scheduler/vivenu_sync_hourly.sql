{% macro create_task_vivenu_hourly_sync(database, schema, sproc_name) %}

{% set sql %}
    CREATE OR REPLACE TASK {{database}}.{{schema}}.TASK_VIVENU_HOURLY
        WAREHOUSE = VIVENU_INGEST_WH
        SCHEDULE = '60 MINUTES'
        OVERLAP_POLICY = NO_OVERLAP
        USER_TASK_TIMEOUT_MS = 14400000
        SUSPEND_TASK_AFTER_NUM_FAILURES = 3
        AUTOCOMMIT = TRUE
    AS
        CALL {{database}}.{{schema}}.SP_VIVENU_SYNC(
            NULL, NULL, NULL, FALSE
        );

{% endset %}

{% do run_query(sql) %}
{% do log("Task TASK_VIVENU_HOURLY created successfully in " ~ database ~ "." ~ schema, info=True) %}

{% endmacro %}