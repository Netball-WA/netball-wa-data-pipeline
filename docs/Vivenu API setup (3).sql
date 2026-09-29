-- =====================================================================
-- VIVENU -> SNOWFLAKE NATIVE INGESTION
-- Fresh installation | Configuration-driven endpoint registry
--
-- No earlier Vivenu setup or migration script is required.
--
-- BEFORE RUNNING
--   1. Replace <VIVENU_API_KEY> below.
--   2. Review INITIAL_FROM_TS in section 3.
--   3. Review enabled streams in section 4.
--   4. Ensure the account permits the Python packages used below.
--
-- DEFAULT OBJECTS
--   Database:     RAW_INGESTION
--   Schema:       VIVENU_NATIVE
--   Warehouse:    VIVENU_INGEST_WH
--   Runtime role: VIVENU_INGEST_ROLE
--
-- SCOPE
--   One Vivenu seller / production environment per schema.
--   Do not change the credential to a different seller without separating
--   or rebuilding that seller's configuration, checkpoints and raw data.
--
-- BEHAVIOUR
--   UPDATED:    server-side update-time filtering and periodic reconciliation.
--   CREATED:    creation-time filtering, overlap and mandatory reconciliation.
--   EVENT_TIME: activity-time filtering, overlap and reconciliation.
--   SNAPSHOT:   refresh the collection at its configured interval.
--
--   Raw records are upserted by stable identity.
--   Unchanged payloads are not rewritten.
--   A stream's data merge and automatic checkpoint commit together.
--   Explicit date replays do not advance automatic checkpoints.
--   Earlier successful streams remain committed if a later stream fails.
--
-- IMPORTANT LIMITS
--   This is not a hard-delete mirror or a historical-version archive.
--   Offset pagination is not a guaranteed point-in-time source snapshot.
--   Creation-time filters do not capture every update to older records.
--   Event-time filters can miss sufficiently late-arriving activity until
--   reconciliation.
--   Lookup-only streams cover supplied IDs, not seller-wide discovery.
--   Parent-scoped streams cover IDs available in the retained parent data.
--
-- SECURITY
--   Use a seller API key with only the required read/list permissions.
--   The original core payloads can contain PII, ticket secrets and barcodes.
--   Optional-stream redaction rules are selective, not a complete PII policy.
--   No downstream read grants are added by this script.
--
-- DEPLOYMENT
--   This is a fresh-install definition, not a migration for older schemas.
--   IF NOT EXISTS does not upgrade an existing table's columns.
--   Do not run/redeploy this script while ingestion is active.
--   No API call is executed and no task is resumed during installation.
--
-- VALIDATION
--   This script has not been executed against your Snowflake account
--   or your Vivenu tenant.
-- =====================================================================


-- =====================================================================
-- 1. ADMINISTRATIVE INFRASTRUCTURE
-- =====================================================================

USE ROLE ACCOUNTADMIN;

ALTER SESSION SET AUTOCOMMIT = TRUE;
ALTER SESSION SET TIMEZONE = 'UTC';

CREATE DATABASE IF NOT EXISTS RAW_INGESTION;

CREATE SCHEMA IF NOT EXISTS RAW_INGESTION.VIVENU_NATIVE;

CREATE WAREHOUSE IF NOT EXISTS VIVENU_INGEST_WH
    WAREHOUSE_SIZE = XSMALL
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE;

CREATE OR REPLACE NETWORK RULE
    RAW_INGESTION.VIVENU_NATIVE.VIVENU_API_RULE
    MODE = EGRESS
    TYPE = HOST_PORT
    VALUE_LIST = (
        'vivenu.com',
        'portier.vivenu.com',
        'seatmap.vivenu.com',
        'marketmaker.vivenu.com'
    );

-- Replace this placeholder before installation.
-- IF NOT EXISTS preserves an existing credential on a later rerun.
-- Use ALTER SECRET for an intentional credential rotation.
CREATE SECRET IF NOT EXISTS
    RAW_INGESTION.VIVENU_NATIVE.VIVENU_API_KEY
    TYPE = GENERIC_STRING
    SECRET_STRING = 'KEY';

CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION VIVENU_API_ACCESS
    ALLOWED_NETWORK_RULES = (
        RAW_INGESTION.VIVENU_NATIVE.VIVENU_API_RULE
    )
    ALLOWED_AUTHENTICATION_SECRETS = (
        RAW_INGESTION.VIVENU_NATIVE.VIVENU_API_KEY
    )
    ENABLED = TRUE;

CREATE ROLE IF NOT EXISTS VIVENU_INGEST_ROLE;

GRANT ROLE VIVENU_INGEST_ROLE TO ROLE SYSADMIN;

GRANT USAGE ON DATABASE RAW_INGESTION
    TO ROLE VIVENU_INGEST_ROLE;

GRANT USAGE ON SCHEMA RAW_INGESTION.VIVENU_NATIVE
    TO ROLE VIVENU_INGEST_ROLE;

GRANT CREATE TABLE, CREATE VIEW, CREATE PROCEDURE, CREATE TASK
    ON SCHEMA RAW_INGESTION.VIVENU_NATIVE
    TO ROLE VIVENU_INGEST_ROLE;

GRANT USAGE ON WAREHOUSE VIVENU_INGEST_WH
    TO ROLE VIVENU_INGEST_ROLE;

GRANT USAGE ON INTEGRATION VIVENU_API_ACCESS
    TO ROLE VIVENU_INGEST_ROLE;

GRANT READ ON SECRET
    RAW_INGESTION.VIVENU_NATIVE.VIVENU_API_KEY
    TO ROLE VIVENU_INGEST_ROLE;

GRANT EXECUTE TASK ON ACCOUNT
    TO ROLE VIVENU_INGEST_ROLE;


-- =====================================================================
-- 2. OPERATIONAL TABLES
-- =====================================================================

USE ROLE VIVENU_INGEST_ROLE;
USE DATABASE RAW_INGESTION;
USE SCHEMA VIVENU_NATIVE;
USE WAREHOUSE VIVENU_INGEST_WH;

ALTER SESSION SET AUTOCOMMIT = TRUE;
ALTER SESSION SET TIMEZONE = 'UTC';

-- Primary keys on these standard tables document intended uniqueness.
-- Runtime checks protect configuration/control singleton assumptions.

CREATE TABLE IF NOT EXISTS VIVENU_CONFIG (
    CONFIG_ID NUMBER NOT NULL,
    INITIAL_FROM_TS TIMESTAMP_TZ NOT NULL,
    PAGE_SIZE NUMBER NOT NULL,
    REQUEST_TIMEOUT_SECONDS NUMBER NOT NULL,
    REQUEST_DELAY_MS NUMBER NOT NULL,
    MAX_PAGES_PER_STREAM NUMBER NOT NULL,
    SAFETY_LAG_SECONDS NUMBER NOT NULL,
    CONSTRAINT VIVENU_CONFIG_PK PRIMARY KEY (CONFIG_ID)
);

CREATE TABLE IF NOT EXISTS VIVENU_STREAM_CONFIG (
    STREAM_NAME VARCHAR NOT NULL,
    ENABLED BOOLEAN NOT NULL DEFAULT FALSE,
    LOOKBACK_SECONDS NUMBER NOT NULL,
    RECONCILE_HOURS NUMBER NOT NULL,
    MIN_SYNC_INTERVAL_MINUTES NUMBER NOT NULL DEFAULT 0,
    API_SPEC VARIANT NOT NULL,
    CONSTRAINT VIVENU_STREAM_CONFIG_PK PRIMARY KEY (STREAM_NAME)
);

CREATE TABLE IF NOT EXISTS VIVENU_SYNC_CONTROL (
    STREAM_NAME VARCHAR NOT NULL,
    LAST_SUCCESS_TO_TS TIMESTAMP_TZ,
    LAST_SUCCESS_AT TIMESTAMP_TZ,
    LAST_FULL_SYNC_AT TIMESTAMP_TZ,
    LAST_RUN_ID VARCHAR,
    CONSTRAINT VIVENU_SYNC_CONTROL_PK PRIMARY KEY (STREAM_NAME)
);

CREATE TABLE IF NOT EXISTS VIVENU_SYNC_RUNS (
    RUN_ID VARCHAR NOT NULL,
    STARTED_AT TIMESTAMP_TZ NOT NULL,
    FINISHED_AT TIMESTAMP_TZ,
    STATUS VARCHAR NOT NULL,
    REQUESTED_STREAM VARCHAR,
    IS_AUTOMATIC BOOLEAN NOT NULL,
    FORCE_FULL BOOLEAN NOT NULL,
    REPLAY_FROM_TS TIMESTAMP_TZ,
    REPLAY_TO_TS TIMESTAMP_TZ,
    API_REQUESTS NUMBER,
    RECORDS_FETCHED NUMBER,
    STREAM_RESULTS VARIANT,
    ERROR_MESSAGE VARCHAR,
    CONSTRAINT VIVENU_SYNC_RUNS_PK PRIMARY KEY (RUN_ID)
);

-- Durable, non-expiring pipeline guard.
-- A forcibly terminated call can leave this held.
-- Recovery commands are included in the commented examples at the end.
CREATE TABLE IF NOT EXISTS VIVENU_SYNC_LOCK (
    LOCK_ID NUMBER NOT NULL,
    RUN_ID VARCHAR,
    ACQUIRED_AT TIMESTAMP_TZ,
    CONSTRAINT VIVENU_SYNC_LOCK_PK PRIMARY KEY (LOCK_ID)
);

-- Explicit IDs for endpoints without a documented collection read.
--
-- Example contexts:
--   fundraise_donations:        {"id":"..."}
--   fundraise_pledges:          {"id":"..."}
--   purchase_intent_strategies: {"id":"..."}
--   seating_object_status:     {"id":"...", "objectId":"..."}
--   seating_object_logs:       {"id":"...", "objectId":"..."}
CREATE TABLE IF NOT EXISTS VIVENU_LOOKUP_CONTEXTS (
    STREAM_NAME VARCHAR NOT NULL,
    CONTEXT VARIANT NOT NULL,
    ENABLED BOOLEAN NOT NULL DEFAULT TRUE
);

CREATE TABLE IF NOT EXISTS VIVENU_RAW_CURRENT (
    STREAM_NAME VARCHAR NOT NULL,
    RECORD_ID VARCHAR NOT NULL,
    SOURCE_UPDATED_AT TIMESTAMP_TZ,
    DATA VARIANT NOT NULL,
    SOURCE_CONTEXT VARIANT,
    RECORD_HASH VARCHAR NOT NULL,
    FIRST_LOADED_AT TIMESTAMP_TZ NOT NULL,
    LAST_CHANGED_AT TIMESTAMP_TZ NOT NULL,
    LAST_RUN_ID VARCHAR NOT NULL,
    CONSTRAINT VIVENU_RAW_CURRENT_PK
        PRIMARY KEY (STREAM_NAME, RECORD_ID)
);

-- Pre-created staging avoids runtime DDL inside data/checkpoint transactions.
CREATE TRANSIENT TABLE IF NOT EXISTS VIVENU_LOAD_STAGE (
    RUN_ID VARCHAR NOT NULL,
    STREAM_NAME VARCHAR NOT NULL,
    RECORD_ID VARCHAR NOT NULL,
    SOURCE_UPDATED_AT TIMESTAMP_TZ,
    PAYLOAD_JSON VARCHAR NOT NULL,
    FETCH_SEQUENCE NUMBER NOT NULL,
    SOURCE_CONTEXT_JSON VARCHAR NOT NULL
);


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

MERGE INTO VIVENU_CONFIG T
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


-- =====================================================================
-- 4. ENDPOINT REGISTRY
-- =====================================================================

-- Registry defaults interpreted by the procedure:
--
--   service        = core
--   shape          = docs
--   paging         = offset
--   keys           = ["_id"]
--   require_cursor = true
--
-- Response shapes:
--   docs / rows = object containing that array and a numeric total
--   array       = bare JSON array
--   object      = one resource object
--   document    = retain the complete JSON response for one context
--
-- Scheduling defaults:
--   UPDATED:    10-minute overlap; weekly retained-history reconciliation
--   CREATED:    7-day overlap; daily retained-history reconciliation
--   EVENT_TIME: 7-day overlap; weekly retained-history reconciliation
--   SNAPSHOT:   daily refresh unless "every" overrides it, in minutes
--
-- RECONCILE_HOURS = 0 disables reconciliation for UPDATED/EVENT_TIME.
-- CREATED streams require a positive value.
--
-- Parent definitions:
--   Parent streams must also be enabled.
--   Dependencies are processed first, subject to their refresh intervals.
--   Child snapshots use all known parent IDs, not only changed parents.
--
-- The JSON "enabled" field supplies the initial ENABLED column value.
-- Subsequently change the ENABLED column, not API_SPEC:enabled.
--
-- This seed does not overwrite existing stream configuration.

MERGE INTO VIVENU_STREAM_CONFIG T
USING (
    SELECT
        F.VALUE:name::VARCHAR AS STREAM_NAME,
        COALESCE(F.VALUE:enabled::BOOLEAN, FALSE) AS ENABLED,
        COALESCE(
            F.VALUE:lookback::NUMBER,
            IFF(F.VALUE:mode::VARCHAR = 'SNAPSHOT', 0, 600)
        ) AS LOOKBACK_SECONDS,
        COALESCE(
            F.VALUE:reconcile::NUMBER,
            IFF(F.VALUE:mode::VARCHAR = 'SNAPSHOT', 0, 168)
        ) AS RECONCILE_HOURS,
        COALESCE(
            F.VALUE:every::NUMBER,
            IFF(F.VALUE:mode::VARCHAR = 'SNAPSHOT', 1440, 0)
        ) AS MIN_SYNC_INTERVAL_MINUTES,
        F.VALUE AS API_SPEC
    FROM TABLE(
        FLATTEN(INPUT => PARSE_JSON($$
[
  {
    "name":"events",
    "enabled":true,
    "path":"/events",
    "mode":"UPDATED",
    "cursor":"updatedAt",
    "shape":"rows"
  },
  {
    "name":"customers",
    "enabled":true,
    "path":"/customers/rich",
    "mode":"UPDATED",
    "cursor":"updatedAt"
  },
  {
    "name":"transactions",
    "enabled":true,
    "path":"/transactions",
    "mode":"UPDATED",
    "cursor":"updatedAt"
  },
  {
    "name":"tickets",
    "enabled":true,
    "path":"/tickets",
    "mode":"UPDATED",
    "cursor":"updatedAt",
    "shape":"rows"
  },
  {
    "name":"subscriptions",
    "enabled":true,
    "path":"/subscriptions",
    "mode":"UPDATED",
    "cursor":"updatedAt",
    "require_cursor":false
  },
  {
    "name":"scans",
    "service":"scans",
    "path":"/scans",
    "mode":"EVENT_TIME",
    "cursor":"time",
    "lookback":600
  },

  {
    "name":"checkouts",
    "enabled":true,
    "path":"/checkouts",
    "mode":"UPDATED",
    "cursor":"updatedAt",
    "redact":[
      "secret",
      "seatingReservationToken",
      "items.*.seatingReservationToken"
    ]
  },
  {
    "name":"ticket_transfers",
    "path":"/ticket-transfers",
    "mode":"UPDATED",
    "cursor":"updatedAt"
  },
  {
    "name":"invoices",
    "enabled":true,
    "path":"/invoices",
    "mode":"CREATED",
    "cursor":"createdAt",
    "shape":"rows",
    "lookback":600,
    "reconcile":24,
    "redact":["secret"]
  },
  {
    "name":"payment_requests",
    "enabled":true,
    "path":"/payments/requests",
    "mode":"CREATED",
    "cursor":"createdAt",
    "lookback":600,
    "reconcile":24,
    "redact":["secret","processors.*.data","history.*.data"]
  },
  {
    "name":"purchase_intents",
    "enabled":true,
    "path":"/purchaseintents",
    "mode":"CREATED",
    "cursor":"createdAt",
    "from_param":"createdAtStart",
    "to_param":"createdAtEnd",
    "allow_upper_boundary":true,
    "shape":"rows",
    "lookback":600,
    "reconcile":24,
    "redact":["secret","seatingReservationToken"]
  },

  {
    "name":"coupons",
    "path":"/coupon/rich",
    "mode":"SNAPSHOT",
    "shape":"rows",
    "every":360
  },
  {
    "name":"coupon_series",
    "path":"/coupon/series",
    "mode":"SNAPSHOT"
  },
  {
    "name":"vouchers",
    "path":"/vouchers",
    "mode":"SNAPSHOT",
    "keys":["code"],
    "every":360
  },
  {
    "name":"products",
    "path":"/products/rich",
    "mode":"SNAPSHOT"
  },
  {
    "name":"product_streams",
    "path":"/products/streams/rich",
    "mode":"SNAPSHOT"
  },
  {
    "name":"product_stream_products",
    "path":"/products/streams/{id}/products",
    "mode":"SNAPSHOT",
    "shape":"array",
    "paging":"none",
    "parent":{
      "stream":"product_streams",
      "fields":{"id":"_id"}
    }
  },
  {
    "name":"price_tables",
    "enabled":true,
    "path":"/price-tables",
    "mode":"SNAPSHOT"
  },
  {
    "name":"data_fields",
    "enabled":true,
    "path":"/data-fields",
    "mode":"SNAPSHOT",
    "shape":"array",
    "paging":"none"
  },
  {
    "name":"sales_channels",
    "enabled":true,
    "path":"/sales-channels",
    "mode":"SNAPSHOT"
  },
  {
    "name":"sales_channel_groups",
    "enabled":true,
    "path":"/sales-channels/groups",
    "mode":"SNAPSHOT"
  },

  {
    "name":"pos_devices",
    "path":"/pos",
    "mode":"SNAPSHOT",
    "shape":"array",
    "redact":["restrictions.code"]
  },
  {
    "name":"pos_sessions",
    "path":"/pos/{id}/sessions",
    "mode":"SNAPSHOT",
    "shape":"array",
    "paging":"none",
    "every":360,
    "parent":{
      "stream":"pos_devices",
      "fields":{"id":"_id"}
    }
  },
  {
    "name":"pos_journal_logs",
    "path":"/pos/{id}/logs",
    "mode":"SNAPSHOT",
    "shape":"rows",
    "every":360,
    "parent":{
      "stream":"pos_devices",
      "fields":{"id":"_id"}
    }
  },
  {
    "name":"webhooks",
    "path":"/webhooks",
    "mode":"SNAPSHOT",
    "redact":["hmacKey"]
  },

  {
    "name":"fundraise_campaigns",
    "path":"/fundraise/campaigns",
    "mode":"SNAPSHOT"
  },
  {
    "name":"fundraise_funds",
    "path":"/fundraise/funds",
    "mode":"SNAPSHOT"
  },
  {
    "name":"fundraise_donations",
    "path":"/fundraise/donations/{id}",
    "mode":"SNAPSHOT",
    "shape":"object",
    "paging":"none",
    "lookup":true
  },
  {
    "name":"fundraise_pledges",
    "path":"/fundraise/pledges/{id}",
    "mode":"SNAPSHOT",
    "shape":"object",
    "paging":"none",
    "lookup":true
  },
  {
    "name":"purchase_intent_strategies",
    "path":"/purchaseintents/strategies/{id}",
    "mode":"SNAPSHOT",
    "shape":"object",
    "paging":"none",
    "keys":[],
    "lookup":true
  },

  {
    "name":"payment_gateways",
    "path":"/payments/gateways",
    "mode":"SNAPSHOT",
    "shape":"array",
    "paging":"none",
    "redact":["secret","data"]
  },
  {
    "name":"customer_payment_methods",
    "enabled":true,
    "path":"/customers/{id}/payment-methods",
    "mode":"SNAPSHOT",
    "shape":"array",
    "paging":"none",
    "parent":{
      "stream":"customers",
      "fields":{"id":"_id"}
    },
    "redact":["secret","token","data","paymentDetails"]
  },

  {
    "name":"access_users",
    "path":"/accessusers",
    "mode":"SNAPSHOT",
    "shape":"array",
    "paging":"none",
    "redact":["token"]
  },
  {
    "name":"access_lists",
    "path":"/access-lists",
    "mode":"SNAPSHOT",
    "redact":["http.hmacKey"]
  },
  {
    "name":"access_list_entries",
    "path":"/access-lists/{listId}/entries",
    "mode":"SNAPSHOT",
    "every":360,
    "parent":{
      "stream":"access_lists",
      "fields":{"listId":"_id"}
    }
  },
  {
    "name":"scan_groups",
    "path":"/scan-groups",
    "mode":"SNAPSHOT"
  },

  {
    "name":"seating_events",
    "service":"seating",
    "path":"/event",
    "mode":"SNAPSHOT",
    "shape":"array",
    "paging":"none"
  },
  {
    "name":"seating_contingents",
    "service":"seating",
    "path":"/event/{id}/contingents",
    "mode":"SNAPSHOT",
    "shape":"document",
    "paging":"none",
    "every":360,
    "parent":{
      "stream":"seating_events",
      "fields":{"id":"_id"}
    }
  },
  {
    "name":"seatmap_revisions",
    "service":"seating",
    "path":"/seatmap/{mapId}/revision",
    "mode":"SNAPSHOT",
    "shape":"document",
    "paging":"none",
    "parent":{
      "stream":"seating_events",
      "fields":{"mapId":"seatMapId"}
    }
  },
  {
    "name":"seatmap_revision_details",
    "service":"seating",
    "path":"/seatmap/{mapId}/revision/{revisionId}",
    "mode":"SNAPSHOT",
    "shape":"document",
    "paging":"none",
    "params":{"includeSeatmap":true},
    "parent":{
      "stream":"seating_events",
      "fields":{
        "mapId":"seatMapId",
        "revisionId":"revisionId"
      }
    }
  },
  {
    "name":"seating_object_status",
    "service":"seating",
    "path":"/event/{id}/status/{objectId}",
    "mode":"SNAPSHOT",
    "shape":"object",
    "paging":"none",
    "keys":[],
    "lookup":true,
    "every":60
  },
  {
    "name":"seating_object_logs",
    "service":"seating",
    "path":"/event/{id}/status/{objectId}/logs",
    "mode":"SNAPSHOT",
    "shape":"document",
    "paging":"none",
    "lookup":true,
    "every":360
  },

  {
    "name":"dynamic_prices",
    "service":"pricing",
    "path":"/pricing/prices",
    "mode":"SNAPSHOT",
    "every":60
  }
]
$$))
    ) F
) S
ON T.STREAM_NAME = S.STREAM_NAME
WHEN NOT MATCHED THEN INSERT (
    STREAM_NAME,
    ENABLED,
    LOOKBACK_SECONDS,
    RECONCILE_HOURS,
    MIN_SYNC_INTERVAL_MINUTES,
    API_SPEC
) VALUES (
    S.STREAM_NAME,
    S.ENABLED,
    S.LOOKBACK_SECONDS,
    S.RECONCILE_HOURS,
    S.MIN_SYNC_INTERVAL_MINUTES,
    S.API_SPEC
);

MERGE INTO VIVENU_SYNC_CONTROL T
USING (
    SELECT STREAM_NAME
    FROM VIVENU_STREAM_CONFIG
) S
ON T.STREAM_NAME = S.STREAM_NAME
WHEN NOT MATCHED THEN INSERT (STREAM_NAME)
VALUES (S.STREAM_NAME);

MERGE INTO VIVENU_SYNC_LOCK T
USING (SELECT 1 AS LOCK_ID) S
ON T.LOCK_ID = S.LOCK_ID
WHEN NOT MATCHED THEN INSERT (LOCK_ID)
VALUES (S.LOCK_ID);


-- =====================================================================
-- 5. CONFIGURATION-DRIVEN INGESTION PROCEDURE
-- =====================================================================

CREATE OR REPLACE PROCEDURE SP_VIVENU_SYNC(
    P_STREAM VARCHAR,
    P_FROM_TS TIMESTAMP_TZ,
    P_TO_TS TIMESTAMP_TZ,
    P_FORCE_FULL BOOLEAN
)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.12'
PACKAGES = ('snowflake-snowpark-python', 'requests')
HANDLER = 'main'
EXTERNAL_ACCESS_INTEGRATIONS = (VIVENU_API_ACCESS)
SECRETS = (
    'vivenu_key' = RAW_INGESTION.VIVENU_NATIVE.VIVENU_API_KEY
)
EXECUTE AS OWNER
AS
$$
import copy
import hashlib
import json
import random
import re
import time
import uuid

from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from urllib.parse import quote

import _snowflake
import requests


SCHEMA = "RAW_INGESTION.VIVENU_NATIVE"

# Fixed host allowlist, separate from editable endpoint metadata.
BASES = {
    "core": "https://vivenu.com/api",
    "scans": "https://portier.vivenu.com/api",
    "seating": "https://seatmap.vivenu.com/api",
    "pricing": "https://marketmaker.vivenu.com/api",
}

MODES = {"UPDATED", "CREATED", "EVENT_TIME", "SNAPSHOT"}
SHAPES = {"docs", "rows", "array", "object", "document"}

# Applies to one encoded SQL staging batch, not the source page size.
# Oversized individual documents fail explicitly rather than truncate.
STAGE_BATCH_BYTES = 4_000_000


class SyncError(Exception):
    pass


def canonical(value):
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    )


def json_value(value):
    return json.loads(value) if isinstance(value, str) else value


def utc(value):
    if isinstance(value, str):
        try:
            value = datetime.fromisoformat(
                value.replace("Z", "+00:00")
            )
        except ValueError:
            raise SyncError("Invalid ISO timestamp")

    if not isinstance(value, datetime) or value.tzinfo is None:
        raise SyncError("Expected a timezone-aware timestamp")

    return value.astimezone(timezone.utc)


def millis(value):
    value = utc(value)
    return value.replace(
        microsecond=(value.microsecond // 1000) * 1000
    )


def iso(value):
    return utc(value).isoformat() if value is not None else None


def get_path(value, path):
    for part in path.split("."):
        if not isinstance(value, dict):
            return None
        value = value.get(part)
    return value


def remove_path(value, parts):
    """Remove configured fields, including paths through arrays."""
    if not parts:
        return

    head, tail = parts[0], parts[1:]

    if head == "*":
        children = (
            value if isinstance(value, list)
            else value.values() if isinstance(value, dict)
            else []
        )
        for child in children:
            remove_path(child, tail)

    elif isinstance(value, dict) and head in value:
        if tail:
            remove_path(value[head], tail)
        else:
            del value[head]


def safe_error(exc):
    # Do not persist HTTP bodies, source payloads or credentials in errors.
    if isinstance(exc, SyncError):
        return str(exc)[:4000]
    return type(exc).__name__ + ": inspect Snowflake query history"


def main(session, p_stream, p_from_ts, p_to_ts, p_force_full):
    def q(sql, params=None):
        return session.sql(sql, params=params or []).collect()

    run_id = str(uuid.uuid4())
    requested = (
        str(p_stream).strip().lower()
        if p_stream is not None else None
    )
    automatic = p_from_ts is None and p_to_ts is None
    force_full = bool(p_force_full)

    if (p_from_ts is None) != (p_to_ts is None):
        raise SyncError("Provide both replay dates, or neither")

    if not automatic and force_full:
        raise SyncError("Replay dates cannot be combined with FORCE_FULL")

    config_rows = q(
        f"SELECT * FROM {SCHEMA}.VIVENU_CONFIG WHERE CONFIG_ID = 1"
    )
    if len(config_rows) != 1:
        raise SyncError("Expected exactly one configuration row")

    config = config_rows[0].as_dict()
    initial = millis(config["INITIAL_FROM_TS"])
    page_size = int(config["PAGE_SIZE"])
    timeout = int(config["REQUEST_TIMEOUT_SECONDS"])
    delay = int(config["REQUEST_DELAY_MS"]) / 1000.0
    max_pages = int(config["MAX_PAGES_PER_STREAM"])
    safety_lag = int(config["SAFETY_LAG_SECONDS"])

    if not 1 <= page_size <= 100:
        raise SyncError("PAGE_SIZE must be between 1 and 100")

    if timeout <= 0 or max_pages <= 0 or delay < 0 or safety_lag < 0:
        raise SyncError("Invalid request limits")

    configs = {}
    for row in q(f"SELECT * FROM {SCHEMA}.VIVENU_STREAM_CONFIG"):
        item = row.as_dict()
        name = item["STREAM_NAME"]

        if name in configs:
            raise SyncError("Duplicate stream configuration")

        spec = json_value(item["API_SPEC"])
        if not isinstance(spec, dict):
            raise SyncError(f"{name}: API_SPEC must be a JSON object")

        item["SPEC"] = spec
        configs[name] = item            # {"events": {"cursor": "updatedAt","enabled": true,"mode": "UPDATED","name": "events","path": "/events","shape": "rows"}}

    roots = (
        [requested] if requested is not None
        else sorted(
            name for name, item in configs.items()
            if item["ENABLED"]      # vivenu_stream_config enabled is true
        )
    )
    if not roots:
        raise SyncError("No enabled streams")

    ordered = []
    visiting = set()
    visited = set()

    def visit(name):
        if name in visited:
            return

        if name in visiting:
            raise SyncError("Circular parent-stream dependency")

        if name not in configs or not configs[name]["ENABLED"]:
            raise SyncError(
                f"{name}: stream or required parent is not enabled"
            )

        spec = configs[name]["SPEC"]

        visiting.add(name)
        parent = spec.get("parent")     ## where to get parent?
        if parent:
            visit(parent["stream"])
        visiting.remove(name)

        visited.add(name)
        ordered.append(name)

    for name in roots:
        visit(name)

    # A snapshot cannot reconstruct historical source state.
    # Date replays are restricted to one root date-filtered stream.
    if not automatic:
        if requested is None:
            raise SyncError("A date replay requires a specific P_STREAM")

        spec = configs[requested]["SPEC"]
        if (
            spec.get("mode") == "SNAPSHOT"
            or spec.get("parent")
            or spec.get("lookup")
        ):
            raise SyncError(
                "Replay dates require a root date-filtered stream"
            )

    http = None
    locked = False
    logged = False
    transaction_open = False
    current_stream = None
    api_requests = 0
    results = {}
    error = None
    cleanup_errors = []

    def validate_spec(name, spec):
        if spec.get("blocked"):
            raise SyncError(f"{name}: {spec['blocked']}")

        mode = spec.get("mode")
        shape = spec.get("shape", "docs")
        paging = spec.get("paging", "offset")
        path = spec.get("path", "")

        if mode not in MODES or shape not in SHAPES:
            raise SyncError(
                f"{name}: unsupported mode or response shape"
            )

        if spec.get("service", "core") not in BASES:
            raise SyncError(f"{name}: unsupported API service")

        if paging not in {"offset", "none"}:
            raise SyncError(f"{name}: unsupported pagination")

        if not re.fullmatch(r"/[A-Za-z0-9_/{\}.-]+", path):
            raise SyncError(f"{name}: invalid endpoint path")

        if (
            "//" in path
            or any(part in {".", ".."} for part in path.split("/"))
        ):
            raise SyncError(f"{name}: invalid relative endpoint path")

        if shape in {"object", "document"} and paging != "none":
            raise SyncError(
                f"{name}: object/document reads cannot be paged"
            )

        if spec.get("parent") and spec.get("lookup"):
            raise SyncError(
                f"{name}: use either parent or lookup scope, not both"
            )

        if mode != "SNAPSHOT":
            cursor = spec.get("cursor")
            if not isinstance(cursor, str) or not cursor:
                raise SyncError(f"{name}: missing cursor definition")

            if spec.get("parent") or spec.get("lookup"):
                raise SyncError(
                    f"{name}: scoped date cursors require "
                    "separate per-scope checkpoints"
                )

        conf = configs[name]
        if (
            int(conf["LOOKBACK_SECONDS"]) < 0
            or int(conf["RECONCILE_HOURS"]) < 0
            or int(conf["MIN_SYNC_INTERVAL_MINUTES"]) < 0
        ):
            raise SyncError(
                f"{name}: invalid scheduling configuration"
            )

        if mode == "CREATED" and int(conf["RECONCILE_HOURS"]) <= 0:
            raise SyncError(
                f"{name}: creation-time polling requires a positive "
                "RECONCILE_HOURS to recover changes to older records"
            )

    def request_json(spec, path, params):
        nonlocal api_requests

        url = BASES[spec.get("service", "core")] + path

        for attempt in range(6):
            if delay:
                time.sleep(delay)

            retry_after = None
            reason = None

            try:
                api_requests += 1
                response = http.get(
                    url,
                    params=params,
                    timeout=(10, timeout),
                    allow_redirects=False,
                )

            except (requests.Timeout, requests.ConnectionError):
                reason = "connection failure or timeout"

            else:
                try:
                    status = response.status_code

                    if status == 429 or 500 <= status < 600:
                        reason = f"HTTP {status}"
                        retry_after = response.headers.get("Retry-After")

                    elif status != 200:
                        # Includes 401/403/404: never treat these as
                        # a successful empty collection.
                        raise SyncError(
                            f"{current_stream}: HTTP {status}; check "
                            "permissions, module access and source scope"
                        )

                    else:
                        try:
                            payload = response.json()
                        except ValueError:
                            raise SyncError(
                                f"{current_stream}: response is not JSON"
                            )

                        if not isinstance(payload, (dict, list)):
                            raise SyncError(
                                f"{current_stream}: unexpected JSON root"
                            )

                        return payload

                finally:
                    response.close()

            if attempt == 5:
                raise SyncError(
                    f"{current_stream}: retries exhausted after {reason}"
                )

            wait = min(2 ** (attempt + 1), 60) + random.random()

            if retry_after:
                try:
                    required = float(retry_after)
                except (TypeError, ValueError):
                    try:
                        required = (
                            utc(parsedate_to_datetime(retry_after))
                            - datetime.now(timezone.utc)
                        ).total_seconds()
                    except Exception:
                        required = 0

                wait = max(wait, required)

            # Do not retry earlier than a long Retry-After instruction.
            # Fail this run instead, preserving its checkpoint.
            if wait > 300:
                raise SyncError(
                    f"{current_stream}: Retry-After exceeds retry budget"
                )

            time.sleep(max(wait, 0))

        raise SyncError("Request failed")

    def scopes(name, spec):
        """Yield path contexts without collecting all parents in memory."""
        parent = spec.get("parent")

        if parent:
            parent_name = parent["stream"]
            fields = parent["fields"]

            if not isinstance(fields, dict) or not fields:
                raise SyncError(
                    f"{name}: invalid parent field mapping"
                )

            keys = list(fields)
            expressions = [
                f"GET_PATH(DATA, ?)::VARCHAR AS P{i}"
                for i in range(len(keys))
            ]
            nonnull = " AND ".join(
                f"P{i} IS NOT NULL AND P{i} NOT IN ('', 'null')"
                for i in range(len(keys))
            )

            sql = f"""
                SELECT DISTINCT *
                FROM (
                    SELECT {", ".join(expressions)}
                    FROM {SCHEMA}.VIVENU_RAW_CURRENT
                    WHERE STREAM_NAME = ?
                )
                WHERE {nonnull}
            """
            params = [fields[key] for key in keys] + [parent_name]      # ?? parent?

            for row in session.sql(
                sql, params=params
            ).to_local_iterator():
                yield {
                    key: str(row[i])
                    for i, key in enumerate(keys)
                }

        elif spec.get("lookup"):
            sql = f"""
                SELECT DISTINCT TO_JSON(CONTEXT) AS CONTEXT_JSON
                FROM {SCHEMA}.VIVENU_LOOKUP_CONTEXTS
                WHERE STREAM_NAME = ? AND ENABLED
            """

            for row in session.sql(
                sql, params=[name]
            ).to_local_iterator():
                context = json_value(row[0])

                if not isinstance(context, dict):
                    raise SyncError(
                        f"{name}: lookup context must be an object"
                    )

                yield context

        else:
            yield {}

    def format_path(name, spec, context):
        required = set(re.findall(r"\{(\w+)\}", spec["path"]))

        if set(context) != required:
            raise SyncError(
                f"{name}: context keys do not match path placeholders"
            )

        for value in context.values():
            if (
                not isinstance(value, str)
                or not value
                or value in {".", ".."}
                or any(ord(ch) < 32 for ch in value)
            ):
                raise SyncError(
                    f"{name}: invalid context identifier"
                )

        return re.sub(
            r"\{(\w+)\}",
            lambda match: quote(
                context[match.group(1)], safe=""
            ),
            spec["path"],
        )

    def base_params(spec):
        output = {}

        for key, value in spec.get("params", {}).items():
            if value is None:
                continue

            if isinstance(value, bool):
                output[key] = str(value).lower()

            elif isinstance(value, list):
                array_key = (
                    key if key.endswith("[]") else key + "[]"
                )
                output[array_key] = value

            elif isinstance(value, (str, int, float)):
                output[key] = value

            else:
                raise SyncError(
                    "Static query parameters must be scalar or array"
                )

        return output

    def record_id(spec, record, context):
        if spec.get("shape") == "document":
            parts = []
        else:
            parts = [
                get_path(record, field)
                for field in spec.get("keys", ["_id"])
            ]

        for part in parts:
            if (
                isinstance(part, bool)
                or not isinstance(part, (str, int))
                or str(part) == ""
            ):
                raise SyncError(
                    f"{current_stream}: configured record key is missing"
                )

        if context:
            # Include parent/lookup scope in the stable identity.
            # Do not use mutable payload content as the identity.
            return "ctx:" + hashlib.sha256(
                canonical([context, parts]).encode("utf-8")
            ).hexdigest()

        if len(parts) == 1:
            return str(parts[0])

        if not parts:
            raise SyncError(
                "A document without a source ID requires context"
            )

        return "key:" + hashlib.sha256(
            canonical(parts).encode("utf-8")
        ).hexdigest()

    def write_batch(name, rows):
        if not rows:
            return

        q(
            f"""
            INSERT INTO {SCHEMA}.VIVENU_LOAD_STAGE (
                RUN_ID,
                STREAM_NAME,
                RECORD_ID,
                SOURCE_UPDATED_AT,
                PAYLOAD_JSON,
                FETCH_SEQUENCE,
                SOURCE_CONTEXT_JSON
            )
            SELECT
                ?,
                ?,
                F.VALUE:rid::VARCHAR,
                TRY_TO_TIMESTAMP_TZ(F.VALUE:u::VARCHAR),
                F.VALUE:p::VARCHAR,
                F.VALUE:n::NUMBER,
                F.VALUE:c::VARCHAR
            FROM TABLE(
                FLATTEN(INPUT => PARSE_JSON(?))
            ) F
            """,
            [run_id, name, "[" + ",".join(rows) + "]"],
        )

    def stage_page(
        name, spec, records, context, lower, upper, stats
    ):
        cursor = spec.get("cursor")
        shape = spec.get("shape", "docs")
        sequence = stats["fetched"]
        stats["fetched"] += len(records)

        batch = []
        batch_bytes = 2
        keys = []

        for index, record in enumerate(records):
            if shape != "document" and not isinstance(record, dict):
                raise SyncError(
                    f"{name}: expected resource objects"
                )

            rid = record_id(spec, record, context)
            keys.append(rid)

            if (
                shape == "object"
                and set(context) == {"id"}
                and isinstance(record, dict)
                and record.get("_id") is not None
                and record["_id"] != context["id"]
            ):
                raise SyncError(
                    f"{name}: lookup returned a different source ID"
                )

            if cursor and lower is not None:
                value = get_path(record, cursor)

                if value is None:
                    if spec.get("require_cursor", True):
                        raise SyncError(
                            f"{name}: returned cursor is missing"
                        )
                    stats["records_without_returned_cursor"] += 1

                else:
                    stamp = utc(value)
                    upper_ok = (
                        stamp <= upper
                        if spec.get("allow_upper_boundary", False)
                        else stamp < upper
                    )

                    if stamp < lower or not upper_ok:
                        raise SyncError(
                            f"{name}: record outside requested cursor "
                            "window; verify the filter and retry"
                        )

            source_updated = (
                record.get("updatedAt")
                if isinstance(record, dict) and shape != "document"
                else None
            )

            if source_updated is not None:
                source_updated = iso(utc(source_updated))

            stored_record = copy.deepcopy(record)
            for path in spec.get("redact", []):
                remove_path(stored_record, path.split("."))

            encoded = canonical({
                "rid": rid,
                "u": source_updated,
                "p": canonical(stored_record),
                "n": sequence + index,
                "c": canonical(context),
            })

            size = len(encoded) + 1

            if size + 2 > STAGE_BATCH_BYTES:
                raise SyncError(
                    f"{name}: one resource/document exceeds the "
                    "staging batch limit; use a larger-object adapter"
                )

            if batch and batch_bytes + size > STAGE_BATCH_BYTES:
                write_batch(name, batch)
                batch = []
                batch_bytes = 2

            batch.append(encoded)
            batch_bytes += size

        write_batch(name, batch)
        return keys

    def fetch_scope(name, spec, context, lower, upper, stats):
        path = format_path(name, spec, context)
        params = base_params(spec)
        shape = spec.get("shape", "docs")
        paging = spec.get("paging", "offset")

        if lower is not None:
            cursor = spec["cursor"]
            params[
                spec.get("from_param", cursor + "[$gte]")
            ] = iso(lower)
            params[
                spec.get("to_param", cursor + "[$lt]")
            ] = iso(upper)

        offset = 0
        expected_total = None
        fingerprints = set()

        while True:
            if stats["pages"] >= max_pages:
                raise SyncError(
                    f"{name}: per-stream page limit reached"
                )

            query = dict(params)
            if paging == "offset":
                query.update({"top": page_size, "skip": offset})

            stats["pages"] += 1
            payload = request_json(spec, path, query)
            total = None

            if shape in {"docs", "rows"}:
                if (
                    not isinstance(payload, dict)
                    or not isinstance(payload.get(shape), list)
                ):
                    raise SyncError(
                        f"{name}: expected a '{shape}' array"
                    )

                records = payload[shape]
                total = payload.get("total")

                if (
                    isinstance(total, bool)
                    or not isinstance(total, (int, float))
                    or total < 0
                    or int(total) != total
                ):
                    raise SyncError(
                        f"{name}: invalid or missing total"
                    )

                total = int(total)

                if expected_total is None:
                    expected_total = total
                elif total != expected_total:
                    raise SyncError(
                        f"{name}: total changed during pagination; retry"
                    )

            elif shape == "array":
                if not isinstance(payload, list):
                    raise SyncError(
                        f"{name}: expected a bare array"
                    )
                records = payload

            elif shape == "object":
                if not isinstance(payload, dict):
                    raise SyncError(
                        f"{name}: expected one resource object"
                    )
                records = [payload]

            else:
                # One stored document per context, including an empty
                # array/object response when that is what the API returns.
                records = [payload]

            if total is not None and offset + len(records) > total:
                raise SyncError(
                    f"{name}: page exceeds reported total"
                )

            if not records:
                if total is not None and offset != total:
                    raise SyncError(
                        f"{name}: pagination ended early"
                    )
                return

            ids = stage_page(
                name, spec, records, context, lower, upper, stats
            )

            fingerprint = hashlib.sha256(
                canonical(ids).encode("utf-8")
            ).hexdigest()

            if fingerprint in fingerprints:
                raise SyncError(
                    f"{name}: repeated page detected"
                )
            fingerprints.add(fingerprint)

            offset += len(records)

            if paging == "none":
                if total is not None and offset != total:
                    raise SyncError(
                        f"{name}: incomplete response; pagination required"
                    )
                return

            if total is not None and offset == total:
                return

            # With no total, continue until an EMPTY page.
            # A short page may reflect a server-side page-size cap.

    try:
        lock_rows = q(
            f"SELECT LOCK_ID FROM {SCHEMA}.VIVENU_SYNC_LOCK "
            "WHERE LOCK_ID = 1"
        )
        if len(lock_rows) != 1:
            raise SyncError(
                "Expected exactly one pipeline lock row"
            )

        q("BEGIN TRANSACTION")
        transaction_open = True

        changed = q(
            f"""
            UPDATE {SCHEMA}.VIVENU_SYNC_LOCK
            SET RUN_ID = ?, ACQUIRED_AT = CURRENT_TIMESTAMP()
            WHERE LOCK_ID = 1 AND RUN_ID IS NULL
            """,
            [run_id],
        )

        if int(changed[0][0]) != 1:
            raise SyncError(
                "Another run is active, or a stopped run left a stale lock"
            )

        q("COMMIT")
        transaction_open = False
        locked = True

        now = millis(q("SELECT CURRENT_TIMESTAMP()")[0][0])
        replay_from = millis(p_from_ts) if not automatic else None
        replay_to = millis(p_to_ts) if not automatic else None
        cutoff = now - timedelta(seconds=safety_lag)

        if not automatic:
            if replay_from >= replay_to or replay_to > now:
                raise SyncError("Invalid replay range")
        elif initial >= cutoff:
            raise SyncError(
                "INITIAL_FROM_TS must precede the run cutoff"
            )

        q(
            f"""
            INSERT INTO {SCHEMA}.VIVENU_SYNC_RUNS (
                RUN_ID,
                STARTED_AT,
                STATUS,
                REQUESTED_STREAM,
                IS_AUTOMATIC,
                FORCE_FULL,
                REPLAY_FROM_TS,
                REPLAY_TO_TS
            )
            SELECT
                ?,
                CURRENT_TIMESTAMP(),
                'RUNNING',
                ?,
                ?,
                ?,
                TRY_TO_TIMESTAMP_TZ(?),
                TRY_TO_TIMESTAMP_TZ(?)
            """,
            [
                run_id,
                requested,
                automatic,
                force_full,
                iso(replay_from),
                iso(replay_to),
            ],
        )
        logged = True

        # Reject invalid enabled definitions before making API calls.
        for name in ordered:
            validate_spec(name, configs[name]["SPEC"])

        token = _snowflake.get_generic_secret_string(
            "vivenu_key"
        ).strip()

        if not token or token.startswith("<"):
            raise SyncError(
                "The Vivenu API secret has not been configured"
            )

        http = requests.Session()
        http.headers.update({
            "Authorization": "Bearer " + token,
            "Accept": "application/json",
            "User-Agent": "snowflake-vivenu-native/2.1",
        })

        for name in ordered:
            current_stream = name
            conf = configs[name]
            spec = conf["SPEC"]

            stats = {
                "status": "RUNNING",
                "mode": spec["mode"],
                "pages": 0,
                "scopes": 0,
                "fetched": 0,
                "inserted": 0,
                "updated": 0,
                "records_without_returned_cursor": 0,
                "watermark_advanced": False,
            }
            results[name] = stats

            control_rows = q(
                f"SELECT * FROM {SCHEMA}.VIVENU_SYNC_CONTROL "
                "WHERE STREAM_NAME = ?",
                [name],
            )

            if len(control_rows) != 1:
                raise SyncError(
                    f"{name}: expected one control row"
                )

            control = control_rows[0].as_dict()

            previous = control["LAST_SUCCESS_TO_TS"]
            last_success = control["LAST_SUCCESS_AT"]
            last_full = control["LAST_FULL_SYNC_AT"]

            previous = (
                utc(previous) if previous is not None else None
            )
            last_success = (
                utc(last_success) if last_success is not None else None
            )
            last_full = (
                utc(last_full) if last_full is not None else None
            )

            snapshot = spec["mode"] == "SNAPSHOT"
            reconcile_hours = int(conf["RECONCILE_HOURS"])
            interval = int(conf["MIN_SYNC_INTERVAL_MINUTES"])

            reconciliation_due = (
                not snapshot
                and reconcile_hours > 0
                and (
                    last_full is None
                    or now - last_full
                    >= timedelta(hours=reconcile_hours)
                )
            )

            # An explicitly requested stream bypasses its minimum interval.
            # FORCE_FULL also bypasses dependency refresh intervals.
            if (
                automatic
                and not force_full
                and name != requested
                and not reconciliation_due
                and last_success is not None
                and interval > 0
                and now - last_success < timedelta(minutes=interval)
            ):
                stats["status"] = "NOT_DUE"
                continue

            full = automatic and (
                snapshot
                or force_full
                or previous is None
                or reconciliation_due
            )

            if snapshot:
                lower = upper = None

            elif not automatic:
                lower, upper = replay_from, replay_to

            else:
                upper = cutoff
                lower = (
                    initial if full
                    else max(
                        initial,
                        previous - timedelta(
                            seconds=int(conf["LOOKBACK_SECONDS"])
                        ),
                    )
                )

            stats.update({
                "from": iso(lower),
                "to": iso(upper),
                "full_refresh": full,
            })

            if lower is not None and lower >= upper:
                stats["status"] = "NO_NEW_WINDOW"
                continue

            for context in scopes(name, spec):
                stats["scopes"] += 1
                fetch_scope(
                    name, spec, context, lower, upper, stats
                )

            if spec.get("lookup") and stats["scopes"] == 0:
                raise SyncError(
                    f"{name}: no enabled lookup contexts supplied"
                )

            staged = q(
                f"""
                SELECT COUNT(*), COUNT(DISTINCT RECORD_ID)
                FROM {SCHEMA}.VIVENU_LOAD_STAGE
                WHERE RUN_ID = ? AND STREAM_NAME = ?
                """,
                [run_id, name],
            )[0]

            if (
                int(staged[0]) != stats["fetched"]
                or int(staged[1]) != stats["fetched"]
            ):
                raise SyncError(
                    f"{name}: duplicate identities or incomplete staging"
                )

            # The merge and automatic checkpoint form one transaction.
            q("BEGIN TRANSACTION")
            transaction_open = True

            merge_result = q(
                f"""
                MERGE INTO {SCHEMA}.VIVENU_RAW_CURRENT T
                USING (
                    SELECT
                        RUN_ID,
                        STREAM_NAME,
                        RECORD_ID,
                        SOURCE_UPDATED_AT,
                        PARSE_JSON(PAYLOAD_JSON) AS DATA,
                        PARSE_JSON(SOURCE_CONTEXT_JSON) AS SOURCE_CONTEXT,
                        SHA2(PAYLOAD_JSON, 256) AS RECORD_HASH
                    FROM {SCHEMA}.VIVENU_LOAD_STAGE
                    WHERE RUN_ID = ? AND STREAM_NAME = ?
                ) S
                ON T.STREAM_NAME = S.STREAM_NAME
                   AND T.RECORD_ID = S.RECORD_ID

                WHEN MATCHED
                    AND T.RECORD_HASH <> S.RECORD_HASH
                    AND (
                        T.SOURCE_UPDATED_AT IS NULL
                        OR S.SOURCE_UPDATED_AT IS NULL
                        OR S.SOURCE_UPDATED_AT >= T.SOURCE_UPDATED_AT
                    )
                THEN UPDATE SET
                    SOURCE_UPDATED_AT = COALESCE(
                        S.SOURCE_UPDATED_AT, T.SOURCE_UPDATED_AT
                    ),
                    DATA = S.DATA,
                    SOURCE_CONTEXT = S.SOURCE_CONTEXT,
                    RECORD_HASH = S.RECORD_HASH,
                    LAST_CHANGED_AT = CURRENT_TIMESTAMP(),
                    LAST_RUN_ID = S.RUN_ID

                WHEN NOT MATCHED THEN INSERT (
                    STREAM_NAME,
                    RECORD_ID,
                    SOURCE_UPDATED_AT,
                    DATA,
                    SOURCE_CONTEXT,
                    RECORD_HASH,
                    FIRST_LOADED_AT,
                    LAST_CHANGED_AT,
                    LAST_RUN_ID
                ) VALUES (
                    S.STREAM_NAME,
                    S.RECORD_ID,
                    S.SOURCE_UPDATED_AT,
                    S.DATA,
                    S.SOURCE_CONTEXT,
                    S.RECORD_HASH,
                    CURRENT_TIMESTAMP(),
                    CURRENT_TIMESTAMP(),
                    S.RUN_ID
                )
                """,
                [run_id, name],
            )

            if automatic:
                checkpoint_result = q(
                    f"""
                    UPDATE {SCHEMA}.VIVENU_SYNC_CONTROL
                    SET
                        LAST_SUCCESS_TO_TS = CASE
                            WHEN ? THEN GREATEST(
                                COALESCE(
                                    LAST_SUCCESS_TO_TS,
                                    TO_TIMESTAMP_TZ(?)
                                ),
                                TO_TIMESTAMP_TZ(?)
                            )
                            ELSE LAST_SUCCESS_TO_TS
                        END,
                        LAST_SUCCESS_AT = CURRENT_TIMESTAMP(),
                        LAST_FULL_SYNC_AT = IFF(
                            ?,
                            CURRENT_TIMESTAMP(),
                            LAST_FULL_SYNC_AT
                        ),
                        LAST_RUN_ID = ?
                    WHERE STREAM_NAME = ?
                    """,
                    [
                        not snapshot,
                        iso(upper),
                        iso(upper),
                        full,
                        run_id,
                        name,
                    ],
                )

                if int(checkpoint_result[0][0]) != 1:
                    raise SyncError(
                        f"{name}: checkpoint update affected an "
                        "unexpected number of rows"
                    )

            q(
                f"DELETE FROM {SCHEMA}.VIVENU_LOAD_STAGE "
                "WHERE RUN_ID = ? AND STREAM_NAME = ?",
                [run_id, name],
            )

            q("COMMIT")
            transaction_open = False

            counts = {
                str(key).lower(): int(value)
                for key, value in merge_result[0].as_dict().items()
            }

            stats["inserted"] = counts.get(
                "number of rows inserted", 0
            )
            stats["updated"] = counts.get(
                "number of rows updated", 0
            )
            stats["watermark_advanced"] = automatic and not snapshot
            stats["status"] = (
                "SUCCEEDED_EMPTY_SCOPE"
                if stats["scopes"] == 0
                else "SUCCEEDED"
            )

    except Exception as exc:
        error = safe_error(exc)

        if (
            current_stream in results
            and results[current_stream]["status"] == "RUNNING"
        ):
            results[current_stream]["status"] = "FAILED"

    finally:
        if transaction_open:
            try:
                q("ROLLBACK")
                transaction_open = False
            except Exception:
                cleanup_errors.append("Rollback failed")

        if http is not None:
            http.close()

        # Do not release the guard after an unresolved transaction failure.
        if locked and not transaction_open:
            try:
                q(
                    f"DELETE FROM {SCHEMA}.VIVENU_LOAD_STAGE "
                    "WHERE RUN_ID = ?",
                    [run_id],
                )
            except Exception:
                cleanup_errors.append("Staging cleanup failed")

            try:
                released = q(
                    f"""
                    UPDATE {SCHEMA}.VIVENU_SYNC_LOCK
                    SET RUN_ID = NULL, ACQUIRED_AT = NULL
                    WHERE LOCK_ID = 1 AND RUN_ID = ?
                    """,
                    [run_id],
                )

                if int(released[0][0]) != 1:
                    cleanup_errors.append(
                        "Pipeline lock was not released"
                    )

            except Exception:
                cleanup_errors.append(
                    "Pipeline lock release failed"
                )

    if cleanup_errors:
        error = "; ".join(
            ([error] if error else []) + cleanup_errors
        )

    fetched = sum(item["fetched"] for item in results.values())
    status = "FAILED" if error else "SUCCEEDED"

    if logged and not transaction_open:
        try:
            q(
                f"""
                UPDATE {SCHEMA}.VIVENU_SYNC_RUNS
                SET
                    FINISHED_AT = CURRENT_TIMESTAMP(),
                    STATUS = ?,
                    API_REQUESTS = ?,
                    RECORDS_FETCHED = ?,
                    STREAM_RESULTS = PARSE_JSON(?),
                    ERROR_MESSAGE = ?
                WHERE RUN_ID = ?
                """,
                [
                    status,
                    api_requests,
                    fetched,
                    canonical(results),
                    error,
                    run_id,
                ],
            )
        except Exception:
            audit_error = "Final audit update failed"
            error = (
                error + "; " + audit_error
                if error else audit_error
            )

    if error:
        raise SyncError(
            f"Vivenu run {run_id} failed: {error}"
        )

    return {
        "run_id": run_id,
        "status": "SUCCEEDED",
        "automatic": automatic,
        "api_requests": api_requests,
        "records_fetched": fetched,
        "streams": results,
    }
$$;


-- =====================================================================
-- 6. REPORTING AND MONITORING VIEWS
-- =====================================================================

-- Generic access to ALL ingested streams.
-- DATA contains the source JSON after any configured field removal.
-- SOURCE_CONTEXT contains parent/lookup path parameters.
--
-- LAST_CHANGED_AT records the latest insert/payload change, not the
-- latest successful poll. Poll timestamps are in VIVENU_SYNC_CONTROL.

CREATE OR REPLACE VIEW VIVENU_RECORDS AS
SELECT
    STREAM_NAME,
    RECORD_ID,
    SOURCE_UPDATED_AT,
    DATA,
    SOURCE_CONTEXT,
    RECORD_HASH,
    FIRST_LOADED_AT,
    LAST_CHANGED_AT,
    LAST_RUN_ID
FROM VIVENU_RAW_CURRENT;

CREATE OR REPLACE VIEW VIVENU_STREAM_STATUS AS
SELECT
    C.STREAM_NAME,
    C.ENABLED,
    C.API_SPEC:mode::VARCHAR AS SYNC_MODE,
    COALESCE(C.API_SPEC:service::VARCHAR, 'core') AS SERVICE,
    C.API_SPEC:path::VARCHAR AS ENDPOINT,
    C.API_SPEC:cursor::VARCHAR AS CURSOR_FIELD,
    C.API_SPEC:parent:stream::VARCHAR AS PARENT_STREAM,
    COALESCE(C.API_SPEC:lookup::BOOLEAN, FALSE) AS USES_LOOKUP_IDS,
    C.API_SPEC:blocked::VARCHAR AS BLOCKED_REASON,
    C.MIN_SYNC_INTERVAL_MINUTES,
    C.LOOKBACK_SECONDS,
    C.RECONCILE_HOURS,
    S.LAST_SUCCESS_TO_TS,
    S.LAST_SUCCESS_AT,
    S.LAST_FULL_SYNC_AT,
    S.LAST_RUN_ID
FROM VIVENU_STREAM_CONFIG C
LEFT JOIN VIVENU_SYNC_CONTROL S
    ON S.STREAM_NAME = C.STREAM_NAME;

CREATE OR REPLACE VIEW VIVENU_RUN_DETAILS AS
SELECT
    R.RUN_ID,
    R.STARTED_AT,
    R.FINISHED_AT,
    R.STATUS AS RUN_STATUS,
    R.REQUESTED_STREAM,
    F.KEY::VARCHAR AS STREAM_NAME,
    F.VALUE:status::VARCHAR AS STREAM_STATUS,
    F.VALUE:mode::VARCHAR AS SYNC_MODE,
    F.VALUE:pages::NUMBER AS PAGES,
    F.VALUE:scopes::NUMBER AS SCOPES,
    F.VALUE:fetched::NUMBER AS RECORDS_FETCHED,
    F.VALUE:inserted::NUMBER AS RECORDS_INSERTED,
    F.VALUE:updated::NUMBER AS RECORDS_UPDATED,
    F.VALUE:full_refresh::BOOLEAN AS FULL_REFRESH,
    F.VALUE:watermark_advanced::BOOLEAN AS WATERMARK_ADVANCED,
    F.VALUE:records_without_returned_cursor::NUMBER
        AS RECORDS_WITHOUT_RETURNED_CURSOR,
    TRY_TO_TIMESTAMP_TZ(F.VALUE:from::VARCHAR) AS FROM_TS,
    TRY_TO_TIMESTAMP_TZ(F.VALUE:to::VARCHAR) AS TO_TS,
    R.ERROR_MESSAGE
FROM VIVENU_SYNC_RUNS R,
LATERAL FLATTEN(INPUT => R.STREAM_RESULTS) F;

CREATE OR REPLACE VIEW EVENTS AS
SELECT
    RECORD_ID AS EVENT_ID,
    DATA:sellerId::VARCHAR AS SELLER_ID,
    DATA:name::VARCHAR AS EVENT_NAME,
    TRY_TO_TIMESTAMP_TZ(DATA:start::VARCHAR) AS START_AT,
    TRY_TO_TIMESTAMP_TZ(DATA:end::VARCHAR) AS END_AT,
    TRY_TO_TIMESTAMP_TZ(DATA:createdAt::VARCHAR) AS CREATED_AT,
    SOURCE_UPDATED_AT AS UPDATED_AT,
    FIRST_LOADED_AT,
    LAST_CHANGED_AT
FROM VIVENU_RAW_CURRENT
WHERE STREAM_NAME = 'events';

CREATE OR REPLACE VIEW CUSTOMERS AS
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
FROM VIVENU_RAW_CURRENT
WHERE STREAM_NAME = 'customers';

CREATE OR REPLACE VIEW TRANSACTIONS AS
SELECT
    RECORD_ID AS TRANSACTION_ID,
    DATA:tid::VARCHAR AS TRANSACTION_NUMBER,
    DATA:sellerId::VARCHAR AS SELLER_ID,
    DATA:eventId::VARCHAR AS EVENT_ID,
    DATA:customerId::VARCHAR AS CUSTOMER_ID,
    DATA:status::VARCHAR AS STATUS,
    DATA:paymentStatus::VARCHAR AS PAYMENT_STATUS,
    DATA:currency::VARCHAR AS CURRENCY,
    TRY_TO_DECIMAL(DATA:realPrice::VARCHAR, 18, 4) AS REAL_PRICE,
    DATA:tickets AS LINE_ITEMS,
    li.INDEX AS LINE_ITEM_INDEX,
    li.VALUE:"_id"::VARCHAR AS LINE_ITEM_ID,
    li.VALUE:name::VARCHAR AS LINE_ITEM_NAME,
    li.VALUE:type::VARCHAR AS LINE_ITEM_TYPE,
    li.VALUE:ticketTypeId::VARCHAR AS LINE_ITEM_TICKET_TYPE_ID,
    TRY_TO_DECIMAL(li.VALUE:price::VARCHAR, 18, 4) AS LINE_ITEM_PRICE,
    TRY_TO_DECIMAL(li.VALUE:netPrice::VARCHAR, 18, 4) AS LINE_ITEM_NET_PRICE,
    TRY_TO_DECIMAL(li.VALUE:taxRate::VARCHAR, 18, 4) AS LINE_ITEM_TAX_RATE,
    li.VALUE:amount::NUMBER AS LINE_ITEM_QUANTITY,
    li.VALUE:seatingInfo.sectionName::VARCHAR AS LINE_ITEM_SEAT_SECTION,
    li.VALUE:seatingInfo.rowName::VARCHAR AS LINE_ITEM_SEAT_ROW,
    li.VALUE:seatingInfo.seatName::VARCHAR AS LINE_ITEM_SEAT_NAME,
    li.VALUE:seatingInfo.gate::VARCHAR AS LINE_ITEM_SEAT_GATE,
    li.VALUE:categoryRef::VARCHAR AS LINE_ITEM_CATEGORY_REF,
    li.VALUE:planId::VARCHAR AS LINE_ITEM_PLAN_ID,
    TRY_TO_TIMESTAMP_TZ(DATA:createdAt::VARCHAR) AS CREATED_AT,
    SOURCE_UPDATED_AT AS UPDATED_AT,
    FIRST_LOADED_AT,
    LAST_CHANGED_AT
FROM VIVENU_RAW_CURRENT,
    LATERAL FLATTEN(INPUT => DATA:tickets, OUTER => TRUE) li
WHERE STREAM_NAME = 'transactions';

CREATE OR REPLACE VIEW TICKETS AS
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
FROM VIVENU_RAW_CURRENT
WHERE STREAM_NAME = 'tickets';

CREATE OR REPLACE VIEW SUBSCRIPTIONS AS
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
FROM VIVENU_RAW_CURRENT
WHERE STREAM_NAME = 'subscriptions';

CREATE OR REPLACE VIEW SCANS AS
SELECT
    RECORD_ID AS SCAN_ID,
    DATA:sellerId::VARCHAR AS SELLER_ID,
    DATA:eventId::VARCHAR AS EVENT_ID,
    DATA:ticketId::VARCHAR AS TICKET_ID,
    DATA:deviceId::VARCHAR AS DEVICE_ID,
    TRY_TO_TIMESTAMP_TZ(DATA:time::VARCHAR) AS SCANNED_AT,
    DATA:type::VARCHAR AS SCAN_TYPE,
    DATA:scanResult::VARCHAR AS SCAN_RESULT,
    FIRST_LOADED_AT,
    LAST_CHANGED_AT
FROM VIVENU_RAW_CURRENT
WHERE STREAM_NAME = 'scans';

CREATE OR REPLACE VIEW CHECKOUTS AS
SELECT
    RECORD_ID AS CHECKOUT_ID,
    DATA:sellerId::VARCHAR AS SELLER_ID,
    DATA:customerId::VARCHAR AS CUSTOMER_ID,
    DATA:email::VARCHAR AS EMAIL,
    DATA:firstname::VARCHAR AS FIRST_NAME,
    DATA:lastname::VARCHAR AS LAST_NAME,
    DATA:status::VARCHAR AS STATUS,
    DATA:type::VARCHAR AS CHECKOUT_TYPE,
    DATA:currency::VARCHAR AS CURRENCY,
    TRY_TO_DECIMAL(DATA:realPrice::VARCHAR, 18, 4) AS REAL_PRICE,
    DATA:salesChannelId::VARCHAR AS SALES_CHANNEL_ID,
    DATA:channel::VARCHAR AS CHANNEL,
    DATA:items AS ITEMS,
    TRY_TO_TIMESTAMP_TZ(DATA:createdAt::VARCHAR) AS CREATED_AT,
    TRY_TO_TIMESTAMP_TZ(DATA:expiresAt::VARCHAR) AS EXPIRES_AT,
    SOURCE_UPDATED_AT AS UPDATED_AT,
    FIRST_LOADED_AT,
    LAST_CHANGED_AT
FROM VIVENU_RAW_CURRENT
WHERE STREAM_NAME = 'checkouts';

CREATE OR REPLACE VIEW CUSTOMER_PAYMENT_METHODS AS
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
FROM VIVENU_RAW_CURRENT
WHERE STREAM_NAME = 'customer_payment_methods';

CREATE OR REPLACE VIEW DATA_FIELDS AS
SELECT
    RECORD_ID AS DATA_FIELD_ID,
    DATA:sellerId::VARCHAR AS SELLER_ID,
    DATA:name::VARCHAR AS FIELD_NAME,
    DATA:title::VARCHAR AS TITLE,
    DATA:slug::VARCHAR AS SLUG,
    DATA:type::VARCHAR AS FIELD_TYPE,
    DATA:isPersonalData::BOOLEAN AS IS_PERSONAL_DATA,
    DATA:options AS OPTIONS,
    DATA:settings AS SETTINGS,
    FIRST_LOADED_AT,
    LAST_CHANGED_AT
FROM VIVENU_RAW_CURRENT
WHERE STREAM_NAME = 'data_fields';

CREATE OR REPLACE VIEW INVOICES AS
SELECT
    RECORD_ID AS INVOICE_ID,
    DATA:sellerId::VARCHAR AS SELLER_ID,
    DATA:transactionId::VARCHAR AS TRANSACTION_ID,
    DATA:customerId::VARCHAR AS CUSTOMER_ID,
    DATA:no::VARCHAR AS INVOICE_NUMBER,
    DATA:type::VARCHAR AS INVOICE_TYPE,
    DATA:currency::VARCHAR AS CURRENCY,
    TRY_TO_DECIMAL(DATA:total::VARCHAR, 18, 4) AS TOTAL,
    TRY_TO_DECIMAL(DATA:includedTax::VARCHAR, 18, 4) AS INCLUDED_TAX,
    TRY_TO_DECIMAL(DATA:discountSum::VARCHAR, 18, 4) AS DISCOUNT_SUM,
    DATA:origin::VARCHAR AS ORIGIN,
    DATA:items AS ITEMS,
    DATA:recipient AS RECIPIENT,
    TRY_TO_TIMESTAMP_TZ(DATA:createdAt::VARCHAR) AS CREATED_AT,
    SOURCE_UPDATED_AT AS UPDATED_AT,
    FIRST_LOADED_AT,
    LAST_CHANGED_AT
FROM VIVENU_RAW_CURRENT
WHERE STREAM_NAME = 'invoices';

CREATE OR REPLACE VIEW PAYMENT_REQUESTS AS
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
FROM VIVENU_RAW_CURRENT
WHERE STREAM_NAME = 'payment_requests';

CREATE OR REPLACE VIEW PRICE_TABLES AS
SELECT
    RECORD_ID AS PRICE_TABLE_ID,
    DATA:sellerId::VARCHAR AS SELLER_ID,
    DATA:name::VARCHAR AS TABLE_NAME,
    DATA:categories AS CATEGORIES,
    DATA:tiers AS TIERS,
    DATA:types AS TYPES,
    FIRST_LOADED_AT,
    LAST_CHANGED_AT
FROM VIVENU_RAW_CURRENT
WHERE STREAM_NAME = 'price_tables';

CREATE OR REPLACE VIEW PURCHASE_INTENTS AS
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
FROM VIVENU_RAW_CURRENT
WHERE STREAM_NAME = 'purchase_intents';

CREATE OR REPLACE VIEW SALES_CHANNELS AS
SELECT
    RECORD_ID AS SALES_CHANNEL_ID,
    DATA:sellerId::VARCHAR AS SELLER_ID,
    DATA:name::VARCHAR AS CHANNEL_NAME,
    DATA:type::VARCHAR AS CHANNEL_TYPE,
    DATA:groupId::VARCHAR AS GROUP_ID,
    DATA:status::VARCHAR AS STATUS,
    TRY_TO_TIMESTAMP_TZ(DATA:createdAt::VARCHAR) AS CREATED_AT,
    SOURCE_UPDATED_AT AS UPDATED_AT,
    FIRST_LOADED_AT,
    LAST_CHANGED_AT
FROM VIVENU_RAW_CURRENT
WHERE STREAM_NAME = 'sales_channels';

CREATE OR REPLACE VIEW SALES_CHANNEL_GROUPS AS
SELECT
    RECORD_ID AS SALES_CHANNEL_GROUP_ID,
    DATA:sellerId::VARCHAR AS SELLER_ID,
    DATA:name::VARCHAR AS GROUP_NAME,
    DATA:status::VARCHAR AS STATUS,
    DATA:restrictEventAccess::BOOLEAN AS RESTRICT_EVENT_ACCESS,
    TRY_TO_TIMESTAMP_TZ(DATA:createdAt::VARCHAR) AS CREATED_AT,
    SOURCE_UPDATED_AT AS UPDATED_AT,
    FIRST_LOADED_AT,
    LAST_CHANGED_AT
FROM VIVENU_RAW_CURRENT
WHERE STREAM_NAME = 'sales_channel_groups';

CREATE OR REPLACE VIEW SEATING_EVENTS AS
SELECT
    RECORD_ID AS SEATING_EVENT_ID,
    DATA:_owner::VARCHAR AS EVENT_ID,
    DATA:seatMapId::VARCHAR AS SEAT_MAP_ID,
    DATA:revisionId::VARCHAR AS REVISION_ID,
    TRY_TO_TIMESTAMP_TZ(DATA:createdAt::VARCHAR) AS CREATED_AT,
    SOURCE_UPDATED_AT AS UPDATED_AT,
    FIRST_LOADED_AT,
    LAST_CHANGED_AT
FROM VIVENU_RAW_CURRENT
WHERE STREAM_NAME = 'seating_events';


-- =====================================================================
-- 7. SCHEDULED TASK
-- =====================================================================

CREATE OR REPLACE TASK TASK_VIVENU_HOURLY
    WAREHOUSE = VIVENU_INGEST_WH
    SCHEDULE = '60 MINUTES'
    OVERLAP_POLICY = NO_OVERLAP
    USER_TASK_TIMEOUT_MS = 14400000
    SUSPEND_TASK_AFTER_NUM_FAILURES = 3
    AUTOCOMMIT = TRUE
AS
    CALL RAW_INGESTION.VIVENU_NATIVE.SP_VIVENU_SYNC(
        NULL, NULL, NULL, FALSE
    );

-- The task is deliberately left suspended.


-- =====================================================================
-- 8. INSTALLATION CHECK
-- =====================================================================

-- This is read-only and does not invoke Vivenu.
SELECT
    STREAM_NAME,
    ENABLED,
    SYNC_MODE,
    SERVICE,
    ENDPOINT,
    PARENT_STREAM,
    USES_LOOKUP_IDS,
    BLOCKED_REASON,
    MIN_SYNC_INTERVAL_MINUTES,
    LOOKBACK_SECONDS,
    RECONCILE_HOURS
FROM VIVENU_STREAM_STATUS
ORDER BY STREAM_NAME;


-- =====================================================================
-- 9. OPTIONAL OPERATIONS
--
-- Everything in this section is commented out.
-- Execute individual examples only after reviewing them.
-- =====================================================================

/*

-- ---------------------------------------------------------------------
-- A. Session setup for manual calls
-- ---------------------------------------------------------------------

USE ROLE VIVENU_INGEST_ROLE;
USE DATABASE RAW_INGESTION;
USE SCHEMA VIVENU_NATIVE;
USE WAREHOUSE VIVENU_INGEST_WH;

ALTER SESSION SET AUTOCOMMIT = TRUE;
ALTER SESSION SET TIMEZONE = 'UTC';


-- ---------------------------------------------------------------------
-- B. Optional historical lower bound: set BEFORE the initial load
-- ---------------------------------------------------------------------

UPDATE VIVENU_CONFIG
SET INITIAL_FROM_TS = '2024-01-01 00:00:00 +00:00'::TIMESTAMP_TZ
WHERE CONFIG_ID = 1;

-- Changing INITIAL_FROM_TS later does not reset a saved checkpoint.
-- Use a forced full read to backfill an earlier retained range.


-- ---------------------------------------------------------------------
-- C. Bounded initial test
--
-- Reads currently available transactions whose updatedAt falls in this
-- range. It is not a historical point-in-time reconstruction.
-- This replay DOES NOT advance the automatic watermark.
-- ---------------------------------------------------------------------

CALL SP_VIVENU_SYNC(
    'transactions',
    DATEADD(DAY, -2, CURRENT_TIMESTAMP()),
    CURRENT_TIMESTAMP(),
    FALSE
);


-- ---------------------------------------------------------------------
-- D. Normal automatic load
--
-- First successful automatic read:
--   INITIAL_FROM_TS -> run cutoff
--
-- Subsequent reads:
--   saved watermark - overlap -> run cutoff
--
-- Periodic reconciliation re-reads the configured retained-history range.
-- ---------------------------------------------------------------------

CALL SP_VIVENU_SYNC(NULL, NULL, NULL, FALSE);

-- Run again to exercise the incremental path.
CALL SP_VIVENU_SYNC(NULL, NULL, NULL, FALSE);


-- ---------------------------------------------------------------------
-- E. Enable additional root streams after confirming module access
--    and API-key permissions
-- ---------------------------------------------------------------------

UPDATE VIVENU_STREAM_CONFIG
SET ENABLED = TRUE
WHERE STREAM_NAME IN (
    'checkouts',
    'ticket_transfers',
    'invoices',
    'purchase_intents',
    'products',
    'product_streams',
    'coupons',
    'coupon_series',
    'vouchers',
    'sales_channels',
    'price_tables'
);

-- An explicitly requested stream bypasses its minimum refresh interval.
CALL SP_VIVENU_SYNC('products', NULL, NULL, FALSE);


-- ---------------------------------------------------------------------
-- F. Enable parent-child streams together
-- ---------------------------------------------------------------------

UPDATE VIVENU_STREAM_CONFIG
SET ENABLED = TRUE
WHERE STREAM_NAME IN (
    'pos_devices',
    'pos_sessions',
    'pos_journal_logs'
);

CALL SP_VIVENU_SYNC('pos_sessions', NULL, NULL, FALSE);

-- Force a fresh parent read as well as the child read.
CALL SP_VIVENU_SYNC('pos_sessions', NULL, NULL, TRUE);


-- ---------------------------------------------------------------------
-- G. Lookup-only example
--
-- These IDs must come from your own known resource inventory.
-- This does not discover all donations.
-- ---------------------------------------------------------------------

INSERT INTO VIVENU_LOOKUP_CONTEXTS (
    STREAM_NAME, CONTEXT, ENABLED
)
SELECT
    'fundraise_donations',
    PARSE_JSON('{"id":"<DONATION_ID>"}'),
    TRUE;

UPDATE VIVENU_STREAM_CONFIG
SET ENABLED = TRUE
WHERE STREAM_NAME = 'fundraise_donations';

CALL SP_VIVENU_SYNC(
    'fundraise_donations', NULL, NULL, FALSE
);


-- ---------------------------------------------------------------------
-- H. Refresh and reconciliation settings
-- ---------------------------------------------------------------------

-- Refresh this snapshot every six hours.
UPDATE VIVENU_STREAM_CONFIG
SET MIN_SYNC_INTERVAL_MINUTES = 360
WHERE STREAM_NAME = 'products';

-- Increase ticket lookback to thirty minutes.
UPDATE VIVENU_STREAM_CONFIG
SET LOOKBACK_SECONDS = 1800
WHERE STREAM_NAME = 'tickets';

-- Disable reconciliation only for update-time streams.
-- This reduces API traffic but removes the periodic recovery read.
UPDATE VIVENU_STREAM_CONFIG
SET RECONCILE_HOURS = 0
WHERE API_SPEC:mode::VARCHAR = 'UPDATED';

-- Do NOT set RECONCILE_HOURS to zero for CREATED streams.
-- The procedure deliberately rejects that configuration.

-- Re-read retained ticket history without truncating the target.
CALL SP_VIVENU_SYNC('tickets', NULL, NULL, TRUE);


-- ---------------------------------------------------------------------
-- I. Monitor loaded data and checkpoints
-- ---------------------------------------------------------------------

SELECT *
FROM VIVENU_STREAM_STATUS
ORDER BY STREAM_NAME;

SELECT
    RUN_ID,
    STARTED_AT,
    FINISHED_AT,
    STATUS,
    API_REQUESTS,
    RECORDS_FETCHED,
    STREAM_RESULTS,
    ERROR_MESSAGE
FROM VIVENU_SYNC_RUNS
ORDER BY STARTED_AT DESC;

SELECT *
FROM VIVENU_RUN_DETAILS
ORDER BY STARTED_AT DESC, STREAM_NAME;

SELECT STREAM_NAME, COUNT(*) AS RECORD_COUNT
FROM VIVENU_RAW_CURRENT
GROUP BY STREAM_NAME
ORDER BY STREAM_NAME;

-- Query an additional stream through the generic view.
SELECT RECORD_ID, DATA, SOURCE_UPDATED_AT, LAST_CHANGED_AT
FROM VIVENU_RECORDS
WHERE STREAM_NAME = 'products';

-- Inspect parent context for a child stream.
SELECT
    SOURCE_CONTEXT:id::VARCHAR AS CUSTOMER_ID,
    DATA:_id::VARCHAR AS PAYMENT_METHOD_ID,
    DATA:status::VARCHAR AS STATUS,
    LAST_CHANGED_AT
FROM VIVENU_RECORDS
WHERE STREAM_NAME = 'customer_payment_methods';


-- ---------------------------------------------------------------------
-- J. Enable scheduling only after validation succeeds
-- ---------------------------------------------------------------------

ALTER TASK TASK_VIVENU_HOURLY RESUME;

-- Suspend future scheduled starts:
-- ALTER TASK TASK_VIVENU_HOURLY SUSPEND;

-- Suspension does not terminate an already-running procedure call.


-- ---------------------------------------------------------------------
-- K. Credential rotation
--
-- Rotate to another key for the SAME seller/environment.
-- Do not rotate to a different seller's key within this schema.
-- ---------------------------------------------------------------------

USE ROLE ACCOUNTADMIN;

ALTER SECRET RAW_INGESTION.VIVENU_NATIVE.VIVENU_API_KEY
    SET SECRET_STRING = '<NEW_VIVENU_API_KEY>';

USE ROLE VIVENU_INGEST_ROLE;


-- ---------------------------------------------------------------------
-- L. Recovery after a forcibly terminated run
--
-- FIRST confirm in Snowflake query/task history that the original call
-- has stopped. Never clear the lock of an active call.
--
-- A stopped run can leave staging rows and a RUNNING audit entry.
-- Data/checkpoints from previously committed streams remain valid.
-- ---------------------------------------------------------------------

SELECT *
FROM VIVENU_SYNC_LOCK;

UPDATE VIVENU_SYNC_LOCK
SET RUN_ID = NULL, ACQUIRED_AT = NULL
WHERE LOCK_ID = 1
  AND RUN_ID = '<CONFIRMED_STOPPED_RUN_ID>';

DELETE FROM VIVENU_LOAD_STAGE
WHERE RUN_ID = '<CONFIRMED_STOPPED_RUN_ID>';

UPDATE VIVENU_SYNC_RUNS
SET
    STATUS = 'FAILED',
    FINISHED_AT = CURRENT_TIMESTAMP(),
    ERROR_MESSAGE = 'Run termination confirmed; manually recovered.'
WHERE RUN_ID = '<CONFIRMED_STOPPED_RUN_ID>'
  AND STATUS = 'RUNNING';

-- Retry normally; successful automatic checkpoints are retained.
CALL SP_VIVENU_SYNC(NULL, NULL, NULL, FALSE);

*/

-- =====================================================================
-- END OF FRESH-INSTALL SCRIPT
-- =====================================================================

-- CALL RAW_INGESTION.VIVENU_NATIVE.SP_VIVENU_SYNC(
--         NULL, NULL, NULL, FALSE
--     );

-- Select * from RAW_INGESTION.VIVENU_NATIVE.CUSTOMERS;
-- Select * from RAW_INGESTION.VIVENU_NATIVE.TICKETS;
-- Select * from RAW_INGESTION.VIVENU_NATIVE.TRANSACTIONS;
-- Select * from RAW_INGESTION.VIVENU_NATIVE.EVENTS;
-- Select * from RAW_INGESTION.VIVENU_NATIVE.CHECKOUTS;
-- Select * from RAW_INGESTION.VIVENU_NATIVE.CUSTOMER_PAYMENT_METHODS;
-- Select * from RAW_INGESTION.VIVENU_NATIVE.DATA_FIELDS;
-- Select * from RAW_INGESTION.VIVENU_NATIVE.INVOICES;
-- Select * from RAW_INGESTION.VIVENU_NATIVE.PAYMENT_REQUESTS;
-- Select * from RAW_INGESTION.VIVENU_NATIVE.PRICE_TABLES;
-- Select * from RAW_INGESTION.VIVENU_NATIVE.PURCHASE_INTENTS;
-- Select * from RAW_INGESTION.VIVENU_NATIVE.SALES_CHANNELS;
-- Select * from RAW_INGESTION.VIVENU_NATIVE.SALES_CHANNEL_GROUPS;
-- Select * from RAW_INGESTION.VIVENU_NATIVE.SEATING_EVENTS;



-- -- Clear the stale lock
-- UPDATE RAW_INGESTION.VIVENU_NATIVE.VIVENU_SYNC_LOCK
-- SET RUN_ID = NULL, ACQUIRED_AT = NULL
-- WHERE LOCK_ID = 1;
