{% macro deploy_sp_vivenu_sync(database, schema) %}

{% set sql %}
CREATE OR REPLACE PROCEDURE {{ database }}.{{ schema }}.SP_VIVENU_SYNC(
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
    'vivenu_key' = {{ database }}.{{ schema }}.VIVENU_API_KEY
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
from concurrent.futures import ThreadPoolExecutor, as_completed

import _snowflake
import requests

SCHEMA = "{{database}}.{{schema}}"

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

    # step 1: fetch config
    config_rows = q(f"SELECT * FROM {SCHEMA}.VIVENU_CONFIG WHERE CONFIG_ID = 1")

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
        configs[name] = item            #-- {"events": {"cursor": "updatedAt","enabled": true,"mode": "UPDATED","name": "events","path": "/events","shape": "rows"}}

    roots = (
        [requested] if requested is not None
        else sorted(
            name for name, item in configs.items()
            if item["ENABLED"]      #-- vivenu_stream_config enabled is true
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


{% endset %}

{% do run_query(sql) %}
{% do log("SP_VIVENU_SYNC deployed successfully to " ~ database ~ "." ~ schema, info=True) %}

{% endmacro %}