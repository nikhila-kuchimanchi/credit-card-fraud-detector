"""Read-only API Lambda behind API Gateway (HTTP API, payload v2).

Routes (all GET, JSON responses):
  /transactions/recent?day=YYYY-MM-DD&limit=50&next=TOKEN  newest first, one UTC day
  /transactions/flagged?limit=50&next=TOKEN                newest predicted-fraud first
  /transactions/{event_id}                                 one transaction
  /health                                                  liveness check

Pagination: pass the "next" value from a response to get the following page.
Uses only GetItem and Query (see db/iam/reader-policy.json); never writes.
"""

import base64
import json
import logging
import os
import re

from db import repository as repo

log = logging.getLogger()
log.setLevel(logging.INFO)

TABLE = os.environ.get("TABLE_NAME", "FraudTransactions")
MAX_LIMIT = 100
_DAY = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_ddb = None


def _client():
    global _ddb
    if _ddb is None:
        import boto3
        _ddb = boto3.client("dynamodb")
    return _ddb


def _response(status, body):
    return {
        "statusCode": status,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps(body, default=str),
    }


def _encode_token(key):
    if not key:
        return None
    return base64.urlsafe_b64encode(json.dumps(key).encode()).decode()


def _decode_token(token):
    if not token:
        return None
    try:
        return json.loads(base64.urlsafe_b64decode(token.encode()))
    except ValueError:
        raise _BadRequest("invalid next token")


class _BadRequest(Exception):
    pass


def _limit(params):
    try:
        value = int(params.get("limit", 50))
    except ValueError:
        raise _BadRequest("limit must be an integer")
    return max(1, min(value, MAX_LIMIT))


def lambda_handler(event, context):
    method = event.get("requestContext", {}).get("http", {}).get("method", "GET")
    path = event.get("rawPath", "/").rstrip("/") or "/"
    params = event.get("queryStringParameters") or {}
    if method == "OPTIONS":
        return _response(204, {})
    if method != "GET":
        return _response(405, {"error": "only GET is supported"})
    try:
        ddb = _client()
        if path == "/health":
            return _response(200, {"status": "ok"})
        if path == "/transactions/recent":
            day = params.get("day")
            if day and not _DAY.match(day):
                raise _BadRequest("day must be YYYY-MM-DD")
            items, key = repo.list_recent(ddb, day=day, limit=_limit(params),
                                          start_key=_decode_token(params.get("next")),
                                          table=TABLE)
            return _response(200, {"items": items, "count": len(items),
                                   "next": _encode_token(key)})
        if path == "/transactions/flagged":
            items, key = repo.list_flagged(ddb, limit=_limit(params),
                                           start_key=_decode_token(params.get("next")),
                                           table=TABLE)
            return _response(200, {"items": items, "count": len(items),
                                   "next": _encode_token(key)})
        match = re.fullmatch(r"/transactions/([0-9a-fA-F-]{36})", path)
        if match:
            item = repo.get_transaction(ddb, match.group(1), table=TABLE)
            if item is None:
                return _response(404, {"error": "transaction not found"})
            return _response(200, item)
        return _response(404, {"error": "unknown route",
                               "routes": ["/transactions/recent", "/transactions/flagged",
                                          "/transactions/{event_id}", "/health"]})
    except _BadRequest as exc:
        return _response(400, {"error": str(exc)})
    except Exception:
        log.exception("read failed")
        return _response(500, {"error": "internal error"})
