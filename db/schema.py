"""DynamoDB table definition for the fraud detector (single source of truth).

Access patterns (see db/README.md):
  1. Get one transaction by event_id              -> table primary key
  2. Recent transactions for a given UTC day      -> GSI "by-day"
  3. Recent flagged (predicted fraud) transactions -> sparse GSI "flagged"
"""

TABLE_NAME = "FraudTransactions"
TTL_ATTRIBUTE = "expires_at"
DEFAULT_TTL_DAYS = 30

GSI_BY_DAY = "by-day"
GSI_FLAGGED = "flagged"
FLAGGED_VALUE = "FLAGGED"

# Item status lifecycle: PENDING -> SCORED, or PENDING -> ERROR -> (retry) SCORED
STATUS_PENDING = "PENDING"
STATUS_SCORED = "SCORED"
STATUS_ERROR = "ERROR"

# Alert lifecycle for flagged items: PENDING -> SENT
ALERT_NONE = "NONE"
ALERT_PENDING = "PENDING"
ALERT_SENT = "SENT"


def create_table_params(table_name=TABLE_NAME):
    """Arguments for dynamodb.create_table (on-demand billing)."""
    return {
        "TableName": table_name,
        "BillingMode": "PAY_PER_REQUEST",
        "AttributeDefinitions": [
            {"AttributeName": "event_id", "AttributeType": "S"},
            {"AttributeName": "emitted_date", "AttributeType": "S"},
            {"AttributeName": "emitted_at", "AttributeType": "S"},
            {"AttributeName": "flag_status", "AttributeType": "S"},
        ],
        "KeySchema": [{"AttributeName": "event_id", "KeyType": "HASH"}],
        "GlobalSecondaryIndexes": [
            {
                "IndexName": GSI_BY_DAY,
                "KeySchema": [
                    {"AttributeName": "emitted_date", "KeyType": "HASH"},
                    {"AttributeName": "emitted_at", "KeyType": "RANGE"},
                ],
                "Projection": {"ProjectionType": "ALL"},
            },
            {
                # Sparse index: only items with flag_status are indexed.
                "IndexName": GSI_FLAGGED,
                "KeySchema": [
                    {"AttributeName": "flag_status", "KeyType": "HASH"},
                    {"AttributeName": "emitted_at", "KeyType": "RANGE"},
                ],
                "Projection": {"ProjectionType": "ALL"},
            },
        ],
        "Tags": [{"Key": "project", "Value": "credit-card-fraud-detector"}],
    }
