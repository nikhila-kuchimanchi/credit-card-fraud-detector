import base64
from datetime import datetime, timezone
import json
from pathlib import Path
import sys
import unittest
from unittest.mock import Mock

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from db import repository as repo, schema

NOW = datetime(2026, 10, 16, 18, 0, 5, tzinfo=timezone.utc)


class ConditionalCheckFailed(Exception):
    response = {"Error": {"Code": "ConditionalCheckFailedException"}}


class DbTests(unittest.TestCase):
    def setUp(self):
        self.event = json.loads((ROOT / "examples/event.json").read_text())
        self.client = Mock()

    def test_lambda_fixture_decodes_to_example_event(self):
        lam = json.loads((ROOT / "examples/lambda_event.json").read_text())
        decoded = json.loads(base64.b64decode(lam["Records"][0]["kinesis"]["data"]))
        self.assertEqual(decoded, self.event)

    def test_pending_item_contains_event_and_derived_keys(self):
        item = repo.build_pending_item(self.event, NOW)
        self.assertEqual(item["event_id"], self.event["event_id"])
        self.assertEqual(item["status"], schema.STATUS_PENDING)
        self.assertEqual(item["emitted_date"], "2026-10-16")
        self.assertNotIn("flag_status", item)  # not in sparse index until flagged
        self.assertEqual(item["expires_at"] - schema.DEFAULT_TTL_DAYS * 86400,
                         int(datetime(2026, 10, 16, 18, tzinfo=timezone.utc).timestamp()))

    def test_missing_field_is_rejected(self):
        del self.event["event_id"]
        with self.assertRaises(ValueError):
            repo.build_pending_item(self.event, NOW)

    def test_serialization_round_trip(self):
        item = repo.build_pending_item(self.event, NOW)
        attrs = {k: repo.to_attr(v) for k, v in item.items()}
        self.assertEqual(attrs["features"]["M"]["amount"], {"N": "8.5"})
        self.assertEqual(repo.from_item(attrs), item)

    def test_non_finite_number_rejected(self):
        with self.assertRaises(ValueError):
            repo.to_attr(float("nan"))

    def test_put_pending_is_conditional_and_detects_duplicate(self):
        self.assertTrue(repo.put_pending(self.client, self.event, now=NOW))
        kwargs = self.client.put_item.call_args.kwargs
        self.assertEqual(kwargs["ConditionExpression"], "attribute_not_exists(event_id)")
        self.client.put_item.side_effect = ConditionalCheckFailed()
        self.assertFalse(repo.put_pending(self.client, self.event, now=NOW))

    def test_other_errors_are_raised(self):
        self.client.put_item.side_effect = RuntimeError("throttled")
        with self.assertRaises(RuntimeError):
            repo.put_pending(self.client, self.event, now=NOW)

    def test_flagged_score_enters_sparse_index_and_queues_alert(self):
        updated, predicted = repo.record_score(self.client, "e1", 0.93, 0.5, "v1", now=NOW)
        self.assertTrue(updated and predicted)
        kwargs = self.client.update_item.call_args.kwargs
        self.assertIn("flag_status = :flag", kwargs["UpdateExpression"])
        self.assertEqual(kwargs["ExpressionAttributeValues"][":alert"], {"S": "PENDING"})
        self.assertIn("#st <> :scored", kwargs["ConditionExpression"])

    def test_normal_score_is_not_flagged(self):
        updated, predicted = repo.record_score(self.client, "e1", 0.1, 0.5, "v1", now=NOW)
        self.assertTrue(updated)
        self.assertFalse(predicted)
        self.assertNotIn("flag_status", self.client.update_item.call_args.kwargs["UpdateExpression"])

    def test_duplicate_score_reports_not_updated(self):
        self.client.update_item.side_effect = ConditionalCheckFailed()
        self.assertEqual(repo.record_score(self.client, "e1", 0.93, 0.5, "v1"), (False, True))

    def test_error_never_overwrites_scored(self):
        self.assertTrue(repo.record_error(self.client, "e1", "timeout", now=NOW))
        self.assertIn("#st <> :scored", self.client.update_item.call_args.kwargs["ConditionExpression"])

    def test_alert_marked_sent_once(self):
        self.assertTrue(repo.mark_alert_sent(self.client, "e1", "msg-1", now=NOW))
        self.client.update_item.side_effect = ConditionalCheckFailed()
        self.assertFalse(repo.mark_alert_sent(self.client, "e1", "msg-1", now=NOW))

    def test_list_flagged_queries_sparse_index_newest_first(self):
        item = {k: repo.to_attr(v) for k, v in repo.build_pending_item(self.event, NOW).items()}
        self.client.query.return_value = {"Items": [item]}
        items, next_key = repo.list_flagged(self.client, limit=10)
        kwargs = self.client.query.call_args.kwargs
        self.assertEqual(kwargs["IndexName"], schema.GSI_FLAGGED)
        self.assertFalse(kwargs["ScanIndexForward"])
        self.assertEqual(items[0]["event_id"], self.event["event_id"])
        self.assertIsNone(next_key)

    def test_table_definition_has_both_indexes(self):
        params = schema.create_table_params()
        self.assertEqual(params["BillingMode"], "PAY_PER_REQUEST")
        names = {g["IndexName"] for g in params["GlobalSecondaryIndexes"]}
        self.assertEqual(names, {schema.GSI_BY_DAY, schema.GSI_FLAGGED})


if __name__ == "__main__":
    unittest.main()
