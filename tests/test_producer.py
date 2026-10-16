import csv
import importlib.util
import json
from pathlib import Path
import sys
import tempfile
import subprocess
import unittest
from unittest.mock import Mock
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
from producer import make_event, row_to_features, send_batch, save_state


class ProducerTests(unittest.TestCase):
    def setUp(self):
        with (ROOT / "examples/synthetic_sample.csv").open(newline="") as f:
            self.row = next(csv.DictReader(f))
        self.run_id = "00000000-0000-0000-0000-000000000001"

    def event(self, cycle=0):
        return make_event(self.row, self.run_id, cycle, 0, "synthetic_sample")

    def test_label_and_identity_are_not_model_features(self):
        event = self.event()
        self.assertNotIn("is_fraud", event["features"])
        self.assertNotIn("cc_num", event)
        self.assertNotIn("first", event["features"])
        self.assertEqual(event["ground_truth"]["is_fraud"], 0)
        self.assertEqual(event["features"]["hour"], 12)

    def test_event_ids_stable_on_retry_and_unique_on_replay(self):
        self.assertEqual(self.event()["event_id"], self.event()["event_id"])
        self.assertNotEqual(self.event()["event_id"], self.event(1)["event_id"])

    def test_retries_only_failed_entries(self):
        first, second = self.event(), self.event(1)
        client = Mock()
        client.put_records.side_effect = [
            {"Records": [{"SequenceNumber": "1"}, {"ErrorCode": "InternalFailure"}]},
            {"Records": [{"SequenceNumber": "2"}]},
        ]
        send_batch(client, "demo", [first, second], sleep=lambda _: None)
        self.assertEqual(len(client.put_records.call_args_list[0].kwargs["Records"]), 2)
        retried = client.put_records.call_args_list[1].kwargs["Records"]
        self.assertEqual(len(retried), 1)
        self.assertEqual(json.loads(retried[0]["Data"])["event_id"], second["event_id"])

    def test_ambiguous_transport_error_retries_same_payload(self):
        client = Mock()
        client.put_records.side_effect = [TimeoutError("network"),
            {"Records": [{"SequenceNumber": "1"}]}]
        send_batch(client, "demo", [self.event()], sleep=lambda _: None)
        self.assertEqual(client.put_records.call_args_list[0].kwargs,
                         client.put_records.call_args_list[1].kwargs)

    def test_exhausted_retry_raises(self):
        client = Mock()
        client.put_records.return_value = {"Records": [{"ErrorCode": "InternalFailure"}]}
        with self.assertRaises(RuntimeError):
            send_batch(client, "demo", [self.event()], attempts=2, sleep=lambda _: None)

    def test_invalid_amount_rejected(self):
        for value in ["NaN", "inf", "-1"]:
            row = dict(self.row, amt=value)
            with self.assertRaises(ValueError):
                row_to_features(row)

    def test_checkpoint_write(self):
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "state.json"
            save_state(path, {"next_row": 5})
            self.assertEqual(json.loads(path.read_text())["next_row"], 5)

    def test_cli_resumes_checkpoint_without_repeating_rows(self):
        with tempfile.TemporaryDirectory() as folder:
            checkpoint = str(Path(folder) / "checkpoint.json")
            command = [sys.executable, str(ROOT / "src/producer.py"), "--csv",
                       str(ROOT / "examples/synthetic_sample.csv"), "--dry-run",
                       "--max-records", "3", "--rate", "100", "--checkpoint", checkpoint]
            first = subprocess.run(command, check=True, text=True, capture_output=True)
            second = subprocess.run(command, check=True, text=True, capture_output=True)
            first_rows = [json.loads(x) for x in first.stdout.splitlines()]
            second_rows = [json.loads(x) for x in second.stdout.splitlines()]
            self.assertEqual([x["source_row"] for x in first_rows], [0, 1, 2])
            self.assertEqual([x["source_row"] for x in second_rows], [3, 4, 5])
            self.assertEqual(first_rows[0]["replay_id"], second_rows[0]["replay_id"])

    def test_template_has_no_inbound_rules_and_requires_imdsv2(self):
        template = json.loads((ROOT / "infra/template.json").read_text())
        sg = template["Resources"]["SecurityGroup"]["Properties"]
        self.assertNotIn("SecurityGroupIngress", sg)
        metadata = template["Resources"]["ProducerInstance"]["Properties"]["MetadataOptions"]
        self.assertEqual(metadata["HttpTokens"], "required")


if __name__ == "__main__":
    unittest.main()
