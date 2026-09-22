#!/usr/bin/env python3
"""Deployment guardrails; no Tapis credentials or network access required."""

import importlib.util
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock

REPO = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location("tapis", REPO / "deploy/beam-pool/tapis.py")
tapis = importlib.util.module_from_spec(spec)
spec.loader.exec_module(tapis)
TOKEN = "test-capability-with-enough-characters"


class DeploymentTest(unittest.TestCase):
    def setUp(self):
        self.plan = tapis.plan("example/beam:release", "beam", 20, "tacc", "tacc", 2048)

    def test_individual_endpoints_and_private_workers(self):
        self.assertEqual(len(self.plan["workers"]), 20)
        args = self.plan["gateway"]["arguments"]
        self.assertEqual(args.count("--worker"), 20)
        self.assertIn("pods-tacc-tacc-beamw20:9001", args)
        for worker in self.plan["workers"]:
            self.assertEqual(worker["networking"]["default"]["protocol"], "local_only")
            self.assertEqual(worker["resources"]["cpu_limit"], 1000)
            self.assertEqual(worker["environment_variables"],
                             self.plan["gateway"]["environment_variables"])

    def test_missing_image_approval_creates_nothing(self):
        api = Mock()
        api.username = "tester"
        api.request.return_value = []
        with self.assertRaisesRegex(ValueError, "allowlist"):
            tapis.up(api, self.plan, 2, TOKEN)
        api.request.assert_called_once_with("GET", "/images")

    def test_name_collision_creates_nothing(self):
        api = Mock()
        api.username = "tester"
        api.request.side_effect = [[{"image": "example/beam"}],
                                   [{"pod_id": "beampool", "image": "another/service:v1"}]]
        with self.assertRaisesRegex(ValueError, "differs"):
            tapis.up(api, self.plan, 2, TOKEN)
        self.assertTrue(all(c.args[0] == "GET" for c in api.request.call_args_list))

    def test_scale_up_preserves_gateway_and_existing_secret(self):
        existing = [dict(p, status="AVAILABLE") for p in
                    self.plan["workers"][:2] + [self.plan["gateway"]]]
        for pod in existing:
            pod["secret_map"] = {"BEAM_POOL_TOKEN": "${secret:tester:beamtoken}"}

        def request(method, path, body=None):
            if path == "/images":
                return [{"image": "example/beam"}]
            if path == "" and method == "GET":
                return existing
            if path == "/secrets/beamtoken/value":
                return {"secret_value": TOKEN}
            self.assertEqual((method, path, body["pod_id"]), ("POST", "", "beamw03"))
            return {"status": "REQUESTED"}

        api = Mock()
        api.username = "tester"
        api.request.side_effect = request
        tapis.up(api, self.plan, 3, TOKEN)
        writes = [c for c in api.request.call_args_list if c.args[0] == "POST"]
        self.assertEqual(len(writes), 1)

    def test_mismatched_secret_is_not_rotated(self):
        api = Mock()
        api.username = "tester"
        api.request.side_effect = [[{"image": "example/beam"}], [], {"secret_value": "other"}]
        with self.assertRaisesRegex(ValueError, "secret differs"):
            tapis.up(api, self.plan, 2, TOKEN)
        self.assertTrue(all(c.args[0] == "GET" for c in api.request.call_args_list))

    def test_first_start_creates_secret_then_only_requested_workers(self):
        api = Mock()
        api.username = "tester"
        api.request.side_effect = [[{"image": "example/beam"}], [],
            tapis.ApiError(404, "secret not found"), {},
            {"status": "REQUESTED"}, {"status": "REQUESTED"}, {"status": "REQUESTED"}]
        tapis.up(api, self.plan, 2, TOKEN)
        writes = [c.args for c in api.request.call_args_list if c.args[0] == "POST"]
        self.assertEqual(writes[0][1], "/secrets")
        self.assertEqual([c[2]["pod_id"] for c in writes[1:]], ["beamw01", "beamw02", "beampool"])
        self.assertEqual(writes[1][2]["secret_map"]["BEAM_POOL_TOKEN"], "${secret:tester:beamtoken}")

    def test_secret_permission_failure_is_not_treated_as_missing(self):
        api = Mock()
        api.username = "tester"
        api.request.side_effect = [[{"image": "example/beam"}], [],
                                  tapis.ApiError(403, "not permitted")]
        with self.assertRaises(tapis.ApiError):
            tapis.up(api, self.plan, 2, TOKEN)
        self.assertTrue(all(c.args[0] == "GET" for c in api.request.call_args_list))

    def test_invalid_capacity_and_environment_are_rejected(self):
        for count in (0, 21):
            with self.assertRaises(ValueError):
                tapis.up(Mock(), self.plan, count, TOKEN)
        with tempfile.TemporaryDirectory() as directory:
            env = Path(directory) / ".env"
            env.write_text('TOKEN="literal$(never-execute)"\n')
            self.assertEqual(tapis.environment([env])["TOKEN"], "literal$(never-execute)")
            env.write_text("not an assignment\n")
            with self.assertRaises(ValueError):
                tapis.environment([env])


if __name__ == "__main__":
    unittest.main()
