"""Pipeline tests use only local mock providers and temporary SQLite files."""
import concurrent.futures
import os
import tempfile
import threading
import time
import unittest
from unittest.mock import patch

os.environ.setdefault("LLM_PROVIDER", "mock")
import app


class PipelineTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db_patch = patch.object(app, "DB_PATH", os.path.join(self.tmp.name, "test.sqlite"))
        self.db_patch.start()
        app.init_db()
        with app._metrics_lock:
            app._metrics.update(llm_calls_total=0, llm_errors_total=0, max_concurrent_llm_calls=0)

    def tearDown(self):
        self.db_patch.stop()
        self.tmp.cleanup()

    def test_concurrency_never_exceeds_configured_semaphore(self):
        active, peak, lock = 0, 0, threading.Lock()

        def slow_valid(_title, _description):
            nonlocal active, peak
            with lock:
                active += 1
                peak = max(peak, active)
            time.sleep(0.04)
            with lock:
                active -= 1
            return {"clean_title": "Test item", "category": "Other", "brand": None, "tags": ["test"]}

        with patch.object(app, "_provider_call", side_effect=slow_valid):
            with concurrent.futures.ThreadPoolExecutor(max_workers=20) as pool:
                list(pool.map(lambda i: app.enrich(f"unique item {i}", ""), range(20)))
        self.assertLessEqual(peak, app.LLM_CONCURRENCY)

    def test_retries_three_times_and_records_failures(self):
        calls = 0

        def flaky(_title, _description):
            nonlocal calls
            calls += 1
            if calls < 4:
                raise RuntimeError("temporary")
            return {"clean_title": "Rice", "category": "Groceries", "brand": None, "tags": ["rice"]}

        with patch.object(app, "_provider_call", side_effect=flaky), patch.object(app.time, "sleep"):
            result = app.enrich("rice", "1 kg")
        self.assertEqual(calls, 4)
        self.assertFalse(result["cache_hit"])
        self.assertEqual(app._metrics["llm_errors_total"], 3)

    def test_simultaneous_identical_products_share_one_call(self):
        active_calls = 0

        def slow_valid(_title, _description):
            nonlocal active_calls
            active_calls += 1
            time.sleep(0.08)
            active_calls -= 1
            return {"clean_title": "Amul Butter", "category": "Groceries", "brand": "Amul", "tags": ["butter"]}

        with patch.object(app, "_provider_call", side_effect=slow_valid):
            with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
                results = list(pool.map(lambda _: app.enrich(" AMUL butter ", " pack of 2"), range(8)))
        self.assertEqual(app._metrics["llm_calls_total"], 1)
        self.assertEqual(sum(not r["cache_hit"] for r in results), 1)
        self.assertTrue(all(r["clean_title"] == "Amul Butter" for r in results))

    def test_completed_result_is_persistently_cached(self):
        valid = {"clean_title": "Rice", "category": "Groceries", "brand": None, "tags": ["rice"]}
        with patch.object(app, "_provider_call", return_value=valid) as provider:
            first = app.enrich("rice", "1 kg")
            second = app.enrich("rice", "1 kg")
        self.assertFalse(first["cache_hit"])
        self.assertTrue(second["cache_hit"])
        provider.assert_called_once()


if __name__ == "__main__":
    unittest.main()
