"""Console inheritance and bounded operational summaries: offline integration tests."""
import os
from pathlib import Path
import sys
import tempfile
import time
import unittest
from unittest import mock

_STATE = tempfile.TemporaryDirectory(prefix="console-usability-")
os.environ["STATE_DIR"] = _STATE.name
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "app"))

from fastapi.testclient import TestClient
import config
import console_auth
import main
from routes import settings_api, console
from runtime_state import AppState
from request_tracking import RequestRegistry


class ConsoleUsabilityTests(unittest.TestCase):
    def setUp(self):
        self.state = AppState()
        self.state.clear_model_override('audit-model')
        self.state.clear_model_override('gemini-4-argon')
        self.client = TestClient(main.app)
        self.addCleanup(self.client.close)
        self.client.cookies.set(console_auth.AUTH_COOKIE, console_auth.issue_session())
        patch = mock.patch.object(settings_api, 'app_state', self.state)
        patch.start()
        self.addCleanup(patch.stop)

    def test_sparse_save_keeps_unedited_values_inherited_and_survives_reload(self):
        self.state.update_settings({'thinking_g3_level': 'low'})
        response = self.client.post('/api/model-overrides/audit-model', json={'first_reply_timeout_seconds': 35})
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()['override'], {'first_reply_timeout_seconds': 35})
        self.state.update_settings({'thinking_g3_level': 'high'})
        self.assertEqual(AppState().get_effective_settings('audit-model')['thinking_g3_level'], 'high')
        self.assertEqual(AppState().get_effective_settings('audit-model')['first_reply_timeout_seconds'], 35)

    def test_reset_one_field_preserves_other_overrides_and_explicit_null(self):
        path = '/api/model-overrides/audit-model'
        self.client.post(path, json={'first_reply_timeout_seconds': 35, 'default_top_p': None})
        response = self.client.post(path, json={'_inherit': ['first_reply_timeout_seconds']})
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()['override'], {'default_top_p': None})
        self.client.post(path, json={'_inherit': ['default_top_p']})
        self.assertNotIn('audit-model', self.state.get_saved_model_overrides())
        self.assertNotIn('audit-model', self.state.get_model_overrides())

    def test_reset_builtin_field_restores_builtin_without_saving_a_copy(self):
        path = '/api/model-overrides/gemini-4-argon'
        self.client.post(path, json={'anti_truncation_mode': 'off', 'first_reply_timeout_seconds': 21})
        response = self.client.post(path, json={'_inherit': ['anti_truncation_mode']})
        self.assertEqual(response.json()['override']['anti_truncation_mode'], 'both')
        self.assertEqual(self.state.get_saved_model_overrides()['gemini-4-argon'], {'first_reply_timeout_seconds': 21})
        with mock.patch.object(settings_api, 'get_express_models', mock.AsyncMock(return_value=['gemini-4-argon'])):
            result = self.client.get('/api/capabilities').json()
        self.assertEqual(result['saved_overrides']['gemini-4-argon'], {'first_reply_timeout_seconds': 21})
        self.assertEqual(result['builtin_overrides'], config.DEFAULT_SETTINGS['model_overrides'])

    def test_invalid_inheritance_is_atomic(self):
        self.state.set_model_override('audit-model', {'first_reply_timeout_seconds': 21})
        for payload in ({'_inherit': None}, {'_inherit': 'first_reply_timeout_seconds'},
                        {'_inherit': ['proxy_urls']}, {'_inherit': [{}]},
                        {'_inherit': ['first_reply_timeout_seconds'], 'first_reply_timeout_seconds': 7},
                        {'_inherit': ['first_reply_timeout_seconds'], 'default_top_p': 3}):
            with self.subTest(payload=payload):
                response = self.client.post('/api/model-overrides/audit-model', json=payload)
                self.assertEqual(response.status_code, 400)
                self.assertEqual(self.state.get_saved_model_overrides()['audit-model'], {'first_reply_timeout_seconds': 21})
        self.client.cookies.clear()
        self.assertEqual(self.client.post('/api/model-overrides/audit-model', json={'_inherit': ['first_reply_timeout_seconds']}).status_code, 401)

    def test_recent_metrics_exclude_old_and_resume_readers_and_keep_cancellation_separate(self):
        registry = RequestRegistry()
        for outcome, age, parent, latency in [('succeeded', 1, None, 100), ('error', 1, None, 900),
                                               ('cancelled', 1, None, None), ('succeeded', 1000, None, 9999),
                                               ('succeeded', 1, 'parent', 9999)]:
            trace = registry.create('/v1/chat/completions')
            trace.data.update(created_at=time.time()-age, parent_id=parent, channel='express',
                              http_active=False, generation_active=False, outcome=outcome)
            trace.ended=time.monotonic()
            if latency is not None:
                trace.timings['first_reply']={'elapsed_ms': latency}
        active=registry.create('/v1/chat/completions')
        active.configure(channel='cookie')
        with mock.patch.object(console, 'request_registry', registry):
            result=self.client.get('/api/recent-metrics')
            self.assertEqual(result.headers['cache-control'], 'no-store')
            data=result.json()
            self.assertEqual([data[k] for k in ['success','error','cancelled','active','sample_count']], [1,1,1,1,4])
            self.assertEqual(data['first_reply_p50_ms'],100)
            self.assertEqual(data['first_reply_p95_ms'],900)
            self.client.cookies.clear()
            self.assertEqual(self.client.get('/api/recent-metrics').status_code,401)
        empty=RequestRegistry().recent_metrics()
        self.assertEqual(empty['sample_count'],0)
        self.assertIsNone(empty['first_reply_p50_ms'])


if __name__ == '__main__':
    unittest.main()
