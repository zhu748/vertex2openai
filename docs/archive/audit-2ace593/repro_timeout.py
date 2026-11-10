"""Offline audit reproducer; prints observed behavior, never contacts Google.

Run after installing requirements-dev.txt. Uses temporary state and fake credentials.
These are diagnostics, not assertions that the defective behavior is desirable.
"""
import asyncio
import json
import os
import sys
import tempfile
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'app'))
_state_dir = tempfile.TemporaryDirectory(prefix='vertex-native-audit-')
os.environ['STATE_DIR'] = _state_dir.name
os.environ['API_KEY'] = 'audit-only'
os.environ['MODELS_CONFIG_URL'] = ''

import httpx
from admission import generation_limiter
from main import app
from runtime_state import app_state
from upstreams import cookie_native as native



async def run():
    app_state.set_channel_mode('web_proxy')
    app_state.set_cookie_accounts([{'id':'account','cookie':'SAPISID=test; SID=test','project_id':'project','enabled':True}])
    app_state.update_settings({'request_timeout_seconds':30,'upstream_max_concurrency':1})
    entered = asyncio.Event()
    async def events(*args, **kwargs):
        entered.set()
        while True:
            await asyncio.sleep(0.1)
            yield 'text', 'x'
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app),base_url='http://audit',headers={'x-goog-api-key':'audit-only'}) as client:
        with patch.object(native,'_events_with_retry',events):
            task = asyncio.create_task(client.post('/v1beta/models/gemini-3.5-flash-lite:generateContent',json={'contents':[]}))
            await entered.wait()
            done, _ = await asyncio.wait({task}, timeout=30.5)
            print(json.dumps({'case':'native_total_deadline','configured_seconds':30,'observed_seconds':30.5,'request_finished':bool(done),'tracked_generation_slots':generation_limiter.active}),flush=True)
            task.cancel()
            await asyncio.gather(task,return_exceptions=True)

asyncio.run(run())
