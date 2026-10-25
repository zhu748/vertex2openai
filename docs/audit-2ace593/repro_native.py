"""Offline audit reproducer; prints observed behavior, never contacts Google.

Run after installing requirements-dev.txt. Uses temporary state and fake credentials.
These are diagnostics, not assertions that the defective behavior is desirable.
"""
import asyncio
import json
import os
import sys
import tempfile
from contextlib import asynccontextmanager
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch, AsyncMock

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
from credential_manager import credential_manager
from upstreams import cookie_native as native
from upstreams.cookie_response import extract_events


async def main():
    app_state.set_channel_mode('web_proxy')
    app_state.set_express_keys([])
    app_state.set_cookie_accounts([{'id': 'audit-account', 'cookie': 'SAPISID=old; SID=old',
                                   'project_id': 'audit-project', 'enabled': True}])
    app_state.update_settings({'upstream_max_concurrency': 1})
    req_body = {'contents': [{'role': 'user', 'parts': [{'text': 'hello'}]}]}
    old_record = app_state.get_cookie_accounts()[0]
    raw = {'results': [{'data': {'candidates': [{'content': {'parts': [
        {'text': 'answer', 'thoughtSignature': 'c2ln'}]}, 'finishReason': 'STOP'}]}}]}

    async def normal_events(*args, **kwargs):
        for event in extract_events(raw):
            yield event

    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='http://audit',
                                headers={'x-goog-api-key': 'audit-only'}) as client:
        with patch.object(native, '_events_with_retry', normal_events):
            lease = generation_limiter.reserve()
            try:
                result = await client.post('/v1beta/models/gemini-3.5-flash-lite:generateContent', json=req_body)
                print(json.dumps({'case': 'native_capacity', 'configured_limit': 1,
                                  'occupied_slots': generation_limiter.active, 'http_status': result.status_code}))
            finally:
                lease.release()
            for query in ['', '?alt=sse']:
                result = await client.post('/v1beta/models/gemini-3.5-flash-lite:streamGenerateContent'+query, json=req_body)
                print(json.dumps({'case': 'native_stream', 'query': query, 'content_type': result.headers['content-type'],
                                  'wire_contains_signature': 'thoughtSignature' in result.text,
                                  'body': result.text}))
            result = await client.post('/v1beta/models/gemini-3.5-flash-lite:generateContent', json=req_body)
            print(json.dumps({'case': 'native_nonstream_signature',
                              'signature_present': 'thoughtSignature' in result.text}))
        print(json.dumps({'case': 'hello_head', 'http_status': (await client.head('/api/hello')).status_code}))

    new_record = dict(old_record, cookie='SAPISID=new; SID=new')
    app_state.set_cookie_accounts([new_record, dict(new_record, id='backup', cookie='SAPISID=backup; SID=backup')])

    @asynccontextmanager
    async def fake_client(*args, **kwargs):
        yield object()

    async def auth_error(*args, **kwargs):
        yield 'cookie_error', native.NativeBridgeError(401, 'stale credential expired')

    request = SimpleNamespace(is_disconnected=AsyncMock(return_value=False))
    with patch.object(native, 'open_upstream_client', fake_client), \
         patch.object(native, '_execute_stream_request_generator', auth_error):
        for _ in range(3):
            try:
                async for _event in native._events_with_retry(request, old_record, {}, 'gemini-3.5-flash-lite'):
                    pass
            except native.NativeBridgeError:
                pass
    snapshot = credential_manager.cookie_health_snapshot()
    print(json.dumps({'case': 'stale_cookie_quarantines_new',
                      'new_cookie_cooldown': snapshot['audit-account']['cooldown_remaining'],
                      'selected_id': credential_manager.get_cookie_record()['id']}))


asyncio.run(main())
