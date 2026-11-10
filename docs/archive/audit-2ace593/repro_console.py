"""Offline audit reproducer; prints observed behavior, never contacts Google.

Run after installing requirements-dev.txt. Uses temporary state and fake credentials.
These are diagnostics, not assertions that the defective behavior is desirable.
"""
import asyncio
import copy
import json
import os
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / 'app'))
_state_dir = tempfile.TemporaryDirectory(prefix='vertex-console-audit-')
os.environ['STATE_DIR'] = _state_dir.name
os.environ['API_KEY'] = 'audit-only'
os.environ['MODELS_CONFIG_URL'] = ''

import httpx
from main import app
from console_auth import AUTH_COOKIE, issue_session
from runtime_state import app_state


async def main():
    app_state.set_express_keys([{'key': 'audit-key', 'project_id': 'audit-project',
                                'project_source': 'manual', 'note': 'original', 'enabled': True}])
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='http://audit',
                                cookies={AUTH_COOKIE: issue_session()}) as client:
        first = (await client.get('/api/express-keys')).json()['keys']
        second = copy.deepcopy(first)
        first[0]['enabled'] = False
        a = await client.post('/api/express-keys', json={'keys': first})
        second[0]['note'] = 'changed in another tab'
        b = await client.post('/api/express-keys', json={'keys': second})
        result = (await client.get('/api/express-keys')).json()['keys'][0]
        print(json.dumps({'case': 'stale_console_save', 'first_save': a.status_code,
                          'second_save': b.status_code, 'account_enabled_after_disable': result['enabled'],
                          'note': result['note']}))


asyncio.run(main())
