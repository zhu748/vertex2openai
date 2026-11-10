/* Shared console helpers, channel configuration and Express credentials.
 * 2026-11 P2-6：并入 IIFE 命名空间体系（与 channel-config / console-models 的
 * 隐式全局 + onclick 契约共存——外部引用面经 window 显式导出，可变状态用
 * getter/setter 与闭包双向同步，行为与原全局版本一致）。 */
(() => {
const $ = (id) => document.getElementById(id);
let keysRevision = null;
let settingsLoaded = false,
  capabilitiesLoaded = false,
  keysLoaded = false,
  runtimeLoaded = false;
const viewScroll = new Map();

let toastTimer;
function toast(m) {
  const t = $('toast');
  clearTimeout(toastTimer);
  t.textContent = m;
  t.classList.add('show');
  toastTimer = setTimeout(() => t.classList.remove('show'), 4000);
}
function fmt(n) {
  return (n || 0).toLocaleString('en-US');
}
async function logout() {
  try {
    const response = await fetch('/api/logout', {
      signal: AbortSignal.timeout(10000),
      method: 'POST'
    });
    if (!response.ok) throw new Error(await responseError(response, '退出失败'));
    location.href = '/';
  } catch (error) {
    toast(error.name === 'TimeoutError' ? '退出请求超时，请重试' : error.message);
  }
}
async function readJSON(url) {
  const response = await fetch(url, { cache: 'no-store', signal: AbortSignal.timeout(15000) });
  if (!response.ok)
    throw new Error(
      response.status === 401
        ? '登录已过期，请重新登录'
        : '加载失败（HTTP ' + response.status + '）'
    );
  return response.json();
}
function loadNotice(section, message) {
  if (section === 'params') {
    const other = $('runtime-load-error');
    if (other) {
      other.classList.toggle('hidden', !message);
      other.querySelector('span').textContent = message;
    }
  }
  const box = $(section + '-load-error');
  box.classList.toggle('hidden', !message);
  box.querySelector('span').textContent = message;
}
async function responseError(response, fallback = '保存失败', result = null) {
  if (response.status === 401) return '登录已过期，请重新登录';
  const data = result || (await response.json().catch(() => ({})));
  const aliases = {
    proxy_url: 'proxy-input',
    proxy_urls: 'proxy-input',
    express_ua_custom: 'express-ua-custom',
    express_ua_profile: 'express-ua-profile',
    cookie_impersonate: 'cookie-impersonate'
  };
  const field = $(aliases[data.field] || data.field);
  if (field) {
    field.setAttribute('aria-invalid', 'true');
    window.consoleForms?.fieldError(field, typeof data.error === 'string' ? data.error : fallback);
    requestAnimationFrame(() => {
      if (field.getClientRects().length && !field.matches(':disabled')) field.focus();
    });
  }
  if (typeof data.error === 'string') return data.error;
  if (Array.isArray(data.detail)) return data.detail.map((item) => item.msg).join('；');
  return typeof data.detail === 'string'
    ? data.detail
    : fallback + '（HTTP ' + response.status + '）';
}

/* /api/settings POST 样板（2026-11 P2-6 收敛：七个调用点同构的 30s 超时 +
 * JSON 头 + 错误归一）。成功时调用 onSaved(保存后响应) 并返回 true；守卫
 * 未过或请求失败时统一 toast 并返回 false。channel-config 的代理编辑器
 * 保存不复用此入口：那里需要 throw 给外层状态机，错误 UI 语义不同。 */
async function postSettings(payload, onSaved) {
  if (!settingsLoaded) {
    toast('设置尚未加载，请先在模型参数页重新加载配置');
    return false;
  }
  try {
    const r = await fetch('/api/settings', {
      signal: AbortSignal.timeout(30000),
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload)
    });
    if (r.ok) {
      const saved = await r.json().catch(() => null);
      if (onSaved) onSaved(saved);
      return true;
    }
    toast('❌ ' + (await responseError(r)));
  } catch (e) {
    toast('❌ 网络请求失败');
  }
  return false;
}

function switchTab(t, updateHash = true) {
  if (!['overview', 'channel', 'params', 'runtime', 'requests', 'logs'].includes(t)) return;
  const previous = document.querySelector('.tab.active')?.dataset.tab;
  const changed = previous !== t;
  if (changed && previous) viewScroll.set(previous, window.scrollY);
  document.querySelectorAll('.tab').forEach((e) => {
    const active = e.dataset.tab === t;
    e.classList.toggle('active', active);
    e.setAttribute('aria-selected', String(active));
    e.tabIndex = active ? 0 : -1;
  });
  ['overview', 'channel', 'params', 'runtime', 'requests', 'logs'].forEach((v) =>
    $('view-' + v).classList.toggle('hidden', v !== t)
  );
  if (updateHash) history.replaceState(null, '', '#' + t);
  $('mobile-nav').value = t;
  document.body.dataset.view = t;
  window.consoleForms?.layout();
  if (changed) window.scrollTo({ top: viewScroll.get(t) || 0, behavior: 'instant' });
  if (t === 'overview') queueMicrotask(fetchRecentMetrics);
  if (t === 'requests') window.requestMonitor?.refresh();
  if (t === 'logs') window.consoleLogs?.onShow();
}

document.querySelector('.tab-bar').addEventListener('keydown', (event) => {
  if (!['ArrowLeft', 'ArrowRight', 'Home', 'End'].includes(event.key)) return;
  const tabs = [...document.querySelectorAll('.tab')];
  const index = tabs.indexOf(document.activeElement);
  if (index < 0) return;
  event.preventDefault();
  const next =
    event.key === 'Home'
      ? 0
      : event.key === 'End'
        ? tabs.length - 1
        : (index + (event.key === 'ArrowRight' ? 1 : -1) + tabs.length) % tabs.length;
  tabs[next].focus();
  switchTab(tabs[next].dataset.tab);
});
function navigateHash() {
  let target;
  try {
    target = decodeURIComponent(location.hash.slice(1));
  } catch {
    target = '';
  }
  if (target === 'main-content') return;
  const anchor = $(target),
    view = anchor?.closest('.view');
  if (view && anchor !== view) {
    switchTab(view.id.slice(5), false);
    requestAnimationFrame(() => {
      window.consoleForms?.layout();
      anchor.scrollIntoView({ block: 'start' });
    });
  } else
    switchTab(
      ['overview', 'channel', 'params', 'runtime', 'requests', 'logs'].includes(target)
        ? target
        : 'overview',
      false
    );
}
window.addEventListener('hashchange', navigateHash);

/* ---------- 通用转义（XSS 防护）：所有拼进 innerHTML 的服务端数据必须先过这里 ---------- */
function esc(s) {
  return String(s == null ? '' : s)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;');
}

/* ---------- Shared credential forms (available even if the editor fails to load) ---------- */
function proxyDraft(value) {
  if (Array.isArray(value))
    return value.map((v) => (typeof v === 'string' ? v : JSON.stringify(v)));
  const text = String(value || '').trim();
  if (!text) return [];
  // Preserve a whole ICE/node object until the shared backend parser expands it.
  if (text.startsWith('{')) return [text];
  if (text.startsWith('[')) {
    try {
      const list = JSON.parse(text);
      if (Array.isArray(list)) return proxyDraft(list);
    } catch (_) {}
  }
  return text
    .split(/[\r\n]+|,\s*(?=[a-zA-Z][a-zA-Z0-9+.-]*:\/\/|turns?:)/i)
    .map((v) => v.trim())
    .filter(Boolean);
}
function credentialCopy(record) {
  return { ...record, proxies: proxyDraft(record.proxies ?? record.proxy) };
}
function projectFromInput(value) {
  const text = value.trim(),
    match = text.match(/[?&]project=([^&#]+)/) || text.match(/\/projects\/([^/?#]+)/);
  if (!match) return text;
  try {
    return decodeURIComponent(match[1]);
  } catch (_) {
    return match[1];
  }
}
function accountProxyFields(scope, index, record) {
  const prefix = scope === 'key' ? 'k' : 'c',
    proxies = proxyDraft(record.proxies ?? record.proxy);
  return `<div class="account-proxy-field"><div class="field-heading"><label for="${scope}-proxy-${index}">专属代理（按顺序尝试）</label>
    <button type="button" class="btn-ghost" onclick="manageProxies('${scope}',${index})">管理代理 · ${proxies.length} 条</button></div>
    <textarea id="${scope}-proxy-${index}" class="inp log ${prefix}-proxy" rows="2" spellcheck="false" autocomplete="off" placeholder="每行一条；留空跟随全局；direct 强制直连">${esc(proxies.join('\n'))}</textarea>
    <label class="fallback-option"><input type="checkbox" class="${prefix}-fallback" ${record.proxy_fallback === false ? '' : 'checked'}>专属代理全部失效后，使用全局代理</label></div>`;
}

/* ---------- Channel ---------- */
let currentMode = 'api_key';
function renderMode(mode) {
  $('keys-box').classList.toggle('hidden', mode === 'web_proxy');
  $('cookie-box').classList.toggle('hidden', mode === 'api_key');
  $('mode-pill').textContent = {
    api_key: '通道 Express API',
    web_proxy: '通道 Cookie 直连',
    mixed: '通道 Key + Cookie 混合'
  }[mode];
}
async function updateMode(m) {
  if (!runtimeLoaded) {
    document.querySelectorAll('input[name=mode]').forEach((r) => (r.checked = false));
    toast('通道状态尚未加载，请先重新加载配置');
    return;
  }
  const pill = $('mode-pill'),
    prev = pill.textContent,
    previousMode = currentMode;
  const radios = document.querySelectorAll('input[name=mode]');
  radios.forEach((r) => (r.disabled = true));
  // 先乐观切显示，请求失败则弹提示并回滚（旧版无错误处理，后端 500 时 UI 与实际状态脱节）
  renderMode(m);
  try {
    const r = await fetch('/api/settings/mode', {
      signal: AbortSignal.timeout(30000),
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ mode: m })
    });
    if (!r.ok) {
      throw new Error('HTTP ' + r.status);
    }
    currentMode = m;
  } catch (e) {
    pill.textContent = prev;
    document.querySelector(`input[name=mode][value=${previousMode}]`).checked = true;
    renderMode(previousMode);
    toast('❌ 通道切换失败，已回滚：' + e.message);
  } finally {
    radios.forEach((r) => (r.disabled = false));
  }
}
async function loadRuntime() {
  try {
    const s = await readJSON('/api/settings/runtime');
    const m = s.mode || (s.use_web_proxy ? 'web_proxy' : 'api_key');
    currentMode = m;
    runtimeLoaded = true;
    document.querySelector(`input[name=mode][value=${m}]`).checked = true;
    renderMode(m);
  } catch (e) {
    loadNotice('channel', '通道状态' + e.message + '。可重新加载配置。');
  }
}

/* ---------- Express Key 列表（多账号，热编辑） ---------- */
let KEYS = [],
  SAVED_KEYS = [];
function keySrcPill(rec) {
  if (rec.project_source === 'auto') return '<span class="pill">来源 auto</span>';
  if (rec.project_source === 'manual') return '<span class="pill">来源 manual</span>';
  return '<span class="pill" style="color:#b45309">未探测</span>';
}
function keyProxyPill(rec) {
  const proxies = proxyDraft(rec.proxies ?? rec.proxy),
    v = (proxies[0] || '').trim().toLowerCase();
  if (!v) return '';
  if (v === 'direct' || v === 'none' || v === '直连')
    return '<span class="pill" style="background:#f5f5f5;color:#737373">强制直连</span>';
  const proto = (proxies[0] || '').split(':')[0] || 'proxy';
  const native = ['http', 'https', 'socks5', 'socks5h'].includes(proto) ? '#047857' : '#1d4ed8';
  return `<span class="pill" style="color:${native}">专属代理 ${proxies.length} 条</span>`;
}
function renderKeys() {
  const box = $('keys-list');
  window.consoleForms?.rememberAccounts('key');
  box.innerHTML = KEYS.map(
    (rec, i) => `
    <div class="key-card" data-i="${i}">
      <div class="key-head">
        <div class="key-title"><span class="pill pill-accent">Key ${i + 1}</span>${keySrcPill(rec)}${keyProxyPill(rec)}</div>
        <div class="key-actions">
          <label><input type="checkbox" class="k-enabled" ${rec.enabled === false ? '' : 'checked'} aria-label="启用 Key ${i + 1}">启用</label>
          <button type="button" onclick="runAction(this,()=>probeOneKey(${i}))">探测项目</button>
          <button type="button" class="danger" onclick="removeKeyRow(${i})" aria-label="删除 Key ${i + 1}">删除</button>
        </div>
      </div>
      <div class="key-fields">
        <div><label for="key-${i}">Express API Key</label><input id="key-${i}" class="inp log k-key" placeholder="粘贴 Express API Key" spellcheck="false" autocomplete="off" value="${esc(rec.key)}"></div>
        <div><label for="project-${i}">Project ID</label><input id="project-${i}" class="inp log k-project" placeholder="留空 = 等待自动探测" spellcheck="false" value="${esc(rec.project_id)}"><label class="note-label" for="key-note-${i}">备注</label><input id="key-note-${i}" class="inp k-note" placeholder="便于区分账号" value="${esc(rec.note)}"></div>
        ${accountProxyFields('key', i, rec)}
      </div>
      ${rec.detected_at ? `<div class="key-meta">上次探测：${esc(new Date(rec.detected_at * 1000).toLocaleString('zh-CN', { hour12: false }))}</div>` : ''}
      ${rec._msg ? `<div class="key-meta ${rec._msg_ok === false ? 'error-message' : ''}" role="status">${esc(rec._msg)}</div>` : ''}
    </div>`
  ).join('');
  window.consoleForms?.accounts('key');
  $('keys-count').textContent = KEYS.length + ' 个';
  $('keys-empty').classList.toggle('hidden', KEYS.length > 0);
}
function collectKeys() {
  // 以输入框的当前内容为准（用户可能改了还没保存）
  [...document.querySelectorAll('#keys-list [data-i]')].forEach((row) => {
    const i = Number(row.dataset.i);
    if (!KEYS[i]) return;
    KEYS[i].key = row.querySelector('.k-key').value.trim();
    const pid = projectFromInput(row.querySelector('.k-project').value);
    if (pid !== (KEYS[i].project_id || '')) {
      KEYS[i].project_id = pid;
      KEYS[i].project_source = pid ? 'manual' : '';
    }
    const kProxy = row.querySelector('.k-proxy');
    if (kProxy) {
      KEYS[i].proxies = proxyDraft(kProxy.value);
      KEYS[i].proxy = KEYS[i].proxies[0] || '';
    }
    KEYS[i].proxy_fallback = row.querySelector('.k-fallback').checked;
    KEYS[i].note = row.querySelector('.k-note').value.trim();
    KEYS[i].enabled = row.querySelector('.k-enabled').checked;
  });
  return KEYS.filter((r) => r.key);
}
/* Express Key 列表：load/save/add/remove 编排由 credential-list.js 的共享
 * 骨架持有（2026-11 P2-6），此处只描述 Key 侧差异。 */
const keyList = credentialList.create({
  endpoint: '/api/express-keys',
  itemsKey: 'keys',
  noticeLabel: '凭证列表',
  loadRetryHint: '重新加载成功后再保存 Key。',
  notLoadedToast: '凭证尚未加载，请先重新加载配置',
  toastNoun: 'Key',
  formsGroup: 'keys',
  accountGroup: 'key',
  // 空列表保存是破坏性操作（环境变量导入不会再发生），必须确认。
  emptyConfirm: () =>
    confirm(
      '将保存为“空 Key 列表”。\n保存后即使环境变量 VERTEX_EXPRESS_API_KEY 有值，重启也不会再导入。\n确定吗？'
    ),
  responseItems: (d) => d.keys || [],
  getItems: () => KEYS,
  setItems: (v) => {
    KEYS = v;
  },
  getSaved: () => SAVED_KEYS,
  setSaved: (v) => {
    SAVED_KEYS = v;
  },
  getRevision: () => keysRevision,
  setRevision: (v) => {
    keysRevision = v;
  },
  isLoaded: () => keysLoaded,
  markLoaded: () => {
    keysLoaded = true;
  },
  render: renderKeys,
  collect: collectKeys,
  blankRecord: () => ({
    key: '',
    project_id: '',
    project_source: '',
    proxy: '',
    proxies: [],
    proxy_fallback: true,
    enabled: true
  }),
  focusNew: (n) => $('key-' + n).focus(),
  afterRemove: () => window.consoleForms?.refresh()
});
async function loadExpressKeys() {
  await keyList.load();
}
async function saveExpressKeys() {
  await keyList.save();
}
function addKeyRow() {
  keyList.add();
}
function removeKeyRow(i) {
  keyList.remove(i);
}
async function probeOneKey(i) {
  collectKeys();
  const rec = KEYS[i];
  if (!rec || !rec.key) {
    toast('请先填入 Key');
    return;
  }
  if (!SAVED_KEYS.some((x) => x.key === rec.key)) {
    toast('请先保存新增或修改后的 Key，再探测项目');
    return;
  }
  toast('正在探测…');
  try {
    // 后端单 Key 探测总预算 45 秒（express_key_probe）+ 余量，避免前端先于后端超时。
    const r = await fetch('/api/express-keys/probe', {
      signal: AbortSignal.timeout(60000),
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ key: rec.key, force: true })
    });
    const d = await r.json();
    if (!r.ok) {
      toast('❌ ' + (d.error || '探测失败'));
      return;
    }
    const res = (d.results || [])[0] || {};
    mergeProbeResults([res.key ? res : { ...res, key: rec.key }], d.keys || [], d.revision);
    toast(res.ok ? '项目：' + (res.project_id || '—') : '❌ ' + (res.message || '探测失败'));
  } catch (e) {
    toast('❌ ' + (e.name === 'TimeoutError' ? '探测超时，请在运行日志确认结果' : '网络请求失败'));
  }
}
async function probeAllKeys() {
  if (!keysLoaded) {
    toast('凭证列表尚未加载，请先重新加载配置');
    return;
  }
  collectKeys();
  toast('正在探测全部 Key…');
  try {
    // 后端 4 路并发、单 Key 45 秒总预算：批量按批次数增长，避免前端先于后端超时。
    const count = Math.max(1, SAVED_KEYS.filter((x) => x.enabled !== false).length);
    const batches = Math.max(1, Math.ceil(count / 4));
    const r = await fetch('/api/express-keys/probe', {
      signal: AbortSignal.timeout(15000 + batches * 46000),
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ all: true, force: true })
    });
    const d = await r.json();
    if (!r.ok) {
      toast('❌ ' + (d.error || '探测失败'));
      return;
    }
    const results = d.results || [];
    mergeProbeResults(results, d.keys || [], d.revision);
    toast('探测完成：成功 ' + results.filter((x) => x.ok).length + ' / ' + results.length);
  } catch (e) {
    toast('❌ ' + (e.name === 'TimeoutError' ? '探测超时，请在运行日志确认结果' : '网络请求失败'));
  }
}

// Probe responses only update diagnostics/project metadata; preserve unsaved rows and edits.
function mergeProbeResults(results, saved, revision = null) {
  collectKeys();
  const expected = SAVED_KEYS.map((rec) => {
    const server = saved.find((row) => row.key === rec.key);
    if (server?.project_source === 'auto' && results.some((res) => res.key === rec.key && res.ok))
      return {
        ...rec,
        project_id: server.project_id,
        project_source: server.project_source,
        detected_at: server.detected_at
      };
    return rec;
  });
  const snapshot = (rows) =>
    JSON.stringify(
      rows.map((rec) => ({
        key: rec.key,
        enabled: rec.enabled !== false,
        note: rec.note || '',
        project_id: rec.project_id || '',
        project_source: rec.project_source || '',
        detected_at: rec.detected_at || 0,
        proxies: proxyDraft(rec.proxies ?? rec.proxy),
        proxy_fallback: rec.proxy_fallback !== false
      }))
    );
  const safeRebase = snapshot(expected) === snapshot(saved);
  if (safeRebase && revision) keysRevision = revision;
  results.forEach((res) => {
    KEYS.filter((x) => x.key === res.key).forEach((hit) => {
      const server = saved.find((x) => x.key === res.key);
      const previous = SAVED_KEYS.find((x) => x.key === res.key);
      const edited =
        previous &&
        (hit.project_id !== previous.project_id || hit.project_source !== previous.project_source);
      if (!edited && (server?.project_id || res.project_id)) {
        hit.project_id = server?.project_id || res.project_id;
        hit.project_source = server?.project_source || 'auto';
        hit.detected_at = server?.detected_at || Date.now() / 1000;
      }
      hit._msg = res.message || '';
      hit._msg_ok = !!res.ok;
    });
  });
  // Unexpected remote edits retain the old baseline/revision, including on
  // repeated probes, so a later save still reports the conflict.
  SAVED_KEYS = (safeRebase ? saved : expected).map(credentialCopy);
  renderKeys();
  window.consoleForms?.rebaseKeys();
}

/* ---------- 出站代理（网络出口） ---------- */
function updateProxyPill(on) {
  const p = $('proxy-status');
  if (!p) return;
  p.textContent = on
    ? typeof on === 'number'
      ? '已配置 ' + on + ' 条'
      : '已启用'
    : '未启用（直连）';
  p.style.background = on ? '#eeeeff' : '#f5f5f5';
  p.style.color = on ? '#5556d9' : '#737373';
}
function updateProxyDraft() {
  const saved = proxyDraft(GLOBAL_SETTINGS.proxy_urls ?? GLOBAL_SETTINGS.proxy_url);
  const draft = proxyDraft($('proxy-input').value);
  $('proxy-draft-state')?.classList.toggle(
    'hidden',
    !settingsLoaded || JSON.stringify(saved) === JSON.stringify(draft)
  );
  // 旧节点的测试结果不能作为新草稿的连通性结论。
  $('proxy-test-result').classList.add('hidden');
}
async function saveProxy() {
  const proxies = proxyDraft($('proxy-input').value);
  await postSettings({ proxy_urls: proxies }, (saved) => {
    GLOBAL_SETTINGS.proxy_urls = saved.proxy_urls;
    GLOBAL_SETTINGS.proxy_url = saved.proxy_url;
    $('proxy-input').value = saved.proxy_urls.join('\n');
    updateProxyPill(saved.proxy_urls.length);
    updateProxyDraft();
    window.consoleForms?.saved('proxy');
    toast(
      proxies.length
        ? '✅ 已保存 ' + saved.proxy_urls.length + ' 条代理（按顺序故障切换）'
        : '已清空代理，恢复直连'
    );
  });
}
async function testProxy() {
  const v = proxyDraft($('proxy-input').value)[0] || 'direct';
  const box = $('proxy-test-result');
  box.classList.remove('hidden');
  box.innerHTML =
    '<span class="text-neutral-500">🛰️ 正在测试，首次启动协议转换可能需要较长时间…</span>';
  try {
    const r = await fetch('/api/proxy/test', {
      signal: AbortSignal.timeout(60000),
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ proxy_url: v })
    });
    const d = await r.json();
    if (!r.ok) throw new Error(await responseError(r, '测试失败', d));
    if (d.ok) {
      box.innerHTML = `<span style="color:#047857">✅ ${esc(d.message)} · 延迟 ${d.latency_ms} ms${d.via_proxy ? ' · 经 ' + esc(d.proxy) : ''}</span>`;
      toast('✅ 连通成功（' + d.latency_ms + ' ms）');
    } else {
      box.innerHTML = `<span style="color:#be123c">❌ ${esc(d.message)} · ${d.latency_ms} ms<br>详细：${esc(d.error || '')}</span>`;
      toast('❌ 连通失败');
    }
  } catch (e) {
    box.innerHTML =
      '<span style="color:#be123c">❌ ' +
      esc(e.name === 'TimeoutError' ? '代理测试超时，请重试或检查节点状态' : e.message) +
      '</span>';
  }
}

/* ---------- Cookie 通道指纹伪装 ---------- */
const FP_LABELS = {
  chrome: 'Chrome',
  edge: 'Edge',
  safari: 'Safari',
  safari_ios: 'Safari iOS',
  firefox: 'Firefox',
  off: '关闭'
};
function updateFpPill(v) {
  const p = $('fp-status');
  if (!p) return;
  const on = v && v !== 'off';
  p.textContent = on ? '已伪装：' + (FP_LABELS[v] || v) : '关闭（httpx 原始行为）';
  p.style.background = on ? '#ecfdf5' : '#f5f5f5';
  p.style.color = on ? '#047857' : '#737373';
}
async function saveFingerprint() {
  const v = $('cookie-impersonate').value;
  await postSettings({ cookie_impersonate: v }, () => {
    GLOBAL_SETTINGS.cookie_impersonate = v;
    updateFpPill(v);
    window.consoleForms?.saved('fingerprint');
    toast(
      v === 'off'
        ? '✅ 已关闭伪装（下一个请求生效）'
        : '✅ 伪装已保存并热生效：' + (FP_LABELS[v] || v)
    );
  });
}

/* ---------- Express 通道请求头指纹 ---------- */
const FP2_LABELS = {
  sdk: '官方 Python SDK',
  node: 'Node.js SDK',
  gcloud: 'gcloud CLI',
  custom: '自定义'
};
function updateExpressPill(v, custom) {
  const p = $('fp2-status');
  if (!p) return;
  const isDefault = !v || v === 'sdk' || (v === 'custom' && !(custom || '').trim());
  p.textContent = isDefault ? '官方默认（SDK 自动）' : '已接管：' + (FP2_LABELS[v] || v);
  p.title = v === 'custom' ? custom || '' : '';
  p.style.background = isDefault ? '#f5f5f5' : '#ecfdf5';
  p.style.color = isDefault ? '#737373' : '#047857';
}
function updateExpressCustomVis() {
  const sel = $('express-ua-profile'),
    inp = $('express-ua-custom');
  if (sel && inp) inp.style.display = sel.value === 'custom' ? '' : 'none';
}
async function saveExpressUa() {
  const v = $('express-ua-profile').value;
  const c = $('express-ua-custom').value.trim();
  await postSettings({ express_ua_profile: v, express_ua_custom: c }, () => {
    GLOBAL_SETTINGS.express_ua_profile = v;
    GLOBAL_SETTINGS.express_ua_custom = c;
    updateExpressPill(v, c);
    updateExpressCustomVis();
    window.consoleForms?.saved('express-fingerprint');
    toast(
      v === 'sdk' || (v === 'custom' && !c)
        ? '✅ 已恢复官方 SDK 原生形态（下一个请求生效）'
        : '✅ 请求头指纹已保存并热生效：' + (FP2_LABELS[v] || v)
    );
  });
}


  /* ---- 外部引用面（与历史隐式全局一致；2026-11 P2-6 并入 IIFE 体系）----
   * $ / toast / esc / readJSON 等基座被其余控制台脚本与 HTML onclick 引用。
   * 函数声明不会重绑，静态挂载即可；可变状态走 getter/setter 双向同步：
   * console-forms 的 discard 会重绑 KEYS，guard 变量由 console-models
   * 置位、channel-config / console-forms 读取，闭包内重绑也要让外部读到。 */
  Object.assign(window, {
  accountProxyFields,
  addKeyRow,
  collectKeys,
  credentialCopy,
  esc,
  fmt,
  keyProxyPill,
  keySrcPill,
  loadExpressKeys,
  loadNotice,
  loadRuntime,
  logout,
  mergeProbeResults,
  navigateHash,
  postSettings,
  probeAllKeys,
  probeOneKey,
  projectFromInput,
  proxyDraft,
  readJSON,
  removeKeyRow,
  renderKeys,
  renderMode,
  responseError,
  saveExpressKeys,
  saveExpressUa,
  saveFingerprint,
  saveProxy,
  switchTab,
  testProxy,
  toast,
  updateExpressCustomVis,
  updateExpressPill,
  updateFpPill,
  updateMode,
  updateProxyDraft,
  updateProxyPill
  });
  Object.defineProperty(window, '$', { get: () => $ });
  Object.defineProperty(window, 'FP_LABELS', { get: () => FP_LABELS });
  Object.defineProperty(window, 'FP2_LABELS', { get: () => FP2_LABELS });
  Object.defineProperty(window, 'settingsLoaded', {
    get: () => settingsLoaded,
    set: (v) => { settingsLoaded = v; }
  });
  Object.defineProperty(window, 'keysLoaded', {
    get: () => keysLoaded,
    set: (v) => { keysLoaded = v; }
  });
  Object.defineProperty(window, 'capabilitiesLoaded', {
    get: () => capabilitiesLoaded,
    set: (v) => { capabilitiesLoaded = v; }
  });
  Object.defineProperty(window, 'runtimeLoaded', {
    get: () => runtimeLoaded,
    set: (v) => { runtimeLoaded = v; }
  });
  Object.defineProperty(window, 'KEYS', {
    get: () => KEYS,
    set: (v) => { KEYS = v; }
  });
  Object.defineProperty(window, 'SAVED_KEYS', {
    get: () => SAVED_KEYS,
    set: (v) => { SAVED_KEYS = v; }
  });
})();
