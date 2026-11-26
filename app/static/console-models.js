/* Model settings, capability rendering and per-model drafts. Requires dashboard.js. */
let CAPS = {},
  curAR = '';
let GLOBAL_SETTINGS = {},
  OVERRIDES = {},
  SAVED_MODEL_OVERRIDES = {},
  BUILTIN_OVERRIDES = {};
const PER_MODEL_KEYS = [
  'first_reply_timeout_seconds',
  'anti_truncation_mode',
  'native_thinking_mode',
  'thinking_g3_level',
  'image_size',
  'image_aspect_ratio',
  'default_temperature',
  'default_top_p',
  'default_max_tokens',
  'inject_system_instruction',
  'inject_prefill',
  'prefill_instruction',
  'image_system_instruction',
  'inject_prefill_for_image',
  'sampling_policy',
  'prefill_cot_guard'
];
const COMMON_ARS = [
  '1:1',
  '3:2',
  '2:3',
  '3:4',
  '4:3',
  '4:5',
  '5:4',
  '9:16',
  '16:9',
  '21:9',
  '1:4',
  '4:1',
  '1:8',
  '8:1',
  '9:21'
];
const MODEL_DRAFTS = new Map();
const MODEL_WRITES = new Map();
let renderedModel = null,
  modelSnapshot = null;

/* ---------- Params ---------- */
function setV(id, v) {
  const el = $(id);
  if (!el) return;
  window.consoleForms?.clearError(el);
  el.removeAttribute('aria-invalid');
  if (id === 'image_aspect_ratio' && v && ![...el.options].some((option) => option.value === v)) {
    const option = document.createElement('option');
    option.value = v;
    option.textContent = v + '（当前模型不支持）';
    option.disabled = true;
    el.add(option);
  }
  if (el.type === 'checkbox') el.checked = !!v;
  else el.value = v === null || v === undefined ? '' : v;
}
async function loadParams() {
  loadNotice('params', '');
  // 设置与模型能力两个接口互不依赖：并行拉取，面板加载不再串行等两次往返。
  const [sRes, cRes] = await Promise.allSettled([
    readJSON('/api/settings'),
    readJSON('/api/capabilities')
  ]);
  // The selection and edits may have changed while the two reads were in flight.
  const selected = $('model-sel').value;
  captureModelDraft();
  if (sRes.status === 'fulfilled') {
    // Capture immediately before applying the response, not before the network wait.
    const formDrafts = window.consoleForms?.capture();
    const s = sRes.value;
    GLOBAL_SETTINGS = s;
    settingsLoaded = true;
    curAR = s.image_aspect_ratio || '';
    [
      'resumable_disconnect_grace_seconds',
      'img_compress_max_dim',
      'img_compress_max_mb',
      'img_compress_quality',
      'retry_max',
      'retry_backoff_seconds',
      'retry_jitter_seconds',
      'fake_streaming_interval',
      'prefill_mode',
      'express_location',
      'resumable_ttl_seconds',
      'resumable_max_streams',
      'request_timeout_seconds',
      'upstream_max_concurrency',
      'resumable_max_buffer_mb',
      'resumable_max_total_mb',
      'keepalive_url',
      'keepalive_interval',
      'cookie_refresh_interval_seconds'
    ].forEach((k) => setV(k, s[k]));
    [
      'img_compress_enabled',
      'fake_streaming',
      'safety_score',
      'cookie_debug',
      'debug_outbound',
      'prefill_suppress_thinking',
      'resumable_enabled',
      'cookie_structured_output',
      'cross_channel_fallback',
      'cookie_auto_refresh'
    ].forEach((k) => setV(k, s[k]));
    setV('credential-strategy', s.roundrobin ? 'roundrobin' : 'random');
    // 出站代理：回填输入框并刷新状态徽标
    setV('proxy-input', proxyDraft(s.proxy_urls ?? s.proxy_url).join('\n'));
    updateProxyPill(proxyDraft(s.proxy_urls ?? s.proxy_url).length);
    updateProxyDraft();
    // Cookie 通道指纹伪装：回填下拉框并刷新徽标
    setV('cookie-impersonate', s.cookie_impersonate || 'chrome');
    updateFpPill(s.cookie_impersonate || 'chrome');
    // Express 通道请求头指纹：回填下拉/自定义框并刷新徽标
    setV('express-ua-profile', s.express_ua_profile || 'sdk');
    setV('express-ua-custom', s.express_ua_custom || '');
    updateExpressPill(s.express_ua_profile || 'sdk', s.express_ua_custom || '');
    updateExpressCustomVis();
    window.consoleForms?.loaded(formDrafts);
    // Legacy thinking switches are normalized by the server. Do not reinterpret
    // an explicitly saved request mode using stale legacy flags.
  } else {
    loadNotice('params', '设置' + sRes.reason.message + '。重新加载成功后再保存。');
  }
  if (cRes.status === 'fulfilled') {
    const c = cRes.value;
    CAPS = c.capabilities || {};
    OVERRIDES = c.overrides || {};
    SAVED_MODEL_OVERRIDES = c.saved_overrides || c.overrides || {};
    BUILTIN_OVERRIDES = c.builtin_overrides || {};
    capabilitiesLoaded = true;
    $('model-sel').innerHTML =
      '<option value="__global__">＊ 全局默认（对所有模型）</option>' +
      (c.models || [])
        .map((m) => {
          const star = OVERRIDES[m] ? ' ★' : '';
          return `<option value="${esc(m)}">${esc(m)}${star}</option>`;
        })
        .join('');
    if ([...$('model-sel').options].some((option) => option.value === selected))
      $('model-sel').value = selected;
    renderCaps(false);
  } else {
    loadNotice('params', '模型能力' + cRes.reason.message + '。重新加载成功后再保存模型参数。');
  }
}
function modelFieldValue(key) {
  const el = $(key);
  return el.type === 'checkbox' ? el.checked : el.value;
}
function modelWireValue(key, raw) {
  if ($(key).type !== 'number') return raw;
  return raw === '' ? (key === 'first_reply_timeout_seconds' ? 0 : null) : Number(raw);
}
function sameExpectation(left, right) {
  // Editable model settings are scalar; null and absence are deliberately distinct.
  return left.present === right.present && left.value === right.value;
}
function modelDraftConverged(key, edit, current) {
  if (edit.kind === 'inherit') return !current.expected.present;
  const value = modelWireValue(key, edit.raw);
  if (current.expected.present && current.expected.value === value) return true;
  // Typing and then undoing is not a deliberate pin. Only abandon the
  // original comparison if the server's own field has not also changed.
  return (
    edit.kind === 'edit' &&
    value === modelWireValue(key, edit.origin.raw) &&
    sameExpectation(current.expected, edit.origin.expected)
  );
}
function modelFieldEdit(key) {
  return MODEL_DRAFTS.get(renderedModel)?.get(key);
}
function recordModelField(key, kind = 'edit') {
  if (!modelSnapshot || !renderedModel) return;
  const draft = MODEL_DRAFTS.get(renderedModel) || new Map();
  const old = draft.get(key);
  if (kind === 'edit' && old?.kind === 'pin') kind = 'pin';
  const current = modelSnapshot[key];
  const edit = { kind, raw: modelFieldValue(key), origin: old?.origin || current };
  const write = MODEL_WRITES.get(renderedModel);
  // Returning to the old baseline can remove a draft while this save is in
  // flight; it is still the user's new intent once the server acknowledges it.
  if (write?.keys.has(key)) write.later.set(key, edit);
  if (modelDraftConverged(key, edit, current)) {
    draft.delete(key);
    if (kind === 'inherit' || modelWireValue(key, edit.raw) !== modelWireValue(key, current.raw))
      setV(key, current.raw);
  } else draft.set(key, edit);
  if (draft.size) MODEL_DRAFTS.set(renderedModel, draft);
  else MODEL_DRAFTS.delete(renderedModel);
}
function refreshModelDraft() {
  const draft = MODEL_DRAFTS.get(renderedModel);
  const count = [...(draft || [])].filter(
    ([key, edit]) => !sameExpectation(edit.origin.expected, modelSnapshot[key].expected)
  ).length;
  const status = $('scope-draft-state');
  status.textContent = count
    ? `未保存 · ${count} 项也被另一页面修改；可撤销本模型修改后重新编辑`
    : '未保存';
  status.classList.toggle('hidden', !draft);
  refreshModelSelStars();
  renderFieldSources();
}
function captureModelDraft() {
  if (!modelSnapshot) return;
  // Also capture direct DOM edits before switching/reloading, even without an input event.
  for (const key of PER_MODEL_KEYS) {
    const edit = modelFieldEdit(key);
    if (modelFieldValue(key) !== (edit ? edit.raw : modelSnapshot[key].raw)) recordModelField(key);
  }
  refreshModelDraft();
}
function finishModelRender(model) {
  renderedModel = model;
  const baseline = modelOwnedFields(model);
  const expected = settingsExpectation(baseline, PER_MODEL_KEYS);
  modelSnapshot = Object.fromEntries(
    PER_MODEL_KEYS.map((key) => [key, { raw: modelFieldValue(key), expected: expected[key] }])
  );
  const draft = MODEL_DRAFTS.get(model);
  if (draft) {
    for (const [key, edit] of draft) {
      if (modelDraftConverged(key, edit, modelSnapshot[key])) {
        draft.delete(key);
        continue;
      }
      // Inheritance follows the latest defaults; pins retain the chosen value.
      setV(key, edit.kind === 'inherit' ? inheritedModelValue(model, key) : edit.raw);
      edit.raw = modelFieldValue(key);
    }
    if (!draft.size) MODEL_DRAFTS.delete(model);
  }
  curAR = $('image_aspect_ratio').value;
  refreshModelDraft();
  updateSamplingDisplay();
  updateModelCapabilities();
}
function discardModelDraft() {
  const model = $('model-sel').value;
  MODEL_WRITES.get(model)?.later.clear();
  MODEL_DRAFTS.delete(model);
  renderCaps(false);
}
function inheritedModelValue(model, key) {
  const builtin = BUILTIN_OVERRIDES[model] || {};
  return Object.hasOwn(builtin, key)
    ? builtin[key]
    : key === 'anti_truncation_mode'
      ? 'off'
      : GLOBAL_SETTINGS[key];
}
function setFieldInheritance(key, inherit) {
  if (!PER_MODEL_KEYS.includes(key) || renderedModel === '__global__') return;
  if (inherit) setV(key, inheritedModelValue(renderedModel, key));
  else if ($(key).disabled) return;
  recordModelField(key, inherit ? 'inherit' : 'pin');
  refreshModelDraft();
  updateModelCapabilities();
}
function renderFieldSources() {
  for (const key of PER_MODEL_KEYS) {
    const el = $(key);
    let row = $('source-' + key);
    if (!row) {
      row = document.createElement('div');
      row.id = 'source-' + key;
      row.className = 'field-source';
      const anchor =
        el.closest('.switch')?.parentElement ||
        (el.parentElement.classList.contains('flex') ? el.parentElement : el);
      anchor.after(row);
    }
    const global = renderedModel === '__global__';
    const edit = modelFieldEdit(key);
    const own =
      !global &&
      (edit
        ? edit.kind !== 'inherit'
        : Object.hasOwn(SAVED_MODEL_OVERRIDES[renderedModel] || {}, key));
    const builtin = Object.hasOwn(BUILTIN_OVERRIDES[renderedModel] || {}, key);
    row.innerHTML = global
      ? ''
      : `<span>${own ? '模型专属' : builtin ? '继承内置默认' : key === 'anti_truncation_mode' ? '继承模型默认' : '继承全局默认'}${edit ? ' · 未保存' : ''}</span><button type="button" data-inherit="${key}" data-value="${own}" ${el.disabled && !own ? 'disabled' : ''}>${own ? '恢复继承' : '设为专属'}</button>`;
    row.hidden = global;
  }
}
function updateModelCapabilities() {
  const cap = CAPS[$('model-sel').value],
    global = $('model-sel').value === '__global__';
  const imageOn = global || !!cap?.is_image;
  $('image-settings').classList.toggle('is-inapplicable', !imageOn);
  $('image-applicability').hidden = imageOn;
  $('image-controls').hidden = !imageOn && !$('show-image-settings').checked;
  for (const key of [
    'image_size',
    'image_aspect_ratio',
    'image_system_instruction',
    'inject_prefill_for_image'
  ])
    $(key).disabled = !imageOn;
  for (const [key, allowed] of [
    ['thinking_g3_level', cap?.thinking?.levels],
    ['image_size', cap?.image_sizes]
  ]) {
    for (const option of $(key).options)
      option.disabled =
        !global && !!allowed?.length && option.value !== '' && !allowed.includes(option.value);
  }
  renderFieldSources();
}
document.addEventListener('click', (event) => {
  const button = event.target.closest('[data-inherit]');
  if (button) {
    const key = button.dataset.inherit;
    setFieldInheritance(key, button.dataset.value === 'true');
    $('source-' + key)
      .querySelector('button')
      ?.focus({ preventScroll: true });
  }
});
function modelOwnedFields(model) {
  return model === '__global__' ? GLOBAL_SETTINGS : SAVED_MODEL_OVERRIDES[model] || {};
}
function modelEffectiveValue(model, key) {
  const override = model === '__global__' ? {} : OVERRIDES[model] || {};
  return Object.hasOwn(override, key)
    ? override[key]
    : key === 'anti_truncation_mode'
      ? 'off'
      : GLOBAL_SETTINGS[key];
}
function modelRawValue(key, value) {
  return $(key).type === 'checkbox' ? !!value : value == null ? '' : String(value);
}
// 按编辑作用域回填模型参数：专属值优先，否则使用全局默认。
function applyModelParamFields(model) {
  PER_MODEL_KEYS.forEach((k) => {
    const v = modelEffectiveValue(model, k);
    if (k === 'image_aspect_ratio') {
      curAR = v || '';
    } else setV(k, v);
  });
  const isGlobal = model === '__global__';
  const has = !isGlobal && !!OVERRIDES[model] && Object.keys(OVERRIDES[model]).length > 0;
  $('ov-badge').classList.toggle('hidden', !has);
  // 保存按钮的落点随作用域切换，避免"看着像全局、存进了专属"（或反过来）
  $('model-footer-scope').textContent = isGlobal ? '保存到：全局默认' : '保存到：' + model;
  $('btn-scope-save').textContent = isGlobal ? '保存为全局默认' : '保存为该模型专属';
  $('btn-scope-clear').classList.toggle('hidden', isGlobal);
  $('ov-hint').textContent = isGlobal
    ? '当前编辑：全局默认。具体模型的专属值优先。'
    : has
      ? '当前编辑：模型专属参数（★）。保存仅影响此模型。'
      : '当前继承默认值。保存后将创建此模型的专属参数。';
}
function settingSnapshot(source, key) {
  return Object.hasOwn(source, key) ? { present: true, value: source[key] } : { present: false };
}
function settingsExpectation(baseline, keys) {
  return Object.fromEntries(Array.from(keys, (key) => [key, settingSnapshot(baseline, key)]));
}
function supersededSettings(before, current, receipt) {
  // A point-in-time save receipt is not a newer reload. Keep any observed
  // scalar value that is neither the submitted baseline nor our saved value.
  return new Set(
    Object.entries(before).flatMap(([key, earlier]) => {
      const now = settingSnapshot(current, key);
      return sameExpectation(now, earlier) || sameExpectation(now, settingSnapshot(receipt, key))
        ? []
        : [key];
    })
  );
}
function changedSettings(values) {
  return Object.fromEntries(
    Object.entries(values).filter(
      ([key, value]) => JSON.stringify(value) !== JSON.stringify(GLOBAL_SETTINGS[key])
    )
  );
}
async function postFormSettings(values, onSaved, draftGroup) {
  const patch = changedSettings(values);
  const keys = Object.keys(patch);
  if (!keys.length) {
    toast('没有需要保存的修改');
    return false;
  }
  const before = settingsExpectation(GLOBAL_SETTINGS, keys);
  const body = { ...patch, _expected: before };
  const forms = window.consoleForms;
  if (forms?.guardSettings) body._expected = forms.guardSettings(body._expected);
  const write = draftGroup && forms?.beginSettingsWrite(draftGroup, keys);
  if (draftGroup && forms && !write) return false;
  try {
    return await postSettings(body, (saved) => {
      const superseded = write ? supersededSettings(before, GLOBAL_SETTINGS, saved) : new Set();
      if (write) forms.acceptSettingsWrite(write, saved, superseded);
      // Do not publish stale values from the response for an unrelated form.
      for (const key of keys) if (!superseded.has(key)) GLOBAL_SETTINGS[key] = saved[key];
      onSaved(saved);
    });
  } finally {
    if (write) forms.endSettingsWrite(write);
  }
}

function completeModelWrite(model, write, applyBaseline) {
  captureModelDraft(); // the selected scope may no longer be the saved one
  const superseded = applyBaseline();
  const draft = MODEL_DRAFTS.get(model) || new Map();
  const expected = settingsExpectation(modelOwnedFields(model), write.keys);
  for (const key of write.keys) {
    const later = write.later.get(key);
    const current = {
      raw: modelRawValue(key, modelEffectiveValue(model, key)),
      expected: expected[key]
    };
    const edit = later && { ...later, origin: superseded.has(key) ? later.origin : current };
    if (edit && !modelDraftConverged(key, edit, current)) {
      draft.set(key, edit);
    } else draft.delete(key);
  }
  if (draft.size) MODEL_DRAFTS.set(model, draft);
  else MODEL_DRAFTS.delete(model);
  renderCaps(false);
}

function applyModelReceipt(model, receipt, write, { patch = {}, inherit = [], reset = false }) {
  const maps = [
    [OVERRIDES, receipt?.override],
    [SAVED_MODEL_OVERRIDES, receipt?.saved_override]
  ];
  const own = receipt?.saved_override;
  if (
    maps.some(([, value]) => !value || typeof value !== 'object' || Array.isArray(value)) ||
    Object.entries(patch).some(
      ([key, value]) =>
        !Object.hasOwn(own, key) || own[key] !== value || receipt.override[key] !== value
    ) ||
    inherit.some((key) => Object.hasOwn(own, key)) ||
    (reset && Object.keys(own).length > 0)
  ) {
    // An old or partial receipt cannot confirm a field's write or inheritance.
    toast('模型保存响应缺少完整的专属参数，请重新加载配置后核对草稿。');
    return false;
  }
  completeModelWrite(model, write, () => {
    const superseded = supersededSettings(write.before, modelOwnedFields(model), own);
    for (const [target, value] of maps) {
      const fields = { ...value };
      for (const key of superseded) {
        const live = target[model] || {};
        if (Object.hasOwn(live, key)) fields[key] = live[key];
        else delete fields[key];
      }
      if (Object.keys(fields).length) target[model] = fields;
      else delete target[model];
    }
    return superseded;
  });
  return true;
}

async function saveModelOverride() {
  if (!settingsLoaded || !capabilitiesLoaded) {
    toast('配置尚未加载，请先重新加载配置');
    return;
  }
  const m = $('model-sel').value;
  if (!m || MODEL_WRITES.has(m)) return;
  captureModelDraft();
  const patch = {},
    expected = {},
    inherit = [];
  for (const [key, edit] of MODEL_DRAFTS.get(m) || []) {
    if (m === '__global__' && key === 'anti_truncation_mode') continue;
    if (edit.kind === 'inherit') inherit.push(key);
    else patch[key] = modelWireValue(key, edit.raw);
    expected[key] = edit.origin.expected;
  }
  const keys = new Set(Object.keys(expected));
  if (!keys.size) {
    toast('没有需要保存的修改');
    return;
  }
  const write = {
    keys,
    later: new Map(),
    before: settingsExpectation(modelOwnedFields(m), m === '__global__' ? keys : PER_MODEL_KEYS)
  };
  MODEL_WRITES.set(m, write);
  const body = { ...patch, ...(inherit.length ? { _inherit: inherit } : {}), _expected: expected };
  try {
    if (m === '__global__') {
      // 全局默认作用域：写进全局设置，而不是任何模型的专属值
      await postSettings(body, (saved) => {
        completeModelWrite(m, write, () => {
          const superseded = supersededSettings(write.before, GLOBAL_SETTINGS, saved);
          for (const key of keys) if (!superseded.has(key)) GLOBAL_SETTINGS[key] = saved[key];
          return superseded;
        });
        toast('已保存为全局默认');
      });
      return;
    }
    const r = await fetch('/api/model-overrides/' + encodeURIComponent(m), {
      signal: AbortSignal.timeout(30000),
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body)
    });
    const d = await r.json();
    if (r.ok) {
      if (!applyModelReceipt(m, d, write, { patch, inherit })) return;
      toast('已保存 ' + m + ' 专属参数');
    } else toast(await responseError(r, '保存失败', d));
  } catch (e) {
    toast('❌ 网络请求失败');
  } finally {
    MODEL_WRITES.delete(m);
  }
}
async function clearModelOverride() {
  if (!capabilitiesLoaded) {
    toast('模型配置尚未加载，请先重新加载配置');
    return;
  }
  const m = $('model-sel').value;
  if (!m || MODEL_WRITES.has(m)) return;
  if (!OVERRIDES[m]) {
    toast('该模型没有专属参数');
    return;
  }
  if (!confirm('清除 ' + m + ' 的全部专属参数及本页草稿，恢复全局 / 内置默认？')) return;
  const write = {
    keys: new Set(PER_MODEL_KEYS),
    later: new Map(),
    before: settingsExpectation(modelOwnedFields(m), PER_MODEL_KEYS)
  };
  MODEL_WRITES.set(m, write);
  try {
    const r = await fetch('/api/model-overrides/' + encodeURIComponent(m), {
      signal: AbortSignal.timeout(30000),
      method: 'DELETE',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ expected_override: SAVED_MODEL_OVERRIDES[m] || {} })
    });
    if (r.ok) {
      const d = await r.json();
      if (!applyModelReceipt(m, d, write, { reset: true })) return;
      toast('已恢复 ' + m + ' 默认参数');
    } else toast(await responseError(r, '清除失败'));
  } catch (e) {
    toast('❌ 网络请求失败');
  } finally {
    MODEL_WRITES.delete(m);
  }
}
function refreshModelSelStars() {
  const sel = $('model-sel');
  const cur = sel.value;
  [...sel.options].forEach((o) => {
    const base = o.value;
    o.textContent =
      (base === '__global__' ? '＊ 全局默认（对所有模型）' : base + (OVERRIDES[base] ? ' ★' : '')) +
      (MODEL_DRAFTS.has(base) ? ' · 草稿' : '');
  });
  sel.value = cur;
}
function chip(t, accent, sampling = false) {
  return `<span class="pill${accent ? ' pill-accent' : ''}"${sampling ? ' data-sampling' : ''}>${esc(t)}</span>`;
}
function fillARFor(cap) {
  const sel = $('image_aspect_ratio');
  const list =
    cap?.is_image && cap.image_aspect_ratios?.length ? cap.image_aspect_ratios : COMMON_ARS;
  sel.innerHTML =
    '<option value="">自动</option>' +
    list.map((a) => `<option value="${esc(a)}">${esc(a)}</option>`).join('');
  if (curAR && !list.includes(curAR))
    sel.insertAdjacentHTML(
      'beforeend',
      `<option value="${esc(curAR)}" disabled>${esc(curAR)}（当前模型不支持）</option>`
    );
  sel.value = curAR || '';
}
function renderCaps(capture = true) {
  if (capture) captureModelDraft();
  const m = $('model-sel').value;
  if (m === '__global__') {
    applyModelParamFields(m);
    $('anti_truncation_mode').disabled = true;
    $('anti-truncation-note').textContent =
      '请先选择具体模型再设置其后缀；普通无后缀名称始终不启用抗截断。';
    $('caps-summary').innerHTML =
      '<span class="text-xs text-neutral-500">全局默认作用域：下面的值对所有未设 ★ 专属的模型生效。切到具体模型可查看该模型的能力与专属值。</span>';
    ['thinking_g3_level', 'image_size', 'image_aspect_ratio'].forEach((id) => {
      const el = $(id);
      if (el) el.disabled = false;
    });
    $('wrap-g3level').style.opacity = '1';
    $('think-note').textContent =
      '全局默认：具体下发时仍按各模型能力自动裁剪（例如 Pro 上 minimal 会就近取 low）。';
    $('image-note').textContent = '全局默认：仅生图模型会用到。';
    $('sampling-note').textContent = '全局默认：已废弃采样参数的模型会自动剥离。';
    fillARFor(null);
    finishModelRender(m);
    return;
  }
  const cap = CAPS[m];
  if (!cap) return;
  applyModelParamFields(m); // 按模型填入专属/全局参数值（含 curAR）
  $('anti_truncation_mode').disabled = !cap.supports_anti_truncation;
  $('anti-truncation-note').textContent = cap.supports_anti_truncation
    ? '保存为该模型专属后刷新客户端列表；仅选择已开启的抗截断后缀才使用抗截断，普通名称不启用。'
    : '该模型不支持文本函数传输，不提供抗截断版本。';
  const th = cap.thinking || {};
  let sum = [chip('家族 ' + cap.family, true)];
  if (th.kind === 'level') sum.push(chip('思考档位 ' + (th.levels || []).join(' / ')));
  else sum.push(chip('无思考调节'));
  if (cap.is_image) {
    sum.push(chip('分辨率 ' + cap.image_sizes.join('/')));
    sum.push(chip('比例 ' + cap.image_aspect_ratios.length + ' 种'));
    sum.push(chip('不支持函数调用'));
  } else {
    sum.push(chip('采样', false, true));
    sum.push(chip('支持函数调用'));
  }
  if (cap.supports_search) sum.push(chip('支持搜索'));
  $('caps-summary').innerHTML = sum.join('');

  const isLevel = th.kind === 'level';
  $('thinking_g3_level').disabled = !isLevel;
  $('wrap-g3level').style.opacity = isLevel ? '1' : '.4';
  let tn = '';
  if (cap.is_image) tn = '生图模型思考档位（默认 high，可在上方调整）。';
  else if (isLevel)
    tn =
      '该模型用思考档位' +
      (th.levels && !th.levels.includes('minimal')
        ? '（此模型最低 low，无 minimal，且无法关闭）'
        : '（无法完全关闭，最省为 minimal）') +
      '。';
  else tn = '该模型不支持思考调节。';
  $('think-note').textContent = tn;

  const imgOn = cap.is_image;
  $('image_size').disabled = !imgOn;
  $('image_aspect_ratio').disabled = !imgOn;
  fillARFor(cap);
  $('image-note').textContent = imgOn
    ? '该模型支持比例：' +
      cap.image_aspect_ratios.join('、') +
      '；分辨率 ' +
      cap.image_sizes.join('/') +
      '。选到不支持的比例不会报错，会自动回退为“由模型决定”。'
    : '仅生图模型（名称含 image）使用此项。';

  finishModelRender(m);
}
function updateSamplingDisplay() {
  const model = $('model-sel').value,
    cap = CAPS[model],
    policy = $('sampling_policy').value;
  if (model === '__global__') {
    $('sampling-note').textContent =
      policy === 'allowed'
        ? '全局默认选择强制保留采样；生图模型仍会剥离，请确认其他模型支持。'
        : policy === 'deprecated'
          ? '全局默认选择强制剥离采样参数。'
          : '全局默认：按各模型内置规则自动处理采样参数。';
    return;
  }
  if (!cap) return;
  const advice =
    policy === 'auto'
      ? (cap.default_sampling_advice ?? cap.sampling_advice)
      : policy === 'deprecated'
        ? 'deprecated'
        : 'recommend_default';
  const label = cap.is_image
    ? '生图模型 · 自动剥离'
    : policy === 'allowed'
      ? '强制保留'
      : policy === 'deprecated'
        ? '强制剥离'
        : advice === 'deprecated'
          ? '自动剥离'
          : '自动判定';
  const chip = $('caps-summary').querySelector('[data-sampling]');
  if (chip) chip.textContent = '采样 ' + label;
  $('sampling-note').textContent = cap.is_image
    ? '生图模型会自动剥离采样参数。'
    : policy === 'allowed'
      ? '当前选择强制保留采样参数；请确认上游模型支持此设置。保存后生效。'
      : policy === 'deprecated'
        ? '当前选择强制剥离 temperature / top_p / top_k；保存后生效，其他参数仍按模型能力处理。'
        : advice === 'deprecated'
          ? '⚠️ 该模型已废弃 temperature/top_p/top_k（官方要求移除，现忽略、未来 400），代理会自动移除；如需更确定的输出请改用系统指令。candidate_count 在 3.x 也不支持。'
          : advice === 'recommend_default'
            ? '官方建议 Gemini 3.x 保持采样默认值（可调，但改动可能致循环/降智）；留空即用模型默认。'
            : '该模型支持采样参数；留空即用模型默认。';
}
function numOr(id, d) {
  const v = $(id).value.trim();
  return v === '' ? d : Number(v);
}
// 说明折叠：点 ⓘ 展开/收起对应的说明块
function hlp(el, id) {
  const b = $(id);
  const on = b.classList.toggle('show');
  b.hidden = !on;
  el.classList.toggle('on', on);
  el.setAttribute('aria-expanded', String(on));
  el.setAttribute('aria-label', on ? '收起说明' : '展开说明');
}
async function saveSettings() {
  if (!settingsLoaded) {
    toast('设置尚未加载，请先重新加载配置');
    return;
  }
  // 只保存全局运行设置。
  // 模型行为字段显示的是当前编辑对象的生效值，不能在这里一并提交。
  // 一起提交会把某个模型的专属值（比如生图的注入内容）写成全局默认。
  const values = {
    img_compress_enabled: $('img_compress_enabled').checked,
    img_compress_max_dim: numOr('img_compress_max_dim', 1536),
    img_compress_max_mb: numOr('img_compress_max_mb', 1.5),
    img_compress_quality: numOr('img_compress_quality', 85),
    retry_max: numOr('retry_max', 10),
    retry_backoff_seconds: numOr('retry_backoff_seconds', 5),
    retry_jitter_seconds: numOr('retry_jitter_seconds', 1),
    cross_channel_fallback: $('cross_channel_fallback').checked,
    fake_streaming: $('fake_streaming').checked,
    fake_streaming_interval: numOr('fake_streaming_interval', 1),
    safety_score: $('safety_score').checked,
    cookie_debug: $('cookie_debug').checked,
    cookie_structured_output: $('cookie_structured_output').checked,
    debug_outbound: $('debug_outbound').checked,
    prefill_mode: $('prefill_mode').value,
    express_location: $('express_location').value,
    prefill_suppress_thinking: $('prefill_suppress_thinking').checked,
    resumable_enabled: $('resumable_enabled').checked,
    resumable_disconnect_grace_seconds: numOr('resumable_disconnect_grace_seconds', 10),
    resumable_ttl_seconds: numOr('resumable_ttl_seconds', 1800),
    resumable_max_streams: numOr('resumable_max_streams', 200),
    request_timeout_seconds: numOr('request_timeout_seconds', 1800),
    upstream_max_concurrency: numOr('upstream_max_concurrency', 32),
    resumable_max_buffer_mb: numOr('resumable_max_buffer_mb', 64),
    resumable_max_total_mb: numOr('resumable_max_total_mb', 256),
    keepalive_url: $('keepalive_url').value.trim(),
    keepalive_interval: numOr('keepalive_interval', 60),
    cookie_auto_refresh: $('cookie_auto_refresh').checked,
    cookie_refresh_interval_seconds: numOr('cookie_refresh_interval_seconds', 600)
  };
  const m = $('model-sel').value;
  const overriding = !!(OVERRIDES[m] && Object.keys(OVERRIDES[m]).length > 0);
  await postFormSettings(
    values,
    () => toast(overriding ? '全局运行设置已保存；模型专属参数保持不变' : '全局运行设置已保存'),
    'runtime'
  );
}
