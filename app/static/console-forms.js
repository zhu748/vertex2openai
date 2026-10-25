/* Form state lives only in this page. Never persist credentials in browser storage. */
(() => {
  const snapshots = new Map(),
    accountOpen = new Map();
  const groups = () => [...document.querySelectorAll('[data-draft-group]')];
  const group = (name) => document.querySelector(`[data-draft-group="${name}"]`);
  const controls = (el) =>
    [...el.querySelectorAll('input,select,textarea')].filter(
      (input) =>
        !input.hasAttribute('data-local') &&
        !input.closest('.credential-filter') &&
        !input.closest('.field-source')
    );
  const value = (input) => (input.type === 'checkbox' ? input.checked : input.value);
  // Compare editable credential fields only. Probe diagnostics are saved by
  // the server and must not masquerade as unsaved edits after a row rerender.
  const keyState = (records) =>
    records.flatMap((record, index) =>
      Object.entries({
        key: record.key || '',
        project_id: record.project_id || '',
        note: record.note || '',
        enabled: record.enabled !== false,
        proxies: proxyDraft(record.proxies ?? record.proxy),
        proxy_fallback: record.proxy_fallback !== false
      }).map(([field, value]) => ({ id: `key-${index}-${field}`, value }))
    );
  const read = (el) => {
    if (el.dataset.draftGroup === 'keys')
      return keyState(
        [...el.querySelectorAll('[data-i]')].map((row) => ({
          key: row.querySelector('.k-key').value.trim(),
          project_id: projectFromInput(row.querySelector('.k-project').value),
          note: row.querySelector('.k-note').value.trim(),
          enabled: row.querySelector('.k-enabled').checked,
          proxies: proxyDraft(row.querySelector('.k-proxy').value),
          proxy_fallback: row.querySelector('.k-fallback').checked
        }))
      );
    return controls(el).map((input) => ({ id: input.id, value: value(input) }));
  };
  const fingerprint = (el) => JSON.stringify(read(el));
  function layout() {
    const nav = [...document.querySelectorAll('.tab-bar,.mobile-navigation')].find(
      (el) => el.getClientRects().length
    );
    const scope = $('model-scope');
    const scopeHeight =
      document.body.dataset.view === 'params' && getComputedStyle(scope).position === 'sticky'
        ? scope.getBoundingClientRect().height
        : 0;
    document.documentElement.style.setProperty(
      '--console-nav-height',
      (nav?.getBoundingClientRect().height || 0) + 'px'
    );
    document.documentElement.style.setProperty('--model-scope-height', scopeHeight + 'px');
  }
  function changed(name) {
    const el = group(name);
    return el && snapshots.has(name) && fingerprint(el) !== JSON.stringify(snapshots.get(name));
  }
  function refresh(name) {
    const targets = typeof name === 'string' ? [group(name)].filter(Boolean) : groups();
    for (const el of targets) {
      const name = el.dataset.draftGroup,
        dirty = changed(name),
        bar = el.querySelector('.draft-toolbar');
      if (!bar) continue;
      if (!snapshots.has(name)) {
        bar.querySelector('[role=status]').textContent = '配置尚未加载';
        bar.querySelector('[data-discard]').disabled = true;
        continue;
      }
      const baseline = snapshots.get(name) || [],
        current = read(el);
      const count =
        current.reduce(
          (n, item, i) => n + (JSON.stringify(item) !== JSON.stringify(baseline[i])),
          0
        ) + Math.max(0, baseline.length - current.length);
      bar.querySelector('[role=status]').textContent = dirty
        ? `${count} 项修改未保存`
        : '修改已保存';
      bar.classList.toggle('is-dirty', !!dirty);
      bar.querySelector('[data-discard]').disabled = !dirty;
    }
  }
  function saved(name) {
    const el = group(name);
    if (el) {
      snapshots.set(name, read(el));
      refresh();
    }
  }
  function rebaseKeys() {
    snapshots.set('keys', keyState(SAVED_KEYS));
    refresh('keys');
  }
  function capture() {
    return Object.fromEntries(
      groups()
        .filter((el) => changed(el.dataset.draftGroup))
        .map((el) => [el.dataset.draftGroup, read(el)])
    );
  }
  function loaded(drafts = {}) {
    for (const name of ['runtime', 'proxy', 'fingerprint', 'express-fingerprint']) {
      saved(name);
      for (const field of drafts?.[name] || []) {
        const el = $(field.id);
        if (el) {
          if (el.type === 'checkbox') el.checked = field.value;
          else el.value = field.value;
        }
      }
    }
    updateExpressCustomVis();
    updateProxyDraft();
    refresh();
  }
  function dirty() {
    return (
      MODEL_DRAFTS.size > 0 ||
      window.consoleChannels?.hasDraft?.() ||
      groups().some((el) => changed(el.dataset.draftGroup))
    );
  }
  function discard(name) {
    if (name === 'keys') {
      KEYS = SAVED_KEYS.map(credentialCopy);
      renderKeys();
    } else if (name === 'cookies') {
      window.consoleChannels?.discard();
    } else
      for (const field of snapshots.get(name) || []) {
        if ($(field.id)) setV(field.id, field.value);
      }
    updateExpressCustomVis();
    updateProxyDraft();
    refresh();
  }
  function fieldError(input, message) {
    const id = input.id + '-error';
    let error = $(id);
    if (!error) {
      error = document.createElement('p');
      error.id = id;
      error.className = 'field-error';
      error.setAttribute('role', 'alert');
      const anchor = input.parentElement.classList.contains('flex') ? input.parentElement : input;
      anchor.after(error);
    }
    error.textContent = message;
    input.setAttribute('aria-invalid', 'true');
    const refs = new Set(
      (input.getAttribute('aria-describedby') || '').split(/\s+/).filter(Boolean)
    );
    refs.add(id);
    input.setAttribute('aria-describedby', [...refs].join(' '));
    for (let el = input.parentElement; el; el = el.parentElement)
      if (el.tagName === 'DETAILS') el.open = true;
  }
  function clearError(input) {
    $(input.id + '-error')?.remove();
    input.removeAttribute('aria-invalid');
    const refs = (input.getAttribute('aria-describedby') || '')
      .split(/\s+/)
      .filter((id) => id && id !== input.id + '-error');
    if (refs.length) input.setAttribute('aria-describedby', refs.join(' '));
    else input.removeAttribute('aria-describedby');
  }
  function validate(button) {
    if (!/保存/.test(button.textContent) || button.closest('dialog')) return true;
    const container = button.closest('fieldset');
    if (!container) return true;
    let first = null;
    for (const input of controls(container)) {
      if (input.disabled || !input.willValidate) continue;
      // Inherited unsupported values may be displayed but are never submitted.
      if (
        container.classList.contains('model-scope-section') &&
        $('model-sel').value !== '__global__' &&
        !modelEdits.has(input.id)
      )
        continue;
      if (!input.checkValidity()) {
        fieldError(input, input.validationMessage);
        first ||= input;
      } else clearError(input);
    }
    if (first) {
      first.scrollIntoView({ block: 'center' });
      first.focus({ preventScroll: true });
      toast('请修正标出的字段后再保存');
      return false;
    }
    return true;
  }
  function filterAccounts(kind) {
    const query = $(kind + '-search')
        .value.trim()
        .toLowerCase(),
      status = $(kind + '-filter').value;
    let visible = 0;
    for (const row of $(kind + '-list').children) {
      const prefix = kind === 'keys' ? 'k' : 'c';
      const enabled = row.querySelector('.' + prefix + '-enabled').checked;
      const text = [
        row.querySelector('.' + prefix + '-note').value,
        row.querySelector('.' + prefix + '-project').value,
        kind === 'keys' ? row.querySelector('.k-key').value.slice(-6) : ''
      ]
        .join(' ')
        .toLowerCase();
      row.hidden = !(
        text.includes(query) &&
        (status === 'all' || enabled === (status === 'enabled'))
      );
      if (!row.hidden) visible++;
    }
    $(kind + '-visible').textContent = `显示 ${visible} / ${$(kind + '-list').children.length} 个`;
  }
  function openAccount(scope, index) {
    const kind = scope === 'key' ? 'keys' : 'cookies';
    $(kind + '-search').value = '';
    $(kind + '-filter').value = 'all';
    filterAccounts(kind);
    const row = document.querySelector(
      scope === 'key' ? `#keys-list [data-i="${index}"]` : `[data-cookie-index="${index}"]`
    );
    if (row) row.querySelector('details').open = true;
  }
  function rememberAccounts(scope) {
    const kind = scope === 'key' ? 'keys' : 'cookies';
    accountOpen.set(
      scope,
      new Set(
        [...$(kind + '-list').children].flatMap((row, i) =>
          row.querySelector('details')?.open ? [i] : []
        )
      )
    );
  }
  function accounts(scope) {
    const kind = scope === 'key' ? 'keys' : 'cookies',
      prefix = scope === 'key' ? 'k' : 'c';
    for (const [index, row] of [...$(kind + '-list').children].entries()) {
      const fields = row.querySelector('.key-fields');
      if (!fields || row.querySelector('details')) continue;
      const details = document.createElement('details');
      details.className = 'account-editor';
      details.open = accountOpen.get(scope)?.has(index) || false;
      const note = row.querySelector('.' + prefix + '-note').value,
        project = row.querySelector('.' + prefix + '-project').value;
      const key = scope === 'key' ? row.querySelector('.k-key') : null;
      const summary = document.createElement('summary');
      summary.innerHTML = `<span class="account-name">${esc(note || `${scope === 'key' ? 'Key' : 'Cookie'} ${index + 1}`)}</span><span class="account-project">${esc(project || '尚未设置项目')}</span><span class="account-mask">${key ? '•••• ' + esc(key.value.slice(-6)) : 'Cookie 已隐藏'}</span><span class="account-edit-label">编辑配置</span>`;
      details.append(summary);
      fields.before(details);
      details.append(fields);
      if (key) {
        key.type = 'password';
        const toggle = document.createElement('button');
        toggle.type = 'button';
        toggle.className = 'secret-toggle';
        toggle.textContent = '显示 Key';
        toggle.setAttribute('aria-pressed', 'false');
        toggle.addEventListener('click', () => {
          const show = key.type === 'password';
          key.type = show ? 'text' : 'password';
          toggle.textContent = show ? '隐藏 Key' : '显示 Key';
          toggle.setAttribute('aria-pressed', String(show));
        });
        key.after(toggle);
      }
      for (const input of [
        row.querySelector('.' + prefix + '-note'),
        row.querySelector('.' + prefix + '-project')
      ])
        input.addEventListener('input', () => {
          summary.querySelector('.account-name').textContent =
            row.querySelector('.' + prefix + '-note').value ||
            `${scope === 'key' ? 'Key' : 'Cookie'} ${index + 1}`;
          summary.querySelector('.account-project').textContent =
            row.querySelector('.' + prefix + '-project').value || '尚未设置项目';
        });
    }
    filterAccounts(kind);
    refresh();
  }
  async function copyBase() {
    try {
      await navigator.clipboard.writeText($('api-base').value);
      toast('接入地址已复制');
    } catch {
      $('api-base').focus();
      $('api-base').select();
      toast('请复制已选中的接入地址');
    }
  }
  for (const el of groups()) {
    const bar = document.createElement('div');
    bar.className = 'draft-toolbar';
    bar.innerHTML = `<span role="status">正在加载配置…</span><button class="btn-ghost" type="button" data-discard disabled>撤销本区修改</button>`;
    bar.querySelector('button').addEventListener('click', () => discard(el.dataset.draftGroup));
    if (['keys', 'cookies'].includes(el.dataset.draftGroup)) {
      const button = document.createElement('button');
      button.type = 'button';
      button.className = 'btn';
      button.textContent = '保存本列表';
      button.addEventListener('click', () =>
        runAction(button, el.dataset.draftGroup === 'keys' ? saveExpressKeys : saveCookieAccounts)
      );
      bar.append(button);
    }
    if (el.dataset.draftGroup === 'runtime') $('runtime-save-bar').prepend(bar);
    else el.append(bar);
  }
  $('api-base').value = location.origin + '/v1';
  window.addEventListener('beforeunload', (event) => {
    if (dirty()) {
      event.preventDefault();
      event.returnValue = '';
    }
  });
  function refreshEditedGroup(event) {
    const el = event.target.closest('[data-draft-group]');
    if (el) refresh(el.dataset.draftGroup);
  }
  document.addEventListener('input', (event) => {
    clearError(event.target);
    refreshEditedGroup(event);
  });
  document.addEventListener('change', refreshEditedGroup);
  document.addEventListener('click', (event) => {
    if (event.target.closest('.key-actions')) queueMicrotask(refresh);
  });
  window.consoleForms = {
    saved,
    rebaseKeys,
    capture,
    loaded,
    refresh,
    dirty,
    discard,
    fieldError,
    clearError,
    validate,
    accounts,
    rememberAccounts,
    openAccount,
    filterAccounts,
    copyBase,
    layout
  };
  if (window.ResizeObserver) {
    const observer = new ResizeObserver(layout);
    document
      .querySelectorAll('.tab-bar,.mobile-navigation,.scope-card')
      .forEach((el) => observer.observe(el));
  }
  window.addEventListener('resize', layout);
  layout();
  // Initialization may already have completed when a cached optional script loads.
  if (settingsLoaded) loaded();
  if (keysLoaded) {
    accounts('key');
    saved('keys');
  }
})();
