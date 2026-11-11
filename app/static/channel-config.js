/* Cookie 账号列表（QUA-P1-4：代理编辑器已拆至 proxy-editor.js，经
 * window.proxyEditorKit 显式命名空间协作）。Shared form helpers live in dashboard.js.
 * Keep state local so a failed/cached script can be retried without redeclaration errors. */
(() => {
  if (window.consoleChannels) return;
  let cookiesRevision = null;
  let COOKIE_ACCOUNTS = [],
    SAVED_COOKIES = [],
    cookiesLoaded = false;
  let cookieRefreshPending = false,
    cookieTestPending = false;

  async function updateCredentialStrategy(select) {
    if (!settingsLoaded) {
      toast('设置尚未加载，请先重新加载配置');
      return;
    }
    const previous = GLOBAL_SETTINGS.roundrobin;
    select.disabled = true;
    const roundrobin = select.value === 'roundrobin';
    const ok = await postSettings({ roundrobin });
    if (ok) {
      GLOBAL_SETTINGS.roundrobin = roundrobin;
      toast('账号调度已切换为' + (roundrobin ? '按顺序轮询' : '随机使用'));
    } else {
      select.value = previous ? 'roundrobin' : 'random';
    }
    select.disabled = false;
  }

  /* Cookie 会话探测结果文案：与后端 cookie_probe 的三种状态一一对应。 */
  const COOKIE_TEST_STATUS = {
    valid: { label: '✓ 会话有效', ok: true },
    invalid: { label: '✕ 会话已失效', ok: false },
    error: { label: '⚠ 无法判定', ok: false }
  };
  function cookieTestMessage(res) {
    const st = COOKIE_TEST_STATUS[res.status] || { label: res.status, ok: false };
    const secs = res.elapsed_ms > 0 ? ` · ${(res.elapsed_ms / 1000).toFixed(1)}s` : '';
    return st.label + (res.detail ? ' · ' + res.detail : '') + secs;
  }
  /* 调度健康 pill：账号被自动冷却隔离或连续失败时才显示，正常状态保持安静。 */
  function cookieHealthPill(rec) {
    const health = rec.health;
    if (!health) return '';
    if (Number(health.cooldown_remaining) > 0)
      return `<span class="pill" style="color:#b45309" title="鉴权失败累计，调度已临时隔离该账号">冷却中 ${Math.ceil(Number(health.cooldown_remaining))}s</span>`;
    if (Number(health.failures) > 0)
      return `<span class="pill" style="color:#b45309" title="近期鉴权失败 ${health.failures} 次，再失败可能进入冷却">失败 ${health.failures} 次</span>`;
    return '';
  }
  function cookieSecretControls(row, record, index) {
    if (!record.id || !record.cookie_configured) return;
    const input = row.querySelector('.c-cookie');
    const actions = document.createElement('div');
    actions.className = 'cookie-secret-actions';
    const toggle = document.createElement('button');
    toggle.type = 'button';
    toggle.className = 'secret-toggle c-cookie-reveal';
    toggle.textContent = '显示 Cookie';
    toggle.setAttribute('aria-expanded', 'false');
    const copy = document.createElement('button');
    copy.type = 'button';
    copy.className = 'secret-toggle c-cookie-copy';
    copy.textContent = '复制 Cookie';
    const preview = document.createElement('textarea');
    preview.id = 'cookie-saved-' + index;
    preview.className = 'inp log cookie-secret-preview';
    preview.rows = 3;
    preview.readOnly = true;
    preview.hidden = true;
    preview.spellcheck = false;
    preview.setAttribute('autocomplete', 'off');
    preview.setAttribute('aria-label', '已保存的 Cookie ' + (index + 1));
    // Viewing a credential must never turn it into a replacement draft or
    // overwrite a newer value saved by automatic Cookie refresh.
    preview.dataset.local = '';
    toggle.setAttribute('aria-controls', preview.id);
    actions.append(toggle, copy);
    input.after(actions, preview);
    let busy = false;
    const show = (cookie) => {
      preview.value = cookie;
      preview.hidden = false;
      toggle.textContent = '隐藏 Cookie';
      toggle.setAttribute('aria-expanded', 'true');
    };
    const hide = () => {
      preview.value = '';
      preview.hidden = true;
      toggle.textContent = '显示 Cookie';
      toggle.setAttribute('aria-expanded', 'false');
    };
    async function useCookie(action) {
      if (busy) return;
      if (action === 'show' && !preview.hidden) {
        hide();
        return;
      }
      busy = true;
      toggle.disabled = copy.disabled = true;
      actions.setAttribute('aria-busy', 'true');
      try {
        // SEC-P3-4：读取 Cookie 明文前需二次确认控制台口令——会话 Cookie
        // 被劫持（XSS/设备失窃）时，攻击者没有口令仍拿不到凭证本体。
        const password = window.prompt(
          '为确认是你本人操作，请输入控制台登录口令以读取 Cookie 明文：'
        );
        if (password === null) return;
        const response = await fetch('/api/cookie-accounts/reveal', {
          method: 'POST',
          cache: 'no-store',
          signal: AbortSignal.timeout(15000),
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ id: record.id, password })
        });
        const result = await response.json();
        if (!response.ok)
          throw new Error(await responseError(response, '读取 Cookie 失败', result));
        if (!row.isConnected) return;
        if (result.id !== record.id || typeof result.cookie !== 'string' || !result.cookie)
          throw new Error('读取响应缺少该账号的 Cookie，请重新加载配置');
        if (action === 'show') {
          show(result.cookie);
          return;
        }
        if (!preview.hidden) preview.value = result.cookie;
        try {
          await navigator.clipboard.writeText(result.cookie);
          toast('Cookie 已复制');
        } catch {
          // Ordinary HTTP sites may not expose the Clipboard API.
          const wasHidden = preview.hidden;
          const focused = document.activeElement;
          show(result.cookie);
          preview.focus({ preventScroll: true });
          preview.select();
          let copied = false;
          try {
            copied = document.execCommand('copy');
          } catch {}
          if (copied) {
            if (wasHidden) hide();
            focused?.focus({ preventScroll: true });
            toast('Cookie 已复制');
          } else {
            toast('自动复制不可用，已选中 Cookie，请手动复制');
          }
        }
      } catch (error) {
        if (row.isConnected) toast('❌ ' + error.message);
      } finally {
        busy = false;
        toggle.disabled = copy.disabled = false;
        actions.removeAttribute('aria-busy');
      }
    }
    toggle.addEventListener('click', () => useCookie('show'));
    copy.addEventListener('click', () => useCookie('copy'));
  }
  function renderCookieAccounts() {
    if (!$('cookies-list')) return; // 分区尚未随页面就绪时保持静默（缓存旧文档/升级中）
    window.consoleForms?.rememberAccounts('cookie');
    $('cookies-list').innerHTML = COOKIE_ACCOUNTS.map(
      (rec, index) => `
    <div class="key-card cookie-card" data-cookie-index="${index}">
      <div class="key-head"><div class="key-title"><span class="pill pill-accent">Cookie ${index + 1}</span>
        <span class="pill">${rec.cookie_configured ? '已保存 Cookie' : '待填写 Cookie'}</span>${cookieHealthPill(rec)}</div>
        <div class="key-actions"><label><input type="checkbox" class="c-enabled" ${rec.enabled === false ? '' : 'checked'} aria-label="启用 Cookie ${index + 1}">启用</label>
          <button type="button" ${rec.enabled === false ? 'disabled title="启用并保存账号后可测试"' : rec.id ? '' : 'disabled title="保存账号后可测试"'} onclick="runAction(this,()=>testCookieAccount(${index}))">测试会话</button>
          <button type="button" ${rec.enabled === false ? 'disabled title="启用并保存账号后可刷新"' : ''} onclick="runAction(this,()=>refreshCookies(${index}))">立即刷新</button>
          <button type="button" class="danger" onclick="removeCookieAccount(${index})" aria-label="删除 Cookie ${index + 1}">删除</button></div></div>
      <div class="key-fields">
        <div><label for="cookie-${index}">Google Cookie（含 HttpOnly 字段）</label>
          <textarea id="cookie-${index}" class="inp log c-cookie" rows="3" spellcheck="false" autocomplete="off" placeholder="${esc(rec.cookie_configured ? '已保存，留空保留原 Cookie；粘贴新值可更新' : '粘贴完整 Cookie 头或 Cookie-Editor JSON')}">${esc(rec.cookie || '')}</textarea>
          ${rec.cookie_preview ? `<div class="key-meta">${esc(rec.cookie_preview)}</div>` : ''}</div>
        <div><label for="cookie-project-${index}">Project ID</label><input id="cookie-project-${index}" class="inp log c-project" spellcheck="false" placeholder="此账号对应的项目 ID 或控制台 URL" value="${esc(rec.project_id)}">
          <label class="note-label" for="cookie-note-${index}">备注</label><input id="cookie-note-${index}" class="inp c-note" placeholder="便于区分账号" value="${esc(rec.note)}"></div>
        ${accountProxyFields('cookie', index, rec)}
      </div>
      ${rec._msg ? `<div class="key-meta ${rec._msg_ok === false ? 'error-message' : ''}" role="status">${esc(rec._msg)}</div>` : ''}
    </div>`
    ).join('');
    window.consoleForms?.accounts('cookie');
    [...$('cookies-list').children].forEach((row, index) =>
      cookieSecretControls(row, COOKIE_ACCOUNTS[index], index)
    );
    $('cookies-count').textContent = COOKIE_ACCOUNTS.length + ' 个';
    $('cookies-empty').classList.toggle('hidden', COOKIE_ACCOUNTS.length > 0);
  }
  function collectCookieAccounts() {
    document.querySelectorAll('[data-cookie-index]').forEach((row) => {
      const rec = COOKIE_ACCOUNTS[Number(row.dataset.cookieIndex)];
      // DOM 与数组失步（多标签页/并发重渲染）时补建记录而不是抛错：
      // 保存动作必须总能把用户当前看到的内容完整提交。
      const target =
        rec ||
        (COOKIE_ACCOUNTS[Number(row.dataset.cookieIndex)] = { id: '', cookie_configured: false });
      target.cookie = row.querySelector('.c-cookie')?.value.trim() ?? '';
      target.project_id = projectFromInput(row.querySelector('.c-project')?.value ?? '');
      target.note = row.querySelector('.c-note')?.value.trim() ?? '';
      target.enabled = row.querySelector('.c-enabled')?.checked ?? true;
      target.proxies = proxyDraft(row.querySelector('.c-proxy')?.value ?? '');
      target.proxy = target.proxies[0] || '';
      target.proxy_fallback = row.querySelector('.c-fallback')?.checked ?? true;
    });
    return COOKIE_ACCOUNTS;
  }
  /* Cookie 账号列表：load/save/add/remove 编排由 credential-list.js 的
   * 共享骨架持有（2026-11 P2-6），此处只描述 Cookie 侧差异。 */
  const cookieList = credentialList.create({
    endpoint: '/api/cookie-accounts',
    itemsKey: 'accounts',
    noticeLabel: 'Cookie 账号',
    loadRetryHint: '请重新加载配置。',
    notLoadedToast: 'Cookie 账号尚未加载，请先重新加载配置',
    toastNoun: 'Cookie 账号',
    formsGroup: 'cookies',
    accountGroup: 'cookie',
    responseItems: (d) => {
      if (!Array.isArray(d.accounts)) {
        const error = new Error('保存响应缺少账号列表，请刷新面板后重试');
        error.ui = true;
        throw error;
      }
      return d.accounts;
    },
    getItems: () => COOKIE_ACCOUNTS,
    setItems: (v) => {
      COOKIE_ACCOUNTS = v;
    },
    getSaved: () => SAVED_COOKIES,
    setSaved: (v) => {
      SAVED_COOKIES = v;
    },
    getRevision: () => cookiesRevision,
    setRevision: (v) => {
      cookiesRevision = v;
    },
    isLoaded: () => cookiesLoaded,
    markLoaded: () => {
      cookiesLoaded = true;
    },
    render: renderCookieAccounts,
    collect: collectCookieAccounts,
    // The server assigns an account ID on save, including on ordinary HTTP sites
    // where crypto.randomUUID() is unavailable.
    blankRecord: () => ({
      cookie: '',
      project_id: '',
      note: '',
      enabled: true,
      proxies: [],
      proxy_fallback: true
    }),
    focusNew: (n) => $('cookie-' + n).focus()
  });
  async function loadCookieAccounts() {
    await cookieList.load();
  }
  function addCookieAccount() {
    cookieList.add();
  }
  function removeCookieAccount(index) {
    cookieList.remove(index);
  }
  async function saveCookieAccounts() {
    await cookieList.save();
  }

  async function refreshCookies(index = null) {
    if (!cookiesLoaded) {
      toast('Cookie 账号尚未加载，请先重新加载配置');
      return;
    }
    if (cookieRefreshPending) {
      toast('Cookie 正在刷新，请等待当前任务完成');
      return;
    }
    if (window.consoleForms?.capture()?.cookies) {
      toast('请先保存或撤销 Cookie 列表的修改，再刷新已保存凭证');
      return;
    }
    const accounts = SAVED_COOKIES.filter(
      (rec) =>
        rec.enabled !== false && rec.id && (index === null || rec.id === COOKIE_ACCOUNTS[index]?.id)
    );
    if (!accounts.length) {
      toast('没有匹配的已保存且启用的 Cookie 账号');
      return;
    }
    const show = (message) => {
      for (const id of ['cookie-refresh-result', 'cookie-accounts-refresh-result']) {
        const box = $(id);
        if (box) {
          box.textContent = message;
          box.hidden = false;
        }
      }
    };
    cookieRefreshPending = true;
    show('正在刷新 ' + accounts.length + ' 个已保存账号，请稍候…');
    try {
      // Each account has a 45-second server deadline plus a one-second gap.
      const response = await fetch('/api/cookie-accounts/refresh', {
        method: 'POST',
        signal: AbortSignal.timeout(15000 + accounts.length * 46000),
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(index === null ? {} : { id: accounts[0].id })
      });
      const result = await response.json();
      if (!response.ok) throw new Error(await responseError(response, '刷新失败', result));
      const labels = { refreshed: '已更新', unchanged: '无新令牌', error: '失败' };
      const lines = (result.results || []).map(
        (rec) => `${rec.label || rec.id}：${labels[rec.status] || rec.status} · ${rec.detail || ''}`
      );
      show(lines.join('\n') || '没有可刷新的账号');
      toast(
        `刷新完成：更新 ${result.refreshed || 0}，无新令牌 ${result.unchanged || 0}，失败 ${result.errors || 0}`
      );
      // Leave credential editors untouched: refreshing must never discard drafts
      // created in the other section while this request was in flight.
    } catch (error) {
      const message =
        error.name === 'TimeoutError' ? '请求超时，请在运行日志确认刷新结果' : error.message;
      show('刷新未完成：' + message);
      toast('❌ ' + message);
    } finally {
      cookieRefreshPending = false;
    }
  }

  /* ---------- Cookie 会话测试（零配额探测） ----------
   * 后端 POST /api/cookie-accounts/test 用不存在的模型名调一次 batchGraphql，
   * 不消耗生成配额，按 gRPC 状态码判定会话有效性（valid/invalid/error）。
   * 与「立即刷新」（RotateCookies 短期令牌续期）互补：刷新不校验会话，测试不续期。 */
  async function testCookieAccount(index = null) {
    if (!cookiesLoaded) {
      toast('Cookie 账号尚未加载，请先重新加载配置');
      return;
    }
    if (cookieTestPending) {
      toast('Cookie 正在测试，请等待当前任务完成');
      return;
    }
    if (window.consoleForms?.capture()?.cookies) {
      toast('请先保存或撤销 Cookie 列表的修改，再测试已保存凭证');
      return;
    }
    const accounts = SAVED_COOKIES.filter(
      (rec) =>
        rec.enabled !== false && rec.id && (index === null || rec.id === COOKIE_ACCOUNTS[index]?.id)
    );
    if (!accounts.length) {
      toast(index === null ? '没有已保存且启用的 Cookie 账号可测试' : '请先保存账号，再测试会话');
      return;
    }
    cookieTestPending = true;
    toast(index === null ? '正在测试 ' + accounts.length + ' 个已保存账号…' : '正在测试会话…');
    try {
      // 后端 4 路并发探测、单账号 45 秒总时限：批量预算按批次数而非账号数增长。
      const batches = Math.max(1, Math.ceil(accounts.length / 4));
      const response = await fetch('/api/cookie-accounts/test', {
        method: 'POST',
        signal: AbortSignal.timeout(15000 + batches * 46000),
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(index === null ? {} : { id: accounts[0].id })
      });
      const result = await response.json();
      if (!response.ok) throw new Error(await responseError(response, '测试失败', result));
      mergeCookieTestResults(result.results || []);
      toast(`测试完成：有效 ${result.valid || 0} / ${result.total || 0}`);
    } catch (error) {
      const message =
        error.name === 'TimeoutError' ? '请求超时，请在运行日志确认测试结果' : error.message;
      toast('❌ ' + message);
    } finally {
      cookieTestPending = false;
    }
  }
  function mergeCookieTestResults(results) {
    // 只更新诊断信息与健康 pill；保留未保存的草稿与行内编辑（与 Express mergeProbeResults 同口径）。
    (Array.isArray(results) ? results : []).forEach((res) => {
      const hit = COOKIE_ACCOUNTS.find((rec) => rec.id === res.id);
      if (!hit) return;
      hit._msg = cookieTestMessage(res);
      hit._msg_ok = COOKIE_TEST_STATUS[res.status]?.ok === true;
      // 只采用服务器健康快照；探测失败并不一定是凭证认证失败。
      if (res.health) hit.health = { ...res.health };
    });
    renderCookieAccounts();
  }

  window.consoleChannels = {
    // 草稿 = Cookie 账号草稿 || 代理编辑器草稿（后者经显式命名空间委托）。
    hasDraft: () => window.proxyEditorKit?.hasDraft?.() ?? false,
    discard: () => {
      COOKIE_ACCOUNTS = SAVED_COOKIES.map(credentialCopy);
      renderCookieAccounts();
    },
    load: loadCookieAccounts,
    refreshCookies,
    testCookieAccount,
    init: () => window.proxyEditorKit?.init?.(),
    open: (scope, index, view) => window.proxyEditorKit?.open?.(scope, index, view),
    sync: () => window.proxyEditorKit?.sync?.()
  };
  // 注意：refreshCookies 不在此处导出到 window——console-bootstrap.js 的
  // 同名全局包装器（含 ensureChannelConfig 懒加载与错误 toast）后加载并
  // 恒定遮蔽本导出；实际入口统一走包装器，命名空间内保留实现供其调用
  // （2026-11 审查收敛：此前三个出口指向两种行为，排障时误导）。
  // 代理编辑器的全局函数（editProxy / applyProxyEditor 等 HTML onclick
  // 契约）随代理段一并迁至 proxy-editor.js（QUA-P1-4）。
  window.proxyEditorKit?.mount({
    isLoaded: () => cookiesLoaded,
    collect: collectCookieAccounts,
    accounts: () => COOKIE_ACCOUNTS,
    render: renderCookieAccounts
  });
  Object.assign(window, {
    updateCredentialStrategy,
    loadCookieAccounts,
    addCookieAccount,
    removeCookieAccount,
    saveCookieAccounts,
    testCookieAccount
  });
})();
