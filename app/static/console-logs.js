/* Bounded log feed and local filters. Exposes only the view activation hook. */
(() => {
  /* ---------- Logs: filtering is local; the bounded SSE feed stays connected. ---------- */
  const logwin = $('logwin');
  let autoscroll = true;
  const LOG_DOM_MAX = 500;
  function setLogFollow(value) {
    autoscroll = value;
    $('log-follow').textContent = '自动滚动：' + (value ? '开' : '关');
    $('log-follow').setAttribute('aria-pressed', String(value));
  }
  logwin.addEventListener('scroll', () => {
    if (autoscroll && logwin.scrollHeight - logwin.scrollTop - logwin.clientHeight >= 50)
      setLogFollow(false);
  });
  function logLevel(text) {
    if (/❌|ERROR|失败/.test(text)) return 'error';
    if (/⚠️|WARN|🔄|重试/.test(text)) return 'warning';
    return 'info';
  }
  function logLine(text) {
    const level = logLevel(text);
    const content = esc(text)
      .replace(/(gemini-[a-zA-Z0-9.\-]+)/g, '<span class="log-model">$1</span>')
      .replace(
        /\b(trace-[a-f0-9]{16})\b/g,
        '<button class="request-link" data-request="$1">$1</button>'
      );
    return `<div class="log-line ${level}" data-level="${level}">${content}</div>`;
  }
  function filterLogs() {
    const query = $('log-search').value.trim().toLowerCase(),
      level = $('log-level').value;
    const noFilter = query === '' && level === 'all';
    let visible = 0;
    if (noFilter && !logwin.querySelector('[hidden]')) {
      // 无过滤且没有隐藏行：跳过对最多 500 行 DOM 的全量文本读取
      // （每条 SSE 日志都会走到这里，突发时是每秒数千次字符串物化）。
      visible = logwin.childElementCount;
    } else {
      for (const row of logwin.children) {
        row.hidden =
          !noFilter &&
          !(
            row.textContent.toLowerCase().includes(query) &&
            (level === 'all' || row.dataset.level === level)
          );
        if (!row.hidden) visible++;
      }
    }
    $('log-count').textContent = `${visible} / ${logwin.childElementCount} 条日志`;
    logwin.dataset.empty = String(visible === 0 && logwin.childElementCount > 0);
  }
  let pendingLogs = [];
  let renderTimer = null;
  function flushLogs() {
    if (renderTimer) clearTimeout(renderTimer);
    renderTimer = null;
    if (!pendingLogs.length) return;
    const batch = pendingLogs;
    pendingLogs = [];
    logwin.insertAdjacentHTML('beforeend', batch.map(logLine).join(''));
    while (logwin.childElementCount > LOG_DOM_MAX) logwin.firstElementChild.remove();
    filterLogs();
    if (autoscroll && !$('view-logs').classList.contains('hidden'))
      logwin.scrollTop = logwin.scrollHeight;
  }
  function updateFilter() {
    flushLogs();
    filterLogs();
  }
  $('log-search').addEventListener('input', updateFilter);
  $('log-level').addEventListener('change', updateFilter);
  $('log-follow').addEventListener('click', () => {
    setLogFollow(!autoscroll);
    if (autoscroll) logwin.scrollTop = logwin.scrollHeight;
  });
  $('log-clear').addEventListener('click', () => {
    pendingLogs = [];
    if (renderTimer) clearTimeout(renderTimer);
    renderTimer = null;
    logwin.replaceChildren();
    filterLogs();
  });
  try {
    const es = new EventSource('/stream-logs');
    es.onopen = () => {
      $('log-state').textContent = '已连接';
      $('log-connection').className = 'status-dot';
    };
    es.onerror = () => {
      $('log-state').textContent = '连接中断，正在重连…';
      $('log-connection').className = 'status-dot error';
    };
    es.onmessage = (event) => {
      if (event.data === 'keep-alive') return;
      // Bound both the pending batch and retained DOM. One append/filter/scroll
      // per burst avoids a forced layout for every incoming SSE message.
      pendingLogs.push(event.data);
      if (pendingLogs.length > LOG_DOM_MAX) pendingLogs.shift();
      if (!renderTimer) renderTimer = setTimeout(flushLogs, 50);
    };
  } catch (error) {
    $('log-state').textContent = '无法建立日志连接';
    $('log-connection').className = 'status-dot error';
  }

  window.consoleLogs = {
    onShow() {
      flushLogs();
      if (autoscroll) logwin.scrollTop = logwin.scrollHeight;
    }
  };
})();
