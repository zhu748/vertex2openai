/* Overview counters and recent diagnostics. Network polls start in console-bootstrap.js. */
(() => {
  /* ---------- Overview ---------- */
  function renderHealth(success, error) {
    const completed = success + error;
    const percent = completed ? (success / completed) * 100 : 0;
    const rate = completed ? percent.toFixed(1) + '%' : '—';
    $('health-ring').style.setProperty('--rate', percent + '%');
    $('health-ring').style.setProperty('--ring-color', completed ? 'var(--danger)' : '#e8ebf3');
    $('health-ring').setAttribute(
      'aria-label',
      completed ? `成功率 ${rate}，成功 ${success}，错误 ${error}` : '暂无请求数据'
    );
    $('health-rate').textContent = rate;
    $('health-success').textContent = fmt(success);
    $('health-error').textContent = fmt(error);
    $('health-note').textContent = completed
      ? `共 ${fmt(completed)} 次成功 / 错误记录`
      : '有请求完成后显示成功率';
  }
  let statsLoading = false;
  async function fetchStats() {
    if (statsLoading) return;
    statsLoading = true;
    try {
      const response = await fetch('/api/stats', {
        cache: 'no-store',
        signal: AbortSignal.timeout(10000)
      });
      if (!response.ok)
        throw new Error(
          response.status === 401 ? '登录已过期，请重新登录' : '统计暂不可用，将自动重试'
        );
      const d = await response.json();
      $('s-total').textContent = fmt(d.total);
      $('s-success').textContent = fmt(d.success);
      $('s-error').textContent = fmt(d.error);
      $('s-retries').textContent = fmt(d.retries);
      $('t-prompt').textContent = fmt(d.prompt_tokens);
      $('t-comp').textContent = fmt(d.completion_tokens);
      const tokens = (d.prompt_tokens || 0) + (d.completion_tokens || 0);
      $('t-total').textContent = fmt(tokens);
      $('prompt-bar').style.width = (tokens ? (d.prompt_tokens / tokens) * 100 : 0) + '%';
      $('completion-bar').style.width = (tokens ? (d.completion_tokens / tokens) * 100 : 0) + '%';
      $('uptime').textContent =
        '已运行 ' +
        (d.uptime < 3600
          ? Math.floor(d.uptime / 60) + ' 分钟'
          : (d.uptime / 3600).toFixed(1) + ' 小时');
      window.requestMonitor?.summary(d.requests);
      renderHealth(d.success || 0, d.error || 0);
      $('stats-sync').textContent =
        '每 3 秒更新 · ' + new Date().toLocaleTimeString('zh-CN', { hour12: false });
      $('stats-sync').classList.remove('error');
      $('uptime').parentElement.classList.remove('service-offline');
    } catch (e) {
      $('stats-sync').textContent =
        e.name === 'TimeoutError' ? '统计连接超时，将自动重试' : e.message;
      $('stats-sync').classList.add('error');
      $('uptime').textContent = e.message.includes('登录已过期') ? '登录已过期' : '暂时无法连接';
      $('uptime').parentElement.classList.add('service-offline');
    } finally {
      statsLoading = false;
    }
  }

  /* Recent diagnostics are a bounded retained sample, not a lifetime counter. */
  let recentLoading = false;
  async function fetchRecentMetrics() {
    if (recentLoading || document.hidden || $('view-overview').classList.contains('hidden')) return;
    recentLoading = true;
    try {
      const d = await readJSON('/api/recent-metrics'),
        completed = d.success + d.error;
      $('recent-rate').textContent = completed
        ? ((d.success / completed) * 100).toFixed(1) + '%'
        : '—';
      const duration = (v) => (v == null ? '—' : (v / 1000).toFixed(1) + 's');
      $('recent-latency').textContent =
        duration(d.first_reply_p50_ms) + ' / ' + duration(d.first_reply_p95_ms);
      $('recent-errors').textContent = fmt(d.error);
      $('recent-note').textContent =
        `近 15 分钟接收且仍保留的主请求 ${d.sample_count} 条，首回复样本 ${d.reply_samples} 条；` +
        Object.entries(d.channels)
          .map(([name, c]) => `${name}：成功 ${c.success} / 失败 ${c.error} / 运行 ${c.active}`)
          .join('；') +
        '。重启和记录回收会缩短样本范围；取消不计入成功率。';
    } catch (error) {
      $('recent-note').textContent = '近期统计暂不可用：' + error.message;
    } finally {
      recentLoading = false;
    }
  }

  Object.assign(window, { fetchStats, fetchRecentMetrics });
})();
