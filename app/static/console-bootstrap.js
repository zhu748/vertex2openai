/* Console startup, optional editor recovery and shared UI event wiring. Load last. */
/* ---------- Project ID auto-extract ---------- */
$('image_aspect_ratio').addEventListener('change', (e) => {
  curAR = e.target.value;
});

/* init */
const channelAssets = {
  channels: {
    file: 'channel-config.js',
    ready: () => typeof window.consoleChannels?.isLoaded === 'function' && window.consoleChannels
  },
  proxy: {
    file: 'proxy-editor.js',
    ready: () =>
      typeof window.proxyEditorKit?.mount === 'function' &&
      typeof window.proxyEditorKit.init === 'function' &&
      typeof window.proxyEditorKit.open === 'function' &&
      window.proxyEditorKit
  }
};
const channelAssetLoads = new Map();
let cookieRowsLoading = null;
function channelAssetNotice(message) {
  let box = $('console-assets-error');
  if (!box) {
    box = document.createElement('div');
    box.id = 'console-assets-error';
    box.className = 'inline-notice hidden';
    box.setAttribute('role', 'alert');
    box.innerHTML =
      '<span></span><button type="button" class="btn-ghost" onclick="loadChannelFeatures(false)">重试编辑器</button>';
    document.querySelector('main').prepend(box);
  }
  box.classList.toggle('hidden', !message);
  box.querySelector('span').textContent = message;
}
function ensureChannelAsset(asset) {
  const current = asset.ready();
  if (current) return Promise.resolve(current);
  if (!channelAssetLoads.has(asset)) {
    const loading = new Promise((resolve, reject) => {
      const script = document.createElement('script');
      const url = new URL('/static/' + asset.file, location.href);
      url.searchParams.set('retry', Date.now());
      script.src = url.href;
      script.dataset.cfasync = 'false';
      const timer = setTimeout(() => finish(new Error('账号与代理编辑器加载超时')), 10000);
      function finish(error) {
        clearTimeout(timer);
        script.onload = script.onerror = null;
        if (error) {
          script.remove();
          reject(error);
        } else resolve(asset.ready());
      }
      script.onload = () => finish(asset.ready() ? null : new Error('账号与代理编辑器版本不一致'));
      script.onerror = () => finish(new Error('账号与代理编辑器加载失败'));
      document.head.append(script);
    }).finally(() => channelAssetLoads.delete(asset));
    channelAssetLoads.set(asset, loading);
  }
  return channelAssetLoads.get(asset);
}
async function readyChannelRows(reload) {
  const channels = await ensureChannelAsset(channelAssets.channels);
  if (reload || !channels.isLoaded()) {
    cookieRowsLoading ||= channels.load().finally(() => (cookieRowsLoading = null));
    await cookieRowsLoading;
  }
  return channels;
}
async function ensureChannelConfig(reload = false) {
  // A missing proxy asset must not prevent independent Cookie rows from loading.
  // Conversely, a successful account editor alone is not a working proxy dialog.
  const [channels] = await Promise.all([
    readyChannelRows(reload),
    ensureChannelAsset(channelAssets.proxy)
  ]);
  channels.init();
  return channels;
}
async function loadChannelFeatures(reload = true) {
  try {
    await ensureChannelConfig(reload);
    channelAssetNotice('');
  } catch (error) {
    channelAssetNotice(error.message + '。可重试编辑器，不会丢弃已填写的配置。');
  }
}
async function refreshCookies(index = null) {
  try {
    const channels = await readyChannelRows(false);
    await channels.refreshCookies(index);
  } catch (error) {
    toast('Cookie 刷新未完成：' + error.message);
  }
}
async function manageProxies(scope = 'global', index = 0, view = 'list') {
  try {
    const channels = await ensureChannelConfig();
    channels.open(scope, index, view);
    channelAssetNotice('');
  } catch (error) {
    channelAssetNotice(error.message + '，请重新加载配置或刷新面板。');
    toast(error.message);
  }
}
// Keep buttons in a cached previous HTML document usable during an upgrade.
function openProxyEditor(scope = 'global', index = 0) {
  return manageProxies(scope, index);
}
function reloadConfiguration() {
  if (window.consoleForms?.dirty() && !confirm('重新加载会丢弃尚未保存的账号修改。继续？')) return;
  loadNotice('channel', '');
  return Promise.allSettled([
    loadRuntime(),
    loadParams(),
    loadExpressKeys(),
    loadChannelFeatures()
  ]);
}
// Read local state before initializing optional dialogs. An asset failure must
// never hide the saved proxy or prevent the user from correcting it.
navigateHash();
fetchStats();
setInterval(() => {
  if (!document.hidden) fetchStats();
}, 3000);
loadRuntime();
loadParams();
loadExpressKeys();
loadChannelFeatures();

async function runAction(button, action) {
  if (button.matches(':disabled')) return;
  const groups = button.closest('dialog')
    ? [...button.closest('dialog').querySelectorAll('fieldset')]
    : button.closest('.inline-notice')
      ? [...document.querySelectorAll('main fieldset')]
      : [button.closest('fieldset')].filter(Boolean);
  if (window.consoleForms && !window.consoleForms.validate(button)) return;
  if (groups.some((group) => group.disabled)) {
    toast('该分区正在处理，请稍后再试');
    return;
  }
  const label = button.textContent;
  button.disabled = true;
  button.setAttribute('aria-busy', 'true');
  button.textContent = '处理中…';
  const disabled = groups.map((group) => group.disabled);
  groups.forEach((group) => {
    group.disabled = true;
    group.setAttribute('aria-busy', 'true');
  });
  try {
    await action();
  } catch (error) {
    console.error(error);
    toast('❌ 操作未完成：' + (error && error.message ? error.message : error));
  } finally {
    button.disabled = false;
    button.removeAttribute('aria-busy');
    if (button.textContent === '处理中…') button.textContent = label;
    groups.forEach((group, i) => {
      group.disabled = disabled[i];
      group.removeAttribute('aria-busy');
    });
    window.consoleChannels?.sync?.();
  }
}
document.addEventListener('input', (event) => {
  event.target.removeAttribute('aria-invalid');
  window.consoleForms?.clearError(event.target);
  if (event.target.id === 'proxy-input') updateProxyDraft();
  if (PER_MODEL_KEYS.includes(event.target.id)) {
    recordModelField(event.target.id);
    refreshModelDraft();
    if (event.target.id === 'sampling_policy') updateSamplingDisplay();
  }
});

fetchRecentMetrics();
setInterval(fetchRecentMetrics, 15000);
