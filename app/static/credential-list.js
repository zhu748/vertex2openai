/* 参数化凭证列表骨架（2026-11 P2-6）。
 *
 * Express Key（dashboard.js）与 Cookie 账号（channel-config.js）此前各持
 * 六对平行函数：load / save / add / remove 的编排完全同构，仅端点、响应
 * 字段、空记录模板与文案不同——漏改一处即行为分叉（本文件收敛前，
 * 「空列表确认」与「保存响应校验」只在单侧存在）。
 *
 * 本模块只持有共享编排；调用方通过 config 描述差异。全局函数名
 * （loadExpressKeys / saveCookieAccounts …）保持不变，HTML 的 onclick
 * 契约不受影响。运行期依赖 dashboard.js 的共享基座（readJSON / toast /
 * responseError / loadNotice / credentialCopy），函数体内延迟解析，因此
 * 本文件在 dashboard.js 之前加载也安全。
 */
(() => {
  if (window.credentialList) return;

  /* cfg 字段：
   *   endpoint       列表端点（GET 加载 / POST 保存）
   *   itemsKey       请求与响应里的列表字段名（'keys' / 'accounts'）
   *   responseItems  从保存响应取列表；Cookie 侧在此校验数组形态并抛
   *                  ui 标记错误（错误文案直达 toast，网络错误仍显示
   *                  统一的网络失败提示）
   *   noticeLabel / loadRetryHint / notLoadedToast / toastNoun
   *   formsGroup     consoleForms.saved(...) 的分组名
   *   accountGroup   consoleForms.openAccount(...) 的分组名
   *   emptyConfirm   空列表保存前的确认（仅 Express Key 需要）
   *   getItems / setItems / getSaved / setSaved / getRevision /
   *   setRevision / isLoaded / markLoaded   —— 调用方闭包状态的读写面
   *   render / collect / blankRecord / focusNew / afterRemove
   */
  function create(cfg) {
    async function load() {
      try {
        const d = await readJSON(cfg.endpoint);
        cfg.setItems((d[cfg.itemsKey] || []).map(credentialCopy));
        cfg.markLoaded();
        cfg.setSaved(cfg.getItems().map(credentialCopy));
        cfg.setRevision(d.revision ?? null);
        cfg.render();
        window.consoleForms?.saved(cfg.formsGroup);
      } catch (e) {
        loadNotice('channel', cfg.noticeLabel + e.message + '。' + cfg.loadRetryHint);
      }
    }

    async function save() {
      if (!cfg.isLoaded()) {
        toast(cfg.notLoadedToast);
        return;
      }
      let payload;
      try {
        payload = cfg.collect();
      } catch (error) {
        toast('❌ 收集账号信息失败：' + error.message);
        return;
      }
      if (cfg.emptyConfirm && payload.length === 0 && !cfg.emptyConfirm()) {
        cfg.render();
        return;
      }
      try {
        const r = await fetch(cfg.endpoint, {
          signal: AbortSignal.timeout(30000),
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ [cfg.itemsKey]: payload, revision: cfg.getRevision() })
        });
        const d = await r.json();
        if (r.ok) {
          const items = cfg.responseItems(d);
          cfg.setRevision(d.revision ?? null);
          cfg.setItems(items);
          cfg.setSaved(items.map(credentialCopy));
          cfg.render();
          window.consoleForms?.saved(cfg.formsGroup);
          toast('已保存 ' + items.length + ' 个' + cfg.toastNoun + '（已热生效）');
        } else {
          const message = await responseError(r, '保存失败', d);
          if (r.status === 409) loadNotice('channel', message);
          toast('❌ ' + message);
        }
      } catch (e) {
        // ui 标记 = 保存响应形态问题（文案可直接展示）；其余为网络失败。
        toast(e && e.ui ? '❌ ' + e.message : '❌ 网络请求失败');
      }
    }

    function add() {
      if (!cfg.isLoaded()) {
        toast(cfg.notLoadedToast);
        return;
      }
      cfg.collect();
      cfg.getItems().push(cfg.blankRecord());
      cfg.render();
      const n = cfg.getItems().length - 1;
      window.consoleForms?.openAccount(cfg.accountGroup, n);
      cfg.focusNew(n);
    }

    function remove(i) {
      cfg.collect();
      cfg.getItems().splice(i, 1);
      cfg.render();
      if (cfg.afterRemove) cfg.afterRemove();
    }

    return { load, save, add, remove };
  }

  window.credentialList = { create };
})();
