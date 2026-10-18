/* Process-local request diagnostics. Server values always pass through esc(). */
(() => {
  const phases = {received:'接收与校验', preparing:'准备消息与连接', waiting_upstream:'等待上游响应',
    receiving:'接收上游分片', thinking:'返回思考', streaming:'输出正文 / 工具', retry_wait:'退避等待',
    resuming:'续接已有生成', delivery:'发送响应', succeeded:'完成', error:'失败', cancelled:'已取消', cancelling:'取消旧任务中'};
  const modes = {stream:'真流式', fake_stream:'假流式', json:'非流式'};
  const connections = {open:'连接中', completed:'发送结束', disconnected:'已断开'};
  const state = {offset:0, limit:25, total:0, selected:null, listController:null, detailController:null, listLoading:false, detailLoading:false};
  const duration = ms => ms == null ? '—' : ms < 1000 ? `${ms} ms` : `${(ms / 1000).toFixed(1)} 秒`;
  const date = seconds => seconds == null ? '—' : new Date(seconds * 1000).toLocaleString('zh-CN', {hour12:false});
  const timing = (r,key) => duration(r.timings?.[key]?.elapsed_ms);
  const badge = r => `<span class="request-status ${esc(r.status)}">${esc(phases[r.phase] || r.phase)}</span>`;
  const metric = (name,value) => `<div><dt>${esc(name)}</dt><dd>${esc(value ?? '—')}</dd></div>`;

  function summary(s) {
    if(!s) return;
    $('r-connections').textContent=s.connections;
    $('r-generations').textContent=s.generations;
    $('r-background').textContent=s.background;
    $('r-retry-wait').textContent=s.retry_wait;
    $('request-active-badge').textContent=s.active;
    $('request-overview').textContent=`前端连接 ${s.connections} · 上游生成 ${s.generations} · 后台运行 ${s.background} · 退避等待 ${s.retry_wait}`;
  }

  async function refresh(force=true) {
    if(document.hidden || $('view-requests').classList.contains('hidden')) return;
    if(!force && state.listLoading) return;
    state.listController?.abort();
    const controller = state.listController = new AbortController();
    state.listLoading=true;
    const query = new URLSearchParams({status:$('request-filter').value, q:$('request-search').value,
      offset:state.offset, limit:state.limit});
    try {
      const response = await fetch('/api/requests?' + query, {signal:controller.signal, cache:'no-store'});
      if(!response.ok) throw new Error(response.status===401?'登录已过期，请重新登录':'请求列表加载失败');
      const data = await response.json();
      if(controller !== state.listController) return;
      state.total=data.total;
      if(state.offset >= data.total && state.offset > 0) { state.offset=0; return refresh(); }
      summary(data.summary);
      $('request-rows').innerHTML=data.items.length ? data.items.map(r => `<tr data-request="${esc(r.id)}" tabindex="0" role="button" aria-label="查看请求 ${esc(r.id)}">
        <td><button class="request-link" data-request="${esc(r.id)}">${esc(r.id.slice(-8))}</button><div style="margin-top:4px;max-width:230px;overflow:hidden;text-overflow:ellipsis" title="${esc(r.model)}">${esc(r.model || '尚未通过校验')}</div><div class="lbl">${esc(r.channel)} · ${esc(modes[r.mode] || r.protocol)}</div></td>
        <td>${badge(r)}${r.parent_id?'<div class="lbl mt-1">续传读者</div>':''}${r.generation_active&&!r.http_active?'<div class="lbl mt-1">后台仍在生成</div>':''}</td>
        <td>${esc(date(r.created_at))}</td><td>${duration(r.duration_ms)}</td><td>${timing(r,r.parent_id?'first_sent_reply':'first_reply')} / ${timing(r,r.parent_id?'first_sent_content':'first_content')}</td>
        <td>${r.attempts} / ${r.retries}</td><td>${fmt(r.content_chars || r.sent_content_chars)} / ${fmt(r.reasoning_chars || r.sent_reasoning_chars)} 字符</td><td>${esc(connections[r.connection])}</td></tr>`).join('') : '<tr><td colspan="8" style="text-align:center;padding:32px">没有符合条件的请求</td></tr>';
      $('request-page-info').textContent=`${data.total?state.offset+1:0}–${Math.min(state.offset+state.limit,data.total)} / ${data.total} 条`;
      $('request-prev').disabled=state.offset===0;
      $('request-next').disabled=state.offset+state.limit>=data.total;
      $('request-refresh-state').textContent='每 2 秒刷新 · 更新于 '+new Date().toLocaleTimeString('zh-CN',{hour12:false});
    } catch(error) {
      if(error.name!=='AbortError') $('request-refresh-state').textContent=error.message+'，稍后自动重试';
    } finally {
      if(controller===state.listController) state.listLoading=false;
    }
  }

  async function detail(force=true) {
    if(!state.selected || !$('request-dialog').open || document.hidden) return;
    if(!force && state.detailLoading) return;
    state.detailController?.abort();
    const controller = state.detailController = new AbortController();
    state.detailLoading=true;
    try {
      const response = await fetch('/api/requests/' + encodeURIComponent(state.selected), {signal:controller.signal,cache:'no-store'});
      if(!response.ok) throw new Error(response.status===404?'记录不存在或已超过保留期限':response.status===401?'登录已过期':'详情加载失败');
      const r = await response.json();
      if(controller !== state.detailController) return;
      $('request-detail-title').textContent=r.model || '请求详情';
      $('request-detail-id').textContent=r.id;
      const retry = r.retry_at && r.phase==='retry_wait' ? ` · 预计 ${Math.max(0,Math.ceil(r.retry_at-Date.now()/1000))} 秒后重试` : '';
      const waiting = r.first_reply_deadline ? ` · 首回复还可等待 ${Math.max(0,Math.ceil(r.first_reply_deadline-Date.now()/1000))} 秒` : '';
      $('request-detail-state').innerHTML=badge(r)+` <span class="lbl">${esc(connections[r.connection])} · ${esc(r.channel)} · ${esc(modes[r.mode]||r.protocol)}${esc(retry+waiting)}</span>`;
      const oldLogs=$('request-detail-logs');
      const detailScroll=$('request-dialog').scrollTop;
      const scroll=oldLogs?.scrollTop||0;
      const atBottom=!oldLogs || oldLogs.scrollTop+oldLogs.clientHeight>=oldLogs.scrollHeight-30;
      const metrics = [metric('开始时间',date(r.created_at)), metric('结束时间',date(r.finished_at)), metric('总耗时',duration(r.duration_ms)),
        metric('上游首分片',timing(r,'first_upstream')),metric('首段思考',timing(r,'first_reasoning')),metric('服务端正文首字',timing(r,'first_content')),
        metric('模型首回复（含思考）',timing(r,'first_reply')),metric('首回复已发送',timing(r,'first_sent_reply')),
        metric('首回复超时设置',r.first_reply_timeout_seconds==null?'—':`${r.first_reply_timeout_seconds} 秒`),
        metric('断开时间',date(r.disconnected_at)),metric('断连续传截止',date(r.resume_expires_at)),
        metric('正文发送到前端',timing(r,'first_sent_content')),metric('上游尝试 / 实际重试',`${r.attempts} / ${r.retries}`),
        metric('生成正文 / 思考',`${fmt(r.content_chars)} / ${fmt(r.reasoning_chars)} 字符`),
        metric('发送正文 / 思考',`${fmt(r.sent_content_chars)} / ${fmt(r.sent_reasoning_chars)} 字符`),
        metric('生成 / 发送事件',`${r.generated_frames} / ${r.sent_frames}`),metric('生成 / 发送字节',`${fmt(r.generated_bytes)} / ${fmt(r.sent_bytes)}`),
        metric('结束原因',r.finish_reasons.join(', ')||'尚未结束'),metric('思考档位 / 输出上限',`${r.thinking_level??'默认'} / ${r.max_output_tokens??'默认'}`),
        metric('HTTP / 流内错误码',`${r.http_status??'—'} / ${r.error_code??'—'}`),metric('最后上游活动',date(r.timings.last_upstream?.at)),
        metric('无上游新数据',duration(r.idle_ms)),metric('上游模型',r.upstream_model),metric('项目 ID',r.project_id)].join('');
      $('request-detail-content').innerHTML=(r.parent_id?`<p class="text-sm mt-3">这是续传连接，<button class="request-link" data-request="${esc(r.parent_id)}">查看原上游请求</button>。此连接没有重复调用模型。</p>`:'')+
        (r.generation_id?`<p class="lbl mt-3">续传 ID：${esc(r.generation_id)}</p>`:'')+
        (r.last_error?`<p style="color:#be123c;background:#fff1f2;padding:12px;border-radius:8px;margin-top:14px;overflow-wrap:anywhere">最近错误：${esc(r.last_error)}</p>`:'')+
        `<dl class="request-metrics">${metrics}</dl><p class="lbl">所有首字耗时均从本次 HTTP 请求到达开始计时；服务端发送成功不代表客户端已渲染。心跳、空角色块和签名不算正文首字。</p>`+
        (r.inspection_skipped_bytes?'<p class="lbl mt-2">部分超大响应事件超过诊断解析上限，只计字节数，不计入正文字数与首字时间。</p>':'')+
        (Object.keys(r.usage).length?`<p class="lbl mt-3">上游用量：${esc(JSON.stringify(r.usage))}</p>`:'')+
        `<h3 class="text-sm font-semibold mt-5">阶段时间线</h3><ol class="request-timeline">${r.events.map(e=>`<li><span style="color:var(--accent);margin-right:10px">+${duration(e.elapsed_ms)}</span><span class="lbl">${esc(date(e.at))}</span><div style="margin-top:4px">${esc(e.message)}</div></li>`).join('')}</ol>`+
        `<h3 class="text-sm font-semibold mt-5 mb-2">关联日志</h3><div id="request-detail-logs" class="request-detail-logs">${esc(r.logs.map(l=>`[${date(l.at)}] ${l.message}`).join('\n') || '暂无关联日志' )}</div>`;
      const logs=$('request-detail-logs');
      logs.scrollTop=atBottom?logs.scrollHeight:scroll;
      $('request-dialog').scrollTop=detailScroll;
    } catch(error) {
      if(error.name!=='AbortError') $('request-detail-state').textContent=error.message;
    } finally {
      if(controller===state.detailController) state.detailLoading=false;
    }
  }

  function open(id) {
    state.selected=id;
    $('request-detail-title').textContent='请求详情';
    $('request-detail-id').textContent=id;
    $('request-detail-state').textContent='加载中…';
    $('request-detail-content').replaceChildren();
    if(!$('request-dialog').open) $('request-dialog').showModal();
    detail();
  }
  document.addEventListener('click',e=>{
    const target=e.target.closest('[data-request]');
    if(target) open(target.dataset.request);
  });
  $('request-rows').addEventListener('keydown',e=>{
    if(e.target.matches('tr[data-request]') && ['Enter',' '].includes(e.key)) {e.preventDefault();open(e.target.dataset.request);}
  });
  $('request-detail-close').addEventListener('click',()=>$('request-dialog').close());
  $('request-dialog').addEventListener('close',()=>{state.selected=null;state.detailController?.abort();});
  $('request-prev').addEventListener('click',()=>{state.offset=Math.max(0,state.offset-state.limit);refresh();});
  $('request-next').addEventListener('click',()=>{state.offset+=state.limit;refresh();});
  let searchTimer;
  $('request-search').addEventListener('input',()=>{clearTimeout(searchTimer);searchTimer=setTimeout(()=>{state.offset=0;refresh();},250);});
  $('request-filter').addEventListener('change',()=>{state.offset=0;refresh();});
  window.requestMonitor={refresh,summary,open};
  setInterval(()=>{refresh(false);detail(false);},2000);
  document.addEventListener('visibilitychange',()=>{if(!document.hidden){refresh();detail();}});
  fetchStats();
})();
