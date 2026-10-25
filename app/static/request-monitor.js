/* Process-local request diagnostics. Server values always pass through esc(). */
(() => {
  const phases = {received:'接收与校验', preparing:'准备消息与连接', waiting_upstream:'等待上游响应',
    receiving:'接收上游分片', thinking:'返回思考', streaming:'输出正文 / 工具', retry_wait:'退避等待',
    resuming:'续接已有生成', delivery:'发送响应', succeeded:'完成', error:'失败', cancelled:'已取消', cancelling:'取消旧任务中'};
  const modes = {stream:'真流式', fake_stream:'假流式', json:'非流式'};
  const protocols = {openai:'OpenAI', claude:'Claude', gemini:'Gemini 原生'};
  const channels = {express:'Express', cookie:'Cookie', mixed:'混合'};
  // 协议·通道·模式三段标签：mode 恒有值，旧写法 modes[r.mode]||r.protocol 会把协议吞掉，
  // 三协议共存的部署里无法分辨请求来自哪套端点。
  const flowLabel = r => [protocols[r.protocol] || r.protocol, channels[r.channel] || r.channel, modes[r.mode]]
    .filter(Boolean).join(' · ');
  const connections = {open:'连接中', completed:'发送结束', disconnected:'已断开'};
  const state = {paused:false, offset:0, limit:matchMedia('(max-width:767px)').matches?10:25, total:0, selected:null, listController:null, detailController:null, listLoading:false, detailLoading:false, loaded:false};
  const duration = ms => ms == null ? '—' : ms < 1000 ? `${ms} ms` : `${(ms / 1000).toFixed(1)} 秒`;
  const date = seconds => seconds == null ? '—' : new Date(seconds * 1000).toLocaleString('zh-CN', {hour12:false});
  const timing = (r,key) => duration(r.timings?.[key]?.elapsed_ms);
  const badge = r => `<span class="request-status ${esc(r.status)}">${esc(phases[r.phase] || r.phase)}</span>`;
  const metric = (name,value,primary=false) => ({primary,html:`<div><dt>${esc(name)}</dt><dd>${esc(value ?? '—')}</dd></div>`});

  function summary(s) {
    if(!s) return;
    $('r-connections').textContent=s.connections;
    $('r-generations').textContent=s.generations;
    $('r-background').textContent=s.background;
    $('r-retry-wait').textContent=s.retry_wait;
    $('request-active-badge').textContent=s.active;
    $('request-overview').textContent=`前端连接 ${s.connections} · 上游生成 ${s.generations} · 后台运行 ${s.background} · 退避等待 ${s.retry_wait}`;
  }

  function reconcileRows(html){
    const fragment=document.createElement('tbody');fragment.innerHTML=html;
    const target=$('request-rows'),old=new Map([...target.children].filter(row=>row.dataset.request).map(row=>[row.dataset.request,row]));
    const keep=new Set();let previous=null;
    for(const fresh of [...fragment.children]){
      const existing=old.get(fresh.dataset.request),row=existing||fresh;
      if(existing){for(let i=0;i<fresh.cells.length;i++)if(existing.cells[i].innerHTML!==fresh.cells[i].innerHTML)existing.cells[i].innerHTML=fresh.cells[i].innerHTML;}
      const next=previous?previous.nextSibling:target.firstChild;
      if(row!==next)target.insertBefore(row,next);
      keep.add(row);previous=row;
    }
    for(const row of [...target.children])if(!keep.has(row))row.remove();
  }
  async function refresh(force=true) {
    if(document.hidden || $('view-requests').classList.contains('hidden')) return;
    if(!force && (state.listLoading||state.paused)) return;
    // Keep text selection usable; keyboard focus is restored after rendering.
    if(!force && !window.getSelection().isCollapsed) return;
    state.listController?.abort();
    const controller = state.listController = new AbortController();
    state.listLoading=true;
    $('request-rows').setAttribute('aria-busy','true');
    const query = new URLSearchParams({status:$('request-filter').value, q:$('request-search').value,
      offset:state.offset, limit:state.limit});
    try {
      const response = await fetch('/api/requests?' + query, {signal:AbortSignal.any([controller.signal,AbortSignal.timeout(12000)]), cache:'no-store'});
      if(!response.ok) throw new Error(response.status===401?'登录已过期，请重新登录':'请求列表加载失败');
      const data = await response.json();
      if(controller !== state.listController) return;
      if(!force && !window.getSelection().isCollapsed) return;
      state.total=data.total;
      if(state.offset >= data.total && state.offset > 0) { state.offset=0; return refresh(); }
      summary(data.summary);
      state.loaded=true;
      const focusedRequest=document.activeElement.closest('#request-rows [data-request]')?.dataset.request;
      const rowsHTML=data.items.length ? data.items.map(r => `<tr data-request="${esc(r.id)}" tabindex="0" role="button" aria-label="查看请求 ${esc(r.id)}">
        <td><span class="request-link request-id">${esc(r.id.slice(-8))}</span><div class="request-model" title="${esc(r.model)}">${esc(r.model || '尚未通过校验')}</div><div class="lbl">${esc(flowLabel(r))}</div></td>
        <td>${badge(r)}${r.parent_id?'<div class="lbl mt-1">续传读者</div>':''}${r.generation_active&&!r.http_active?'<div class="lbl mt-1">后台仍在生成</div>':''}</td>
        <td data-label="开始时间">${esc(date(r.created_at))}</td><td data-label="总耗时">${duration(r.duration_ms)}</td><td data-label="首回复 / 正文">${timing(r,r.parent_id?'first_sent_reply':'first_reply')} / ${timing(r,r.parent_id?'first_sent_content':'first_content')}</td>
        <td data-label="尝试 / 重试">${r.attempts} / ${r.retries}</td><td data-label="输出 / 思考">${fmt(r.content_chars || r.sent_content_chars)} / ${fmt(r.reasoning_chars || r.sent_reasoning_chars)} 字符</td><td data-label="连接状态">${esc(connections[r.connection])}</td></tr>`).join('') : '<tr><td colspan="8" class="table-empty"><span class="empty-title">没有符合条件的请求</span><span class="empty-description">可调整搜索与筛选；新请求将自动出现在这里。</span></td></tr>';
      reconcileRows(rowsHTML);
      if(focusedRequest) $('request-rows').querySelector(`[data-request="${CSS.escape(focusedRequest)}"]`)?.focus({preventScroll:true});
      $('request-page-info').textContent=`${data.total?state.offset+1:0}–${Math.min(state.offset+state.limit,data.total)} / ${data.total} 条`;
      $('request-prev').disabled=state.offset===0;
      $('request-next').disabled=state.offset+state.limit>=data.total;
      $('request-refresh-state').textContent=(state.paused?'已暂停 · 更新于 ':'每 2 秒刷新 · 更新于 ')+new Date().toLocaleTimeString('zh-CN',{hour12:false});
      $('request-refresh-state').classList.remove('error-message');
    } catch(error) {
      if(error.name!=='AbortError') {
        const message=error.name==='TimeoutError'?'请求列表连接超时':error.message;
        $('request-refresh-state').textContent=message+'，稍后自动重试';
        $('request-refresh-state').classList.add('error-message');
        if(!state.loaded) $('request-rows').innerHTML=`<tr><td colspan="8" class="table-empty"><span class="empty-title">${esc(message)}</span><span class="empty-description">请检查连接，或点击刷新重试。</span></td></tr>`;
      }
    } finally {
      if(controller===state.listController) {state.listLoading=false;$('request-rows').removeAttribute('aria-busy');}
    }
  }

  async function detail(force=true) {
    if(!state.selected || !$('request-dialog').open || document.hidden) return;
    if(!force && (state.detailLoading||state.paused)) return;
    const selection=window.getSelection();
    if(!force && !selection.isCollapsed && $('request-dialog').contains(selection.anchorNode)) return;
    state.detailController?.abort();
    const controller = state.detailController = new AbortController();
    state.detailLoading=true;
    try {
      const response = await fetch('/api/requests/' + encodeURIComponent(state.selected), {signal:AbortSignal.any([controller.signal,AbortSignal.timeout(12000)]),cache:'no-store'});
      if(!response.ok) throw new Error(response.status===404?'记录不存在或已超过保留期限':response.status===401?'登录已过期':'详情加载失败');
      const r = await response.json();
      if(controller !== state.detailController) return;
      if(!force && !selection.isCollapsed && $('request-dialog').contains(selection.anchorNode)) return;
      $('request-detail-title').textContent=r.model || '请求详情';
      $('request-detail-id').textContent=r.id;
      const retry = r.retry_at && r.phase==='retry_wait' ? ` · 预计 ${Math.max(0,Math.ceil(r.retry_at-Date.now()/1000))} 秒后重试` : '';
      const waiting = r.first_reply_deadline ? ` · 首回复还可等待 ${Math.max(0,Math.ceil(r.first_reply_deadline-Date.now()/1000))} 秒` : '';
      $('request-detail-state').innerHTML=badge(r)+` <span class="lbl">${esc(connections[r.connection])} · ${esc(flowLabel(r))}${esc(retry+waiting)}</span>`;
      const oldLogs=$('request-detail-logs');
      const detailScroll=$('request-dialog').scrollTop;
      const scroll=oldLogs?.scrollTop||0;
      const atBottom=!oldLogs || oldLogs.scrollTop+oldLogs.clientHeight>=oldLogs.scrollHeight-30;
      const advancedOpen=$('request-advanced')?.open || false;
      const focused=document.activeElement;
      const focusId=$('request-detail-content').contains(focused)?focused.id:null;
      const focusSummary=focused.matches('#request-advanced > summary');
      const metrics = [metric('开始时间',date(r.created_at),true), metric('结束时间',date(r.finished_at)), metric('总耗时',duration(r.duration_ms),true),
        metric('上游首分片',timing(r,'first_upstream')),metric('首段思考',timing(r,'first_reasoning')),metric('服务端正文首字',timing(r,'first_content'),true),
        metric('模型首回复（含思考）',timing(r,'first_reply'),true),metric('首回复已发送',timing(r,'first_sent_reply')),
        metric('首回复超时设置',r.first_reply_timeout_seconds==null?'—':`${r.first_reply_timeout_seconds} 秒`),
        metric('断开时间',date(r.disconnected_at)),metric('断连续传截止',date(r.resume_expires_at)),
        metric('正文发送到前端',timing(r,'first_sent_content')),metric('上游尝试 / 实际重试',`${r.attempts} / ${r.retries}`,true),
        metric('生成正文 / 思考',`${fmt(r.content_chars)} / ${fmt(r.reasoning_chars)} 字符`),
        metric('发送正文 / 思考',`${fmt(r.sent_content_chars)} / ${fmt(r.sent_reasoning_chars)} 字符`),
        metric('生成 / 发送事件',`${r.generated_frames} / ${r.sent_frames}`),metric('生成 / 发送字节',`${fmt(r.generated_bytes)} / ${fmt(r.sent_bytes)}`),
        metric('结束原因',r.finish_reasons.join(', ')||'尚未结束'),metric('思考档位 / 输出上限',`${r.thinking_level??'默认'} / ${r.max_output_tokens??'默认'}`),
        metric('HTTP / 流内错误码',`${r.http_status??'—'} / ${r.error_code??'—'}`),metric('最后上游活动',date(r.timings.last_upstream?.at)),
        metric('无上游新数据',duration(r.idle_ms),true),metric('上游模型',r.upstream_model),metric('项目 ID',r.project_id)];
      $('request-detail-content').innerHTML=(r.parent_id?`<p class="text-sm mt-3">这是续传连接，<button class="request-link" data-request="${esc(r.parent_id)}">查看原上游请求</button>。此连接没有重复调用模型。</p>`:'')+
        (r.generation_id?`<p class="lbl mt-3">续传 ID：${esc(r.generation_id)}</p>`:'')+
        (r.last_error?`<p class="request-error">最近错误：${esc(r.last_error)}</p>`:'')+
        `<h3 class="request-section-title">关键时序</h3><dl class="request-metrics">${metrics.filter(m=>m.primary).map(m=>m.html).join('')}</dl><p class="lbl">所有首字耗时均从本次 HTTP 请求到达开始计时；服务端发送成功不代表客户端已渲染。心跳、空角色块和签名不算正文首字。</p>`+
        `<details id="request-advanced" class="field-help" ${advancedOpen?'open':''}><summary>交付与上游诊断</summary><dl class="request-metrics">${metrics.filter(m=>!m.primary).map(m=>m.html).join('')}</dl></details>`+
        (r.inspection_skipped_bytes?'<p class="lbl mt-2">部分超大响应事件超过诊断解析上限，只计字节数，不计入正文字数与首字时间。</p>':'')+
        (Object.keys(r.usage).length?`<p class="lbl mt-3">上游用量：${esc(JSON.stringify(r.usage))}</p>`:'')+
        `<h3 class="request-section-title">阶段时间线</h3><ol class="request-timeline">${r.events.map(e=>`<li><span style="color:var(--accent);margin-right:10px">+${duration(e.elapsed_ms)}</span><span class="lbl">${esc(date(e.at))}</span><div style="margin-top:4px">${esc(e.message)}</div></li>`).join('')}</ol>`+
        `<h3 class="request-section-title">关联日志</h3><div id="request-detail-logs" class="request-detail-logs" tabindex="0" aria-label="此请求的关联日志">${esc(r.logs.map(l=>`[${date(l.at)}] ${l.message}`).join('\n') || '暂无关联日志' )}</div>`;
      const logs=$('request-detail-logs');
      logs.scrollTop=atBottom?logs.scrollHeight:scroll;
      if(focusSummary) $('request-advanced').querySelector('summary').focus({preventScroll:true});
      else if(focusId) $(focusId)?.focus({preventScroll:true});
      $('request-dialog').scrollTop=detailScroll;
    } catch(error) {
      if(error.name!=='AbortError') $('request-detail-state').textContent=(error.name==='TimeoutError'?'详情连接超时':error.message)+'，稍后自动重试';
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
    // 与列表/详情刷新处的选区守卫同口径：行内拖选复制 trace-ID、模型名时
    // 松开鼠标的 click 不应触发弹窗抢焦点、破坏选区。
    if(!window.getSelection().isCollapsed) return;
    const target=e.target.closest('[data-request]');
    if(target) open(target.dataset.request);
  });
  $('request-rows').addEventListener('keydown',e=>{
    if(e.target.matches('tr[data-request]') && ['Enter',' '].includes(e.key)) {e.preventDefault();open(e.target.dataset.request);}
  });
  $('request-detail-close').addEventListener('click',()=>$('request-dialog').close());
  $('request-dialog').addEventListener('click',e=>{if(e.target===$('request-dialog')){const r=e.target.getBoundingClientRect();if(e.clientX<r.left||e.clientX>r.right||e.clientY<r.top||e.clientY>r.bottom)e.target.close();}});
  $('request-dialog').addEventListener('close',()=>{state.selected=null;state.detailController?.abort();});
  $('request-prev').addEventListener('click',()=>{state.offset=Math.max(0,state.offset-state.limit);refresh();});
  $('request-next').addEventListener('click',()=>{state.offset+=state.limit;refresh();});
  $('request-refresh').addEventListener('click',()=>{refresh();detail();});
  $('request-page-size').value=String(state.limit);
  $('request-page-size').addEventListener('change',()=>{state.limit=Number($('request-page-size').value);state.offset=0;refresh();});
  $('request-columns').addEventListener('change',()=>document.querySelector('.request-table').classList.toggle('show-details',$('request-columns').checked));
  $('request-pause').addEventListener('click',()=>{
    state.paused=!state.paused;
    $('request-pause').setAttribute('aria-pressed',String(state.paused));
    $('request-pause').textContent=state.paused?'恢复刷新':'暂停刷新';
    if(state.paused){state.listController?.abort();state.detailController?.abort();$('request-refresh-state').textContent='已暂停自动刷新，可手动刷新';}
    else{refresh();detail();}
  });
  let searchTimer;
  $('request-search').addEventListener('input',()=>{clearTimeout(searchTimer);searchTimer=setTimeout(()=>{state.offset=0;refresh();},250);});
  $('request-filter').addEventListener('change',()=>{state.offset=0;refresh();});
  window.requestMonitor={refresh,summary,open};
  setInterval(()=>{refresh(false);detail(false);},2000);
  document.addEventListener('visibilitychange',()=>{if(!document.hidden&&!state.paused){refresh();detail();}});
  // 首次 stats 已由 dashboard.js 末尾的 fetchStats() 发起（并通过
  // window.requestMonitor.summary 回填这里），再拉一次是冗余往返。
})();
