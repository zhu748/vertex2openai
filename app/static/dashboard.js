const $ = id => document.getElementById(id);
let CAPS = {}, curAR = "";
let GLOBAL_SETTINGS = {}, OVERRIDES = {}, SAVED_MODEL_OVERRIDES = {}, BUILTIN_OVERRIDES = {};
let modelEdits=new Set(), modelInherit=new Set(), modelPins=new Set();
let settingsLoaded=false, capabilitiesLoaded=false, keysLoaded=false, runtimeLoaded=false;
const PER_MODEL_KEYS = ['first_reply_timeout_seconds','anti_truncation_mode','native_thinking_mode','thinking_g3_level','image_size','image_aspect_ratio','default_temperature','default_top_p','default_max_tokens','inject_system_instruction','inject_prefill','prefill_instruction','image_system_instruction','inject_prefill_for_image','sampling_policy','prefill_cot_guard'];
const COMMON_ARS = ["1:1","3:2","2:3","3:4","4:3","4:5","5:4","9:16","16:9","21:9","1:4","4:1","1:8","8:1","9:21"];
const MODEL_DRAFTS = new Map();
let renderedModel=null, modelSnapshot=null;
const viewScroll=new Map();

let toastTimer;
function toast(m){ const t=$('toast'); clearTimeout(toastTimer); t.textContent=m; t.classList.add('show'); toastTimer=setTimeout(()=>t.classList.remove('show'),4000); }
function fmt(n){ return (n||0).toLocaleString('en-US'); }
async function logout(){
  try{
    const response=await fetch('/api/logout',{signal:AbortSignal.timeout(10000),method:'POST'});
    if(!response.ok) throw new Error(await responseError(response,'退出失败'));
    location.href='/';
  }catch(error){toast(error.name==='TimeoutError'?'退出请求超时，请重试':error.message);}
}
async function readJSON(url){
  const response=await fetch(url,{cache:'no-store',signal:AbortSignal.timeout(15000)});
  if(!response.ok) throw new Error(response.status===401?'登录已过期，请重新登录':'加载失败（HTTP '+response.status+'）');
  return response.json();
}
function loadNotice(section,message){
  if(section==='params') {const other=$('runtime-load-error');if(other){other.classList.toggle('hidden',!message);other.querySelector('span').textContent=message;}}
  const box=$(section+'-load-error');
  box.classList.toggle('hidden',!message);
  box.querySelector('span').textContent=message;
}
async function responseError(response,fallback='保存失败',result=null){
  if(response.status===401) return '登录已过期，请重新登录';
  const data=result || await response.json().catch(()=>({}));
  const aliases={proxy_url:'proxy-input',proxy_urls:'proxy-input',express_ua_custom:'express-ua-custom',express_ua_profile:'express-ua-profile',cookie_impersonate:'cookie-impersonate'};
  const field=$(aliases[data.field]||data.field);
  if(field){
    field.setAttribute('aria-invalid','true');
    window.consoleForms?.fieldError(field,typeof data.error==='string'?data.error:fallback);
    requestAnimationFrame(()=>{if(field.getClientRects().length && !field.matches(':disabled')) field.focus();});
  }
  if(typeof data.error==='string') return data.error;
  if(Array.isArray(data.detail)) return data.detail.map(item=>item.msg).join('；');
  return typeof data.detail==='string'?data.detail:fallback+'（HTTP '+response.status+'）';
}

function switchTab(t, updateHash=true){
  if(!['overview','channel','params','runtime','requests','logs'].includes(t)) return;
  const previous=document.querySelector('.tab.active')?.dataset.tab;
  const changed=previous!==t;
  if(changed&&previous) viewScroll.set(previous,window.scrollY);
  document.querySelectorAll('.tab').forEach(e=>{
    const active=e.dataset.tab===t;
    e.classList.toggle('active', active);
    e.setAttribute('aria-selected', String(active));
    e.tabIndex=active?0:-1;
  });
  ['overview','channel','params','runtime','requests','logs'].forEach(v=>$('view-'+v).classList.toggle('hidden', v!==t));
  if(updateHash) history.replaceState(null,'','#'+t);
  $('mobile-nav').value=t;
  document.body.dataset.view=t;
  window.consoleForms?.layout();
  if(changed) window.scrollTo({top:viewScroll.get(t)||0,behavior:'instant'});
  if(t==='overview') queueMicrotask(fetchRecentMetrics);
  if(t==='requests') window.requestMonitor?.refresh();
  if(t==='logs' && autoscroll) logwin.scrollTop=logwin.scrollHeight;
}

document.querySelector('.tab-bar').addEventListener('keydown',event=>{
  if(!['ArrowLeft','ArrowRight','Home','End'].includes(event.key)) return;
  const tabs=[...document.querySelectorAll('.tab')];
  const index=tabs.indexOf(document.activeElement);
  if(index<0) return;
  event.preventDefault();
  const next=event.key==='Home'?0:event.key==='End'?tabs.length-1:(index+(event.key==='ArrowRight'?1:-1)+tabs.length)%tabs.length;
  tabs[next].focus();switchTab(tabs[next].dataset.tab);
});
function navigateHash(){
  let target;try{target=decodeURIComponent(location.hash.slice(1));}catch{target='';}
  if(target==='main-content') return;
  const anchor=$(target),view=anchor?.closest('.view');
  if(view&&anchor!==view){
    switchTab(view.id.slice(5),false);
    requestAnimationFrame(()=>{window.consoleForms?.layout();anchor.scrollIntoView({block:'start'});});
  }else switchTab(['overview','channel','params','runtime','requests','logs'].includes(target)?target:'overview',false);
}
window.addEventListener('hashchange',navigateHash);

/* ---------- Overview ---------- */
function renderHealth(success,error){
  const completed=success+error;
  const percent=completed?success/completed*100:0;
  const rate=completed?percent.toFixed(1)+'%':'—';
  $('health-ring').style.setProperty('--rate',percent+'%');
  $('health-ring').style.setProperty('--ring-color',completed?'var(--danger)':'#e8ebf3');
  $('health-ring').setAttribute('aria-label',completed?`成功率 ${rate}，成功 ${success}，错误 ${error}`:'暂无请求数据');
  $('health-rate').textContent=rate;
  $('health-success').textContent=fmt(success);
  $('health-error').textContent=fmt(error);
  $('health-note').textContent=completed?`共 ${fmt(completed)} 次成功 / 错误记录`:'有请求完成后显示成功率';
}
let statsLoading=false;
async function fetchStats(){
  if(statsLoading) return;
  statsLoading=true;
  try{
    const response=await fetch('/api/stats',{cache:'no-store',signal:AbortSignal.timeout(10000)});
    if(!response.ok) throw new Error(response.status===401?'登录已过期，请重新登录':'统计暂不可用，将自动重试');
    const d=await response.json();
    $('s-total').textContent=fmt(d.total); $('s-success').textContent=fmt(d.success);
    $('s-error').textContent=fmt(d.error); $('s-retries').textContent=fmt(d.retries);
    $('t-prompt').textContent=fmt(d.prompt_tokens); $('t-comp').textContent=fmt(d.completion_tokens);
    const tokens=(d.prompt_tokens||0)+(d.completion_tokens||0);
    $('t-total').textContent=fmt(tokens);
    $('prompt-bar').style.width=(tokens?d.prompt_tokens/tokens*100:0)+'%';
    $('completion-bar').style.width=(tokens?d.completion_tokens/tokens*100:0)+'%';
    $('uptime').textContent='已运行 '+(d.uptime<3600?Math.floor(d.uptime/60)+' 分钟':(d.uptime/3600).toFixed(1)+' 小时');
    window.requestMonitor?.summary(d.requests);
    renderHealth(d.success||0,d.error||0);
    $('stats-sync').textContent='每 3 秒更新 · '+new Date().toLocaleTimeString('zh-CN',{hour12:false});
    $('stats-sync').classList.remove('error');
    $('uptime').parentElement.classList.remove('service-offline');
  }catch(e){
    $('stats-sync').textContent=e.name==='TimeoutError'?'统计连接超时，将自动重试':e.message;
    $('stats-sync').classList.add('error');
    $('uptime').textContent=e.message.includes('登录已过期')?'登录已过期':'暂时无法连接';
    $('uptime').parentElement.classList.add('service-offline');
  }finally{statsLoading=false;}
}

/* ---------- 通用转义（XSS 防护）：所有拼进 innerHTML 的服务端数据必须先过这里 ---------- */
function esc(s){ return String(s==null?'':s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;').replace(/'/g,'&#39;'); }

/* ---------- Shared credential forms (available even if the editor fails to load) ---------- */
function proxyDraft(value){
  if(Array.isArray(value)) return value.map(v=>typeof v==='string'?v:JSON.stringify(v));
  const text=String(value||'').trim();
  if(!text) return [];
  // Preserve a whole ICE/node object until the shared backend parser expands it.
  if(text.startsWith('{')) return [text];
  if(text.startsWith('[')){
    try{const list=JSON.parse(text);if(Array.isArray(list)) return proxyDraft(list);}catch(_){}
  }
  return text.split(/[\r\n]+|,\s*(?=[a-zA-Z][a-zA-Z0-9+.-]*:\/\/|turns?:)/i).map(v=>v.trim()).filter(Boolean);
}
function credentialCopy(record){return {...record,proxies:proxyDraft(record.proxies??record.proxy)};}
function projectFromInput(value){
  const text=value.trim(), match=text.match(/[?&]project=([^&#]+)/)||text.match(/\/projects\/([^/?#]+)/);
  if(!match) return text;
  try{return decodeURIComponent(match[1]);}catch(_){return match[1];}
}
function accountProxyFields(scope,index,record){
  const prefix=scope==='key'?'k':'c', proxies=proxyDraft(record.proxies??record.proxy);
  return `<div class="account-proxy-field"><div class="field-heading"><label for="${scope}-proxy-${index}">专属代理（按顺序尝试）</label>
    <button type="button" class="btn-ghost" onclick="manageProxies('${scope}',${index})">管理代理 · ${proxies.length} 条</button></div>
    <textarea id="${scope}-proxy-${index}" class="inp log ${prefix}-proxy" rows="2" spellcheck="false" autocomplete="off" placeholder="每行一条；留空跟随全局；direct 强制直连">${esc(proxies.join('\n'))}</textarea>
    <label class="fallback-option"><input type="checkbox" class="${prefix}-fallback" ${record.proxy_fallback===false?'':'checked'}>专属代理全部失效后，使用全局代理</label></div>`;
}

/* ---------- Channel ---------- */
let currentMode='api_key';
function renderMode(mode){
  $('keys-box').classList.toggle('hidden',mode==='web_proxy');
  $('cookie-box').classList.toggle('hidden',mode==='api_key');
  $('mode-pill').textContent=({api_key:'通道 Express API',web_proxy:'通道 Cookie 直连',mixed:'通道 Key + Cookie 混合'})[mode];
}
async function updateMode(m){
  if(!runtimeLoaded){document.querySelectorAll('input[name=mode]').forEach(r=>r.checked=false);toast('通道状态尚未加载，请先重新加载配置');return;}
  const pill=$('mode-pill'), prev=pill.textContent, previousMode=currentMode;
  const radios=document.querySelectorAll('input[name=mode]');
  radios.forEach(r=>r.disabled=true);
  // 先乐观切显示，请求失败则弹提示并回滚（旧版无错误处理，后端 500 时 UI 与实际状态脱节）
  renderMode(m);
  try{
    const r=await fetch('/api/settings/mode',{signal:AbortSignal.timeout(30000),method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({mode:m})});
    if(!r.ok){ throw new Error('HTTP '+r.status); }
    currentMode=m;
  }catch(e){
    pill.textContent=prev;
    document.querySelector(`input[name=mode][value=${previousMode}]`).checked=true;
    renderMode(previousMode);
    toast('❌ 通道切换失败，已回滚：'+e.message);
  }finally{radios.forEach(r=>r.disabled=false);}
}
async function loadRuntime(){
  try{
    const s=await readJSON('/api/settings/runtime');
    const m=s.mode||(s.use_web_proxy?'web_proxy':'api_key');
    currentMode=m;
    runtimeLoaded=true;
    document.querySelector(`input[name=mode][value=${m}]`).checked=true;
    renderMode(m);
  }catch(e){loadNotice('channel','通道状态'+e.message+'。可重新加载配置。');}
}

/* ---------- Express Key 列表（多账号，热编辑） ---------- */
let KEYS = [], SAVED_KEYS = [];
function keySrcPill(rec){
  if(rec.project_source==='auto') return '<span class="pill" style="text-transform:none">来源 auto</span>';
  if(rec.project_source==='manual') return '<span class="pill" style="text-transform:none">来源 manual</span>';
  return '<span class="pill" style="text-transform:none;color:#b45309">未探测</span>';
}
function keyProxyPill(rec){
  const proxies=proxyDraft(rec.proxies??rec.proxy), v=(proxies[0]||'').trim().toLowerCase();
  if(!v) return '';
  if(v==='direct'||v==='none'||v==='直连') return '<span class="pill" style="text-transform:none;background:#f5f5f5;color:#737373">强制直连</span>';
  const proto=(proxies[0]||'').split(':')[0]||'proxy';
  const native=['http','https','socks5','socks5h'].includes(proto)?'#047857':'#1d4ed8';
  return `<span class="pill" style="text-transform:none;color:${native}">专属代理 ${proxies.length} 条</span>`;
}
function renderKeys(){
  const box=$('keys-list');
  window.consoleForms?.rememberAccounts('key');
  box.innerHTML=KEYS.map((rec,i)=>`
    <div class="key-card" data-i="${i}">
      <div class="key-head">
        <div class="key-title"><span class="pill pill-accent">Key ${i+1}</span>${keySrcPill(rec)}${keyProxyPill(rec)}</div>
        <div class="key-actions">
          <label><input type="checkbox" class="k-enabled" ${rec.enabled===false?'':'checked'} aria-label="启用 Key ${i+1}">启用</label>
          <button type="button" onclick="runAction(this,()=>probeOneKey(${i}))">探测项目</button>
          <button type="button" class="danger" onclick="removeKeyRow(${i})" aria-label="删除 Key ${i+1}">删除</button>
        </div>
      </div>
      <div class="key-fields">
        <div><label for="key-${i}">Express API Key</label><input id="key-${i}" class="inp log k-key" placeholder="粘贴 Express API Key" spellcheck="false" autocomplete="off" value="${esc(rec.key)}"></div>
        <div><label for="project-${i}">Project ID</label><input id="project-${i}" class="inp log k-project" placeholder="留空 = 等待自动探测" spellcheck="false" value="${esc(rec.project_id)}"><label class="note-label" for="key-note-${i}">备注</label><input id="key-note-${i}" class="inp k-note" placeholder="便于区分账号" value="${esc(rec.note)}"></div>
        ${accountProxyFields('key',i,rec)}
      </div>
      ${rec.detected_at?`<div class="key-meta">上次探测：${esc(new Date(rec.detected_at*1000).toLocaleString('zh-CN',{hour12:false}))}</div>`:''}
      ${rec._msg?`<div class="key-meta ${rec._msg_ok===false?'error-message':''}" role="status">${esc(rec._msg)}</div>`:''}
    </div>`).join('');
  window.consoleForms?.accounts('key');
  $('keys-count').textContent=KEYS.length+' 个';
  $('keys-empty').classList.toggle('hidden',KEYS.length>0);
}
function collectKeys(){
  // 以输入框的当前内容为准（用户可能改了还没保存）
  [...document.querySelectorAll('#keys-list [data-i]')].forEach(row=>{
    const i=Number(row.dataset.i);
    if(!KEYS[i]) return;
    KEYS[i].key = row.querySelector('.k-key').value.trim();
    const pid = projectFromInput(row.querySelector('.k-project').value);
    if(pid !== (KEYS[i].project_id||'')){ KEYS[i].project_id = pid; KEYS[i].project_source = pid?'manual':''; }
    const kProxy = row.querySelector('.k-proxy');
    if(kProxy){ KEYS[i].proxies=proxyDraft(kProxy.value); KEYS[i].proxy=KEYS[i].proxies[0]||''; }
    KEYS[i].proxy_fallback=row.querySelector('.k-fallback').checked;
    KEYS[i].note=row.querySelector('.k-note').value.trim();
    KEYS[i].enabled = row.querySelector('.k-enabled').checked;
  });
  return KEYS.filter(r=>r.key);
}
function addKeyRow(){ if(!keysLoaded){toast('凭证列表尚未加载，请先重新加载配置');return;} collectKeys(); KEYS.push({key:'',project_id:'',project_source:'',proxy:'',proxies:[],proxy_fallback:true,enabled:true}); renderKeys(); window.consoleForms?.openAccount('key',KEYS.length-1);$('key-'+(KEYS.length-1)).focus(); }
function removeKeyRow(i){ collectKeys(); KEYS.splice(i,1); renderKeys();window.consoleForms?.refresh(); }
async function loadExpressKeys(){
  try{
    const d=await readJSON('/api/express-keys');
    KEYS=(d.keys||[]).map(credentialCopy);
    SAVED_KEYS=KEYS.map(credentialCopy);
    keysLoaded=true;
    renderKeys();window.consoleForms?.saved('keys');
  }catch(e){loadNotice('channel','凭证列表'+e.message+'。重新加载成功后再保存 Key。');}
}
async function saveExpressKeys(){
  if(!keysLoaded){toast('凭证尚未加载，请先重新加载配置');return;}
  const payload=collectKeys();
  if(payload.length===0 && !confirm('将保存为“空 Key 列表”。\n保存后即使环境变量 VERTEX_EXPRESS_API_KEY 有值，重启也不会再导入。\n确定吗？')) { renderKeys(); return; }
  try{
    const r=await fetch('/api/express-keys',{signal:AbortSignal.timeout(30000),method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({keys:payload})});
    const d=await r.json();
    if(r.ok){ KEYS=(d.keys||[]).map(credentialCopy); SAVED_KEYS=KEYS.map(credentialCopy); renderKeys();window.consoleForms?.saved('keys'); toast('已保存 '+KEYS.length+' 个 Key（已热生效）'); }
    else toast('❌ '+(d.error||'保存失败'));
  }catch(e){ toast('❌ 网络请求失败'); }
}
async function probeOneKey(i){
  collectKeys();
  const rec=KEYS[i]; if(!rec||!rec.key){ toast('请先填入 Key'); return; }
  if(!SAVED_KEYS.some(x=>x.key===rec.key)){toast('请先保存新增或修改后的 Key，再探测项目');return;}
  toast('正在探测…');
  try{
    const r=await fetch('/api/express-keys/probe',{signal:AbortSignal.timeout(30000),method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({key:rec.key,force:true})});
    const d=await r.json();
    if(!r.ok){ toast('❌ '+(d.error||'探测失败')); return; }
    const res=(d.results||[])[0]||{};
    mergeProbeResults([res.key?res:{...res,key:rec.key}],d.keys||[]);
    toast(res.ok?('项目：'+(res.project_id||'—')):('❌ '+(res.message||'探测失败')));
  }catch(e){ toast('❌ 网络请求失败'); }
}
async function probeAllKeys(){
  if(!keysLoaded){toast('凭证列表尚未加载，请先重新加载配置');return;}
  collectKeys();
  toast('正在探测全部 Key…');
  try{
    const r=await fetch('/api/express-keys/probe',{signal:AbortSignal.timeout(30000),method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({all:true,force:true})});
    const d=await r.json();
    if(!r.ok){ toast('❌ '+(d.error||'探测失败')); return; }
    const results=d.results||[];
    mergeProbeResults(results,d.keys||[]);
    toast('探测完成：成功 '+results.filter(x=>x.ok).length+' / '+results.length);
  }catch(e){ toast('❌ 网络请求失败'); }
}

// Probe responses only update diagnostics/project metadata; preserve unsaved rows and edits.
function mergeProbeResults(results,saved){
  collectKeys();
  results.forEach(res=>{
    KEYS.filter(x=>x.key===res.key).forEach(hit=>{
      const server=saved.find(x=>x.key===res.key);
      const previous=SAVED_KEYS.find(x=>x.key===res.key);
      const edited=previous && (hit.project_id!==previous.project_id || hit.project_source!==previous.project_source);
      if(!edited && (server?.project_id || res.project_id)){
        hit.project_id=server?.project_id || res.project_id;
        hit.project_source=server?.project_source || 'auto';
        hit.detected_at=server?.detected_at || Date.now()/1000;
      }
      hit._msg=res.message||'';hit._msg_ok=!!res.ok;
    });
  });
  SAVED_KEYS=saved.map(credentialCopy);
  renderKeys();
  window.consoleForms?.rebaseKeys();
}

/* ---------- 出站代理（网络出口） ---------- */
function updateProxyPill(on){
  const p=$('proxy-status'); if(!p) return;
  p.textContent = on ? (typeof on==='number'?'已配置 '+on+' 条':'已启用') : '未启用（直连）';
  p.style.background = on ? '#eeeeff' : '#f5f5f5';
  p.style.color = on ? '#5556d9' : '#737373';
}
function updateProxyDraft(){
  const saved=proxyDraft(GLOBAL_SETTINGS.proxy_urls??GLOBAL_SETTINGS.proxy_url);
  const draft=proxyDraft($('proxy-input').value);
  $('proxy-draft-state')?.classList.toggle('hidden',!settingsLoaded||JSON.stringify(saved)===JSON.stringify(draft));
  // 旧节点的测试结果不能作为新草稿的连通性结论。
  $('proxy-test-result').classList.add('hidden');
}
async function saveProxy(){
  if(!settingsLoaded){toast('设置尚未加载，请先在模型参数页重新加载配置');return;}
  const proxies=proxyDraft($('proxy-input').value);
  try{
    const r=await fetch('/api/settings',{signal:AbortSignal.timeout(30000),method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({proxy_urls:proxies})});
    if(r.ok){ const saved=await r.json(); GLOBAL_SETTINGS.proxy_urls=saved.proxy_urls;GLOBAL_SETTINGS.proxy_url=saved.proxy_url;$('proxy-input').value=saved.proxy_urls.join('\n');updateProxyPill(saved.proxy_urls.length);updateProxyDraft();window.consoleForms?.saved('proxy'); toast(proxies.length?'✅ 已保存 '+saved.proxy_urls.length+' 条代理（按顺序故障切换）':'已清空代理，恢复直连'); }
    else toast('❌ '+await responseError(r));
  }catch(e){ toast('❌ 网络请求失败'); }
}
async function testProxy(){
  const v=proxyDraft($('proxy-input').value)[0]||'direct';
  const box=$('proxy-test-result'); box.classList.remove('hidden');
  box.innerHTML='<span class="text-neutral-500">🛰️ 正在测试，首次启动协议转换可能需要较长时间…</span>';
  try{
    const r=await fetch('/api/proxy/test',{signal:AbortSignal.timeout(60000),method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({proxy_url:v})});
    const d=await r.json();
    if(!r.ok) throw new Error(await responseError(r,'测试失败',d));
    if(d.ok){
      box.innerHTML=`<span style="color:#047857">✅ ${esc(d.message)} · 延迟 ${d.latency_ms} ms${d.via_proxy?(' · 经 '+esc(d.proxy)):''}</span>`;
      toast('✅ 连通成功（'+d.latency_ms+' ms）');
    }else{
      box.innerHTML=`<span style="color:#be123c">❌ ${esc(d.message)} · ${d.latency_ms} ms<br>详细：${esc(d.error||'')}</span>`;
      toast('❌ 连通失败');
    }
  }catch(e){
    box.innerHTML='<span style="color:#be123c">❌ '+esc(e.name==='TimeoutError'?'代理测试超时，请重试或检查节点状态':e.message)+'</span>';
  }
}

/* ---------- Cookie 通道指纹伪装 ---------- */
const FP_LABELS = {chrome:'Chrome',edge:'Edge',safari:'Safari',safari_ios:'Safari iOS',firefox:'Firefox',off:'关闭'};
function updateFpPill(v){
  const p=$('fp-status'); if(!p) return;
  const on = v && v !== 'off';
  p.textContent = on ? ('已伪装：'+(FP_LABELS[v]||v)) : '关闭（httpx 原始行为）';
  p.style.background = on ? '#ecfdf5' : '#f5f5f5';
  p.style.color = on ? '#047857' : '#737373';
}
async function saveFingerprint(){
  if(!settingsLoaded){toast('设置尚未加载，请先在模型参数页重新加载配置');return;}
  const v=$('cookie-impersonate').value;
  try{
    const r=await fetch('/api/settings',{signal:AbortSignal.timeout(30000),method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({cookie_impersonate:v})});
    if(r.ok){ GLOBAL_SETTINGS.cookie_impersonate=v;updateFpPill(v);window.consoleForms?.saved('fingerprint'); toast(v==='off'?'✅ 已关闭伪装（下一个请求生效）':'✅ 伪装已保存并热生效：'+(FP_LABELS[v]||v)); }
    else{
      let msg='保存失败';
      try{ const d=await r.json(); if(d && d.error) msg=d.error; }catch(_){}
      toast('❌ '+msg);
    }
  }catch(e){ toast('❌ 网络请求失败'); }
}

/* ---------- Express 通道请求头指纹 ---------- */
const FP2_LABELS = {sdk:'官方 Python SDK',node:'Node.js SDK',gcloud:'gcloud CLI',custom:'自定义'};
function updateExpressPill(v,custom){
  const p=$('fp2-status'); if(!p) return;
  const isDefault = !v || v==='sdk' || (v==='custom' && !(custom||'').trim());
  p.textContent = isDefault ? '官方默认（SDK 自动）' : ('已接管：'+(FP2_LABELS[v]||v));
  p.title=v==='custom'?(custom||''):'';
  p.style.background = isDefault ? '#f5f5f5' : '#ecfdf5';
  p.style.color = isDefault ? '#737373' : '#047857';
}
function updateExpressCustomVis(){
  const sel=$('express-ua-profile'), inp=$('express-ua-custom');
  if(sel && inp) inp.style.display = sel.value==='custom' ? '' : 'none';
}
async function saveExpressUa(){
  if(!settingsLoaded){toast('设置尚未加载，请先在模型参数页重新加载配置');return;}
  const v=$('express-ua-profile').value;
  const c=$('express-ua-custom').value.trim();
  try{
    const r=await fetch('/api/settings',{signal:AbortSignal.timeout(30000),method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({express_ua_profile:v,express_ua_custom:c})});
    if(r.ok){ GLOBAL_SETTINGS.express_ua_profile=v;GLOBAL_SETTINGS.express_ua_custom=c;updateExpressPill(v,c); updateExpressCustomVis();window.consoleForms?.saved('express-fingerprint');
      toast(v==='sdk'||(v==='custom'&&!c)?'✅ 已恢复官方 SDK 原生形态（下一个请求生效）':'✅ 请求头指纹已保存并热生效：'+(FP2_LABELS[v]||v)); }
    else{
      let msg='保存失败';
      try{ const d=await r.json(); if(d && d.error) msg=d.error; }catch(_){}
      toast('❌ '+msg);
    }
  }catch(e){ toast('❌ 网络请求失败'); }
}

/* ---------- Params ---------- */
function setV(id,v){
  const el=$(id);if(!el)return;
  window.consoleForms?.clearError(el);el.removeAttribute('aria-invalid');
  if(id==='image_aspect_ratio'&&v&&![...el.options].some(option=>option.value===v)){
    const option=document.createElement('option');option.value=v;option.textContent=v+'（当前模型不支持）';option.disabled=true;el.add(option);
  }
  if(el.type==='checkbox')el.checked=!!v;else el.value=(v===null||v===undefined)?'':v;
}
async function loadParams(){
  captureModelDraft();
  const selected=$('model-sel').value;
  const formDrafts=window.consoleForms?.capture();
  loadNotice('params','');
  // 设置与模型能力两个接口互不依赖：并行拉取，面板加载不再串行等两次往返。
  const [sRes,cRes]=await Promise.allSettled([readJSON('/api/settings'),readJSON('/api/capabilities')]);
  if(sRes.status==='fulfilled'){
    const s=sRes.value;
    GLOBAL_SETTINGS=s;
    settingsLoaded=true;
    curAR = s.image_aspect_ratio || "";
    ['resumable_disconnect_grace_seconds','img_compress_max_dim','img_compress_max_mb','img_compress_quality','retry_max','retry_backoff_seconds','retry_jitter_seconds','fake_streaming_interval','prefill_mode','express_location','resumable_ttl_seconds','resumable_max_streams','request_timeout_seconds','upstream_max_concurrency','resumable_max_buffer_mb','resumable_max_total_mb','keepalive_url','keepalive_interval','cookie_refresh_interval_seconds'].forEach(k=>setV(k,s[k]));
    ['img_compress_enabled','fake_streaming','safety_score','cookie_debug','debug_outbound','prefill_suppress_thinking','resumable_enabled','cookie_structured_output','cross_channel_fallback','cookie_auto_refresh'].forEach(k=>setV(k,s[k]));
    setV('credential-strategy',s.roundrobin?'roundrobin':'random');
    // 出站代理：回填输入框并刷新状态徽标
    setV('proxy-input', proxyDraft(s.proxy_urls??s.proxy_url).join('\n'));
    updateProxyPill(proxyDraft(s.proxy_urls??s.proxy_url).length);
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
  }else{loadNotice('params','设置'+sRes.reason.message+'。重新加载成功后再保存。');}
  if(cRes.status==='fulfilled'){
    const c=cRes.value;
    CAPS=c.capabilities||{};
    OVERRIDES=c.overrides||{};
    SAVED_MODEL_OVERRIDES=c.saved_overrides||c.overrides||{};
    BUILTIN_OVERRIDES=c.builtin_overrides||{};
    capabilitiesLoaded=true;
    $('model-sel').innerHTML='<option value="__global__">＊ 全局默认（对所有模型）</option>'
      + (c.models||[]).map(m=>{const star=OVERRIDES[m]?' ★':''; return `<option value="${esc(m)}">${esc(m)}${star}</option>`;}).join('');
    if([...$('model-sel').options].some(option=>option.value===selected)) $('model-sel').value=selected;
    renderCaps(false);
  }else{loadNotice('params','模型能力'+cRes.reason.message+'。重新加载成功后再保存模型参数。');}
}
function readModelFields(){
  return Object.fromEntries(PER_MODEL_KEYS.map(key=>[key,$(key).type==='checkbox'?$(key).checked:$(key).value]));
}
function captureModelDraft(){
  if(!renderedModel || modelSnapshot===null) return;
  const values=readModelFields(), baseline=JSON.parse(modelSnapshot);
  for(const key of PER_MODEL_KEYS){
    if(!modelInherit.has(key)&&(values[key]!==baseline[key]||modelPins.has(key))) modelEdits.add(key);
    else modelEdits.delete(key);
  }
  const dirty=renderedModel==='__global__'?JSON.stringify(values)!==modelSnapshot:modelEdits.size>0||modelInherit.size>0;
  if(dirty) MODEL_DRAFTS.set(renderedModel,{values,edited:[...modelEdits],inherit:[...modelInherit],pinned:[...modelPins]});
  else MODEL_DRAFTS.delete(renderedModel);
  $('scope-draft-state').classList.toggle('hidden',!dirty);
  refreshModelSelStars();
  renderFieldSources();
}
function finishModelRender(model){
  modelSnapshot=JSON.stringify(readModelFields());
  modelEdits=new Set();modelInherit=new Set();modelPins=new Set();
  const draft=MODEL_DRAFTS.get(model);
  if(draft){
    // Reapply only edited fields: inherited values must follow newer defaults.
    for(const key of draft.edited) setV(key,draft.values[key]);
    for(const key of draft.inherit) setV(key,inheritedModelValue(model,key));
    modelEdits=new Set(draft.edited);modelInherit=new Set(draft.inherit);modelPins=new Set(draft.pinned||[]);
  }
  curAR=$('image_aspect_ratio').value;
  renderedModel=model;
  captureModelDraft();
  updateSamplingDisplay();
  updateModelCapabilities();
}
function discardModelDraft(){
  MODEL_DRAFTS.delete($('model-sel').value);renderCaps(false);
}
function inheritedModelValue(model,key){
  const builtin=BUILTIN_OVERRIDES[model]||{};
  return Object.hasOwn(builtin,key)?builtin[key]:key==='anti_truncation_mode'?'off':GLOBAL_SETTINGS[key];
}
function setFieldInheritance(key,inherit){
  if(!PER_MODEL_KEYS.includes(key)||renderedModel==='__global__') return;
  if(inherit){
    modelEdits.delete(key);modelPins.delete(key);
    if(Object.hasOwn(SAVED_MODEL_OVERRIDES[renderedModel]||{},key)) modelInherit.add(key);else modelInherit.delete(key);
    setV(key,inheritedModelValue(renderedModel,key));
  }else{if($(key).disabled)return;modelInherit.delete(key);modelPins.add(key);}
  captureModelDraft();updateModelCapabilities();
}
function renderFieldSources(){
  for(const key of PER_MODEL_KEYS){
    const el=$(key);let row=$('source-'+key);
    if(!row){row=document.createElement('div');row.id='source-'+key;row.className='field-source';
      const anchor=el.closest('.switch')?.parentElement||(el.parentElement.classList.contains('flex')?el.parentElement:el);anchor.after(row);}
    const global=renderedModel==='__global__';
    const own=!global&&!modelInherit.has(key)&&(modelEdits.has(key)||Object.hasOwn(SAVED_MODEL_OVERRIDES[renderedModel]||{},key));
    const builtin=Object.hasOwn(BUILTIN_OVERRIDES[renderedModel]||{},key);
    row.innerHTML=global?'':`<span>${own?'模型专属':builtin?'继承内置默认':key==='anti_truncation_mode'?'继承模型默认':'继承全局默认'}${modelEdits.has(key)||modelInherit.has(key)?' · 未保存':''}</span><button type="button" data-inherit="${key}" data-value="${own}" ${el.disabled&&!own?'disabled':''}>${own?'恢复继承':'设为专属'}</button>`;
    row.hidden=global;
  }
}
function updateModelCapabilities(){
  const cap=CAPS[$('model-sel').value],global=$('model-sel').value==='__global__';
  const imageOn=global||!!cap?.is_image;
  $('image-settings').classList.toggle('is-inapplicable',!imageOn);
  $('image-applicability').hidden=imageOn;
  $('image-controls').hidden=!imageOn&&!$('show-image-settings').checked;
  for(const key of ['image_size','image_aspect_ratio','image_system_instruction','inject_prefill_for_image']) $(key).disabled=!imageOn;
  for(const [key,allowed] of [['thinking_g3_level',cap?.thinking?.levels],['image_size',cap?.image_sizes]]){
    for(const option of $(key).options) option.disabled=!global&&!!allowed?.length&&option.value!==''&&!allowed.includes(option.value);
  }
  renderFieldSources();
}
document.addEventListener('click',event=>{const button=event.target.closest('[data-inherit]');if(button){const key=button.dataset.inherit;setFieldInheritance(key,button.dataset.value==='true');$('source-'+key).querySelector('button')?.focus({preventScroll:true});}});
// 按编辑作用域回填模型参数：专属值优先，否则使用全局默认。
function applyModelParamFields(model){
  // __global__ = 全局默认作用域：只显示全局值，不掺任何模型专属值
  const ov = (model==='__global__') ? {} : (OVERRIDES[model] || {});
  PER_MODEL_KEYS.forEach(k=>{
    const v = (k in ov) ? ov[k] : (k==='anti_truncation_mode' ? 'off' : GLOBAL_SETTINGS[k]);
    if(k==='image_aspect_ratio'){ curAR = v || ""; }
    else setV(k, v);
  });
  const isGlobal = (model==='__global__');
  const has = !isGlobal && !!OVERRIDES[model] && Object.keys(OVERRIDES[model]).length>0;
  $('ov-badge').classList.toggle('hidden', !has);
  // 保存按钮的落点随作用域切换，避免"看着像全局、存进了专属"（或反过来）
  $('model-footer-scope').textContent=isGlobal?'保存到：全局默认':'保存到：'+model;
  $('btn-scope-save').textContent = isGlobal ? '保存为全局默认' : '保存为该模型专属';
  $('btn-scope-clear').classList.toggle('hidden', isGlobal);
  $('ov-hint').textContent = isGlobal
    ? '当前编辑：全局默认。具体模型的专属值优先。'
    : (has ? '当前编辑：模型专属参数（★）。保存仅影响此模型。'
           : '当前继承默认值。保存后将创建此模型的专属参数。');
}
async function saveModelOverride(){
  if(!settingsLoaded || !capabilitiesLoaded){toast('配置尚未加载，请先重新加载配置');return;}
  const m=$('model-sel').value; if(!m) return;
  let patch={
    first_reply_timeout_seconds:numOr('first_reply_timeout_seconds',15),
    anti_truncation_mode:$('anti_truncation_mode').value,
    native_thinking_mode:$('native_thinking_mode').value,
    thinking_g3_level:$('thinking_g3_level').value,
    image_size:$('image_size').value,
    image_aspect_ratio:$('image_aspect_ratio').value,
    default_temperature:numOrNull('default_temperature'),
    default_top_p:numOrNull('default_top_p'),
    default_max_tokens:numOrNull('default_max_tokens'),
    inject_system_instruction:$('inject_system_instruction').value,
    inject_prefill:$('inject_prefill').value,
    prefill_instruction:$('prefill_instruction').value,
    image_system_instruction:$('image_system_instruction').checked,
    inject_prefill_for_image:$('inject_prefill_for_image').checked,
    sampling_policy:$('sampling_policy').value,
    prefill_cot_guard:$('prefill_cot_guard').checked,
  };
  if(m==='__global__'){
    delete patch.anti_truncation_mode;
    // 全局默认作用域：写进全局设置，而不是任何模型的专属值
    try{
      const r=await fetch('/api/settings',{signal:AbortSignal.timeout(30000),method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(patch)});
      if(r.ok){ Object.assign(GLOBAL_SETTINGS,patch); MODEL_DRAFTS.delete(m); renderCaps(false); toast('已保存为全局默认'); }
      else toast(await responseError(r));
    }catch(e){ toast('保存失败'); }
    return;
  }
  patch=Object.fromEntries(Object.entries(patch).filter(([key])=>modelEdits.has(key)));
  if(modelInherit.size) patch._inherit=[...modelInherit];
  if(!Object.keys(patch).length){toast('没有需要保存的修改');return;}
  try{
    const r=await fetch('/api/model-overrides/'+encodeURIComponent(m),{signal:AbortSignal.timeout(30000),method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(patch)});
    const d=await r.json();
    if(r.ok){ if(Object.keys(d.override||{}).length)OVERRIDES[m]=d.override;else delete OVERRIDES[m];
      const own={...(SAVED_MODEL_OVERRIDES[m]||{})};for(const key of modelInherit)delete own[key];
      Object.assign(own,Object.fromEntries(Object.entries(patch).filter(([key])=>key!=='_inherit')));SAVED_MODEL_OVERRIDES[m]=own;
      MODEL_DRAFTS.delete(m); renderCaps(false); toast('已保存 '+m+' 专属参数'); }
    else toast(await responseError(r,'保存失败',d));
  }catch(e){ toast('❌ 网络请求失败'); }
}
async function clearModelOverride(){
  if(!capabilitiesLoaded){toast('模型配置尚未加载，请先重新加载配置');return;}
  const m=$('model-sel').value; if(!m) return;
  if(!OVERRIDES[m]){ toast('该模型没有专属参数'); return; }
  if(!confirm('清除 '+m+' 的全部专属参数及本页草稿，恢复全局 / 内置默认？'))return;
  try{
    const r=await fetch('/api/model-overrides/'+encodeURIComponent(m),{signal:AbortSignal.timeout(30000),method:'DELETE'});
    if(r.ok){
      const d=await r.json();
      if(d.override && Object.keys(d.override).length) OVERRIDES[m]=d.override;
      else delete OVERRIDES[m];
      delete SAVED_MODEL_OVERRIDES[m];
      MODEL_DRAFTS.delete(m); renderCaps(false); toast('已恢复 '+m+' 默认参数');
    }
    else toast(await responseError(r,'清除失败'));
  }catch(e){ toast('❌ 网络请求失败'); }
}
function refreshModelSelStars(){
  const sel=$('model-sel'); const cur=sel.value;
  [...sel.options].forEach(o=>{ const base=o.value; o.textContent = (base==='__global__'?'＊ 全局默认（对所有模型）':base+(OVERRIDES[base]?' ★':''))+(MODEL_DRAFTS.has(base)?' · 草稿':''); });
  sel.value=cur;
}
function chip(t, accent,sampling=false){ return `<span class="pill${accent?' pill-accent':''}"${sampling?' data-sampling':''}>${esc(t)}</span>`; }
function fillARFor(cap){
  const sel=$('image_aspect_ratio');
  const list = cap?.is_image && cap.image_aspect_ratios?.length ? cap.image_aspect_ratios : COMMON_ARS;
  sel.innerHTML='<option value="">自动</option>' + list.map(a=>`<option value="${esc(a)}">${esc(a)}</option>`).join('');
  if(curAR&&!list.includes(curAR)) sel.insertAdjacentHTML('beforeend',`<option value="${esc(curAR)}" disabled>${esc(curAR)}（当前模型不支持）</option>`);
  sel.value=curAR||'';
}
function renderCaps(capture=true){
  if(capture) captureModelDraft();
  const m=$('model-sel').value;
  if(m==='__global__'){
    applyModelParamFields(m);
    $('anti_truncation_mode').disabled=true;
    $('anti-truncation-note').textContent='请先选择具体模型再设置其后缀；普通无后缀名称始终不启用抗截断。';
    $('caps-summary').innerHTML='<span class="text-xs text-neutral-500">全局默认作用域：下面的值对所有未设 ★ 专属的模型生效。切到具体模型可查看该模型的能力与专属值。</span>';
    ['thinking_g3_level','image_size','image_aspect_ratio'].forEach(id=>{const el=$(id); if(el) el.disabled=false;});
    $('wrap-g3level').style.opacity='1';
    $('think-note').textContent='全局默认：具体下发时仍按各模型能力自动裁剪（例如 Pro 上 minimal 会就近取 low）。';
    $('image-note').textContent='全局默认：仅生图模型会用到。';
    $('sampling-note').textContent='全局默认：已废弃采样参数的模型会自动剥离。';
    fillARFor(null);
    finishModelRender(m);
    return;
  }
  const cap=CAPS[m]; if(!cap) return;
  applyModelParamFields(m);   // 按模型填入专属/全局参数值（含 curAR）
  $('anti_truncation_mode').disabled=!cap.supports_anti_truncation;
  $('anti-truncation-note').textContent=cap.supports_anti_truncation
    ? '保存为该模型专属后刷新客户端列表；仅选择已开启的抗截断后缀才使用抗截断，普通名称不启用。'
    : '该模型不支持文本函数传输，不提供抗截断版本。';
  const th=cap.thinking||{};
  let sum=[chip('家族 '+cap.family, true)];
  if(th.kind==='level') sum.push(chip('思考档位 '+(th.levels||[]).join(' / ')));
  else sum.push(chip('无思考调节'));
  if(cap.is_image){
    sum.push(chip('分辨率 '+cap.image_sizes.join('/')));
    sum.push(chip('比例 '+cap.image_aspect_ratios.length+' 种'));
    sum.push(chip('不支持函数调用'));
  } else {
    sum.push(chip('采样',false,true));
    sum.push(chip('支持函数调用'));
  }
  if(cap.supports_search) sum.push(chip('支持搜索'));
  $('caps-summary').innerHTML=sum.join('');

  const isLevel=th.kind==='level';
  $('thinking_g3_level').disabled=!isLevel;
  $('wrap-g3level').style.opacity=isLevel?'1':'.4';
  let tn='';
  if(cap.is_image) tn='生图模型思考档位（默认 high，可在上方调整）。';
  else if(isLevel) tn='该模型用思考档位' + ((th.levels && !th.levels.includes('minimal')) ? '（此模型最低 low，无 minimal，且无法关闭）' : '（无法完全关闭，最省为 minimal）') + '。';
  else tn='该模型不支持思考调节。';
  $('think-note').textContent=tn;

  const imgOn=cap.is_image;
  $('image_size').disabled=!imgOn; $('image_aspect_ratio').disabled=!imgOn;
  fillARFor(cap);
  $('image-note').textContent = imgOn
    ? ('该模型支持比例：' + cap.image_aspect_ratios.join('、') + '；分辨率 ' + cap.image_sizes.join('/') + '。选到不支持的比例不会报错，会自动回退为“由模型决定”。')
    : '仅生图模型（名称含 image）使用此项。';

  finishModelRender(m);
}
function updateSamplingDisplay(){
  const model=$('model-sel').value,cap=CAPS[model],policy=$('sampling_policy').value;
  if(model==='__global__'){
    $('sampling-note').textContent=policy==='allowed'?'全局默认选择强制保留采样；生图模型仍会剥离，请确认其他模型支持。':policy==='deprecated'?'全局默认选择强制剥离采样参数。':'全局默认：按各模型内置规则自动处理采样参数。';
    return;
  }
  if(!cap) return;
  const advice=policy==='auto'?(cap.default_sampling_advice??cap.sampling_advice):policy==='deprecated'?'deprecated':'recommend_default';
  const label=cap.is_image?'生图模型 · 自动剥离':policy==='allowed'?'强制保留':policy==='deprecated'?'强制剥离':advice==='deprecated'?'自动剥离':'自动判定';
  const chip=$('caps-summary').querySelector('[data-sampling]');
  if(chip) chip.textContent='采样 '+label;
  $('sampling-note').textContent = cap.is_image
    ? '生图模型会自动剥离采样参数。'
    : policy==='allowed'?'当前选择强制保留采样参数；请确认上游模型支持此设置。保存后生效。'
    : policy==='deprecated'?'当前选择强制剥离 temperature / top_p / top_k；保存后生效，其他参数仍按模型能力处理。'
    : (advice==='deprecated'
        ? '⚠️ 该模型已废弃 temperature/top_p/top_k（官方要求移除，现忽略、未来 400），代理会自动移除；如需更确定的输出请改用系统指令。candidate_count 在 3.x 也不支持。'
        : advice==='recommend_default'
          ? '官方建议 Gemini 3.x 保持采样默认值（可调，但改动可能致循环/降智）；留空即用模型默认。'
          : '该模型支持采样参数；留空即用模型默认。');
}
function numOrNull(id){ const v=$(id).value.trim(); return v===''?null:Number(v); }
function numOr(id,d){ const v=$(id).value.trim(); return v===''?d:Number(v); }
// 说明折叠：点 ⓘ 展开/收起对应的说明块
function hlp(el,id){const b=$(id);const on=b.classList.toggle('show');b.hidden=!on;el.classList.toggle('on',on);el.setAttribute('aria-expanded',String(on));el.setAttribute('aria-label',on?'收起说明':'展开说明');}
async function saveSettings(){
  if(!settingsLoaded){toast('设置尚未加载，请先重新加载配置');return;}
  // 只保存全局运行设置。
  // 模型行为字段显示的是当前编辑对象的生效值，不能在这里一并提交。
  // 一起提交会把某个模型的专属值（比如生图的注入内容）写成全局默认。
  const patch={
    img_compress_enabled:$('img_compress_enabled').checked,
    img_compress_max_dim:numOr('img_compress_max_dim',1536),
    img_compress_max_mb:numOr('img_compress_max_mb',1.5),
    img_compress_quality:numOr('img_compress_quality',85),
    retry_max:numOr('retry_max',10),
    retry_backoff_seconds:numOr('retry_backoff_seconds',5),
    retry_jitter_seconds:numOr('retry_jitter_seconds',1),
    cross_channel_fallback:$('cross_channel_fallback').checked,
    fake_streaming:$('fake_streaming').checked,
    fake_streaming_interval:numOr('fake_streaming_interval',1),
    safety_score:$('safety_score').checked,
    cookie_debug:$('cookie_debug').checked,
    cookie_structured_output:$('cookie_structured_output').checked,
    debug_outbound:$('debug_outbound').checked,
    prefill_mode:$('prefill_mode').value,
    express_location:$('express_location').value,
    prefill_suppress_thinking:$('prefill_suppress_thinking').checked,
    resumable_enabled:$('resumable_enabled').checked,
    resumable_disconnect_grace_seconds:numOr('resumable_disconnect_grace_seconds',10),
    resumable_ttl_seconds:numOr('resumable_ttl_seconds',1800),
    resumable_max_streams:numOr('resumable_max_streams',200),
    request_timeout_seconds:numOr('request_timeout_seconds',1800),
    upstream_max_concurrency:numOr('upstream_max_concurrency',32),
    resumable_max_buffer_mb:numOr('resumable_max_buffer_mb',64),
    resumable_max_total_mb:numOr('resumable_max_total_mb',256),
    keepalive_url:$('keepalive_url').value.trim(),
    keepalive_interval:numOr('keepalive_interval',60),
    cookie_auto_refresh:$('cookie_auto_refresh').checked,
    cookie_refresh_interval_seconds:numOr('cookie_refresh_interval_seconds',600),
  };
  const m=$('model-sel').value;
  const overriding = !!(OVERRIDES[m] && Object.keys(OVERRIDES[m]).length>0);
  try{
    const r=await fetch('/api/settings',{signal:AbortSignal.timeout(30000),method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(patch)});
    if(r.ok){
      const saved=await r.json();
      // Another section may have saved model defaults while this response was in flight.
      // Only merge the fields owned by this form, never an older full-state snapshot.
      Object.keys(patch).forEach(key=>GLOBAL_SETTINGS[key]=saved[key]);
      window.consoleForms?.saved('runtime');
      toast(overriding?'全局运行设置已保存；模型专属参数保持不变':'全局运行设置已保存');
    }
    else toast(await responseError(r));
  }catch(e){ toast('❌ 网络请求失败'); }
}

/* ---------- Logs: filtering is local; the bounded SSE feed stays connected. ---------- */
const logwin=$('logwin');
let autoscroll=true;
const LOG_DOM_MAX=500;
function setLogFollow(value){
  autoscroll=value;
  $('log-follow').textContent='自动滚动：'+(value?'开':'关');
  $('log-follow').setAttribute('aria-pressed',String(value));
}
logwin.addEventListener('scroll',()=>{if(autoscroll && logwin.scrollHeight-logwin.scrollTop-logwin.clientHeight>=50) setLogFollow(false);});
function logLevel(text){
  if(/❌|ERROR|失败/.test(text)) return 'error';
  if(/⚠️|WARN|🔄|重试/.test(text)) return 'warning';
  return 'info';
}
function logLine(text){
  const level=logLevel(text);
  const content=esc(text).replace(/(gemini-[a-zA-Z0-9.\-]+)/g,'<span class="log-model">$1</span>')
    .replace(/\b(trace-[a-f0-9]{16})\b/g,'<button class="request-link" data-request="$1">$1</button>');
  return `<div class="log-line ${level}" data-level="${level}">${content}</div>`;
}
function filterLogs(){
  const query=$('log-search').value.trim().toLowerCase(),level=$('log-level').value;
  const noFilter=query===''&&level==='all';
  let visible=0;
  if(noFilter && !logwin.querySelector('[hidden]')){
    // 无过滤且没有隐藏行：跳过对最多 500 行 DOM 的全量文本读取
    // （每条 SSE 日志都会走到这里，突发时是每秒数千次字符串物化）。
    visible=logwin.childElementCount;
  }else{
    for(const row of logwin.children){
      row.hidden=!noFilter&&!(row.textContent.toLowerCase().includes(query)&&(level==='all'||row.dataset.level===level));
      if(!row.hidden) visible++;
    }
  }
  $('log-count').textContent=`${visible} / ${logwin.childElementCount} 条日志`;
  logwin.dataset.empty=String(visible===0 && logwin.childElementCount>0);
}
let logFilterTimer=null;
function scheduleLogFilter(){
  // 突发日志合并刷新：150ms 内的多条消息只做一次全量过滤；
  // 无过滤条件时走上面的快速路径，仍然只是计数更新。
  if(logFilterTimer) return;
  logFilterTimer=setTimeout(()=>{logFilterTimer=null;filterLogs();},150);
}
$('log-search').addEventListener('input',filterLogs);
$('log-level').addEventListener('change',filterLogs);
$('log-follow').addEventListener('click',()=>{setLogFollow(!autoscroll);if(autoscroll) logwin.scrollTop=logwin.scrollHeight;});
$('log-clear').addEventListener('click',()=>{logwin.replaceChildren();filterLogs();});
try{
  const es=new EventSource('/stream-logs');
  es.onopen=()=>{$('log-state').textContent='已连接';$('log-connection').className='status-dot';};
  es.onerror=()=>{$('log-state').textContent='连接中断，正在重连…';$('log-connection').className='status-dot error';};
  es.onmessage=event=>{
    if(event.data==='keep-alive') return;
    const follow=autoscroll;
    logwin.insertAdjacentHTML('beforeend',logLine(event.data));
    while(logwin.childElementCount>LOG_DOM_MAX) logwin.firstElementChild.remove();
    // 无过滤时新行天然可见；有过滤时 150ms 合并刷新一次，
    // 避免每条消息都触发全量 DOM 过滤。
    scheduleLogFilter();
    if(follow && !$('view-logs').classList.contains('hidden')) logwin.scrollTop=logwin.scrollHeight;
  };
}catch(error){$('log-state').textContent='无法建立日志连接';$('log-connection').className='status-dot error';}

/* ---------- Project ID auto-extract ---------- */
$('image_aspect_ratio').addEventListener('change', e=>{ curAR=e.target.value; });

/* init */
let channelConfigLoading=null;
function channelAssetNotice(message){
  let box=$('console-assets-error');
  if(!box){
    box=document.createElement('div');box.id='console-assets-error';box.className='inline-notice hidden';box.setAttribute('role','alert');
    box.innerHTML='<span></span><button type="button" class="btn-ghost" onclick="location.reload()">刷新面板</button>';
    document.querySelector('main').prepend(box);
  }
  box.classList.toggle('hidden',!message);box.querySelector('span').textContent=message;
}
async function ensureChannelConfig(){
  if(window.consoleChannels) return window.consoleChannels;
  if(!channelConfigLoading){
    channelConfigLoading=new Promise((resolve,reject)=>{
      const script=document.createElement('script');
      const source=document.querySelector('script[src^="/static/channel-config.js"]')?.src || '/static/channel-config.js';
      const url=new URL(source,location.href);url.searchParams.set('retry',Date.now());
      script.src=url.href;script.async=false;script.dataset.cfasync='false';
      const timer=setTimeout(()=>finish(new Error('账号与代理编辑器加载超时')),10000);
      function finish(error){
        clearTimeout(timer);script.onload=script.onerror=null;
        if(error){script.remove();reject(error);}else resolve(window.consoleChannels);
      }
      script.onload=()=>finish(window.consoleChannels?null:new Error('账号与代理编辑器版本不一致'));
      script.onerror=()=>finish(new Error('账号与代理编辑器加载失败'));
      document.head.append(script);
    });
  }
  try{return await channelConfigLoading;}finally{channelConfigLoading=null;}
}
async function loadChannelFeatures(){
  try{
    const channels=await ensureChannelConfig();
    await channels.load();
    channels.init();
    channelAssetNotice('');
  }catch(error){channelAssetNotice(error.message+'。已保存的 Key、代理和模型参数仍可查看与修改，请刷新或重新加载配置以重试编辑器。');}
}
async function refreshCookies(index=null){
  try{
    const channels=await ensureChannelConfig();
    await channels.refreshCookies(index);
  }catch(error){toast('Cookie 刷新未完成：'+error.message);}
}
async function manageProxies(scope='global',index=0,view='list'){
  try{
    const channels=await ensureChannelConfig();channels.init();channels.open(scope,index,view);
  }catch(error){channelAssetNotice(error.message+'，请重新加载配置或刷新面板。');toast(error.message);}
}
// Keep buttons in a cached previous HTML document usable during an upgrade.
function openProxyEditor(scope='global',index=0){return manageProxies(scope,index);}
function reloadConfiguration(){
  if(window.consoleForms?.dirty()&&!confirm('重新加载会丢弃尚未保存的账号修改。继续？'))return;
  loadNotice('channel','');
  return Promise.allSettled([loadRuntime(),loadParams(),loadExpressKeys(),loadChannelFeatures()]);
}
// Read local state before initializing optional dialogs. An asset failure must
// never hide the saved proxy or prevent the user from correcting it.
navigateHash();
fetchStats(); setInterval(()=>{ if(!document.hidden) fetchStats(); },3000);
loadRuntime(); loadParams(); loadExpressKeys(); loadChannelFeatures();

async function runAction(button,action){
  if(button.matches(':disabled')) return;
  const groups=button.closest('dialog')?[...button.closest('dialog').querySelectorAll('fieldset')]:button.closest('.inline-notice')?[...document.querySelectorAll('main fieldset')]:[button.closest('fieldset')].filter(Boolean);
  if(window.consoleForms && !window.consoleForms.validate(button)) return;
  if(groups.some(group=>group.disabled)){toast('该分区正在处理，请稍后再试');return;}
  const label=button.textContent;
  button.disabled=true;button.setAttribute('aria-busy','true');button.textContent='处理中…';
  const disabled=groups.map(group=>group.disabled);
  groups.forEach(group=>{group.disabled=true;group.setAttribute('aria-busy','true');});
  try{await action();}
  finally{button.disabled=false;button.removeAttribute('aria-busy');if(button.textContent==='处理中…') button.textContent=label;groups.forEach((group,i)=>{group.disabled=disabled[i];group.removeAttribute('aria-busy');});window.consoleChannels?.sync?.();}
}
document.addEventListener('input',event=>{
  event.target.removeAttribute('aria-invalid');window.consoleForms?.clearError(event.target);
  if(event.target.id==='proxy-input') updateProxyDraft();
  if(PER_MODEL_KEYS.includes(event.target.id)){modelEdits.add(event.target.id);modelInherit.delete(event.target.id);captureModelDraft();if(event.target.id==='sampling_policy') updateSamplingDisplay();}
});

/* Recent diagnostics are a bounded retained sample, not a lifetime counter. */
let recentLoading=false;
async function fetchRecentMetrics(){
  if(recentLoading||document.hidden||$('view-overview').classList.contains('hidden'))return;
  recentLoading=true;
  try{
    const d=await readJSON('/api/recent-metrics'),completed=d.success+d.error;
    $('recent-rate').textContent=completed?(d.success/completed*100).toFixed(1)+'%':'—';
    const duration=v=>v==null?'—':(v/1000).toFixed(1)+'s';
    $('recent-latency').textContent=duration(d.first_reply_p50_ms)+' / '+duration(d.first_reply_p95_ms);
    $('recent-errors').textContent=fmt(d.error);
    $('recent-note').textContent=`近 15 分钟接收且仍保留的主请求 ${d.sample_count} 条，首回复样本 ${d.reply_samples} 条；`+Object.entries(d.channels).map(([name,c])=>`${name}：成功 ${c.success} / 失败 ${c.error} / 运行 ${c.active}`).join('；')+'。重启和记录回收会缩短样本范围；取消不计入成功率。';
  }catch(error){$('recent-note').textContent='近期统计暂不可用：'+error.message;}
  finally{recentLoading=false;}
}
fetchRecentMetrics();setInterval(fetchRecentMetrics,15000);
