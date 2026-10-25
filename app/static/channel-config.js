/* Cookie accounts and the ordered proxy editor. Shared form helpers live in dashboard.js.
 * Keep state local so a failed/cached script can be retried without redeclaration errors. */
(() => {
if(window.consoleChannels) return;
let COOKIE_ACCOUNTS=[], SAVED_COOKIES=[], cookiesLoaded=false, proxyEditor=null, proxyEditorInitialized=false;
let cookieRefreshPending=false, cookieTestPending=false;
// In-memory only: proxy credentials and test results never enter localStorage.
const proxyResults=new Map();

async function updateCredentialStrategy(select){
  if(!settingsLoaded){toast('设置尚未加载，请先重新加载配置');return;}
  const previous=GLOBAL_SETTINGS.roundrobin;
  select.disabled=true;
  try{
    const roundrobin=select.value==='roundrobin';
    const response=await fetch('/api/settings',{method:'POST',signal:AbortSignal.timeout(30000),headers:{'Content-Type':'application/json'},body:JSON.stringify({roundrobin})});
    if(!response.ok) throw new Error(await responseError(response));
    GLOBAL_SETTINGS.roundrobin=roundrobin;toast('账号调度已切换为'+(roundrobin?'按顺序轮询':'随机使用'));
  }catch(error){select.value=previous?'roundrobin':'random';toast(error.message);}
  finally{select.disabled=false;}
}

/* Cookie 会话探测结果文案：与后端 cookie_probe 的三种状态一一对应。 */
const COOKIE_TEST_STATUS={
  valid:{label:'✓ 会话有效',ok:true},
  invalid:{label:'✕ 会话已失效',ok:false},
  error:{label:'⚠ 无法判定',ok:false},
};
function cookieTestMessage(res){
  const st=COOKIE_TEST_STATUS[res.status]||{label:res.status,ok:false};
  const secs=(res.elapsed_ms>0)?` · ${(res.elapsed_ms/1000).toFixed(1)}s`:'';
  return st.label+(res.detail?' · '+res.detail:'')+secs;
}
/* 调度健康 pill：账号被自动冷却隔离或连续失败时才显示，正常状态保持安静。 */
function cookieHealthPill(rec){
  const health=rec.health;
  if(!health) return '';
  if(Number(health.cooldown_remaining)>0)
    return `<span class="pill" style="text-transform:none;color:#b45309" title="鉴权失败累计，调度已临时隔离该账号">冷却中 ${Math.ceil(Number(health.cooldown_remaining))}s</span>`;
  if(Number(health.failures)>0)
    return `<span class="pill" style="text-transform:none;color:#b45309" title="近期鉴权失败 ${health.failures} 次，再失败可能进入冷却">失败 ${health.failures} 次</span>`;
  return '';
}
function renderCookieAccounts(){
  if(!$('cookies-list')) return; // 分区尚未随页面就绪时保持静默（缓存旧文档/升级中）
  window.consoleForms?.rememberAccounts('cookie');
  $('cookies-list').innerHTML=COOKIE_ACCOUNTS.map((rec,index)=>`
    <div class="key-card cookie-card" data-cookie-index="${index}">
      <div class="key-head"><div class="key-title"><span class="pill pill-accent">Cookie ${index+1}</span>
        <span class="pill">${rec.cookie_configured?'已保存 Cookie':'待填写 Cookie'}</span>${cookieHealthPill(rec)}</div>
        <div class="key-actions"><label><input type="checkbox" class="c-enabled" ${rec.enabled===false?'':'checked'} aria-label="启用 Cookie ${index+1}">启用</label>
          <button type="button" ${rec.enabled===false?'disabled title="启用并保存账号后可测试"':(rec.id?'':'disabled title="保存账号后可测试"')} onclick="runAction(this,()=>testCookieAccount(${index}))">测试会话</button>
          <button type="button" ${rec.enabled===false?'disabled title="启用并保存账号后可刷新"':''} onclick="runAction(this,()=>refreshCookies(${index}))">立即刷新</button>
          <button type="button" class="danger" onclick="removeCookieAccount(${index})" aria-label="删除 Cookie ${index+1}">删除</button></div></div>
      <div class="key-fields">
        <div><label for="cookie-${index}">Google Cookie（含 HttpOnly 字段）</label>
          <textarea id="cookie-${index}" class="inp log c-cookie" rows="3" spellcheck="false" autocomplete="off" placeholder="${esc(rec.cookie_configured?'已保存，留空保留原 Cookie；粘贴新值可更新':'粘贴完整 Cookie 头或 Cookie-Editor JSON')}">${esc(rec.cookie||'')}</textarea>
          ${rec.cookie_preview?`<div class="key-meta">${esc(rec.cookie_preview)}</div>`:''}</div>
        <div><label for="cookie-project-${index}">Project ID</label><input id="cookie-project-${index}" class="inp log c-project" spellcheck="false" placeholder="此账号对应的项目 ID 或控制台 URL" value="${esc(rec.project_id)}">
          <label class="note-label" for="cookie-note-${index}">备注</label><input id="cookie-note-${index}" class="inp c-note" placeholder="便于区分账号" value="${esc(rec.note)}"></div>
        ${accountProxyFields('cookie',index,rec)}
      </div>
      ${rec._msg?`<div class="key-meta ${rec._msg_ok===false?'error-message':''}" role="status">${esc(rec._msg)}</div>`:''}
    </div>`).join('');
  window.consoleForms?.accounts('cookie');
  $('cookies-count').textContent=COOKIE_ACCOUNTS.length+' 个';
  $('cookies-empty').classList.toggle('hidden',COOKIE_ACCOUNTS.length>0);
}
function collectCookieAccounts(){
  document.querySelectorAll('[data-cookie-index]').forEach(row=>{
    const rec=COOKIE_ACCOUNTS[Number(row.dataset.cookieIndex)];
    // DOM 与数组失步（多标签页/并发重渲染）时补建记录而不是抛错：
    // 保存动作必须总能把用户当前看到的内容完整提交。
    const target=rec||(COOKIE_ACCOUNTS[Number(row.dataset.cookieIndex)]={id:'',cookie_configured:false});
    target.cookie=row.querySelector('.c-cookie')?.value.trim()??'';
    target.project_id=projectFromInput(row.querySelector('.c-project')?.value??'');
    target.note=row.querySelector('.c-note')?.value.trim()??'';
    target.enabled=row.querySelector('.c-enabled')?.checked??true;
    target.proxies=proxyDraft(row.querySelector('.c-proxy')?.value??'');
    target.proxy=target.proxies[0]||'';
    target.proxy_fallback=row.querySelector('.c-fallback')?.checked??true;
  });
  return COOKIE_ACCOUNTS;
}
async function loadCookieAccounts(){
  try{
    const data=await readJSON('/api/cookie-accounts');
    COOKIE_ACCOUNTS=(data.accounts||[]).map(credentialCopy);
    cookiesLoaded=true;SAVED_COOKIES=COOKIE_ACCOUNTS.map(credentialCopy);renderCookieAccounts();window.consoleForms?.saved('cookies');
  }catch(error){loadNotice('channel','Cookie 账号'+error.message+'。请重新加载配置。');}
}
function addCookieAccount(){
  if(!cookiesLoaded){toast('Cookie 账号尚未加载，请先重新加载配置');return;}
  collectCookieAccounts();
  // The server assigns an account ID on save, including on ordinary HTTP sites
  // where crypto.randomUUID() is unavailable.
  COOKIE_ACCOUNTS.push({cookie:'',project_id:'',note:'',enabled:true,proxies:[],proxy_fallback:true});
  renderCookieAccounts();window.consoleForms?.openAccount('cookie',COOKIE_ACCOUNTS.length-1);$('cookie-'+(COOKIE_ACCOUNTS.length-1)).focus();
}
function removeCookieAccount(index){collectCookieAccounts();COOKIE_ACCOUNTS.splice(index,1);renderCookieAccounts();}
async function saveCookieAccounts(){
  if(!cookiesLoaded){toast('Cookie 账号尚未加载，请先重新加载配置');return;}
  let accounts;
  try{accounts=collectCookieAccounts();}
  catch(error){toast('❌ 收集账号信息失败：'+error.message);return;}
  try{
    const response=await fetch('/api/cookie-accounts',{method:'POST',signal:AbortSignal.timeout(30000),headers:{'Content-Type':'application/json'},body:JSON.stringify({accounts})});
    const result=await response.json();
    if(!response.ok) throw new Error(await responseError(response,'保存失败',result));
    if(!Array.isArray(result.accounts)) throw new Error('保存响应缺少账号列表，请刷新面板后重试');
    COOKIE_ACCOUNTS=result.accounts.map(credentialCopy);SAVED_COOKIES=COOKIE_ACCOUNTS.map(credentialCopy);renderCookieAccounts();window.consoleForms?.saved('cookies');
    toast('已保存 '+COOKIE_ACCOUNTS.length+' 个 Cookie 账号（已热生效）');
  }catch(error){toast('❌ '+error.message);}
}

async function refreshCookies(index=null){
  if(!cookiesLoaded){toast('Cookie 账号尚未加载，请先重新加载配置');return;}
  if(cookieRefreshPending){toast('Cookie 正在刷新，请等待当前任务完成');return;}
  if(window.consoleForms?.capture()?.cookies){toast('请先保存或撤销 Cookie 列表的修改，再刷新已保存凭证');return;}
  const accounts=SAVED_COOKIES.filter(rec=>rec.enabled!==false && rec.id && (index===null || rec.id===COOKIE_ACCOUNTS[index]?.id));
  if(!accounts.length){toast('没有匹配的已保存且启用的 Cookie 账号');return;}
  const show=message=>{
    for(const id of ['cookie-refresh-result','cookie-accounts-refresh-result']){
      const box=$(id);if(box){box.textContent=message;box.hidden=false;}
    }
  };
  cookieRefreshPending=true;
  show('正在刷新 '+accounts.length+' 个已保存账号，请稍候…');
  try{
    // Each account has a 45-second server deadline plus a one-second gap.
    const response=await fetch('/api/cookie-accounts/refresh',{method:'POST',
      signal:AbortSignal.timeout(15000+accounts.length*46000),headers:{'Content-Type':'application/json'},
      body:JSON.stringify(index===null?{}:{id:accounts[0].id})});
    const result=await response.json();
    if(!response.ok)throw new Error(await responseError(response,'刷新失败',result));
    const labels={refreshed:'已更新',unchanged:'无新令牌',error:'失败'};
    const lines=(result.results||[]).map(rec=>`${rec.label||rec.id}：${labels[rec.status]||rec.status} · ${rec.detail||''}`);
    show(lines.join('\n')||'没有可刷新的账号');
    toast(`刷新完成：更新 ${result.refreshed||0}，无新令牌 ${result.unchanged||0}，失败 ${result.errors||0}`);
    // Leave credential editors untouched: refreshing must never discard drafts
    // created in the other section while this request was in flight.
  }catch(error){
    const message=error.name==='TimeoutError'?'请求超时，请在运行日志确认刷新结果':error.message;
    show('刷新未完成：'+message);toast('❌ '+message);
  }finally{cookieRefreshPending=false;}
}

/* ---------- Cookie 会话测试（零配额探测） ----------
 * 后端 POST /api/cookie-accounts/test 用不存在的模型名调一次 batchGraphql，
 * 不消耗生成配额，按 gRPC 状态码判定会话有效性（valid/invalid/error）。
 * 与「立即刷新」（RotateCookies 短期令牌续期）互补：刷新不校验会话，测试不续期。 */
async function testCookieAccount(index=null){
  if(!cookiesLoaded){toast('Cookie 账号尚未加载，请先重新加载配置');return;}
  if(cookieTestPending){toast('Cookie 正在测试，请等待当前任务完成');return;}
  if(window.consoleForms?.capture()?.cookies){toast('请先保存或撤销 Cookie 列表的修改，再测试已保存凭证');return;}
  const accounts=SAVED_COOKIES.filter(rec=>rec.enabled!==false && rec.id && (index===null || rec.id===COOKIE_ACCOUNTS[index]?.id));
  if(!accounts.length){toast(index===null?'没有已保存且启用的 Cookie 账号可测试':'请先保存账号，再测试会话');return;}
  cookieTestPending=true;
  toast(index===null?('正在测试 '+accounts.length+' 个已保存账号…'):'正在测试会话…');
  try{
    // 后端 4 路并发探测、单账号 45 秒总时限：批量预算按批次数而非账号数增长。
    const batches=Math.max(1,Math.ceil(accounts.length/4));
    const response=await fetch('/api/cookie-accounts/test',{method:'POST',
      signal:AbortSignal.timeout(15000+batches*46000),headers:{'Content-Type':'application/json'},
      body:JSON.stringify(index===null?{}:{id:accounts[0].id})});
    const result=await response.json();
    if(!response.ok)throw new Error(await responseError(response,'测试失败',result));
    mergeCookieTestResults(result.results||[]);
    toast(`测试完成：有效 ${result.valid||0} / ${result.total||0}`);
  }catch(error){
    const message=error.name==='TimeoutError'?'请求超时，请在运行日志确认测试结果':error.message;
    toast('❌ '+message);
  }finally{cookieTestPending=false;}
}
function mergeCookieTestResults(results){
  // 只更新诊断信息与健康 pill；保留未保存的草稿与行内编辑（与 Express mergeProbeResults 同口径）。
  (Array.isArray(results)?results:[]).forEach(res=>{
    const hit=COOKIE_ACCOUNTS.find(rec=>rec.id===res.id);
    if(!hit) return;
    hit._msg=cookieTestMessage(res);
    hit._msg_ok=COOKIE_TEST_STATUS[res.status]?.ok===true;
    // 与后端健康表口径同步：成功清零；失败累计（冷却秒数以下次加载的快照为准）。
    if(res.status==='valid') hit.health={cooldown_remaining:0,failures:0};
    else if(res.status!=='error') hit.health={cooldown_remaining:hit.health?.cooldown_remaining||0,failures:(hit.health?.failures||0)+1};
  });
  renderCookieAccounts();
}

/* Proxy editor stores a draft until Apply; account list/global Save persists it. */
// 可拆字段填写的原生代理协议；其余类型（sing-box 节点链接 / 直连）只走粘贴框。
const BRIDGE_PROXY_TYPES=['turn','turns','sstp'];
const STRUCTURED_PROXY_TYPES=['socks5','socks5h','http','https','socks4','socks4a',...BRIDGE_PROXY_TYPES];
function openProxyEditor(scope='global',index=0,view='list'){
  if((scope==='key'&&!keysLoaded)||(scope==='cookie'&&!cookiesLoaded)||(scope==='global'&&!settingsLoaded)){
    toast('配置尚未加载，请先重新加载');return;
  }
  if(scope==='key') collectKeys();
  if(scope==='cookie') collectCookieAccounts();
  const source=scope==='global'?proxyDraft($('proxy-input').value):(scope==='key'?KEYS[index]:COOKIE_ACCOUNTS[index]).proxies;
  if(proxyEditor) return;
  proxyEditor={scope,index,proxies:[...source],initial:[...source],editing:null,opener:document.activeElement,view,batch:null,job:null};
  $('proxy-dialog-title').textContent=scope==='global'?'全局代理列表':(scope==='key'?'Key ':'Cookie ')+(index+1)+' · 专属代理';
  $('proxy-dialog-hint').textContent='从第一条开始，连接失败时按顺序切换。'+(scope==='global'?'保存后立即生效。':'应用到账号草稿后，请点击「'+(scope==='key'?'保存 Key 列表':'保存 Cookie 列表')+'」生效。');
  $('proxy-test-target').value=scope==='cookie'?'cookie':'vertex';
  $('proxy-import-text').value='';$('proxy-import-type').value='socks5';$('proxy-import-tested').checked=true;
  $('proxy-import-source').open=true;
  $('proxy-close-prompt').classList.add('hidden');
  $('proxy-dialog-save').classList.toggle('hidden',scope!=='global');
  $('proxy-dialog-apply').textContent=scope==='global'?'应用到页面草稿':'应用到账号草稿';
  $('proxy-dialog-apply').classList.toggle('btn',scope!=='global');
  $('proxy-dialog-apply').classList.toggle('btn-ghost',scope==='global');
  resetProxyForm();renderProxyEditor();renderProxyImport();switchProxyView(view);$('proxy-dialog').showModal();
}
function proxyFormDirty(){return ['proxy-paste','proxy-host','proxy-port','proxy-username','proxy-password','proxy-name','proxy-sni','proxy-ca','proxy-realm','proxy-mtu'].some(id=>$(id).value.trim())||$('proxy-type').value==='direct'||$('proxy-insecure').checked;}
function closeProxyEditor(discard=false){
  if(!proxyEditor) return;
  if(proxyEditor.saving){toast('正在保存，请稍候');return;}
  if(!discard&&(JSON.stringify(proxyEditor.initial)!==JSON.stringify(proxyEditor.proxies)||proxyFormDirty()||$('proxy-import-text').value.trim())){
    $('proxy-close-prompt').classList.remove('hidden');return;
  }
  stopProxyTests();
  $('proxy-dialog').close();
  const opener=proxyEditor?.opener;proxyEditor=null;
  if(opener?.isConnected) opener.focus();
}
function switchProxyView(view){
  if(!proxyEditor||!['list','form','import'].includes(view)) return;
  proxyEditor.view=view;
  for(const name of ['list','form','import']){
    const active=view===name;
    $('proxy-panel-'+name).classList.toggle('hidden',!active);
    $('proxy-tab-'+name).setAttribute('aria-selected',String(active));
    $('proxy-tab-'+name).tabIndex=active?0:-1;
  }
  $('proxy-dialog').querySelector('.proxy-dialog-body').scrollTop=0;
  updateProxyEditorControls();
}
function proxyLabel(uri){
  if(['direct','none','off','直连'].includes(uri.toLowerCase())) return '强制直连';
  try{
    const url=new URL(uri.replace(/^(turns?):(?!\/\/)/i,'$1://'));
    const proto=url.protocol.replace(/:$/,'');
    let name='';
    try{name=decodeURIComponent(url.hash.replace(/^#/,'')).trim();}catch(_){name=url.hash.replace(/^#/,'').trim();}
    if(proto==='vmess'){
      // vmess:// 整体是 base64(JSON)：解出地址与备注才能区分多条同类节点。
      try{
        const b64=uri.slice('vmess://'.length).replace(/-/g,'+').replace(/_/g,'/');
        const bytes=Uint8Array.from(atob(b64),c=>c.charCodeAt(0));
        const payload=JSON.parse(new TextDecoder().decode(bytes));
        const host=String(payload.add||'')+(payload.port?':'+payload.port:'');
        return 'vmess://'+(host||'（分享链接）')+(payload.ps?' · '+payload.ps:'');
      }catch(_){return 'vmess://（分享链接）';}
    }
    if(proto==='ss'&&!url.username&&!url.password) return 'ss://（分享链接）';
    if(STRUCTURED_PROXY_TYPES.includes(proto)){
      const port=url.port||({turn:'3478',turns:'5349',sstp:'443',http:'80',https:'443'})[proto];
      const endpoint=url.hostname+(port?':'+port:'');
      return proto+'://'+((url.username||url.password||url.searchParams.has('username')||url.searchParams.has('password'))?'***@':'')+endpoint+(name?' · '+name:'');
    }
    // 其余分享链接（vless/trojan/hy2/tuic 等）：显示端点与节点备注。
    return proto+'://'+url.host+(name?' · '+name:'');
  }catch(_){return '待识别的代理地址';}
}
function proxyMissingFields(uri){
  try{
    const url=new URL(uri.replace(/^(turns?):(?!\/\/)/i,'$1://'));
    const proto=url.protocol.slice(0,-1);
    if(!BRIDGE_PROXY_TYPES.includes(proto)) return [];
    // 与后端 parse_bridge_proxy_uri 同口径：TURN/TURNS 无凭据是合法形态
    //（匿名分配服务器，RFC 7065 允许无 userinfo 的 URI）；仅 SSTP 的
    // MS-CHAPv2 认证协议强制，缺了才提醒补齐。
    if(proto!=='sstp') return [];
    return [['username','账号'],['password','密码']].filter(([key])=>!(url[key]||url.searchParams.get(key))).map(([,label])=>label);
  }catch(_){return [];}
}
function renderProxyEditor(){
  $('proxy-editor-list').innerHTML=proxyEditor.proxies.map((uri,index)=>`
    <div class="proxy-row${proxyEditor.editing===index?' is-editing':''}" data-proxy-index="${index}"><div class="proxy-row-title"><span class="proxy-order">${index+1}</span><div><strong>${esc(proxyLabel(uri))}</strong><small>${proxyMissingFields(uri).length?'待补齐'+proxyMissingFields(uri).join(' / ')+' · ':''}${proxyEditor.editing===index?'正在编辑 · ':''}${index===0?'首选出口':'第 '+(index+1)+' 顺位'}</small></div></div>
      <div class="proxy-row-actions">
        ${index>0?`<button type="button" onclick="moveProxy(${index},0)" aria-label="将第 ${index+1} 条设为首选">设为首选</button><button type="button" onclick="moveProxy(${index},${index-1})" aria-label="上移第 ${index+1} 条">↑</button>`:''}
        ${index<proxyEditor.proxies.length-1?`<button type="button" onclick="moveProxy(${index},${index+1})" aria-label="下移第 ${index+1} 条">↓</button>`:''}
        <button type="button" data-proxy-test ${proxyMissingFields(uri).length?'disabled title="请先编辑并补齐账号密码"':''} onclick="testEditorProxy(${index})">测试</button>
        <button type="button" onclick="runAction(this,()=>editProxy(${index}))">编辑</button>
        <button type="button" class="danger" onclick="deleteProxy(${index})" aria-label="删除第 ${index+1} 条代理">删除</button></div>
      <div class="proxy-row-result" id="proxy-result-${index}" role="status"></div></div>`).join('')||'<p class="empty-state">尚无代理。填写表单添加，或粘贴多条地址批量导入。<br>清空专属列表后跟随全局；清空全局列表后直连。</p>';
  $('proxy-editor-count').textContent=proxyEditor.proxies.length+' / 32 条';
  $('proxy-form-add').textContent=proxyEditor.editing===null?'加入列表':'更新第 '+(proxyEditor.editing+1)+' 条';
  $('proxy-tab-form').textContent=proxyEditor.editing===null?'单条添加':'编辑代理';
  updateProxyResults();updateProxyEditorControls();
}
function moveProxy(from,to){
  const editing=proxyEditor.editing;
  const [uri]=proxyEditor.proxies.splice(from,1);proxyEditor.proxies.splice(to,0,uri);
  if(editing===from) proxyEditor.editing=to;
  else if(editing!==null && from<editing && to>=editing) proxyEditor.editing--;
  else if(editing!==null && from>editing && to<=editing) proxyEditor.editing++;
  renderProxyEditor();
}
function deleteProxy(index){
  proxyEditor.proxies.splice(index,1);
  if(proxyEditor.editing===index) resetProxyForm();
  else if(proxyEditor.editing!==null && proxyEditor.editing>index) proxyEditor.editing--;
  renderProxyEditor();
}
function resetProxyForm({keepType=false}={}){
  const type=keepType?$('proxy-type').value:'socks5';
  if(proxyEditor){proxyEditor.editing=null;proxyEditor.recognizedSource='';}
  ['proxy-paste','proxy-host','proxy-port','proxy-username','proxy-password','proxy-name','proxy-sni','proxy-ca','proxy-realm','proxy-mtu'].forEach(id=>$(id).value='');
  $('proxy-insecure').checked=false;
  $('proxy-advanced').open=false;
  $('proxy-type').value=type;$('proxy-form-add').textContent='加入列表';
  $('proxy-editor-error').textContent='';$('proxy-paste-hint').textContent='支持 用户:密码@主机:端口、主机:端口、标准链接及 JSON；无协议的简写按所选类型识别。';
  proxyTypeChanged();
}
function proxyTypeChanged(){
  const type=$('proxy-type').value;
  const structured=STRUCTURED_PROXY_TYPES.includes(type);
  $('proxy-structured-fields').classList.toggle('hidden',!structured);
  const bridge=BRIDGE_PROXY_TYPES.includes(type), tls=type==='turns'||type==='sstp';
  $('proxy-advanced').classList.toggle('hidden',!bridge);
  $('proxy-tls-fields').classList.toggle('hidden',!tls);
  $('proxy-turn-fields').classList.toggle('hidden',type!=='turn'&&type!=='turns');
  $('proxy-sstp-fields').classList.toggle('hidden',type!=='sstp');
  $('proxy-port').placeholder=({turn:'3478',turns:'5349',sstp:'443',http:'80',https:'443'})[type]||'1080';
  // 桥接类型里只有 SSTP 协议强制凭据；TURN/TURNS 匿名节点允许留空。
  const credentialsRequired=type==='sstp';
  document.querySelector('label[for="proxy-username"]').textContent=credentialsRequired?'账号（必填）':'账号（可选）';
  document.querySelector('label[for="proxy-password"]').textContent=credentialsRequired?'密码（必填）':'密码（可选）';
  $('proxy-protocol-hint').textContent=type==='sstp'?'SSTP：MS-CHAPv2、IPv4 出口、本机 DNS；默认校验证书。可粘贴 用户:密码@主机:端口 或 主机:端口，再补齐凭据。':(type==='turn'||type==='turns'?'TURN：需要 RFC 6062 TCP 中继，使用本机 DNS。可粘贴 用户:密码@主机:端口 或 主机:端口；开放服务器可留空凭据（匿名分配），无前缀时沿用所选类型。':'');
  // 选中 sing-box 节点时，粘贴框即节点链接输入框。
  const nodeLink=type==='singbox';
  $('proxy-paste-label').textContent=nodeLink?'节点链接':'粘贴代理 · 自动识别';
  $('proxy-paste').placeholder=nodeLink?'vless:// vmess:// trojan:// ss:// hysteria2:// tuic:// …（每行一条，可批量）':'粘贴一条自动填入字段，粘贴多条可批量导入';
}
async function parseProxyInput(payload){
  const response=await fetch('/api/proxy/parse',{method:'POST',signal:AbortSignal.timeout(15000),headers:{'Content-Type':'application/json'},body:JSON.stringify(payload)});
  const result=await response.json();
  if(!response.ok) throw new Error(await responseError(response,'无法识别代理',result));
  return result.proxies;
}
function fillProxyFields(proxy){
  let type=proxy.url==='direct'?'direct':proxy.type;
  // 分享链接系（vless/vmess/trojan/ss/hysteria2/tuic 及未知协议）统一归入 sing-box 节点。
  if(type!=='direct'&&!STRUCTURED_PROXY_TYPES.includes(type)) type='singbox';
  $('proxy-type').value=type;
  for(const field of ['host','port','username','password','name']) $('proxy-'+field).value=proxy[field]??'';
  const options=proxy.options||{};
  for(const field of ['sni','ca','realm','mtu']) $('proxy-'+field).value=options[field]??'';
  $('proxy-insecure').checked=['true','1'].includes(String(options.insecure||'').toLowerCase());
  $('proxy-advanced').open=Object.keys(options).some(key=>key!=='transport');
  if(!STRUCTURED_PROXY_TYPES.includes(type)) $('proxy-paste').value=proxy.url;
  proxyTypeChanged();
}
async function recognizeProxyPaste(){
  const source=$('proxy-paste').value.trim();
  if(!source) return;
  const editor=proxyEditor;
  try{
    // Preview first: a malformed line must not prevent its neighbours from
    // reaching the batch review. It also accepts more than 32 candidates.
    const preview=await fetchProxyPreview(source,$('proxy-type').value,[]);
    if(proxyEditor!==editor || $('proxy-paste').value.trim()!==source) return;
    if(preview.items.length>1){
      $('proxy-import-text').value=source;
      $('proxy-import-type').value=STRUCTURED_PROXY_TYPES.includes($('proxy-type').value)?$('proxy-type').value:'socks5';
      $('proxy-import-source').open=true;
      resetProxyForm({keepType:true});editor.batch=null;renderProxyImport();switchProxyView('import');
      toast('已转入批量导入，请识别并测试后选择要导入的代理');return;
    }
    const proxies=await parseProxyInput({text:source,type:$('proxy-type').value,draft:true});
    if(proxyEditor!==editor || $('proxy-paste').value.trim()!==source) return;
    $('proxy-editor-error').textContent='';
    if(proxies.length===1){
      fillProxyFields(proxies[0]);
      editor.recognizedSource=$('proxy-paste').value.trim();
      const t=proxies[0].type;
      const identified=proxies[0].url==='direct'?'强制直连':(t==='share'?'分享链接':t.toUpperCase());
      const missing=(proxies[0].missing_fields||[]).map(field=>field==='username'?'账号':'密码');
      $('proxy-paste-hint').textContent='已识别 '+identified+'，'+(missing.length?'请补齐'+missing.join(' / ')+'后加入列表。':'检查后加入列表。');
    }
  }catch(error){if(proxyEditor===editor) $('proxy-editor-error').textContent=error.message;}
}
async function editProxy(index){
  const editor=proxyEditor;
  try{
    const proxies=await parseProxyInput({text:editor.proxies[index],draft:true});
    if(proxyEditor!==editor) return;
    if(proxies.length>1){
      const expanded=[...editor.proxies];expanded.splice(index,1,...proxies.map(proxy=>proxy.url));
      const merged=[...new Set(expanded)];
      if(merged.length>32) throw new Error('每个列表最多 32 条代理');
      editor.proxies=merged;resetProxyForm();renderProxyEditor();
      toast('已展开 '+proxies.length+' 条代理，请选择需要编辑的节点');return;
    }
    resetProxyForm();proxyEditor.editing=index;fillProxyFields(proxies[0]);
    renderProxyEditor();switchProxyView('form');
    const target=$(STRUCTURED_PROXY_TYPES.includes($('proxy-type').value)?'proxy-host':'proxy-paste');
    target.focus({preventScroll:true});target.scrollIntoView({block:'center',behavior:'smooth'});
  }catch(error){if(proxyEditor===editor) $('proxy-editor-error').textContent=error.message;}
}
async function addProxyDraft(testFirst=false){
  const editor=proxyEditor;
  const type=$('proxy-type').value;
  const structured=STRUCTURED_PROXY_TYPES.includes(type);
  try{
    if(type==='singbox'&&!$('proxy-paste').value.trim())
      throw new Error('请先在上方粘贴节点链接（vless://、vmess:// 等）');
    // Directly adding newly pasted text must recognize it too; otherwise the
    // visible input could be ignored in favour of fields from the previous node.
    if((structured||type==='singbox') && $('proxy-paste').value.trim() && $('proxy-paste').value.trim()!==editor.recognizedSource){
      await recognizeProxyPaste();
      if(proxyEditor!==editor || $('proxy-editor-error').textContent) return false;
      if(!$('proxy-paste').value.trim()) return false; // Switched to batch review.
      return addProxyDraft(testFirst);
    }
    const payload=structured?{type,host:$('proxy-host').value.trim(),port:$('proxy-port').value.trim(),username:$('proxy-username').value,password:$('proxy-password').value,name:$('proxy-name').value.trim()}
      :{text:type==='direct'?'direct':$('proxy-paste').value.trim()};
    if(BRIDGE_PROXY_TYPES.includes(type)){
      payload.options={};
      const fields=type==='sstp'?['sni','ca','mtu']:(type==='turns'?['sni','ca','realm']:['realm']);
      for(const field of fields){const value=$('proxy-'+field).value.trim();if(value) payload.options[field]=value;}
      if(type!=='turn'&&$('proxy-insecure').checked) payload.options.insecure=true;
    }
    const proxies=await parseProxyInput(payload);
    if(proxyEditor!==editor) return false;
    if(proxies.length>1){$('proxy-import-text').value=$('proxy-paste').value;resetProxyForm({keepType:true});switchProxyView('import');return false;}
    if(testFirst){
      if(editor.job) throw new Error('请等待当前测试完成或停止测试');
      const [result]=await runProxyTests([{uri:proxies[0].url}],'form');
      if(proxyEditor!==editor) return false;
      if(result.state!=='passed') throw new Error(result.message);
    }
    if(proxyEditor.editing!==null){proxyEditor.proxies.splice(proxyEditor.editing,1,proxies[0].url);}
    else{
      const merged=[...new Set([...proxyEditor.proxies,...proxies.map(p=>p.url)])];
      if(merged.length>32) throw new Error('每个列表最多 32 条代理');
      proxyEditor.proxies=merged;
    }
    resetProxyForm({keepType:true});renderProxyEditor();switchProxyView('list');return true;
  }catch(error){if(proxyEditor===editor) $('proxy-editor-error').textContent=error.message;return false;}
}
async function applyProxyEditor(persist=false){
  const editor=proxyEditor;
  if(!editor||editor.saving) return false;
  if(editor.job){$('proxy-editor-error').textContent='请等待测试完成或停止测试后再保存';return false;}
  const structured=STRUCTURED_PROXY_TYPES.includes($('proxy-type').value);
  if((structured&&['host','port','username','password','name'].some(field=>$('proxy-'+field).value.trim()))||$('proxy-paste').value.trim()||$('proxy-type').value==='direct'){
    if(!await addProxyDraft()) return false;
  }
  try{
    editor.saving=true;updateProxyEditorControls();
    const pending=editor.proxies.findIndex(uri=>proxyMissingFields(uri).length);
    if(pending!==-1) throw new Error('第 '+(pending+1)+' 条代理待补齐'+proxyMissingFields(editor.proxies[pending]).join(' / ')+'，请编辑后再应用。');
    const normalized=editor.proxies.length?(await parseProxyInput({text:JSON.stringify(editor.proxies)})).map(p=>p.url):[];
    if(proxyEditor!==editor) return;
    if(editor.scope==='global'){
      if(persist){
        const response=await fetch('/api/settings',{method:'POST',signal:AbortSignal.timeout(30000),headers:{'Content-Type':'application/json'},body:JSON.stringify({proxy_urls:normalized})});
        const saved=await response.json();
        if(!response.ok) throw new Error(await responseError(response,'保存失败',saved));
        if(proxyEditor!==editor) return false;
        GLOBAL_SETTINGS.proxy_urls=saved.proxy_urls;GLOBAL_SETTINGS.proxy_url=saved.proxy_url;
        updateProxyPill(saved.proxy_urls.length);
      }
      $('proxy-input').value=normalized.join('\n');updateProxyDraft();if(persist)window.consoleForms?.saved('proxy');else window.consoleForms?.refresh();
    }
    else{
      const record=(editor.scope==='key'?KEYS:COOKIE_ACCOUNTS)[editor.index];
      record.proxies=normalized;record.proxy=normalized[0]||'';
      if(editor.scope==='key') renderKeys();else renderCookieAccounts();
    }
    editor.saving=false;closeProxyEditor(true);toast(persist&&editor.scope==='global'?'代理列表已保存并生效':'代理列表已应用，请保存'+(editor.scope==='global'?'代理':editor.scope==='key'?' Key 列表':' Cookie 列表')+'后生效');return true;
  }catch(error){if(proxyEditor===editor) $('proxy-editor-error').textContent='保存未完成，草稿已保留：'+error.message;return false;}
  finally{editor.saving=false;if(proxyEditor===editor) updateProxyEditorControls();}
}
function resultKey(uri,target){return target+'\n'+uri;}
function rememberProxyResult(uri,target,result){
  const key=resultKey(uri,target);proxyResults.delete(key);proxyResults.set(key,result);
  while(proxyResults.size>512) proxyResults.delete(proxyResults.keys().next().value);
}
function proxyResultText(result){
  if(!result) return '未测试';
  if(['queued','testing','cancelled'].includes(result.state)) return ({queued:'等待测试',testing:'正在测试…',cancelled:'测试已停止，尚未完成'})[result.state];
  const time=result.testedAt?' · '+new Date(result.testedAt).toLocaleTimeString('zh-CN',{hour12:false}):'';
  const latency=Number.isFinite(result.latency_ms)?' · HTTPS '+result.latency_ms+' ms'+(result.setup_ms>0?'（准备 '+result.setup_ms+' ms）':''):'';
  return (result.state==='passed'?'✓ 连通通过':'✕ 本次未通过')+latency+' · '+result.message+time;
}
function updateProxyResults(){
  if(!proxyEditor) return;
  const target=$('proxy-test-target').value;
  proxyEditor.proxies.forEach((uri,index)=>{
    const box=$('proxy-result-'+index);if(!box) return;
    const result=proxyResults.get(resultKey(uri,target));
    box.textContent=proxyResultText(result);box.dataset.state=result?.state||'untested';
  });
}
function updateProxyEditorControls(){
  if(!proxyEditor) return;
  const job=proxyEditor.job;
  const saving=!!proxyEditor.saving;
  const importing=proxyEditor.view==='import';
  $('proxy-stop-tests').classList.toggle('hidden',!job);
  $('proxy-job-state').textContent=job?'正在测试 '+job.completed+' / '+job.entries.length+' · 最多 3 条同时探测':(proxyEditor.lastJobMessage||'连通性测试不代表账号或模型调用成功');
  $('proxy-test-target').disabled=!!job||saving;
  for(const id of ['proxy-test-all','proxy-form-test-add','proxy-dialog-save','proxy-dialog-apply','proxy-import-preview','proxy-import-tested','proxy-import-type']) $(id).disabled=!!job||saving;
  for(const id of ['proxy-dialog-close','proxy-dialog-cancel']) $(id).disabled=saving;
  $('proxy-dialog-save').classList.toggle('hidden',proxyEditor.scope!=='global'||importing);
  $('proxy-dialog-apply').classList.toggle('hidden',importing);
  $('proxy-import-text').readOnly=!!job;
  document.querySelectorAll('[data-proxy-test]').forEach((button,index)=>button.disabled=!!job||proxyMissingFields(proxyEditor.proxies[index]).length>0);
  $('proxy-save-hint').textContent=importing?'按原顺序追加，仅导入勾选项':proxyEditor.scope==='global'?'保存后生效；也可先应用到页面草稿':'专属代理修改后，还需保存对应账号列表';
  updateProxyImportSelection();
}
function proxyTargetChanged(){
  if(!proxyEditor||proxyEditor.job) return;
  proxyEditor.lastJobMessage='';
  // A pass for one Google service must never select a different target's row.
  if(proxyEditor.batch) for(const row of proxyEditor.batch.items){
    if(!row.url||['invalid','duplicate','skipped'].includes(row.status)) continue;
    row.result=proxyResults.get(resultKey(row.url,$('proxy-test-target').value));
    row.state=row.result?.state||'ready';row.selected=canImportProxy(row);
  }
  updateProxyResults();renderProxyImport();updateProxyEditorControls();
}
async function probeOneProxy(uri,target,signal){
  try{
    const timeout=AbortSignal.timeout(65000);
    const response=await fetch('/api/proxy/test',{method:'POST',signal:signal?AbortSignal.any([signal,timeout]):timeout,headers:{'Content-Type':'application/json'},body:JSON.stringify({proxy_url:uri,target})});
    const result=await response.json();
    if(response.status===401) return {state:'failed',message:'登录已过期，请重新登录',fatal:true,testedAt:Date.now()};
    if(!response.ok) return {state:'failed',message:result.message||await responseError(response,'测试失败',result),testedAt:Date.now()};
    return {...result,state:result.ok?'passed':'failed',testedAt:Date.now()};
  }catch(error){
    return {state:signal?.aborted?'cancelled':'failed',message:signal?.aborted?'已停止测试':error.name==='TimeoutError'?'探测超时，可稍后重试':'请求失败，请检查面板连接后重试',testedAt:Date.now()};
  }
}
async function runProxyTests(entries,origin='list'){
  const editor=proxyEditor;if(!editor||editor.job||!entries.length) return [];
  const job={controller:new AbortController(),entries,origin,completed:0,target:$('proxy-test-target').value};
  editor.job=job;editor.lastJobMessage='';
  for(const entry of entries){
    rememberProxyResult(entry.uri,job.target,{state:'queued'});
    if(entry.row){entry.row.state='queued';entry.row.selected=false;entry.row.result=null;}
  }
  updateProxyResults();renderProxyImport();updateProxyEditorControls();
  let cursor=0;
  async function worker(){
    while(!job.controller.signal.aborted&&cursor<entries.length){
      const entry=entries[cursor++];
      rememberProxyResult(entry.uri,job.target,{state:'testing'});
      if(entry.row) entry.row.state='testing';
      if(proxyEditor===editor){updateProxyResults();renderProxyImport();}
      let result=await probeOneProxy(entry.uri,job.target,job.controller.signal);
      // A closed dialog may already have a new job probing the same URI.
      // Neither its cache nor its selected import rows belong to this job.
      if(proxyEditor!==editor||editor.job!==job)return;
      if(job.controller.signal.aborted)result={state:'cancelled',message:'已停止测试'};
      entry.result=result;rememberProxyResult(entry.uri,job.target,result);job.completed++;
      if(entry.row){entry.row.state=result.state;entry.row.result=result;entry.row.selected=result.state==='passed';}
      if(result.fatal){job.controller.abort();if(proxyEditor===editor) $('proxy-editor-error').textContent=result.message;}
      if(proxyEditor===editor){updateProxyResults();renderProxyImport();updateProxyEditorControls();}
    }
  }
  try{await Promise.all(Array.from({length:Math.min(3,entries.length)},()=>worker()));}
  finally{
    for(const entry of entries) if(!entry.result){
      entry.result={state:'cancelled',message:'已停止测试'};
      if(proxyEditor===editor&&editor.job===job)rememberProxyResult(entry.uri,job.target,entry.result);
      if(entry.row){entry.row.state='cancelled';entry.row.result=entry.result;entry.row.selected=false;}
    }
    if(proxyEditor===editor&&editor.job===job){
      editor.job=null;editor.lastJobMessage=(job.controller.signal.aborted?'测试已停止':'测试完成')+' · 通过 '+entries.filter(entry=>entry.result.state==='passed').length+' / '+entries.length+' · 结果仅代表目标站连通性';
      updateProxyResults();renderProxyImport();updateProxyEditorControls();
    }
  }
  return entries.map(entry=>entry.result);
}
function stopProxyTests(){proxyEditor?.job?.controller.abort();}
function testEditorProxy(index){
  const uri=proxyEditor?.proxies[index];
  if(uri&&!proxyMissingFields(uri).length) return runProxyTests([{uri}]);
}
function testAllEditorProxies(){
  if(!proxyEditor) return;
  return runProxyTests([...new Set(proxyEditor.proxies)].filter(uri=>!proxyMissingFields(uri).length).map(uri=>({uri})));
}
async function fetchProxyPreview(text,type,existing){
  const response=await fetch('/api/proxy/import-preview',{method:'POST',signal:AbortSignal.timeout(15000),headers:{'Content-Type':'application/json'},body:JSON.stringify({text,type,existing})});
  const result=await response.json();
  if(!response.ok) throw new Error(await responseError(response,'无法预览导入内容',result));
  return result;
}
async function previewProxyImport(){
  const editor=proxyEditor;if(!editor||editor.job) return;
  const text=$('proxy-import-text').value,type=$('proxy-import-type').value;
  try{
    const batch=await fetchProxyPreview(text,type,editor.proxies);
    if(proxyEditor!==editor||$('proxy-import-text').value!==text||$('proxy-import-type').value!==type) return;
    batch.items.forEach(row=>{row.state=row.status;row.selected=row.status==='ready'&&!$('proxy-import-tested').checked;});
    editor.batch=batch;$('proxy-editor-error').textContent='';renderProxyImport();
    $('proxy-import-source').open=false;
    $('proxy-dialog').querySelector('.proxy-dialog-body').scrollTop=0;
    if($('proxy-import-tested').checked) testProxyImport();
  }catch(error){if(proxyEditor===editor) $('proxy-editor-error').textContent=error.message;}
}
function canImportProxy(row){return row.status==='ready'&&(!$('proxy-import-tested').checked||row.state==='passed');}
function renderProxyImport(){
  if(!proxyEditor) return;
  const batch=proxyEditor.batch;
  $('proxy-import-preview-panel').classList.toggle('hidden',!batch);
  $('proxy-import-preview').textContent=$('proxy-import-tested').checked?'识别并测试':'识别并预览';
  $('proxy-import-source').querySelector('summary').textContent=batch?'已识别 '+batch.items.length+' 条 · 修改输入':'粘贴与识别';
  if(!batch){$('proxy-import-list').innerHTML='';$('proxy-import-source').open=true;return;}
  const items=batch.items;
  const count=state=>items.filter(row=>row.state===state).length;
  const summary=['识别 '+items.length+' 条','通过 '+count('passed')];
  for(const [state,label] of [['failed','失败'],['invalid','格式错误'],['duplicate','重复'],['skipped','跳过']]) if(count(state)) summary.push(label+' '+count(state));
  const unfinished=items.filter(row=>['ready','queued','testing','cancelled'].includes(row.state)).length;
  if(unfinished) summary.push('未完成 '+unfinished);
  $('proxy-import-summary').textContent=summary.join(' · ');
  $('proxy-import-list').innerHTML=items.map(row=>`<div class="proxy-import-row" data-import-id="${esc(row.id)}">
    <input type="checkbox" aria-label="选择${esc(row.source)}" ${row.selected?'checked':''} ${!canImportProxy(row)||proxyEditor.job?'disabled':''} onchange="toggleProxyImport('${esc(row.id)}',this.checked)">
    <div><strong>${esc(row.name||row.label)}</strong><small>${esc(row.source)}${row.name?' · '+esc(row.label):''}</small></div>
    <div class="proxy-row-result" data-state="${esc(row.state)}">${esc(row.result?proxyResultText(row.result):({ready:'未测试',queued:'等待测试',testing:'正在测试…',cancelled:'测试已停止'})[row.state]||row.message)}</div></div>`).join('');
  $('proxy-import-retry').disabled=!!proxyEditor.job||!items.some(row=>row.status==='ready'&&['failed','cancelled','ready'].includes(row.state));
  $('proxy-import-select').disabled=!!proxyEditor.job;
  updateProxyImportSelection();
}
function updateProxyImportSelection(){
  if(!proxyEditor) return;
  const selected=proxyEditor.batch?.items.filter(row=>row.selected&&canImportProxy(row)).length||0;
  const capacity=Math.max(0,32-proxyEditor.proxies.length);
  $('proxy-import-select').textContent='按剩余容量选择';
  $('proxy-import-select').disabled=!!proxyEditor.job||capacity===0;
  $('proxy-import-selection').textContent='已选 '+selected+' 条 · 剩余容量 '+capacity+' 条'+(selected>capacity?'，请减少选择':'');
  for(const id of ['proxy-import-add','proxy-import-commit']) $(id).disabled=!!proxyEditor.job||!selected||selected>capacity;
  $('proxy-import-commit').textContent='导入 '+selected+' 条'+(proxyEditor.scope==='global'?'并保存':'到账号草稿');
}
function toggleProxyImport(id,checked){
  if(!proxyEditor?.batch||proxyEditor.job) return;
  const row=proxyEditor.batch.items.find(row=>row.id===id);
  if(row) row.selected=checked&&canImportProxy(row);
  updateProxyImportSelection();
}
function selectProxyImport(selected=true){
  if(!proxyEditor?.batch||proxyEditor.job) return;
  let available=Math.max(0,32-proxyEditor.proxies.length);
  for(const row of proxyEditor.batch.items) row.selected=selected&&canImportProxy(row)&&available-->0;
  renderProxyImport();
}
function proxyImportPolicyChanged(){
  if(!proxyEditor||proxyEditor.job) return;
  if(proxyEditor.batch) for(const row of proxyEditor.batch.items) row.selected=canImportProxy(row);
  renderProxyImport();
}
function testProxyImport(onlyFailed=false){
  if(!proxyEditor?.batch||proxyEditor.job) return;
  const items=proxyEditor.batch.items.filter(row=>row.status==='ready'&&(!onlyFailed||['ready','failed','cancelled'].includes(row.state)));
  return runProxyTests(items.map(row=>({uri:row.url,row})),'import');
}
async function importSelectedProxies(commit=false){
  const editor=proxyEditor;if(!editor?.batch||editor.job) return;
  const selected=editor.batch.items.filter(row=>row.selected&&canImportProxy(row));
  if(!selected.length) return;
  if(commit&&proxyFormDirty()){$('proxy-editor-error').textContent='单条表单还有未加入的内容，请先加入列表或清空表单，再导入并保存。';return;}
  try{
    // Recheck against the current list: it can change after the preview.
    const preview=await fetchProxyPreview(JSON.stringify(selected.map(row=>row.url)),$('proxy-import-type').value,editor.proxies);
    if(proxyEditor!==editor) return;
    if(preview.items.some(row=>row.status==='invalid')) throw new Error('选中的代理存在格式错误，请重新识别');
    const additions=preview.items.filter(row=>row.status==='ready').map(row=>row.url);
    if(editor.proxies.length+additions.length>32) throw new Error('导入后超过 32 条，请减少选择');
    const previous=[...editor.proxies];
    editor.proxies.push(...additions);renderProxyEditor();
    if(commit){
      const applied=await applyProxyEditor(editor.scope==='global');
      if(!applied&&proxyEditor===editor){editor.proxies=previous;renderProxyEditor();renderProxyImport();switchProxyView('import');}
    }else{
      editor.batch=null;$('proxy-import-text').value='';renderProxyImport();switchProxyView('list');
      toast('已加入 '+additions.length+' 条，跳过 '+(selected.length-additions.length)+' 条重复代理');
    }
  }catch(error){if(proxyEditor===editor) $('proxy-editor-error').textContent=error.message;}
}
function initProxyEditor(){
  if(proxyEditorInitialized) return;
  if(!['proxy-dialog-close','proxy-dialog-cancel','proxy-dialog','proxy-paste'].every(id=>$(id))) throw new Error('代理编辑器页面版本不一致，请刷新面板');
  $('proxy-dialog-close').addEventListener('click',()=>closeProxyEditor());
  $('proxy-dialog-cancel').addEventListener('click',()=>closeProxyEditor(true));
  $('proxy-dialog').addEventListener('cancel',event=>{event.preventDefault();closeProxyEditor();});
  $('proxy-paste').addEventListener('paste',()=>setTimeout(()=>{if(proxyEditor) runAction(document.querySelector('.proxy-recognize'),recognizeProxyPaste);},0));
  for(const id of ['proxy-import-text','proxy-import-type']) $(id).addEventListener(id==='proxy-import-text'?'input':'change',()=>{
    if(!proxyEditor||proxyEditor.job) return;proxyEditor.batch=null;renderProxyImport();
  });
  document.querySelector('.proxy-nav').addEventListener('keydown',event=>{
    if(!['ArrowLeft','ArrowRight','Home','End'].includes(event.key)) return;
    const names=['list','form','import'];const current=names.indexOf(proxyEditor.view);
    const next=event.key==='Home'?0:event.key==='End'?2:(current+(event.key==='ArrowRight'?1:2))%3;
    event.preventDefault();switchProxyView(names[next]);$('proxy-tab-'+names[next]).focus();
  });
  proxyEditorInitialized=true;
}

window.consoleChannels={hasDraft:()=>!!proxyEditor&&(JSON.stringify(proxyEditor.initial)!==JSON.stringify(proxyEditor.proxies)||proxyFormDirty()||!!$('proxy-import-text').value.trim()),discard:()=>{COOKIE_ACCOUNTS=SAVED_COOKIES.map(credentialCopy);renderCookieAccounts();},load:loadCookieAccounts,refreshCookies,testCookieAccount,init:initProxyEditor,open:openProxyEditor,sync:updateProxyEditorControls};
Object.assign(window,{updateCredentialStrategy,loadCookieAccounts,addCookieAccount,removeCookieAccount,saveCookieAccounts,refreshCookies,testCookieAccount,
  closeProxyEditor,moveProxy,deleteProxy,resetProxyForm,proxyTypeChanged,recognizeProxyPaste,editProxy,addProxyDraft,
  applyProxyEditor,testEditorProxy,renderProxyEditor,proxyMissingFields,switchProxyView,proxyTargetChanged,
  stopProxyTests,testAllEditorProxies,previewProxyImport,proxyImportPolicyChanged,testProxyImport,
  selectProxyImport,toggleProxyImport,importSelectedProxies});
})();
