/* Cookie accounts and the ordered proxy editor. Shared form helpers live in dashboard.js.
 * Keep state local so a failed/cached script can be retried without redeclaration errors. */
(() => {
if(window.consoleChannels) return;
let COOKIE_ACCOUNTS=[], cookiesLoaded=false, proxyEditor=null, proxyEditorInitialized=false;

async function updateCredentialStrategy(select){
  if(!settingsLoaded){toast('设置尚未加载，请先重新加载配置');return;}
  const previous=GLOBAL_SETTINGS.roundrobin;
  select.disabled=true;
  try{
    const roundrobin=select.value==='roundrobin';
    const response=await fetch('/api/settings',{method:'POST',signal:AbortSignal.timeout(30000),headers:{'Content-Type':'application/json'},body:JSON.stringify({roundrobin})});
    if(!response.ok) throw new Error(await responseError(response));
    GLOBAL_SETTINGS.roundrobin=roundrobin;setV('roundrobin',roundrobin);toast('账号调度已切换为'+(roundrobin?'按顺序轮询':'随机使用'));
  }catch(error){select.value=previous?'roundrobin':'random';toast(error.message);}
  finally{select.disabled=false;}
}

function renderCookieAccounts(){
  $('cookies-list').innerHTML=COOKIE_ACCOUNTS.map((rec,index)=>`
    <div class="key-card cookie-card" data-cookie-index="${index}">
      <div class="key-head"><div class="key-title"><span class="pill pill-accent">Cookie ${index+1}</span>
        <span class="pill">${rec.cookie_configured?'已保存 Cookie':'待填写 Cookie'}</span></div>
        <div class="key-actions"><label><input type="checkbox" class="c-enabled" ${rec.enabled===false?'':'checked'} aria-label="启用 Cookie ${index+1}">启用</label>
          <button type="button" class="danger" onclick="removeCookieAccount(${index})" aria-label="删除 Cookie ${index+1}">删除</button></div></div>
      <div class="key-fields">
        <div><label for="cookie-${index}">Google Cookie（含 HttpOnly 字段）</label>
          <textarea id="cookie-${index}" class="inp log c-cookie" rows="3" spellcheck="false" autocomplete="off" placeholder="${esc(rec.cookie_configured?'已保存，留空保留原 Cookie；粘贴新值可更新':'粘贴完整 Cookie 头或 Cookie-Editor JSON')}">${esc(rec.cookie||'')}</textarea>
          ${rec.cookie_preview?`<div class="key-meta">${esc(rec.cookie_preview)}</div>`:''}</div>
        <div><label for="cookie-project-${index}">Project ID</label><input id="cookie-project-${index}" class="inp log c-project" spellcheck="false" placeholder="此账号对应的项目 ID 或控制台 URL" value="${esc(rec.project_id)}">
          <label class="note-label" for="cookie-note-${index}">备注</label><input id="cookie-note-${index}" class="inp c-note" placeholder="便于区分账号" value="${esc(rec.note)}"></div>
        ${accountProxyFields('cookie',index,rec)}
      </div>
    </div>`).join('');
  $('cookies-count').textContent=COOKIE_ACCOUNTS.length+' 个';
  $('cookies-empty').classList.toggle('hidden',COOKIE_ACCOUNTS.length>0);
}
function collectCookieAccounts(){
  document.querySelectorAll('[data-cookie-index]').forEach(row=>{
    const rec=COOKIE_ACCOUNTS[Number(row.dataset.cookieIndex)];
    rec.cookie=row.querySelector('.c-cookie').value.trim();
    rec.project_id=projectFromInput(row.querySelector('.c-project').value);
    rec.note=row.querySelector('.c-note').value.trim();
    rec.enabled=row.querySelector('.c-enabled').checked;
    rec.proxies=proxyDraft(row.querySelector('.c-proxy').value);
    rec.proxy=rec.proxies[0]||'';
    rec.proxy_fallback=row.querySelector('.c-fallback').checked;
  });
  return COOKIE_ACCOUNTS;
}
async function loadCookieAccounts(){
  try{
    const data=await readJSON('/api/cookie-accounts');
    COOKIE_ACCOUNTS=(data.accounts||[]).map(credentialCopy);
    cookiesLoaded=true;renderCookieAccounts();
  }catch(error){loadNotice('channel','Cookie 账号'+error.message+'。请重新加载配置。');}
}
function addCookieAccount(){
  if(!cookiesLoaded){toast('Cookie 账号尚未加载，请先重新加载配置');return;}
  collectCookieAccounts();
  COOKIE_ACCOUNTS.push({id:crypto.randomUUID(),cookie:'',project_id:'',note:'',enabled:true,proxies:[],proxy_fallback:true});
  renderCookieAccounts();$('cookie-'+(COOKIE_ACCOUNTS.length-1)).focus();
}
function removeCookieAccount(index){collectCookieAccounts();COOKIE_ACCOUNTS.splice(index,1);renderCookieAccounts();}
async function saveCookieAccounts(){
  if(!cookiesLoaded){toast('Cookie 账号尚未加载，请先重新加载配置');return;}
  const accounts=collectCookieAccounts();
  try{
    const response=await fetch('/api/cookie-accounts',{method:'POST',signal:AbortSignal.timeout(30000),headers:{'Content-Type':'application/json'},body:JSON.stringify({accounts})});
    const result=await response.json();
    if(!response.ok) throw new Error(await responseError(response,'保存失败',result));
    COOKIE_ACCOUNTS=result.accounts.map(credentialCopy);renderCookieAccounts();
    toast('已保存 '+COOKIE_ACCOUNTS.length+' 个 Cookie 账号（已热生效）');
  }catch(error){toast('❌ '+error.message);}
}

/* Proxy editor stores a draft until Apply; account list/global Save persists it. */
function openProxyEditor(scope='global',index=0){
  if((scope==='key'&&!keysLoaded)||(scope==='cookie'&&!cookiesLoaded)||(scope==='global'&&!settingsLoaded)){
    toast('配置尚未加载，请先重新加载');return;
  }
  if(scope==='key') collectKeys();
  if(scope==='cookie') collectCookieAccounts();
  const source=scope==='global'?proxyDraft($('proxy-input').value):(scope==='key'?KEYS[index]:COOKIE_ACCOUNTS[index]).proxies;
  proxyEditor={scope,index,proxies:[...source],editing:null,opener:document.activeElement};
  $('proxy-dialog-title').textContent=scope==='global'?'全局代理列表':(scope==='key'?'Key ':'Cookie ')+(index+1)+' · 专属代理';
  $('proxy-dialog-hint').textContent='从第一条开始，连接失败时按顺序切换。应用后请点击「'+(scope==='global'?'保存代理':scope==='key'?'保存 Key 列表':'保存 Cookie 列表')+'」生效。';
  resetProxyForm();renderProxyEditor();$('proxy-dialog').showModal();
}
function closeProxyEditor(){
  $('proxy-dialog').close();
  const opener=proxyEditor?.opener;proxyEditor=null;
  if(opener?.isConnected) opener.focus();
}
function proxyLabel(uri){
  if(['direct','none','off','直连'].includes(uri.toLowerCase())) return '强制直连';
  try{
    const url=new URL(uri);
    if(url.protocol==='vmess:') return 'vmess://（分享链接）';
    if(url.protocol==='ss:'&&!url.username&&!url.password) return 'ss://（分享链接）';
    return url.protocol+'//'+(url.username||url.password?'***@':'')+url.host;
  }catch(_){return '待识别的代理地址';}
}
function renderProxyEditor(){
  $('proxy-editor-list').innerHTML=proxyEditor.proxies.map((uri,index)=>`
    <div class="proxy-row${proxyEditor.editing===index?' is-editing':''}" data-proxy-index="${index}"><div class="proxy-row-title"><span class="proxy-order">${index+1}</span><div><strong>${esc(proxyLabel(uri))}</strong><small>${proxyEditor.editing===index?'正在编辑 · ':''}${index===0?'首选出口':'第 '+(index+1)+' 顺位'}</small></div></div>
      <div class="proxy-row-actions">
        ${index>0?`<button type="button" onclick="moveProxy(${index},0)" aria-label="将第 ${index+1} 条设为首选">设为首选</button><button type="button" onclick="moveProxy(${index},${index-1})" aria-label="上移第 ${index+1} 条">↑</button>`:''}
        ${index<proxyEditor.proxies.length-1?`<button type="button" onclick="moveProxy(${index},${index+1})" aria-label="下移第 ${index+1} 条">↓</button>`:''}
        <button type="button" onclick="runAction(this,()=>testEditorProxy(${index}))">测试</button>
        <button type="button" onclick="runAction(this,()=>editProxy(${index}))">编辑</button>
        <button type="button" class="danger" onclick="deleteProxy(${index})" aria-label="删除第 ${index+1} 条代理">删除</button></div>
      <div class="proxy-row-result" id="proxy-result-${index}" role="status"></div></div>`).join('')||'<p class="empty-state">尚无代理。填写表单添加，或粘贴多条地址批量导入。<br>清空专属列表后跟随全局；清空全局列表后直连。</p>';
  $('proxy-editor-count').textContent=proxyEditor.proxies.length+' / 32 条';
  $('proxy-form-add').textContent=proxyEditor.editing===null?'加入列表':'更新第 '+(proxyEditor.editing+1)+' 条';
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
function resetProxyForm(){
  if(proxyEditor) proxyEditor.editing=null;
  ['proxy-paste','proxy-host','proxy-port','proxy-username','proxy-password'].forEach(id=>$(id).value='');
  $('proxy-type').value='socks5';$('proxy-form-add').textContent='加入列表';
  $('proxy-editor-error').textContent='';$('proxy-paste-hint').textContent='支持标准 URL、主机:端口:用户:密码、用户:密码:主机:端口及分享链接。';
  proxyTypeChanged();
}
function proxyTypeChanged(){
  const type=$('proxy-type').value;
  $('proxy-structured-fields').classList.toggle('hidden',['share','direct'].includes(type));
}
async function parseProxyInput(payload){
  const response=await fetch('/api/proxy/parse',{method:'POST',signal:AbortSignal.timeout(15000),headers:{'Content-Type':'application/json'},body:JSON.stringify(payload)});
  const result=await response.json();
  if(!response.ok) throw new Error(await responseError(response,'无法识别代理',result));
  return result.proxies;
}
function fillProxyFields(proxy){
  const type=proxy.url==='direct'?'direct':proxy.type;
  $('proxy-type').value=type;
  for(const field of ['host','port','username','password']) $('proxy-'+field).value=proxy[field]??'';
  if(type==='share'||type==='direct') $('proxy-paste').value=proxy.url;
  proxyTypeChanged();
}
async function recognizeProxyPaste(){
  const source=$('proxy-paste').value.trim();
  if(!source) return;
  const editor=proxyEditor;
  try{
    const proxies=await parseProxyInput({text:source,type:$('proxy-type').value});
    if(proxyEditor!==editor || $('proxy-paste').value.trim()!==source) return;
    $('proxy-editor-error').textContent='';
    if(proxies.length===1){
      fillProxyFields(proxies[0]);
      $('proxy-paste-hint').textContent='已识别 '+(proxies[0].url==='direct'?'强制直连':proxies[0].type==='share'?'分享链接':proxies[0].type.toUpperCase())+'，检查字段后加入列表。';
    }else{
      const merged=[...new Set([...proxyEditor.proxies,...proxies.map(p=>p.url)])];
      if(merged.length>32) throw new Error('每个列表最多 32 条代理');
      proxyEditor.proxies=merged;
      resetProxyForm();renderProxyEditor();toast('已识别并加入 '+proxies.length+' 条代理，请应用配置');
    }
  }catch(error){if(proxyEditor===editor) $('proxy-editor-error').textContent=error.message;}
}
async function editProxy(index){
  const editor=proxyEditor;
  try{
    const proxies=await parseProxyInput({text:editor.proxies[index]});
    if(proxyEditor!==editor) return;
    resetProxyForm();proxyEditor.editing=index;fillProxyFields(proxies[0]);
    renderProxyEditor();
    const target=$(['share','direct'].includes($('proxy-type').value)?'proxy-paste':'proxy-host');
    target.focus({preventScroll:true});target.scrollIntoView({block:'center',behavior:'smooth'});
  }catch(error){if(proxyEditor===editor) $('proxy-editor-error').textContent=error.message;}
}
async function addProxyDraft(){
  const editor=proxyEditor;
  const type=$('proxy-type').value;
  try{
    const payload=['share','direct'].includes(type)?{text:type==='direct'?'direct':$('proxy-paste').value.trim()}:
      {type,host:$('proxy-host').value.trim(),port:$('proxy-port').value.trim(),username:$('proxy-username').value,password:$('proxy-password').value};
    const proxies=await parseProxyInput(payload);
    if(proxyEditor!==editor) return false;
    if(proxyEditor.editing!==null){proxyEditor.proxies.splice(proxyEditor.editing,1,proxies[0].url);}
    else{
      const merged=[...new Set([...proxyEditor.proxies,...proxies.map(p=>p.url)])];
      if(merged.length>32) throw new Error('每个列表最多 32 条代理');
      proxyEditor.proxies=merged;
    }
    resetProxyForm();renderProxyEditor();return true;
  }catch(error){if(proxyEditor===editor) $('proxy-editor-error').textContent=error.message;return false;}
}
async function applyProxyEditor(){
  const editor=proxyEditor;
  if($('proxy-host').value.trim()||$('proxy-paste').value.trim()||$('proxy-type').value==='direct'){
    if(!await addProxyDraft()) return;
  }
  try{
    const normalized=editor.proxies.length?(await parseProxyInput({text:JSON.stringify(editor.proxies)})).map(p=>p.url):[];
    if(proxyEditor!==editor) return;
    if(editor.scope==='global') $('proxy-input').value=normalized.join('\n');
    else{
      const record=(editor.scope==='key'?KEYS:COOKIE_ACCOUNTS)[editor.index];
      record.proxies=normalized;record.proxy=normalized[0]||'';
      if(editor.scope==='key') renderKeys();else renderCookieAccounts();
    }
    closeProxyEditor();toast('代理列表已应用，请保存'+(editor.scope==='global'?'代理':editor.scope==='key'?' Key 列表':' Cookie 列表')+'后生效');
  }catch(error){if(proxyEditor===editor) $('proxy-editor-error').textContent=error.message;}
}
async function testEditorProxy(index){
  const editor=proxyEditor, target=$('proxy-result-'+index);
  target.textContent='正在测试…';
  try{
    const response=await fetch('/api/proxy/test',{method:'POST',signal:AbortSignal.timeout(30000),headers:{'Content-Type':'application/json'},body:JSON.stringify({proxy_url:editor.proxies[index]})});
    const result=await response.json();
    if(!response.ok) throw new Error(await responseError(response,'测试失败',result));
    if(proxyEditor===editor && target.isConnected){target.textContent=(result.ok?'✓ ':'✕ ')+result.message+' · '+result.latency_ms+' ms';target.classList.toggle('error-message',!result.ok);}
  }catch(error){if(target.isConnected) target.textContent='测试失败：'+error.message;}
}
function initProxyEditor(){
  if(proxyEditorInitialized) return;
  if(!['proxy-dialog-close','proxy-dialog-cancel','proxy-dialog','proxy-paste'].every(id=>$(id))) throw new Error('代理编辑器页面版本不一致，请刷新面板');
  $('proxy-dialog-close').addEventListener('click',closeProxyEditor);
  $('proxy-dialog-cancel').addEventListener('click',closeProxyEditor);
  $('proxy-dialog').addEventListener('cancel',event=>{event.preventDefault();closeProxyEditor();});
  $('proxy-paste').addEventListener('paste',()=>setTimeout(()=>{if(proxyEditor) runAction(document.querySelector('.proxy-recognize'),recognizeProxyPaste);},0));
  proxyEditorInitialized=true;
}

window.consoleChannels={load:loadCookieAccounts,init:initProxyEditor,open:openProxyEditor};
Object.assign(window,{updateCredentialStrategy,loadCookieAccounts,addCookieAccount,removeCookieAccount,saveCookieAccounts,
  closeProxyEditor,moveProxy,deleteProxy,resetProxyForm,proxyTypeChanged,recognizeProxyPaste,editProxy,addProxyDraft,
  applyProxyEditor,testEditorProxy,renderProxyEditor});
})();
