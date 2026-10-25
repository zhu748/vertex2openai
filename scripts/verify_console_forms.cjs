/* Offline DOM interaction regression. npm ci && npm run test:console.
 * jsdom checks behavior, not layout; use verify_console_ui.cjs for rendering. */
const assert=require('node:assert/strict');
const fs=require('node:fs');
const path=require('node:path');
const {JSDOM,VirtualConsole}=require('jsdom');
const root=path.resolve(__dirname,'..');
const tick=()=>new Promise(r=>setTimeout(r,20));
async function main(){
 const errors=[],requests=[];
 const virtualConsole=new VirtualConsole();virtualConsole.on('jsdomError',e=>errors.push(e.message));
 const dom=new JSDOM(fs.readFileSync(path.join(root,'app/templates/dashboard.html'),'utf8'),{url:'http://console.test/',runScripts:'dangerously',pretendToBeVisual:true,virtualConsole});
 const w=dom.window,d=w.document;
 const settings={thinking_g3_level:'low',native_thinking_mode:'request',first_reply_timeout_seconds:15,image_size:'4K',image_aspect_ratio:'16:9',sampling_policy:'auto',default_top_p:null,default_temperature:null,default_max_tokens:null,roundrobin:false,proxy_urls:[],cookie_impersonate:'chrome',express_ua_profile:'sdk',retry_max:10,retry_backoff_seconds:5,retry_jitter_seconds:1,request_timeout_seconds:1800,upstream_max_concurrency:32,resumable_enabled:true,resumable_disconnect_grace_seconds:10,resumable_ttl_seconds:1800,resumable_max_streams:200,resumable_max_buffer_mb:64,resumable_max_total_mb:256,keepalive_interval:60,img_compress_enabled:true,img_compress_max_dim:1536,img_compress_max_mb:1.5,img_compress_quality:85,fake_streaming_interval:1,prefill_mode:'smart',express_location:'global'};
 const overrides={},builtins={text:{anti_truncation_mode:'both'}};
 settings.cookie_auto_refresh=true;settings.cookie_refresh_interval_seconds=600;
 let mode='mixed';
 let keys=[{key:'ui-fixture-key',project_id:'project-one',note:'主账号',enabled:true,proxies:[],proxy_fallback:true}];
 let cookies=[{id:'cookie-one',project_id:'project-cookie',note:'备用账号',enabled:true,cookie_configured:true,proxies:[],proxy_fallback:true}];
 const cap={family:'3',thinking:{kind:'level',levels:['low','medium','high']},supports_anti_truncation:true,supports_search:true,image_sizes:['1K','2K','4K'],image_aspect_ratios:['1:1','16:9']};
 const summary={connections:1,generations:1,background:0,retry_wait:0,active:1};
 const record={id:'trace-1234567890abcdef',model:'text',protocol:'openai',channel:'express',mode:'stream',phase:'streaming',status:'running',connection:'open',created_at:Date.now()/1000,duration_ms:1000,timings:{},attempts:1,retries:0,finish_reasons:[],usage:{},events:[],logs:[]};
 let scrollPosition=0,transport=null,requestTotal=1;
 Object.defineProperty(w,'scrollY',{get:()=>scrollPosition});
 w.AbortSignal=AbortSignal;w.AbortController=AbortController;w.matchMedia=()=>({matches:false});w.scrollTo=({top})=>{scrollPosition=top;};w.HTMLElement.prototype.scrollIntoView=()=>{};w.confirm=()=>true;
 w.CSS={escape:s=>s};w.HTMLDialogElement.prototype.showModal=function(){this.open=true;};w.HTMLDialogElement.prototype.close=function(){this.open=false;this.dispatchEvent(new w.Event('close'));};
 w.EventSource=class {close(){}};
 w.fetch=async(url,options={})=>{
  const target=new URL(url,w.location.href),method=options.method||'GET',body=options.body?JSON.parse(options.body):null;requests.push({path:target.pathname,query:target.search,method,body});
  const intercepted=transport?.(target,options);if(intercepted!==undefined)return intercepted;
  let result={};
  if(target.pathname==='/api/settings/runtime')result={mode};
  else if(target.pathname==='/api/settings'){if(body)Object.assign(settings,body);result={...settings};}
  else if(target.pathname==='/api/capabilities')result={models:['text','image'],capabilities:{text:{...cap,is_image:false},image:{...cap,is_image:true}},overrides:{text:{...builtins.text,...overrides.text},...Object.fromEntries(Object.entries(overrides).filter(([m])=>m!=='text'))},saved_overrides:overrides,builtin_overrides:builtins};
  else if(target.pathname.startsWith('/api/model-overrides/')){const model=target.pathname.split('/').pop();if(method==='DELETE')delete overrides[model];else{const current={...overrides[model]};for(const key of body._inherit||[])delete current[key];Object.assign(current,Object.fromEntries(Object.entries(body).filter(([k])=>k!=='_inherit')));overrides[model]=current;}result={override:{...builtins[model],...overrides[model]}};}
  else if(target.pathname==='/api/express-keys'){if(body)keys=body.keys;result={keys};}
  else if(target.pathname==='/api/cookie-accounts'){if(body)cookies=body.accounts;result={accounts:cookies};}
  else if(target.pathname==='/api/cookie-accounts/refresh'){
   const results=cookies.filter(rec=>rec.enabled!==false&&(!body.id||rec.id===body.id)).map(rec=>({id:rec.id,label:rec.note,status:'unchanged',changed:[],detail:'无新令牌 <img src=x onerror="window.refreshInjected=true">'}));
   result={results,total:results.length,refreshed:0,unchanged:results.length,errors:0};
  }
  else if(target.pathname==='/api/stats')result={total:3,success:1,error:1,retries:0,requests:summary,prompt_tokens:100,completion_tokens:200,uptime:50};
  else if(target.pathname==='/api/recent-metrics')result={sample_count:3,success:1,error:1,active:1,reply_samples:2,first_reply_p50_ms:100,first_reply_p95_ms:900,channels:{express:{success:1,error:1,active:1}}};
  else if(target.pathname==='/api/requests'){
   const offset=Number(target.searchParams.get('offset')||0),limit=Number(target.searchParams.get('limit')||25);
   result={summary,total:requestTotal,items:Array.from({length:Math.max(0,Math.min(limit,requestTotal-offset))},(_,i)=>({...record,id:requestTotal===1?record.id:record.id+'-'+(offset+i)}))};
  }
  else if(target.pathname.startsWith('/api/requests/'))result={...record,id:decodeURIComponent(target.pathname.split('/').pop())};
  else if(target.pathname==='/api/proxy/parse')result={proxies:[]};
  else if(target.pathname==='/api/settings/mode'){mode=body.mode;result={status:'success'};}
  else throw Error('Unexpected endpoint '+target.pathname);
  return new Response(JSON.stringify(result),{status:200,headers:{'content-type':'application/json'}});
 };
 const el=id=>d.getElementById(id);
 function edit(id,value){const input=el(id);if(input.type==='checkbox')input.checked=value;else input.value=value;input.dispatchEvent(new w.Event('input',{bubbles:true}));input.dispatchEvent(new w.Event('change',{bubbles:true}));}
 async function save(id){el(id).click();await tick();}
 try{
  for(const name of ['channel-config.js','dashboard.js','console-forms.js','request-monitor.js'])require('node:vm').runInContext(fs.readFileSync(path.join(root,'app/static',name),'utf8'),dom.getInternalVMContext(),{filename:name});
  await tick();
  assert.deepEqual(errors,[]);
  assert.equal(w.consoleForms.dirty(),false);
  assert.equal(el('recent-rate').textContent,'50.0%');
  assert.equal(el('runtime-save-bar').closest('[role=tabpanel]').id,'view-runtime');
  assert.equal(el('roundrobin'),null);
  assert.equal(el('keys-list').querySelector('details').open,false);
  assert.equal(el('key-0').type,'password');
  console.log('✓ Navigation, initial clean state, account masking/collapse, recent metrics');

  w.switchTab('channel');edit('key-note-0','切换保留 Key 草稿');edit('cookie-note-0','切换保留 Cookie 草稿');
  const credentialWrites=requests.filter(r=>r.method==='POST'&&['/api/express-keys','/api/cookie-accounts'].includes(r.path)).length;
  const modeVisible=selected=>{
   assert.equal(d.querySelector('[data-draft-group=keys]').classList.contains('hidden'),selected==='web_proxy');
   assert.equal(el('cookie-box').classList.contains('hidden'),selected==='api_key');
  };
  for(const selected of ['api_key','web_proxy','mixed']){
   d.querySelector(`input[name=mode][value=${selected}]`).checked=true;await w.updateMode(selected);
   assert.equal(mode,selected);modeVisible(selected);
   await w.loadRuntime();modeVisible(selected);
   assert.equal(d.querySelector('input[name=mode]:checked').value,selected);
   assert.equal(el('key-note-0').value,'切换保留 Key 草稿');assert.equal(el('cookie-note-0').value,'切换保留 Cookie 草稿');
  }
  transport=target=>{if(target.pathname==='/api/settings/mode'){transport=null;return new Response('{}',{status:503});}};
  d.querySelector('input[name=mode][value=web_proxy]').checked=true;await w.updateMode('web_proxy');
  assert.equal(mode,'mixed');modeVisible('mixed');assert.equal(d.querySelector('input[name=mode]:checked').value,'mixed');
  assert.equal(requests.filter(r=>r.method==='POST'&&['/api/express-keys','/api/cookie-accounts'].includes(r.path)).length,credentialWrites);
  w.consoleForms.discard('keys');w.consoleForms.discard('cookies');assert.equal(w.consoleForms.dirty(),false);
  console.log('✓ Three channel modes show matching credential lists, preserve drafts and roll back failed saves');

  for(const link of d.querySelectorAll('.settings-jump a')){
   w.history.replaceState(null,'',link.href);w.navigateHash();
   const panel=el(link.hash.slice(1)).closest('.view');
   assert.equal(panel.classList.contains('hidden'),false);assert.equal(el('mobile-nav').value,panel.id.slice(5));
  }
  w.switchTab('params');w.scrollTo({top:420});w.switchTab('runtime');w.scrollTo({top:180});
  w.switchTab('params');assert.equal(w.scrollY,420);w.switchTab('runtime');assert.equal(w.scrollY,180);
  w.history.replaceState(null,'','#main-content');w.navigateHash();assert.equal(el('mobile-nav').value,'runtime');
  w.history.replaceState(null,'','#global-settings');w.navigateHash();assert.equal(el('mobile-nav').value,'runtime');
  await tick();
  console.log('✓ Section deep links, mobile navigation, page scroll restoration and skip link');

  w.switchTab('params');edit('model-sel','text');edit('first_reply_timeout_seconds','35');await save('btn-scope-save');
  const first=requests.filter(r=>r.path==='/api/model-overrides/text').at(-1).body;
  assert.deepEqual(first,{first_reply_timeout_seconds:35});
  assert.equal(el('source-thinking_g3_level').textContent.includes('继承全局'),true);
  w.setFieldInheritance('first_reply_timeout_seconds',true);await save('btn-scope-save');
  assert.deepEqual(requests.filter(r=>r.path==='/api/model-overrides/text').at(-1).body,{_inherit:['first_reply_timeout_seconds']});
  edit('first_reply_timeout_seconds','24');edit('model-sel','image');edit('model-sel','text');
  assert.equal(el('first_reply_timeout_seconds').value,'24');
  edit('model-sel','__global__');edit('thinking_g3_level','high');await save('btn-scope-save');
  edit('model-sel','text');assert.equal(el('first_reply_timeout_seconds').value,'24');
  assert.equal(el('thinking_g3_level').value,'high');
  assert.equal(el('source-thinking_g3_level').textContent.includes('继承全局'),true);
  w.discardModelDraft();assert.equal(el('first_reply_timeout_seconds').value,'15');
  assert.equal(el('image-controls').hidden,true);
  assert.equal(el('thinking_g3_level').querySelector('[value=minimal]').disabled,true);
  console.log('✓ Sparse model save, per-field inheritance, capability options and model drafts');

  edit('first_reply_timeout_seconds','25');edit('first_reply_timeout_seconds','15');
  assert.equal(w.consoleForms.dirty(),false);
  w.setFieldInheritance('first_reply_timeout_seconds',false);assert.equal(w.consoleForms.dirty(),true);
  edit('model-sel','image');edit('model-sel','text');
  await save('btn-scope-save');
  assert.deepEqual(requests.filter(r=>r.path==='/api/model-overrides/text').at(-1).body,{first_reply_timeout_seconds:15});
  w.setFieldInheritance('first_reply_timeout_seconds',true);await save('btn-scope-save');
  edit('first_reply_timeout_seconds','25');w.setFieldInheritance('first_reply_timeout_seconds',true);
  assert.equal(w.consoleForms.dirty(),false);
  overrides.text.image_size='1K';overrides.image={image_aspect_ratio:'1:1'};settings.image_aspect_ratio='21:9';
  await w.loadParams();edit('show-image-settings',true);
  assert.equal(el('image_size').disabled,true);assert.equal(el('source-image_size').querySelector('button').disabled,false);
  w.setFieldInheritance('image_size',true);await save('btn-scope-save');assert.equal(Object.hasOwn(overrides.text,'image_size'),false);
  edit('model-sel','image');w.setFieldInheritance('image_aspect_ratio',true);
  assert.equal(el('image_aspect_ratio').value,'21:9');assert.equal(el('image_aspect_ratio').selectedOptions[0].disabled,true);
  await save('btn-scope-save');edit('default_max_tokens','512');await save('btn-scope-save');
  assert.deepEqual(requests.filter(r=>r.path==='/api/model-overrides/image').at(-1).body,{default_max_tokens:512});
  builtins.text.default_top_p=null;settings.default_top_p=0.8;await w.loadParams();edit('model-sel','text');
  assert.equal(el('default_top_p').value,'');edit('default_top_p','0.5');await save('btn-scope-save');
  w.setFieldInheritance('default_top_p',true);assert.equal(el('default_top_p').value,'');await save('btn-scope-save');
  delete builtins.text.default_top_p;delete overrides.image;settings.image_aspect_ratio='16:9';settings.default_top_p=null;
  await w.loadParams();
  console.log('✓ Reverted edits, deliberate pins, obsolete override cleanup and exact inherited values');

  edit('model-sel','__global__');edit('default_top_p','2');await save('btn-scope-save');
  assert.equal(el('default_top_p').getAttribute('aria-invalid'),'true');
  assert(el('default_top_p-error'));
  edit('default_top_p','');assert.equal(el('default_top_p-error'),null);w.discardModelDraft();
  w.switchTab('runtime');const help=d.querySelector('[aria-controls=h_ccf]');help.click();assert.equal(el('h_ccf').hidden,false);help.click();assert.equal(el('h_ccf').hidden,true);
  console.log('✓ Persistent field validation and accessible help disclosure');
  edit('retry_max','12');assert.equal(w.consoleForms.dirty(),true);
  await w.loadParams();assert.equal(el('retry_max').value,'12');
  w.consoleForms.discard('runtime');assert.equal(el('retry_max').value,'10');
  edit('retry_max','13');d.querySelector('button[onclick*="saveSettings"]').click();await tick();
  assert.equal(settings.retry_max,13);assert.equal(w.consoleForms.dirty(),false);
  console.log('✓ Runtime draft reload preservation, save and undo');

  assert.equal(el('cookie_auto_refresh').checked,true);
  edit('cookie_auto_refresh',true);edit('cookie_refresh_interval_seconds','900');
  assert.equal(w.consoleForms.dirty(),true);
  d.querySelector('button[onclick*="saveSettings"]').click();await tick();
  assert.equal(settings.cookie_auto_refresh,true);assert.equal(settings.cookie_refresh_interval_seconds,900);
  assert.equal(w.consoleForms.dirty(),false);
  await w.loadParams();assert.equal(el('cookie_auto_refresh').checked,true);
  assert.equal(el('cookie_refresh_interval_seconds').value,'900');
  edit('cookie_refresh_interval_seconds','299');
  const beforeInvalid=requests.length;d.querySelector('button[onclick*="saveSettings"]').click();await tick();
  assert.equal(requests.length,beforeInvalid);assert.equal(el('cookie_refresh_interval_seconds').getAttribute('aria-invalid'),'true');
  w.consoleForms.discard('runtime');assert.equal(el('cookie_refresh_interval_seconds').value,'900');
  console.log('✓ Cookie refresh controls save, reload, validate limits and undo');

  edit('cookie_auto_refresh',false);d.querySelector('button[onclick*="saveSettings"]').click();await tick();
  const settingsWrites=requests.filter(r=>r.path==='/api/settings'&&r.method==='POST').length;
  await w.refreshCookies(0);
  assert.deepEqual(requests.filter(r=>r.path==='/api/cookie-accounts/refresh').at(-1).body,{id:'cookie-one'});
  assert(el('cookie-refresh-result').textContent.includes('无新令牌 <img'));
  assert.equal(el('cookie-refresh-result').querySelector('img'),null);assert.equal(w.refreshInjected,undefined);
  assert.equal(settings.cookie_auto_refresh,false);
  let resolveRefresh;
  transport=target=>{if(target.pathname==='/api/cookie-accounts/refresh'){transport=null;return new Promise(resolve=>{resolveRefresh=resolve;});}};
  el('cookie-refresh-now').click();await tick();assert.equal(el('cookie-refresh-now').matches(':disabled'),true);
  const refreshRequests=requests.filter(r=>r.path==='/api/cookie-accounts/refresh').length;
  await w.refreshCookies(0);assert.equal(requests.filter(r=>r.path==='/api/cookie-accounts/refresh').length,refreshRequests);
  edit('cookie-note-0','刷新期间保留草稿');
  resolveRefresh(new Response(JSON.stringify({results:[{id:'cookie-one',label:'备用账号',status:'refreshed',detail:'已更新 SIDCC'}],total:1,refreshed:1,unchanged:0,errors:0})));
  await tick();assert.equal(el('cookie-refresh-now').matches(':disabled'),false);
  assert.equal(el('cookie-note-0').value,'刷新期间保留草稿');assert.equal(w.consoleForms.dirty(),true);
  await w.refreshCookies(0);assert.equal(requests.filter(r=>r.path==='/api/cookie-accounts/refresh').length,refreshRequests);
  w.consoleForms.discard('cookies');
  transport=target=>{if(target.pathname==='/api/cookie-accounts/refresh'){transport=null;return new Response(JSON.stringify({error:'Cookie 正在刷新'}),{status:409});}};
  await w.refreshCookies(0);assert(el('cookie-refresh-result').textContent.includes('Cookie 正在刷新'));
  assert.equal(requests.filter(r=>r.path==='/api/settings'&&r.method==='POST').length,settingsWrites);
  assert.equal(w.consoleForms.dirty(),false);
  console.log('✓ Manual Cookie refresh works with auto off, escapes results, prevents duplicates and preserves drafts');

  w.switchTab('channel');w.consoleForms.openAccount('key',0);edit('key-note-0','新备注');
  assert.equal(w.consoleForms.dirty(),true);assert.equal(d.querySelector('.account-name').textContent,'新备注');
  w.consoleForms.discard('keys');assert.equal(el('key-note-0').value,'主账号');
  edit('keys-search','missing');assert.equal(el('keys-list').firstElementChild.hidden,true);
  edit('keys-search','');assert.equal(el('keys-list').firstElementChild.hidden,false);
  w.addKeyRow();assert.equal(el('keys-list').lastElementChild.querySelector('details').open,true);
  w.consoleForms.discard('keys');assert.equal(el('keys-list').children.length,1);
  edit('credential-strategy','roundrobin');await tick();assert.equal(settings.roundrobin,true);assert.equal(w.consoleForms.dirty(),false);
  edit('proxy-input','http://proxy.test:80');await w.saveProxy();assert.equal(w.consoleForms.dirty(),false);
  console.log('✓ Credential drafts, filters, add/undo, immediate strategy and proxy save state');

  let probed={...keys[0],project_id:'detected-project',project_source:'auto'};
  keys=[probed];w.mergeProbeResults([{key:probed.key,ok:true,project_id:probed.project_id}],keys);
  assert.equal(el('project-0').value,'detected-project');assert.equal(w.consoleForms.dirty(),false);
  edit('project-0','temporary-project');assert.equal(w.consoleForms.dirty(),true);
  edit('project-0','detected-project');assert.equal(w.consoleForms.dirty(),false);
  assert.equal(w.collectKeys()[0].project_source,'auto');
  edit('key-note-0','保留草稿');
  probed={...probed,project_id:'new-detected-project'};keys=[probed];
  w.mergeProbeResults([{key:probed.key,ok:true,project_id:probed.project_id}],keys);
  assert.equal(el('key-note-0').value,'保留草稿');assert.equal(w.consoleForms.dirty(),true);
  w.consoleForms.discard('keys');assert.equal(el('project-0').value,'new-detected-project');
  assert.equal(el('key-note-0').value,'主账号');assert.equal(w.consoleForms.dirty(),false);
  w.addKeyRow();assert.equal(w.consoleForms.dirty(),true);w.consoleForms.discard('keys');
  console.log('✓ Persisted probe results rebase saved state without swallowing credential drafts');

  let resolveOldProxy;
  transport=target=>{if(target.pathname==='/api/proxy/test'){transport=null;return new Promise(resolve=>{resolveOldProxy=resolve;});}};
  w.consoleChannels.open('global');const oldProxyJob=w.testAllEditorProxies();await tick();
  w.closeProxyEditor(true);w.consoleChannels.open('global');
  transport=target=>{if(target.pathname==='/api/proxy/test'){transport=null;return new Response(JSON.stringify({ok:true,message:'fresh pass'}));}};
  await w.testAllEditorProxies();assert.equal(el('proxy-result-0').dataset.state,'passed');
  resolveOldProxy(new Response(JSON.stringify({ok:false,message:'late stale failure'})));await oldProxyJob;
  w.renderProxyEditor();assert.equal(el('proxy-result-0').dataset.state,'passed');
  assert(el('proxy-result-0').textContent.includes('fresh pass'));w.closeProxyEditor(true);
  let resolveStoppedProxy;
  transport=target=>{if(target.pathname==='/api/proxy/test'){transport=null;return new Promise(resolve=>{resolveStoppedProxy=resolve;});}};
  w.consoleChannels.open('global');const stoppedProxyJob=w.testAllEditorProxies();await tick();w.stopProxyTests();
  resolveStoppedProxy(new Response(JSON.stringify({ok:true,message:'late cancelled pass'})));await stoppedProxyJob;
  assert.equal(el('proxy-result-0').dataset.state,'cancelled');
  assert(el('proxy-job-state').textContent.includes('通过 0 / 1'));w.closeProxyEditor(true);
  console.log('✓ Closed and cancelled proxy probes cannot overwrite newer results or import cancelled passes');

  w.switchTab('requests');await tick();const row=el('request-rows').firstElementChild;
  await w.requestMonitor.refresh();assert.equal(el('request-rows').firstElementChild,row);
  el('request-pause').click();const count=requests.length;await w.requestMonitor.refresh(false);assert.equal(requests.length,count);
  assert.equal(el('request-pause').getAttribute('aria-pressed'),'true');el('request-pause').click();await tick();
  edit('request-page-size','10');await tick();assert.equal(el('request-page-size').value,'10');

  requestTotal=52;edit('request-page-size','25');await tick();
  el('request-next').click();await tick();el('request-next').click();await tick();
  assert.equal(el('request-page-info').textContent,'51–52 / 52 条');
  requestTotal=30;await w.requestMonitor.refresh();assert.equal(el('request-page-info').textContent,'26–30 / 30 条');
  assert.equal(new URLSearchParams(requests.filter(r=>r.path==='/api/requests').at(-1).query).get('offset'),'25');
  let rejectOld;
  transport=target=>{if(target.pathname==='/api/requests'){transport=null;return new Promise((resolve,reject)=>{rejectOld=reject;});}};
  const oldRequest=w.requestMonitor.refresh();await w.requestMonitor.refresh();
  const freshState=el('request-refresh-state').textContent;rejectOld(new Error('outdated list failure'));await oldRequest;
  assert.equal(el('request-refresh-state').textContent,freshState);assert.equal(el('request-refresh-state').classList.contains('error-message'),false);
  let resolvePaused;
  transport=target=>{if(target.pathname==='/api/requests'){transport=null;return new Promise(resolve=>{resolvePaused=resolve;});}};
  const pausedRequest=w.requestMonitor.refresh();el('request-pause').click();
  resolvePaused(new Response(JSON.stringify({summary,total:0,items:[]})));await pausedRequest;
  assert.equal(el('request-page-info').textContent,'26–30 / 30 条');assert.equal(el('request-refresh-state').textContent,'已暂停自动刷新，可手动刷新');
  transport=target=>{if(target.pathname==='/api/requests'){transport=null;return new Response('{}',{status:500});}};
  await w.requestMonitor.refresh();assert(el('request-refresh-state').textContent.includes('请手动刷新重试'));
  el('request-pause').click();await tick();
  let rejectDetail;
  transport=target=>{if(target.pathname==='/api/requests/old-record'){transport=null;return new Promise((resolve,reject)=>{rejectDetail=reject;});}};
  w.requestMonitor.open('old-record');w.requestMonitor.open('new-record');await tick();
  const detailState=el('request-detail-state').textContent;rejectDetail(new Error('outdated detail failure'));await tick();
  assert.equal(el('request-detail-id').textContent,'new-record');assert.equal(el('request-detail-state').textContent,detailState);
  el('request-dialog').close();
  console.log('✓ Retained-page clamping, stale response isolation and paused retry messages');
  const ids=[...d.querySelectorAll('[id]')].map(e=>e.id);assert.equal(new Set(ids).size,ids.length);
  assert.deepEqual(errors,[]);
  console.log('✓ Stable request rows, pause/resume, pagination and unique DOM IDs');
  console.log('14 groups of console DOM checks passed. Layout requires the browser suite.');
 }finally{dom.window.close();}
}
main().catch(error=>{console.error(error);process.exitCode=1;});
