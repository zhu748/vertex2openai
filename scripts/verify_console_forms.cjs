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
 let keys=[{key:'ui-fixture-key',project_id:'project-one',note:'主账号',enabled:true,proxies:[],proxy_fallback:true}];
 let cookies=[{id:'cookie-one',project_id:'project-cookie',note:'备用账号',enabled:true,cookie_configured:true,proxies:[],proxy_fallback:true}];
 let cookieResult={status:'valid',reason:'session_accepted',detail:'会话有效',health:{failures:0,cooldown_remaining:0}};
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
  if(target.pathname==='/api/settings/runtime')result={mode:'mixed'};
  else if(target.pathname==='/api/settings'){if(body)Object.assign(settings,body);result={...settings};}
  else if(target.pathname==='/api/capabilities')result={models:['text','image'],capabilities:{text:{...cap,is_image:false},image:{...cap,is_image:true}},overrides:{text:{...builtins.text,...overrides.text},...Object.fromEntries(Object.entries(overrides).filter(([m])=>m!=='text'))},saved_overrides:overrides,builtin_overrides:builtins};
  else if(target.pathname.startsWith('/api/model-overrides/')){const model=target.pathname.split('/').pop();if(method==='DELETE')delete overrides[model];else{const current={...overrides[model]};for(const key of body._inherit||[])delete current[key];Object.assign(current,Object.fromEntries(Object.entries(body).filter(([k])=>k!=='_inherit')));overrides[model]=current;}result={override:{...builtins[model],...overrides[model]}};}
  else if(target.pathname==='/api/express-keys'){if(body)keys=body.keys;result={keys};}
  else if(target.pathname==='/api/cookie-accounts'){
   if(body)cookies=body.accounts.map((item,i)=>{const saved={...item,id:item.id||'saved-cookie-'+i,cookie_configured:true};delete saved.cookie;return saved;});
   result={accounts:cookies};
  }
  else if(target.pathname==='/api/cookie-accounts/test')result={results:[{id:body.id,checked_at:Date.now()/1000,elapsed_ms:120,...cookieResult}]};
  else if(target.pathname==='/api/stats')result={total:3,success:1,error:1,retries:0,requests:summary,prompt_tokens:100,completion_tokens:200,uptime:50};
  else if(target.pathname==='/api/recent-metrics')result={sample_count:3,success:1,error:1,active:1,reply_samples:2,first_reply_p50_ms:100,first_reply_p95_ms:900,channels:{express:{success:1,error:1,active:1}}};
  else if(target.pathname==='/api/requests'){
   const offset=Number(target.searchParams.get('offset')||0),limit=Number(target.searchParams.get('limit')||25);
   result={summary,total:requestTotal,items:Array.from({length:Math.max(0,Math.min(limit,requestTotal-offset))},(_,i)=>({...record,id:requestTotal===1?record.id:record.id+'-'+(offset+i)}))};
  }
  else if(target.pathname.startsWith('/api/requests/'))result={...record,id:decodeURIComponent(target.pathname.split('/').pop())};
  else if(target.pathname==='/api/proxy/parse')result={proxies:[]};
  else if(target.pathname==='/api/settings/mode')result={status:'success'};
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

  assert.equal(requests.some(r=>r.path==='/api/cookie-accounts/test'),false);
  const cookieInput=el('cookie-note-0');edit('cookie-note-0','保留本地备注');cookieInput.focus();
  cookieResult={status:'valid',detail:'<img src=x onerror="window.injected=true">',health:{failures:0,cooldown_remaining:0}};
  await w.probeCookieAccounts(0);
  assert.equal(d.querySelector('.cookie-health-badge').textContent,'会话有效');assert.equal(el('cookie-note-0'),cookieInput);
  assert.equal(cookieInput.value,'保留本地备注');assert.equal(w.consoleForms.dirty(),true);assert.equal(d.querySelector('.cookie-probe-result img'),null);
  assert.deepEqual(requests.filter(r=>r.path==='/api/cookie-accounts/test').at(-1).body,{id:'cookie-one'});
  assert.equal(d.activeElement,cookieInput);
  cookieResult={status:'valid',reason:'resource_exhausted',detail:'资源受限',health:{failures:0,cooldown_remaining:0}};
  await w.probeCookieAccounts(0);assert.equal(d.querySelector('.cookie-health-badge').textContent,'认证通过 · 资源受限');
  assert(el('cookie-probe-summary').textContent.includes('资源受限 1'));
  cookieResult={status:'invalid',reason:'permission_denied',detail:'访问被拒绝',health:{failures:0,cooldown_remaining:120}};
  await w.probeCookieAccounts(0);assert.equal(d.querySelector('.cookie-health-badge').textContent,'权限被拒绝');
  assert(d.querySelector('.cookie-probe-result').textContent.includes('冷却中'));assert.equal(d.querySelector('.c-enabled').checked,true);
  transport=target=>{if(target.pathname==='/api/cookie-accounts/test'){transport=null;return new Response('{}',{status:503});}};
  await w.probeCookieAccounts(0);assert.equal(d.querySelector('.cookie-health-badge').textContent,'无法判断');
  edit('cookie-project-0','unsaved-project');assert.equal(d.querySelector('.cookie-probe-button').disabled,true);
  assert.equal(d.querySelector('.cookie-health-badge').textContent,'待保存重测');
  const probeCount=requests.filter(r=>r.path==='/api/cookie-accounts/test').length;await w.probeCookieAccounts(0);
  assert.equal(requests.filter(r=>r.path==='/api/cookie-accounts/test').length,probeCount);
  edit('cookie-project-0','project-cookie');edit('cookie-0','SAPISID=unsaved');assert.equal(d.querySelector('.cookie-probe-button').disabled,true);
  edit('cookie-0','');cookieResult={status:'valid',detail:'会话有效',health:{failures:0,cooldown_remaining:0}};
  await w.probeCookieAccounts(0);assert.equal(d.querySelector('.cookie-probe-result').textContent.includes('冷却中'),false);
  edit('proxy-input','http://changed-proxy.test:80');await w.saveProxy();w.consoleChannels.sync();
  assert.equal(d.querySelector('.cookie-health-badge').textContent,'配置已变化');
  w.consoleForms.discard('cookies');assert.equal(w.consoleForms.dirty(),false);
  console.log('✓ Cookie single probes, reason labels, cooldown, draft guards and safe result rendering');

  const cookieFixture=(id,enabled=true)=>({id,enabled,cookie_configured:true,project_id:'project',proxies:[],proxy_fallback:true});
  cookies=Array.from({length:8},(_,i)=>cookieFixture('batch-'+i));cookies.push(cookieFixture('disabled',false));
  await w.loadCookieAccounts();w.addCookieAccount();edit('cookie-project-1','not-saved');edit('cookie-note-2','备注不丢失');
  let activeProbes=0,peakProbes=0;
  transport=target=>{if(target.pathname==='/api/cookie-accounts/test'){
   activeProbes++;peakProbes=Math.max(peakProbes,activeProbes);
   const id=requests.at(-1).body.id,kind=Number(id.split('-').at(-1))%3;
   return new Promise(resolve=>setTimeout(()=>{activeProbes--;resolve(new Response(JSON.stringify({results:[{id,status:['valid','invalid','error'][kind],reason:kind===1?'permission_denied':'network_error',detail:'fixture',elapsed_ms:10,checked_at:Date.now()/1000}]})));},10));
  }};
  const batchStart=requests.length,batch=w.probeCookieAccounts();assert.equal(el('cookie-probe-stop').hidden,false);
  await batch;transport=null;
  const batchIDs=requests.slice(batchStart).filter(r=>r.path==='/api/cookie-accounts/test').map(r=>r.body.id);
  assert.equal(batchIDs.length,7);assert.equal(peakProbes,3);assert(!batchIDs.includes('batch-1')&&!batchIDs.includes('disabled'));
  assert(el('cookie-probe-summary').textContent.includes('已完成 7 / 7'));assert(el('cookie-probe-summary').textContent.includes('无法判断 2'));
  assert(el('cookie-probe-summary').textContent.includes('跳过 3'));assert.equal(el('cookie-note-2').value,'备注不丢失');
  assert.equal(el('cookie-project-1').value,'not-saved');assert.equal(w.consoleForms.dirty(),true);
  console.log('✓ Bounded Cookie batch concurrency, progress, skipped drafts and independent failures');

  cookies=Array.from({length:5},(_,i)=>cookieFixture('cancel-'+i));await w.loadCookieAccounts();
  const late=[];
  transport=target=>{if(target.pathname==='/api/cookie-accounts/test'){const id=requests.at(-1).body.id;return new Promise(resolve=>late.push(()=>resolve(new Response(JSON.stringify({results:[{id,status:'invalid',detail:'late failure'}]})))));}};
  const cancelStart=requests.length,cancelled=w.probeCookieAccounts();assert.equal(late.length,3);
  w.stopCookieProbes();assert(el('cookie-probe-summary').textContent.includes('已停止'));transport=null;
  await w.probeCookieAccounts(0);const latestSummary=el('cookie-probe-summary').textContent;
  late.forEach(resolve=>resolve());await cancelled;
  assert.equal(el('cookie-probe-summary').textContent,latestSummary);assert.equal(d.querySelector('.cookie-health-badge').textContent,'会话有效');
  assert.equal(requests.slice(cancelStart).filter(r=>r.path==='/api/cookie-accounts/test').length,4);
  let completeOld;
  transport=target=>{if(target.pathname==='/api/cookie-accounts/test'){transport=null;return new Promise(resolve=>{completeOld=()=>resolve(new Response(JSON.stringify({results:[{id:'cancel-0',status:'invalid',detail:'old cookie'}]})));});}};
  const beforeSave=w.probeCookieAccounts(0);edit('cookie-0','SAPISID=replaced');await w.saveCookieAccounts();
  completeOld();await beforeSave;
  assert.equal(d.querySelector('.cookie-health-badge').textContent,'未检测');assert.equal(el('cookie-0').value,'');assert.equal(w.consoleForms.dirty(),false);
  console.log('✓ Stopping Cookie batches and isolating late results after replacement');

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
  console.log('12 groups of console DOM checks passed. Layout requires the browser suite.');
 }finally{dom.window.close();}
}
main().catch(error=>{console.error(error);process.exitCode=1;});
