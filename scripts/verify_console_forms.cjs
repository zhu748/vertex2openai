/* Offline DOM interaction regression. npm ci && npm run test:console.
 * jsdom checks behavior, not layout; use verify_console_ui.cjs for rendering. */
const assert=require('node:assert/strict');
const {isDeepStrictEqual}=require('node:util');
const fs=require('node:fs');
const path=require('node:path');
const {JSDOM,VirtualConsole}=require('jsdom');
const root=path.resolve(__dirname,'..');
const tick=()=>new Promise(r=>setTimeout(r,20));
async function main(){
 const errors=[],requests=[],clipboard=[];
 let passed=0;const check=label=>{passed++;console.log('✓ '+label);};
 const virtualConsole=new VirtualConsole();virtualConsole.on('jsdomError',e=>errors.push(e.message));
 const dom=new JSDOM(fs.readFileSync(path.join(root,'app/templates/dashboard.html'),'utf8'),{url:'http://console.test/',runScripts:'dangerously',pretendToBeVisual:true,virtualConsole});
 const w=dom.window,d=w.document;
 const settings={thinking_g3_level:'low',native_thinking_mode:'request',first_reply_timeout_seconds:15,image_size:'4K',image_aspect_ratio:'16:9',sampling_policy:'auto',default_top_p:null,default_temperature:null,default_max_tokens:null,roundrobin:false,proxy_urls:[],cookie_impersonate:'chrome',express_ua_profile:'sdk',retry_max:10,retry_backoff_seconds:5,retry_jitter_seconds:1,request_timeout_seconds:1800,upstream_max_concurrency:32,resumable_enabled:true,resumable_disconnect_grace_seconds:10,resumable_ttl_seconds:1800,resumable_max_streams:200,resumable_max_buffer_mb:64,resumable_max_total_mb:256,keepalive_interval:60,img_compress_enabled:true,img_compress_max_dim:1536,img_compress_max_mb:1.5,img_compress_quality:85,fake_streaming_interval:1,prefill_mode:'smart',express_location:'global'};
 const overrides={},builtins={text:{anti_truncation_mode:'both'}};
 settings.cookie_auto_refresh=true;settings.cookie_refresh_interval_seconds=600;
 let mode='mixed';
 let keys=[{key:'ui-fixture-key',project_id:'project-one',note:'主账号',enabled:true,proxies:[],proxy_fallback:true}];
 let cookies=[{id:'cookie-one',project_id:'project-cookie',note:'备用账号',enabled:true,cookie_configured:true,proxies:[],proxy_fallback:true}];
 let savedCookie='SAPISID=ui-cookie-secret; SID=ui-cookie-sid';
 const cap={family:'3',thinking:{kind:'level',levels:['low','medium','high']},supports_anti_truncation:true,supports_search:true,image_sizes:['1K','2K','4K'],image_aspect_ratios:['1:1','16:9']};
 const summary={connections:1,generations:1,background:0,retry_wait:0,active:1};
 const record={id:'trace-1234567890abcdef',model:'text',protocol:'openai',channel:'express',mode:'stream',phase:'streaming',status:'running',connection:'open',created_at:Date.now()/1000,duration_ms:1000,timings:{},attempts:1,retries:0,finish_reasons:[],usage:{},events:[],logs:[]};
 let scrollPosition=0,transport=null,holdRead=null,requestTotal=1,modelCatalog=['text','image'];
 Object.defineProperty(w,'scrollY',{get:()=>scrollPosition});
 w.AbortSignal=AbortSignal;w.AbortController=AbortController;w.matchMedia=()=>({matches:false});w.scrollTo=({top})=>{scrollPosition=top;};w.HTMLElement.prototype.scrollIntoView=()=>{};w.confirm=()=>true;w.prompt=()=>'console-password';
 w.CSS={escape:s=>s};w.HTMLDialogElement.prototype.showModal=function(){this.open=true;};w.HTMLDialogElement.prototype.close=function(){this.open=false;this.dispatchEvent(new w.Event('close'));};
 const eventSources=[];
 w.EventSource=class {constructor(url){this.url=url;eventSources.push(this);}close(){}};
 const clipboardAPI={writeText:async value=>{clipboard.push(value);}};
 Object.defineProperty(w.navigator,'clipboard',{configurable:true,value:clipboardAPI});
 w.fetch=async(url,options={})=>{
  const target=new URL(url,w.location.href),method=options.method||'GET',body=options.body?JSON.parse(options.body):null;requests.push({path:target.pathname,query:target.search,method,body});
  const intercepted=transport?.(target,options);if(intercepted!==undefined)return intercepted;
  let result={};
  if(method==='DELETE'&&body?.expected_override&&!isDeepStrictEqual(body.expected_override,overrides[target.pathname.split('/').pop()]||{})){
   return new Response(JSON.stringify({error:'已被另一页面修改，请重新加载后检查草稿再保存。'}),{status:409});
  }
  if(body?._expected){
   const baseline=target.pathname==='/api/settings'?settings:(overrides[target.pathname.split('/').pop()]||{});
   const conflict=Object.entries(body._expected).find(([key,old])=>old.present!==Object.hasOwn(baseline,key)||(old.present&&JSON.stringify(baseline[key])!==JSON.stringify(old.value)));
   if(conflict)return new Response(JSON.stringify({error:'已被另一页面修改，请重新加载后检查草稿再保存。',field:conflict[0]}),{status:409});
  }
  if(target.pathname==='/api/settings/runtime')result={mode};
  else if(target.pathname==='/api/settings'){if(body)Object.assign(settings,Object.fromEntries(Object.entries(body).filter(([k])=>k!=='_expected')));result={...settings};}
  else if(target.pathname==='/api/capabilities')result={models:modelCatalog,capabilities:Object.fromEntries(modelCatalog.map(m=>[m,{...cap,is_image:m==='image'}])),overrides:{text:{...builtins.text,...overrides.text},...Object.fromEntries(Object.entries(overrides).filter(([m])=>m!=='text'))},saved_overrides:overrides,builtin_overrides:builtins};
  else if(target.pathname.startsWith('/api/model-overrides/')){const model=target.pathname.split('/').pop();if(method==='DELETE')delete overrides[model];else{const current={...overrides[model]};for(const key of body._inherit||[])delete current[key];Object.assign(current,Object.fromEntries(Object.entries(body).filter(([k])=>!['_inherit','_expected'].includes(k))));overrides[model]=current;}result={override:{...builtins[model],...overrides[model]},saved_override:{...overrides[model]}};}
  else if(target.pathname==='/api/express-keys'){if(body)keys=body.keys;result={keys};}
  else if(target.pathname==='/api/cookie-accounts'){if(body)cookies=body.accounts;result={accounts:cookies};}
  else if(target.pathname==='/api/cookie-accounts/reveal')result={id:body.id,cookie:savedCookie};
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
  else if(target.pathname==='/api/settings/mode'){mode=body.mode;result={status:'success',mode};}
  else throw Error('Unexpected endpoint '+target.pathname);
  const response=new Response(JSON.stringify(result),{status:200,headers:{'content-type':'application/json'}});
  return (method==='GET'&&holdRead?.(target.pathname,response))||response;
 };
 function holdConfigRead(endpoint){
  let release;
  holdRead=(pathname,response)=>{
   if(pathname!==endpoint)return;
   holdRead=null;
   return new Promise(resolve=>{release=()=>resolve(response);});
  };
  return ()=>release();
 }
 const el=id=>d.getElementById(id);
 function edit(id,value){const input=el(id);if(input.type==='checkbox')input.checked=value;else input.value=value;input.dispatchEvent(new w.Event('input',{bubbles:true}));input.dispatchEvent(new w.Event('change',{bubbles:true}));}
 async function save(id){el(id).click();await tick();}
 try{
  for(const name of [...d.querySelectorAll('script[src]')].map(script=>new URL(script.src).pathname).filter(src=>src.startsWith('/static/')&&src.endsWith('.js')).map(src=>path.basename(src)))require('node:vm').runInContext(fs.readFileSync(path.join(root,'app/static',name),'utf8'),dom.getInternalVMContext(),{filename:name});
  await tick();
  assert.deepEqual(errors,[]);
  assert.equal(w.consoleForms.dirty(),false);
  assert.equal(el('recent-rate').textContent,'50.0%');
  assert.equal(el('runtime-save-bar').closest('[role=tabpanel]').id,'view-runtime');
  assert.equal(el('roundrobin'),null);
  assert.equal(el('keys-list').querySelector('details').open,false);
  assert.equal(el('key-0').type,'password');
  check('Navigation, initial clean state, account masking/collapse, recent metrics');

  w.switchTab('logs');
  const logSource=eventSources.find(source=>source.url==='/stream-logs');
  const logwin=el('logwin');let logScrollReads=0;
  Object.defineProperty(logwin,'scrollHeight',{configurable:true,get(){logScrollReads++;return logwin.childElementCount*20;}});
  for(let i=0;i<700;i++)logSource.onmessage({data:`burst ${i} <img src=x onerror=alert(1)>`});
  assert.equal(logwin.childElementCount,0);
  await new Promise(resolve=>setTimeout(resolve,80));
  assert.equal(logwin.childElementCount,500);
  assert(logwin.firstElementChild.textContent.startsWith('burst 200 '));
  assert(logwin.lastElementChild.textContent.startsWith('burst 699 '));
  assert.equal(logwin.querySelector('img'),null);assert(logScrollReads<=2);
  edit('log-level','error');logSource.onmessage({data:'ERROR batch-visible'});
  await new Promise(resolve=>setTimeout(resolve,80));
  assert.equal(logwin.querySelectorAll('.log-line:not([hidden])').length,1);
  logSource.onmessage({data:'must not return after clear'});el('log-clear').click();
  await new Promise(resolve=>setTimeout(resolve,80));assert.equal(logwin.childElementCount,0);
  edit('log-level','all');delete logwin.scrollHeight;
  check('Burst logs batch DOM updates, retain newest 500, preserve filters/escaping, clear pending messages');

  w.switchTab('channel');
  let cookieRow=el('cookies-list').firstElementChild;
  const cookiePreview=()=>el('cookies-list').querySelector('.cookie-secret-preview');
  assert.equal(cookiePreview().hidden,true);assert.equal(cookiePreview().value,'');
  assert.equal(el('cookie-0').value,'');assert(!d.body.innerHTML.includes(savedCookie));
  cookieRow.querySelector('.c-cookie-reveal').click();await tick();
  assert.equal(cookiePreview().value,savedCookie);assert.equal(cookiePreview().readOnly,true);
  assert.equal(cookiePreview().hidden,false);assert.equal(w.consoleForms.dirty(),false);
  assert.deepEqual(requests.filter(r=>r.path==='/api/cookie-accounts/reveal').at(-1).body,{id:'cookie-one',password:'console-password'});
  cookieRow.querySelector('.c-cookie-reveal').click();
  assert.equal(cookiePreview().hidden,true);assert.equal(cookiePreview().value,'');
  savedCookie='SAPISID=ui-cookie-refreshed; SID=ui-cookie-sid';
  cookieRow.querySelector('.c-cookie-copy').click();await tick();
  assert.equal(clipboard.at(-1),savedCookie);assert.equal(cookiePreview().hidden,true);
  assert.equal(el('cookie-0').value,'');assert.equal(w.consoleForms.dirty(),false);
  Object.defineProperty(w.navigator,'clipboard',{configurable:true,value:undefined});
  d.execCommand=command=>{assert.equal(command,'copy');clipboard.push(d.activeElement.value);return true;};
  cookieRow.querySelector('.c-cookie-copy').click();await tick();
  assert.equal(clipboard.at(-1),savedCookie);assert.equal(cookiePreview().hidden,true);
  d.execCommand=()=>false;
  cookieRow.querySelector('.c-cookie-copy').click();await tick();
  assert.equal(cookiePreview().hidden,false);assert.equal(cookiePreview().selectionStart,0);
  assert.equal(cookiePreview().selectionEnd,savedCookie.length);
  assert(el('toast').textContent.includes('手动复制'));assert.equal(w.consoleForms.dirty(),false);
  cookieRow.querySelector('.c-cookie-reveal').click();
  Object.defineProperty(w.navigator,'clipboard',{configurable:true,value:clipboardAPI});
  transport=target=>{if(target.pathname==='/api/cookie-accounts/reveal'){transport=null;return new Response('{}',{status:401});}};
  cookieRow.querySelector('.c-cookie-reveal').click();await tick();
  assert.equal(cookiePreview().value,'');assert(el('toast').textContent.includes('登录已过期'));
  let resolveReveal;
  transport=target=>{if(target.pathname==='/api/cookie-accounts/reveal'){transport=null;return new Promise(resolve=>{resolveReveal=resolve;});}};
  cookieRow.querySelector('.c-cookie-copy').click();await tick();
  const copiesBeforeRemoval=clipboard.length;
  w.removeCookieAccount(0);
  resolveReveal(new Response(JSON.stringify({id:'cookie-one',cookie:savedCookie}),{status:200}));await tick();
  assert.equal(clipboard.length,copiesBeforeRemoval);
  w.consoleForms.discard('cookies');assert.equal(w.consoleForms.dirty(),false);
  assert.equal(cookiePreview().hidden,true);assert.equal(cookiePreview().value,'');
  check('Saved Cookie reveal/hide/copy, HTTP clipboard fallback, session errors and stale-row isolation');

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
  transport=target=>{if(target.pathname==='/api/settings/mode'){transport=null;mode='api_key';return new Response(JSON.stringify({error:'conflict',mode}),{status:409});}};
  d.querySelector('input[name=mode][value=web_proxy]').checked=true;await w.updateMode('web_proxy');
  modeVisible('api_key');assert.equal(d.querySelector('input[name=mode]:checked').value,'api_key');
  transport=target=>{if(target.pathname==='/api/settings/mode'){transport=null;return new Response('{}',{status:200});}};
  d.querySelector('input[name=mode][value=mixed]').checked=true;await w.updateMode('mixed');
  modeVisible('api_key');assert.equal(d.querySelector('input[name=mode]:checked').value,'api_key');
  assert.equal(requests.filter(r=>r.method==='POST'&&['/api/express-keys','/api/cookie-accounts'].includes(r.path)).length,credentialWrites);
  w.consoleForms.discard('keys');w.consoleForms.discard('cookies');assert.equal(w.consoleForms.dirty(),false);
  check('Three channel modes show matching credential lists, preserve drafts and roll back failed saves');

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
  check('Section deep links, mobile navigation, page scroll restoration and skip link');

  w.switchTab('params');edit('model-sel','text');edit('first_reply_timeout_seconds','35');await save('btn-scope-save');
  const first=requests.filter(r=>r.path==='/api/model-overrides/text').at(-1).body;
  assert.deepEqual(first,{first_reply_timeout_seconds:35,_expected:{first_reply_timeout_seconds:{present:false}}});
  assert.equal(el('source-thinking_g3_level').textContent.includes('继承全局'),true);
  w.setFieldInheritance('first_reply_timeout_seconds',true);await save('btn-scope-save');
  assert.deepEqual(requests.filter(r=>r.path==='/api/model-overrides/text').at(-1).body,{_inherit:['first_reply_timeout_seconds'],_expected:{first_reply_timeout_seconds:{present:true,value:35}}});
  edit('first_reply_timeout_seconds','24');edit('model-sel','image');edit('model-sel','text');
  assert.equal(el('first_reply_timeout_seconds').value,'24');
  edit('model-sel','__global__');edit('thinking_g3_level','high');await save('btn-scope-save');
  edit('model-sel','text');assert.equal(el('first_reply_timeout_seconds').value,'24');
  assert.equal(el('thinking_g3_level').value,'high');
  assert.equal(el('source-thinking_g3_level').textContent.includes('继承全局'),true);
  w.discardModelDraft();assert.equal(el('first_reply_timeout_seconds').value,'15');
  assert.equal(el('image-controls').hidden,true);
  assert.equal(el('thinking_g3_level').querySelector('[value=minimal]').disabled,true);
  check('Sparse model save, per-field inheritance, capability options and model drafts');

  {
   const old=overrides.text;edit('first_reply_timeout_seconds','29');
   overrides.text={...old,default_top_p:.65};  // another editor pins an unrelated field
   await save('btn-scope-save');
   assert.equal(el('default_top_p').value,'0.65');assert.match(el('source-default_top_p').textContent,/模型专属/);
   assert.match(el('source-anti_truncation_mode').textContent,/继承内置/);
   edit('default_top_p','0.7');await save('btn-scope-save');
   assert.deepEqual(requests.filter(r=>r.path==='/api/model-overrides/text').at(-1).body,
     {default_top_p:.7,_expected:{default_top_p:{present:true,value:.65}}});
   assert.equal(overrides.text.default_top_p,.7);
   edit('first_reply_timeout_seconds','31');delete overrides.text.default_top_p;
   await save('btn-scope-save');assert.match(el('source-default_top_p').textContent,/继承全局/);
   overrides.text=old;await w.loadParams();
   check('A disjoint model save adopts server-owned additions and removals without pinning built-ins or inventing a conflict');
  }

  edit('first_reply_timeout_seconds','25');edit('first_reply_timeout_seconds','15');
  assert.equal(w.consoleForms.dirty(),false);
  w.setFieldInheritance('first_reply_timeout_seconds',false);assert.equal(w.consoleForms.dirty(),true);
  edit('model-sel','image');edit('model-sel','text');
  await save('btn-scope-save');
  assert.deepEqual(requests.filter(r=>r.path==='/api/model-overrides/text').at(-1).body,{first_reply_timeout_seconds:15,_expected:{first_reply_timeout_seconds:{present:false}}});
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
  assert.deepEqual(requests.filter(r=>r.path==='/api/model-overrides/image').at(-1).body,{default_max_tokens:512,_expected:{default_max_tokens:{present:false}}});
  builtins.text.default_top_p=null;settings.default_top_p=0.8;await w.loadParams();edit('model-sel','text');
  assert.equal(el('default_top_p').value,'');edit('default_top_p','0.5');await save('btn-scope-save');
  w.setFieldInheritance('default_top_p',true);assert.equal(el('default_top_p').value,'');await save('btn-scope-save');
  delete builtins.text.default_top_p;delete overrides.image;settings.image_aspect_ratio='16:9';settings.default_top_p=null;
  await w.loadParams();
  check('Reverted edits, deliberate pins, obsolete override cleanup and exact inherited values');

  overrides.image={default_max_tokens:256};await w.loadParams();edit('model-sel','image');
  edit('first_reply_timeout_seconds','45');overrides.image.default_top_p=0.4;
  await save('btn-scope-clear');
  assert.deepEqual(requests.filter(r=>r.path==='/api/model-overrides/image').at(-1).body,{expected_override:{default_max_tokens:256}});
  assert.equal(el('first_reply_timeout_seconds').value,'45');assert.equal(w.consoleForms.dirty(),true);
  assert.equal(overrides.image.default_top_p,0.4);assert.match(el('toast').textContent,/另一页面/);
  await w.loadParams();await save('btn-scope-clear');assert.equal(overrides.image,undefined);
  assert.equal(w.consoleForms.dirty(),false);
  check('Full model reset sends the saved baseline, preserves a stale draft and permits an informed fresh reset');

  const lastModelBody=()=>requests.filter(r=>r.path==='/api/model-overrides/image'&&r.method==='POST').at(-1).body;
  edit('default_temperature','0.5');
  overrides.image={default_temperature:0.7,default_max_tokens:777};
  edit('model-sel','text');await w.loadParams();edit('model-sel','image');await w.loadParams();
  assert.equal(el('default_temperature').value,'0.5');assert.equal(el('default_max_tokens').value,'777');
  await save('btn-scope-save');
  assert.equal(overrides.image.default_temperature,0.7);
  assert.deepEqual(lastModelBody(),{default_temperature:0.5,_expected:{default_temperature:{present:false}}});
  assert.match(el('scope-draft-state').textContent,/另一页面/);
  edit('default_temperature','0.7');assert.equal(w.consoleForms.dirty(),false);
  edit('default_temperature','0.61');await save('btn-scope-save');
  assert.deepEqual(lastModelBody(),{default_temperature:0.61,_expected:{default_temperature:{present:true,value:0.7}}});

  w.setFieldInheritance('default_temperature',true);overrides.image.default_temperature=0.84;
  await w.loadParams();edit('model-sel','text');edit('model-sel','image');
  await save('btn-scope-save');
  assert.deepEqual(lastModelBody(),{_inherit:['default_temperature'],_expected:{default_temperature:{present:true,value:0.61}}});
  assert.equal(overrides.image.default_temperature,0.84);
  // Re-pinning the inherited (null) value does not bless another editor's change.
  w.setFieldInheritance('default_temperature',false);await w.loadParams();await save('btn-scope-save');
  assert.deepEqual(lastModelBody(),{default_temperature:null,_expected:{default_temperature:{present:true,value:0.61}}});
  w.discardModelDraft();assert.equal(el('default_temperature').value,'0.84');
  w.setFieldInheritance('default_temperature',true);delete overrides.image.default_temperature;
  await w.loadParams();assert.equal(w.consoleForms.dirty(),false);

  w.setFieldInheritance('default_top_p',false);settings.default_top_p=0.8;
  await w.loadParams();assert.equal(el('default_top_p').value,'');
  overrides.image.default_top_p=0.6;await w.loadParams();await save('btn-scope-save');
  assert.deepEqual(lastModelBody(),{default_top_p:null,_expected:{default_top_p:{present:false}}});
  overrides.image.default_top_p=null;await w.loadParams();assert.equal(w.consoleForms.dirty(),false);
  check('Sparse overrides freeze presence and value through reload, scope switching and pin/inherit; identical writes and removals converge');

  edit('model-sel','__global__');const origTimeout=settings.first_reply_timeout_seconds,origSize=settings.image_size;
  edit('first_reply_timeout_seconds','22');settings.first_reply_timeout_seconds=33;settings.image_size='1K';
  await w.loadParams();edit('model-sel','text');edit('model-sel','__global__');await w.loadParams();
  assert.equal(el('first_reply_timeout_seconds').value,'22');assert.equal(el('image_size').value,'1K');
  await save('btn-scope-save');
  const modelGlobalBody=()=>requests.filter(r=>r.path==='/api/settings'&&r.method==='POST').at(-1).body;
  assert.equal(settings.first_reply_timeout_seconds,33);
  assert.deepEqual(modelGlobalBody(),{first_reply_timeout_seconds:22,_expected:{first_reply_timeout_seconds:{present:true,value:origTimeout}}});
  assert.match(el('scope-draft-state').textContent,/另一页面/);
  w.discardModelDraft();assert.equal(el('first_reply_timeout_seconds').value,'33');
  edit('first_reply_timeout_seconds','22');await save('btn-scope-save');assert.equal(settings.first_reply_timeout_seconds,22);
  edit('first_reply_timeout_seconds','44');settings.first_reply_timeout_seconds=44;
  await w.loadParams();assert.equal(w.consoleForms.dirty(),false);
  settings.first_reply_timeout_seconds=origTimeout;settings.image_size=origSize;settings.default_top_p=null;delete overrides.image;
  await w.loadParams();
  check('Global model defaults freeze only edited fields across reloads, support discard/re-edit and independent convergence');

  let releaseModelSettings;
  transport=target=>{if(target.pathname==='/api/settings'){transport=null;return new Promise(resolve=>{releaseModelSettings=resolve;});}};
  const loadingModels=w.loadParams();edit('model-sel','image');edit('default_max_tokens','321');
  releaseModelSettings(new Response(JSON.stringify(settings)));await loadingModels;
  assert.equal(el('model-sel').value,'image');assert.equal(el('default_max_tokens').value,'321');
  w.discardModelDraft();
  check('Reload applies to the currently selected model without erasing edits made in flight');



  edit('model-sel','__global__');edit('default_top_p','2');await save('btn-scope-save');
  assert.equal(el('default_top_p').getAttribute('aria-invalid'),'true');
  assert(el('default_top_p-error'));
  edit('default_top_p','');assert.equal(el('default_top_p-error'),null);w.discardModelDraft();
  w.switchTab('runtime');const help=d.querySelector('[aria-controls=h_ccf]');help.click();assert.equal(el('h_ccf').hidden,false);help.click();assert.equal(el('h_ccf').hidden,true);
  check('Persistent field validation and accessible help disclosure');
  edit('retry_max','12');assert.equal(w.consoleForms.dirty(),true);
  await w.loadParams();assert.equal(el('retry_max').value,'12');
  w.consoleForms.discard('runtime');assert.equal(el('retry_max').value,'10');
  {
   const saveRuntime=async()=>{d.querySelector('button[onclick*="saveSettings"]').click();await tick();};
   const writes=()=>requests.filter(r=>r.path==='/api/settings'&&r.method==='POST');
   edit('retry_max','12');
   for(const bad of [{},{status:'success'},{retry_max:11},null,[]]){
    const before=writes().length;
    transport=(url,options)=>url.pathname==='/api/settings'&&options.method==='POST'?new Response(JSON.stringify(bad)):undefined;
    await saveRuntime();
    assert.equal(writes().length,before+1);assert.equal(w.consoleForms.dirty(),true);
    assert.deepEqual(writes().at(-1).body._expected.retry_max,{present:true,value:10});
    assert.equal(el('retry_max').value,'12');assert.equal(settings.retry_max,10);
    assert(el('toast').textContent.includes('核对草稿'));
   }
   const falsy={cookie_auto_refresh:false,retry_jitter_seconds:0,default_top_p:null,keepalive_url:''};
   transport=(url,options)=>url.pathname==='/api/settings'&&options.method==='POST'?new Response(JSON.stringify(falsy)):undefined;
   let observed=false;assert.equal(await w.postSettings(falsy,()=>{observed=true;}),true);assert(observed);
   transport=null;w.consoleForms.discard('runtime');assert.equal(el('retry_max').value,'10');
   check('Unconfirmed settings receipts keep edits and the last CAS baseline; explicit false/zero/null/empty values are accepted');
  }
  edit('retry_max','13');d.querySelector('button[onclick*="saveSettings"]').click();await tick();
  assert.equal(settings.retry_max,13);assert.equal(w.consoleForms.dirty(),false);
  check('Runtime draft reload preservation, save and undo');

  {
   const original={express_location:settings.express_location,keepalive_url:settings.keepalive_url};
   const saveRuntime=async()=>{d.querySelector('button[onclick*="saveSettings"]').click();await tick();};
   const lastPatch=()=>requests.filter(r=>r.path==='/api/settings'&&r.method==='POST').at(-1).body;
   settings.express_location='us-west2';
   settings.keepalive_url='  https://maintenance.invalid/healthz  ';
   await w.loadParams();
   assert.equal(el('express_location').value,'us-west2');
   assert.match(el('express_location').selectedOptions[0].textContent,/us-west2/);
   assert.equal(el('keepalive_url').value,'https://maintenance.invalid/healthz');
   assert.equal(w.consoleForms.dirty(),false);
   const oldRetry=settings.retry_max;
   edit('retry_max',String(oldRetry+1));await saveRuntime();
   assert.deepEqual(lastPatch(),{retry_max:oldRetry+1,_expected:{retry_max:{present:true,value:oldRetry}}});
   assert.equal(settings.express_location,'us-west2');
   assert.equal(settings.keepalive_url,'  https://maintenance.invalid/healthz  ');
   // Selecting the empty route intentionally is still a real edit.
   edit('express_location','');await saveRuntime();
   assert.deepEqual(lastPatch(),{express_location:'',_expected:{express_location:{present:true,value:'us-west2'}}});
   assert.equal(settings.express_location,'');
   assert.equal(w.consoleForms.dirty(),false);
   Object.assign(settings,original);await w.loadParams();
   check('Runtime shows saved unlisted regions, never writes untouched rendered values, and can explicitly clear a pin');
  }

  assert.equal(el('cookie_auto_refresh').checked,true);
  edit('cookie_auto_refresh',true);edit('cookie_refresh_interval_seconds','900');
  assert.equal(w.consoleForms.dirty(),true);
  d.querySelector('button[onclick*="saveSettings"]').click();await tick();
  assert.equal(settings.cookie_auto_refresh,true);assert.equal(settings.cookie_refresh_interval_seconds,900);
  assert.equal(w.consoleForms.dirty(),false);
  await w.loadParams();assert.equal(el('cookie_auto_refresh').checked,true);
  assert.equal(el('cookie_refresh_interval_seconds').value,'900');
  edit('cookie_refresh_interval_seconds','299');
  const runtimeWrites=()=>requests.filter(r=>r.path==='/api/settings'&&r.method==='POST').length;
  const beforeInvalid=runtimeWrites();d.querySelector('button[onclick*="saveSettings"]').click();await tick();
  assert.equal(runtimeWrites(),beforeInvalid);assert.equal(el('cookie_refresh_interval_seconds').getAttribute('aria-invalid'),'true');
  w.consoleForms.discard('runtime');assert.equal(el('cookie_refresh_interval_seconds').value,'900');
  check('Cookie refresh controls save, reload, validate limits and undo');

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
  check('Manual Cookie refresh works with auto off, escapes results, prevents duplicates and preserves drafts');

  w.switchTab('channel');w.consoleForms.openAccount('key',0);edit('key-note-0','新备注');
  assert.equal(w.consoleForms.dirty(),true);assert.equal(d.querySelector('.account-name').textContent,'新备注');
  w.consoleForms.discard('keys');assert.equal(el('key-note-0').value,'主账号');
  edit('keys-search','missing');assert.equal(el('keys-list').firstElementChild.hidden,true);
  edit('keys-search','');assert.equal(el('keys-list').firstElementChild.hidden,false);
  w.addKeyRow();assert.equal(el('keys-list').lastElementChild.querySelector('details').open,true);
  w.consoleForms.discard('keys');assert.equal(el('keys-list').children.length,1);
  edit('credential-strategy','roundrobin');await tick();assert.equal(settings.roundrobin,true);assert.equal(w.consoleForms.dirty(),false);
  edit('proxy-input','http://proxy.test:80');await w.saveProxy();assert.equal(w.consoleForms.dirty(),false);
  check('Credential drafts, filters, add/undo, immediate strategy and proxy save state');

  {
   const original=keys, posts=()=>requests.filter(r=>r.path==='/api/express-keys'&&r.method==='POST');
   keys=[{key:'fixture-A'},{key:'fixture-B'}];await w.loadExpressKeys();
   const before=posts().length;edit('key-0','   ');edit('key-note-1','keep this draft');
   await w.saveExpressKeys();
   assert.equal(posts().length,before);assert.deepEqual(keys.map(k=>k.key),['fixture-A','fixture-B']);
   assert.equal(el('key-note-1').value,'keep this draft');assert.equal(w.consoleForms.dirty(),true);
   assert.equal(el('key-0').getAttribute('aria-invalid'),'true');assert(el('key-0').closest('details').open);
   edit('key-0','replacement');w.addKeyRow();  // an unused placeholder can be ignored
   await w.saveExpressKeys();assert.deepEqual(keys.map(k=>k.key),['replacement','fixture-B']);
   assert.equal(keys[1].note,'keep this draft');
   w.addKeyRow();edit('key-note-2','unfinished');await w.saveExpressKeys();
   assert.equal(posts().length,before+1);assert.equal(el('key-note-2').value,'unfinished');
   w.removeKeyRow(2);w.removeKeyRow(0);await w.saveExpressKeys();
   assert.deepEqual(keys.map(k=>k.key),['fixture-B']);  // explicit deletion is still supported
   let confirms=0;w.confirm=()=>{confirms++;return false;};w.removeKeyRow(0);await w.saveExpressKeys();
   assert.equal(confirms,1);assert.equal(posts().length,before+2);w.confirm=()=>true;
   keys=original;await w.loadExpressKeys();
   check('Clearing a stored Key or leaving a partial new row preserves drafts without a write; explicit removal and pristine placeholders still work');
  }

  {
   const original=keys;
   const base={key:'fixture-A',project_id:'old-project',project_source:'auto',detected_at:123,note:'keep note',proxies:['http://local.test:80']};
   const sibling={key:'fixture-B',project_id:'sibling',project_source:'auto',detected_at:234};
   keys=[sibling,base];await w.loadExpressKeys();
   edit('key-1','new-key');
   assert.equal(el('project-1').value,'');assert.equal(el('key-1').closest('[data-i]').querySelector('[data-detected]').hidden,true);
   w.removeKeyRow(0); // saved origin must follow the draft, never its index
   edit('key-0',base.key);
   assert.equal(el('project-0').value,base.project_id);
   assert.equal(w.collectKeys()[0].detected_at,123);assert.equal(w.collectKeys()[0].project_source,'auto');
   edit('key-0','new-key');w.addKeyRow();await w.saveExpressKeys();
   assert.deepEqual([keys[0].key,keys[0].project_id,keys[0].project_source,keys[0].detected_at],['new-key','','',0]);
   assert.equal(keys[0].note,base.note);assert.deepEqual(keys[0].proxies,base.proxies);
   keys=[base,sibling];await w.loadExpressKeys();edit('key-0','replacement');
   edit('project-0',base.project_id); // deliberately assigning even the same project to the NEW key is manual
   await w.saveExpressKeys();
   assert.deepEqual([keys[0].project_id,keys[0].project_source,keys[0].detected_at],[base.project_id,'manual',0]);
   assert.equal(keys[1].detected_at,234);
   keys=original;await w.loadExpressKeys();
   check('Replacing a key clears only its detected project; rerenders, key undo and an explicit manual project preserve intent');
  }

  {
   for(const [endpoint,field,note,load,save,list,readSaved] of [
    ['/api/express-keys','keys','key-note-0',()=>w.loadExpressKeys(),()=>w.saveExpressKeys(),'keys-list',()=>keys],
    ['/api/cookie-accounts','accounts','cookie-note-0',()=>w.loadCookieAccounts(),()=>w.saveCookieAccounts(),'cookies-list',()=>cookies]
   ]){
    const initial=structuredClone(readSaved());
    const answer=value=>{transport=target=>target.pathname===endpoint?new Response(JSON.stringify(value)):undefined;};
    answer({[field]:initial,revision:'last-good'});await load();
    edit(note,'keep pending draft');
    for(const invalid of [{revision:'unverified'}, {[field]:{}, revision:'wrong'}, {[field]:[null], revision:'wrong-row'},
                          {[field]:initial}, {[field]:initial,revision:null}, {[field]:initial,revision:'  '}, {[field]:initial,revision:42}]){
      answer(invalid);await save();assert(el('toast').textContent.includes('草稿已保留'));
      assert.equal(el(note).value,'keep pending draft');assert.equal(w.consoleForms.dirty(),true);
      assert.equal(requests.filter(r=>r.path===endpoint&&r.method==='POST').at(-1).body.revision,'last-good');
      await load();assert.equal(el(note).value,'keep pending draft');assert.equal(w.consoleForms.dirty(),true);
    }
    answer({[field]:[],revision:'empty-confirmed'});await load();assert.equal(el(list).children.length,0);
    answer({[field]:initial,revision:'restored'});await load();transport=null;
   }
   check('Malformed credential receipts cannot drop a known revision or the draft; initial legacy responses and explicit empty lists remain valid');
  }

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
  check('Persisted probe results rebase saved state without swallowing credential drafts');

  transport=target=>{if(target.pathname==='/api/express-keys'){transport=null;return new Response(JSON.stringify({keys,revision:'r1'}));}};
  await w.loadExpressKeys();
  let latest=keys.map(rec=>({...rec,project_id:'revision-project',project_source:'auto',detected_at:123}));
  w.mergeProbeResults([{key:latest[0].key,ok:true,project_id:'revision-project'}],latest,'r2');
  edit('key-note-0','保存探测后的修改');
  transport=(target,options)=>{if(target.pathname==='/api/express-keys'){
    transport=null;const body=JSON.parse(options.body);assert.equal(body.revision,'r2');keys=body.keys;
    return new Response(JSON.stringify({keys,revision:'r3'}));
  }};
  await w.saveExpressKeys();
  edit('key-note-0','冲突时保留的草稿');
  latest=keys.map(rec=>({...rec,note:'另一标签页的备注',project_id:'remote-probed-project',project_source:'auto',detected_at:456}));
  for(let i=0;i<2;i++)w.mergeProbeResults([{key:latest[0].key,ok:true,project_id:'remote-probed-project'}],latest,'r4');
  transport=(target,options)=>{if(target.pathname==='/api/express-keys'){
    transport=null;assert.equal(JSON.parse(options.body).revision,'r3');
    return new Response(JSON.stringify({error:'服务器凭证列表已更新，当前草稿已保留。'}),{status:409});
  }};
  await w.saveExpressKeys();
  assert.equal(el('key-note-0').value,'冲突时保留的草稿');assert.equal(w.consoleForms.dirty(),true);
  assert(el('channel-load-error').textContent.includes('草稿已保留'));
  transport=target=>{if(target.pathname==='/api/express-keys'){transport=null;return new Response(JSON.stringify({keys,revision:'r4'}));}};
  await w.loadExpressKeys();
  check('Probe revisions rebase expected metadata only; repeated probes cannot bypass conflicts or discard drafts');

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
  check('Closed and cancelled proxy probes cannot overwrite newer results or import cancelled passes');

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
  check('Retained-page clamping, stale response isolation and paused retry messages');
  const ids=[...d.querySelectorAll('[id]')].map(e=>e.id);assert.equal(new Set(ids).size,ids.length);
  assert.deepEqual(errors,[]);
  check('Stable request rows, pause/resume, pagination and unique DOM IDs');
  await w.loadParams();w.switchTab('runtime');
  const retryBefore=Number(el('retry_max').value),keepaliveBefore=Number(el('keepalive_interval').value);
  settings.retry_max=retryBefore+1;
  edit('keepalive_interval',String(keepaliveBefore+5));d.querySelector('button[onclick*="saveSettings"]').click();await tick();
  assert.equal(settings.retry_max,retryBefore+1);
  const sparse=requests.filter(r=>r.path==='/api/settings'&&r.method==='POST').at(-1).body;
  assert.deepEqual(Object.keys(sparse).sort(),['_expected','keepalive_interval']);
  edit('retry_max',String(retryBefore+2));d.querySelector('button[onclick*="saveSettings"]').click();await tick();
  assert.equal(settings.retry_max,retryBefore+1);assert(el('toast').textContent.includes('另一页面'));
  assert.equal(w.consoleForms.dirty(),true);w.consoleForms.discard('runtime');await w.loadParams();
  w.switchTab('params');edit('model-sel','__global__');
  const oldThinking=el('thinking_g3_level').value;
  settings.thinking_g3_level=oldThinking==='high'?'low':'high';
  edit('inject_system_instruction','concurrent global edit');await save('btn-scope-save');
  assert.equal(settings.thinking_g3_level,oldThinking==='high'?'low':'high');
  edit('thinking_g3_level','medium');await save('btn-scope-save');
  assert(el('toast').textContent.includes('另一页面'));w.discardModelDraft();
  check('Cross-tab sparse runtime/global saves and same-field conflict preservation');
  await w.loadParams();
  const lastSettings=()=>requests.filter(r=>r.path==='/api/settings'&&r.method==='POST').at(-1).body;
  const clickRuntime=async()=>{d.querySelector('button[onclick*="saveSettings"]').click();await tick();};
  const initialRetry=settings.retry_max,initialInterval=settings.keepalive_interval;
  edit('keepalive_interval',String(initialInterval+3));
  settings.retry_max=initialRetry+1;
  await w.loadParams();
  assert.equal(el('retry_max').value,String(initialRetry+1));
  assert.equal(el('keepalive_interval').value,String(initialInterval+3));
  await clickRuntime();
  assert.deepEqual(lastSettings(),{keepalive_interval:initialInterval+3,_expected:{keepalive_interval:{present:true,value:initialInterval}}});
  const local=initialRetry+4,remote=initialRetry+2;
  edit('retry_max',String(local));settings.retry_max=remote;
  for(let i=0;i<2;i++)await w.loadParams();
  await clickRuntime();
  assert.equal(settings.retry_max,remote);assert.equal(el('retry_max').value,String(local));
  assert.deepEqual(lastSettings()._expected.retry_max,{present:true,value:initialRetry+1});
  assert(d.querySelector('[data-draft-group=runtime] .draft-toolbar').textContent.includes('另一页面'));
  edit('retry_max',String(remote));edit('retry_max',String(local));await clickRuntime();
  assert.equal(settings.retry_max,local);
  edit('retry_max',String(local+1));settings.retry_max=local+1;await w.loadParams();
  assert.equal(w.consoleForms.dirty(),false);
  check('Runtime reload merges only dirty fields; repeated reload retains the original comparison; matching edits converge');

  for(const [id,key,group,local,remote,save] of [
    ['proxy-input','proxy_urls','proxy','http://local.invalid:81','http://remote.invalid:82',()=>w.saveProxy()],
    ['cookie-impersonate','cookie_impersonate','fingerprint','firefox','edge',()=>w.saveFingerprint()],
    ['express-ua-profile','express_ua_profile','express-fingerprint','node','gcloud',()=>w.saveExpressUa()]
  ]){
    const before=settings[key],asWire=v=>key==='proxy_urls'?[v]:v;
    edit(id,local);settings[key]=asWire(remote);await w.loadParams();await w.loadParams();
    await save();
    assert.deepEqual(lastSettings()._expected[key],{present:true,value:before});assert.deepEqual(settings[key],asWire(remote));
    assert.equal(el(id).value,local);w.consoleForms.discard(group);assert.equal(el(id).value,remote);
    edit(id,local);await save();assert.deepEqual(settings[key],asWire(local));
  }
  settings.express_ua_custom='server-before';await w.loadParams();
  edit('express-ua-profile','custom');settings.express_ua_custom='server-after';await w.loadParams();
  assert.equal(el('express-ua-custom').value,'server-after');await w.saveExpressUa();
  assert.deepEqual(lastSettings(),{express_ua_profile:'custom',_expected:{express_ua_profile:{present:true,value:'node'}}});
  check('Proxy and both fingerprint forms preserve same-field conflicts and save only changed settings');

  const proxyAtOpen=settings.proxy_urls;
  w.consoleChannels.open('global');w.deleteProxy(0);
  settings.proxy_urls=['http://another.invalid:83'];await w.loadParams();
  assert.equal(await w.applyProxyEditor(true),false);
  assert.deepEqual(lastSettings()._expected.proxy_urls,{present:true,value:proxyAtOpen});
  assert.equal(el('proxy-dialog').open,true);assert(el('proxy-editor-error').textContent.includes('另一页面'));
  assert.equal(await w.applyProxyEditor(false),true);assert.equal(el('proxy-input').value,'');
  await w.saveProxy();
  assert.deepEqual(lastSettings()._expected.proxy_urls,{present:true,value:proxyAtOpen});
  assert.deepEqual(settings.proxy_urls,['http://another.invalid:83']);
  w.consoleForms.discard('proxy');assert.equal(el('proxy-input').value,'http://another.invalid:83');
  check('A global proxy dialog retains its opening comparison for direct saves and applying to the page draft');

  let resolveSettings;
  transport=target=>{if(target.pathname==='/api/settings'){transport=null;return new Promise(resolve=>{resolveSettings=resolve;});}};
  const loading=w.loadParams();edit('retry_max',String(settings.retry_max+2));
  resolveSettings(new Response(JSON.stringify(settings)));await loading;
  assert.equal(el('retry_max').value,String(settings.retry_max+2));w.consoleForms.discard('runtime');
  check('Typing while a reload is in flight does not drop the draft');

  // Hold completed writes at the transport boundary to exercise draft changes and
  // reload ordering independently of the normal busy-fieldset interaction lock.
  function deferWrite(endpoint, method='POST', receipt=null){
   let release,body;
   transport=(target,options)=>{if(target.pathname===endpoint&&options.method===method){
    transport=null;body=JSON.parse(options.body);
    let result, status=200;
    if(receipt)({result,status=200}=receipt(body));
    else if(endpoint==='/api/settings'){
     Object.assign(settings,Object.fromEntries(Object.entries(body).filter(([key])=>key!=='_expected')));
     result={...settings};
    }else{
     const model=endpoint.split('/').at(-1), own={...overrides[model]};
     for(const key of body._inherit||[])delete own[key];
     Object.assign(own,Object.fromEntries(Object.entries(body).filter(([key])=>!['_expected','_inherit'].includes(key))));
     if(method==='DELETE')delete overrides[model];else overrides[model]=own;
     result={override:{...builtins[model],...overrides[model]},saved_override:{...overrides[model]}};
    }
    const response=new Response(JSON.stringify(result),{status});
    return new Promise(resolve=>{release=()=>resolve(response);});
   }};
   return {finish:()=>release(),submitted:()=>body};
  }

  w.switchTab('runtime');
  const retryOld=settings.retry_max,keepaliveOld=settings.keepalive_interval;
  edit('retry_max',String(retryOld+1));
  let delayed=deferWrite('/api/settings'),saving=w.saveSettings();
  assert.equal(delayed.submitted().retry_max,retryOld+1);
  edit('retry_max',String(retryOld)); // an explicit late return to the pre-submit value
  edit('keepalive_interval',String(keepaliveOld+9));
  await w.loadParams(); // GET sees our saved reply before the POST acknowledgement
  assert.equal(el('retry_max').value,String(retryOld));
  delayed.finish();await saving;
  assert.equal(settings.retry_max,retryOld+1);
  assert.equal(el('retry_max').value,String(retryOld));
  assert.equal(el('keepalive_interval').value,String(keepaliveOld+9));
  assert.match(el('runtime-save-bar').textContent,/2 项修改未保存/);
  assert.equal(w.consoleForms.dirty(),true);
  await w.saveSettings();
  for(const [key,before,after] of [['retry_max',retryOld+1,retryOld],['keepalive_interval',keepaliveOld,keepaliveOld+9]]){
   assert.equal(lastSettings()[key],after);
   assert.deepEqual(lastSettings()._expected[key],{present:true,value:before});
  }
  assert.equal(w.consoleForms.dirty(),false);

  edit('retry_max',String(retryOld+2));
  delayed=deferWrite('/api/settings');saving=w.saveSettings();
  edit('retry_max',String(retryOld+3));w.consoleForms.discard('runtime');
  delayed.finish();await saving;
  assert.equal(el('retry_max').value,String(retryOld+2));
  assert.equal(w.consoleForms.dirty(),false);
  check('Delayed runtime receipts acknowledge only submitted controls, retain later reverted/disjoint edits across reload, and honor discard');

  edit('retry_max',String(retryOld+3));
  delayed=deferWrite('/api/settings');saving=w.saveSettings();
  settings.retry_max=retryOld+4; // another editor changed the value after this write committed
  settings.keepalive_interval=keepaliveOld+12;
  await w.loadParams();delayed.finish();await saving;
  assert.equal(el('retry_max').value,String(retryOld+4));
  assert.equal(el('keepalive_interval').value,String(keepaliveOld+12));
  assert.equal(w.consoleForms.dirty(),false);
  check('A delayed runtime receipt never replaces a visibly reloaded newer value');

  w.switchTab('params');
  for(const [model,endpoint] of [['text','/api/model-overrides/text'],['__global__','/api/settings']]){
   edit('model-sel',model);
   const old=Number(el('first_reply_timeout_seconds').value);
   const before=model==='text'
     ? {present:Object.hasOwn(overrides.text,'first_reply_timeout_seconds'),value:overrides.text.first_reply_timeout_seconds}
     : {present:true,value:old};
   edit('first_reply_timeout_seconds',String(old+10));
   delayed=deferWrite(endpoint);saving=w.saveModelOverride();
   assert.equal(delayed.submitted()._expected.first_reply_timeout_seconds.present,before.present);
   edit('first_reply_timeout_seconds',String(old));
   const max=String(Number(el('default_max_tokens').value)+345);
   edit('default_max_tokens',max);
   edit('model-sel','image');edit('inject_prefill','other model draft');
   delayed.finish();await saving;
   assert.equal(el('model-sel').value,'image');
   assert.equal(el('inject_prefill').value,'other model draft');
   w.discardModelDraft();edit('model-sel',model);
   assert.equal(el('first_reply_timeout_seconds').value,String(old));
   assert.equal(el('default_max_tokens').value,max);
   assert.equal(el('scope-draft-state').classList.contains('hidden'),false);
   await w.saveModelOverride();
   const posted=requests.filter(r=>r.path===endpoint&&r.method==='POST').at(-1).body;
   assert.deepEqual(posted._expected.first_reply_timeout_seconds,{present:true,value:old+10});
   assert.equal(posted.first_reply_timeout_seconds,old);
   assert.equal(posted.default_max_tokens,Number(max));
   assert.equal(w.consoleForms.dirty(),false);
  }
  check('Owned model and global-default writes preserve same-field reversions, disjoint drafts and the active scope after a delayed receipt');

  edit('model-sel','text');
  w.setFieldInheritance('first_reply_timeout_seconds',true);await w.saveModelOverride();
  // Switching back to inheritance reaches the old absent baseline before the pin returns.
  w.setFieldInheritance('first_reply_timeout_seconds',false);
  delayed=deferWrite('/api/model-overrides/text');saving=w.saveModelOverride();
  w.setFieldInheritance('first_reply_timeout_seconds',true);
  delayed.finish();await saving;
  assert.match(el('source-first_reply_timeout_seconds').textContent,/继承全局默认 · 未保存/);
  await w.saveModelOverride();
  assert.deepEqual(requests.filter(r=>r.path==='/api/model-overrides/text'&&r.method==='POST').at(-1).body._inherit,
    ['first_reply_timeout_seconds']);
  assert.equal(w.consoleForms.dirty(),false);

  edit('default_max_tokens','123');
  delayed=deferWrite('/api/model-overrides/text','POST',()=>({result:{override:{},saved_override:{}}}));
  saving=w.saveModelOverride();delayed.finish();await saving;
  assert.match(el('toast').textContent,/缺少完整/);
  assert.equal(el('default_max_tokens').value,'123');assert.equal(w.consoleForms.dirty(),true);
  delayed=deferWrite('/api/model-overrides/text','DELETE');saving=w.clearModelOverride();
  edit('default_max_tokens','234');delayed.finish();await saving;
  assert.equal(el('default_max_tokens').value,'234');
  assert.equal(overrides.text,undefined);assert.equal(w.consoleForms.dirty(),true);
  w.discardModelDraft();
  assert.equal(el('default_max_tokens').value,String(settings.default_max_tokens));
  assert.equal(w.consoleForms.dirty(),false);
  check('Late inheritance intent and post-reset drafts survive; unconfirmed model receipts never clear edits');

  const unloadBlocked=()=>{
   const event=new w.Event('beforeunload',{cancelable:true});
   w.dispatchEvent(event);return event.defaultPrevented;
  };
  {
   w.switchTab('runtime');
   Object.assign(settings,{retry_max:10,retry_backoff_seconds:5});
   await w.loadParams();
   const runtimeStatus=el('runtime-save-bar').querySelector('[role=status]');
   const before=requests.filter(r=>r.path==='/api/settings'&&r.method==='POST').length;
   for(const raw of ['5.00','5e0','']){
    edit('retry_backoff_seconds',raw);edit('retry_max','');
    assert.equal(w.consoleForms.dirty(),false);
    assert.equal(runtimeStatus.textContent,'修改已保存');assert.equal(unloadBlocked(),false);
    await clickRuntime();
    assert.equal(requests.filter(r=>r.path==='/api/settings'&&r.method==='POST').length,before);
   }
   settings.retry_max=13;await w.loadParams();edit('retry_max','');
   edit('retry_backoff_seconds','5.000');
   assert.match(runtimeStatus.textContent,/^1 项修改未保存/);assert.equal(unloadBlocked(),true);
   w.switchTab('params');await w.loadParams();w.switchTab('runtime');
   assert.equal(el('retry_max').value,'');
   await clickRuntime();
   assert.deepEqual(lastSettings(),{retry_max:10,_expected:{retry_max:{present:true,value:13}}});
   assert.equal(w.consoleForms.dirty(),false);
   // Clearing back to the pre-submit default during a pending write is a real edit.
   edit('retry_max','15');
   delayed=deferWrite('/api/settings');saving=w.saveSettings();
   edit('retry_max','');await w.loadParams();delayed.finish();await saving;
   assert.equal(el('retry_max').value,'');assert.equal(w.consoleForms.dirty(),true);
   await clickRuntime();
   assert.deepEqual(lastSettings(),{retry_max:10,_expected:{retry_max:{present:true,value:15}}});
   assert.equal(unloadBlocked(),false);
   check('Numeric formatting and blank defaults compare like their wire values; intentional resets survive reload and a delayed save');
  }
  {
   w.switchTab('params');
   const literal='another <draft & text>';
   modelCatalog.push(literal);await w.loadParams();
   settings.default_top_p=0.7;
   await w.loadParams();edit('model-sel','text');
   edit('default_top_p','0.700');
   assert.equal(w.consoleForms.dirty(),false); // inherited number; formatting cannot pin it
   w.setFieldInheritance('default_top_p',false);
   edit('default_top_p',''); // explicit pin of null, not inheritance
   edit('model-sel',literal);edit('inject_prefill','leave only if I choose');
   modelCatalog=['image'];
   const writes=()=>requests.filter(r=>r.method==='POST'||r.method==='DELETE').length;
   const before=writes();
   await w.loadParams();assert.equal(el('model-sel').value,'__global__');
   const notice=el('orphaned-model-drafts');
   const discardButtons=()=>[...notice.querySelectorAll('[data-discard-orphan-model]')];
   assert.equal(notice.hidden,false);assert.equal(unloadBlocked(),true);
   assert.deepEqual(discardButtons().map(b=>b.dataset.discardOrphanModel),['text',literal]);
   assert.match(notice.textContent,/another <draft & text>/);
   assert.equal(el('model-sel').querySelector('[value=text]'),null);
   await save('btn-scope-save');assert.equal(writes(),before);
   // A stale option cannot make a model missing from the received catalog writable.
   edit('model-sel','image');
   el('model-sel').add(new w.Option('stale','text'));
   edit('model-sel','text');await w.saveModelOverride();await w.clearModelOverride();
   assert.equal(writes(),before);
   discardButtons()[1].click();
   assert.deepEqual(discardButtons().map(b=>b.dataset.discardOrphanModel),['text']);
   assert.equal(unloadBlocked(),true);assert.equal(writes(),before);
   modelCatalog=['text','image',literal];
   notice.querySelector('[data-reload-model-list]').click();await tick();
   assert.equal(notice.hidden,true);
   assert.equal(d.activeElement.id,'model-sel');
   assert.match(el('model-sel').querySelector('[value=text]').textContent,/草稿/);
   edit('model-sel',literal);assert.equal(el('scope-draft-state').classList.contains('hidden'),true);
   assert.notEqual(el('inject_prefill').value,'leave only if I choose');
   edit('model-sel','text');assert.equal(el('default_top_p').value,'');
   await save('btn-scope-save');
   assert.deepEqual(requests.filter(r=>r.path==='/api/model-overrides/text'&&r.method==='POST').at(-1).body,
     {default_top_p:null,_expected:{default_top_p:{present:false}}});
   assert.equal(unloadBlocked(),false);
   edit('inject_prefill','discard me');
   modelCatalog=['image'];await w.loadParams();discardButtons()[0].click();
   assert.equal(notice.hidden,true);assert.equal(unloadBlocked(),false);
   assert.equal(d.activeElement.id,'model-sel');
   check('Unavailable model drafts stay visible, recover with the original scope and pin intent, or discard individually without writing');

   modelCatalog=['text','image'];await w.loadParams();edit('model-sel','text');
   edit('first_reply_timeout_seconds','31');
   delayed=deferWrite('/api/model-overrides/text');saving=w.saveModelOverride();
   edit('first_reply_timeout_seconds',''); // explicitly disable the timeout, not inherit
   modelCatalog=['image'];await w.loadParams();
   delayed.finish();await saving;
   assert.equal(notice.hidden,false);assert.equal(unloadBlocked(),true);
   modelCatalog=['text','image'];await w.loadParams();edit('model-sel','text');
   assert.equal(el('first_reply_timeout_seconds').value,'');
   await save('btn-scope-save');
   assert.deepEqual(requests.filter(r=>r.path==='/api/model-overrides/text'&&r.method==='POST').at(-1).body,
     {first_reply_timeout_seconds:0,_expected:{first_reply_timeout_seconds:{present:true,value:31}}});
   assert.equal(unloadBlocked(),false);
   check('A model disappearing during a pending write still retains a later explicit reset on reappearance');
  }

  {
   w.switchTab('runtime');
   settings.retry_max=10;settings.retry_jitter_seconds=1;settings.keepalive_interval=60;
   await w.loadParams();
   settings.retry_jitter_seconds=2; // disjoint server change, not in our write
   const release=holdConfigRead('/api/settings');
   const pending=w.loadParams();
   edit('retry_max','7');await clickRuntime();
   assert.equal(settings.retry_max,7);
   edit('keepalive_interval','75');
   release();await pending;
   assert.equal(el('retry_max').value,'7');
   assert.equal(el('retry_jitter_seconds').value,'2');
   assert.equal(el('keepalive_interval').value,'75');
   assert.match(el('runtime-save-bar').textContent,/1 项修改未保存/);
   edit('retry_max','8');await clickRuntime();
   assert.deepEqual(lastSettings(),{retry_max:8,keepalive_interval:75,
     _expected:{retry_max:{present:true,value:7},keepalive_interval:{present:true,value:60}}});
   settings.retry_max=9;await w.loadParams();
   assert.equal(el('retry_max').value,'9');assert.equal(w.consoleForms.dirty(),false);
   check('A read started before a runtime save cannot rewind it or its CAS, but adopts unrelated fields and preserves later drafts');
  }

  {
   w.switchTab('params');
   settings.first_reply_timeout_seconds=15;
   overrides.text={first_reply_timeout_seconds:40};
   overrides.image={};
   await w.loadParams();edit('model-sel','text');
   overrides.image.default_max_tokens=128; // independent model update in the delayed read
   const release=holdConfigRead('/api/capabilities');
   const pending=w.loadParams();
   el('source-first_reply_timeout_seconds').querySelector('button').click();
   await save('btn-scope-save');
   assert.equal(Object.hasOwn(overrides.text,'first_reply_timeout_seconds'),false);
   release();await pending;
   assert.equal(el('first_reply_timeout_seconds').value,'15');
   assert.match(el('source-first_reply_timeout_seconds').textContent,/继承全局/);
   edit('model-sel','image');assert.equal(el('default_max_tokens').value,'128');
   edit('model-sel','text');edit('first_reply_timeout_seconds','33');await save('btn-scope-save');
   assert.deepEqual(requests.filter(r=>r.path==='/api/model-overrides/text'&&r.method==='POST').at(-1).body,
     {first_reply_timeout_seconds:33,_expected:{first_reply_timeout_seconds:{present:false}}});
   overrides.text.first_reply_timeout_seconds=38;await w.loadParams();
   assert.equal(el('first_reply_timeout_seconds').value,'38');
   check('A delayed capability read preserves an acknowledged inheritance/removal and only protects the saved model fields');
  }

  {
   w.switchTab('params');edit('model-sel','image');
   settings.retry_max=11; // only the held older settings GET succeeds
   const release=holdConfigRead('/api/settings'), pending=w.loadParams();
   const previous=el('retry_max').value;
   modelCatalog=['image'];
   transport=(url,opts)=>url.pathname==='/api/settings'&&!opts.method?new Response('{}',{status:503}):undefined;
   await w.loadParams();transport=null;
   assert.equal(el('model-sel').querySelector('[value=text]'),null);
   assert.equal(el('retry_max').value,previous);
   edit('default_max_tokens','321');
   release();await pending;
   assert.equal(el('retry_max').value,'11');
   assert.equal(el('model-sel').querySelector('[value=text]'),null);
   assert.equal(el('model-sel').value,'image');assert.equal(el('default_max_tokens').value,'321');
   assert.match(el('params-load-error').textContent,/503/);
   modelCatalog=['text','image'];await w.loadParams();w.discardModelDraft();
   assert.equal(el('params-load-error').classList.contains('hidden'),true);
   check('Different configuration datasets arbitrate overlapping GETs independently; a failed newer settings read does not poison the model list');
  }

  console.log(`${passed} groups of console DOM checks passed. Layout requires the browser suite.`);
 }finally{dom.window.close();}
}
main().catch(error=>{console.error(error);process.exitCode=1;});
