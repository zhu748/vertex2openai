/* Optional browser regression. Requires Playwright + Chromium and requirements-dev.txt.
 * Run: node scripts/verify_console_ui.cjs
 * PLAYWRIGHT_CHROMIUM_EXECUTABLE can select an existing Chromium binary.
 * UI_SCREENSHOT_DIR optionally saves screenshots. Uses temporary state and fake keys;
 * UI_TEST_CJK_FONT optionally supplies a local CJK font for minimal CI images.
 * lifespan is disabled and probes are intercepted, so no Google requests are made.
 */
const assert = require('node:assert/strict');
const {spawn} = require('node:child_process');
const {mkdtemp, rm, mkdir} = require('node:fs/promises');
const {tmpdir} = require('node:os');
const path = require('node:path');
const http = require('node:http');
const {chromium} = require('playwright');

const root = path.resolve(__dirname, '..');
const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));
const fixture = `
import time
from main import app
from logger import stats, rt_logger
from request_tracking import request_registry
from runtime_state import app_state
import uvicorn, os
app_state.set_express_keys([{'key':'ui-fixture-key','project_id':'fixture-project','project_source':'auto','enabled':True}])
app_state.set_cookie_accounts([])
# Start disabled so checking the guard below makes a real model-specific edit.
# Its production default is already enabled; an unchanged value is not posted.
app_state.update_settings({'image_aspect_ratio':'16:9','express_ua_profile':'custom','express_ua_custom':'fixture-gateway/'+'x'*360,
                          'hide_thoughts':True,'native_thinking_mode':'request','prefill_cot_guard':False})
stats.total_requests=32
stats.success_requests=25
stats.error_requests=5
stats.retry_counts=4
stats.prompt_tokens=900
stats.completion_tokens=100
for i in range(32):
    trace=request_registry.create('/v1/chat/completions')
    trace.started-=30+i
    trace.configure(model='gemini-4-argon' if i<30 else 'gemini-4-argon-antitruncation-stream',
                    protocol='openai',channel='express',stream=True,mode='stream',
                    upstream_model='projects/fixture-project/locations/global/publishers/google/models/gemini-4-argon',
                    project_id='fixture-project')
    trace.start_generation()
    trace.attempt()
    trace.upstream_chunk()
    trace.milestone('first_reply','收到模型首回复（包含思考）。',phase='thinking')
    trace.log('诊断内容 <img src=x onerror="window.uiInjected=true">')
    if i<30:
        trace.milestone('first_content','已发送正文首字。',phase='streaming')
        if i<5: trace.fail('测试上游错误：连接中断，请稍后重试。',502)
        trace.end_generation('error' if i<5 else 'succeeded')
        trace.end_connection()
    elif i==31:
        trace.add_retry()
        trace.retry(30,'测试 429：正在等待下一次尝试。')
        trace.end_connection(disconnected=True)
rt_logger.push('ERROR UI_TEST_ERROR 测试错误日志')
rt_logger.push('WARN UI_TEST_WARN 测试警告与重试')
rt_logger.push('UI_TEST_INFO <img src=x onerror="window.uiInjected=true">')
uvicorn.run(app,host='127.0.0.1',port=int(os.environ['UI_TEST_PORT']),lifespan='off',log_level='warning')
`;

async function main() {
  const state = await mkdtemp(path.join(tmpdir(), 'vertex-console-ui-'));
  const reservation = http.createServer();
  await new Promise(resolve => reservation.listen(0, '127.0.0.1', resolve));
  const port = reservation.address().port;
  await new Promise(resolve => reservation.close(resolve));
  const origin = `http://127.0.0.1:${port}`;
  const server = spawn(process.env.PYTHON || 'python', ['-u', '-c', fixture], {
    cwd: path.join(root, 'app'),
    env: {...process.env, STATE_DIR: state, API_KEY: 'ui-browser-test', UI_TEST_PORT: String(port),
      VERTEX_EXPRESS_API_KEY: '', VERTEX_EXPRESS_API_KEYS: '', GOOGLE_COOKIE: '', GOOGLE_COOKIES: '',
      GOOGLE_PROJECT_ID: '', GOOGLE_PROJECT_IDS: '', PROXY_URL: '', PROXY_URLS: '', UPSTREAM_MODE: 'api_key', MODELS_CONFIG_URL: ''},
    stdio: ['ignore', 'pipe', 'pipe'],
  });
  let serverOutput = '';
  for (const stream of [server.stdout, server.stderr]) stream.on('data', data => { serverOutput = (serverOutput + data).slice(-5000); });
  let browser;
  let passed = 0;
  const check = label => { console.log('✓ ' + label); passed++; };
  const screenshots = process.env.UI_SCREENSHOT_DIR;
  if (screenshots) await mkdir(screenshots, {recursive: true});
  try {
    for (let i = 0; i < 100; i++) {
      if (server.exitCode !== null) throw new Error('Fixture server failed: ' + serverOutput);
      try { if ((await fetch(origin)).ok) break; } catch {}
      if (i === 99) throw new Error('Fixture server did not start: ' + serverOutput);
      await sleep(100);
    }
    browser = await chromium.launch({headless: true,
      executablePath: process.env.PLAYWRIGHT_CHROMIUM_EXECUTABLE || undefined,
      args: ['--no-sandbox']});
    const context = await browser.newContext({viewport: {width: 1440, height: 1000}});
    // Ordinary remote HTTP origins do not expose randomUUID. Account creation
    // must work there too; the server owns durable Cookie account identities.
    await context.addInitScript(() => Object.defineProperty(Crypto.prototype, 'randomUUID', {value: undefined}));
    const errors = [], external = [];
    context.on('page', page => {page.on('pageerror', error => errors.push(error.message));page.on('dialog', dialog => dialog.accept());});
    await context.route('**/*', route => {
      if (!route.request().url().startsWith(origin)) {external.push(route.request().url());return route.abort();}
      return route.continue();
    });
    if(process.env.UI_TEST_CJK_FONT){
      await context.route('**/__ui-test-font.otf',route=>route.fulfill({path:process.env.UI_TEST_CJK_FONT,contentType:'font/otf'}));
      await context.addInitScript(()=>document.addEventListener('DOMContentLoaded',()=>{
        const style=document.createElement('style');
        style.textContent="@font-face{font-family:ConsoleReviewCJK;src:url('/__ui-test-font.otf')}body{font-family:ConsoleReviewCJK,sans-serif!important}";
        document.head.append(style);
      }));
    }
    const page = await context.newPage();
    const responseFor = (pathname, method = 'POST') => page.waitForResponse(r => new URL(r.url()).pathname === pathname && r.request().method() === method);
    const screenshot = async name => {if (screenshots) await page.screenshot({path: path.join(screenshots, name + '.png'), fullPage: false});};
    const openTab = async tab => {
      await page.evaluate(t => switchTab(t), tab);
      if(tab==='channel') {
        // Keep locator indexes stable as clicks remove the closed state.
        const summaries = page.locator('#view-channel .account-editor > summary:visible');
        for(const summary of await summaries.all()) {
          if(!await summary.evaluate(el => el.parentElement.open)) await summary.click();
        }
        assert.equal(await page.locator('#view-channel .account-editor:not([open]) > summary:visible').count(), 0);
      }
    };
    const openProxyForm = async () => {await page.click('#proxy-tab-form');};
    const finishProxyBatch = async (allowEmpty=false) => {
      await page.waitForSelector('#proxy-panel-import:not(.hidden)');
      await page.click('#proxy-import-preview');
      await page.waitForFunction(()=>!document.querySelector('#proxy-import-preview-panel').classList.contains('hidden') && document.querySelector('#proxy-stop-tests').classList.contains('hidden') && !document.querySelector('#proxy-import-preview').matches(':disabled'));
      if(allowEmpty){assert.equal(await page.locator('#proxy-import-add').isDisabled(),true);await page.click('#proxy-tab-list');}
      else {await page.click('#proxy-import-add');await page.waitForSelector('#proxy-panel-list:not(.hidden)');}
    };
    await page.goto(origin);
    await page.fill('#pw', 'wrong-password');
    await page.click('#login-submit');
    await page.waitForFunction(() => document.querySelector('#err').textContent.includes('密码错误'));
    await page.click('#password-toggle');
    assert.equal(await page.getAttribute('#pw', 'type'), 'text');
    await page.fill('#pw', 'ui-browser-test');
    await page.click('#login-submit');
    await page.waitForSelector('#s-total');
    if(process.env.UI_TEST_CJK_FONT)await page.evaluate(()=>document.fonts.ready);
    await page.waitForFunction(() => document.querySelector('#s-total').textContent === '32');
    await page.waitForFunction(() => document.querySelector('#model-sel').options.length > 1);
    assert.equal(await page.textContent('#health-rate'), '83.3%');
    assert.equal(await page.evaluate(() => document.querySelector('#prompt-bar').style.width), '90%');
    assert.equal(await page.evaluate(() => document.querySelector('#completion-bar').style.width), '10%');
    await screenshot('overview-desktop');
    check('登录错误、密码显隐、真实统计比例和本地资源');

    await page.focus('#tab-overview');
    await page.keyboard.press('ArrowRight');
    assert.equal(await page.getAttribute('#tab-channel', 'aria-selected'), 'true');
    check('导航支持键盘与 ARIA 状态');

    await openTab('params');
    assert.equal(await page.inputValue('#native_thinking_mode'), 'request');
    await page.selectOption('#model-sel', 'gemini-4-argon');
    await page.fill('#first_reply_timeout_seconds', '28.5');
    assert.equal(await page.locator('#prefill_cot_guard').isChecked(), false);
    await page.check('#prefill_cot_guard');
    let pending = responseFor('/api/model-overrides/gemini-4-argon');
    await page.click('#btn-scope-save');
    let response = await pending;
    assert.equal(response.status(), 200);
    assert.equal(response.request().postDataJSON().prefill_cot_guard, true);
    assert.equal(response.request().postDataJSON().first_reply_timeout_seconds, 28.5);
    assert.deepEqual(Object.keys(response.request().postDataJSON()).sort(),['first_reply_timeout_seconds','prefill_cot_guard']);
    assert.equal((await response.json()).override.prefill_cot_guard, true);
    await page.waitForFunction(() => !document.querySelector('#btn-scope-save').disabled);
    await openTab('runtime');
    assert.equal(await page.locator('#cross_channel_fallback').isChecked(),false);
    assert.equal(await page.locator('#cookie_structured_output').isChecked(),false);
    await page.locator('#cookie_structured_output').check();
    assert.equal(await page.locator('#cookie_auto_refresh').isChecked(),true);
    assert.equal(await page.inputValue('#cookie_refresh_interval_seconds'),'600');
    await page.locator('#cookie_auto_refresh').check();
    await page.fill('#cookie_refresh_interval_seconds','900');
    pending = responseFor('/api/settings');
    await page.locator('button[onclick*="saveSettings"]').click();
    response = await pending;
    assert.equal(response.status(), 200);
    assert.ok(!('first_reply_timeout_seconds' in response.request().postDataJSON()));
    assert.ok(!('prefill_cot_guard' in response.request().postDataJSON()));
    assert.equal(response.request().postDataJSON().cookie_structured_output,true);
    assert.equal(response.request().postDataJSON().cookie_auto_refresh,true);
    assert.equal(response.request().postDataJSON().cookie_refresh_interval_seconds,900);
    const upstreamSettingsPage=await context.newPage();
    await upstreamSettingsPage.goto(origin+'/#params');
    await upstreamSettingsPage.waitForFunction(()=>document.querySelector('#cookie_structured_output')?.checked===true);
    await upstreamSettingsPage.waitForFunction(()=>document.querySelector('#cookie_auto_refresh')?.checked===true && document.querySelector('#cookie_refresh_interval_seconds')?.value==='900');
    await upstreamSettingsPage.close();
    await page.bringToFront();
    check('Cookie 结构化输出默认关闭、自动续期默认开启，保存后在新页面正确回填');
    await openTab('params');
    await page.selectOption('#model-sel', '__global__');
    assert.equal(await page.inputValue('#image_aspect_ratio'), '16:9');
    assert.match(await page.locator('#model-sel option').first().textContent(), /全局默认/);
    assert.equal(await page.locator('.model-scope-section #prefill_cot_guard').count(), 1);
    check('模型与全局保存作用域、思维链守卫分组、生图比例回填');

    await page.fill('#first_reply_timeout_seconds', '35');
    await page.selectOption('#model-sel', 'gemini-4-argon');
    await page.fill('#first_reply_timeout_seconds', '41');
    await page.fill('#inject_prefill', '模型草稿：保留切换前的文字');
    await page.selectOption('#model-sel', '__global__');
    assert.equal(await page.inputValue('#first_reply_timeout_seconds'), '35');
    assert.equal(await page.locator('#scope-draft-state').isVisible(), true);
    await page.selectOption('#model-sel', 'gemini-4-argon');
    assert.equal(await page.inputValue('#first_reply_timeout_seconds'), '41');
    assert.equal(await page.inputValue('#inject_prefill'), '模型草稿：保留切换前的文字');
    await page.selectOption('#sampling_policy', 'allowed');
    assert.match(await page.textContent('#sampling-note'), /强制保留/);
    await page.selectOption('#sampling_policy', 'deprecated');
    assert.match(await page.textContent('#sampling-note'), /强制剥离/);
    await page.selectOption('#sampling_policy', 'auto');
    assert.match(await page.textContent('#sampling-note'), /自动移除/);
    await page.fill('#default_top_p', '2');
    await page.click('#btn-scope-save');
    await page.waitForFunction(()=>document.querySelector('#default_top_p').getAttribute('aria-invalid')==='true' && document.activeElement.id==='default_top_p');
    assert.equal(await page.locator('#default_top_p-error').isVisible(),true);
    await page.fill('#default_top_p','');
    assert.equal(await page.getAttribute('#default_top_p','aria-invalid'),null);
    let releaseModel, modelStarted;
    const modelGate=new Promise(resolve=>{releaseModel=resolve;});
    const modelReady=new Promise(resolve=>{modelStarted=resolve;});
    await page.route('**/api/model-overrides/gemini-4-argon',async route=>{modelStarted();await modelGate;await route.continue();});
    pending=responseFor('/api/model-overrides/gemini-4-argon');
    await page.click('#btn-scope-save');
    await modelReady;
    assert.equal(await page.locator('#first_reply_timeout_seconds').isDisabled(),true);
    assert.equal(await page.locator('#model-sel').isDisabled(),true);
    assert.equal(await page.locator('#btn-scope-clear').isDisabled(),true);
    await page.evaluate(()=>loadNotice('params','测试：保存期间不应重载表单'));
    await page.locator('#params-load-error button').click();
    assert.match(await page.textContent('#toast'),/正在处理/);
    assert.equal(await page.locator('#model-sel').isDisabled(),true);
    releaseModel();
    response=await pending;
    assert.equal(response.status(),200);
    await page.waitForFunction(()=>!document.querySelector('#model-sel').matches(':disabled'));
    assert.equal(await page.locator('#scope-draft-state').isVisible(),false);
    await page.unroute('**/api/model-overrides/gemini-4-argon');
    await page.evaluate(()=>loadNotice('params',''));
    await page.selectOption('#model-sel','__global__');
    assert.equal(await page.inputValue('#first_reply_timeout_seconds'),'35');
    check('模型草稿隔离、慢保存防覆盖、具体参数报错与采样提示');

    const staleSettings=await (await page.request.get(origin+'/api/settings')).json();
    let releaseRuntime, runtimeStarted;
    const runtimeGate=new Promise(resolve=>{releaseRuntime=resolve;});
    const runtimeReady=new Promise(resolve=>{runtimeStarted=resolve;});
    await page.route('**/api/settings',async route=>{
      const patch=route.request().postDataJSON();
      if(!patch || !('retry_max' in patch)) return route.continue();
      runtimeStarted();await runtimeGate;
      return route.fulfill({json:{...staleSettings,...patch}});
    });
    await openTab('runtime');
    const runtimeResponse=page.waitForResponse(r=>new URL(r.url()).pathname==='/api/settings' && r.request().postDataJSON()?.retry_max!==undefined);
    await page.locator('button[onclick*="saveSettings"]').click();
    await runtimeReady;
    await openTab('params');
    pending=responseFor('/api/settings');
    await page.click('#btn-scope-save');
    response=await pending;
    assert.equal(response.status(),200);
    releaseRuntime();
    await runtimeResponse;
    await page.waitForFunction(()=>!document.querySelector('.global-settings-section').disabled);
    await page.selectOption('#model-sel','gemini-4-argon');
    await page.selectOption('#model-sel','__global__');
    assert.equal(await page.inputValue('#first_reply_timeout_seconds'),'35');
    await page.unroute('**/api/settings');
    check('不同分区同时保存时，延迟的旧响应不会回滚模型默认值');

    await openTab('channel');
    await page.waitForSelector('.k-key');
    await page.fill('.k-project', 'unsaved-manual-project');
    await page.fill('.k-proxy', 'direct');
    await page.locator('button[onclick="addKeyRow()"]').click();
    const draft = 'draft<&\"\'><img src=x onerror=window.uiInjected=true>';
    await page.locator('.k-key').nth(1).fill(draft);
    await page.route('**/api/express-keys/probe', route => route.fulfill({json: {
      keys: [{key: 'ui-fixture-key', project_id: 'probe-project', project_source: 'auto'}],
      results: [{key: 'ui-fixture-key', project_id: 'probe-project', ok: true, message: '项目探测成功'}],
    }}));
    await page.locator('.key-actions button').first().click();
    await page.waitForFunction(() => document.querySelector('.key-meta[role="status"]'));
    assert.equal(await page.locator('.k-key').count(), 2);
    assert.equal(await page.locator('.k-key').nth(1).inputValue(), draft);
    assert.equal(await page.locator('.k-project').first().inputValue(), 'unsaved-manual-project');
    assert.equal(await page.locator('.k-proxy').first().inputValue(), 'direct');
    assert.equal(await page.evaluate(() => Boolean(window.uiInjected)), false);
    await page.route('**/api/settings/mode', route => route.fulfill({status: 503, json: {error: '测试失败'}}));
    await page.click('input[name=mode][value=web_proxy]');
    await page.waitForFunction(() => document.querySelector('input[name=mode][value=api_key]').checked);
    assert.equal(await page.locator('#cookie-box').isVisible(), false);
    assert.equal(await page.locator('#keys-box').isVisible(), true);
    await page.unroute('**/api/settings/mode');
    check('凭证探测保留草稿、服务端字符串转义、通道失败回滚');
    assert.ok((await page.textContent('#fp2-status')).length<60);
    assert.equal((await page.getAttribute('#fp2-status','title')).length,376);
    await page.locator('.k-project').nth(1).fill('second-fixture-project');
    let releaseKeys, keysStarted;
    const keysGate=new Promise(resolve=>{releaseKeys=resolve;});
    const keysReady=new Promise(resolve=>{keysStarted=resolve;});
    await page.route('**/api/express-keys',async route=>{
      if(route.request().method()!=='POST') return route.continue();
      keysStarted();await keysGate;return route.continue();
    });
    pending=responseFor('/api/express-keys');
    await page.locator('button[onclick*="saveExpressKeys"]').click();
    await keysReady;
    assert.equal(await page.locator('.k-key').first().isDisabled(),true);
    assert.equal(await page.locator('button[onclick="addKeyRow()"]').isDisabled(),true);
    releaseKeys();
    response=await pending;
    assert.equal(response.status(),200);
    await page.waitForFunction(()=>!document.querySelector('.k-key').matches(':disabled'));
    assert.equal(await page.locator('.k-key').nth(1).inputValue(),draft);
    await page.unroute('**/api/express-keys');
    check('凭证保存期间隔离编辑与探测，长请求头状态不撑开页面');

    await page.getByRole('button',{name:'管理 / 添加代理',exact:true}).click();
    await openProxyForm();
    await page.fill('#proxy-paste','socks5://127.0.0.1:1080:proxy-user:p%40ss');
    await page.click('.proxy-recognize');
    await page.waitForFunction(()=>document.querySelector('#proxy-username').value==='proxy-user' && !document.querySelector('#proxy-form-add').matches(':disabled'));
    assert.equal(await page.inputValue('#proxy-password'),'p@ss');
    await openProxyForm();
    await page.click('#proxy-form-add');
    await page.waitForFunction(()=>document.querySelectorAll('[data-proxy-index]').length===1);
    await openProxyForm();
    await page.selectOption('#proxy-type','http');
    await openProxyForm();
    await page.fill('#proxy-paste','proxy-second.invalid:8080');
    await page.click('.proxy-recognize');
    await page.waitForFunction(()=>document.querySelector('#proxy-host').value==='proxy-second.invalid' && !document.querySelector('#proxy-form-add').matches(':disabled'));
    assert.equal(await page.inputValue('#proxy-type'),'http');
    await openProxyForm();
    await page.click('#proxy-form-add');
    await page.waitForFunction(()=>document.querySelectorAll('[data-proxy-index]').length===2);
    await page.click('#proxy-tab-list');
    await page.locator('[data-proxy-index="1"]').getByRole('button',{name:'将第 2 条设为首选',exact:true}).click();
    await page.click('#proxy-tab-list');
    await page.locator('[data-proxy-index="0"]').getByRole('button',{name:'编辑',exact:true}).click();
    await page.waitForFunction(()=>document.querySelector('#proxy-host').value==='proxy-second.invalid' && !document.querySelector('#proxy-form-add').matches(':disabled'));
    await openProxyForm();
    await page.fill('#proxy-port','8081');
    await page.click('#proxy-tab-list');
    await page.locator('[data-proxy-index="0"]').getByRole('button',{name:'下移第 1 条',exact:true}).click();
    assert.equal(await page.inputValue('#proxy-port'),'8081');
    assert.equal(await page.locator('[data-proxy-index="1"]').evaluate(el=>el.classList.contains('is-editing')),true);
    assert.equal(await page.textContent('#proxy-form-add'),'更新第 2 条');
    await page.click('#proxy-tab-list');
    await page.locator('[data-proxy-index="1"]').getByRole('button',{name:'上移第 2 条',exact:true}).click();
    assert.equal(await page.inputValue('#proxy-port'),'8081');
    assert.equal(await page.textContent('#proxy-form-add'),'更新第 1 条');
    await openProxyForm();
    await page.click('#proxy-form-add');
    await page.waitForFunction(()=>document.querySelector('[data-proxy-index="0"] strong').textContent.includes('8081'));
    await page.route('**/api/proxy/test',route=>route.fulfill({json:{ok:true,via_proxy:true,message:'代理连通成功',latency_ms:75,status_code:403}}));
    await page.click('#proxy-tab-list');
    await page.locator('[data-proxy-index="0"]').getByRole('button',{name:'测试',exact:true}).click();
    await page.waitForFunction(()=>document.querySelector('#proxy-result-0').textContent.includes('75 ms') && !document.querySelector('#proxy-form-add').matches(':disabled'));
    await screenshot('proxy-editor-desktop');
    await page.click('#proxy-dialog-apply');
    await page.waitForFunction(()=>!document.querySelector('#proxy-dialog').open);
    assert.equal(await page.inputValue('#proxy-input'),'http://proxy-second.invalid:8081\nsocks5://proxy-user:p%40ss@127.0.0.1:1080');
    // The raw editor accepts the same comma-separated complete URLs as env.
    await page.fill('#proxy-input','http://proxy-second.invalid:8081, socks5://proxy-user:p%40ss@127.0.0.1:1080');
    pending=responseFor('/api/settings');
    await page.locator('button[onclick*="saveProxy()"]').click();
    response=await pending;
    assert.equal(response.status(),200);
    assert.equal(response.request().postDataJSON().proxy_urls.length,2);
    await page.waitForFunction(()=>document.querySelector('#proxy-draft-state').classList.contains('hidden') && !document.querySelector('#proxy-input').matches(':disabled'));
    check('代理弹窗识别供应商格式、字段填写、特殊字符、排序、编辑、测试与持久化');

    const savedProxyText=await page.inputValue('#proxy-input');
    await page.locator('button[onclick*="testProxy()"]').click();
    await page.waitForFunction(()=>document.querySelector('#proxy-test-result').textContent.includes('75 ms') && !document.querySelector('#proxy-input').matches(':disabled'));
    await page.fill('#proxy-input','socks5://invalid.example:0');
    assert.equal(await page.locator('#proxy-draft-state').isVisible(),true);
    assert.equal(await page.locator('#proxy-test-result').isVisible(),false);
    pending=responseFor('/api/settings');
    await page.locator('button[onclick*="saveProxy()"]').click();
    assert.equal((await pending).status(),400);
    await page.waitForFunction(()=>document.activeElement.id==='proxy-input' && document.querySelector('#proxy-input').getAttribute('aria-invalid')==='true');
    await page.fill('#proxy-input',savedProxyText);
    assert.equal(await page.locator('#proxy-draft-state').isVisible(),false);
    const expiredProxyTest=route=>route.fulfill({status:401,json:{detail:'未登录'}});
    await page.route('**/api/proxy/test',expiredProxyTest);
    await page.locator('button[onclick*="testProxy()"]').click();
    await page.waitForFunction(()=>document.querySelector('#proxy-test-result').textContent.includes('登录已过期') && !document.querySelector('#proxy-input').matches(':disabled'));
    await page.unroute('**/api/proxy/test',expiredProxyTest);
    await page.fill('#proxy-input',savedProxyText);
    check('代理草稿提示、编辑后清除旧测试结果、错误定位与会话过期反馈');

    await page.getByRole('button',{name:'管理 / 添加代理',exact:true}).click();
    await openProxyForm();
    await page.selectOption('#proxy-type','sstp');
    await openProxyForm();
    await page.fill('#proxy-host','vpn.example.com');
    await openProxyForm();
    await page.fill('#proxy-username','domain\\fixture');
    await openProxyForm();
    await page.fill('#proxy-password','private:@password');
    await page.locator('#proxy-advanced summary').click();
    await openProxyForm();
    await page.fill('#proxy-sni','certificate.example.com');
    await openProxyForm();
    await page.fill('#proxy-ca','/app/certificates/company.pem');
    await openProxyForm();
    await page.fill('#proxy-mtu','1280');
    await openProxyForm();
    await page.click('#proxy-form-add');
    await page.waitForFunction(()=>document.querySelectorAll('[data-proxy-index]').length===3 && !document.querySelector('#proxy-form-add').matches(':disabled'),undefined,{polling:100});
    assert.ok(!(await page.locator('[data-proxy-index="2"] strong').textContent()).includes('private'));
    await page.click('#proxy-tab-list');
    await page.locator('[data-proxy-index="2"]').getByRole('button',{name:'编辑',exact:true}).click();
    await page.waitForFunction(()=>document.querySelector('#proxy-type').value==='sstp' && !document.querySelector('#proxy-form-add').matches(':disabled'));
    assert.equal(await page.inputValue('#proxy-port'),'443');
    assert.equal(await page.inputValue('#proxy-password'),'private:@password');
    assert.equal(await page.inputValue('#proxy-sni'),'certificate.example.com');
    assert.equal(await page.inputValue('#proxy-ca'),'/app/certificates/company.pem');
    assert.equal(await page.inputValue('#proxy-mtu'),'1280');
    await screenshot('proxy-editor-sstp');
    await openProxyForm();
    await page.click('#proxy-form-add');
    await page.waitForFunction(()=>document.querySelector('#proxy-type').value==='sstp' && !document.querySelector('#proxy-form-add').matches(':disabled'));
    await openProxyForm();
    await page.fill('#proxy-paste','turn:relay.example.com:3478?transport=tcp&username=fixture&password=private-turn-password');
    await page.click('.proxy-recognize');
    await page.waitForFunction(()=>document.querySelector('#proxy-type').value==='turn' && !document.querySelector('#proxy-form-add').matches(':disabled'));
    assert.equal(await page.inputValue('#proxy-host'),'relay.example.com');
    assert.equal(await page.locator('#proxy-tls-fields').isVisible(),false);
    await openProxyForm();
    await page.click('#proxy-form-add');
    await page.waitForFunction(()=>document.querySelectorAll('[data-proxy-index]').length===4 && !document.querySelector('#proxy-form-add').matches(':disabled'),undefined,{polling:100});
    await openProxyForm();
    await page.selectOption('#proxy-type','turns');
    await openProxyForm();
    await page.fill('#proxy-host','relay-tls.example.com');
    await openProxyForm();
    await page.fill('#proxy-username','fixture');
    await openProxyForm();
    await page.fill('#proxy-password','private-turns-password');
    await openProxyForm();
    await page.click('#proxy-form-add');
    await page.waitForFunction(()=>document.querySelectorAll('[data-proxy-index]').length===5 && !document.querySelector('#proxy-form-add').matches(':disabled'));
    await openProxyForm();
    await page.selectOption('#proxy-type','sstp');
    await openProxyForm();
    await page.fill('#proxy-paste','vpn-supplier.example|443|domain\\fixture|private@password');
    // Add also recognizes fresh paste without clicking Recognize first.
    await openProxyForm();
    await page.click('#proxy-form-add');
    await page.waitForFunction(()=>document.querySelectorAll('[data-proxy-index]').length===6 && !document.querySelector('#proxy-form-add').matches(':disabled'),undefined,{polling:100});
    assert.match(await page.locator('[data-proxy-index="5"] strong').textContent(),/sstp:\/\/\*\*\*@vpn-supplier.example:443/);
    await openProxyForm();
    await page.fill('#proxy-paste',JSON.stringify({iceServers:[{urls:'stun:discovery.example'},{
      urls:['turn:ice.example:3478?transport=tcp','turns:ice.example:5349?transport=tcp'],username:'fixture',credential:'private+ice%password'}]}));
    await page.click('.proxy-recognize');
    await finishProxyBatch();
    await page.waitForFunction(()=>document.querySelectorAll('[data-proxy-index]').length===8 && !document.querySelector('#proxy-form-add').matches(':disabled'));
    const node={type:'sstp',server:'json-vpn.example',username:'fixture',password:'private-json-password',name:'办公 VPN',options:{sni:'cert.example',mtu:1280}};
    await openProxyForm();
    await page.fill('#proxy-paste',JSON.stringify(node));
    await page.click('.proxy-recognize');
    await page.waitForFunction(()=>document.querySelector('#proxy-name').value==='办公 VPN' && !document.querySelector('#proxy-form-add').matches(':disabled'));
    // A changed paste takes precedence over the previous recognized fields.
    node.server='updated-json-vpn.example';node.name='备用 VPN';
    await openProxyForm();
    await page.fill('#proxy-paste',JSON.stringify(node));
    await openProxyForm();
    await page.click('#proxy-form-add');
    await page.waitForFunction(()=>document.querySelectorAll('[data-proxy-index]').length===9 && !document.querySelector('#proxy-form-add').matches(':disabled'));
    assert.match(await page.locator('[data-proxy-index="8"] strong').textContent(),/updated-json-vpn.example:443 · 备用 VPN/);
    assert.ok(!(await page.textContent('#proxy-editor-list')).includes('private'));
    await page.click('#proxy-tab-list');
    await page.locator('[data-proxy-index="8"]').getByRole('button',{name:'编辑',exact:true}).click();
    await page.waitForFunction(()=>document.querySelector('#proxy-name').value==='备用 VPN' && !document.querySelector('#proxy-form-add').matches(':disabled'));
    assert.equal(await page.inputValue('#proxy-sni'),'cert.example');
    assert.equal(await page.inputValue('#proxy-mtu'),'1280');
    await openProxyForm();
    await page.fill('#proxy-name','VPN 备份');
    await openProxyForm();
    await page.click('#proxy-form-add');
    await page.waitForFunction(()=>document.querySelector('[data-proxy-index="8"] strong').textContent.includes('VPN 备份') && !document.querySelector('#proxy-form-add').matches(':disabled'));
    await screenshot('proxy-editor-import-formats');
    await page.click('#proxy-dialog-cancel');
    check('TURN / TURNS / SSTP 多格式识别、ICE / 节点 JSON、直接添加粘贴、备注编辑与凭据脱敏');

    const originalProxyDraft=await page.inputValue('#proxy-input');
    await page.fill('#proxy-input',JSON.stringify({urls:['turn:raw-ice.example?transport=tcp','turns:raw-ice.example?transport=tcp'],username:'fixture',credential:'private-raw-ice'}));
    await page.getByRole('button',{name:'管理 / 添加代理',exact:true}).click();
    await page.click('#proxy-tab-list');
    await page.locator('[data-proxy-index="0"]').getByRole('button',{name:'编辑',exact:true}).click();
    await page.waitForFunction(()=>document.querySelectorAll('[data-proxy-index]').length===2 && !document.querySelector('#proxy-form-add').matches(':disabled'));
    assert.match(await page.locator('[data-proxy-index="0"] strong').textContent(),/turn:\/\//);
    assert.match(await page.locator('[data-proxy-index="1"] strong').textContent(),/turns:\/\//);
    await page.click('#proxy-dialog-cancel');
    await page.fill('#proxy-input',originalProxyDraft);
    check('原始 ICE JSON 编辑先展开全部节点，取消保留配置草稿');

    await page.getByRole('button',{name:'管理 / 添加代理',exact:true}).click();
    await openProxyForm();
    await page.selectOption('#proxy-type','turn');
    const pasteProxy=async value=>{
      await openProxyForm();
      await page.evaluate(text=>{
        const input=document.querySelector('#proxy-paste');
        input.value=text;input.dispatchEvent(new Event('paste',{bubbles:true}));
      },value);
    };
    await pasteProxy('test:1234@192.0.2.10:3478');
    await page.waitForFunction(()=>document.querySelector('#proxy-password').value==='1234' && !document.querySelector('#proxy-form-add').matches(':disabled'),undefined,{polling:100});
    assert.equal(await page.inputValue('#proxy-type'),'turn');
    assert.equal(await page.inputValue('#proxy-host'),'192.0.2.10');
    await openProxyForm();
    await page.click('#proxy-form-add');
    await page.waitForFunction(()=>document.querySelectorAll('[data-proxy-index]').length===3 && !document.querySelector('#proxy-form-add').matches(':disabled'),undefined,{polling:100});
    assert.equal(await page.inputValue('#proxy-type'),'turn');
    await pasteProxy('192.0.2.20:5349');
    await page.waitForFunction(()=>document.querySelector('#proxy-host').value==='192.0.2.20' && !document.querySelector('#proxy-form-add').matches(':disabled'));
    assert.equal(await page.inputValue('#proxy-type'),'turn'); // Port does not choose TLS.
    assert.equal(await page.inputValue('#proxy-port'),'5349');
    assert.equal(await page.inputValue('#proxy-username'),'');
    // 匿名 TURN（RFC 7065）合法：不再提示补齐凭据，前后端 missing_fields 均为空。
    assert.match(await page.textContent('#proxy-paste-hint'),/已识别.*检查后加入列表/);
    assert.deepEqual(await page.evaluate(()=>proxyMissingFields('turn://12.192.169.147:3479')),[]);
    // SSTP 的 MS-CHAPv2 认证协议强制：无凭据节点仍被标记待补齐（与后端同口径）。
    assert.deepEqual(await page.evaluate(()=>proxyMissingFields('sstp://192.0.2.50:443')),['账号','密码']);
    await openProxyForm();
    await page.fill('#proxy-username','test');await page.fill('#proxy-password','test');
    await openProxyForm();
    await page.click('#proxy-form-add');
    await page.waitForFunction(()=>document.querySelectorAll('[data-proxy-index]').length===4 && !document.querySelector('#proxy-form-add').matches(':disabled'),undefined,{polling:100});
    await pasteProxy('test:1234@192.0.2.30:3478\n192.0.2.40:3479');
    await finishProxyBatch();
    await page.waitForFunction(()=>document.querySelectorAll('[data-proxy-index]').length===6 && !document.querySelector('#proxy-form-add').matches(':disabled'),undefined,{polling:100});
    // 无凭据 TURN 节点直接加入：可测试、可直接应用，不再阻断保存。
    assert.equal((await page.locator('[data-proxy-index="5"] small').textContent()).includes('待补齐'),false);
    assert.equal(await page.locator('[data-proxy-index="5"]').getByRole('button',{name:'测试',exact:true}).isDisabled(),false);
    await page.click('#proxy-dialog-apply');
    await page.waitForFunction(()=>!document.querySelector('#proxy-dialog').open);
    assert.equal((await page.inputValue('#proxy-input')).split('\n').length,6);
    await page.fill('#proxy-input',originalProxyDraft);
    check('所选协议下粘贴即填字段，匿名 TURN 免凭据直通，SSTP 仍强制凭据');

    await page.getByRole('button',{name:'管理 / 添加代理',exact:true}).click();
    const legacySsPayload=Buffer.from('chacha20-ietf-poly1305:private-password@9.9.9.9:9999').toString('base64');
    await openProxyForm();
    await page.fill('#proxy-paste','ss://'+legacySsPayload);
    await page.click('.proxy-recognize');
    await page.waitForFunction(()=>document.querySelector('#proxy-type').value==='singbox' && !document.querySelector('#proxy-form-add').matches(':disabled'));
    await openProxyForm();
    await page.click('#proxy-form-add');
    await page.waitForFunction(()=>document.querySelectorAll('[data-proxy-index]').length===3 && !document.querySelector('#proxy-form-add').matches(':disabled'),undefined,{polling:100});
    assert.ok(!(await page.locator('[data-proxy-index="2"] strong').textContent()).includes(legacySsPayload));
    await page.click('#proxy-tab-list');
    await page.locator('[data-proxy-index="2"]').getByRole('button',{name:'删除第 3 条代理',exact:true}).click();
    // 分享链接协议：下拉统一归入 sing-box 节点选项，列表行显示协议://端点 与节点名。
    await openProxyForm();
    await page.fill('#proxy-paste','vless://12345678-90ab-4cde-8f90-1234567890ab@203.0.113.10:2096?security=tls&type=ws&path=%2F#NL%20Node');
    await page.click('.proxy-recognize');
    await page.waitForFunction(()=>document.querySelector('#proxy-type').value==='singbox' && !document.querySelector('#proxy-form-add').matches(':disabled'));
    // 选中 sing-box 节点后：结构化字段隐藏，粘贴框切换为节点链接输入。
    assert.equal(await page.locator('#proxy-structured-fields').evaluate(el=>el.classList.contains('hidden')),true);
    assert.equal(await page.textContent('#proxy-paste-label'),'节点链接');
    assert.equal(await page.inputValue('#proxy-paste'),'vless://12345678-90ab-4cde-8f90-1234567890ab@203.0.113.10:2096?security=tls&type=ws&path=%2F#NL%20Node');
    await screenshot('proxy-editor-singbox-node');
    await openProxyForm();
    await page.click('#proxy-form-add');
    await page.waitForFunction(()=>document.querySelectorAll('[data-proxy-index]').length===3 && !document.querySelector('#proxy-form-add').matches(':disabled'),undefined,{polling:100});
    const vlessLabel=await page.locator('[data-proxy-index="2"] strong').textContent();
    assert.ok(vlessLabel.includes('vless://203.0.113.10:2096') && vlessLabel.includes('NL Node'),vlessLabel);
    const vmessPayload=Buffer.from(JSON.stringify({v:'2',ps:'东京 01',add:'vm.example.com',port:'443',id:'11111111-2222-3333-4444-555555555555',aid:'0',scy:'auto',net:'ws',type:'none',host:'',path:'/ws',tls:'tls',sni:'',alpn:'',fp:''})).toString('base64');
    await openProxyForm();
    await page.fill('#proxy-paste','vmess://'+vmessPayload);
    await page.click('.proxy-recognize');
    await page.waitForFunction(()=>document.querySelector('#proxy-type').value==='singbox' && !document.querySelector('#proxy-form-add').matches(':disabled'));
    await openProxyForm();
    await page.click('#proxy-form-add');
    await page.waitForFunction(()=>document.querySelectorAll('[data-proxy-index]').length===4 && !document.querySelector('#proxy-form-add').matches(':disabled'),undefined,{polling:100});
    const vmessLabel=await page.locator('[data-proxy-index="3"] strong').textContent();
    assert.ok(vmessLabel.includes('vmess://vm.example.com:443') && vmessLabel.includes('东京 01'),vmessLabel);
    assert.ok(!vmessLabel.includes(vmessPayload));
    await page.click('#proxy-tab-list');
    await page.locator('[data-proxy-index="3"]').getByRole('button',{name:'删除第 4 条代理',exact:true}).click();
    await page.click('#proxy-tab-list');
    await page.locator('[data-proxy-index="2"]').getByRole('button',{name:'删除第 3 条代理',exact:true}).click();
    await openProxyForm();
    await page.fill('#proxy-paste',JSON.stringify(Array.from({length:30},(_,index)=>`http://draft-${index}.invalid:8080`)));
    await page.click('.proxy-recognize');
    await finishProxyBatch();
    await page.waitForFunction(()=>document.querySelectorAll('[data-proxy-index]').length===32 && !document.querySelector('#proxy-form-add').matches(':disabled'));
    await openProxyForm();
    await page.fill('#proxy-paste','["http://draft-0.invalid:8080","http://draft-1.invalid:8080"]');
    await page.click('.proxy-recognize');
    await finishProxyBatch(true);
    await page.waitForFunction(()=>document.querySelector('#proxy-paste').value==='' && !document.querySelector('#proxy-form-add').matches(':disabled'));
    assert.equal(await page.textContent('#proxy-editor-error'),'');
    assert.equal(await page.locator('[data-proxy-index]').count(),32);
    await page.click('#proxy-dialog-cancel');
    assert.equal(await page.inputValue('#proxy-input'),'http://proxy-second.invalid:8081\nsocks5://proxy-user:p%40ss@127.0.0.1:1080');
    check('编辑代理时排序保留输入，重复批量导入按去重后的数量校验，取消不保存草稿');

    const batchBaseline=await page.inputValue('#proxy-input');
    const batchCalls=[];
    let batchActive=0,batchMaxActive=0,failBatchNode=true,slowBatch=false;
    const slowBatchReleases=[];
    const batchProbe=async route=>{
      const body=route.request().postDataJSON();batchCalls.push(body);batchActive++;
      batchMaxActive=Math.max(batchMaxActive,batchActive);
      try{
        if(slowBatch&&body.proxy_url.includes('cancel-batch-')) await new Promise(resolve=>slowBatchReleases.push(resolve));
        else await sleep(80);
        const failed=body.proxy_url.includes('all-fail-')||(failBatchNode&&body.proxy_url.includes('batch-retry.invalid'));
        await route.fulfill({json:{ok:!failed,via_proxy:true,latency_ms:failed?8000:120,status_code:failed?504:403,
          message:failed?'本次连接超时，可重试':'目标站可达 <img src=x onerror="window.uiInjected=true">'}});
      }catch{/* Aborted browser requests have no response recipient. */}
      finally{batchActive--;}
    };
    await page.route('**/api/proxy/test',batchProbe);
    await page.getByRole('button',{name:'批量导入',exact:true}).click();
    const batchText=['http://batch-user:batch-private@batch-fast.invalid:8080#主用出口',
      'http://batch-retry.invalid:8080','socks5://batch-third.invalid:1080',
      'http://batch-user:batch-private@batch-fast.invalid:8080#同节点不同备注',
      'socks5://bad-batch.invalid:0',batchBaseline.split('\n')[0]].join('\n');
    await page.fill('#proxy-import-text',batchText);
    await page.click('#proxy-import-preview');
    await page.waitForFunction(()=>document.querySelector('#proxy-stop-tests').classList.contains('hidden')&&document.querySelector('#proxy-import-summary').textContent.includes('失败 1')&&!document.querySelector('#proxy-import-preview').matches(':disabled'));
    assert.equal(await page.locator('[data-import-id]').count(),6);
    assert.equal(await page.locator('[data-import-id] input:checked').count(),2);
    assert.equal(await page.locator('[data-import-id] [data-state="invalid"]').count(),1);
    assert.equal(await page.locator('[data-import-id] [data-state="duplicate"]').count(),2);
    assert.equal(batchMaxActive,3);
    assert.ok(!(await page.textContent('#proxy-import-list')).includes('batch-private'));
    assert.equal(await page.evaluate(()=>Boolean(window.uiInjected)),false);
    await screenshot('proxy-import-results-desktop');
    await page.click('#proxy-tab-list');
    await page.locator('[data-proxy-index="0"]').getByRole('button',{name:'下移第 1 条',exact:true}).click();
    assert.ok((await page.textContent('#proxy-result-1')).includes('75 ms'));
    await page.locator('[data-proxy-index="1"]').getByRole('button',{name:'上移第 2 条',exact:true}).click();
    await page.click('#proxy-tab-import');
    await page.selectOption('#proxy-test-target','cookie');
    assert.equal(await page.locator('[data-import-id] input:checked').count(),0);
    await page.selectOption('#proxy-test-target','vertex');
    assert.equal(await page.locator('[data-import-id] input:checked').count(),2);
    await page.locator('[data-import-id="0"] input').uncheck();
    const callsBeforeRetry=batchCalls.length;failBatchNode=false;
    await page.click('#proxy-import-retry');
    await page.waitForFunction(()=>document.querySelector('#proxy-stop-tests').classList.contains('hidden')&&document.querySelector('#proxy-import-summary').textContent.includes('通过 3'));
    assert.equal(batchCalls.length,callsBeforeRetry+1);
    assert.equal(await page.locator('[data-import-id] input:checked').count(),2);
    await page.click('#proxy-import-select');
    const failBatchSave=route=>route.request().method()==='POST'?route.fulfill({status:503,json:{error:'测试落盘失败'}}):route.fallback();
    await page.route('**/api/settings',failBatchSave);
    await page.click('#proxy-import-commit');
    await page.waitForFunction(()=>document.querySelector('#proxy-editor-error').textContent.includes('测试落盘失败')&&!document.querySelector('#proxy-import-commit').matches(':disabled'));
    assert.equal(await page.inputValue('#proxy-import-text'),batchText);
    assert.equal(await page.locator('[data-import-id] input:checked').count(),3);
    assert.equal(await page.locator('[data-proxy-index]').count(),2);
    assert.equal(await page.inputValue('#proxy-input'),batchBaseline);
    await page.unroute('**/api/settings',failBatchSave);
    let releaseBatchSave;
    const batchSaveGate=new Promise(resolve=>{releaseBatchSave=resolve;});
    const delayedBatchSave=async route=>{if(route.request().method()==='POST')await batchSaveGate;await route.fallback();};
    await page.route('**/api/settings',delayedBatchSave);
    const savingBatchRequest=page.waitForRequest(r=>new URL(r.url()).pathname==='/api/settings'&&r.method()==='POST');
    await page.click('#proxy-import-commit');await savingBatchRequest;
    assert.equal(await page.locator('#proxy-dialog-close').isDisabled(),true);
    assert.equal(await page.locator('#proxy-dialog-cancel').isDisabled(),true);
    releaseBatchSave();
    await page.waitForFunction(()=>!document.querySelector('#proxy-dialog').open);
    await page.unroute('**/api/settings',delayedBatchSave);
    const importedSettings=await (await page.request.get(origin+'/api/settings')).json();
    assert.equal(importedSettings.cookie_structured_output,true,'Proxy import must preserve the upstream structured output setting');
    const importedGlobal=importedSettings.proxy_urls;
    assert.equal(importedGlobal.length,5);
    assert.deepEqual(importedGlobal.slice(2).map(uri=>new URL(uri).hostname),['batch-fast.invalid','batch-retry.invalid','batch-third.invalid']);
    assert.equal(await page.locator('#proxy-draft-state').isVisible(),false);
    await page.fill('#proxy-input',batchBaseline);
    await page.locator('button[onclick*="saveProxy()"]').click();
    await page.waitForFunction(()=>document.querySelector('#proxy-draft-state').classList.contains('hidden')&&!document.querySelector('#proxy-input').matches(':disabled'));
    check('批量导入逐条错误、去重、三路并发、目标隔离、重试、结果保留、保存失败回滚和按原序保存');

    await page.getByRole('button',{name:'批量导入',exact:true}).click();
    await page.fill('#proxy-import-text','http://untested-one.invalid:8080\nhttp://untested-two.invalid:8080');
    await page.locator('#proxy-import-tested').uncheck();
    const callsBeforeUntested=batchCalls.length;
    await page.click('#proxy-import-preview');
    await page.waitForFunction(()=>!document.querySelector('#proxy-import-add').matches(':disabled'));
    assert.equal(batchCalls.length,callsBeforeUntested);
    await page.click('#proxy-import-add');
    await page.waitForSelector('#proxy-panel-list:not(.hidden)');
    assert.equal(await page.textContent('#proxy-result-2'),'未测试');
    await page.click('#proxy-dialog-close');
    await page.waitForSelector('#proxy-close-prompt:not(.hidden)');
    await page.locator('#proxy-close-prompt').getByRole('button',{name:'继续编辑',exact:true}).click();
    assert.equal(await page.locator('[data-proxy-index]').count(),4);
    await page.click('#proxy-dialog-cancel');
    assert.equal(await page.inputValue('#proxy-input'),batchBaseline);
    check('可选跳过测试、未测试状态、关闭前草稿保护和明确放弃操作');

    await page.getByRole('button',{name:'批量导入',exact:true}).click();
    await page.fill('#proxy-import-text',Array.from({length:40},(_,index)=>`http://capacity-${index}.invalid:8080`).join('\n'));
    await page.locator('#proxy-import-tested').uncheck();
    await page.click('#proxy-import-preview');
    await page.waitForFunction(()=>document.querySelector('#proxy-import-summary').textContent.includes('识别 40'));
    assert.equal(await page.locator('#proxy-import-commit').isDisabled(),true);
    await page.click('#proxy-import-select');
    assert.equal(await page.locator('[data-import-id] input:checked').count(),30);
    assert.equal(await page.locator('#proxy-import-commit').isDisabled(),false);
    assert.equal(await page.locator('#proxy-dialog-save').isVisible(),false);
    assert.equal(await page.locator('#proxy-dialog-apply').isVisible(),false);
    await page.click('#proxy-dialog-cancel');
    check('候选列表可超过 32 条，按剩余容量一键选择，导入视图只提供对应导入动作');

    await page.getByRole('button',{name:'批量导入',exact:true}).click();
    await page.fill('#proxy-import-text','http://all-fail-one.invalid:8080\nhttp://all-fail-two.invalid:8080');
    await page.click('#proxy-import-preview');
    await page.waitForFunction(()=>document.querySelector('#proxy-stop-tests').classList.contains('hidden')&&document.querySelector('#proxy-import-summary').textContent.includes('失败 2'));
    assert.equal(await page.locator('#proxy-import-commit').isDisabled(),true);
    assert.equal((await (await page.request.get(origin+'/api/settings')).json()).proxy_urls.length,2);
    await page.click('#proxy-dialog-cancel');
    check('全部探测失败时禁止空导入，保留原有生效代理');

    await page.getByRole('button',{name:'批量导入',exact:true}).click();
    await page.fill('#proxy-import-text',Array.from({length:8},(_,index)=>`http://cancel-batch-${index}.invalid:8080`).join('\n'));
    slowBatch=true;const callsBeforeCancel=batchCalls.length;
    await page.click('#proxy-import-preview');
    await page.waitForFunction(()=>document.querySelector('#proxy-job-state').textContent.includes('0 / 8'));
    for(let i=0;i<30&&batchCalls.length<callsBeforeCancel+3;i++)await sleep(20);
    assert.equal(batchCalls.length,callsBeforeCancel+3);
    assert.equal(await page.locator('#proxy-import-text').getAttribute('readonly'),'');
    await page.click('#proxy-stop-tests');
    await page.waitForFunction(()=>document.querySelector('#proxy-stop-tests').classList.contains('hidden'));
    assert.equal(await page.locator('[data-import-id] [data-state="cancelled"]').count(),8);
    assert.equal(await page.locator('#proxy-import-commit').isDisabled(),true);
    slowBatch=false;slowBatchReleases.splice(0).forEach(resolve=>resolve());
    await sleep(100);assert.equal(batchCalls.length,callsBeforeCancel+3);
    await page.click('#proxy-import-retry');
    await page.waitForFunction(()=>document.querySelector('#proxy-stop-tests').classList.contains('hidden')&&document.querySelector('#proxy-import-summary').textContent.includes('通过 8'));
    assert.equal(await page.locator('[data-import-id] input:checked').count(),8);
    await page.setViewportSize({width:390,height:900});
    assert.ok(await page.locator('#proxy-dialog').evaluate(el=>el.scrollWidth<=el.clientWidth));
    assert.equal(await page.locator('#proxy-import-source').evaluate(el=>el.open),false);
    assert.ok(await page.locator('[data-import-id]').first().evaluate(el=>el.getBoundingClientRect().bottom<=document.querySelector('.proxy-import-selection').getBoundingClientRect().top),'Mobile results should be visible without scrolling through the original input');
    await screenshot('proxy-import-results-mobile');
    await page.click('#proxy-dialog-cancel');
    await page.setViewportSize({width:1440,height:1000});
    await page.unroute('**/api/proxy/test',batchProbe);
    check('停止测试立即取消在途与排队项，忽略迟到结果，并可重试未完成节点；移动端导入布局');

    await page.evaluate(()=>consoleForms.openAccount('key',0));
    await page.locator('#keys-list .field-heading button').first().click();
    await page.click('#proxy-tab-list');
    await page.locator('[data-proxy-index="0"]').getByRole('button',{name:'删除第 1 条代理',exact:true}).click();
    await openProxyForm();
    await page.fill('#proxy-paste','["socks5://10.0.0.1:1080:user:pass","http://dedicated-backup:8080"]');
    await page.click('.proxy-recognize');
    await finishProxyBatch();
    await page.waitForFunction(()=>document.querySelectorAll('[data-proxy-index]').length===2 && !document.querySelector('#proxy-form-add').matches(':disabled'));
    await page.click('#proxy-dialog-apply');
    await page.waitForFunction(()=>!document.querySelector('#proxy-dialog').open);
    await page.locator('.k-fallback').first().uncheck();
    pending=responseFor('/api/express-keys');
    await page.locator('button[onclick*="saveExpressKeys"]').click();
    response=await pending;
    assert.equal(response.status(),200);
    const savedKeys=await response.json();
    assert.equal(savedKeys.keys[0].proxies.length,2);
    assert.equal(savedKeys.keys[0].proxy_fallback,false);
    check('账号专属代理批量导入与全局回退开关保存');

    pending=responseFor('/api/settings/mode');
    await page.click('input[name=mode][value=mixed]');
    response=await pending;
    assert.equal(response.status(),200);
    await page.getByRole('button',{name:'＋ 添加 Cookie',exact:true}).click();
    assert.equal(await page.evaluate(() => typeof crypto.randomUUID), 'undefined');
    await page.locator('.c-cookie').first().fill('SAPISID=ui-cookie-one; SID=ui-sid-one');
    await page.locator('.c-project').first().fill('https://console.cloud.google.com/?project=cookie-project-one');
    await page.getByRole('button',{name:'＋ 添加 Cookie',exact:true}).click();
    await page.locator('.c-cookie').nth(1).fill('SAPISID=ui-cookie-two; SID=ui-sid-two');
    await page.locator('.c-project').nth(1).fill('cookie-project-two');
    await page.locator('#cookies-list .field-heading button').first().click();
    await openProxyForm();
    await page.fill('#proxy-host','cookie-proxy.invalid');
    await openProxyForm();
    await page.fill('#proxy-port','1080');
    await openProxyForm();
    await page.fill('#proxy-username','cookie-user');
    await openProxyForm();
    await page.fill('#proxy-password','private:password');
    await openProxyForm();
    await page.click('#proxy-form-add');
    await page.waitForFunction(()=>document.querySelectorAll('[data-proxy-index]').length===1);
    await page.click('#proxy-dialog-apply');
    await page.waitForFunction(()=>!document.querySelector('#proxy-dialog').open);
    assert.equal(await page.locator('.c-cookie').nth(1).inputValue(),'SAPISID=ui-cookie-two; SID=ui-sid-two');
    await page.locator('.c-fallback').first().uncheck();
    pending=responseFor('/api/cookie-accounts');
    await page.locator('button[onclick*="saveCookieAccounts"]').click();
    response=await pending;
    assert.equal(response.status(),200);
    assert.ok(!(await response.text()).includes('ui-cookie-one'));
    await page.waitForFunction(()=>document.querySelector('.c-cookie').value==='' && !document.querySelector('.c-cookie').matches(':disabled'));
    assert.equal(await page.locator('.c-project').first().inputValue(),'cookie-project-one');
    await page.locator('.c-project').first().fill('changed-cookie-project');
    await page.locator('.c-enabled').nth(1).uncheck();
    pending=responseFor('/api/cookie-accounts');
    await page.locator('button[onclick*="saveCookieAccounts"]').click();
    response=await pending;
    assert.equal(response.status(),200);
    const savedCookies=await response.json();
    assert.equal(savedCookies.enabled,1);
    assert.equal(savedCookies.accounts[0].cookie_configured,true);
    assert.equal(savedCookies.accounts[0].proxy_fallback,false);
    assert.equal(savedCookies.accounts[0].proxies[0],'socks5://cookie-user:private%3Apassword@cookie-proxy.invalid:1080');
    assert.equal((await (await page.request.get(origin+'/api/settings/runtime')).json()).mode,'mixed');
    for(const selected of ['api_key','web_proxy','mixed']){
      pending=responseFor('/api/settings/mode');
      await page.click(`input[name=mode][value=${selected}]`);
      assert.equal((await pending).status(),200);
      await page.waitForFunction(mode=>!document.querySelector(`input[name=mode][value=${mode}]`).disabled,selected);
      assert.equal(await page.locator('#keys-box').isVisible(),selected!=='web_proxy');
      assert.equal(await page.locator('#cookie-box').isVisible(),selected!=='api_key');
      assert.equal(await page.locator('.k-key').count(),savedKeys.keys.length);
      assert.equal(await page.locator('.c-cookie').count(),savedCookies.accounts.length);
      assert.deepEqual((await (await page.request.get(origin+'/api/express-keys')).json()).keys,savedKeys.keys);
      assert.deepEqual((await (await page.request.get(origin+'/api/cookie-accounts')).json()).accounts,savedCookies.accounts);
    }
    await page.selectOption('#credential-strategy','roundrobin');
    await page.waitForFunction(()=>!document.querySelector('#credential-strategy').disabled);
    assert.equal((await (await page.request.get(origin+'/api/settings')).json()).roundrobin,true);
    check('三种模式显示隔离且保留两类凭证，混合模式支持多 Cookie 和轮询');

    await page.route('**/api/cookie-accounts/refresh',route=>route.fulfill({json:{
      results:[{id:savedCookies.accounts[0].id,label:'Cookie 1',status:'refreshed',detail:'已更新 SIDCC <img src=x>'}],
      total:1,refreshed:1,unchanged:0,errors:0,
    }}));
    pending=responseFor('/api/cookie-accounts/refresh');
    await page.getByRole('button',{name:'立即刷新',exact:true}).first().click();
    response=await pending;
    assert.deepEqual(response.request().postDataJSON(),{id:savedCookies.accounts[0].id});
    await page.waitForFunction(()=>document.querySelector('#cookie-accounts-refresh-result').textContent.includes('已更新 SIDCC'));
    assert.equal(await page.locator('#cookie-accounts-refresh-result img').count(),0);
    await page.unroute('**/api/cookie-accounts/refresh');
    check('单账号立即刷新显示结果，服务端文本按纯文本渲染');

    pending=responseFor('/api/settings');
    await page.selectOption('#credential-strategy','random');
    assert.equal((await pending).status(),200);
    assert.equal(await page.locator('#roundrobin').count(),0);
    check('账号调度仅有一个入口，选择后立即保存');

    await openTab('requests');
    await page.waitForSelector('#request-rows tr[data-request]');
    assert.equal(await page.locator('#request-rows tr[data-request]').count(), 25);
    await page.click('#request-next');
    await page.waitForFunction(() => document.querySelector('#request-page-info').textContent.startsWith('26'));
    assert.equal(await page.locator('#request-rows tr[data-request]').count(), 7);
    await page.click('#request-prev');
    await page.waitForFunction(() => document.querySelector('#request-page-info').textContent.startsWith('1–'));
    await page.selectOption('#request-filter', 'error');
    await page.waitForFunction(() => document.querySelectorAll('#request-rows tr[data-request]').length === 5);
    await page.fill('#request-search', 'missing-model-for-empty-state');
    await page.waitForSelector('#request-rows .empty-title');
    await page.fill('#request-search', '');
    await page.selectOption('#request-filter', 'all');
    await page.waitForSelector('#request-rows tr[data-request]');
    await page.locator('#request-rows tr[data-request]').first().focus();
    const focusedId = await page.locator('#request-rows tr[data-request]').first().getAttribute('data-request');
    await sleep(2200);
    assert.equal(await page.evaluate(() => document.activeElement.dataset.request), focusedId);
    await page.keyboard.press('Enter');
    await page.waitForSelector('#request-advanced');
    assert.equal(await page.locator('#request-detail-content > .request-metrics > div').count(), 6);
    await page.click('#request-advanced summary');
    await sleep(2200);
    assert.equal(await page.locator('#request-advanced').evaluate(el => el.open), true);
    assert.equal(await page.evaluate(() => document.activeElement.matches('#request-advanced > summary')), true);
    const copied = await page.evaluate(() => {
      const range=document.createRange();range.selectNodeContents(document.querySelector('#request-detail-logs'));
      const selection=getSelection();selection.removeAllRanges();selection.addRange(range);return selection.toString();
    });
    await sleep(2200);
    assert.equal(await page.evaluate(() => getSelection().toString()), copied);
    await page.evaluate(() => getSelection().removeAllRanges());
    assert.equal(await page.evaluate(() => Boolean(window.uiInjected)), false);
    await screenshot('request-detail-desktop');
    await page.keyboard.press('Escape');
    await page.route('**/api/requests?*', route => route.fulfill({status: 503, json: {error: '暂不可用'}}));
    await page.click('#request-refresh');
    await page.waitForSelector('#request-refresh-state.error-message');
    await page.unroute('**/api/requests?*');
    await page.click('#request-refresh');
    await page.waitForFunction(() => !document.querySelector('#request-refresh-state').classList.contains('error-message'));
    check('请求分页、筛选、空态、失败恢复、详情展开与刷新焦点');

    for (const width of [1440, 1024, 768, 390, 320]) {
      await page.setViewportSize({width, height: 900});
      for (const tab of ['overview', 'channel', 'params', 'runtime', 'requests', 'logs']) {
        await openTab(tab);
        await page.evaluate(() => scrollTo(0, 0));
        assert.ok(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth), `${width}px ${tab}: horizontal overflow`);
        if (width === 390 || width === 1440) await screenshot(`${tab}-${width}`);
        if(tab==='channel'){await page.locator('.account-editor').evaluateAll(items=>items.forEach(el=>el.open=false));assert.ok(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth));}
        if(tab==='params'||tab==='runtime'){
          const target=tab==='params'?'model-instructions':'runtime-compatibility';
          await page.click(`#view-${tab} .settings-jump a[href="#${target}"]`);
          await page.waitForFunction(id=>{
            const nav=[...document.querySelectorAll('.tab-bar,.mobile-navigation')].find(el=>el.getClientRects().length);
            const scope=document.querySelector('#model-scope');
            const scopeBottom=scope.getClientRects().length&&getComputedStyle(scope).position==='sticky'?scope.getBoundingClientRect().bottom:0;
            const top=document.getElementById(id).getBoundingClientRect().top;
            return top>=Math.max(nav?.getBoundingClientRect().bottom||0,scopeBottom)-1&&top<innerHeight;
          },target);
        }
      }
      await openTab('requests');
      await page.locator('#request-rows tr[data-request]').first().click();
      await page.waitForSelector('#request-advanced');
      assert.ok(await page.locator('#request-dialog').evaluate(el => el.scrollWidth <= el.clientWidth), `${width}px dialog overflow`);
      if (width === 390) await screenshot('request-detail-mobile');
      await page.keyboard.press('Escape');
      await openTab('channel');
      await page.getByRole('button',{name:'管理 / 添加代理',exact:true}).click();
      assert.ok(await page.locator('#proxy-dialog').evaluate(el=>el.scrollWidth<=el.clientWidth),`${width}px proxy dialog overflow`);
      assert.ok(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth),`${width}px modal page overflow`);
      if(width===390) await screenshot('proxy-editor-mobile');
      if(width===320 || width===390){
        await openProxyForm();
        await page.selectOption('#proxy-type','sstp');
        await page.locator('#proxy-advanced summary').click();
        await openProxyForm();
        await page.fill('#proxy-ca','/app/certificates/company.pem');
        await openProxyForm();
        await page.fill('#proxy-mtu','1280');
        assert.ok(await page.locator('#proxy-dialog').evaluate(el=>el.scrollWidth<=el.clientWidth),`${width}px SSTP advanced settings overflow`);
        await page.locator('#proxy-password').click();
        await openProxyForm();
        await page.fill('#proxy-host','mobile-draft.invalid');
        await openProxyForm();
        await page.fill('#proxy-port','1080');
        await openProxyForm();
        await page.fill('#proxy-password','mobile-draft');
        const fieldBounds=await page.locator('#proxy-password').evaluate(el=>{
          const rect=el.getBoundingClientRect(), header=document.querySelector('.proxy-dialog-head').getBoundingClientRect(), footer=document.querySelector('.proxy-dialog-foot').getBoundingClientRect();
          return {top:rect.top,bottom:rect.bottom,header:header.bottom,footer:footer.top};
        });
        await screenshot(`proxy-editor-${width}-fields`);
        // Layout coordinates use fractional CSS pixels while native focus
        // scrolling rounds to device pixels. Allow one pixel of rounding.
        assert.ok(fieldBounds.top>=fieldBounds.header-1 && fieldBounds.bottom<=fieldBounds.footer+1,`${width}px mobile password field obscured: ${JSON.stringify(fieldBounds)}`);
      }
      await page.keyboard.press('Escape');
      if(await page.locator('#proxy-close-prompt').isVisible()) await page.locator('#proxy-close-prompt').getByRole('button',{name:'放弃并关闭',exact:true}).click();
      assert.equal(await page.locator('#proxy-dialog').evaluate(el=>el.open),false);
    }
    check('六个页面、分区跳转遮挡及长请求详情通过 320 / 390 / 768 / 1024 / 1440px 宽度检查');

    await openTab('logs');
    await page.waitForSelector('.log-line.error');
    await page.selectOption('#log-level', 'error');
    assert.equal(await page.locator('.log-line:not([hidden]):not(.error)').count(), 0);
    await page.fill('#log-search', 'UI_TEST_ERROR');
    assert.equal(await page.locator('.log-line:not([hidden])').count(), 1);
    await page.fill('#log-search', 'NO_MATCH_LOG');
    assert.equal(await page.getAttribute('#logwin', 'data-empty'), 'true');
    await page.selectOption('#log-level','all');
    await page.fill('#log-search','');
    if(await page.getAttribute('#log-follow','aria-pressed')==='true') await page.click('#log-follow');
    await page.evaluate(()=>{const box=document.querySelector('#logwin');box.scrollTop=box.scrollHeight;});
    await sleep(100);
    assert.equal(await page.getAttribute('#log-follow','aria-pressed'),'false');
    await page.click('#log-clear');
    assert.equal(await page.locator('.log-line').count(), 0);
    await page.click('#log-follow');
    assert.equal(await page.getAttribute('#log-follow', 'aria-pressed'), 'true');
    check('日志搜索、级别、空态、清空与自动滚动');

    await page.route('**/api/stats', route => route.fulfill({json: {total: 0, success: 0, error: 0, retries: 0, uptime: 0}}));
    await openTab('overview');
    await page.evaluate(() => fetchStats());
    await page.waitForFunction(() => document.querySelector('#health-rate').textContent === '—');
    assert.equal(await page.textContent('#health-rate'), '—');
    assert.equal(await page.evaluate(() => document.querySelector('#prompt-bar').style.width), '0%');
    check('零请求与零 Token 正确显示，避免 NaN 或虚假比例');

    const cachedComponentPage=await context.newPage();
    let componentRequests=0;
    await cachedComponentPage.route('**/static/channel-config.js*',route=>{
      componentRequests++;
      // A previous deployment used top-level names. Retrying the current
      // component must not collide with those cached declarations.
      return componentRequests===1?route.fulfill({contentType:'application/javascript',body:'let COOKIE_ACCOUNTS=[], cookiesLoaded=false, proxyEditor=null;'}):route.continue();
    });
    await cachedComponentPage.goto(origin+'/#channel');
    await cachedComponentPage.waitForFunction(()=>window.consoleChannels && document.querySelectorAll('[data-cookie-index]').length===2 && document.querySelectorAll('.k-key').length===2);
    assert.equal(componentRequests,2);
    assert.equal(await cachedComponentPage.inputValue('#first_reply_timeout_seconds'),'35');
    await cachedComponentPage.getByRole('button',{name:'管理 / 添加代理',exact:true}).click();
    await cachedComponentPage.waitForSelector('#proxy-dialog[open]');
    await cachedComponentPage.keyboard.press('Escape');
    await cachedComponentPage.close();
    check('旧版缓存脚本自动重新加载，账号与代理组件重试不发生全局变量冲突');

    const originalSettings=await (await page.request.get(origin+'/api/settings')).json();
    const offlineProxy='socks5://invalid-user:invalid-pass@127.0.0.1:9';
    assert.equal((await page.request.post(origin+'/api/settings',{data:{proxy_urls:[offlineProxy]}})).status(),200);
    const offlinePage=await context.newPage();
    await offlinePage.route('**/static/channel-config.js*',route=>route.abort());
    await offlinePage.goto(origin+'/#channel');
    await offlinePage.waitForSelector('#console-assets-error:not(.hidden)');
    await offlinePage.waitForFunction(()=>settingsLoaded && keysLoaded && runtimeLoaded && capabilitiesLoaded);
    assert.equal(await offlinePage.locator('.k-key').count(),2);
    assert.ok(await offlinePage.locator('#model-sel option').count()>1);
    assert.equal(await offlinePage.inputValue('#proxy-input'),offlineProxy);
    await offlinePage.fill('#proxy-input','http://replacement.invalid:8080');
    pending=offlinePage.waitForResponse(r=>new URL(r.url()).pathname==='/api/settings' && r.request().method()==='POST');
    await offlinePage.locator('button[onclick*="saveProxy()"]').click();
    response=await pending;
    assert.equal(response.status(),200);
    assert.deepEqual((await response.json()).proxy_urls,['http://replacement.invalid:8080']);
    await offlinePage.unroute('**/static/channel-config.js*');
    await offlinePage.evaluate(()=>reloadConfiguration());
    await offlinePage.waitForFunction(()=>window.consoleChannels && document.querySelectorAll('[data-cookie-index]').length===2);
    assert.equal(await offlinePage.locator('#console-assets-error').isHidden(),true);
    await offlinePage.getByRole('button',{name:'管理 / 添加代理',exact:true}).click();
    await offlinePage.waitForSelector('#proxy-dialog[open]');
    await offlinePage.keyboard.press('Escape');
    await offlinePage.close();
    assert.equal((await page.request.post(origin+'/api/settings',{data:{proxy_urls:originalSettings.proxy_urls}})).status(),200);
    check('代理离线且编辑器脚本不可用时，Key、模型与参数仍加载，原代理可修改保存，组件可重新加载恢复');

    const mismatchedPage=await context.newPage();
    await mismatchedPage.route(origin+'/',async route=>{
      const response=await route.fetch();
      await route.fulfill({response,body:(await response.text()).replace('id="proxy-dialog-close"','id="old-proxy-dialog-close"')});
    });
    await mismatchedPage.goto(origin+'/#channel');
    await mismatchedPage.waitForSelector('#console-assets-error:not(.hidden)');
    await mismatchedPage.waitForFunction(()=>settingsLoaded && keysLoaded && capabilitiesLoaded && document.querySelectorAll('[data-cookie-index]').length===2);
    assert.ok(await mismatchedPage.locator('#model-sel option').count()>1);
    assert.equal(await mismatchedPage.inputValue('#proxy-input'),originalSettings.proxy_urls.join('\n'));
    assert.ok((await mismatchedPage.textContent('#console-assets-error')).includes('版本不一致'));
    await mismatchedPage.close();
    await page.bringToFront();
    check('弹窗初始化异常不阻断 Cookie、Key、代理和本地模型目录加载');

    const failedPage = await context.newPage();
    await failedPage.route('**/api/settings', route => route.fulfill({status: 503, json: {error: 'test'}}));
    await failedPage.goto(origin + '/#params');
    await failedPage.waitForSelector('#params-load-error:not(.hidden)');
    await failedPage.locator('#btn-scope-save').click();
    await failedPage.waitForFunction(() => document.querySelector('#toast').textContent.includes('尚未加载'));
    await failedPage.unroute('**/api/settings');
    await failedPage.locator('#params-load-error button').click();
    await failedPage.waitForFunction(() => document.querySelector('#params-load-error').classList.contains('hidden'));
    await failedPage.waitForFunction(() => document.querySelector('#first_reply_timeout_seconds').value === '35' && !document.querySelector('#params-load-error button').disabled);
    assert.equal(await failedPage.getAttribute('#tab-params', 'aria-selected'), 'true');
    check('配置加载失败提示、禁止空数据保存、重试与链接定位');
    await failedPage.goto(origin+'/#global-settings');
    await failedPage.waitForFunction(()=>document.querySelector('#tab-runtime').getAttribute('aria-selected')==='true');
    await failedPage.reload();
    await failedPage.waitForFunction(()=>document.querySelector('#tab-runtime').getAttribute('aria-selected')==='true');
    assert.equal(await failedPage.locator('#global-settings').isVisible(),true);
    await failedPage.goto(origin+'/#model-scope');
    await failedPage.waitForFunction(()=>document.querySelector('#tab-params').getAttribute('aria-selected')==='true');
    await failedPage.waitForFunction(()=>document.querySelector('#first_reply_timeout_seconds').value==='35');
    assert.equal(await failedPage.locator('#scope-draft-state').isVisible(),false);
    check('参数分区链接在直接打开及刷新后定位正确，本页草稿不会跨刷新残留');
    await page.bringToFront();
    const beforeLogout=page.url();
    await page.route('**/api/logout',route=>route.fulfill({status:503,json:{error:'测试退出失败'}}));
    pending=responseFor('/api/logout');
    await page.locator('button[onclick="logout()"]').click();
    await pending;
    await page.waitForFunction(()=>document.querySelector('#toast').textContent.includes('测试退出失败'));
    assert.equal(page.url(),beforeLogout);
    assert.equal((await page.request.get(origin+'/api/stats')).status(),200);
    await page.unroute('**/api/logout');
    await page.locator('button[onclick="logout()"]').click();
    await page.waitForSelector('#pw');
    check('退出失败保留当前页面并提示原因，成功退出返回登录页');
    assert.deepEqual(errors, [], 'Browser JavaScript errors');
    assert.deepEqual(external, [], 'Console must work without external requests');
    console.log(`\n${passed} 组浏览器检查通过；无 JavaScript 异常或第三方资源请求。`);
  } finally {
    if (browser) await browser.close();
    server.kill('SIGTERM');
    await new Promise(resolve => {if (server.exitCode !== null) resolve();else server.once('exit', resolve);});
    await rm(state, {recursive: true, force: true});
  }
}
main().catch(error => {console.error(error);process.exitCode = 1;});
