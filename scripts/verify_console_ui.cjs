/* Optional browser regression. Requires Playwright + Chromium and requirements-dev.txt.
 * Run: node scripts/verify_console_ui.cjs
 * PLAYWRIGHT_CHROMIUM_EXECUTABLE can select an existing Chromium binary.
 * UI_SCREENSHOT_DIR optionally saves screenshots. Uses temporary state and fake keys;
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
app_state.update_settings({'image_aspect_ratio':'16:9','express_ua_profile':'custom','express_ua_custom':'fixture-gateway/'+'x'*360})
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
      VERTEX_EXPRESS_API_KEY: '', GOOGLE_COOKIE: '', GOOGLE_PROJECT_ID: '', PROXY_URL: '', MODELS_CONFIG_URL: ''},
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
    const errors = [], external = [];
    context.on('page', page => page.on('pageerror', error => errors.push(error.message)));
    await context.route('**/*', route => {
      if (!route.request().url().startsWith(origin)) {external.push(route.request().url());return route.abort();}
      return route.continue();
    });
    const page = await context.newPage();
    const responseFor = (pathname, method = 'POST') => page.waitForResponse(r => new URL(r.url()).pathname === pathname && r.request().method() === method);
    const screenshot = async name => {if (screenshots) await page.screenshot({path: path.join(screenshots, name + '.png'), fullPage: false});};
    const openTab = async tab => {await page.evaluate(t => switchTab(t), tab);};
    await page.goto(origin);
    await page.fill('#pw', 'wrong-password');
    await page.click('#login-submit');
    await page.waitForFunction(() => document.querySelector('#err').textContent.includes('密码错误'));
    await page.click('#password-toggle');
    assert.equal(await page.getAttribute('#pw', 'type'), 'text');
    await page.fill('#pw', 'ui-browser-test');
    await page.click('#login-submit');
    await page.waitForSelector('#s-total');
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
    await page.selectOption('#model-sel', 'gemini-4-argon');
    await page.fill('#first_reply_timeout_seconds', '28.5');
    await page.check('#prefill_cot_guard');
    let pending = responseFor('/api/model-overrides/gemini-4-argon');
    await page.click('#btn-scope-save');
    let response = await pending;
    assert.equal(response.status(), 200);
    assert.equal(response.request().postDataJSON().prefill_cot_guard, true);
    assert.equal(response.request().postDataJSON().first_reply_timeout_seconds, 28.5);
    await page.waitForFunction(() => !document.querySelector('#btn-scope-save').disabled);
    pending = responseFor('/api/settings');
    await page.locator('button[onclick*="saveSettings"]').click();
    response = await pending;
    assert.equal(response.status(), 200);
    assert.ok(!('first_reply_timeout_seconds' in response.request().postDataJSON()));
    assert.ok(!('prefill_cot_guard' in response.request().postDataJSON()));
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
    pending=responseFor('/api/model-overrides/gemini-4-argon');
    await page.click('#btn-scope-save');
    response=await pending;
    assert.equal(response.status(),400);
    await page.waitForFunction(()=>document.querySelector('#default_top_p').getAttribute('aria-invalid')==='true' && document.activeElement.id==='default_top_p');
    assert.match(await page.textContent('#toast'), /default_top_p.*\[0, 1\]/);
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
    const runtimeResponse=page.waitForResponse(r=>new URL(r.url()).pathname==='/api/settings' && r.request().postDataJSON()?.retry_max!==undefined);
    await page.locator('button[onclick*="saveSettings"]').click();
    await runtimeReady;
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
      for (const tab of ['overview', 'channel', 'params', 'requests', 'logs']) {
        await openTab(tab);
        await page.evaluate(() => scrollTo(0, 0));
        assert.ok(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth), `${width}px ${tab}: horizontal overflow`);
        if (width === 390 || width === 1440) await screenshot(`${tab}-${width}`);
      }
      await openTab('requests');
      await page.locator('#request-rows tr[data-request]').first().click();
      await page.waitForSelector('#request-advanced');
      assert.ok(await page.locator('#request-dialog').evaluate(el => el.scrollWidth <= el.clientWidth), `${width}px dialog overflow`);
      if (width === 390) await screenshot('request-detail-mobile');
      await page.keyboard.press('Escape');
    }
    check('五个页面及长请求详情通过 320 / 390 / 768 / 1024 / 1440px 宽度检查');

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
    await failedPage.waitForFunction(()=>document.querySelector('#tab-params').getAttribute('aria-selected')==='true');
    await failedPage.reload();
    await failedPage.waitForFunction(()=>document.querySelector('#tab-params').getAttribute('aria-selected')==='true');
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
