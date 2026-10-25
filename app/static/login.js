/* No third-party scripts are needed to authenticate. */
const password = document.getElementById('pw');
const toggle = document.getElementById('password-toggle');
toggle.addEventListener('click', () => {
  const visible = password.type === 'password';
  password.type = visible ? 'text' : 'password';
  toggle.textContent = visible ? '隐藏' : '显示';
  toggle.setAttribute('aria-label', visible ? '隐藏密码' : '显示密码');
  toggle.setAttribute('aria-pressed', String(visible));
});
document.getElementById('login-form').addEventListener('submit', async (event) => {
  event.preventDefault();
  const button = document.getElementById('login-submit');
  const error = document.getElementById('err');
  if (button.disabled) return;
  button.disabled = true;
  // 只改文本节点，保留内部装饰性箭头 span（aria-hidden，读屏器应忽略）。
  // 旧实现用 textContent 赋值会销毁 span，恢复后的箭头变成裸文本节点。
  const label = button.firstChild;
  if (label) label.textContent = '正在登录…';
  error.textContent = '';
  password.removeAttribute('aria-invalid');
  try {
    const response = await fetch('/api/login', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ password: password.value }),
      signal: AbortSignal.timeout(15000)
    });
    if (response.ok) {
      location.assign('/');
      return;
    }
    const result = await response.json().catch(() => ({}));
    error.textContent =
      result.error || (response.status === 429 ? '尝试过于频繁，请稍后再试' : '登录失败，请重试');
    if (response.status === 401) {
      password.setAttribute('aria-invalid', 'true');
      password.focus();
      password.select();
    }
  } catch (err) {
    error.textContent =
      err.name === 'TimeoutError' ? '连接超时，请稍后再试' : '无法连接服务，请检查网络后重试';
  } finally {
    button.disabled = false;
    const label = button.firstChild;
    if (label) label.textContent = '进入控制台 ';
  }
});
