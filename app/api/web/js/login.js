const loginForm = document.querySelector('#login-form');
const usernameInput = document.querySelector('#login-username');
const passwordInput = document.querySelector('#login-password');
const passwordToggle = document.querySelector('#login-password-toggle');
const submitButton = document.querySelector('#login-submit');
const messageBox = document.querySelector('#login-message');

function setLoginMessage(message, type = 'error') {
  messageBox.textContent = message;
  messageBox.classList.toggle('is-info', type === 'info');
}

function requestedDestination() {
  return new URLSearchParams(window.location.search).get('next') || '/';
}

passwordToggle.addEventListener('click', () => {
  const revealing = passwordInput.type === 'password';
  passwordInput.type = revealing ? 'text' : 'password';
  passwordToggle.textContent = revealing ? '隐藏' : '显示';
  passwordToggle.setAttribute('aria-label', revealing ? '隐藏密码' : '显示密码');
  passwordInput.focus();
});

loginForm.addEventListener('submit', async (event) => {
  event.preventDefault();
  const username = usernameInput.value.trim();
  const password = passwordInput.value;
  if (!username || !password) {
    setLoginMessage('请输入用户名和密码。');
    (username ? passwordInput : usernameInput).focus();
    return;
  }

  submitButton.disabled = true;
  setLoginMessage('正在验证身份…', 'info');

  try {
    const response = await fetch('/login', {
      method: 'POST',
      credentials: 'same-origin',
      headers: {
        'Content-Type': 'application/json',
        'Accept': 'application/json',
      },
      body: JSON.stringify({
        username,
        password,
        next: requestedDestination(),
      }),
    });

    if (response.status === 429) {
      const retryAfter = Number(response.headers.get('Retry-After') || 60);
      setLoginMessage(`尝试次数过多，请在约 ${retryAfter} 秒后再试。`);
      return;
    }
    if (response.status === 401) {
      setLoginMessage('用户名或密码不正确。');
      passwordInput.select();
      return;
    }
    if (!response.ok) {
      setLoginMessage('登录服务暂时不可用，请稍后再试。');
      return;
    }

    const result = await response.json();
    setLoginMessage('验证通过，正在进入控制台…', 'info');
    window.location.replace(result.redirect_to || '/');
  } catch (error) {
    setLoginMessage('网络连接失败，请检查连接后重试。');
  } finally {
    submitButton.disabled = false;
  }
});
