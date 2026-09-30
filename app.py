"""Password protected management page; run exactly one Uvicorn worker."""
import asyncio
import contextlib
import fcntl
import hashlib
import hmac
import json
import logging
import os
import secrets
import time
from collections import deque
from pathlib import Path

import uvicorn
from starlette.applications import Starlette
from starlette.exceptions import HTTPException
from starlette.middleware import Middleware
from starlette.middleware.sessions import SessionMiddleware
from starlette.responses import FileResponse, JSONResponse
from starlette.routing import Route

import engine as e
from service import CloudBot, LineClient, public_error

ROOT = Path(__file__).parent


class Runtime:
    def __init__(self, data_dir):
        self.data_dir = data_dir
        self.bot = None
        self.error = ''
        self.client = None

    async def start(self):
        while self.bot is None:
            try:
                self.client = LineClient(os.environ.get('LINE_CHANNEL_ACCESS_TOKEN', ''), os.environ.get('LINE_USER_ID', ''))
                state = await self.client.check()
                self.bot = CloudBot(state, self.client.push, self.data_dir / 'state.sqlite3')
                self.bot.start()
                self.error = ''
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.error = public_error(exc)
                logging.getLogger('jin10').error('初始化尚未完成：%s', self.error)
                if self.client:
                    await self.client.client.aclose()
                await asyncio.sleep(60)

    async def close(self):
        if self.bot:
            await self.bot.close()
        if self.client:
            await self.client.client.aclose()


class WebPolicy:
    """Bound request bodies before parsing; browser security headers."""
    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope['type'] != 'http':
            return await self.app(scope, receive, send)
        if scope['method'] == 'POST':
            chunks, total = [], 0
            while True:
                message = await receive()
                if message['type'] == 'http.disconnect':
                    return
                total += len(message.get('body', b''))
                if total > 256000:
                    return await JSONResponse({'error': '資料過大'}, status_code=413)(scope, receive, send)
                chunks.append(message.get('body', b''))
                if not message.get('more_body'):
                    break
            consumed = False
            original_receive = receive

            async def bounded_receive():
                nonlocal consumed
                if not consumed:
                    consumed = True
                    return {'type': 'http.request', 'body': b''.join(chunks), 'more_body': False}
                return await original_receive()
            receive = bounded_receive

        async def add_headers(message):
            if message['type'] == 'http.response.start':
                message['headers'] = list(message.get('headers', [])) + [
                    (b'cache-control', b'no-store'), (b'x-content-type-options', b'nosniff'),
                    (b'x-frame-options', b'DENY'), (b'referrer-policy', b'no-referrer'),
                    (b'content-security-policy', b"default-src 'self'; script-src 'self'; style-src 'self'; connect-src 'self'; img-src 'self' data:; frame-ancestors 'none'; base-uri 'none'; form-action 'self'")]
            await send(message)
        await self.app(scope, receive, add_headers)


def create_app(*, password=None, data_dir=None, runtime=None, secure_cookie=True):
    password = password if password is not None else os.environ.get('ADMIN_PASSWORD', '')
    if len(password) < 16 or len(password) > 256:
        raise RuntimeError('ADMIN_PASSWORD 請設為 16～256 字元，作為你自己的管理頁密碼。')
    selected_dir = data_dir or os.environ.get('DATA_DIR') or os.environ.get('RAILWAY_VOLUME_MOUNT_PATH') or './data'
    data_dir = Path(selected_dir)
    real_runtime = runtime is None
    runtime = runtime or Runtime(data_dir)
    failures = deque(maxlen=100)
    session_secret = hashlib.sha256(('jin10-admin-session:' + password).encode()).hexdigest()

    @contextlib.asynccontextmanager
    async def lifespan(app):
        data_dir.mkdir(parents=True, exist_ok=True)
        # Railway deployments must have a real volume attached for persistence.
        if real_runtime and os.environ.get('RAILWAY_ENVIRONMENT_ID'):
            mount = os.environ.get('RAILWAY_VOLUME_MOUNT_PATH')
            if not mount or Path(mount).resolve() != data_dir.resolve():
                raise RuntimeError('請先在 Railway 掛載 Volume 到 /data，並設定 DATA_DIR=/data。')
        lock = (data_dir / '.instance.lock').open('a')
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            lock.close()
            raise RuntimeError('同一資料夾已有程式執行；請保持 1 個 replica、1 個 worker。') from None
        task = asyncio.create_task(runtime.start()) if real_runtime else None
        app.state.runtime = runtime
        try:
            yield
        finally:
            if task:
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
            await runtime.close()
            lock.close()

    def authorized(request):
        if request.session.get('owner') is not True:
            raise HTTPException(401, '請先登入。')

    def bot_or_error():
        if runtime.bot is None:
            raise HTTPException(503, runtime.error or '正在連線，請稍候。')
        return runtime.bot

    async def body(request, *, login=False):
        if request.headers.get('content-type', '').split(';')[0].strip() != 'application/json':
            raise HTTPException(415, '請使用 JSON。')
        if request.headers.get('sec-fetch-site') == 'cross-site':
            raise HTTPException(403, '來源驗證失敗。')
        if not login:
            authorized(request)
            token = request.headers.get('x-csrf-token', '')
            expected = request.session.get('csrf', '')
            if not expected or not hmac.compare_digest(token.encode(), expected.encode()):
                raise HTTPException(403, '頁面已過期，請重新登入。')
        try:
            value = await request.json()
        except (ValueError, UnicodeError):
            raise HTTPException(400, 'JSON 格式不正確。') from None
        if not isinstance(value, dict):
            raise HTTPException(400, '資料必須是物件。')
        return value

    async def index(request):
        return FileResponse(ROOT/'web/index.html')

    async def asset(request):
        name = request.path_params['name']
        if name not in ('app.js', 'style.css'):
            raise HTTPException(404)
        return FileResponse(ROOT/'web'/name)

    async def health(request):
        return JSONResponse({'status': 'ok'})

    async def login(request):
        data = await body(request, login=True)
        now = time.monotonic()
        while failures and now-failures[0] > 300:
            failures.popleft()
        if len(failures) >= 20:
            raise HTTPException(429, '嘗試次數過多，請 5 分鐘後再試。')
        value = data.get('password')
        if not isinstance(value, str) or not hmac.compare_digest(value.encode(), password.encode()):
            failures.append(now)
            raise HTTPException(401, '密碼不正確。')
        request.session.clear()
        request.session.update(owner=True, csrf=secrets.token_urlsafe(32))
        return JSONResponse({'ok': True})

    async def logout(request):
        await body(request)
        request.session.clear()
        return JSONResponse({'ok': True})

    async def status(request):
        authorized(request)
        bot = runtime.bot
        result = {'ready': bot is not None, 'error': runtime.error, 'csrf': request.session['csrf']}
        if bot:
            result.update(enabled=bot.enabled, rules=bot.rules, ws_status=bot.ws_status,
                ws_error=bot.ws_error, line_error=bot.line_error, last_event=bot.last_event,
                counts=bot.counts(), stats=dict(bot.stats), recent=list(reversed(bot.recent)),
                logs=list(reversed(bot.logs))[:15], recipient=bot.state.get('recipient_name',''),
                bot_name=bot.state.get('bot_name', ''))
        return JSONResponse(result)

    async def settings(request):
        data = await body(request)
        rules = bot_or_error().save_rules(data)
        return JSONResponse({'rules': rules})

    async def trial(request):
        data = await body(request)
        text = data.get('text')
        if not isinstance(text, str) or not 1 <= len(text.strip()) <= 30000:
            raise HTTPException(400, '請貼一則新聞原文，最多 30,000 字。')
        rules = e.validate_user_filters(data.get('rules'))
        return JSONResponse(e.classify_with_user_filters(e.text_only(text), rules))

    async def control(request):
        data = await body(request)
        bot = bot_or_error()
        action = data.get('action')
        if action == 'notify':
            bot.set_enabled(data.get('enabled'))
        elif action == 'retry-line':
            bot.resume_line()
        elif action == 'reconnect':
            bot.reconnect_event.set()
        else:
            raise HTTPException(400, '不支援的操作。')
        return JSONResponse({'ok': True})

    async def backup(request):
        authorized(request)
        return JSONResponse(bot_or_error().rules, headers={'Content-Disposition': 'attachment; filename="user_filters.json"'})

    async def http_error(request, exc):
        return JSONResponse({'error': exc.detail}, status_code=exc.status_code)

    async def bot_error(request, exc):
        return JSONResponse({'error': str(exc)}, status_code=400)

    app = Starlette(routes=[
        Route('/', index), Route('/assets/{name}', asset), Route('/healthz', health),
        Route('/api/login', login, methods=['POST']), Route('/api/logout', logout, methods=['POST']),
        Route('/api/status', status), Route('/api/settings', settings, methods=['POST']),
        Route('/api/trial', trial, methods=['POST']), Route('/api/control', control, methods=['POST']),
        Route('/api/backup', backup)],
        middleware=[Middleware(WebPolicy), Middleware(SessionMiddleware, secret_key=session_secret,
                    https_only=secure_cookie, same_site='strict', max_age=12*3600)],
        lifespan=lifespan, exception_handlers={HTTPException: http_error, e.BotError: bot_error})
    return app


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')
    for name in ('httpx2','httpcore2','httpx','httpcore','websockets'):
        logging.getLogger(name).setLevel(logging.CRITICAL)
    local_http = os.environ.get('LOCAL_HTTP') == '1' and not os.environ.get('RAILWAY_ENVIRONMENT_ID')
    uvicorn.run(create_app(secure_cookie=not local_http), host='0.0.0.0',
                port=int(os.environ.get('PORT', '8080')), workers=1,
                proxy_headers=True, forwarded_allow_ips='*', access_log=False)
