"""Single-process, persistent Jin10 receiver and LINE outbox."""
import asyncio
import contextlib
import json
import logging
import os
import random
import re
from collections import OrderedDict, deque
from datetime import datetime, timezone
from pathlib import Path

import httpx2
from websockets.exceptions import ConnectionClosed, InvalidStatus

import engine as e


class BoundedSeen:
    def __init__(self, values=(), limit=50000):
        self.items = OrderedDict.fromkeys(values)
        self.limit = limit

    def __contains__(self, value):
        return value in self.items

    def __len__(self):
        return len(self.items)

    def add(self, value):
        self.items[value] = None
        self.items.move_to_end(value)
        while len(self.items) > self.limit:
            self.items.popitem(last=False)


def redact(value):
    text = str(value)
    for name in ('LINE_CHANNEL_ACCESS_TOKEN', 'LINE_USER_ID', 'ADMIN_PASSWORD'):
        secret = os.environ.get(name, '')
        if secret:
            text = text.replace(secret, '[已遮蔽]')
    text = re.sub(r'\bU[0-9a-fA-F]{32}\b', '[LINE ID]', text)
    return text


def public_error(exc):
    return redact(exc) if isinstance(exc, (e.BotError, e.ProtocolError)) else type(exc).__name__


class LineClient:
    def __init__(self, token, user_id):
        token = token.removeprefix('Bearer ').strip()
        if not token or any(c.isspace() for c in token):
            raise e.BotError('請在 Railway Variables 填入 LINE_CHANNEL_ACCESS_TOKEN。')
        if not re.fullmatch(r'U[0-9a-fA-F]{32}', user_id):
            raise e.BotError('LINE_USER_ID 請填你自己的 Your user ID。')
        self.token, self.user_id = token, user_id
        self.client = httpx2.AsyncClient(timeout=15, follow_redirects=False)

    async def check(self):
        headers = {'Authorization': 'Bearer ' + self.token}
        bot = await self.client.get('https://api.line.me/v2/bot/info', headers=headers)
        if bot.status_code != 200:
            raise e.BotError(f'LINE 帳號檢查 HTTP {bot.status_code}，請確認 Token。')
        info = bot.json()
        if info.get('userId') == self.user_id:
            raise e.BotError('LINE_USER_ID 填到了 Bot 本身，請改填你的個人 ID。')
        person = await self.client.get('https://api.line.me/v2/bot/profile/' + self.user_id, headers=headers)
        if person.status_code != 200:
            raise e.BotError(f'LINE 收件人檢查 HTTP {person.status_code}，請確認 ID 並加入 Bot 好友。')
        return {'token': self.token, 'user_id': self.user_id, 'bot_id': info['userId'],
                'bot_name': info.get('displayName', ''), 'recipient_name': person.json().get('displayName', '')}

    async def push(self, state, text, retry_key):
        return await self.client.post('https://api.line.me/v2/bot/message/push',
            headers={'Authorization': 'Bearer ' + state['token'], 'X-Line-Retry-Key': retry_key},
            json={'to': state['user_id'], 'messages': [{'type': 'text', 'text': text}]})


class CloudBot(e.WSNewsBot):
    def __init__(self, state, push, path, **kwargs):
        super().__init__(state, push, path, redactor=redact, **kwargs)
        self.db.execute('PRAGMA journal_mode=WAL')
        self.db.execute('PRAGMA synchronous=FULL')
        self.db.execute('CREATE TABLE IF NOT EXISTS settings (scope TEXT,key TEXT,value TEXT,PRIMARY KEY(scope,key))')
        self.db.execute('CREATE TABLE IF NOT EXISTS seen (scope TEXT,key TEXT,stamp REAL,PRIMARY KEY(scope,key))')
        self.db.execute('CREATE TABLE IF NOT EXISTS source_cache (scope TEXT,id TEXT,value TEXT,stamp REAL,PRIMARY KEY(scope,id))')
        columns = {r['name'] for r in self.db.execute('PRAGMA table_info(outbox)')}
        for name, kind in (('news_id', 'TEXT'), ('row_json', 'TEXT')):
            if name not in columns:
                self.db.execute(f'ALTER TABLE outbox ADD COLUMN {name} {kind}')
        self.db.commit()
        self.rules = e.validate_user_filters(self.read_setting('filters', e.DEFAULT_USER_FILTERS))
        e.USER_FILTERS = self.rules
        self.enabled = self.read_setting('enabled', False) is True
        self.line_error = self.read_setting('line_error', '')
        self.ws_status, self.ws_error = '等待連線', ''
        self.last_event = None
        self.recent, self.logs = deque(maxlen=40), deque(maxlen=50)
        seen_rows = self.db.execute('SELECT key FROM seen WHERE scope=? ORDER BY stamp DESC LIMIT 50000', (self.scope,)).fetchall()
        self.seen_versions = BoundedSeen(r['key'] for r in reversed(seen_rows))
        cached = self.db.execute('SELECT id,value FROM source_cache WHERE scope=? ORDER BY stamp DESC LIMIT 5000', (self.scope,)).fetchall()
        self.cache = OrderedDict((r['id'], json.loads(r['value'])) for r in reversed(cached))
        self.initialized = self.read_setting('initialized', False) is True
        self.reconnect_event = asyncio.Event()
        self.receive_task = self.send_task = None
        self.prune()

    def read_setting(self, name, default=None):
        row = self.db.execute('SELECT value FROM settings WHERE scope=? AND key=?', (self.scope, name)).fetchone()
        return json.loads(row['value']) if row else default

    def write_setting(self, name, value):
        self.db.execute('INSERT OR REPLACE INTO settings VALUES(?,?,?)',
                        (self.scope, name, json.dumps(value, ensure_ascii=False)))

    def log(self, message):
        message = redact(message)
        if hasattr(self, 'logs'):
            self.logs.append({'time': self.now().isoformat(), 'text': message[:500]})
        logging.getLogger('jin10').info(message)

    def prune(self):
        cutoff = self.now().timestamp() - 7 * 86400
        with self.db:
            self.db.execute("UPDATE outbox SET status='skipped' WHERE scope=? AND status='queued' AND news_time<?", (self.scope, self.now().timestamp()-900))
            self.db.execute("DELETE FROM outbox WHERE scope=? AND status IN ('accepted','cancelled','skipped') AND news_time<?", (self.scope, cutoff))
            self.db.execute('DELETE FROM seen WHERE scope=? AND stamp<?', (self.scope, self.now().timestamp()-86400))
            self.db.execute('DELETE FROM source_cache WHERE scope=? AND id NOT IN (SELECT id FROM source_cache WHERE scope=? ORDER BY stamp DESC LIMIT 5000)', (self.scope, self.scope))

    def cache_item(self, item):
        merged = super().cache_item(item)
        with self.db:
            self.db.execute('INSERT OR REPLACE INTO source_cache VALUES(?,?,?,?)',
                (self.scope, str(merged['id']), json.dumps(merged, ensure_ascii=False), self.now().timestamp()))
        return merged

    def queue(self, row):
        key = super().queue(row)
        saved = {k: v for k, v in row.items() if k != 'received'}
        with self.db:
            self.db.execute("UPDATE outbox SET news_id=?,row_json=? WHERE scope=? AND event_key=? AND status='queued'",
                (row['id'], json.dumps(saved, ensure_ascii=False), self.scope, key))
        return key

    def reject_news(self, item, error, *, packet_code=None):
        super().reject_news(item, error, packet_code=packet_code)
        news_id = str(item.get('id') or '') if isinstance(item, dict) else ''
        if news_id.isdigit():
            # An unusable edit cannot leave an older, unsent version queued.
            # In-flight/pending bodies remain immutable for retry safety.
            with self.db:
                self.db.execute("UPDATE outbox SET status='cancelled' WHERE scope=? AND news_id=? AND status='queued'", (self.scope, news_id))

    def handle_news(self, item, *, baseline=False, recovered=False):
        self.last_event = self.now().isoformat()
        super().handle_news(item, baseline=baseline, recovered=recovered)
        news_id = str(item.get('id') or '')
        action = item.get('action', 1)
        if action == 3:
            with self.db:
                self.db.execute("UPDATE outbox SET status='cancelled' WHERE scope=? AND news_id=? AND status='queued'", (self.scope, news_id))
                self.db.execute('DELETE FROM source_cache WHERE scope=? AND id=?', (self.scope, news_id))
            return
        if action not in (1, 2):
            return
        try:
            row = e.ws_news_row(self.cache.get(news_id, item), received=self.now(), changed=action==2, recovered=recovered)
        except e.BotError:
            row = None
        with self.db:
            if row:
                key = e.event_key(row)
                self.db.execute("UPDATE outbox SET status='cancelled' WHERE scope=? AND news_id=? AND status='queued' AND event_key<>?", (self.scope, news_id, key))
            if row is None or not self.eligible(row):
                self.db.execute("UPDATE outbox SET status='cancelled' WHERE scope=? AND news_id=? AND status='queued'", (self.scope, news_id))
        queued = self.backlog.pop(news_id, None)
        if queued and self.enabled:
            self.queue(queued)
            self.changed_event.set()
        # Metadata edits may keep the same event key; refresh only unsent bodies.
        if row and self.eligible(row):
            saved = {k: v for k, v in row.items() if k != 'received'}
            with self.db:
                self.db.execute("UPDATE outbox SET row_json=?,text=? WHERE scope=? AND event_key=? AND status='queued'",
                    (json.dumps(saved, ensure_ascii=False), redact(e.format_ws_news(row)), self.scope, e.event_key(row)))
        # Record observation only after the outbox is durable. A crash between
        # these commits can be replayed safely using the same unique event key.
        if row:
            with self.db:
                self.db.execute('INSERT OR IGNORE INTO seen VALUES(?,?,?)', (self.scope, e.event_key(row), self.now().timestamp()))
        if row and not baseline:
            self.recent.append({k: row[k] for k in ('id','text','time','url','decision','reason','important')})
        if self.stats['received'] and self.stats['received'] % 1000 == 0:
            self.prune()

    def handle_snapshot(self, items):
        super().handle_snapshot(items)
        with self.db:
            self.write_setting('initialized', True)
        self.ws_status, self.ws_error = '已連線', ''

    def save_rules(self, rules):
        rules = e.validate_user_filters(rules)
        with self.db:
            self.write_setting('filters', rules)
            for record in self.db.execute("SELECT * FROM outbox WHERE scope=? AND status='queued'", (self.scope,)).fetchall():
                if not record['row_json']:
                    continue
                row = json.loads(record['row_json'])
                row.update(e.classify_with_user_filters(row['text'], rules))
                self.db.execute('UPDATE outbox SET status=?,text=?,row_json=? WHERE scope=? AND event_key=?',
                    ('queued' if self.eligible(row) else 'cancelled', redact(e.format_ws_news(row)),
                     json.dumps(row, ensure_ascii=False), self.scope, record['event_key']))
        self.rules = rules
        e.USER_FILTERS = rules
        self.log('關鍵字已儲存，新規則已生效。')
        return rules

    def set_enabled(self, enabled):
        if type(enabled) is not bool:
            raise e.BotError('通知開關格式不正確。')
        with self.db:
            self.write_setting('enabled', enabled)
            if not enabled:
                self.db.execute("UPDATE outbox SET status='skipped' WHERE scope=? AND status='queued'", (self.scope,))
        self.enabled = enabled
        self.changed_event.set()
        self.log('LINE 通知已啟用。' if enabled else 'LINE 通知已暫停；仍會接收快訊供查看。')

    def block_line(self, reason):
        self.line_error = reason
        with self.db:
            self.write_setting('line_error', reason)
        self.log(reason)

    def resume_line(self):
        self.block_line('')
        self.changed_event.set()

    def counts(self):
        counts = {r['status']: r['n'] for r in self.db.execute('SELECT status,COUNT(*) AS n FROM outbox WHERE scope=? GROUP BY status', (self.scope,))}
        midnight = self.now().astimezone(e.TAIPEI).replace(hour=0,minute=0,second=0,microsecond=0).timestamp()
        counts['today'] = self.db.execute("SELECT COUNT(*) FROM outbox WHERE scope=? AND status='accepted' AND accepted_at>=?", (self.scope, midnight)).fetchone()[0]
        return counts

    async def receive_forever(self):
        delay = 5
        while not self.stop_requested:
            self.ws_status = '連線中'
            opened = self.monotonic()
            try:
                async with self.connector() as ws:
                    self.stats['connections'] += 1
                    await self._session(ws, float('inf'))
                if self.stop_requested:
                    return
                raise OSError('Connection ended')
            except asyncio.CancelledError:
                raise
            except (e.BotError, e.ProtocolError) as exc:
                self.ws_error = public_error(exc)
            except InvalidStatus as exc:
                status = exc.response.status_code
                if status < 500:
                    self.ws_error = f'金十 WebSocket HTTP {status}，請確認接入權限或限制。'
            except ConnectionClosed as exc:
                if getattr(getattr(exc, 'rcvd', None), 'code', None) in (1008, 4001, 4003):
                    self.ws_error = '金十中斷了接收權限，請向客服確認。'
            except (OSError, TimeoutError):
                pass
            except Exception as exc:
                self.ws_error = '接收程式停止：' + public_error(exc)
            if self.ws_error:
                self.ws_status = '需要處理'
                self.log(self.ws_error)
                self.reconnect_event.clear()
                await self.reconnect_event.wait()
                self.ws_error = ''
            else:
                if self.monotonic()-opened > 60:
                    delay = 5
                self.ws_status = f'斷線，約 {delay} 秒後重連'
                self.stats['reconnects'] += 1
                await self.sleep(delay + random.uniform(0, 1))
                delay = min(delay*2, 60)

    async def send_one(self):
        """One durable delivery; returns idle/accepted/retry/blocked."""
        if not self.enabled or self.line_error:
            return 'idle'
        record = self.db.execute("SELECT * FROM outbox WHERE scope=? AND status IN ('pending','queued') ORDER BY CASE status WHEN 'pending' THEN 0 ELSE 1 END,news_time LIMIT 1", (self.scope,)).fetchone()
        if record is None:
            return 'idle'
        stamp = self.now().timestamp()
        if record['status'] == 'queued' and stamp-record['news_time'] > 900:
            with self.db:
                self.db.execute("UPDATE outbox SET status='skipped' WHERE scope=? AND event_key=?", (self.scope, record['event_key']))
            return 'accepted'
        if record['last_attempt'] is not None and stamp-record['last_attempt'] < 5:
            return 'retry'
        try:
            self.currently_sending = record['news_id']
            await self._deliver({'scope': self.scope, 'keys': [record['event_key']]})
            self.stats['accepted'] += 1
            return 'accepted'
        except (httpx2.TransportError, TimeoutError, OSError):
            self.log('LINE 暫時無法確認，稍後沿用原訊息重試。')
            return 'retry'
        except e.BotError as exc:
            current = self.get(record['event_key'])
            expired = current['first_attempt'] is not None and stamp-current['first_attempt'] >= 23*3600
            if current['last_status'] is not None and current['last_status'] >= 500 and not expired:
                return 'retry'
            message = f"LINE HTTP {current['last_status']}，請確認 Token、權限或訊息額度，再按「重試 LINE」。" if current['last_status'] else public_error(exc)
            if expired:
                message = '有一則傳送結果不明且已超過 23 小時，已暫停 LINE。請保留紀錄並回報，避免重複傳送。'
            self.block_line(message)
            return 'blocked'
        finally:
            self.currently_sending = None

    async def send_forever(self):
        delay = 5
        while not self.stop_requested:
            try:
                result = await self.send_one()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.block_line('LINE 傳送停止：' + public_error(exc))
                result = 'blocked'
            if result == 'retry':
                await self.sleep(delay)
                delay = min(delay*2, 60)
            elif result == 'accepted':
                delay = 5
                await self.sleep(.5)
            else:
                delay = 5
                self.changed_event.clear()
                with contextlib.suppress(TimeoutError):
                    await asyncio.wait_for(self.changed_event.wait(), 5)

    def start(self):
        self.receive_task = asyncio.create_task(self.receive_forever())
        self.send_task = asyncio.create_task(self.send_forever())

    async def close(self):
        self.stop_requested = True
        tasks = [t for t in (self.receive_task, self.send_task) if t is not None]
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        self.db.close()
