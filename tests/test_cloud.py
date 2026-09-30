"""Offline integration tests. All news and LINE responses are fixtures."""
import asyncio
import contextlib
import json
import struct
import sys
import tempfile
import types
import unittest
from datetime import datetime, timezone, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import httpx2
import engine as e
from service import CloudBot
from app import create_app

STATE = {'token':'fixture-token', 'user_id':'U'+'a'*32, 'bot_id':'fixture-bot',
         'bot_name':'我的快訊', 'recipient_name':'個人帳號'}
PASSWORD = 'Fixture-password-2026-only'


def item(n, text='美国30年期国债收益率上涨至5.2%。', now=None, **kwargs):
    return {'id':f'20260930{n:014d}', 'time':(now or datetime.now(timezone.utc)).isoformat(),
            'type':0, 'action':1, 'important':1, 'data':{'content':text}, **kwargs}


def packet(code, body=None):
    data = struct.pack('<h', code)
    if code == 1200:
        data += struct.pack('<i',len(body)) + b''.join(e.wire_string(json.dumps(x,ensure_ascii=False)) for x in body)
    elif code != 1201:
        data += e.wire_string(json.dumps(body,ensure_ascii=False))
    return e.xor_bytes(data, '3.2')


class Socket:
    def __init__(self, snapshot=(), frames=()):
        self.frames = [struct.pack('<IIII',0,2,3,0),packet(4002,{'status':101}),packet(1200,list(snapshot)),*frames]
        self.sent=[]
        self.closed=False

    async def recv(self):
        await asyncio.sleep(0)
        if not self.frames:
            await asyncio.Event().wait()
        value=self.frames.pop(0)
        if isinstance(value,Exception):
            raise value
        if callable(value):
            return await value()
        return value

    async def send(self,value):
        self.sent.append(value)


def connector(*sockets):
    todo=list(sockets)
    @contextlib.asynccontextmanager
    async def connect():
        socket=todo.pop(0)
        try:
            yield socket
        finally:
            socket.closed=True
    return connect


async def quick_sleep(_):
    await asyncio.sleep(0)


class RuntimeFixture:
    def __init__(self,bot):
        self.bot=bot
        self.error=''

    async def close(self):
        pass


class CloudTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmp=tempfile.TemporaryDirectory()
        self.path=Path(self.tmp.name)/'state.sqlite3'
        self.bots=[]
        self.sent=[]
        self.now=datetime.now(timezone.utc)

    async def asyncTearDown(self):
        for bot in self.bots:
            await bot.close()
        self.tmp.cleanup()

    async def push(self,state,text,retry_key):
        self.sent.append((state['user_id'],text,retry_key))
        return types.SimpleNamespace(status_code=200,headers={})

    def bot(self,push=None,**kwargs):
        bot=CloudBot(STATE,push or self.push,self.path,now=lambda:self.now,**kwargs)
        self.bots.append(bot)
        return bot

    async def reopen(self,bot,**kwargs):
        await bot.close()
        self.bots.remove(bot)
        return self.bot(**kwargs)

    async def test_baseline_then_restart_recovers_only_new_news_and_rules(self):
        bot=self.bot()
        bot.save_rules({'version':1,'include':['台積電'],'exclude':['主題'],'conflict':'exclude'})
        bot.set_enabled(True)
        bot.handle_snapshot([item(1,now=self.now)])
        self.assertEqual(await bot.send_one(),'idle')
        bot.handle_news(item(2,now=self.now))
        await bot.send_one()
        bot=await self.reopen(bot)
        self.assertEqual(bot.rules['include'],['台積電'])
        self.assertTrue(bot.enabled)
        bot.handle_snapshot([item(1,now=self.now),item(2,now=self.now),item(3,now=self.now)])
        await bot.send_one()
        self.assertEqual(len(self.sent),2)
        self.assertIn('重連後補回',self.sent[-1][1])

    async def test_durable_queue_survives_crash_before_send(self):
        bot=self.bot();bot.set_enabled(True);bot.handle_snapshot([])
        bot.handle_news(item(2,now=self.now))
        bot=await self.reopen(bot)
        await bot.send_one()
        self.assertEqual(len(self.sent),1)
        self.assertEqual(bot.counts()['accepted'],1)

    async def test_lost_response_keeps_original_key_and_body_after_restart_and_new_filter(self):
        async def fail(state,text,key):
            await self.push(state,text,key)
            raise httpx2.ReadTimeout('fixture')
        bot=self.bot(push=fail);bot.set_enabled(True);bot.handle_snapshot([])
        bot.handle_news(item(2,now=self.now))
        self.assertEqual(await bot.send_one(),'retry')
        bot.save_rules({'version':1,'include':[],'exclude':['国债'],'conflict':'exclude'})
        async def accepted_before(state,text,key):
            await self.push(state,text,key)
            return types.SimpleNamespace(status_code=409,headers={'x-line-accepted-request-id':'fixture'})
        self.now+=timedelta(seconds=6)
        bot=await self.reopen(bot,push=accepted_before)
        self.assertEqual(await bot.send_one(),'accepted')
        self.assertEqual(self.sent[0],self.sent[1])
        self.assertEqual(bot.counts()['accepted'],1)
        self.assertEqual(await bot.send_one(),'idle')

    async def test_filter_update_excludes_unsent_and_keyword_priority(self):
        bot=self.bot();bot.set_enabled(True);bot.handle_snapshot([])
        bot.handle_news(item(2,now=self.now))
        bot.save_rules({'version':1,'include':[],'exclude':['國債'],'conflict':'exclude'})
        self.assertEqual(await bot.send_one(),'idle')
        rules={'version':1,'include':['台積電'],'exclude':['主題'],'conflict':'exclude'}
        self.assertEqual(e.classify_with_user_filters('台积电主题演讲',rules)['decision'],'排除')
        rules['conflict']='include'
        self.assertEqual(e.classify_with_user_filters('台积电主题演讲',rules)['decision'],'候選')
        self.assertNotEqual(e.classify_with_user_filters('farm',{'version':1,'include':['ARM'],'exclude':[],'conflict':'include'})['decision'],'候選')
        self.assertEqual(e.ws_news_row(item(5,'期货热点追踪：美国国债收益率上涨。'))['decision'],'排除')

    async def test_pause_delete_edit_and_metadata(self):
        bot=self.bot();bot.set_enabled(True);bot.handle_snapshot([])
        bot.handle_news(item(2,now=self.now,important=0))
        bot.handle_news({'id':item(2)['id'],'action':2,'important':1})
        await bot.send_one()
        self.assertIn('金十標記：重要',self.sent[0][1])
        bot.handle_news(item(3,now=self.now));bot.handle_news({'id':item(3)['id'],'action':3})
        bot.handle_news(item(4,now=self.now));bot.handle_news(item(4,'美国国债主题报告',now=self.now,action=2))
        self.assertEqual(await bot.send_one(),'idle')
        bot.handle_news(item(5,now=self.now));bot.set_enabled(False)
        bot.handle_news(item(6,now=self.now));bot.set_enabled(True)
        self.assertEqual(await bot.send_one(),'idle')
        self.assertEqual(len(self.sent),1)

    async def test_line_429_blocks_across_restart_without_auto_retry(self):
        async def limited(state,text,key):
            await self.push(state,text,key)
            return types.SimpleNamespace(status_code=429,headers={})
        bot=self.bot(push=limited);bot.set_enabled(True);bot.handle_snapshot([])
        bot.handle_news(item(2,now=self.now))
        self.assertEqual(await bot.send_one(),'blocked')
        bot=await self.reopen(bot)
        self.assertTrue(bot.line_error)
        self.now+=timedelta(seconds=6)
        self.assertEqual(await bot.send_one(),'idle')
        self.assertEqual(len(self.sent),1)
        bot.resume_line()
        await bot.send_one()
        self.assertEqual(self.sent[0],self.sent[1])

    async def test_expired_pending_never_gets_new_retry_key(self):
        async def fail(*args):
            raise httpx2.ReadTimeout('fixture')
        bot=self.bot(push=fail);bot.set_enabled(True);bot.handle_snapshot([])
        bot.handle_news(item(2,now=self.now));await bot.send_one()
        self.now+=timedelta(hours=23,seconds=1)
        self.assertEqual(await bot.send_one(),'blocked')
        self.assertIn('23 小時',bot.line_error)

    async def test_ws_heartbeat_continues_while_line_waits(self):
        started,release,heartbeat=asyncio.Event(),asyncio.Event(),asyncio.Event()
        async def slow(*args):
            started.set();await release.wait();return await self.push(*args)
        async def after_push():
            await started.wait();return packet(1201)
        socket=Socket([item(1,now=self.now)],[packet(1000,item(2,now=self.now)),after_push])
        original=socket.send
        async def observe(value):
            await original(value)
            if value==b'':heartbeat.set()
        socket.send=observe
        bot=self.bot(push=slow,connector=connector(socket));bot.set_enabled(True);bot.start()
        await asyncio.wait_for(heartbeat.wait(),1)
        self.assertEqual(len(self.sent),0)
        release.set()
        for _ in range(50):
            await asyncio.sleep(.002)
            if self.sent:break
        self.assertEqual(len(self.sent),1)

    async def test_reconnect_snapshot_no_duplicates(self):
        first=Socket([item(1,now=self.now)],[packet(1000,item(2,now=self.now)),OSError('disconnect')])
        second=Socket([item(1,now=self.now),item(2,now=self.now),item(3,now=self.now)])
        bot=self.bot(connector=connector(first,second),sleep=quick_sleep);bot.set_enabled(True);bot.start()
        for _ in range(100):
            await asyncio.sleep(.002)
            if len(self.sent)==2:break
        self.assertEqual(len(self.sent),2)
        self.assertEqual(bot.stats['reconnects'],1)

    async def test_bad_records_do_not_stop_cloud_receiver_or_poison_cache(self):
        socket=Socket([{'id':'bad'}],[packet(1000,{'action':1,'time':None}),
                      packet(1000,item(2,now=self.now,time=None)),packet(1201),
                      packet(1000,item(3,now=self.now))])
        bot=self.bot(connector=connector(socket));bot.set_enabled(True);bot.start()
        for _ in range(100):
            await asyncio.sleep(.002)
            if self.sent:break
        self.assertEqual(len(self.sent),1)
        self.assertIn(item(3)['id'],self.sent[0][1])
        self.assertEqual(bot.stats['invalid_records'],3)
        self.assertIn(b'',socket.sent)
        self.assertFalse(bot.receive_task.done())
        self.assertEqual(bot.db.execute('SELECT COUNT(*) FROM source_cache').fetchone()[0],1)

    async def test_invalid_edit_cancels_unsent_but_preserves_pending_body(self):
        async def fail(state,text,key):
            await self.push(state,text,key)
            raise httpx2.ReadTimeout('fixture')
        bot=self.bot(push=fail);bot.set_enabled(True);bot.handle_snapshot([])
        bot.handle_news(item(1,now=self.now));await bot.send_one()
        pending=dict(bot.pending()[0])
        bot.handle_news(item(2,now=self.now))
        for number in (1,2):
            self.assertFalse(bot.accept_news({'id':item(number)['id'],'action':2,'time':None}))
        self.assertEqual(dict(bot.pending()[0]),pending)
        self.assertNotEqual(bot.cache[item(1)['id']]['time'],None)
        self.assertEqual(bot.counts().get('queued',0),0)

    async def test_web_auth_csrf_live_save_trial_backup_and_body_limit(self):
        bot=self.bot();runtime=RuntimeFixture(bot)
        app=create_app(password=PASSWORD,data_dir=Path(self.tmp.name),runtime=runtime)
        async with app.router.lifespan_context(app):
            async with httpx2.AsyncClient(transport=httpx2.ASGITransport(app=app),base_url='https://fixture.test') as client:
                for path in ('/api/status','/api/backup'):
                    self.assertEqual((await client.get(path)).status_code,401)
                self.assertEqual((await client.get('/healthz')).status_code,200)
                response=await client.post('/api/login',json={'password':PASSWORD})
                self.assertEqual(response.status_code,200)
                self.assertIn('secure',response.headers['set-cookie'].lower())
                self.assertIn('httponly',response.headers['set-cookie'].lower())
                data=(await client.get('/api/status')).json();headers={'X-CSRF-Token':data['csrf']}
                self.assertNotIn(STATE['token'],json.dumps(data))
                self.assertNotIn(STATE['user_id'],json.dumps(data))
                rules={'version':1,'include':['台積電'],'exclude':['主題'],'conflict':'exclude'}
                self.assertEqual((await client.post('/api/settings',json=rules)).status_code,403)
                self.assertEqual((await client.post('/api/settings',json=rules,headers={**headers,'Sec-Fetch-Site':'cross-site'})).status_code,403)
                trial=await client.post('/api/trial',json={'rules':rules,'text':'台积电家庭日'},headers=headers)
                self.assertEqual(trial.json()['decision'],'候選')
                self.assertEqual(bot.rules['include'],[])
                response=await client.post('/api/settings',json=rules,headers=headers)
                self.assertEqual(response.status_code,200)
                self.assertEqual((await client.get('/api/backup')).json(),bot.rules)
                self.assertEqual((await client.post('/api/control',json={'action':'notify','enabled':True},headers=headers)).status_code,200)
                self.assertTrue(bot.enabled)
                large=await client.post('/api/settings',content=b'x'*256001,headers={**headers,'Content-Type':'application/json'})
                self.assertEqual(large.status_code,413)
                self.assertEqual(len(self.sent),0)


if __name__=='__main__':
    unittest.main()
