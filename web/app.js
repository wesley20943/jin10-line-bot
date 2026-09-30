'use strict';
const $ = id => document.getElementById(id);
let csrf = '', current = null, dirty = false, loaded = false, busy = false;
const show = (id, visible) => $(id).classList.toggle('hidden', !visible);
const clock = s => s ? new Date(s).toLocaleString('zh-TW', {timeZone:'Asia/Taipei',hour12:false}) : '尚無資料';
async function api(path, data) {
  const options = {credentials:'same-origin',headers:{}};
  if (data !== undefined) {
    options.method='POST'; options.headers={'Content-Type':'application/json','X-CSRF-Token':csrf}; options.body=JSON.stringify(data);
  }
  const response = await fetch(path, options);
  const result = await response.json();
  if (!response.ok) {
    if (response.status===401 && path!=='/api/login') {show('login-panel',true);show('desk',false);show('logout',false);loaded=false;}
    throw new Error(result.error || `HTTP ${response.status}`);
  }
  return result;
}
function draft() {return {version:1,exclude:$('exclude').value.split(/\r?\n/).map(x=>x.trim()).filter(Boolean),include:$('include').value.split(/\r?\n/).map(x=>x.trim()).filter(Boolean),conflict:$('conflict').value};}
function fill(rules) {$('exclude').value=rules.exclude.join('\n');$('include').value=rules.include.join('\n');$('conflict').value=rules.conflict;}
function edited() {dirty=true;$('save-status').textContent='有尚未儲存的修改';}
function node(tag, text, cls) {const n=document.createElement(tag);n.textContent=text;if(cls)n.className=cls;return n;}
function feed(rows) {
  const target=$('feed');target.replaceChildren();
  if(!rows.length){target.append(node('p','等待新的快訊；首次啟動不補發歷史消息。','empty'));return;}
  for(const row of rows.slice(0,15)) {
    const article=document.createElement('article'),meta=node('div','','meta');
    meta.append(node('span',clock(row.time)),node('span',row.decision,'badge'));
    if(row.important===true)meta.append(node('span','● 金十重要','badge important'));
    article.append(meta,node('p',row.text),node('p',row.reason,'reason'));
    if(/^https:\/\/flash\.jin10\.com\/detail\/\d+$/.test(row.url)){const link=node('a','查看金十原文 ↗');link.href=row.url;link.target='_blank';link.rel='noopener noreferrer';article.append(link);}
    target.append(article);
  }
}
async function refresh() {
  try {
    const data=await api('/api/status');current=data;csrf=data.csrf;
    show('login-panel',false);show('desk',true);show('logout',true);
    $('save').disabled=!data.ready;$('notify').disabled=!data.ready;
    if(!data.ready){$('connection-error').textContent=data.error||'正在確認 LINE 設定…';show('connection-error',true);return;}
    if(!loaded){fill(data.rules);loaded=true;dirty=false;$('save-status').textContent='已載入儲存的設定';}
    $('ws-status').textContent=data.ws_status;$('last-event').textContent='最近接收 '+clock(data.last_event);
    $('line-status').textContent=data.line_error?'需要處理':data.enabled?'通知中':'暫停中';
    $('recipient').textContent=data.bot_name+' → '+data.recipient;
    $('today').replaceChildren(document.createTextNode(String(data.counts.today||0)+' '),node('em','則'));
    $('notify').textContent=data.enabled?'暫停 LINE':'啟用 LINE';
    $('notify-hint').textContent=data.enabled?'新快訊符合篩選條件時，會自動傳送到你的 LINE。':'啟用後傳送新快訊；暫停期間的消息不補發。';
    const errors=[data.ws_error,data.line_error].filter(Boolean);$('connection-error').textContent=errors.join('\n');show('connection-error',errors.length>0);
    show('reconnect',!!data.ws_error);show('retry-line',!!data.line_error);
    feed(data.recent);
    $('logs').replaceChildren(...data.logs.map(x=>node('p',clock(x.time)+'　'+x.text)));
  } catch(error) {if(loaded){$('connection-error').textContent='畫面更新失敗：'+error.message;show('connection-error',true);}}
}
async function action(button, fn, status) {
  if(busy)return;busy=true;button.disabled=true;
  try {await fn();} catch(error){$(status).textContent=error.message;} finally {busy=false;button.disabled=false;}
}
$('login-form').addEventListener('submit',async event=>{event.preventDefault();await action(event.submitter,async()=>{await api('/api/login',{password:$('password').value});$('password').value='';$('login-error').textContent='';await refresh();},'login-error');});
$('logout').addEventListener('click',async()=>{await api('/api/logout',{});csrf='';loaded=false;show('desk',false);show('logout',false);show('login-panel',true);});
for(const id of ['exclude','include','conflict'])$(id).addEventListener('input',edited);
$('save').addEventListener('click',()=>action($('save'),async()=>{const result=await api('/api/settings',draft());fill(result.rules);dirty=false;$('save-status').textContent='已儲存，新規則立即生效';await refresh();},'save-status'));
$('trial').addEventListener('click',()=>action($('trial'),async()=>{show('trial-result',true);const r=await api('/api/trial',{text:$('trial-text').value,rules:draft()});$('trial-result').textContent=(r.decision==='候選'?'收錄候選':r.decision)+'\n'+r.reason;},'trial-result'));
async function control(data){show('connection-error',true);await api('/api/control',data);await refresh();}
$('notify').addEventListener('click',()=>action($('notify'),()=>control({action:'notify',enabled:!current.enabled}),'connection-error'));
$('retry-line').addEventListener('click',()=>action($('retry-line'),()=>control({action:'retry-line'}),'connection-error'));
$('reconnect').addEventListener('click',()=>action($('reconnect'),()=>control({action:'reconnect'}),'connection-error'));
$('download').addEventListener('click',()=>{if(dirty){$('import-status').textContent='請先儲存修改，再下載備份。';return;}window.location.assign('/api/backup');});
$('import').addEventListener('change',async event=>{
  const file=event.target.files[0];if(!file)return;
  try{if(file.size>256000)throw new Error('檔案過大');const rules=JSON.parse(await file.text());
    if(rules.version!==1||!['exclude','include'].includes(rules.conflict)||!['exclude','include'].every(k=>Array.isArray(rules[k])&&rules[k].length<=300&&rules[k].every(s=>typeof s==='string'&&s.length<=100&&!/[\r\n]/.test(s))))throw new Error('設定格式不符，請使用本工具或 Colab 的 JSON 備份');
    fill(rules);edited();$('import-status').textContent='備份已載入文字框。檢查後按「儲存並套用」。';
  }catch(error){$('import-status').textContent=error.message;}finally{event.target.value='';}
});
window.addEventListener('beforeunload',event=>{if(dirty){event.preventDefault();event.returnValue='';}});
refresh();setInterval(()=>{if(!busy)refresh();},5000);
