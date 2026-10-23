import test from 'node:test';
import assert from 'node:assert/strict';
import worker, {TopicHub} from '../src/index.js';
import {configured, authorized, allowedTopic, eventMessage, sincePredicate, formatLine, VERSION} from '../src/core.js';

const env=()=>({PUBLISH_TOKEN:'P'.repeat(48),SUBSCRIBE_USER:'ntfy',
 SUBSCRIBE_PASSWORD:'S'.repeat(48),NTFY_TOPIC:'T'.repeat(40),NTFY_TOPIC_INFO:'I'.repeat(40)});
const post=(body, auth)=>new Request('https://test.local/',{
 method:'POST',headers:{'Content-Type':'application/json',...(auth?{Authorization:auth}:{})},body:JSON.stringify(body)
});
function mockDO(){let calls=[];return {calls,get(id){return {fetch:async req=>{
 calls.push({id,url:req.url,method:req.method,headers:Object.fromEntries(req.headers),
 body:req.method==='POST'?await req.json():null});
 return new Response(JSON.stringify({ok:true}),{headers:{'Content-Type':'application/json'}});
 }}},idFromName:t=>t};}

test('empty or unsafe configuration fails closed',()=>{
 const e=env();assert.equal(configured(e),true);delete e.PUBLISH_TOKEN;
 assert.equal(configured(e),false);
 e.PUBLISH_TOKEN='P'.repeat(48);e.NTFY_TOPIC='short';assert.equal(configured(e),false);
});
test('publish and subscribe credentials stay separate',async()=>{
 const e=env();const pub=new Request('https://test.local',{headers:{Authorization:'Bearer '+e.PUBLISH_TOKEN}});
 const sub=new Request('https://test.local',{headers:{Authorization:'Basic '+btoa('ntfy:'+e.SUBSCRIBE_PASSWORD)}});
 assert.equal(await authorized(pub,e,'publish'),true);
 assert.equal(await authorized(pub,e,'subscribe'),false);
 assert.equal(await authorized(sub,e,'subscribe'),true);
 assert.equal(await authorized(sub,e,'publish'),false);
});
test('Worker rejects anonymous publishing before DO access',async()=>{
 const e=env();e.TOPIC_HUB=mockDO();
 const r=await worker.fetch(post({topic:e.NTFY_TOPIC,message:'TEST'}),e);
 assert.equal(r.status,401);assert.equal(e.TOPIC_HUB.calls.length,0);
});
test('valid authenticated publish writes an ntfy event only to DO',async()=>{
 const e=env();e.TOPIC_HUB=mockDO();
 const r=await worker.fetch(post({topic:e.NTFY_TOPIC,message:'TEST ONLY',priority:5,
 title:'SECURITY TEST',tags:['warning'],click:'https://example.com/alert'},'Bearer '+e.PUBLISH_TOKEN),e);
 assert.equal(r.status,200);assert.equal(e.TOPIC_HUB.calls.length,1);
 const sent=e.TOPIC_HUB.calls[0].body;
 assert.equal(sent.topic,e.NTFY_TOPIC);assert.equal(sent.priority,5);
 assert.equal(sent.event,'message');assert.equal(sent.message,'TEST ONLY');
 assert.equal(sent.expires-sent.time,172800);
});
test('unapproved topics and priority types never reach DO',async()=>{
 const e=env();e.TOPIC_HUB=mockDO();
 for(const body of [{topic:'Z'.repeat(40),message:'x'},
    {topic:e.NTFY_TOPIC,priority:9,message:'x'},
    {topic:e.NTFY_TOPIC,priority:4,message:'x',click:'http://unsafe.example'},
    {topic:e.NTFY_TOPIC,priority:4,message:'x',extra:'surprise'}]){
    const res=await worker.fetch(post(body,'Bearer '+e.PUBLISH_TOKEN),e);
    assert.equal(res.status,400);
 }
 assert.equal(e.TOPIC_HUB.calls.length,0);
});
test('Priority 2 and 4 preserved and INFO topic allowlisted',()=>{
 const e=env();assert.equal(eventMessage({topic:e.NTFY_TOPIC,message:'m',priority:2},e).priority,2);
 assert.equal(eventMessage({topic:e.NTFY_TOPIC,message:'m',priority:4},e).priority,4);
 assert.equal(allowedTopic(e.NTFY_TOPIC_INFO,e),true);
});
test('subscriber credential cannot use publisher token',async()=>{
 const e=env();e.TOPIC_HUB=mockDO();
 let req=new Request('https://test.local/'+e.NTFY_TOPIC+'/json?poll=1',{
  headers:{Authorization:'Bearer '+e.PUBLISH_TOKEN}});
 assert.equal((await worker.fetch(req,e)).status,401);
 req=new Request('https://test.local/'+e.NTFY_TOPIC+'/json?poll=1',{
  headers:{Authorization:'Basic '+btoa('ntfy:'+e.SUBSCRIBE_PASSWORD)}});
 assert.equal((await worker.fetch(req,e)).status,200);
 assert.equal(e.TOPIC_HUB.calls.length,1);
 assert.equal(e.TOPIC_HUB.calls[0].headers['x-internal-topic'],e.NTFY_TOPIC);
});
test('health does not assert handset delivery or expose secret',async()=>{
 const e=env();e.TOPIC_HUB=mockDO();
 const req=new Request('https://test.local/v1/health',{headers:{Authorization:'Bearer '+e.PUBLISH_TOKEN}});
 const res=await worker.fetch(req,e);assert.equal(res.status,200);
 const data=await res.json();assert.equal(data.version,VERSION);
 assert.equal(data.androidVerified,false);assert.equal(data.publishingVerified,false);
 assert.equal(JSON.stringify(data).includes(e.PUBLISH_TOKEN),false);
});
test('since parser rejects invalid values and supports replay forms',()=>{
 assert.deepEqual(sincePredicate('all'),{type:'all'});
 assert.deepEqual(sincePredicate('latest'),{type:'latest'});
 assert.equal(sincePredicate('2026-01-01'),null);
 assert.deepEqual(sincePredicate('30m',2000),{type:'timestamp',value:200});
 assert.deepEqual(sincePredicate('1700000000'),{type:'timestamp',value:1700000000});
});
test('stream formats are ntfy-compatible',()=>{
 const e={event:'message',topic:'t',message:'ok'};
 assert.equal(formatLine(e,'json').endsWith('\n'),true);
 assert.equal(formatLine(e,'sse').startsWith('data: '),true);
});
test('subscription bearer credential must differ from publisher token',()=>{
 const e=env();e.SUBSCRIBE_TOKEN=e.PUBLISH_TOKEN;assert.equal(configured(e),false);
 e.SUBSCRIBE_TOKEN='B'.repeat(48);assert.equal(configured(e),true);
});
function fakeSQLite(){
 const state={topic:null,messages:[],sequence:0};
 const exec=(sql,...args)=>{
  if(sql.startsWith('CREATE'))return [];
  if(sql.startsWith('INSERT OR IGNORE INTO metadata')){
   if(!state.topic)state.topic=args[1];return [];
  }
  if(sql.startsWith('SELECT value FROM metadata'))return state.topic?[{value:state.topic}]:[];
  if(sql.startsWith('INSERT INTO messages')){
   const [id,time,payload]=args;state.messages.push({id,time,payload,seq:++state.sequence});return [];
  }
  if(sql.startsWith('DELETE FROM messages WHERE time')){
   state.messages=state.messages.filter(m=>m.time>=args[0]);return [];
  }
  if(sql.startsWith('DELETE FROM messages WHERE seq NOT')){
   state.messages=state.messages.slice(-args[0]);return [];
  }
  if(sql.startsWith('SELECT seq FROM messages')){
   const m=state.messages.find(m=>m.id===args[0]);return m?[{seq:m.seq}]:[];
  }
  if(sql.startsWith('SELECT payload FROM messages')){
   let rows=state.messages;
   if(sql.includes('time >= ?'))rows=rows.filter(m=>m.time>=args[0]);
   if(sql.includes('seq > ?'))rows=rows.filter(m=>m.seq>args[0]);
   return rows.slice().reverse().slice(0,args.at(-1)).map(m=>({payload:m.payload}));
  }
  throw new Error('Unexpected SQL '+sql);
 };
 return {exec,state};
}
test('DO stores and replays authenticated messages (SQLite mock)',async()=>{
 const e=env();const sql=fakeSQLite();
 const hub=new TopicHub({storage:{sql},getWebSockets:()=>[]},e);
 const poll=()=>new Request('https://internal/subscribe?format=json&poll=1&since=all',{
  headers:{'x-internal-topic':e.NTFY_TOPIC}});
 let res=await hub.fetch(poll());assert.equal(res.status,200);
 assert.equal(await res.text(),'');
 const event=eventMessage({topic:e.NTFY_TOPIC,message:'TEST ONLY',priority:4},e);
 res=await hub.fetch(new Request('https://internal/publish',{method:'POST',
  body:JSON.stringify(event),headers:{'Content-Type':'application/json'}}));
 assert.equal(res.status,200);
 res=await hub.fetch(poll());
 const replay=(await res.text()).trim().split('\n').map(JSON.parse);
 assert.equal(replay.length,1);
 assert.equal(replay[0].id,event.id);
 assert.equal(replay[0].priority,4);
 res=await hub.fetch(new Request('https://internal/subscribe?format=json&poll=1&since=latest',{
  headers:{'x-internal-topic':e.NTFY_TOPIC}}));
 assert.equal(JSON.parse((await res.text()).trim()).id,event.id);
});
test('DO refuses internal topic mismatch',async()=>{
 const e=env();const sql=fakeSQLite();const hub=new TopicHub({storage:{sql},getWebSockets:()=>[]},e);
 const first=eventMessage({topic:e.NTFY_TOPIC,message:'FIRST'},e);
 assert.equal((await hub.fetch(new Request('https://internal/publish',{method:'POST',body:JSON.stringify(first)}))).status,200);
 const wrong=eventMessage({topic:e.NTFY_TOPIC_INFO,message:'WRONG'},e);
 const r=await hub.fetch(new Request('https://internal/publish',{method:'POST',body:JSON.stringify(wrong)}));
 assert.equal(r.status,403);
 assert.equal(sql.state.messages.length,1);
});
