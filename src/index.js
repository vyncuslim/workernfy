import {configured, allowedTopic, authorized, eventMessage, sincePredicate,
  jsonReply, noStoreHeaders, protocolEvent, formatLine, VERSION} from './core.js';

const JSON_TYPE = 'application/json; charset=utf-8';
const MAX_REQUEST_BYTES = 8192;
const TOPIC_MAX_CACHED = 250;
const KEEP_ALIVE_MS = 20000;
const STREAM_LIFETIME_MS = 65000;

function unauthorized() {
  return jsonReply({error:'unauthorized'},401,{'WWW-Authenticate':'Basic realm="workernfy"'});
}
function notFound() {return jsonReply({error:'not_found'},404);}
function serviceNotReady() {return jsonReply({error:'service_not_configured'},503);}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    if (!configured(env) || !env.TOPIC_HUB) return serviceNotReady();
    if (request.method === 'GET' && url.pathname === '/v1/health') {
      if (!(await authorized(request, env, 'either'))) return unauthorized();
      return jsonReply({healthy:true,service:'workernfy',version:VERSION,
        publishingVerified:false,androidVerified:false});
    }
    if (request.method === 'POST' && url.pathname === '/') {
      if (!(await authorized(request, env, 'publish'))) return unauthorized();
      if (!/^application\/json(?:;|$)/i.test(request.headers.get('content-type')||''))
        return jsonReply({error:'json_required'},415);
      if (Number(request.headers.get('content-length') || 0) > MAX_REQUEST_BYTES)
        return jsonReply({error:'request_too_large'},413);
      let body;
      try {
        const text = await request.text();
        if (new TextEncoder().encode(text).byteLength > MAX_REQUEST_BYTES)
          return jsonReply({error:'request_too_large'},413);
        body = JSON.parse(text);
      } catch {return jsonReply({error:'invalid_json'},400);}
      const message = eventMessage(body, env);
      if (!message) return jsonReply({error:'invalid_message_or_topic'},400);
      const stub = env.TOPIC_HUB.get(env.TOPIC_HUB.idFromName(message.topic));
      return stub.fetch(new Request('https://internal/publish', {
        method:'POST',body:JSON.stringify(message),headers:{'Content-Type':JSON_TYPE},
      }));
    }
    if (request.method !== 'GET') return notFound();
    // One configured topic per subscription. Do not accept comma-separated topics.
    const m = /^\/([A-Za-z0-9_-]{24,64})\/(json|sse|raw|ws)$/.exec(url.pathname);
    if (!m || !allowedTopic(m[1], env)) return notFound();
    if (!(await authorized(request, env, 'subscribe'))) return unauthorized();
    const [,topic,format] = m;
    const poll = url.searchParams.get('poll') === '1';
    const since = sincePredicate(url.searchParams.get('since') ?? (poll?'all':'none'));
    if (!since) return jsonReply({error:'invalid_since'},400);
    if (format === 'ws' && request.headers.get('Upgrade')?.toLowerCase() !== 'websocket')
      return jsonReply({error:'websocket_upgrade_required'},426);
    if (format !== 'ws' && request.headers.get('Upgrade')) return jsonReply({error:'invalid_upgrade'},400);
    const stub = env.TOPIC_HUB.get(env.TOPIC_HUB.idFromName(topic));
    // This internal request contains no user credentials or topic secret values.
    const internal = new URL('https://internal/subscribe');
    internal.searchParams.set('format',format);
    internal.searchParams.set('since', url.searchParams.get('since') ?? (poll?'all':'none'));
    if(poll) internal.searchParams.set('poll','1');
    return stub.fetch(new Request(internal.toString(), {
      headers:format==='ws'?{Upgrade:'websocket','x-internal-topic':topic}:{'x-internal-topic':topic},
    }));
  },
};

// Durable Object with SQLite message replay and hibernatable WebSockets.
// This is a minimal ntfy-compatible implementation, not the full ntfy server.
export class TopicHub {
  constructor(ctx, env) {
    this.ctx = ctx;
    this.env = env;
    this.streams = new Set();
    this.sql = ctx.storage.sql;
    this.sql.exec(`CREATE TABLE IF NOT EXISTS messages (
      seq INTEGER PRIMARY KEY AUTOINCREMENT,
      id TEXT NOT NULL UNIQUE,
      time INTEGER NOT NULL,
      payload TEXT NOT NULL
    )`);
    this.sql.exec('CREATE INDEX IF NOT EXISTS messages_time ON messages(time)');
  }
  initMetadata(topic) {
    this.sql.exec('CREATE TABLE IF NOT EXISTS metadata (key TEXT PRIMARY KEY, value TEXT NOT NULL)');
    this.sql.exec('INSERT OR IGNORE INTO metadata(key,value) VALUES (?,?)','topic',topic);
    const stored = [...this.sql.exec('SELECT value FROM metadata WHERE key = ?', 'topic')][0];
    return stored?.value === topic;
  }
  history(since, poll) {
    const mode = sincePredicate(since ?? (poll?'all':'none'));
    if (!mode) return null;
    if (mode.type==='none') return [];
    this.sql.exec('DELETE FROM messages WHERE time < ?', Math.floor(Date.now()/1000)-48*3600);
    let query='SELECT payload FROM messages';
    let args=[];
    if (mode.type==='timestamp') {query+=' WHERE time >= ?'; args=[mode.value];}
    if (mode.type==='id') {
      const id=[...this.sql.exec('SELECT seq FROM messages WHERE id = ?', mode.value)][0];
      if (!id) return [];
      query+=' WHERE seq > ?';args=[id.seq];
    }
    query+=' ORDER BY seq DESC LIMIT ?'; args.push(mode.type==='latest'?1:TOPIC_MAX_CACHED);
    const rows=[...this.sql.exec(query,...args)].reverse();
    return rows.map(r=>JSON.parse(r.payload));
  }
  broadcast(event) {
    const msg=JSON.stringify(event);
    for (const ws of this.ctx.getWebSockets()) {
      try {ws.send(msg);}catch{/* Closed sockets will be pruned by runtime. */}
    }
    for (const stream of this.streams) {
      try {stream.controller.enqueue(stream.encoder.encode(formatLine(event,stream.format)));}
      catch {stream.cleanup();}
    }
  }
  async fetch(request) {
    const url=new URL(request.url);
    if (url.pathname==='/publish' && request.method==='POST') {
      let event;
      try {event=await request.json();}catch{return jsonReply({error:'internal_invalid_json'},400);}
      if (!event || typeof event.topic!=='string' || typeof event.id!=='string' ||
          !this.initMetadata(event.topic)) return jsonReply({error:'topic_mismatch'},403);
      this.sql.exec('INSERT INTO messages(id,time,payload) VALUES (?,?,?)',
        event.id,event.time,JSON.stringify(event));
      this.sql.exec('DELETE FROM messages WHERE time < ?',Math.floor(Date.now()/1000)-48*3600);
      this.sql.exec('DELETE FROM messages WHERE seq NOT IN (SELECT seq FROM messages ORDER BY seq DESC LIMIT ?)',TOPIC_MAX_CACHED);
      this.broadcast(event);
      return jsonReply(event);
    }
    if(url.pathname!=='/subscribe' || request.method!=='GET') return notFound();
    const format=url.searchParams.get('format');
    if(!['json','sse','raw','ws'].includes(format)) return notFound();
    const since=url.searchParams.get('since')??'none';
    this.sql.exec('CREATE TABLE IF NOT EXISTS metadata (key TEXT PRIMARY KEY, value TEXT NOT NULL)');
    const entry=[...this.sql.exec('SELECT value FROM metadata WHERE key = ?', 'topic')][0];
    // The front Worker passes topic only through this internal DO binding.
    const topic = entry?.value || request.headers.get('x-internal-topic');
    if (!topic || !this.initMetadata(topic))return jsonReply({error:'topic_uninitialized'},503);
    const messages=this.history(since,url.searchParams.get('poll')==='1');
    if(messages===null)return jsonReply({error:'invalid_since'},400);
    if(format==='ws'){
      const pair=new WebSocketPair();
      const [client,server]=Object.values(pair);
      this.ctx.acceptWebSocket(server);
      server.send(JSON.stringify(protocolEvent(topic,'open')));
      for(const event of messages)server.send(JSON.stringify(event));
      return new Response(null,{status:101,webSocket:client});
    }
    if(url.searchParams.get('poll')==='1'){
      return new Response(messages.map(m=>formatLine(m,format)).join(''),{
        headers:{...noStoreHeaders,'Content-Type':format==='sse'?'text/event-stream; charset=utf-8':
          format==='raw'?'text/plain; charset=utf-8':'application/x-ndjson; charset=utf-8'},
      });
    }
    if(this.streams.size>=20)return jsonReply({error:'too_many_streams'},503);
    const enc=new TextEncoder();
    let streamRef;
    const stream=new ReadableStream({
      start:controller=>{
        const send=e=>controller.enqueue(enc.encode(formatLine(e,format)));
        send(protocolEvent(topic,'open'));
        for(const m of messages)send(m);
        const cleanup=()=>{
          if(!streamRef || !this.streams.has(streamRef))return;
          clearInterval(streamRef.heartbeat);
          clearTimeout(streamRef.deadline);
          this.streams.delete(streamRef);
          try{controller.close();}catch{}
        };
        streamRef={controller,encoder:enc,format,cleanup,heartbeat:null,deadline:null};
        this.streams.add(streamRef);
        streamRef.heartbeat=setInterval(()=>{
          try {send(protocolEvent(topic,'keepalive'));}catch{cleanup();}
        },KEEP_ALIVE_MS);
        streamRef.deadline=setTimeout(cleanup,STREAM_LIFETIME_MS);
      },
      cancel:()=>streamRef?.cleanup(),
    });
    return new Response(stream,{headers:{...noStoreHeaders,
      'Content-Type':format==='sse'?'text/event-stream; charset=utf-8':
        format==='raw'?'text/plain; charset=utf-8':'application/x-ndjson; charset=utf-8',
      'X-Accel-Buffering':'no',
    }});
  }
  async webSocketMessage(ws,data){
    // Subscribers never publish. Optional safe ping/pong for compatibility.
    if(data==='ping')ws.send('pong');
  }
  async webSocketClose(ws, code, reason){try{ws.close(code,reason)}catch{}}
  async webSocketError(ws){try{ws.close(1011,'connection error')}catch{}}
}
