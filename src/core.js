export const VERSION = '2026-10-23-workernfy-v1';
export const TOPIC_PATTERN = /^[A-Za-z0-9_-]{24,64}$/;
const authEncoder = new TextEncoder();

export function configured(env) {
  return typeof env?.PUBLISH_TOKEN === 'string' && env.PUBLISH_TOKEN.length >= 32 &&
    typeof env?.SUBSCRIBE_PASSWORD === 'string' && env.SUBSCRIBE_PASSWORD.length >= 32 &&
    TOPIC_PATTERN.test(env?.NTFY_TOPIC || '') &&
    (!env?.NTFY_TOPIC_INFO || TOPIC_PATTERN.test(env.NTFY_TOPIC_INFO)) &&
    env.PUBLISH_TOKEN !== env.SUBSCRIBE_PASSWORD &&
    (!env.SUBSCRIBE_TOKEN || (env.SUBSCRIBE_TOKEN.length >= 32 &&
      env.SUBSCRIBE_TOKEN !== env.PUBLISH_TOKEN && env.SUBSCRIBE_TOKEN !== env.SUBSCRIBE_PASSWORD)) &&
    (env.SUBSCRIBE_USER === undefined || /^[a-zA-Z0-9_-]{1,40}$/.test(env.SUBSCRIBE_USER));
}

export function allowedTopic(topic, env) {
  return typeof topic === 'string' && (topic === env.NTFY_TOPIC ||
    (Boolean(env.NTFY_TOPIC_INFO) && topic === env.NTFY_TOPIC_INFO));
}

async function constantTimeMatch(a, b) {
  if (typeof a !== 'string' || typeof b !== 'string') return false;
  const [da, db] = await Promise.all([
    crypto.subtle.digest('SHA-256', authEncoder.encode(a)),
    crypto.subtle.digest('SHA-256', authEncoder.encode(b)),
  ]);
  const va = new Uint8Array(da), vb = new Uint8Array(db);
  let different = 0;
  for (let i = 0; i < va.length; i++) different |= va[i] ^ vb[i];
  return different === 0;
}

export async function authorized(request, env, role) {
  const header = request.headers.get('Authorization') || '';
  if (header.startsWith('Bearer ')) {
    const supplied = header.slice(7);
    if (role === 'publish' || role === 'either') {
      if (await constantTimeMatch(supplied, env.PUBLISH_TOKEN)) return true;
    }
    if ((role === 'subscribe' || role === 'either') && env.SUBSCRIBE_TOKEN) {
      if (await constantTimeMatch(supplied, env.SUBSCRIBE_TOKEN)) return true;
    }
  }
  if ((role === 'subscribe' || role === 'either') && header.startsWith('Basic ')) {
    try {
      const decoded = atob(header.slice(6));
      const separator = decoded.indexOf(':');
      if (separator < 0) return false;
      const username = decoded.slice(0, separator);
      const password = decoded.slice(separator + 1);
      return await constantTimeMatch(username, env.SUBSCRIBE_USER || 'ntfy') &&
        await constantTimeMatch(password, env.SUBSCRIBE_PASSWORD);
    } catch { return false; }
  }
  return false;
}

export function eventMessage(body, env, now = Date.now()) {
  if (!body || typeof body !== 'object' || Array.isArray(body) ||
    !allowedTopic(body.topic, env) ||
    typeof body.message !== 'string' || body.message.length < 1 || body.message.length > 3000 ||
    (body.title !== undefined && (typeof body.title !== 'string' || body.title.length > 160)) ||
    (body.priority !== undefined && ![1,2,3,4,5].includes(body.priority)) ||
    (body.tags !== undefined && (!Array.isArray(body.tags) || body.tags.length > 8 ||
       body.tags.some(t => typeof t !== 'string' || t.length > 40 || !/^[\w-]+$/.test(t)))) ||
    (body.click !== undefined && (typeof body.click !== 'string' || body.click.length > 1024 ||
      !body.click.startsWith('https://') || !isValidHttps(body.click))) ||
    Object.keys(body).some(k => !['topic', 'message', 'title', 'priority', 'tags', 'click'].includes(k))) {
    return null;
  }
  const time = Math.floor(now / 1000);
  const id = crypto.randomUUID().replace(/-/g, '').slice(0, 12);
  return {
    id, time, expires: time + 48 * 3600, event: 'message', topic: body.topic,
    message: body.message,
    ...(body.title ? {title:body.title} : {}),
    priority: body.priority ?? 3,
    ...(body.tags ? {tags: body.tags} : {}),
    ...(body.click ? {click:body.click} : {}),
  };
}

function isValidHttps(raw) {
  try { const u = new URL(raw); return u.protocol === 'https:' && !u.username && !u.password; }
  catch { return false; }
}

export function sincePredicate(since, now = Math.floor(Date.now() / 1000)) {
  if (since === undefined || since === '' || since === 'none') return {type:'none'};
  if (since === 'all') return {type:'all'};
  if (since === 'latest') return {type:'latest'};
  if (/^\d{10,13}$/.test(since)) return {type:'timestamp',value:Math.floor(Number(since) / (since.length === 13 ? 1000 : 1))};
  const dur = /^(\d{1,6})(s|m|h|d)$/.exec(since);
  if (dur) {
    const units = {s:1,m:60,h:3600,d:86400};
    return {type:'timestamp',value:now - Number(dur[1]) * units[dur[2]]};
  }
  if (/^[a-f0-9]{12}$/i.test(since)) return {type:'id',value:since};
  return null;
}

export const noStoreHeaders = {
  'Cache-Control':'private, no-store, max-age=0',
  'X-Content-Type-Options':'nosniff',
  'Referrer-Policy':'no-referrer',
  'Access-Control-Allow-Origin':'null',
};
export function jsonReply(payload, status=200, headers={}) {
  return new Response(JSON.stringify(payload), {
    status, headers:{...noStoreHeaders,'Content-Type':'application/json; charset=utf-8',...headers},
  });
}
export function protocolEvent(topic, type) {
  return {id:crypto.randomUUID().replace(/-/g,'').slice(0,12),
    time:Math.floor(Date.now()/1000), event:type, topic};
}
export function formatLine(event, format) {
  if (format === 'sse') return 'data: ' + JSON.stringify(event) + '\n\n';
  if (format === 'raw') return (event.event === 'message' ? event.message : '') + '\n';
  return JSON.stringify(event) + '\n';
}
