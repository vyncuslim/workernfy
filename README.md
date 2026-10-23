# workernfy — private ntfy-compatible Cloudflare Worker (experimental)

A small **independent ntfy-compatible service** on Cloudflare Workers + SQLite Durable Objects. Designed to replace `ntfy.sh` publishing for `vyncuslim/bug` without routing alerts to ntfy.sh.

**Important:** This is not the official ntfy server. Real ntfy Android app compatibility, OPPO background reception, offline replay and production alarms are not yet verified.

### Supported API
- `POST /`: authorized ntfy JSON publish (`topic, message, title, priority, tags, click`).
- `GET /TOPIC/json?poll=1`: authenticated polling and recent-message replay.
- `GET /TOPIC/json`, `/sse`, `/raw`: short-lived streaming subscriptions, approximately 65-second max connection length.
- `GET /TOPIC/ws`: Durable Object hibernatable WebSocket.
- `GET /v1/health`: authenticated liveness; never asserts Android receipt.

### Configure
Install Node.js, then use `npm install` and `npx wrangler login`. Create **different** cryptographically random secrets on Cloudflare Worker `workernfy`:

```sh
npx wrangler secret put PUBLISH_TOKEN
npx wrangler secret put SUBSCRIBE_PASSWORD
npx wrangler secret put NTFY_TOPIC
# Optional:
npx wrangler secret put NTFY_TOPIC_INFO
```

`PUBLISH_TOKEN` and `SUBSCRIBE_PASSWORD` must be distinct, at least 32 characters each. `NTFY_TOPIC` must be 24–64 ASCII alphanumeric/`_`/`-` characters and must match the existing topic in Worker `bug`. Optional `SUBSCRIBE_USER` defaults to `ntfy`. Avoid sending secrets in chat, screenshots or GitHub. Deploy with `npx wrangler deploy`. Cloudflare builds can also be connected to this GitHub repo.

### Test safely
With a deployed Worker URL such as `https://workernfy.<your-workers-subdomain>.workers.dev/`:

```powershell
$base='https://YOUR-WORKER-URL/'
$token=Read-Host 'PUBLISH_TOKEN'
Invoke-RestMethod ($base+'v1/health') -Headers @{Authorization="Bearer $token"}
$topic=Read-Host 'NTFY_TOPIC'
$body=@{topic=$topic;title='TEST ONLY';message='Not a real incident';priority=2}|ConvertTo-Json
Invoke-RestMethod -Method Post -Uri $base -ContentType 'application/json' -Headers @{Authorization="Bearer $token"} -Body $body
```

Subscribe in Android ntfy to the **new custom server URL**, using the same topic, username `ntfy` and the separate `SUBSCRIBE_PASSWORD`. The previous ntfy.sh subscription is a distinct server and will not receive these notifications. Manually test app receipt, Priority 4/5 alert sound, reconnect and offline replay. Firebase delivery is **not implemented** in this prototype.

Only when all tests pass, in the existing **bug** Worker set `NTFY_BASE_URL` to the complete HTTPS root Worker URL (with trailing `/`), and `NTFY_TOKEN` to the `PUBLISH_TOKEN`. Keep existing `NTFY_TOPIC` and admin key. **Do not cut over yet.** Keep production rollback settings.

### Limitations
- Partial ntfy protocol compatibility; no native ntfy Firebase bridge, file attachments, user-management API, scheduled messages or guaranteed offline delivery.
- Up to 250 cached messages per topic, pruned after 48 hours on reads/writes; streams close/reconnect after ~65s.
- No self-imposed send cooldown, no automatic retries, and no ntfy.sh dependency. Cloudflare Free/DO quotas **still apply**.
- Publication success proves only that the server stored/broadcast the message; it does not prove Android receipt.
- All topic reads and writes require authentication. Never commit real tokens or passwords.

### Tests
`npm test` and `npm run check` (Wrangler dry run). Production endpoint, live Android compatibility and Cloudflare deploy remain independent acceptance gates.

References: [ntfy API](https://docs.ntfy.sh/subscribe/api/), [ntfy JSON publishing](https://docs.ntfy.sh/publish/), [Durable Objects WebSockets](https://developers.cloudflare.com/durable-objects/best-practices/websockets/), [DO pricing](https://developers.cloudflare.com/durable-objects/platform/pricing/).
