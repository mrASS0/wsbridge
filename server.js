'use strict';
/*
 * wsbridge — minimal WebSocket frame relay: one publisher per room pushes binary
 * frames (outbound connection, works behind NAT/CGNAT), any number of viewers
 * receive them in the browser.
 *
 *   publisher:  wss://<host>/pub?room=R&key=<PUB_KEY>     (binary frames)
 *   viewer:     https://<host>/?room=R&key=<VIEW_KEY>     (page)
 *               wss://<host>/sub?room=R&key=<VIEW_KEY>    (frames)
 *               https://<host>/snap?room=R&key=<VIEW_KEY> (latest frame)
 *               https://<host>/health                     (status, JSON)
 *
 * The publisher is told how many viewers are connected ({viewers:n}) so it can
 * stop sending when nobody is watching, and gets an {ack:n} for every frame
 * received: keeping at most a couple of frames in flight bounds the latency to
 * ~one frame even on a slow uplink. Slow viewers skip frames instead of
 * accumulating them (latest-frame semantics). Ping/pong every 25 s on every
 * socket: dead connections are closed and re-established by the clients.
 *
 * Config (environment): PUB_KEY, VIEW_KEY, PORT.
 */
const http = require('http');
const crypto = require('crypto');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 8090;
const PUB_KEY = process.env.PUB_KEY || crypto.randomBytes(6).toString('hex');
const VIEW_KEY = process.env.VIEW_KEY || crypto.randomBytes(6).toString('hex');
const MAX_FRAME = 4 * 1024 * 1024;          // larger frames are dropped
const PING_MS = 25000;

// room -> { publisher, viewers:Set, last:Buffer|null, lastAt:number, frames:number, pubSince:number }
const rooms = new Map();
const room = (r) => { if (!rooms.has(r)) rooms.set(r, { publisher: null, viewers: new Set(), last: null, lastAt: 0, frames: 0, pubSince: 0 }); return rooms.get(r); };
const keyOk = (a, b) => typeof a === 'string' && a.length === b.length && crypto.timingSafeEqual(Buffer.from(a), Buffer.from(b));
const notify = (r) => {
  const st = rooms.get(r); if (!st) return;
  const msg = JSON.stringify({ viewers: st.viewers.size });
  if (st.publisher && st.publisher.readyState === 1) st.publisher.send(msg);
  const vmsg = JSON.stringify({ publisher: !!st.publisher, viewers: st.viewers.size });
  for (const v of st.viewers) if (v.readyState === 1) v.send(vmsg);
};

const server = http.createServer((req, res) => {
  const u = new URL(req.url, 'http://x');
  const r = u.searchParams.get('room') || 'main';
  if (u.pathname === '/health') {
    const out = {}; for (const [k, v] of rooms) out[k] = { publisher: !!v.publisher, viewers: v.viewers.size, frames: v.frames, lastFrameAgeS: v.lastAt ? Math.round((Date.now() - v.lastAt) / 1000) : null };
    res.writeHead(200, { 'Content-Type': 'application/json' }); return res.end(JSON.stringify({ ok: true, uptimeS: Math.round(process.uptime()), rooms: out }));
  }
  if (!keyOk(u.searchParams.get('key'), VIEW_KEY)) { res.writeHead(403); return res.end('Forbidden'); }
  if (u.pathname === '/snap') {
    const st = rooms.get(r);
    if (!st || !st.last) { res.writeHead(503, { 'Content-Type': 'text/plain' }); return res.end('no frame yet'); }
    res.writeHead(200, { 'Content-Type': 'image/jpeg', 'Cache-Control': 'no-store', 'Content-Length': st.last.length }); return res.end(st.last);
  }
  if (u.pathname === '/') { res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' }); return res.end(viewerPage(r)); }
  res.writeHead(404); res.end('not found');
});

const wss = new WebSocketServer({ noServer: true, maxPayload: MAX_FRAME });
server.on('upgrade', (req, socket, head) => {
  const u = new URL(req.url, 'http://x');
  const r = u.searchParams.get('room') || 'main';
  const key = u.searchParams.get('key');
  if (u.pathname === '/pub' && keyOk(key, PUB_KEY)) wss.handleUpgrade(req, socket, head, (ws) => onPublisher(ws, r));
  else if (u.pathname === '/sub' && keyOk(key, VIEW_KEY)) wss.handleUpgrade(req, socket, head, (ws) => onViewer(ws, r));
  else socket.destroy();
});

function alive(ws) { ws.isAlive = true; ws.on('pong', () => { ws.isAlive = true; }); }

function onPublisher(ws, r) {
  const st = room(r);
  if (st.publisher) { try { st.publisher.terminate(); } catch (e) {} }
  st.publisher = ws; st.pubSince = Date.now(); alive(ws);
  console.log(`[pub] connected room=${r}`);
  notify(r);
  ws.on('message', (data, isBinary) => {
    if (!isBinary) return;
    st.last = Buffer.from(data); st.lastAt = Date.now(); st.frames++;
    // slow viewer (previous frame still unsent): skip, it will get the next one
    for (const v of st.viewers) if (v.readyState === 1 && v.bufferedAmount === 0) v.send(data, { binary: true });
    if (ws.readyState === 1) ws.send(JSON.stringify({ ack: st.frames }));
  });
  ws.on('close', () => { if (st.publisher === ws) { st.publisher = null; notify(r); } console.log(`[pub] closed room=${r} (${st.frames} frames)`); });
  ws.on('error', () => {});
}

function onViewer(ws, r) {
  const st = room(r);
  st.viewers.add(ws); alive(ws);
  console.log(`[sub] +1 room=${r} (tot ${st.viewers.size})`);
  notify(r);
  if (st.last && Date.now() - st.lastAt < 5000) ws.send(st.last, { binary: true });   // show something immediately
  ws.on('close', () => { st.viewers.delete(ws); notify(r); console.log(`[sub] -1 room=${r} (tot ${st.viewers.size})`); });
  ws.on('error', () => {});
}

// Periodic ping: sockets that miss the pong are terminated (clients reconnect on their own).
setInterval(() => {
  for (const ws of wss.clients) {
    if (ws.isAlive === false) { try { ws.terminate(); } catch (e) {} continue; }
    ws.isAlive = false; try { ws.ping(); } catch (e) {}
  }
}, PING_MS);

function viewerPage(r) {
  return `<!doctype html><html lang="en"><head><meta charset=utf-8>
<meta name=viewport content="width=device-width,initial-scale=1">
<title>${r}</title>
<style>
html,body{margin:0;background:#000;color:#bbb;font:14px system-ui,sans-serif;height:100%}
#img{width:100%;height:auto;display:block;max-height:100vh;object-fit:contain;background:#000}
#bar{position:fixed;top:8px;left:8px;right:8px;display:flex;gap:8px;align-items:center;background:rgba(0,0,0,.55);padding:6px 10px;border-radius:8px}
#bar span{flex:1}
button,a.b{background:#222;color:#ddd;border:1px solid #444;border-radius:6px;padding:4px 10px;font:inherit;text-decoration:none;cursor:pointer}
.off{color:#f66}
</style></head><body>
<div id=bar><span id=st>connecting…</span><a class=b id=snap href="#" download="frame.jpg">snapshot</a><button id=fs>fullscreen</button></div>
<img id=img alt="live">
<script>
const room=${JSON.stringify(r)}, key=new URLSearchParams(location.search).get('key');
const img=document.getElementById('img'), st=document.getElementById('st'), snap=document.getElementById('snap');
snap.href='/snap?room='+encodeURIComponent(room)+'&key='+encodeURIComponent(key);
document.getElementById('fs').onclick=()=>{ (document.fullscreenElement?document.exitFullscreen():document.documentElement.requestFullscreen()).catch(()=>{}); };
let url=null,n=0,t0=Date.now(),pub=null,lastFrame=0,fps='';
function stato(){ const age=lastFrame?Math.round((Date.now()-lastFrame)/1000):null;
  st.innerHTML=(pub===false?'<b class=off>source offline</b> · ':'')+(fps||'waiting for frames…')+(age>3?' · last frame '+age+' s ago':''); }
setInterval(stato,1000);
function connetti(){
  const ws=new WebSocket((location.protocol==='https:'?'wss':'ws')+'://'+location.host+'/sub?room='+encodeURIComponent(room)+'&key='+encodeURIComponent(key));
  ws.binaryType='blob';
  ws.onopen=()=>{ st.textContent='connected, waiting for frames…'; };
  ws.onmessage=e=>{
    if(typeof e.data==='string'){ try{ const m=JSON.parse(e.data); pub=m.publisher; }catch(_){} stato(); return; }
    if(url) URL.revokeObjectURL(url); url=URL.createObjectURL(e.data); img.src=url; lastFrame=Date.now();
    n++; const dt=(Date.now()-t0)/1000; if(dt>=1){ fps=(n/dt).toFixed(1)+' fps'; n=0; t0=Date.now(); stato(); }
  };
  ws.onclose=()=>{ st.textContent='disconnected, retrying…'; setTimeout(connetti,1500); };
  ws.onerror=()=>ws.close();
}
connetti();
</script></body></html>`;
}

server.listen(PORT, () => {
  console.log(`wsbridge listening on :${PORT}`);
  console.log(`PUB_KEY=${PUB_KEY}`);
  console.log(`VIEW_KEY=${VIEW_KEY}`);
});
