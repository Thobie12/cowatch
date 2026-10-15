// CoWatch phone chat: joins the same encrypted room as the browser extension.
import { MqttClient } from './mqtt.js';
import { deriveRoom, seal, unseal, normalizeCode } from './crypto.js';

const RELAYS = ['wss://broker.hivemq.com:8884/mqtt', 'wss://broker.emqx.io:8084/mqtt', 'wss://test.mosquitto.org:8081/mqtt'];
const VOICE_CHUNK = 24000;
const EMOJI = '😂 🤣 😭 😱 😍 🥰 😘 😮 🤯 😳 🙄 😬 🤔 😴 😤 😡 💀 👀 🫣 🙈 🤡 🥺 😏 😎 🤭 😊 🫠 ❤️ 💔 💕 🔥 ✨ 💯 🎉 👏 🙌 👍 👎 🤝 🙏 🍿 🍕 🍷 🏆 💸 🚩 🎯 🫶'.split(' ');

const $ = (id) => document.getElementById(id);
const rid = () => Math.random().toString(36).slice(2, 10) + Date.now().toString(36).slice(-4);
const esc = (s) => String(s ?? '').replace(/[&<>"']/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
const hue = (id) => [...String(id)].reduce((h, c) => (h * 31 + c.charCodeAt(0)) % 360, 0);
const fmt = (s) => { s = Math.max(0, Math.floor(s || 0)); const h = Math.floor(s / 3600), m = Math.floor((s % 3600) / 60), x = s % 60; return (h ? h + ':' + String(m).padStart(2, '0') : m) + ':' + String(x).padStart(2, '0'); };
const store = { get: (k) => { try { return localStorage.getItem(k); } catch (e) { return null; } }, set: (k, v) => { try { localStorage.setItem(k, v); } catch (e) {} } };

// ---- settings from the link (#r=code&n=name&a=laptop-id) or from last time
const hp = new URLSearchParams(location.hash.slice(1));
let code = normalizeCode(hp.get('r') || store.get('cw.code') || '');
let name = (hp.get('n') || store.get('cw.name') || '').slice(0, 24);
let alias = hp.get('a') || (hp.get('r') ? '' : store.get('cw.alias') || '');
const relays = hp.get('relays') ? hp.get('relays').split(',') : RELAYS;
if (hp.get('r')) {
  store.set('cw.code', code);
  store.set('cw.alias', alias);
  if (name) store.set('cw.name', name);
  history.replaceState(null, '', location.pathname); // keep the room code out of the address bar
}

const me = rid();
const seen = new Set();
let room = null; // { topic, key }
let clients = [];
let gotHistory = false;
const people = new Map(); // id -> { name, last }
const voiceParts = new Map();
const voices = new Map();
const typing = new Map();
let lastTyping = 0;

const mineId = (from) => from === me || (alias && from === alias);

// ---------------------------------------------------------------- UI
function show(app) {
  for (const id of ['head', 'log', 'composer', 'typing']) $(id).classList.toggle('hidden', !app);
  $('remote').classList.toggle('hidden', !app || !alias);
  $('join').classList.toggle('hidden', app);
}

function sys(text) {
  const d = document.createElement('div');
  d.className = 'sys';
  d.textContent = text;
  append(d);
}

function append(el) {
  const log = $('log');
  const atBottom = log.scrollHeight - log.scrollTop - log.clientHeight < 80;
  log.appendChild(el);
  while (log.children.length > 200) log.firstChild.remove();
  if (atBottom || el.classList.contains('me')) log.scrollTop = log.scrollHeight;
}

const isEmojiOnly = (t) => /^(\p{Extended_Pictographic}|\p{Emoji_Component}|‍|️|\s){1,12}$/u.test(t) && /\p{Extended_Pictographic}/u.test(t);

function addMsg(m) {
  if (m.id && document.querySelector(`[data-mid="${CSS.escape(m.id)}"]`)) return;
  const mine = mineId(m.from);
  const d = document.createElement('div');
  d.className = 'msg' + (mine ? ' me' : '') + (m.text && isEmojiOnly(m.text) ? ' big' : '');
  if (m.id) d.dataset.mid = m.id;
  const who = mine ? '' : `<span class="who" style="color:hsl(${hue(m.from)},90%,72%)">${esc(m.name)}</span>`;
  d.innerHTML = who + (m.voice ? voiceHtml(m.id, m.dur) : esc(m.text));
  append(d);
}

function voiceHtml(id, dur) {
  return `<span class="voice"><button data-play="${esc(id)}" aria-label="Play voice note">▶</button><span class="bar"><i data-bar="${esc(id)}"></i></span><span>${fmt(dur)}</span></span>`;
}

function renderStatus() {
  const up = clients.some((c) => c.connected);
  $('dot').classList.toggle('on', up);
  const others = [...people.entries()].filter(([id, p]) => !mineId(id) && Date.now() - p.last < 45000).map(([, p]) => p.name);
  $('status').textContent = !up ? 'Connecting…' : others.length ? `With ${[...new Set(others)].join(', ')}` : 'Connected · waiting for others';
}
setInterval(renderStatus, 5000);

function renderTyping() {
  const names = [...typing.keys()];
  $('typing').textContent = names.length ? `${names.join(', ')} ${names.length > 1 ? 'are' : 'is'} typing…` : '';
}

// ---------------------------------------------------------------- relay
async function connect() {
  room = await deriveRoom(code);
  clients = relays.map((url) => new MqttClient(url, {
    onMessage: onRelay,
    onStatus: (up) => {
      renderStatus();
      if (up) send({ type: 'hello' });
    },
  }));
  for (const c of clients) c.subscribe(room.topic);
  show(true);
  sys(alias ? `Joined as ${name}. Messages here show up as you on the big screen.` : `Joined as ${name}.`);
  if (!alias) setInterval(() => send({ type: 'presence' }), 15000);
  renderStatus();
}

async function send(msg) {
  const full = { ...msg, id: rid(), from: me, name, sentAt: Date.now() };
  if (alias) full.alias = alias;
  seen.add(full.id);
  const bytes = await seal(room.key, full);
  for (const c of clients) c.publish(room.topic, bytes);
  return full;
}

async function onRelay(topic, bytes) {
  if (!room || topic !== room.topic) return;
  const m = await unseal(room.key, bytes);
  if (!m || !m.id || seen.has(m.id) || m.from === me) return;
  seen.add(m.id);
  const from = m.alias || m.from;
  if (!m.alias && m.type !== 'bye') {
    const isNew = !people.has(m.from);
    people.set(m.from, { name: m.name, last: Date.now() });
    if (isNew && !mineId(m.from)) sys(`${m.name} is here`);
    renderStatus();
  }
  switch (m.type) {
    case 'chat':
      typing.delete(m.name);
      renderTyping();
      addMsg({ ...m, from });
      break;
    case 'react':
      addMsg({ id: m.id, from, name: m.name, text: m.emoji });
      break;
    case 'typing':
      if (mineId(from)) break;
      clearTimeout(typing.get(m.name));
      typing.set(m.name, setTimeout(() => { typing.delete(m.name); renderTyping(); }, 4000));
      renderTyping();
      break;
    case 'voice-chunk': {
      let v = voiceParts.get(m.vid);
      if (!v) voiceParts.set(m.vid, (v = { parts: new Array(Math.min(m.n | 0, 200)), got: 0 }));
      if (!v.parts[m.i]) { v.parts[m.i] = m.data; v.got++; }
      if (v.got === v.parts.length) {
        voiceParts.delete(m.vid);
        voices.set(m.vid, { b64: v.parts.join(''), mime: m.mime });
        typing.delete(m.name);
        renderTyping();
        addMsg({ id: m.vid, from, name: m.name, voice: true, dur: m.dur });
        if (!mineId(from)) playVoice(m.vid, true);
      }
      break;
    }
    case 'snapshot':
      if (m.to !== me || gotHistory) break;
      gotHistory = true;
      if ((m.history || []).some((h) => !h.voice)) sys('Earlier messages');
      for (const h of m.history || []) if (!h.voice) addMsg({ ...h, from: h.from });
      break;
    case 'state':
      if (alias && from === alias || !alias) {
        $('np').textContent = `${m.playing ? '▶ Playing' : '⏸ Paused'} · ${fmt(m.time)}${m.reason === 'sync' ? '' : ` · ${m.name}`}`;
      }
      if (['play', 'pause', 'seek'].includes(m.reason)) {
        const verb = m.reason === 'play' ? 'pressed play' : m.reason === 'pause' ? 'paused' : 'jumped to';
        sys(`${m.name} ${verb} · ${fmt(m.time)}`);
      }
      break;
    case 'nav':
      if (!m.to) sys(`${m.name} switched to ${m.title || 'another video'}`);
      break;
    case 'bye':
      if (people.delete(m.from)) sys(`${m.name} left`);
      renderStatus();
      break;
    case 'poll':
      sys(`${m.name} asks: ${m.q} (vote on the big screen)`);
      break;
    case 'bingo':
      sys(`BINGO! ${m.name} got a line`);
      break;
  }
}

// ---------------------------------------------------------------- composer
async function sendText() {
  const input = $('text');
  const text = input.value.trim();
  if (!text) return;
  input.value = '';
  $('emojis').classList.remove('open');
  if (alias && /^\/(pause|play)$/i.test(text)) return remote(text.slice(1).toLowerCase());
  const m = await send({ type: 'chat', text });
  addMsg(m);
}

function remote(cmd) {
  send({ type: 'remote', cmd });
}

$('send').addEventListener('click', sendText);
$('text').addEventListener('keydown', (e) => {
  if (e.key === 'Enter' && !e.isComposing) {
    e.preventDefault();
    sendText();
  }
});
$('text').addEventListener('input', () => {
  if (Date.now() - lastTyping > 2500 && $('text').value) {
    lastTyping = Date.now();
    send({ type: 'typing' });
  }
});
$('emojis').innerHTML = EMOJI.map((e) => `<button type="button" data-e="${e}">${e}</button>`).join('');
$('emojiBtn').addEventListener('click', () => $('emojis').classList.toggle('open'));
$('emojis').addEventListener('click', (e) => {
  const b = e.target.closest('button');
  if (!b) return;
  const input = $('text');
  input.value += b.dataset.e;
});
$('remote').addEventListener('click', (e) => {
  const b = e.target.closest('button');
  if (b) remote(b.dataset.r);
});

// ---------------------------------------------------------------- voice
let rec = null;
let audioUnlocked = false;
document.addEventListener('pointerdown', () => (audioUnlocked = true), { once: true });

$('mic').addEventListener('click', async () => {
  if (rec) return stopRec();
  let stream;
  try {
    stream = await navigator.mediaDevices.getUserMedia({ audio: { echoCancellation: true, noiseSuppression: true } });
  } catch (e) {
    return sys('Microphone blocked. Allow it in your browser settings to send voice notes.');
  }
  const mime = ['audio/webm;codecs=opus', 'audio/mp4', 'audio/webm', 'audio/aac'].find((t) => window.MediaRecorder && MediaRecorder.isTypeSupported(t)) || '';
  const r = new MediaRecorder(stream, mime ? { mimeType: mime, audioBitsPerSecond: 24000 } : {});
  const chunks = [];
  r.ondataavailable = (e) => e.data && e.data.size && chunks.push(e.data);
  r.start(250);
  rec = { r, stream, chunks, started: Date.now(), timer: setTimeout(stopRec, 60000) };
  $('mic').classList.add('rec');
  $('recnote').classList.add('on');
});

function stopRec() {
  if (!rec) return;
  const { r, stream, chunks, started, timer } = rec;
  rec = null;
  clearTimeout(timer);
  $('mic').classList.remove('rec');
  $('recnote').classList.remove('on');
  r.onstop = async () => {
    stream.getTracks().forEach((t) => t.stop());
    const blob = new Blob(chunks, { type: r.mimeType || 'audio/mp4' });
    if (blob.size < 200) return;
    const b64 = await new Promise((res) => { const fr = new FileReader(); fr.onload = () => res(String(fr.result).split(',')[1]); fr.readAsDataURL(blob); });
    const dur = Math.round((Date.now() - started) / 100) / 10;
    const vid = rid();
    const n = Math.max(1, Math.ceil(b64.length / VOICE_CHUNK));
    for (let i = 0; i < n; i++) await send({ type: 'voice-chunk', vid, i, n, mime: blob.type, dur, data: b64.slice(i * VOICE_CHUNK, (i + 1) * VOICE_CHUNK) });
    voices.set(vid, { b64, mime: blob.type });
    addMsg({ id: vid, from: me, name, voice: true, dur });
  };
  r.stop();
}

let current = null;
function playVoice(id, auto) {
  const v = voices.get(id);
  if (!v) return;
  if (auto && !audioUnlocked) return; // phones block sound until you tap once
  if (current) current.pause();
  const bytes = Uint8Array.from(atob(v.b64), (c) => c.charCodeAt(0));
  const url = URL.createObjectURL(new Blob([bytes], { type: v.mime || 'audio/webm' }));
  const a = new Audio(url);
  current = a;
  const bar = document.querySelector(`[data-bar="${CSS.escape(id)}"]`);
  a.ontimeupdate = () => bar && a.duration && isFinite(a.duration) && (bar.style.width = (a.currentTime / a.duration) * 100 + '%');
  a.onended = () => { bar && (bar.style.width = '100%'); URL.revokeObjectURL(url); };
  a.play().catch(() => {});
}
$('log').addEventListener('click', (e) => {
  const b = e.target.closest('[data-play]');
  if (b) playVoice(b.dataset.play);
});

// ---------------------------------------------------------------- start
function startJoinForm() {
  show(false);
  $('jcode').value = code;
  $('jname').value = name;
  $('jgo').addEventListener('click', () => {
    code = normalizeCode($('jcode').value);
    name = $('jname').value.trim().slice(0, 24) || 'Guest';
    if (!code) return $('jcode').focus();
    store.set('cw.code', code);
    store.set('cw.name', name);
    alias = hp.get('a') || '';
    store.set('cw.alias', alias);
    connect();
  });
}

if (code && name) connect();
else startJoinForm();
window.__cwPhone = { get me() { return me; }, send };
