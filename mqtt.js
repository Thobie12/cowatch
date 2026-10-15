// Minimal MQTT 3.1.1 client over WebSocket (QoS 0 only).
// Used to talk to free public relays so nobody has to host a server.

const te = new TextEncoder();
const td = new TextDecoder();

function concat(...parts) {
  let len = 0;
  for (const p of parts) len += p.length;
  const out = new Uint8Array(len);
  let o = 0;
  for (const p of parts) { out.set(p, o); o += p.length; }
  return out;
}
function str(s) {
  const b = te.encode(s);
  return concat(new Uint8Array([b.length >> 8, b.length & 255]), b);
}
function varLen(n) {
  const out = [];
  do {
    let d = n % 128;
    n = Math.floor(n / 128);
    if (n > 0) d |= 128;
    out.push(d);
  } while (n > 0);
  return new Uint8Array(out);
}
function packet(first, body) {
  return concat(new Uint8Array([first]), varLen(body.length), body);
}

export class MqttClient {
  constructor(url, { onMessage, onStatus } = {}) {
    this.url = url;
    this.onMessage = onMessage || (() => {});
    this.onStatus = onStatus || (() => {});
    this.topics = new Set();
    this.connected = false;
    this.buf = new Uint8Array(0);
    this.pid = 1;
    this.queue = []; // [{at, topic, bytes}]
    this.backoff = 1000;
    this.closed = false;
    this.connect();
  }

  connect() {
    if (this.closed) return;
    let ws;
    try {
      ws = new WebSocket(this.url, ['mqtt']);
    } catch (e) {
      this.scheduleReconnect();
      return;
    }
    this.ws = ws;
    ws.binaryType = 'arraybuffer';
    ws.onopen = () => {
      const clientId = 'cw-' + Math.random().toString(36).slice(2, 12);
      const body = concat(
        str('MQTT'),
        new Uint8Array([4, 0x02, 0, 60]), // level 4, clean session, keepalive 60s
        str(clientId)
      );
      ws.send(packet(0x10, body));
    };
    ws.onmessage = (ev) => {
      this.buf = concat(this.buf, new Uint8Array(ev.data));
      this.parse();
    };
    ws.onclose = () => this.handleDown();
    ws.onerror = () => { try { ws.close(); } catch (e) {} };
  }

  handleDown() {
    clearInterval(this.pingTimer);
    const was = this.connected;
    this.connected = false;
    this.buf = new Uint8Array(0);
    if (was) this.onStatus(false);
    this.scheduleReconnect();
  }

  scheduleReconnect() {
    if (this.closed) return;
    clearTimeout(this.reTimer);
    this.reTimer = setTimeout(() => this.connect(), this.backoff);
    this.backoff = Math.min(this.backoff * 2, 15000);
  }

  parse() {
    while (this.buf.length >= 2) {
      let mult = 1, len = 0, i = 1, b;
      do {
        if (i >= this.buf.length) return; // need more bytes
        b = this.buf[i++];
        len += (b & 127) * mult;
        mult *= 128;
      } while (b & 128);
      if (this.buf.length < i + len) return;
      const type = this.buf[0];
      const body = this.buf.slice(i, i + len);
      this.buf = this.buf.slice(i + len);
      this.handle(type, body);
    }
  }

  handle(first, body) {
    const type = first >> 4;
    if (type === 2) { // CONNACK
      if (body[1] === 0) {
        this.connected = true;
        this.backoff = 1000;
        for (const t of this.topics) this.sendSub(t);
        clearInterval(this.pingTimer);
        this.pingTimer = setInterval(() => this.raw(new Uint8Array([0xc0, 0])), 20000);
        const now = Date.now();
        const q = this.queue.filter((m) => now - m.at < 10000);
        this.queue = [];
        for (const m of q) this.publish(m.topic, m.bytes);
        this.onStatus(true);
      } else {
        try { this.ws.close(); } catch (e) {}
      }
    } else if (type === 3) { // PUBLISH
      const qos = (first >> 1) & 3;
      const tlen = (body[0] << 8) | body[1];
      const topic = td.decode(body.slice(2, 2 + tlen));
      let off = 2 + tlen;
      if (qos > 0) off += 2;
      this.onMessage(topic, body.slice(off));
    }
    // SUBACK, PINGRESP, UNSUBACK: nothing to do
  }

  raw(bytes) {
    if (this.ws && this.ws.readyState === 1) {
      try { this.ws.send(bytes); return true; } catch (e) {}
    }
    return false;
  }

  nextPid() {
    this.pid = (this.pid % 65535) + 1;
    return new Uint8Array([this.pid >> 8, this.pid & 255]);
  }

  sendSub(topic) {
    this.raw(packet(0x82, concat(this.nextPid(), str(topic), new Uint8Array([0]))));
  }

  subscribe(topic) {
    this.topics.add(topic);
    if (this.connected) this.sendSub(topic);
  }

  unsubscribe(topic) {
    this.topics.delete(topic);
    if (this.connected) this.raw(packet(0xa2, concat(this.nextPid(), str(topic))));
  }

  publish(topic, bytes) {
    if (!this.connected || !this.raw(packet(0x30, concat(str(topic), bytes)))) {
      this.queue.push({ at: Date.now(), topic, bytes });
      if (this.queue.length > 50) this.queue.shift();
    }
  }

  close() {
    this.closed = true;
    clearInterval(this.pingTimer);
    clearTimeout(this.reTimer);
    try { this.ws && this.ws.close(); } catch (e) {}
  }
}
