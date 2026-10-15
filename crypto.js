// Room codes double as the encryption key: the relay only ever sees
// scrambled bytes on a hashed topic name.

const te = new TextEncoder();
const td = new TextDecoder();

const WORDS = (
  'apple amber aqua aspen basil berry birch bloom blue brave breeze candy cedar cherry cider ' +
  'clover cocoa comet coral cozy crisp daisy dawn dune echo ember fable fern fig flame ' +
  'fluff frost fudge gala gem ginger glow grape hazel honey indigo iris ivy jade jazz ' +
  'juniper kiwi koala lark lava lemon lilac lime lotus lucky lunar mango maple marble ' +
  'meadow melon mint misty mocha moon moss nectar noble nova oak ocean olive onyx opal ' +
  'orbit otter panda peach pearl pepper pine pixel plum polar poppy prism quartz quill ' +
  'rain raven river robin rose ruby saffron sage salsa sky snow solar spark spice ' +
  'star storm sugar sunny swan taco tango teal tiger toast topaz tulip velvet violet ' +
  'wave willow wind wren zest zinc'
).split(' ');

export function makeCode() {
  const r = crypto.getRandomValues(new Uint32Array(4));
  const w = (n) => WORDS[n % WORDS.length];
  return `${w(r[0])}-${w(r[1])}-${w(r[2])}-${10 + (r[3] % 90)}`;
}

export function normalizeCode(input) {
  if (!input) return '';
  let s = String(input).trim();
  const m = s.match(/cowatch=([a-z0-9-]+)/i);
  if (m) s = m[1];
  return s.toLowerCase().replace(/[^a-z0-9-]/g, '');
}

function hex(buf) {
  return [...new Uint8Array(buf)].map((b) => b.toString(16).padStart(2, '0')).join('');
}

export async function deriveRoom(code) {
  const topicHash = await crypto.subtle.digest('SHA-256', te.encode('cowatch-topic:' + code));
  const topic = 'cowatch-x7/v1/' + hex(topicHash).slice(0, 32);
  const base = await crypto.subtle.importKey('raw', te.encode(code), 'PBKDF2', false, ['deriveKey']);
  const key = await crypto.subtle.deriveKey(
    { name: 'PBKDF2', salt: te.encode('cowatch-salt-v1'), iterations: 100000, hash: 'SHA-256' },
    base,
    { name: 'AES-GCM', length: 256 },
    false,
    ['encrypt', 'decrypt']
  );
  return { topic, key };
}

export async function seal(key, obj) {
  const iv = crypto.getRandomValues(new Uint8Array(12));
  const ct = await crypto.subtle.encrypt({ name: 'AES-GCM', iv }, key, te.encode(JSON.stringify(obj)));
  const out = new Uint8Array(12 + ct.byteLength);
  out.set(iv, 0);
  out.set(new Uint8Array(ct), 12);
  return out;
}

export async function unseal(key, bytes) {
  try {
    const pt = await crypto.subtle.decrypt({ name: 'AES-GCM', iv: bytes.slice(0, 12) }, key, bytes.slice(12));
    return JSON.parse(td.decode(pt));
  } catch (e) {
    return null;
  }
}
