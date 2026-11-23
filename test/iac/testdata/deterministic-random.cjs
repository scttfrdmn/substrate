// Preloaded into the CDK CLI (and the app it synthesizes) with NODE_OPTIONS=--require, so the
// tool's own random values repeat from run to run (#1628).
//
// The double-run check compares what two substrate servers answered. The CDK CLI puts fresh
// random values in its requests — `cdk bootstrap` names its change set
// "cdk-bootstrap-change-set-<uuid>" and describes it "CDK Changeset for execution <uuid>" — and
// substrate echoes them, so without this the two runs differ for a reason that is the caller's,
// not substrate's. Replacing the randomness at its source leaves substrate as the only thing that
// could make the runs disagree.
//
// Every random source Node offers is replaced with one xorshift128+ stream from a fixed seed:
// crypto.randomUUID, randomBytes, randomFillSync, randomInt, getRandomValues (on both
// node:crypto and the Web Crypto global) and Math.random. Nothing here is cryptographic, and
// nothing in a journey needs it to be: the journey talks plain HTTP to a local server.
'use strict';

const crypto = require('node:crypto');
const { syncBuiltinESMExports } = require('node:module');

let s0 = 0x9e3779b97f4a7c15n;
let s1 = 0xd1b54a32d192ed03n;
const mask = (1n << 64n) - 1n;

function next64() {
  let x = s0;
  const y = s1;
  s0 = y;
  x ^= (x << 23n) & mask;
  s1 = x ^ y ^ (x >> 17n) ^ (y >> 26n);
  return (s1 + y) & mask;
}

function fill(view) {
  for (let i = 0; i < view.length; i++) {
    view[i] = Number(next64() & 0xffn);
  }
  return view;
}

function bytes(n) {
  return fill(Buffer.alloc(n));
}

crypto.randomBytes = function randomBytes(n, cb) {
  const out = bytes(n);
  if (typeof cb === 'function') {
    process.nextTick(cb, null, out);
    return undefined;
  }
  return out;
};

crypto.randomFillSync = function randomFillSync(buf, offset = 0, size) {
  const view = new Uint8Array(buf.buffer || buf, (buf.byteOffset || 0) + offset,
    size === undefined ? (buf.byteLength - offset) : size);
  fill(view);
  return buf;
};

crypto.randomFill = function randomFill(buf, offset, size, cb) {
  if (typeof offset === 'function') { cb = offset; offset = 0; size = undefined; }
  if (typeof size === 'function') { cb = size; size = undefined; }
  crypto.randomFillSync(buf, offset, size);
  process.nextTick(cb, null, buf);
};

crypto.randomInt = function randomInt(min, max, cb) {
  if (max === undefined || typeof max === 'function') { cb = max; max = min; min = 0; }
  const value = min + Number(next64() % BigInt(max - min));
  if (typeof cb === 'function') {
    process.nextTick(cb, null, value);
    return undefined;
  }
  return value;
};

function uuid() {
  const b = bytes(16);
  b[6] = (b[6] & 0x0f) | 0x40;
  b[8] = (b[8] & 0x3f) | 0x80;
  const h = b.toString('hex');
  return `${h.slice(0, 8)}-${h.slice(8, 12)}-${h.slice(12, 16)}-${h.slice(16, 20)}-${h.slice(20)}`;
}

crypto.randomUUID = uuid;

function getRandomValues(view) {
  fill(new Uint8Array(view.buffer, view.byteOffset, view.byteLength));
  return view;
}

// node:crypto's own getRandomValues is an accessor that refuses redefinition and forwards to the
// Web Crypto object, so the Web Crypto object and its prototype are replaced as well. A property
// that refuses redefinition is left as it is.
function override(target, name, value) {
  if (!target) {
    return;
  }
  try {
    Object.defineProperty(target, name, { value, configurable: true, writable: true });
  } catch {
    // Not redefinable; see above.
  }
}

for (const target of [crypto, crypto.webcrypto, globalThis.crypto,
  globalThis.crypto && Object.getPrototypeOf(globalThis.crypto)]) {
  override(target, 'getRandomValues', getRandomValues);
  override(target, 'randomUUID', uuid);
}

Math.random = function random() {
  return Number(next64() >> 11n) / 2 ** 53;
};

// ESM importers of node:crypto bind its exports when they load; this pushes the replacements
// into those bindings too.
syncBuiltinESMExports();
