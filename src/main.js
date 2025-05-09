/**
 * @kv/js - A lightning fast and ultra lightweight persistent key-value database for Node.js
 * Features:
 * - Automatic persistence
 * - Optimized for high throughput
 * - Zero dependencies
 * - TTL support (time-to-live for entries)
 */

// kv-store/main.js
const KV = require('./core/kv-core');
const AsyncKV = require('./core/async-kv');

module.exports = {
  KV,
  AsyncKV,
};