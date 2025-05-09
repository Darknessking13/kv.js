// src/core/async-kv.js
const EventEmitter = require('events'); // Needed if AsyncKV itself emits its own lifecycle events
const KVCore = require('./kv-core');

class AsyncKV extends EventEmitter { // Extend EE if AsyncKV has its own event lifecycle beyond proxying
  constructor(options = {}) {
    super();
    // Ensure KVCore gets its own emitter, and AsyncKV can use it or its own.
    // For simplicity, let AsyncKV have its own emitter and relay.
    this.emitterForSelf = new EventEmitter();
    this.kv = new KVCore({ ...options, eventEmitter: this.emitterForSelf });

    const eventsToRelay = [
        'ready', 'error', 'set', 'get', 'miss', 'delete', 'expired',
        'data_flush', 'index_wal_flush',
        'compact_start', 'compact_end',
        'checkpoint_start', 'checkpoint_end',
        'clear', 'closing', 'close', 'warn', 'log', 'wal_replayed'
    ];
    eventsToRelay.forEach(event => {
        this.emitterForSelf.on(event, (...args) => this.emit(event, ...args));
    });
  }

  _promisifyKvMethod(methodName, ...args) {
    return new Promise((resolve, reject) => {
      setImmediate(() => {
        try {
          const result = this.kv[methodName](...args);
          if (result && typeof result.then === 'function') {
            result.then(resolve).catch(reject);
          } else {
            resolve(result);
          }
        } catch (err) {
          reject(err);
        }
      });
    });
  }

  async set(key, value, options) { return this._promisifyKvMethod('set', key, value, options); }
  async get(key) { return this._promisifyKvMethod('get', key); }
  async has(key) { return this._promisifyKvMethod('has', key); }
  async delete(key) { return this._promisifyKvMethod('delete', key); }
  async clear() { return this._promisifyKvMethod('clear'); }
  async keys() { return this._promisifyKvMethod('keys'); }
  async size() { return this._promisifyKvMethod('size'); }
  async getStats() { return this._promisifyKvMethod('getStats'); }

  async flush(forceSync = false) { return this.kv.flush(forceSync); }
  async compact() { return this.kv.compact(); }
  async close() { return this.kv.close(); }
  async checkpoint(forceSync = false) {
    // Accessing protected method, maybe add a public wrapper in KVCore if preferred
    return this.kv.indexManager.performCheckpoint(forceSync, this.kv.isClosing);
  }

  // Event handling for AsyncKV instance itself
  on(event, listener) { super.on(event, listener); return this; }
  once(event, listener) { super.once(event, listener); return this; }
  off(event, listener) { super.off(event, listener); return this; }
  // emit is inherited
  removeAllListeners(event) { super.removeAllListeners(event); return this; }
}

module.exports = AsyncKV;