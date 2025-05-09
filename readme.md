# @kv/js

A persistent key-value database for Node.js with Write-Ahead Logging, TTL, and compaction.

[![npm version](https://badge.fury.io/js/%40kv%2Fjs.svg)](https://badge.fury.io/js/%40kv%2Fjs)  [![License: Apache](https://img.shields.io/badge/License-Apache-yellow.svg)](https://opensource.org/licenses/Apache)

---

## 📋 Navigation

* [Installation](#installation)
* [Quick Start](#quick-start)
* [Benchmark](#benchmark)
* [API](#api)
* [Durability](#durability)
* [Compaction & Checkpointing](#compaction--checkpointing)
* [Data Types](#data-types)
* [Contributing](#contributing)
* [License](#license)

---

## 🚀 Installation

Install via npm:

```bash
npm install @kv/js
```

Or yarn:

```bash
yarn add @kv/js
```

---

## ✨ Quick Start

```javascript
// CommonJS
const { KV, AsyncKV } = require('@kv/js');
const path = require('path');

// Synchronous API
const db = new KV({
  dbPath: path.join(__dirname, 'data', 'sync.db'),
  indexPath: path.join(__dirname, 'data', 'sync.index'),
  walPath: path.join(__dirname, 'data', 'sync.index.wal')
});

db.set('user:1', { name: 'Alice' });
console.log(db.get('user:1')); // { name: 'Alice' }
db.close();

// Asynchronous API
(async () => {
  const asyncDb = new AsyncKV({
    dbPath: path.join(__dirname, 'data', 'async.db'),
    indexPath: path.join(__dirname, 'data', 'async.index'),
    walPath: path.join(__dirname, 'data', 'async.index.wal')
  });

  await asyncDb.set('session:xyz', { token: 'abc123' });
  console.log(await asyncDb.get('session:xyz'));
  await asyncDb.close();
})();
```

---

## 📝 Note: benchmark mostly depend on hardware.

## 📊 Benchmark

Below are the results from a local benchmark run on 2025-05-09. Your mileage may vary based on hardware and configuration.

| Mode                             | Set ops/sec | Get (warm) ops/sec | Get (cold) ops/sec | Delete ops/sec |
| -------------------------------- | ----------- | ------------------ | ------------------ | -------------- |
| **Sync (syncOnWrite: false)**    | 235,428     | 1,009,279          | 892,819            | 1,483,895      |
| **Sync (syncOnWrite: true)**     | 13,916      | 1,084,180          | 1,664,195          | 3,069,349      |
| **AsyncKV (default, 60s flush)** | 11,344      | 59,635             | 113,027            | 170,419        |

> **Notes:**
>
> * Buffered writes (`syncOnWrite: false`) yield the highest throughput for most use cases.
> * Full sync mode (`syncOnWrite: true`) ensures durability at the cost of write performance.
> * Async mode balances durability and speed, suitable for dynamic workloads.

---

## 📚 API

### `new KV(options)` & `new AsyncKV(options)`

Creates a new database instance. Both share common behavior; `AsyncKV` methods return Promises.

**Options:**

* `dbPath` (String): Path to data file. Default: `kv.db`.
* `indexPath` (String): Path to base index. Default: `kv.index`.
* `walPath` (String): Path to WAL for index. Default: `indexPath + '.wal'`.
* `flushInterval` (Number | null): Auto-flush interval in ms. Default: `100`.
* `syncOnWrite` (Boolean): Force fsync on writes. Default: `false`.
* `defaultTTL` (Number | null): Default TTL in ms. Default: `null`.
* `preload` (Boolean): Load keys into memory on startup. Default: `true`.
* `maxMemoryKeys` (Number): Max keys in in-memory LRU. Default: `Infinity`.
* `compact` (Object): Compaction settings — `interval` (ms, default `3600000`), `threshold` (0.5).
* `checkpoint` (Object): WAL checkpoint settings — `interval` (ms, default `600000`), `walSizeThreshold` (bytes, default `5242880`).
* `eventEmitter` (EventEmitter): Custom emitter for advanced usage.

### Common Methods

| Method                       | Description                                       |
| ---------------------------- | ------------------------------------------------- |
| `set(key, value, [options])` | Store a key-value pair.                           |
| `get(key)`                   | Retrieve a value; `undefined` if missing/expired. |
| `has(key)`                   | Check existence (not expired).                    |
| `delete(key)`                | Remove a key; returns `true` if removed.          |
| `keys()`                     | List all keys.                                    |
| `size()`                     | Number of keys.                                   |
| `clear()`                    | Remove all entries.                               |
| `flush([forceSync])`         | Manually flush to disk.                           |
| `compact()`                  | Trigger file compaction.                          |
| `checkpoint([forceSync])`    | Merge WAL into base index (AsyncKV only).         |
| `getStats()`                 | Retrieve database statistics.                     |
| `close()`                    | Flush, checkpoint, and close resources.           |

### Events

All instances emit events via `on`, `once`, `off`:

* `ready`, `error`, `set`, `get`, `miss`, `delete`, `expired`
* `data_flush`, `index_wal_flush`, `compact_start`, `compact_end`
* `checkpoint_start`, `checkpoint_end`, `clear`
* `closing`, `close`, `warn`, `log`, `wal_replayed`

---

## 🗄️ Compaction & Checkpointing

Automatic or manual maintenance tasks to reclaim space and merge WAL.

## 🛠️ Contributing

Contributions welcome! Please open issues or PRs. Follow standard GitHub workflow.


## 📜 License

Distributed under the Apache 2.0 License. See `LICENSE` for details.
