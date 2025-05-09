const fs = require('fs');
const path = require('path');
const { KV, AsyncKV } = require('../src/main.js'); // Assuming kv.js is in the same directory

const NUM_OPERATIONS_SMALL = 10000;
const NUM_OPERATIONS_LARGE = 50000; // For less frequent, more I/O bound tests

const DATA_DIR = './benchmark_data';
const LOG_FILE = './benchmark_results.log';

// --- Helper Functions ---
function cleanupDbFiles(baseName) {
  const dbPath = path.join(DATA_DIR, `${baseName}.db`);
  const indexPath = path.join(DATA_DIR, `${baseName}.index`);
  if (fs.existsSync(dbPath)) fs.unlinkSync(dbPath);
  if (fs.existsSync(indexPath)) fs.unlinkSync(indexPath);
}

function ensureDataDir() {
  if (!fs.existsSync(DATA_DIR)) {
    fs.mkdirSync(DATA_DIR, { recursive: true });
  }
}

function logResult(message) {
  console.log(message);
  fs.appendFileSync(LOG_FILE, `${new Date().toISOString()} | ${message}\n`);
}

function formatMs(ms) {
  if (ms < 1000) return `${ms.toFixed(3)}ms`;
  if (ms < 60000) return `${(ms / 1000).toFixed(3)}s`;
  return `${(ms / 60000).toFixed(2)}m`;
}

// --- Benchmark Runner ---
async function runSingleTest(label, numOps, actionFn, preFn, postFn) {
  if (preFn) await preFn();

  const startTime = process.hrtime.bigint();
  await actionFn();
  const endTime = process.hrtime.bigint();
  const durationMs = Number(endTime - startTime) / 1e6;

  const opsPerSec = (numOps / (durationMs / 1000)).toFixed(2);

  logResult(
    `${label.padEnd(45)} | ${numOps.toLocaleString().padStart(10)} ops | ${formatMs(durationMs).padStart(10)} | ${opsPerSec.padStart(12)} ops/sec`
  );

  if (postFn) await postFn();
  return durationMs;
}

// --- Main Benchmark Suite ---
async function runBenchmarkSuite() {
  ensureDataDir();
  fs.writeFileSync(LOG_FILE, `Benchmark Run: ${new Date().toISOString()}\n`);
  logResult('='.repeat(100));
  logResult(
    `${'Test Name'.padEnd(45)} | ${'Operations'.padStart(10)} ops | ${'Duration'.padStart(10)} | ${'Throughput'.padStart(12)} ops/sec`
  );
  logResult('-'.repeat(100));

  const smallPayload = { message: 'This is a small test payload.', ts: Date.now() };
  // const largePayload = { data: 'X'.repeat(1024 * 4) }; // 4KB payload for some tests


  // === Synchronous KV Benchmarks ===
  logResult('\n=== Synchronous KV (syncOnWrite: false, periodicFlush: 60s) ===');
  let kvSyncDefault;
  const kvSyncDefaultOptions = {
    dbPath: path.join(DATA_DIR, 'sync_default_kv.db'),
    indexPath: path.join(DATA_DIR, 'sync_default_kv.index'),
    syncOnWrite: false,
    flushInterval: 60000, // High to avoid auto-flush during test
    preload: false // Start with cold cache for read tests after writes
  };

  await runSingleTest(
    'KV Set (syncOnWrite: false)', NUM_OPERATIONS_SMALL,
    async () => {
      for (let i = 0; i < NUM_OPERATIONS_SMALL; i++) {
        kvSyncDefault.set(`key:${i}`, { ...smallPayload, id: i });
      }
      await kvSyncDefault.flush(false); // Manual flush, no forced fsync
    },
    () => { cleanupDbFiles('sync_default_kv'); kvSyncDefault = new KV(kvSyncDefaultOptions); },
    null // No specific post for this one, reads will use this DB state
  );

  await runSingleTest(
    'KV Get (syncOnWrite: false, warm cache)', NUM_OPERATIONS_SMALL,
    async () => {
      for (let i = 0; i < NUM_OPERATIONS_SMALL; i++) {
        kvSyncDefault.get(`key:${i}`);
      }
    }
    // No pre/post, uses existing DB state
  );

  // For cold cache get, we need to re-init the DB without preload
  kvSyncDefault.close();
  kvSyncDefault = new KV({...kvSyncDefaultOptions, preload: false}); // Re-open with preload false

  await runSingleTest(
    'KV Get (syncOnWrite: false, cold cache)', NUM_OPERATIONS_SMALL,
    async () => {
      for (let i = 0; i < NUM_OPERATIONS_SMALL; i++) {
        kvSyncDefault.get(`key:${i}`);
      }
    },
    null, // DB already re-opened correctly
    null
  );


  await runSingleTest(
    'KV Delete (syncOnWrite: false)', NUM_OPERATIONS_SMALL,
    async () => {
      for (let i = 0; i < NUM_OPERATIONS_SMALL; i++) {
        kvSyncDefault.delete(`key:${i}`);
      }
      await kvSyncDefault.flush(false); // Manual flush
    },
    null, // Uses existing DB state
    () => { kvSyncDefault.close(); cleanupDbFiles('sync_default_kv'); }
  );


  logResult('\n=== Synchronous KV (syncOnWrite: true, periodicFlush: 60s) ===');
  let kvSyncTrue;
  const kvSyncTrueOptions = {
    dbPath: path.join(DATA_DIR, 'sync_true_kv.db'),
    indexPath: path.join(DATA_DIR, 'sync_true_kv.index'),
    syncOnWrite: true,
    flushInterval: 60000,
    preload: false
  };
  const numOpsSyncTrue = Math.min(NUM_OPERATIONS_SMALL / 10, 1000); // sync:true is slow, reduce ops

  await runSingleTest(
    `KV Set (syncOnWrite: true)`, numOpsSyncTrue,
    async () => {
      for (let i = 0; i < numOpsSyncTrue; i++) {
        kvSyncTrue.set(`key:${i}`, { ...smallPayload, id: i });
      }
      // No explicit flush needed, syncOnWrite handles it
    },
    () => { cleanupDbFiles('sync_true_kv'); kvSyncTrue = new KV(kvSyncTrueOptions); }
  );

  await runSingleTest(
    `KV Get (syncOnWrite: true, warm cache)`, numOpsSyncTrue,
    async () => {
      for (let i = 0; i < numOpsSyncTrue; i++) {
        kvSyncTrue.get(`key:${i}`);
      }
    }
  );
  kvSyncTrue.close();
  kvSyncTrue = new KV({...kvSyncTrueOptions, preload: false});


  await runSingleTest(
    `KV Get (syncOnWrite: true, cold cache)`, numOpsSyncTrue,
    async () => {
      for (let i = 0; i < numOpsSyncTrue; i++) {
        kvSyncTrue.get(`key:${i}`);
      }
    }
  );


  await runSingleTest(
    `KV Delete (syncOnWrite: true)`, numOpsSyncTrue,
    async () => {
      for (let i = 0; i < numOpsSyncTrue; i++) {
        kvSyncTrue.delete(`key:${i}`);
      }
    },
    null,
    () => { kvSyncTrue.close(); cleanupDbFiles('sync_true_kv'); }
  );


  // === Asynchronous AsyncKV Benchmarks ===
  logResult('\n=== Asynchronous AsyncKV (defaults, periodicFlush: 60s) ===');
  let asyncKv;
  const asyncKvOptions = {
    dbPath: path.join(DATA_DIR, 'async_kv.db'),
    indexPath: path.join(DATA_DIR, 'async_kv.index'),
    flushInterval: 60000, // Underlying KV syncOnWrite is false by default
    preload: false
  };
  const numOpsAsync = NUM_OPERATIONS_SMALL / 2; // Async has overhead, adjust if needed

  await runSingleTest(
    'AsyncKV Set', numOpsAsync,
    async () => {
      for (let i = 0; i < numOpsAsync; i++) {
        await asyncKv.set(`key:${i}`, { ...smallPayload, id: i });
      }
      await asyncKv.flush(); // Manual flush
    },
    () => { cleanupDbFiles('async_kv'); asyncKv = new AsyncKV(asyncKvOptions); }
  );

  await runSingleTest(
    'AsyncKV Get (warm cache)', numOpsAsync,
    async () => {
      for (let i = 0; i < numOpsAsync; i++) {
        await asyncKv.get(`key:${i}`);
      }
    }
  );

  asyncKv.close(); // Close to clear memory cache for cold read test
  asyncKv = new AsyncKV({...asyncKvOptions, preload: false});


  await runSingleTest(
    'AsyncKV Get (cold cache)', numOpsAsync,
    async () => {
      for (let i = 0; i < numOpsAsync; i++) {
        await asyncKv.get(`key:${i}`);
      }
    }
  );

  await runSingleTest(
    'AsyncKV Delete', numOpsAsync,
    async () => {
      for (let i = 0; i < numOpsAsync; i++) {
        await asyncKv.delete(`key:${i}`);
      }
      await asyncKv.flush(); // Manual flush
    },
    null,
    () => { asyncKv.close(); cleanupDbFiles('async_kv'); }
  );


  logResult('\n' + '='.repeat(100));
  logResult('Benchmark suite complete.');
  logResult(`Results also logged to: ${LOG_FILE}`);

  // Optional: Clean up the entire benchmark_data directory
  // if (fs.existsSync(DATA_DIR)) {
  //   fs.rmSync(DATA_DIR, { recursive: true, force: true });
  //   logResult('Cleaned up benchmark_data directory.');
  // }
}

runBenchmarkSuite().catch(err => {
  console.error("Benchmark suite failed:", err);
  logResult(`Benchmark suite failed: ${err.stack || err}`);
});