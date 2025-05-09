// example.js
const { KV, AsyncKV } = require('../src/main.js');

// Example 1: Basic Usage (Synchronous API)
console.log('=== Example 1: Basic Usage ===');
const db = new KV({ dbPath: './data/test.db', indexPath: './data/test.index' });

// Set values
db.set('user:1', { name: 'John', age: 30 });
db.set('user:2', { name: 'Alice', age: 25 });
db.set('counter', 42);
db.set('binary', Buffer.from([0x01, 0x02, 0x03, 0x04]));

// Get values
console.log(db.get('user:1')); // { name: 'John', age: 30 }
console.log(db.get('counter')); // 42
console.log(db.get('binary')); // <Buffer 01 02 03 04>

// Check if key exists
console.log('Has user:1:', db.has('user:1')); // true
console.log('Has user:3:', db.has('user:3')); // false

// Delete a key
db.delete('user:2');
console.log('After delete, has user:2:', db.has('user:2')); // false

// Force flush to disk
db.flush();

// Get all keys
console.log('All keys:', db.keys()); // ['user:1', 'counter', 'binary']

// Get database size
console.log('Database size:', db.size()); // 3

// Get statistics
console.log('Stats:', db.getStats());

// Example 2: TTL (Time-To-Live)
console.log('\n=== Example 2: TTL ===');
const ttlDb = new KV({
  dbPath: './data/ttl.db',
  indexPath: './data/ttl.index',
  defaultTTL: 5000 // 5 seconds default TTL
});

// Set with default TTL
ttlDb.set('session:123', { userId: 456, active: true });

// Set with custom TTL (2 seconds)
ttlDb.set('temp', 'This will expire in 2 seconds', { ttl: 2000 });

console.log('Initial value:', ttlDb.get('temp'));

// Wait and check again after TTL
setTimeout(() => {
  console.log('After 2.5s, temp value:', ttlDb.get('temp')); // undefined (expired)
  console.log('After 2.5s, session value:', ttlDb.get('session:123')); // still exists
}, 2500);

// Example 3: Binary data performance
console.log('\n=== Example 3: Binary Data ===');
const binaryDb = new KV({
  dbPath: './data/binary.db',
  indexPath: './data/binary.index'
});

// Store different types of data
binaryDb.set('string', 'Simple string value');
binaryDb.set('number', 12345.6789);
binaryDb.set('binary', Buffer.from('Binary data stored efficiently', 'utf8'));
binaryDb.set('object', { complex: 'object', with: ['nested', 'arrays'] });
binaryDb.set('boolean', true);
binaryDb.set('null', null);

// Retrieve and verify
console.log('String value:', binaryDb.get('string'));
console.log('Number value:', binaryDb.get('number'));
console.log('Binary value:', binaryDb.get('binary').toString('utf8'));
console.log('Object value:', binaryDb.get('object'));
console.log('Boolean value:', binaryDb.get('boolean'));
console.log('Null value:', binaryDb.get('null'));

// Example 4: Async API
console.log('\n=== Example 4: Async API ===');
const asyncDb = new AsyncKV({
  dbPath: './data/async.db',
  indexPath: './data/async.index'
});

async function testAsync() {
  // Set values
  await asyncDb.set('user:1', { name: 'John', role: 'admin' });
  await asyncDb.set('user:2', { name: 'Alice', role: 'user' });
  
  // Get values
  const user1 = await asyncDb.get('user:1');
  console.log('Async user1:', user1);
  
  // Other operations
  console.log('Async database size:', await asyncDb.size());
  console.log('Async database keys:', await asyncDb.keys());
  
  // Delete and check
  await asyncDb.delete('user:2');
  console.log('After delete, has user:2:', await asyncDb.has('user:2'));
  
  // Force flush
  await asyncDb.flush();
}

testAsync().catch(console.error);

// Example 5: Events
console.log('\n=== Example 5: Events ===');
const eventDb = new KV({
  dbPath: './data/event.db',
  indexPath: './data/event.index'
});

// Listen for events
eventDb.on('set', (key, value) => {
  console.log(`Event: Key "${key}" was set to:`, value);
});

eventDb.on('get', (key, value) => {
  console.log(`Event: Key "${key}" was accessed`);
});

eventDb.on('delete', (key) => {
  console.log(`Event: Key "${key}" was deleted`);
});

eventDb.on('expired', (key) => {
  console.log(`Event: Key "${key}" expired`);
});

eventDb.on('flush', (count) => {
  console.log(`Event: ${count} keys flushed to disk`);
});

eventDb.on('compact', (size) => {
  console.log(`Event: Database compacted, new size: ${size} bytes`);
});

// Trigger events
eventDb.set('counter', 1);
eventDb.get('counter');
eventDb.set('counter', 2); // Update
eventDb.set('willExpire', 'gone soon', { ttl: 1000 });
eventDb.delete('counter');

// Force a flush
eventDb.flush();

// Wait for expiration
setTimeout(() => {
  console.log('Checking expired key:', eventDb.get('willExpire'));
}, 1500);

// Example 6: Performance Test
console.log('\n=== Example 6: Performance Test ===');
const perfDb = new KV({
  dbPath: './data/perf.db',
  indexPath: './data/perf.index',
  syncOnWrite: false // For maximum performance
});

const ITEMS = 10000; // Number of items to test with

console.time('Write Performance');
for (let i = 0; i < ITEMS; i++) {
  perfDb.set(`key:${i}`, `value:${i}`);
}
console.timeEnd('Write Performance');

// Force flush to ensure all writes are persistent
perfDb.flush();
console.log(`Wrote ${ITEMS} items to disk`);

console.time('Read Performance');
for (let i = 0; i < ITEMS; i++) {
  perfDb.get(`key:${i}`);
}
console.timeEnd('Read Performance');

console.log('Performance database size:', perfDb.size());
console.log('Performance stats:', perfDb.getStats());

// Example 7: Database Compaction
console.log('\n=== Example 7: Database Compaction ===');
const compactDb = new KV({
  dbPath: './data/compact.db',
  indexPath: './data/compact.index',
  compact: {
    interval: 5000, // 5 seconds for demo
    threshold: 0.3  // 30% waste triggers compaction
  }
});

// Write some data
for (let i = 0; i < 100; i++) {
  compactDb.set(`item:${i}`, { id: i, data: `Some data ${i}` });
}

// Delete some items to create waste space
for (let i = 0; i < 50; i++) {
  compactDb.delete(`item:${i}`);
}

// Force compaction
console.log('Before compaction stats:', compactDb.getStats());
compactDb.compact().then(() => {
  console.log('After compaction stats:', compactDb.getStats());
});

// Clean up resources when done
setTimeout(() => {
  console.log('\n=== Closing databases ===');
  db.close();
  ttlDb.close();
  binaryDb.close();
  asyncDb.close();
  eventDb.close();
  perfDb.close();
  compactDb.close();
  console.log('All databases closed successfully');
}, 6000);