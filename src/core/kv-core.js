// kv-store/kv-core.js
const fs = 'fs'; // Placeholder, fs operations are now in Storage/IndexManager
const path = require('path');
const EventEmitter = require('events');
const Storage = require('../storage/storage');
const IndexManager = require('./index');
// Constants are not directly used here but passed to IndexManager/Storage if needed by them

class KV extends EventEmitter {
  constructor(options = {}) {
    super(); // Call EventEmitter constructor

    // Default options merging
    const defaultOptions = {
      dbPath: 'kv.db',
      indexPath: 'kv.index',
      walPath: `${options.indexPath || 'kv.index'}.wal`,
      flushInterval: 100,
      syncOnWrite: false,
      defaultTTL: null,
      preload: true,
      maxMemoryKeys: Infinity,
      compact: { interval: 3600000, threshold: 0.5 },
      checkpoint: { interval: 600000, walSizeThreshold: 5 * 1024 * 1024 },
      eventEmitter: this, // Pass self as emitter for submodules
    };
    this.options = { ...defaultOptions, ...options };
    // Ensure sub-options are also defaulted if not fully provided
    this.options.compact = { ...defaultOptions.compact, ...(options.compact || {}) };
    this.options.checkpoint = { ...defaultOptions.checkpoint, ...(options.checkpoint || {}) };


    this.store = new Map(); // In-memory cache (LRU managed)
    this.ttlMap = new Map(); // Manages TTL timeouts: key -> { expiry, timeoutId }
    this.dirtyDataKeys = new Set(); // Keys whose data in `store` needs writing to data file

    this.flushDataPending = false;
    this.flushIndexPending = false; // For scheduling IndexManager's flushToWAL
    this.isClosing = false;
    this.isCompacting = false;
    // isCheckpointing is managed by IndexManager

    this.stats = {
      reads: 0, writes: 0, deletes: 0, hits: 0, misses: 0, diskReads: 0,
      lastAccess: Date.now(), bytesWrittenToDataFile: 0, bytesReadFromDataFile: 0,
      bytesWrittenToWAL: 0, compactions: 0, lastCompactionTime: 0,
      wastedSpace: 0, indexSizeBytes: 0, walSizeBytes: 0, checkpoints: 0, lastCheckpointTime: 0,
    };
    this.lruKeys = [];

    // Initialize submodules
    try {
      this.storage = new Storage(this.options.dbPath, this.stats, this);
      // Pass KV's _setTTL method to IndexManager for WAL replay TTL setup
      this.indexManager = new IndexManager(
        { indexPath: this.options.indexPath, walPath: this.options.walPath, syncOnWrite: this.options.syncOnWrite, checkpoint: this.options.checkpoint },
        this.stats,
        this,
        this.ttlMap,
        (key, ttl, schedule) => this._setTTL(key, ttl, schedule) // Bound _setTTL
      );
    } catch (initErr) {
        this.emit('error', new Error(`KV Core: Submodule initialization failed: ${initErr.message}`));
        throw initErr; // Critical failure
    }


    this._init();
  }

  _init() {
    // Post-submodule initialization tasks
    if (this.options.preload) {
      this._loadAllKeysToMemory();
    }
    this.stats.wastedSpace = this.storage.getWastedSpace(this.indexManager.index); // Initial wasted space

    this._setupBackgroundTasks();

    this._boundClose = this.close.bind(this);
    process.on('exit', this._boundClose);
    this._boundCloseAndExitSignal = () => this.close().then(() => process.exit(0)).catch(() => process.exit(1));
    process.on('SIGINT', this._boundCloseAndExitSignal);
    process.on('SIGTERM', this._boundCloseAndExitSignal);
    process.on('SIGHUP', this._boundCloseAndExitSignal);

    this.emit('ready', this.indexManager.size);
  }

  _loadAllKeysToMemory() {
    let count = 0;
    for (const [key, meta] of this.indexManager.entries()) {
      if (this.store.size >= this.options.maxMemoryKeys && this.options.maxMemoryKeys !== Infinity) {
        this.emit('warn', `Preload limit reached: maxMemoryKeys (${this.options.maxMemoryKeys}). Some keys not preloaded.`);
        break;
      }
      try {
        const value = this.storage.readValue(meta);
        this.store.set(key, value);
        this._updateLRU(key, true);
        count++;
      } catch (err) {
        this.emit('error', new Error(`KV Core: Failed to preload key "${key}": ${err.message}.`));
      }
    }
    if (count > 0) this.emit('log', `KV Core: Preloaded ${count} keys into memory cache.`);
  }

  _updateLRU(key, isNewEntryToCache) {
    if (this.options.maxMemoryKeys === Infinity) return;
    const lruIndex = this.lruKeys.indexOf(key);
    if (lruIndex !== -1) this.lruKeys.splice(lruIndex, 1);
    this.lruKeys.push(key);
    if (this.lruKeys.length > this.options.maxMemoryKeys) {
      const keyToEvict = this.lruKeys.shift();
      this.store.delete(keyToEvict);
    }
  }

  _setupBackgroundTasks() {
    if (this.options.flushInterval !== null && this.options.flushInterval > 0) {
      this._flushIntervalId = setInterval(() => {
        if (!this.isClosing) {
            this._flushDataToStorage();
            this._flushIndexManagerToWAL();
        }
      }, this.options.flushInterval);
    }
    if (this.options.compact.interval !== null && this.options.compact.interval > 0) {
      this._compactIntervalId = setInterval(() => {
        if (!this.isClosing && !this.isCompacting) this._compactIfNeeded();
      }, this.options.compact.interval);
    }
    if (this.options.checkpoint.interval !== null && this.options.checkpoint.interval > 0 && this.indexManager) {
        this._checkpointIntervalId = setInterval(() => {
            if(!this.isClosing && !this.indexManager.isCheckpointing) {
                this.indexManager.performCheckpoint(this.options.syncOnWrite); // Use configured sync for interval checkpoints
            }
        }, this.options.checkpoint.interval);
    }
  }

  set(key, value, options = {}) {
    if (this.isClosing) { /* ... error ... */ return false; }
    if (typeof key !== 'string' || key.length === 0) throw new TypeError('Key must be a non-empty string.');

    const oldMetaInMemory = this.indexManager.get(key);
    this.store.set(key, value);
    this._updateLRU(key, oldMetaInMemory === undefined);
    this.dirtyDataKeys.add(key);

    this.stats.writes++;
    this.stats.lastAccess = Date.now();

    const ttl = options.ttl || this.options.defaultTTL;
    if (ttl) {
      this._setTTL(key, ttl, true);
    } else if (this.ttlMap.has(key)) {
      clearTimeout(this.ttlMap.get(key).timeout);
      this.ttlMap.delete(key);
      if (this.indexManager.has(key)) {
          const currentMeta = { ...this.indexManager.get(key) };
          delete currentMeta.expiry;
          this.indexManager.set(key, currentMeta); // Update in-memory index directly
          this.indexManager.addChange(key, IndexManager.WAL_OP_SET, currentMeta); // Use constant from IndexManager if defined there
          this._scheduleIndexFlush();
      }
    }

    this._scheduleDataFlush();
    if (this.options.syncOnWrite) {
      this._flushDataToStorage(true);
      this._flushIndexManagerToWAL(true);
    }
    this.emit('set', key, value);
    return true;
  }

  get(key) {
    if (this.isClosing) return undefined;
    if (typeof key !== 'string' || key.length === 0) throw new TypeError('Key must be a non-empty string.');

    this.stats.reads++; this.stats.lastAccess = Date.now();
    if (this.store.has(key)) {
      this.stats.hits++; const value = this.store.get(key);
      this._updateLRU(key, false); this.emit('get', key, value); return value;
    }
    if (this.indexManager.has(key)) {
      try {
        const meta = this.indexManager.get(key);
        const value = this.storage.readValue(meta);
        this.stats.diskReads++; this.store.set(key, value);
        this._updateLRU(key, true); this.stats.hits++;
        this.emit('get', key, value); return value;
      } catch (err) { /* ... error ... */ this.stats.misses++; this.emit('miss', key); return undefined; }
    }
    this.stats.misses++; this.emit('miss', key); return undefined;
  }

  has(key) {
    if (this.isClosing) return false;
    if (typeof key !== 'string' || key.length === 0) throw new TypeError('Key must be a non-empty string.');
    return this.indexManager.has(key);
  }

  delete(key) {
    if (this.isClosing) return false;
    if (typeof key !== 'string' || key.length === 0) throw new TypeError('Key must be a non-empty string.');

    let changed = false;
    if (this.store.has(key)) { this.store.delete(key); /* ... LRU ... */ changed = true; }
    if (this.indexManager.has(key)) {
      const oldMeta = this.indexManager.get(key);
      this.stats.wastedSpace += oldMeta.size;
      this.indexManager.delete(key);
      this.indexManager.addChange(key, IndexManager.WAL_OP_DELETE || 2); // Use constant from IndexManager (WAL_OP_DELETE)
      changed = true;
    }
    if (changed) {
      if (this.ttlMap.has(key)) { /* ... clear TTL ... */ }
      this.stats.deletes++; this.stats.lastAccess = Date.now();
      this._scheduleIndexFlush();
      if (this.options.syncOnWrite) this._flushIndexManagerToWAL(true);
      this.emit('delete', key);
    }
    return changed;
  }

  clear() {
    if (this.isClosing) return;
    const oldSize = this.indexManager.size;
    this.emit('log', 'KV Core: Clear operation initiated.');

    this.store.clear(); this.lruKeys = [];
    this.indexManager.index.clear(); // Clear in-memory map in IndexManager
    this.dirtyDataKeys.clear();
    this.indexManager.pendingChanges.clear(); // Clear pending WAL changes in IndexManager

    for (const [, data] of this.ttlMap) clearTimeout(data.timeout);
    this.ttlMap.clear();

    try {
      this.storage.truncateDataFile();
      this.indexManager.truncateWALAndIndex(); // This will also perform a checkpoint of empty state
    } catch (err) { /* ... error handling ... */ }

    this.stats.lastAccess = Date.now();
    // Reset relevant stats
    this.stats.writes = 0; this.stats.reads = 0; this.stats.deletes = 0;
    this.stats.hits = 0; this.stats.misses = 0; this.stats.diskReads = 0;

    this.emit('clear', oldSize);
  }

  keys() { return this.indexManager.keys(); }
  size() { return this.indexManager.size; }
  getStats() { /* ... update from submodules and self ... */
    this.stats.wastedSpace = this.storage.getWastedSpace(this.indexManager.index); // Ensure fresh
    return {
        ...this.stats,
        activeKeys: this.indexManager.size,
        memoryStoreKeys: this.store.size,
        lruKeysCount: this.lruKeys.length,
        pendingDataWrites: this.dirtyDataKeys.size,
        pendingIndexChangesToWAL: this.indexManager.pendingChanges.size,
        dataFileSize: (this.storage.getFd() && !this.isClosing && fs.existsSync(this.options.dbPath)) ? fs.fstatSync(this.storage.getFd()).size : 0,
    };
  }

  async flush(forceSync = false) {
    if (this.isClosing) return false;
    let dataFlushed = false, indexFlushed = false;
    try {
        dataFlushed = this._flushDataToStorage(this.options.syncOnWrite || forceSync);
        indexFlushed = this._flushIndexManagerToWAL(this.options.syncOnWrite || forceSync);
    } catch (e) { /* ... error handling ... */ return false; }
    return dataFlushed || indexFlushed;
  }

  _scheduleDataFlush() {
    if (this.isClosing || this.flushDataPending || this.dirtyDataKeys.size === 0) return;
    this.flushDataPending = true;
    setImmediate(() => {
      this.flushDataPending = false;
      if (!this.isClosing && this.dirtyDataKeys.size > 0) this._flushDataToStorage(this.options.syncOnWrite);
    });
  }
  _scheduleIndexFlush() {
    if (this.isClosing || this.flushIndexPending || this.indexManager.pendingChanges.size === 0) return;
    this.flushIndexPending = true;
    setImmediate(() => {
      this.flushIndexPending = false;
      if (!this.isClosing && this.indexManager.pendingChanges.size > 0) this._flushIndexManagerToWAL(this.options.syncOnWrite);
    });
  }

  _flushDataToStorage(forceSyncDataFile = false) {
    if (this.isClosing || this.dirtyDataKeys.size === 0) return false;

    let dataFileCurrentOffset;
    try { dataFileCurrentOffset = fs.fstatSync(this.storage.getFd()).size; }
    catch (err) { /* ... error ... */ return false; }

    const flushedCount = this.dirtyDataKeys.size;
    const keysToProcess = Array.from(this.dirtyDataKeys);
    this.dirtyDataKeys.clear();

    for (const key of keysToProcess) {
      if (!this.store.has(key)) { /* ... log skip ... */ continue; }
      const value = this.store.get(key);
      try {
        const oldMeta = this.indexManager.get(key);
        if (oldMeta) this.stats.wastedSpace += oldMeta.size;

        const newMeta = this.storage.writeData(key, value, dataFileCurrentOffset);
        dataFileCurrentOffset += newMeta.size; // Update offset for next write

        if (this.ttlMap.has(key)) newMeta.expiry = this.ttlMap.get(key).expiry;

        this.indexManager.set(key, newMeta); // Update in-memory index
        this.indexManager.addChange(key, IndexManager.WAL_OP_SET || 1, newMeta); // Use constant
      } catch (err) { this.dirtyDataKeys.add(key); /* ... error ... */ }
    }
    // Note: stats.bytesWrittenToDataFile is updated by storage.writeData

    if (forceSyncDataFile && this.storage.getFd() && (dataFileCurrentOffset > (fs.fstatSync(this.storage.getFd()).size - (keysToProcess.length * 1024)) ) ) { // Heuristic: only fsync if data actually written
      try { fs.fsyncSync(this.storage.getFd()); }
      catch (err) { /* ... error ... */ }
    }
    if (flushedCount > 0) {
      this.emit('data_flush', flushedCount);
      if (this.indexManager.pendingChanges.size > 0) this._scheduleIndexFlush();
    }
    return flushedCount > 0;
  }

  _flushIndexManagerToWAL(forceSyncWAL = false) {
    if (this.isClosing) return false;
    return this.indexManager.flushToWAL(forceSyncWAL);
  }

  _setTTL(key, ttlMs, scheduleIndexUpdate = true) {
    if (this.ttlMap.has(key)) clearTimeout(this.ttlMap.get(key).timeout);
    if (ttlMs <= 0) {
        if (this.ttlMap.has(key)) {
            this.ttlMap.delete(key);
            if (scheduleIndexUpdate && this.indexManager.has(key)) {
                const meta = { ...this.indexManager.get(key) }; delete meta.expiry;
                this.indexManager.set(key, meta);
                this.indexManager.addChange(key, IndexManager.WAL_OP_SET || 1, meta);
                this._scheduleIndexFlush();
            }
        } return;
    }
    const expiry = Date.now() + ttlMs;
    const timeoutId = setTimeout(() => { /* ... expiry logic ... */
        if (this.isClosing) return;
        const currentTTL = this.ttlMap.get(key);
        if (currentTTL && currentTTL.expiry === expiry) {
            this.emit('log', `KV Core: TTL expired for key "${key}". Deleting.`);
            this.delete(key);
            if (this.ttlMap.has(key) && this.ttlMap.get(key).expiry === expiry) this.ttlMap.delete(key);
            this.emit('expired', key);
        }
    }, ttlMs);
    timeoutId.unref(); this.ttlMap.set(key, { expiry, timeout: timeoutId });
    if (scheduleIndexUpdate && this.indexManager.has(key)) {
        const meta = { ...this.indexManager.get(key) }; meta.expiry = expiry;
        this.indexManager.set(key, meta);
        this.indexManager.addChange(key, IndexManager.WAL_OP_SET || 1, meta);
        this._scheduleIndexFlush();
    }
  }

  async compact() {
    if (this.isClosing || this.isCompacting) { /* ... warn ... */ return false; }
    this.isCompacting = true; this.emit('compact_start');
    let success = false;
    try {
        await this.flush(true); // Sync all pending changes first

        const tempDataPath = `${this.options.dbPath}.compacting`;
        const tempStorage = new Storage(tempDataPath, { bytesWrittenToDataFile: 0 }, this); // Temp stats for this op
        const newIndexMap = new Map();
        let newOffset = 0;

        for (const [key, oldMeta] of this.indexManager.entries()) {
            let value;
            if (this.store.has(key)) value = this.store.get(key);
            else { try { value = this.storage.readValue(oldMeta); this.stats.diskReads++; } catch (e) { /* ... skip ... */ continue; } }

            try {
                const newTempMeta = tempStorage.writeData(key, value, newOffset);
                newOffset += newTempMeta.size;
                if (oldMeta.expiry) newTempMeta.expiry = oldMeta.expiry; // Preserve TTL
                newIndexMap.set(key, newTempMeta);
            } catch (e) { /* ... skip ... */ continue; }
        }
        fs.fsyncSync(tempStorage.getFd()); tempStorage.close(); // Sync and close temp data file

        this.storage.close(); // Close old data file
        fs.renameSync(tempDataPath, this.options.dbPath); // Replace old with new
        this.storage.reopenDataFile(); // Reopen new data file as primary

        this.indexManager.index = newIndexMap; // Update IndexManager's internal index
        this.indexManager.performCheckpoint(true, this.isClosing); // Force checkpoint, new index, clears WAL

        this.stats.compactions++; this.stats.lastCompactionTime = Date.now();
        this.stats.bytesWrittenToDataFile = newOffset; // Reset to size of new file
        this.stats.wastedSpace = 0;
        this.emit('compact_end', newOffset); success = true;
    } catch (err) { /* ... error handling ... */ }
    finally { this.isCompacting = false; }
    return success;
  }

  _compactIfNeeded() {
    if (this.isClosing || this.isCompacting) return;
    this.stats.wastedSpace = this.storage.getWastedSpace(this.indexManager.index);
    const dataFileSize = (this.storage.getFd() && !this.isClosing && fs.existsSync(this.options.dbPath)) ? fs.fstatSync(this.storage.getFd()).size : 0;
    if (dataFileSize === 0 || this.indexManager.size === 0) { /* ... */ return; }
    const wasteRatio = this.stats.wastedSpace / dataFileSize;
    if (wasteRatio >= this.options.compact.threshold) {
      this.compact().catch(err => this.emit('error', new Error(`KV Core: Automatic compaction failed: ${err.message}`)));
    }
  }

  async close() {
    if (this.isClosing) return true;
    this.isClosing = true; this.emit('closing');
    this.emit('log', 'KV Core: Close operation initiated.');

    if (this._flushIntervalId) clearInterval(this._flushIntervalId);
    if (this._compactIntervalId) clearInterval(this._compactIntervalId);
    if (this._checkpointIntervalId) clearInterval(this._checkpointIntervalId);
    for (const [, data] of this.ttlMap) if (data.timeout) clearTimeout(data.timeout);
    this.ttlMap.clear();

    try {
      this.emit('log', 'KV Core: Final flush...');
      await this._flushDataToStorage(true);
      await this._flushIndexManagerToWAL(true);
      this.emit('log', 'KV Core: Final checkpoint...');
      if (this.indexManager) await this.indexManager.performCheckpoint(true, true); // true for sync, true for closing
    } catch (err) { this.emit('error', new Error(`KV Core: Error during final operations on close: ${err.message}`)); }

    this.emit('log', 'KV Core: Closing submodules...');
    if (this.storage) this.storage.close();
    if (this.indexManager) this.indexManager.close();

    process.removeListener('exit', this._boundClose);
    process.removeListener('SIGINT', this._boundCloseAndExitSignal);
    /* ... remove other listeners ... */

    this.emit('log', 'KV Core: Database closed.');
    this.emit('close'); this.removeAllListeners();
    return true;
  }
}

module.exports = KV;