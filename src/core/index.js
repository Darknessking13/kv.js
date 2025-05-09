// src/core/index.js
const fs = require('fs');
const path = require('path');
const { WAL_OP_SET, WAL_OP_DELETE, WAL_ENTRY_HEADER_SIZE } = require('./constants');

class IndexManager {
  constructor(options, statsRef, eventEmitterRef, ttlMapRef, setTTLFunctionRef) {
    this.indexPath = options.indexPath;
    this.walPath = options.walPath;
    this.syncOnWrite = options.syncOnWrite; // For checkpoint sync behavior
    this.checkpointOptions = options.checkpoint;

    this.index = new Map(); // In-memory representation: key -> {offset, size, type, expiry?}
    this.pendingChanges = new Map(); // Key -> { op: WAL_OP_SET/DELETE, meta?: object }
    this.walFile = null;
    this.isCheckpointing = false;

    this.stats = statsRef;
    this.emitter = eventEmitterRef;
    this.ttlMap = ttlMapRef; // Reference to KV's ttlMap
    this._setTTL = setTTLFunctionRef; // Reference to KV's _setTTL method

    this._init();
  }

  _init() {
    try {
      this.walFile = fs.openSync(this.walPath, 'a+');
      this._loadBase();
      this._replayWAL();
      this._updateWalSizeStat();
    } catch (err) {
      const initError = new Error(`IndexManager: Initialization failed: ${err.message}`);
      initError.cause = err;
      if (this.emitter) this.emitter.emit('error', initError);
      throw initError;
    }
  }

  close() {
    if (this.walFile !== null) {
      try {
        fs.closeSync(this.walFile);
        this.walFile = null;
      } catch (err) {
        if (this.emitter) this.emitter.emit('error', new Error(`IndexManager: Failed to close WAL file: ${err.message}`));
      }
    }
  }

  get(key) { return this.index.get(key); }
  has(key) { return this.index.has(key); }
  set(key, meta) { this.index.set(key, meta); }
  delete(key) { return this.index.delete(key); }
  keys() { return Array.from(this.index.keys()); }
  entries() { return this.index.entries(); }
  values() { return this.index.values(); }
  get size() { return this.index.size; }

  addChange(key, op, meta) {
    this.pendingChanges.set(key, { op, meta });
  }

  _loadBase() {
    try {
      if (fs.existsSync(this.indexPath)) {
        const indexFileStat = fs.statSync(this.indexPath);
        if (indexFileStat.size > 0) {
          const indexData = fs.readFileSync(this.indexPath);
          const parsed = JSON.parse(indexData.toString('utf8'));
          this.index = new Map(Object.entries(parsed.index || {}));
          // Restore only stats that are part of the checkpoint, others are dynamic
          if (parsed.stats) {
            this.stats.lastCheckpointTime = parsed.stats.lastCheckpointTime || 0;
            this.stats.checkpoints = parsed.stats.checkpoints || 0;
            // Other stats like bytesWrittenToWAL are reset or re-evaluated.
          }
          this.stats.indexSizeBytes = indexFileStat.size;
        }
      }
    } catch (err) {
      if (this.emitter) this.emitter.emit('error', new Error(`IndexManager: Failed to load base index: ${err.message}. Proceeding with empty or WAL-replayed index.`));
      this.index = new Map();
    }
  }

  _replayWAL() {
    try {
      const walStat = fs.fstatSync(this.walFile);
      if (walStat.size === 0) return;

      const walBuffer = Buffer.alloc(walStat.size);
      fs.readSync(this.walFile, walBuffer, 0, walStat.size, 0);
      let offset = 0;
      let replayedOps = 0;
      const now = Date.now();

      if (this.emitter) this.emitter.emit('log', `IndexManager WAL_REPLAY: Starting replay of ${walStat.size} bytes.`);
      while (offset < walStat.size) {
        const initialOffset = offset;
        if (offset + WAL_ENTRY_HEADER_SIZE > walStat.size) {
          if (this.emitter) this.emitter.emit('warn', `IndexManager WAL_REPLAY: Incomplete header at offset ${offset}. Truncating WAL here.`);
          // fs.ftruncateSync(this.walFile, offset); // Optional: Truncate corrupted part
          break;
        }
        const opType = walBuffer.readUInt8(offset);
        const keyLength = walBuffer.readUInt32LE(offset + 1);
        offset += WAL_ENTRY_HEADER_SIZE;

        if (offset + keyLength > walStat.size) {
          if (this.emitter) this.emitter.emit('warn', `IndexManager WAL_REPLAY: Incomplete key data (keyLength=${keyLength}) for op ${opType}. Truncating WAL.`);
          // fs.ftruncateSync(this.walFile, initialOffset);
          break;
        }
        const key = walBuffer.toString('utf8', offset, offset + keyLength);
        offset += keyLength;

        if (opType === WAL_OP_SET) {
          if (offset + 4 > walStat.size) {
             if (this.emitter) this.emitter.emit('warn', `IndexManager WAL_REPLAY: Incomplete meta length for SET op on key "${key}". Truncating WAL.`);
             // fs.ftruncateSync(this.walFile, initialOffset);
             break;
          }
          const metaLength = walBuffer.readUInt32LE(offset);
          offset += 4;
          if (offset + metaLength > walStat.size) {
            if (this.emitter) this.emitter.emit('warn', `IndexManager WAL_REPLAY: Incomplete meta data (metaLength=${metaLength}) for SET op on key "${key}". Truncating WAL.`);
            // fs.ftruncateSync(this.walFile, initialOffset);
            break;
          }
          const metaString = walBuffer.toString('utf8', offset, offset + metaLength);
          offset += metaLength;
          try {
            const meta = JSON.parse(metaString);
            this.index.set(key, meta);
            if (meta.expiry && meta.expiry > now) {
                this._setTTL(key, meta.expiry - now, false); // false: already from WAL, don't re-queue
            } else if (meta.expiry && meta.expiry <= now) {
                this.index.delete(key);
            }
          } catch (e) {
            if (this.emitter) this.emitter.emit('error', new Error(`IndexManager WAL_REPLAY: Failed to parse meta for key "${key}": ${e.message}. Skipping entry.`));
          }
        } else if (opType === WAL_OP_DELETE) {
          this.index.delete(key);
          if (this.ttlMap.has(key)) {
            clearTimeout(this.ttlMap.get(key).timeout);
            this.ttlMap.delete(key);
          }
        } else {
          if (this.emitter) this.emitter.emit('warn', `IndexManager WAL_REPLAY: Unknown WAL op type ${opType} at offset ${initialOffset}. Truncating.`);
          // fs.ftruncateSync(this.walFile, initialOffset);
          break;
        }
        replayedOps++;
      }
      if (this.emitter) this.emitter.emit('wal_replayed', { replayedOps, finalIndexSize: this.index.size });
    } catch (err) {
      if (this.emitter) this.emitter.emit('error', new Error(`IndexManager: Failed to replay WAL: ${err.message}. Index may be inconsistent.`));
    }
  }

  _updateWalSizeStat() {
    if (this.walFile) {
      try { this.stats.walSizeBytes = fs.fstatSync(this.walFile).size; }
      catch (e) { this.stats.walSizeBytes = 0; }
    } else {
        this.stats.walSizeBytes = 0;
    }
  }

  flushToWAL(forceSyncWAL = false) {
    if (this.pendingChanges.size === 0) return false;

    const walEntriesBufferArray = [];
    let totalPayloadLength = 0;
    const changesToFlush = new Map(this.pendingChanges);
    this.pendingChanges.clear();

    for (const [key, change] of changesToFlush) {
      const keyBuffer = Buffer.from(key, 'utf8');
      if (keyBuffer.length > 0xFFFFFFFF) { /* ... error ... */ continue; }

      const entryHeader = Buffer.allocUnsafe(WAL_ENTRY_HEADER_SIZE);
      entryHeader.writeUInt8(change.op, 0);
      entryHeader.writeUInt32LE(keyBuffer.length, 1);
      walEntriesBufferArray.push(entryHeader, keyBuffer);
      totalPayloadLength += WAL_ENTRY_HEADER_SIZE + keyBuffer.length;

      if (change.op === WAL_OP_SET) {
        const metaString = JSON.stringify(change.meta);
        const metaBuffer = Buffer.from(metaString, 'utf8');
        if (metaBuffer.length > 0xFFFFFFFF) { /* ... error ... */ continue; }
        const metaHeader = Buffer.allocUnsafe(4);
        metaHeader.writeUInt32LE(metaBuffer.length, 0);
        walEntriesBufferArray.push(metaHeader, metaBuffer);
        totalPayloadLength += 4 + metaBuffer.length;
      }
    }

    if (walEntriesBufferArray.length > 0) {
      const fullWalPayload = Buffer.concat(walEntriesBufferArray, totalPayloadLength);
      try {
        const currentWalWriteOffset = this.stats.walSizeBytes; // Write at the end of current WAL
        fs.writeSync(this.walFile, fullWalPayload, 0, fullWalPayload.length, currentWalWriteOffset);
        this.stats.bytesWrittenToWAL += fullWalPayload.length;
        this.stats.walSizeBytes += fullWalPayload.length;

        if (forceSyncWAL) {
          fs.fsyncSync(this.walFile);
        }
        if (this.emitter) this.emitter.emit('index_wal_flush', changesToFlush.size);
        this.checkIfCheckpointNeededBySize();
        return true;
      } catch (err) {
        if (this.emitter) this.emitter.emit('error', new Error(`IndexManager: Failed to write ${changesToFlush.size} changes to WAL: ${err.message}`));
        // Re-queue failed changes for next attempt
        for(const [key, change] of changesToFlush) this.pendingChanges.set(key, change);
        this._updateWalSizeStat(); // Correct WAL size stat after failed write
        return false;
      }
    }
    return false;
  }

  performCheckpoint(forceSyncIndexFile = false, kvIsClosing = false) {
    if (this.isCheckpointing && !kvIsClosing) { // Allow checkpoint during close even if one was ongoing
      if (this.emitter) this.emitter.emit('warn', 'IndexManager: Checkpoint attempt skipped, already in progress.');
      return false;
    }
    this.isCheckpointing = true;
    if (this.emitter && !kvIsClosing) this.emitter.emit('checkpoint_start');
    let success = false;

    try {
      // Ensure any last-minute pending changes are flushed to WAL before checkpointing
      // This is crucial so the in-memory index accurately reflects all persisted operations.
      // If KV is closing, it handles prior flushes. Otherwise, we ensure WAL is up-to-date.
      if (!kvIsClosing) {
          this.flushToWAL(this.syncOnWrite || forceSyncIndexFile);
      }


      const indexObjectForJSON = {};
      for (const [key, meta] of this.index.entries()) {
        indexObjectForJSON[key] = meta;
      }

      const persistentStatsSnapshot = { // Only save stats relevant to a checkpoint
          lastCheckpointTime: this.stats.lastCheckpointTime,
          checkpoints: this.stats.checkpoints,
          // The main stats object in KV holds the rest (bytesWritten, etc.)
      };

      const indexContent = JSON.stringify({
        index: indexObjectForJSON,
        stats: persistentStatsSnapshot,
        updatedAt: Date.now(),
      });

      const tempIndexPath = `${this.indexPath}.tmp`;
      fs.writeFileSync(tempIndexPath, indexContent);
      if (forceSyncIndexFile) {
        const tempFd = fs.openSync(tempIndexPath, 'r+');
        try { fs.fsyncSync(tempFd); } finally { fs.closeSync(tempFd); }
      }
      fs.renameSync(tempIndexPath, this.indexPath);
      this.stats.indexSizeBytes = Buffer.byteLength(indexContent, 'utf8');

      try {
        fs.ftruncateSync(this.walFile, 0); // Clear the WAL file
        if (forceSyncIndexFile || this.syncOnWrite) { // Also sync WAL truncation if base index was synced
            fs.fsyncSync(this.walFile);
        }
        this.stats.walSizeBytes = 0;
        this.stats.bytesWrittenToWAL = 0; // Reset session counter for WAL writes
      } catch (walErr) {
        if (this.emitter) this.emitter.emit('error', new Error(`IndexManager: Failed to truncate WAL after checkpoint: ${walErr.message}. WAL needs manual attention.`));
      }
      this.stats.checkpoints++;
      this.stats.lastCheckpointTime = Date.now();
      if (this.emitter && !kvIsClosing) this.emitter.emit('checkpoint_end', this.index.size);
      success = true;
    } catch (err) {
      if (this.emitter) this.emitter.emit('error', new Error(`IndexManager: Checkpoint (base index save) failed: ${err.message}`));
      const tempIndexPath = `${this.indexPath}.tmp`;
      if (fs.existsSync(tempIndexPath)) try { fs.unlinkSync(tempIndexPath); } catch (e) { /* ignore */ }
    } finally {
      this.isCheckpointing = false;
    }
    return success;
  }

  checkIfCheckpointNeededBySize() { // Called after WAL flush
    if (this.isCheckpointing) return;
    if (this.stats.walSizeBytes >= this.checkpointOptions.walSizeThreshold) {
        if (this.emitter) this.emitter.emit('log', `IndexManager: WAL size ${this.stats.walSizeBytes}b reached threshold ${this.checkpointOptions.walSizeThreshold}b. Triggering checkpoint.`);
        this.performCheckpoint(true); // Auto-checkpoint due to size should be durable
    }
  }

  truncateWALAndIndex() { // Used by KV's clear()
    // Close and truncate WAL file
    if (this.walFile !== null) { fs.closeSync(this.walFile); this.walFile = null; }
    fs.writeFileSync(this.walPath, ''); // Truncate WAL
    this.walFile = fs.openSync(this.walPath, 'a+');
    this.stats.bytesWrittenToWAL = 0;
    this.stats.walSizeBytes = 0;

    // Write an empty base index file
    this.performCheckpoint(this.syncOnWrite, true); // true for kvIsClosing to simplify logic
  }
}

module.exports = IndexManager;