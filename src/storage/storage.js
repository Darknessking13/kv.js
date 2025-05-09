// src/storage/storage.js
const fs = require('fs');
const {
  TYPE_NULL, TYPE_UNDEFINED, TYPE_BOOLEAN, TYPE_NUMBER,
  TYPE_STRING, TYPE_BUFFER, TYPE_OBJECT, TYPE_ARRAY,
} = require('../core/constants');

class Storage {
  constructor(dbPath, statsRef, eventEmitterRef) {
    this.dbPath = dbPath;
    this.dataFile = null;
    this.stats = statsRef; // Reference to KV's stats object
    this.emitter = eventEmitterRef; // Reference to KV's event emitter
    this._init();
  }

  _init() {
    try {
      this.dataFile = fs.openSync(this.dbPath, 'a+');
    } catch (err) {
      const openError = new Error(`Storage: Failed to open data file "${this.dbPath}": ${err.message}`);
      openError.cause = err;
      if (this.emitter) this.emitter.emit('error', openError);
      throw openError;
    }
  }

  close() {
    if (this.dataFile !== null) {
      try {
        fs.closeSync(this.dataFile);
        this.dataFile = null;
      } catch (err) {
        if (this.emitter) this.emitter.emit('error', new Error(`Storage: Failed to close data file: ${err.message}`));
      }
    }
  }

  getFd() {
    return this.dataFile;
  }

  reopenDataFile() {
    if (this.dataFile !== null) {
        try { fs.closeSync(this.dataFile); } catch (e) { /* ignore */ }
    }
    this.dataFile = fs.openSync(this.dbPath, 'a+');
  }

  truncateDataFile() {
    if (this.dataFile !== null) {
        try { fs.closeSync(this.dataFile); } catch (e) { /* ignore */ }
    }
    fs.writeFileSync(this.dbPath, ''); // Truncate
    this.dataFile = fs.openSync(this.dbPath, 'a+');
    this.stats.bytesWrittenToDataFile = 0;
    this.stats.wastedSpace = 0;
  }


  writeData(key, value, currentOffset) {
    let serializedValue;
    try {
      serializedValue = this.serializeValue(value);
    } catch (err) {
      const serializeError = new Error(`Storage: Failed to serialize value for key "${key}": ${err.message}`);
      if (this.emitter) this.emitter.emit('error', serializeError);
      throw serializeError; // Re-throw for caller to handle
    }

    try {
      fs.writeSync(this.dataFile, serializedValue, 0, serializedValue.length, currentOffset);
      this.stats.bytesWrittenToDataFile += serializedValue.length;
      return {
        offset: currentOffset,
        size: serializedValue.length,
        type: this.getValueType(value),
      };
    } catch (err) {
      const writeError = new Error(`Storage: Failed to write key "${key}" to data file: ${err.message}`);
      if (this.emitter) this.emitter.emit('error', writeError);
      throw writeError;
    }
  }

  readValue(meta) { // meta is {offset, size, type}
    const bufferWithHeader = Buffer.alloc(meta.size);
    let bytesRead;
    try {
      bytesRead = fs.readSync(this.dataFile, bufferWithHeader, 0, meta.size, meta.offset);
    } catch (err) {
      throw new Error(`Storage: File read error at offset ${meta.offset} for size ${meta.size}: ${err.message}`);
    }

    if (bytesRead !== meta.size) {
      throw new Error(`Storage: Short read from data file: expected ${meta.size}, got ${bytesRead} at offset ${meta.offset}`);
    }
    this.stats.bytesReadFromDataFile += meta.size;
    return this.deserializeValue(bufferWithHeader);
  }

  serializeValue(value) {
    let type, dataBuffer;
    if (value === null) { type = TYPE_NULL; dataBuffer = Buffer.alloc(0); }
    else if (value === undefined) { type = TYPE_UNDEFINED; dataBuffer = Buffer.alloc(0); }
    else if (typeof value === 'boolean') { type = TYPE_BOOLEAN; dataBuffer = Buffer.from([value ? 1 : 0]); }
    else if (typeof value === 'number') { type = TYPE_NUMBER; dataBuffer = Buffer.alloc(8); dataBuffer.writeDoubleLE(value, 0); }
    else if (typeof value === 'string') { type = TYPE_STRING; dataBuffer = Buffer.from(value, 'utf8'); }
    else if (Buffer.isBuffer(value)) { type = TYPE_BUFFER; dataBuffer = value; }
    else if (Array.isArray(value)) { type = TYPE_ARRAY; dataBuffer = Buffer.from(JSON.stringify(value), 'utf8'); }
    else if (typeof value === 'object') { type = TYPE_OBJECT; dataBuffer = Buffer.from(JSON.stringify(value), 'utf8'); }
    else { throw new Error(`Unsupported value type for serialization: ${typeof value}`); }

    const header = Buffer.allocUnsafe(5);
    header[0] = type;
    header.writeUInt32LE(dataBuffer.length, 1);
    return Buffer.concat([header, dataBuffer]);
  }

  deserializeValue(bufferWithHeader) {
    if (!bufferWithHeader || bufferWithHeader.length < 5) {
      throw new Error('Storage: Invalid data buffer: too short for header.');
    }
    const type = bufferWithHeader[0];
    const dataLength = bufferWithHeader.readUInt32LE(1);
    if (5 + dataLength > bufferWithHeader.length) {
      throw new Error(`Storage: Invalid data buffer: declared data length ${dataLength} exceeds available buffer ${bufferWithHeader.length - 5}.`);
    }
    const dataBuffer = bufferWithHeader.subarray(5, 5 + dataLength);

    switch (type) {
      case TYPE_NULL: return null;
      case TYPE_UNDEFINED: return undefined;
      case TYPE_BOOLEAN: return dataBuffer[0] === 1;
      case TYPE_NUMBER: return dataBuffer.readDoubleLE(0);
      case TYPE_STRING: return dataBuffer.toString('utf8');
      case TYPE_BUFFER: return dataBuffer;
      case TYPE_OBJECT: case TYPE_ARRAY: return JSON.parse(dataBuffer.toString('utf8'));
      default: throw new Error(`Storage: Unknown data type flag during deserialization: ${type}`);
    }
  }

  getValueType(value) {
    if (value === null) return TYPE_NULL;
    if (value === undefined) return TYPE_UNDEFINED;
    if (typeof value === 'boolean') return TYPE_BOOLEAN;
    if (typeof value === 'number') return TYPE_NUMBER;
    if (typeof value === 'string') return TYPE_STRING;
    if (Buffer.isBuffer(value)) return TYPE_BUFFER;
    if (Array.isArray(value)) return TYPE_ARRAY;
    if (typeof value === 'object') return TYPE_OBJECT;
    throw new Error(`Storage: Unsupported value type for getType: ${typeof value}`);
  }

  getWastedSpace(currentIndexMap) {
    if (!this.dataFile) return 0;
    try {
        const dataFileStat = fs.fstatSync(this.dataFile);
        const totalDataFileSize = dataFileStat.size;
        let liveDataSize = 0;
        for (const meta of currentIndexMap.values()) {
            liveDataSize += meta.size;
        }
        return Math.max(0, totalDataFileSize - liveDataSize);
    } catch(e) {
        if(this.emitter) this.emitter.emit('error', new Error(`Storage: Error calculating wasted space: ${e.message}`));
        return 0;
    }
  }
}

module.exports = Storage;