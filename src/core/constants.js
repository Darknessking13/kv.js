// kv-store/constants.js
const TYPE_NULL = 0;
const TYPE_UNDEFINED = 1;
const TYPE_BOOLEAN = 2;
const TYPE_NUMBER = 3;
const TYPE_STRING = 4;
const TYPE_BUFFER = 5;
const TYPE_OBJECT = 6;
const TYPE_ARRAY = 7;

const WAL_OP_SET = 1;
const WAL_OP_DELETE = 2;
const WAL_ENTRY_HEADER_SIZE = 1 + 4; // opType (1 byte) + keyLength (4 bytes)

module.exports = {
  TYPE_NULL,
  TYPE_UNDEFINED,
  TYPE_BOOLEAN,
  TYPE_NUMBER,
  TYPE_STRING,
  TYPE_BUFFER,
  TYPE_OBJECT,
  TYPE_ARRAY,
  WAL_OP_SET,
  WAL_OP_DELETE,
  WAL_ENTRY_HEADER_SIZE,
};