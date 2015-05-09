package hbase

import (
	"encoding/binary"
)

const MAGIC = 255
const MAGIC_SIZE = 1
const ID_LENGTH_SIZE = 4
const MD5_HEX_LENGTH = 32
const SERVERNAME_SEPARATOR = ","
const NINES = "99999999999999"
const ZEROS = "00000000000000"
const RPC_TIMEOUT = 30000
const PING_TIMEOUT = 30000
const CALL_TIMEOUT = 5000
const SOCKET_RETRY_WAIT_MS = 200
const MAX_ACTION_RETRIES = 3

var BYTE_ORDER binary.ByteOrder = binary.BigEndian
var HEADER []byte = []byte("HBas")
var META_TABLE_NAME []byte = []byte("hbase:meta")
var META_REGION_NAME []byte = []byte("hbase:meta,,1")
