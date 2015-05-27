package hbase

import (
	"encoding/binary"
)

const magic = 255
const magic_size = 1
const id_length_size = 4
const md5_hex_length = 32
const servername_separator = ","
const nines = "99999999999999"
const zeros = "00000000000000"
const rpc_timeout = 30000
const ping_timeout = 30000
const call_timeout = 5000
const socket_retry_wait_ms = 200
const max_action_retries = 3

var byte_order binary.ByteOrder = binary.BigEndian
var hbase_header_bytes []byte = []byte("HBas")
var meta_table_name []byte = []byte("hbase:meta")
var meta_region_name []byte = []byte("hbase:meta,,1")
