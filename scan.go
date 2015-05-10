package hbase

import (
	pb "github.com/golang/protobuf/proto"
	"github.com/lazyshot/go-hbase/proto"

	"bytes"
)

type Scan struct {
	client *Client

	id    uint64
	table []byte

	startRow     []byte
	nextStartRow []byte
	stopRow      []byte

	numCached int
	closed    bool

	location *regionInfo
	server   *connection
}

func newScan(table, startRow, stopRow []byte, client *Client) *Scan {
	return &Scan{
		client:       client,
		table:        table,
		startRow:     startRow,
		stopRow:      stopRow,
		nextStartRow: nil,

		numCached: 100,
		closed:    false,
	}
}

func (s *Scan) getData(nextStart) {
	if s.closed {
		return
	}

	server, location := s.getServerAndLocation(s.table, nextStart)

	req := &proto.ScanRequest{
		Region: &proto.RegionSpecifier{
			Type:  proto.RegionSpecifier_REGION_NAME.Enum(),
			Value: location.name,
		},
		NumberOfRows: pb.Uint32(uint32(s.numCached)),
		Scan:         &proto.Scan{},
	}

	if s.id > 0 {
		req.ScannerId = pb.Uint64(s.id)
	}

	if s.startRow != nil && s.stopRow != nil {
		req.Scan.StartRow = s.startRow
		req.Scan.StopRow = s.stopRow
	}

	cl := newCall(req)
	server.call(cl)

	response := <-cl.responseCh
}

func (s *Scan) processResponse(response pb.Message) {
	var res *proto.ScanResponse
	nextRegion := true

	switch r := (*response).(type) {
	case *proto.ScanResponse:
		res = r
	default:
		return
	}

	results := res.GetResults()
	n := len(results)

	if (n == s.numCached) ||
		len(s.location.endKey) == 0 ||
		(s.stopRow && bytes.Compare(s.location.endKey, s.stopRow) > 0 && n < s.numCached) {
		nextRegion = false
	}

	if n < s.numCached {
		s.nextStartRow = incrementByteString(s.location.endKey)
	}

	if nextRegion {
		s.closeScan(server, location, s.id)
	}

}

func (s *Scan) closeScan(server *connection, location *regionInfo, id uint64) {

}

func (s *Scan) getServerAndLocation(table, startRow []byte) (server *connection, location *regionInfo) {
	location = s.client.locateRegion(table, startRow, true)
	server = s.client.getRegionConnection(location.server)
}
