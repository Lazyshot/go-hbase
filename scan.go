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

	StartRow []byte
	StopRow  []byte

	Columns []*proto.Column

	nextStartRow []byte

	numCached int
	closed    bool

	location *regionInfo
	server   *connection
}

func newScan(table []byte, client *Client) *Scan {
	return &Scan{
		client:       client,
		table:        table,
		nextStartRow: nil,

		numCached: 100,
		closed:    false,
	}
}

func (s *Scan) SetCached(n int) {
	s.numCached = n
}

func (s *Scan) Map(f func(*ResultRow)) {
	for {
		results := s.next()

		if results == nil {
			break
		}

		for _, v := range results {
			f(v)
		}
	}
}

func (s *Scan) Close() {
	if s.closed == false {
		s.closeScan(s.server, s.location, s.id)
		s.closed = true
	}
}

func (s *Scan) getData(nextStart []byte) []*ResultRow {
	if s.closed {
		return nil
	}

	server, location := s.getServerAndLocation(s.table, nextStart)

	req := &proto.ScanRequest{
		Region: &proto.RegionSpecifier{
			Type:  proto.RegionSpecifier_REGION_NAME.Enum(),
			Value: []byte(location.name),
		},
		NumberOfRows: pb.Uint32(uint32(s.numCached)),
		Scan:         &proto.Scan{},
	}

	if s.id > 0 {
		req.ScannerId = pb.Uint64(s.id)
	}

	if s.StartRow != nil && s.StopRow != nil {
		req.Scan.StartRow = s.StartRow
		req.Scan.StopRow = s.StopRow
	}
	log.Debug("sending scan request: [server=%s]", server.name)

	cl := newCall(req)
	server.call(cl)

	return s.processResponse(<-cl.responseCh)
}

func (s *Scan) processResponse(response *pb.Message) []*ResultRow {
	var res *proto.ScanResponse
	switch r := (*response).(type) {
	case *proto.ScanResponse:
		res = r
	default:
		return nil
	}

	nextRegion := true
	s.nextStartRow = nil
	s.id = res.GetScannerId()

	results := res.GetResults()
	n := len(results)

	if res.GetMoreResults() && n != 0 {
		tbr := make([]*ResultRow, n)
		for i, v := range results {
			tbr[i] = newResultRow(v)
		}

		return tbr
	}

	if (n == s.numCached) ||
		len(s.location.endKey) == 0 ||
		(s.StopRow != nil && bytes.Compare(s.location.endKey, s.StopRow) > 0 && n < s.numCached) {
		nextRegion = false
	}

	if nextRegion {
		s.nextStartRow = incrementByteString(s.location.endKey, len(s.location.endKey)-1)
		s.closeScan(s.server, s.location, s.id)
		s.server = nil
		s.location = nil
		s.id = 0
	}

	if n == 0 {
		s.Close()
	}

	tbr := make([]*ResultRow, n)
	for i, v := range results {
		tbr[i] = newResultRow(v)
	}

	return tbr
}

func (s *Scan) next() []*ResultRow {
	startRow := s.nextStartRow
	if startRow == nil {
		startRow = s.StartRow
	}

	return s.getData(startRow)
}

func (s *Scan) closeScan(server *connection, location *regionInfo, id uint64) {
	req := &proto.ScanRequest{
		Region: &proto.RegionSpecifier{
			Type:  proto.RegionSpecifier_REGION_NAME.Enum(),
			Value: []byte(location.name),
		},
		ScannerId:    pb.Uint64(id),
		CloseScanner: pb.Bool(true),
	}

	server.call(newCall(req))
}

func (s *Scan) getServerAndLocation(table, startRow []byte) (server *connection, location *regionInfo) {
	if s.server != nil && s.location != nil {
		server = s.server
		location = s.location
		return
	}
	log.Debug("Get next region to scan [table=%s] [startRow=%s]", table, startRow)

	location = s.client.locateRegion(table, startRow, true)
	server = s.client.getRegionConnection(location.server)

	log.Debug("Got the next region to scan [table=%s] [regionName=%s] [conn=%s]", table, location.name, server.name)

	s.server = server
	s.location = location
	return
}
