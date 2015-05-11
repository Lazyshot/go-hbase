package hbase

import (
	pb "github.com/golang/protobuf/proto"
	"github.com/lazyshot/go-hbase/proto"

	"bytes"
	"fmt"
	"strings"
)

type Scan struct {
	client *Client

	id    uint64
	table []byte

	StartRow []byte
	StopRow  []byte

	families   [][]byte
	qualifiers [][][]byte

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

		families:   make([][]byte, 0),
		qualifiers: make([][][]byte, 0),

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
		log.Debug("Closing scan: %d", s.id)
		s.closeScan(s.server, s.location, s.id)
		s.closed = true
	}
}

func (s *Scan) AddString(famqual string) error {
	parts := strings.Split(famqual, ":")

	if len(parts) > 2 {
		return fmt.Errorf("Too many colons were found in the family:qualifier string. '%s'", famqual)
	} else if len(parts) == 2 {
		s.AddStringColumn(parts[0], parts[1])
	} else {
		s.AddStringFamily(famqual)
	}

	return nil
}

func (s *Scan) AddStringColumn(family, qual string) {
	s.AddColumn([]byte(family), []byte(qual))
}

func (s *Scan) AddStringFamily(family string) {
	s.AddFamily([]byte(family))
}

func (s *Scan) AddColumn(family, qual []byte) {
	s.AddFamily(family)

	pos := s.posOfFamily(family)

	s.qualifiers[pos] = append(s.qualifiers[pos], qual)
}

func (s *Scan) AddFamily(family []byte) {
	pos := s.posOfFamily(family)

	if pos == -1 {
		s.families = append(s.families, family)
		s.qualifiers = append(s.qualifiers, make([][]byte, 0))
	}
}

func (s *Scan) posOfFamily(family []byte) int {
	for p, v := range s.families {
		if bytes.Equal(family, v) {
			return p
		}
	}
	return -1
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

	for i, v := range s.families {
		req.Scan.Column = append(req.Scan.Column, &proto.Column{
			Family:    v,
			Qualifier: s.qualifiers[i],
		})
	}

	log.Debug("sending scan request: [server=%s] [id=%d]", server.name, s.id)

	cl := newCall(req)
	server.call(cl)

	log.Debug("sent scan request: [server=%s] [id=%d]", server.name, s.id)

	select {
	case msg := <-cl.responseCh:
		return s.processResponse(msg)
	}
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

	if (n == s.numCached) ||
		len(s.location.endKey) == 0 ||
		(s.StopRow != nil && bytes.Compare(s.location.endKey, s.StopRow) > 0 && n < s.numCached) {
		nextRegion = false
	}

	if nextRegion {
		log.Debug("Reset values, go to next region")
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
	cl := newCall(req)
	server.call(cl)
	<-cl.responseCh
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
