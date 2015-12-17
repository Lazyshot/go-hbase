package hbase

import (
	"math"
	"time"

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

	//for filters
	timeRange *TimeRange

	location *regionInfo
	server   *connection
}

const (
	// copy from golang source code time/time.go
	// since it is private in time package, just copy it
	secondsPerHour       = 60 * 60
	secondsPerDay        = 24 * secondsPerHour
	unixToInternal int64 = (1969*365 + 1969/4 - 1969/100 + 1969/400) * secondsPerDay
)

var maxTimestamp time.Time

func init() {
	// because golang's internal time represent is from Jan 1st, 1
	// so if we want the max timestamp, we have to minus time.unixToInternal
	// and as hbase accept a microsecond based timestamp, we divided it by 1000
	maxTimestamp = time.Unix((math.MaxInt64-unixToInternal)/1000, 0)
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

		timeRange: nil,
	}
}

// set scan time range
func (s *Scan) SetTimeRange(from time.Time, to time.Time) {
	if s.timeRange == nil {
		s.timeRange = &TimeRange{
			From: from,
			To:   to,
		}
	} else {
		s.timeRange.From = from
		s.timeRange.To = to
	}
}

// set scan time start only.
// if set start only, use a max timestamp as end automaticly.
// but since max timestamp is not a precise value,
// you'd better use `SetTimeRange()` set start and end yourself.
func (s *Scan) SetTimeRangeFrom(from time.Time) {
	s.SetTimeRange(from, maxTimestamp)
}

// set scan time end only.
//if only set end, use time.Unix(0, 0)(Jan 1st, 1970) as start automaticly
func (s *Scan) SetTimeRangeTo(to time.Time) {
	s.SetTimeRange(time.Unix(0, 0), to)
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

			if s.closed {
				return
			}
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
	} else {
		if s.StartRow != nil {
			req.Scan.StartRow = s.StartRow
		}
		if s.StopRow != nil {
			req.Scan.StopRow = s.StopRow
		}
		if s.timeRange != nil {
			req.Scan.TimeRange = &proto.TimeRange{
				From: pb.Uint64(uint64(s.timeRange.From.Unix()) * 1000),
				To:   pb.Uint64(uint64(s.timeRange.To.Unix()) * 1000),
			}
		}
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

var lastRegionRows int = 0

func (s *Scan) processResponse(response pb.Message) []*ResultRow {
	var res *proto.ScanResponse
	switch r := response.(type) {
	case *proto.ScanResponse:
		res = r
	default:
		log.Error("Invalid response returned: %T", response)
		return nil
	}

	nextRegion := true
	s.nextStartRow = nil
	s.id = res.GetScannerId()

	results := res.GetResults()
	n := len(results)

	lastRegionRows += n

	if (n == s.numCached) ||
		len(s.location.endKey) == 0 ||
		(s.StopRow != nil && bytes.Compare(s.location.endKey, s.StopRow) > 0 && n < s.numCached) ||
		(res.GetMoreResults() && n > 0) {
		nextRegion = false
	}

	if n < s.numCached {
		s.nextStartRow = incrementByteString(s.location.endKey, len(s.location.endKey)-1)
	}

	if nextRegion {
		log.Debug("Finished %s. Num results from region: %d. On to the next one.", s.location.name, lastRegionRows)
		s.closeScan(s.server, s.location, s.id)
		s.server = nil
		s.location = nil
		s.id = 0
		lastRegionRows = 0
	}

	if n == 0 && !nextRegion {
		log.Debug("N == 0 and !nextRegion")
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
