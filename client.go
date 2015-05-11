package hbase

import (
	pb "github.com/golang/protobuf/proto"
	"github.com/lazyshot/go-hbase/proto"
	"github.com/op/go-logging"
	"github.com/samuel/go-zookeeper/zk"

	"bytes"
	"crypto/md5"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"os"
	"strings"
	"time"
)

type Client struct {
	zkClient         *zk.Conn
	zkHosts          []string
	zkRoot           string
	zkRootRegionPath string

	servers               map[string]*connection
	cachedRegionLocations map[string]map[string]*regionInfo

	prefetched map[string]bool

	rootServer *proto.ServerName
}

var log = logging.MustGetLogger("hbase-client")
var format = logging.MustStringFormatter(
	"%{color}%{time:15:04:05.000} %{shortfunc} [%{level:.5s}]:%{color:reset} %{message}",
)

func init() {
	backend := logging.NewLogBackend(os.Stderr, "", 0)
	backendFormatter := logging.NewBackendFormatter(backend, format)
	logging.SetBackend(backendFormatter)

	logging.SetLevel(logging.INFO, "hbase-client")
}

func NewClient(zkHosts []string, zkRoot string) *Client {
	cl := &Client{
		zkHosts:          zkHosts,
		zkRoot:           zkRoot,
		zkRootRegionPath: "/meta-region-server",

		servers:               make(map[string]*connection),
		cachedRegionLocations: make(map[string]map[string]*regionInfo),
		prefetched:            make(map[string]bool),
	}

	cl.initZk()

	return cl
}

func (c *Client) SetLogLevel(level string) {
	lvl, _ := logging.LogLevel(level)
	logging.SetLevel(lvl, "hbase-client")
}

func (c *Client) initZk() {
	zkclient, _, err := zk.Connect(c.zkHosts, time.Second*30)
	if err != nil {
		panic(err)
	}

	c.zkClient = zkclient

	res, _, _, err := c.zkClient.GetW(c.zkRoot + c.zkRootRegionPath)

	if err != nil {
		panic(err)
	}

	c.rootServer = c.decodeMeta(res)
	c.getRegionConnection(c.getServerName(c.rootServer))
}

func (c *Client) decodeMeta(data []byte) *proto.ServerName {
	if data[0] != MAGIC {
		return nil
	}

	var n int32
	binary.Read(bytes.NewBuffer(data[1:]), BYTE_ORDER, &n)

	dataOffset := MAGIC_SIZE + ID_LENGTH_SIZE + int(n)

	data = data[(dataOffset + 4):]

	var mrs proto.MetaRegionServer
	err := pb.Unmarshal(data, &mrs)
	if err != nil {
		panic(err)
	}

	return mrs.GetServer()
}

func (c *Client) getServerName(server *proto.ServerName) string {
	return fmt.Sprintf("%s:%d", server.GetHostName(), server.GetPort())
}

func (c *Client) getRegionConnection(server string) *connection {
	if s, ok := c.servers[server]; ok {
		return s
	}

	conn, err := newConnection(server)
	if err != nil {
		panic(err)
	}

	c.servers[server] = conn

	return conn
}

func (c *Client) action(table, row []byte, action Action) *call {
	log.Debug("Attempting action [table: %s] [row: %s] [action: %#v]", table, row, action)

	region := c.locateRegion(table, row, true)
	conn := c.getRegionConnection(region.server)

	regionSpecifier := &proto.RegionSpecifier{
		Type:  proto.RegionSpecifier_REGION_NAME.Enum(),
		Value: []byte(region.name),
	}

	var cl *call = nil
	switch a := action.(type) {
	case *Get:
		cl = newCall(&proto.GetRequest{
			Region: regionSpecifier,
			Get:    a.toProto().(*proto.Get),
		})
	case *Put:
		cl = newCall(&proto.MutateRequest{
			Region:   regionSpecifier,
			Mutation: a.toProto().(*proto.MutationProto),
		})
	}

	if cl != nil {
		conn.call(cl)
	}

	return cl
}

type multiaction struct {
	row    []byte
	action Action
}

func (c *Client) multiaction(table []byte, actions []multiaction) []*call {
	actionsByServer := make(map[string]map[string][]multiaction)

	for _, action := range actions {
		region := c.locateRegion(table, action.row, true)

		if _, ok := actionsByServer[region.server]; !ok {
			actionsByServer[region.server] = make(map[string][]multiaction)
		}

		if _, ok := actionsByServer[region.server][region.name]; ok {
			actionsByServer[region.server][region.name] = append(actionsByServer[region.server][region.name], action)
		} else {
			actionsByServer[region.server][region.name] = []multiaction{action}
		}
	}

	calls := make([]*call, 0)

	for server, as := range actionsByServer {
		region_actions := make([]*proto.RegionAction, len(as))
		i := 0

		for region, acts := range as {
			racts := make([]*proto.Action, len(acts))
			for i, act := range acts {
				racts[i] = &proto.Action{
					Index: pb.Uint32(uint32(i)),
				}

				switch a := act.action.(type) {
				case *Get:
					racts[i].Get = a.toProto().(*proto.Get)
				case *Put:
					racts[i].Mutation = a.toProto().(*proto.MutationProto)
				}
			}

			log.Debug("Sending Actions [n=%d]", len(racts))

			region_actions[i] = &proto.RegionAction{
				Region: &proto.RegionSpecifier{
					Type:  proto.RegionSpecifier_REGION_NAME.Enum(),
					Value: []byte(region),
				},
				Action: racts,
			}
		}

		log.Debug("Sending RegionActions [n=%d]", len(region_actions))

		req := &proto.MultiRequest{
			RegionAction: region_actions,
		}

		cl := newCall(req)
		conn := c.getRegionConnection(server)
		conn.call(cl)

		calls = append(calls, cl)
	}

	return calls
}

func (c *Client) locateRegion(table, row []byte, useCache bool) *regionInfo {
	metaRegion := &regionInfo{
		startKey: []byte{},
		endKey:   []byte{},
		name:     string(META_REGION_NAME),
		server:   c.getServerName(c.rootServer),
	}

	if bytes.Equal(table, META_TABLE_NAME) {
		return metaRegion
	}

	c.prefetchRegionCache(table)

	if r := c.getCachedLocation(table, row); r != nil {
		return r
	}

	conn := c.getRegionConnection(metaRegion.server)

	regionRow := c.createRegionName(table, row, "", true)

	call := newCall(&proto.GetRequest{
		Region: &proto.RegionSpecifier{
			Type:  proto.RegionSpecifier_REGION_NAME.Enum(),
			Value: META_REGION_NAME,
		},
		Get: &proto.Get{
			Row: regionRow,
			Column: []*proto.Column{&proto.Column{
				Family: []byte("info"),
			}},
			ClosestRowBefore: pb.Bool(true),
		},
	})

	conn.call(call)

	response := <-call.responseCh

	switch r := (*response).(type) {
	case *proto.GetResponse:
		rr := newResultRow(r.GetResult())
		if region := c.parseRegion(rr); region != nil {
			log.Debug("Found region [region: %s]", region.name)

			c.cacheLocation(table, region)

			return region
		}
	}

	log.Debug("Couldn't find the region: [table=%s] [row=%s] [region_row=%s]", table, row, regionRow)

	return nil
}

func (c *Client) createRegionName(table, startKey []byte, id string, newFormat bool) []byte {
	if len(startKey) == 0 {
		startKey = make([]byte, 1)
	}

	b := bytes.Join([][]byte{table, startKey, []byte(id)}, []byte(","))

	if newFormat {
		m := md5.Sum(b)
		mhex := []byte(hex.EncodeToString(m[:]))
		b = append(bytes.Join([][]byte{b, mhex}, []byte(".")), []byte(".")...)
	}
	return b
}

func (c *Client) prefetchRegionCache(table []byte) {
	if bytes.Equal(table, META_TABLE_NAME) {
		return
	}

	if v, ok := c.prefetched[string(table)]; ok && v {
		return
	}

	startRow := table
	stopRow := incrementByteString(table, len(table)-1)

	scan := newScan(META_TABLE_NAME, c)

	scan.StartRow = startRow
	scan.StopRow = stopRow

	scan.Map(func(r *ResultRow) {
		region := c.parseRegion(r)
		if region != nil {
			c.cacheLocation(table, region)
		}
	})

	c.prefetched[string(table)] = true
}

func (c *Client) parseRegion(rr *ResultRow) *regionInfo {
	if regionInfoCol, ok := rr.Columns["info:regioninfo"]; ok {
		offset := strings.Index(regionInfoCol.Value.String(), "PBUF") + 4
		regionInfoBytes := regionInfoCol.Value[offset:]

		var info proto.RegionInfo
		err := pb.Unmarshal(regionInfoBytes, &info)

		if err != nil {
			log.Error("Unable to parse region location: %#v", err)
		}

		log.Debug("Parsed region info [name=%s]", rr.Row.String())

		return &regionInfo{
			server:   rr.Columns["info:server"].Value.String(),
			startKey: info.GetStartKey(),
			endKey:   info.GetEndKey(),
			name:     rr.Row.String(),
			ts:       rr.Columns["info:server"].Timestamp.String(),
		}
	}

	log.Error("Unable to parse region location (no regioninfo column): %#v", rr)

	return nil
}

func (c *Client) cacheLocation(table []byte, region *regionInfo) {
	tablestr := string(table)
	if _, ok := c.cachedRegionLocations[tablestr]; !ok {
		c.cachedRegionLocations[tablestr] = make(map[string]*regionInfo)
	}

	c.cachedRegionLocations[tablestr][region.name] = region
}

func (c *Client) getCachedLocation(table, row []byte) *regionInfo {
	tablestr := string(table)

	if regions, ok := c.cachedRegionLocations[tablestr]; ok {
		for _, region := range regions {
			if (len(region.endKey) == 0 ||
				bytes.Compare(row, region.endKey) < 0) &&
				(len(region.startKey) == 0 ||
					bytes.Compare(row, region.startKey) >= 0) {

				return region
			}
		}
	}

	return nil
}
