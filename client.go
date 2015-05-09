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

	rootServer *proto.ServerName
}

var log = logging.MustGetLogger("hbase-client")

func NewClient(zkHosts []string, zkRoot string) *Client {
	cl := &Client{
		zkHosts:          zkHosts,
		zkRoot:           zkRoot,
		zkRootRegionPath: "/meta-region-server",

		servers:               make(map[string]*connection),
		cachedRegionLocations: make(map[string]map[string]*regionInfo),
	}

	cl.initZk()

	return cl
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
	c.getRegionConnection(&regionInfo{
		startKey: []byte{},
		endKey:   []byte{},
		name:     string(META_REGION_NAME),
		server:   c.getServerName(c.rootServer),
	})
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

func (c *Client) getRegionConnection(server *regionInfo) *connection {
	if s, ok := c.servers[server.name]; ok {
		return s
	}

	conn, err := newConnection(server.server)
	if err != nil {
		panic(err)
	}

	c.servers[server.name] = conn

	return conn
}

func (c *Client) action(table, row []byte, action Action) *call {
	log.Debug("Attempting action [table: %s] [row: %s] [action: %#v]", table, row, action)

	region := c.locateRegion(table, row, true)
	conn := c.getRegionConnection(region)

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

func (c *Client) Get(table string, get *Get) (*ResultRow, error) {
	cl := c.action([]byte(table), get.key, get)

	response := <-cl.responseCh
	switch r := (*response).(type) {
	case *proto.GetResponse:
		return newResultRow(r.GetResult()), nil
	}

	return nil, fmt.Errorf("No valid response seen [response: %#v]", response)
}

func (c *Client) Put(table string, put *Put) (bool, error) {
	cl := c.action([]byte(table), put.key, put)

	response := <-cl.responseCh
	switch r := (*response).(type) {
	case *proto.MutateResponse:
		return r.GetProcessed(), nil
	}

	return false, fmt.Errorf("No valid response seen [response: %#v]", response)
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

	if r := c.getCachedLocation(table, row); r != nil {
		return r
	}

	conn := c.getRegionConnection(metaRegion)

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
		if regionInfoCol, ok := rr.Columns["info:regioninfo"]; ok {
			offset := strings.Index(regionInfoCol.Value.String(), "PBUF") + 4
			regionInfoBytes := regionInfoCol.Value[offset:]

			var info proto.RegionInfo
			pb.Unmarshal(regionInfoBytes, &info)

			region := &regionInfo{
				server:   rr.Columns["info:server"].Value.String(),
				startKey: info.GetStartKey(),
				endKey:   info.GetEndKey(),
				name:     rr.Row.String(),
				ts:       rr.Columns["info:server"].Timestamp.String(),
			}

			log.Debug("Found region [region: %#v]", region)

			c.cacheLocation(table, region)

			return region
		}
	}

	return nil
}

func (c *Client) createRegionName(table, startKey []byte, id string, newFormat bool) []byte {
	b := bytes.Join([][]byte{table, startKey, []byte(id)}, []byte(","))

	if newFormat {
		m := md5.Sum(b)
		mhex := []byte(hex.EncodeToString(m[:]))
		b = append(bytes.Join([][]byte{b, mhex}, []byte(".")), []byte(".")...)
	}
	return b
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
