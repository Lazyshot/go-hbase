package hbase

import (
	pb "github.com/golang/protobuf/proto"
	"github.com/lazyshot/go-hbase/proto"
)

type call struct {
	methodName     string
	request        pb.Message
	responseBuffer pb.Message
	responseCh     chan *pb.Message
}

func newCall(request pb.Message) *call {
	var responseBuffer pb.Message
	var methodName string

	switch request.(type) {
	case *proto.GetRequest:
		responseBuffer = &proto.GetResponse{}
		methodName = "Get"
	case *proto.MutateRequest:
		responseBuffer = &proto.MutateResponse{}
		methodName = "Mutate"
	case *proto.MultiRequest:
		responseBuffer = &proto.MultiResponse{}
		methodName = "Multi"
	case *proto.ScanRequest:
		responseBuffer = &proto.ScanResponse{}
		methodName = "Scan"
	}

	return &call{
		methodName:     methodName,
		request:        request,
		responseBuffer: responseBuffer,
		responseCh:     make(chan *pb.Message),
	}
}

func (c *call) complete(err error, response []byte) {
	log.Debug("Response received [err=%#v] [response_n=%d]", err, len(response))

	if err != nil {
		panic(err)
	}

	err2 := pb.Unmarshal(response, c.responseBuffer)
	if err2 != nil {
		panic(err2)
	}

	c.responseCh <- &c.responseBuffer
}
