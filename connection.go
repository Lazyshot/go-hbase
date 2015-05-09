package hbase

import (
	pb "github.com/golang/protobuf/proto"
	"github.com/lazyshot/go-hbase/proto"
	"github.com/op/go-logging"

	"fmt"
	"net"
)

type connection struct {
	connstr string

	id   int
	name string

	socket net.Conn
	in     *inputStream

	calls  map[int]*call
	callId *atomicCounter
	log    *logging.Logger
}

var connectionIds *atomicCounter = newAtomicCounter()

func newConnection(connstr string) (*connection, error) {
	id := connectionIds.IncrAndGet()

	log.Debug("Connecting to server[id=%d]: %s", id, connstr)

	socket, err := net.Dial("tcp", connstr)

	if err != nil {
		return nil, err
	}

	c := &connection{
		connstr: connstr,

		id:   id,
		name: fmt.Sprintf("connection(%s) id: %d", connstr, id),

		socket: socket,
		in:     newInputStream(socket),

		calls:  make(map[int]*call),
		callId: newAtomicCounter(),

		log: logging.MustGetLogger("connection"),
	}

	err = c.init()
	if err != nil {
		return nil, err
	}

	c.log.Debug("Initted connection")

	return c, nil
}

func (c *connection) init() error {

	err := c.writeHead()
	if err != nil {
		return err
	}

	err = c.writeConnectionHeader()
	if err != nil {
		return err
	}

	go c.processMessages()

	return nil
}

func (c *connection) writeHead() error {
	buf := newOutputBuffer()
	buf.Write(HEADER)
	buf.WriteByte(0)
	buf.WriteByte(80)

	n, err := c.socket.Write(buf.Bytes())
	c.log.Debug("Outgoing Bytes [Head] [n=%d]: %#v", n, buf.Bytes())
	return err
}

func (c *connection) writeConnectionHeader() error {
	buf := newOutputBuffer()
	err := buf.WritePBMessage(&proto.ConnectionHeader{
		UserInfo: &proto.UserInformation{
			EffectiveUser: pb.String("bryan"),
		},
		ServiceName: pb.String("ClientService"),
	})
	if err != nil {
		return err
	}

	err = buf.PrependSize()
	if err != nil {
		return err
	}

	n, err := c.socket.Write(buf.Bytes())
	if err != nil {
		return err
	}

	c.log.Debug("Outgoing Bytes [ConnHeader] [n=%d]: %#v", n, buf.Bytes())

	return nil
}

func (c *connection) call(request *call) {
	id := c.callId.IncrAndGet()
	rh := &proto.RequestHeader{
		CallId:       pb.Uint32(uint32(id)),
		MethodName:   pb.String(request.methodName),
		RequestParam: pb.Bool(true),
	}

	bfrh := newOutputBuffer()
	bfrh.WritePBMessage(rh)

	bfr := newOutputBuffer()
	bfr.WritePBMessage(request.request)

	buf := newOutputBuffer()
	buf.writeDelimitedBuffers(bfrh, bfr)

	c.calls[id] = request
	n, err := c.socket.Write(buf.Bytes())
	if err != nil {
		panic(err)
	}

	if n != len(buf.Bytes()) {
		panic("Sent bytes not match number bytes")
	}
}

func (c *connection) processMessages() {
	for {
		msgs := c.in.processData()
		if msgs == nil || len(msgs) == 0 || len(msgs[0]) == 0 {
			continue
		}

		c.log.Debug("Messages received [n=%d] [msg_1=%d]", len(msgs), len(msgs[0]))

		var rh proto.ResponseHeader
		err := pb.Unmarshal(msgs[0], &rh)
		if err != nil {
			panic(err)
		}

		callId := rh.GetCallId()
		log.Debug("Resp: %s", rh.String())
		call, ok := c.calls[int(callId)]
		if !ok {
			panic(fmt.Errorf("Invalid call id: %d", callId))
		}

		delete(c.calls, int(callId))

		exception := rh.GetException()
		if exception != nil {
			call.complete(fmt.Errorf("Exception returned: %s\n%s", exception.GetExceptionClassName(), exception.GetStackTrace()), nil)
		} else if len(msgs) == 2 {
			call.complete(nil, msgs[1])
		}
	}
}
