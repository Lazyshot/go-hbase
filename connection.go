package hbase

import (
	pb "github.com/golang/protobuf/proto"
	"github.com/lazyshot/go-hbase/proto"

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
}

var connectionIds *atomicCounter = newAtomicCounter()

func newConnection(connstr string) (*connection, error) {
	id := connectionIds.IncrAndGet()

	log.Debug("Connecting to server[id=%d] [%s]", id, connstr)

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
	}

	err = c.init()
	if err != nil {
		return nil, err
	}

	log.Debug("Initiated connection [id=%d] [%s]", id, connstr)

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
	log.Debug("Outgoing Head [n=%d]", n)
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

	log.Debug("Outgoing ConnHeader [n=%d]", n)

	return nil
}

func (c *connection) call(request *call) error {
	id := c.callId.IncrAndGet()
	rh := &proto.RequestHeader{
		CallId:       pb.Uint32(uint32(id)),
		MethodName:   pb.String(request.methodName),
		RequestParam: pb.Bool(true),
	}

	request.setid(uint32(id))

	bfrh := newOutputBuffer()
	err := bfrh.WritePBMessage(rh)
	if err != nil {
		panic(err)
	}

	bfr := newOutputBuffer()
	err = bfr.WritePBMessage(request.request)
	if err != nil {
		panic(err)
	}

	buf := newOutputBuffer()
	buf.writeDelimitedBuffers(bfrh, bfr)

	c.calls[id] = request
	n, err := c.socket.Write(buf.Bytes())

	if err != nil {
		return err
	}

	log.Debug("Sent bytes to server [callId=%d] [n=%d] [connection=%s]", id, n, c.name)

	if n != len(buf.Bytes()) {
		return fmt.Errorf("Sent bytes not match number bytes [n=%d] [actual_n=%d]", n, len(buf.Bytes()))
	}

	return nil
}

func (c *connection) processMessages() {
	for {
		msgs := c.in.processData()
		if msgs == nil || len(msgs) == 0 || len(msgs[0]) == 0 {
			continue
		}

		var rh proto.ResponseHeader
		err := pb.Unmarshal(msgs[0], &rh)
		if err != nil {
			panic(err)
		}

		log.Debug("Responseheader received [id=%d] [conn=%s]", rh.GetCallId(), c.name)

		callId := rh.GetCallId()
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
