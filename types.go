package hbase

import (
	pb "github.com/golang/protobuf/proto"
)

type regionInfo struct {
	server   string
	startKey []byte
	endKey   []byte
	name     string
	ts       string
}

type action interface {
	toProto() pb.Message
}

type exception struct {
	msg string
}

func (m *exception) Reset()         { *m = exception{} }
func (m *exception) String() string { return m.msg }
func (*exception) ProtoMessage()    {}
