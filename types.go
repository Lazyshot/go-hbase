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

type Action interface {
	toProto() pb.Message
}
