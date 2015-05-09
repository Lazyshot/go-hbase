package hbase

import (
	pb "github.com/golang/protobuf/proto"
	"github.com/lazyshot/go-hbase/proto"
)

type Put struct {
	key        []byte
	family     []byte
	qualifiers [][]byte
	values     [][]byte

	response chan interface{}
}

func CreateNewPut(key, family []byte) *Put {
	return &Put{
		key:        key,
		family:     family,
		qualifiers: make([][]byte, 0),
		values:     make([][]byte, 0),
		response:   make(chan interface{}, 1),
	}
}

func (this *Put) AddValue(column, value []byte) {
	this.qualifiers = append(this.qualifiers, column)
	this.values = append(this.values, value)
}

func (this *Put) AddStringValue(column, value string) {
	this.AddValue([]byte(column), []byte(value))
}

func (this *Put) toProto() pb.Message {
	p := &proto.MutationProto{
		Row:        this.key,
		MutateType: proto.MutationProto_PUT.Enum(),
	}

	cv := &proto.MutationProto_ColumnValue{
		Family: this.family,
	}

	for i, _ := range this.qualifiers {
		cv.QualifierValue = append(cv.QualifierValue, &proto.MutationProto_ColumnValue_QualifierValue{
			Qualifier: this.qualifiers[i],
			Value:     this.values[i],
		})
	}

	p.ColumnValue = append(p.ColumnValue, cv)

	log.Debug("Put: %#v", p)

	return p
}
