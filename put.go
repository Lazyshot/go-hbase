package hbase

import (
	pb "github.com/golang/protobuf/proto"
	"github.com/lazyshot/go-hbase/proto"

	"bytes"
)

type Put struct {
	key        []byte
	families   [][]byte
	qualifiers [][][]byte
	values     [][][]byte
}

func CreateNewPut(key []byte) *Put {
	return &Put{
		key:        key,
		families:   make([][]byte, 0),
		qualifiers: make([][][]byte, 0),
		values:     make([][][]byte, 0),
	}
}

func (this *Put) AddValue(family, column, value []byte) {
	pos := this.posOfFamily(family)

	if pos == -1 {
		this.families = append(this.families, family)
		this.qualifiers = append(this.qualifiers, make([][]byte, 0))
		this.values = append(this.values, make([][]byte, 0))

		pos = this.posOfFamily(family)
	}

	this.qualifiers[pos] = append(this.qualifiers[pos], column)
	this.values[pos] = append(this.values[pos], value)
}

func (this *Put) AddStringValue(family, column, value string) {
	this.AddValue([]byte(family), []byte(column), []byte(value))
}

func (this *Put) posOfFamily(family []byte) int {
	for p, v := range this.families {
		if bytes.Equal(family, v) {
			return p
		}
	}
	return -1
}

func (this *Put) toProto() pb.Message {
	p := &proto.MutationProto{
		Row:        this.key,
		MutateType: proto.MutationProto_PUT.Enum(),
	}

	for i, family := range this.families {
		cv := &proto.MutationProto_ColumnValue{
			Family: family,
		}

		for j, _ := range this.qualifiers[i] {
			cv.QualifierValue = append(cv.QualifierValue, &proto.MutationProto_ColumnValue_QualifierValue{
				Qualifier: this.qualifiers[i][j],
				Value:     this.values[i][j],
			})
		}

		p.ColumnValue = append(p.ColumnValue, cv)
	}

	return p
}
