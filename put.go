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
	timestamp  [][]int64
}

func CreateNewPut(key []byte) *Put {
	return &Put{
		key:        key,
		families:   make([][]byte, 0),
		qualifiers: make([][][]byte, 0),
		values:     make([][][]byte, 0),
		timestamp:  make([][]int64, 0),
	}
}

func (this *Put) AddValue(family, column, value []byte) {
	this.AddValueTS(family, column, value, 0)
}

// AddValueTS use user specified timestamp
func (this *Put) AddValueTS(family, column, value []byte, ts int64) {
	pos := this.posOfFamily(family)

	if pos == -1 {
		this.families = append(this.families, family)
		this.qualifiers = append(this.qualifiers, make([][]byte, 0))
		this.values = append(this.values, make([][]byte, 0))
		this.timestamp = append(this.timestamp, make([]int64, 0))

		pos = this.posOfFamily(family)
	}

	this.qualifiers[pos] = append(this.qualifiers[pos], column)
	this.values[pos] = append(this.values[pos], value)
	this.timestamp[pos] = append(this.timestamp[pos], ts)
}

func (this *Put) AddStringValue(family, column, value string) {
	this.AddValueTS([]byte(family), []byte(column), []byte(value), 0)
}

// AddStringValueTS use user specified timestamp
func (this *Put) AddStringValueTS(family, column, value string, ts int64) {
	this.AddValueTS([]byte(family), []byte(column), []byte(value), ts)
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
			qv := &proto.MutationProto_ColumnValue_QualifierValue{
				Qualifier: this.qualifiers[i][j],
				Value:     this.values[i][j],
			}
			if this.timestamp[i][j] > 0 {
				qv.Timestamp = pb.Uint64(uint64(this.timestamp[i][j]))
			}
			cv.QualifierValue = append(cv.QualifierValue, qv)
		}

		p.ColumnValue = append(p.ColumnValue, cv)
	}

	return p
}
