package hbase

import (
	pb "github.com/golang/protobuf/proto"
	"github.com/lazyshot/go-hbase/proto"

	"bytes"
	"fmt"
	"math"
	"strings"
)

type Delete struct {
	key        []byte
	families   [][]byte
	qualifiers [][][]byte
}

func CreateNewDelete(key []byte) *Delete {
	return &Delete{
		key:        key,
		families:   make([][]byte, 0),
		qualifiers: make([][][]byte, 0),
	}
}

func (this *Delete) AddString(famqual string) error {
	parts := strings.Split(famqual, ":")

	if len(parts) > 2 {
		return fmt.Errorf("Too many colons were found in the family:qualifier string. '%s'", famqual)
	} else if len(parts) == 2 {
		this.AddStringColumn(parts[0], parts[1])
	} else {
		this.AddStringFamily(famqual)
	}

	return nil
}

func (this *Delete) AddStringColumn(family, qual string) {
	this.AddColumn([]byte(family), []byte(qual))
}

func (this *Delete) AddStringFamily(family string) {
	this.AddFamily([]byte(family))
}

func (this *Delete) AddColumn(family, qual []byte) {
	this.AddFamily(family)

	pos := this.posOfFamily(family)

	this.qualifiers[pos] = append(this.qualifiers[pos], qual)
}

func (this *Delete) AddFamily(family []byte) {
	pos := this.posOfFamily(family)

	if pos == -1 {
		this.families = append(this.families, family)
		this.qualifiers = append(this.qualifiers, make([][]byte, 0))
	}
}

func (this *Delete) posOfFamily(family []byte) int {
	for p, v := range this.families {
		if bytes.Equal(family, v) {
			return p
		}
	}
	return -1
}

func (this *Delete) toProto() pb.Message {
	d := &proto.MutationProto{
		Row:        this.key,
		MutateType: proto.MutationProto_DELETE.Enum(),
	}

	for i, v := range this.families {
		cv := &proto.MutationProto_ColumnValue{
			Family:         v,
			QualifierValue: make([]*proto.MutationProto_ColumnValue_QualifierValue, 0),
		}

		if len(this.qualifiers[i]) == 0 {
			cv.QualifierValue = append(cv.QualifierValue, &proto.MutationProto_ColumnValue_QualifierValue{
				Qualifier:  nil,
				Timestamp:  pb.Uint64(uint64(math.MaxInt64)),
				DeleteType: proto.MutationProto_DELETE_FAMILY.Enum(),
			})
		}

		for _, v := range this.qualifiers[i] {
			cv.QualifierValue = append(cv.QualifierValue, &proto.MutationProto_ColumnValue_QualifierValue{
				Qualifier:  v,
				Timestamp:  pb.Uint64(uint64(math.MaxInt64)),
				DeleteType: proto.MutationProto_DELETE_MULTIPLE_VERSIONS.Enum(),
			})
		}

		d.ColumnValue = append(d.ColumnValue, cv)
	}

	return d
}
