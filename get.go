package hbase

import (
	pb "github.com/golang/protobuf/proto"
	"github.com/lazyshot/go-hbase/proto"

	"bytes"
	"fmt"
	"strings"
)

type Get struct {
	key        []byte
	families   [][]byte
	qualifiers [][][]byte
	versions   int32
}

func CreateNewGet(key []byte) *Get {
	return &Get{
		key:        key,
		families:   make([][]byte, 0),
		qualifiers: make([][][]byte, 0),
		versions:   1,
	}
}

func (this *Get) AddString(famqual string) error {
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

func (this *Get) AddStringColumn(family, qual string) {
	this.AddColumn([]byte(family), []byte(qual))
}

func (this *Get) AddStringFamily(family string) {
	this.AddFamily([]byte(family))
}

func (this *Get) AddColumn(family, qual []byte) {
	this.AddFamily(family)

	pos := this.posOfFamily(family)

	this.qualifiers[pos] = append(this.qualifiers[pos], qual)
}

func (this *Get) AddFamily(family []byte) {
	pos := this.posOfFamily(family)

	if pos == -1 {
		this.families = append(this.families, family)
		this.qualifiers = append(this.qualifiers, make([][]byte, 0))
	}
}

func (this *Get) posOfFamily(family []byte) int {
	for p, v := range this.families {
		if bytes.Equal(family, v) {
			return p
		}
	}
	return -1
}

func (this *Get) toProto() pb.Message {
	g := &proto.Get{
		Row: this.key,
	}

	for i, v := range this.families {
		g.Column = append(g.Column, &proto.Column{
			Family:    v,
			Qualifier: this.qualifiers[i],
		})
	}

	g.MaxVersions = pb.Uint32(uint32(this.versions))

	return g
}
