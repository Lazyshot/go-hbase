package hbase

import (
	"encoding/binary"
	pb "github.com/golang/protobuf/proto"
)

type outputBuffer struct {
	b []byte
}

func newOutputBuffer() *outputBuffer {
	b := []byte{}

	return &outputBuffer{
		b: b,
	}
}

func (b *outputBuffer) Bytes() []byte {
	return b.b
}

func (b *outputBuffer) Write(d []byte) (int, error) {
	b.b = append(b.b, d...)
	return len(d), nil
}

func (b *outputBuffer) WriteByte(d byte) error {
	return binary.Write(b, byte_order, d)
}

func (b *outputBuffer) WriteString(d string) error {
	return binary.Write(b, byte_order, d)
}

func (b *outputBuffer) WriteInt32(d int32) error {
	return binary.Write(b, byte_order, d)
}

func (b *outputBuffer) WriteInt64(d int64) error {
	return binary.Write(b, byte_order, d)
}

func (b *outputBuffer) WriteFloat32(d float32) error {
	return binary.Write(b, byte_order, d)
}

func (b *outputBuffer) WriteFloat64(d float64) error {
	return binary.Write(b, byte_order, d)
}

func (b *outputBuffer) WriteVarint32(n int32) error {
	for true {
		if (n & 0x7F) == 0 {
			b.WriteByte(byte(n))
			return nil
		} else {
			b.WriteByte(byte((n & 0x7F) | 0x80))
			n >>= 7
		}
	}

	return nil
}

func (b *outputBuffer) WritePBMessage(d pb.Message) error {
	buf, err := pb.Marshal(d)
	if err != nil {
		return err
	}

	_, err = b.Write(buf)
	return err
}

func (b *outputBuffer) writeDelimitedBuffers(bufs ...*outputBuffer) error {
	totalLength := 0
	lens := make([][]byte, len(bufs))
	for i, v := range bufs {
		n := len(v.Bytes())
		lenb := pb.EncodeVarint(uint64(n))

		totalLength += len(lenb) + n
		lens[i] = lenb
	}

	b.WriteInt32(int32(totalLength))

	for i, v := range bufs {
		b.Write(lens[i])
		b.Write(v.Bytes())
	}

	return nil
}

func (b *outputBuffer) PrependSize() error {
	size := int32(len(b.b))
	newBuf := newOutputBuffer()

	err := newBuf.WriteInt32(size)
	if err != nil {
		return err
	}

	_, err = newBuf.Write(b.b)
	if err != nil {
		return err
	}

	*b = *newBuf
	return nil
}
