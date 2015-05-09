package hbase

import (
	"bytes"
	"github.com/lazyshot/go-hbase/proto"
	"time"
)

type ResultRow struct {
	Row           EncodedValue
	Columns       map[string]*ResultRowColumn
	SortedColumns []*ResultRowColumn
}

type ResultRowColumn struct {
	ColumnName string

	Family    EncodedValue
	Qualifier EncodedValue

	Timestamp time.Time
	Value     EncodedValue

	Values map[time.Time]EncodedValue
}

func newResultRow(result *proto.Result) *ResultRow {
	res := &ResultRow{}
	res.Columns = make(map[string]*ResultRowColumn)
	res.SortedColumns = make([]*ResultRowColumn, 0)

	for _, cell := range result.GetCell() {
		res.Row = cell.GetRow()

		col := ResultRowColumn{
			Family:    cell.GetFamily(),
			Qualifier: cell.GetQualifier(),
			Value:     cell.GetValue(),
			Timestamp: time.Unix(int64(cell.GetTimestamp()), 0),
		}

		col.ColumnName = col.Family.String() + ":" + col.Qualifier.String()

		if v, exists := res.Columns[col.ColumnName]; exists {

			if col.Timestamp.After(v.Timestamp) {
				v.Value = col.Value
				v.Timestamp = col.Timestamp
			}

			v.Values[col.Timestamp] = col.Value

		} else {
			col.Values = map[time.Time]EncodedValue{col.Timestamp: col.Value}

			res.Columns[col.ColumnName] = &col
			res.SortedColumns = append(res.SortedColumns, &col)
		}

	}

	return res
}

type EncodedValue []byte

func (e EncodedValue) String() string {
	return bytes.NewBuffer(e).String()
}
