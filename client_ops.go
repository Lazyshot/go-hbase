package hbase

import (
	"github.com/lazyshot/go-hbase/proto"

	"fmt"
)

func (c *Client) Get(table string, get *Get) (*ResultRow, error) {
	ch := c.action([]byte(table), get.key, get, true, 0)

	response := <-ch
	switch r := response.(type) {
	case *proto.GetResponse:
		return newResultRow(r.GetResult()), nil
	}

	return nil, fmt.Errorf("No valid response seen [response: %#v]", response)
}

func (c *Client) AsyncGets(table string, results chan *ResultRow, gets []*Get) {
	actions := make([]multiaction, len(gets))

	for i, v := range gets {
		actions[i] = multiaction{
			row:    v.key,
			action: v,
		}
	}

	ch := c.multiaction([]byte(table), actions, true, 0)

	for r := range ch {
		switch rs := r.(type) {
		case *proto.MultiResponse:
			log.Debug("Received region action result [n=%d]", len(rs.GetRegionActionResult()))
			for _, v := range rs.GetRegionActionResult() {
				log.Debug("Received result or exceptions [n=%d]", len(v.GetResultOrException()))
				for _, v2 := range v.GetResultOrException() {
					if res := v2.GetResult(); res != nil {
						results <- newResultRow(res)
					}
				}
			}
		}
	}

	log.Debug("Finished receiving region action results")

	close(results)
}

func (c *Client) Gets(table string, gets []*Get) ([]*ResultRow, error) {
	results := make(chan *ResultRow, 100)
	go c.AsyncGets(table, results, gets)
	tbr := make([]*ResultRow, 0)

	for r := range results {
		tbr = append(tbr, r)
	}

	return tbr, nil
}

func (c *Client) Put(table string, put *Put) (bool, error) {
	ch := c.action([]byte(table), put.key, put, true, 0)

	response := <-ch
	switch r := response.(type) {
	case *proto.MutateResponse:
		return r.GetProcessed(), nil
	}

	return false, fmt.Errorf("No valid response seen [response: %#v]", response)
}

func (c *Client) Puts(table string, puts []*Put) (bool, error) {
	actions := make([]multiaction, len(puts))

	for i, v := range puts {
		actions[i] = multiaction{
			row:    v.key,
			action: v,
		}
	}

	ch := c.multiaction([]byte(table), actions, true, 0)

	for _ = range ch {
	}

	return true, nil
}

func (c *Client) Delete(table string, del *Delete) (bool, error) {
	ch := c.action([]byte(table), del.key, del, true, 0)

	response := <-ch
	switch r := response.(type) {
	case *proto.MutateResponse:
		return r.GetProcessed(), nil
	}

	return false, fmt.Errorf("No valid response seen [response: %#v]", response)
}

func (c *Client) Deletes(table string, dels []*Delete) (bool, error) {
	actions := make([]multiaction, len(dels))

	for i, v := range dels {
		actions[i] = multiaction{
			row:    v.key,
			action: v,
		}
	}

	ch := c.multiaction([]byte(table), actions, true, 0)

	for _ = range ch {
	}

	return true, nil
}

func (c *Client) Scan(table string) *Scan {
	return newScan([]byte(table), c)
}

func (c *Client) GetTables() []TableInfo {
	res := c.adminAction(&proto.GetTableDescriptorsRequest{})

	response := <-res
	switch r := response.(type) {
	case *proto.GetTableDescriptorsResponse:
		tables := make([]TableInfo, len(r.GetTableSchema()))
		for i, table := range r.GetTableSchema() {
			tables[i] = TableInfo{
				TableName: string(table.GetTableName().GetQualifier()),
				Families:  make([]string, len(table.GetColumnFamilies())),
			}

			for j, cf := range table.GetColumnFamilies() {
				tables[i].Families[j] = string(cf.GetName())
			}
		}
		return tables
	}

	return nil
}
