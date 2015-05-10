package hbase

import (
	"github.com/lazyshot/go-hbase/proto"

	"fmt"
)

func (c *Client) Get(table string, get *Get) (*ResultRow, error) {
	cl := c.action([]byte(table), get.key, get)

	response := <-cl.responseCh
	switch r := (*response).(type) {
	case *proto.GetResponse:
		return newResultRow(r.GetResult()), nil
	}

	return nil, fmt.Errorf("No valid response seen [response: %#v]", response)
}

func (c *Client) AsyncGets(table string, gets []*Get) chan *ResultRow {
	actions := make([]multiaction, len(gets))

	for i, v := range gets {
		actions[i] = multiaction{
			row:    v.key,
			action: v,
		}
	}

	calls := c.multiaction([]byte(table), actions)
	results := make(chan *ResultRow, 1000)
	i := 0

	for _, call := range calls {
		go func() {
			r := <-call.responseCh

			switch rs := (*r).(type) {
			case *proto.MultiResponse:
				for _, v := range rs.GetRegionActionResult() {
					for _, v2 := range v.GetResultOrException() {
						if res := v2.GetResult(); res != nil {
							results <- newResultRow(res)
						}
					}
				}
			}

			i++
			if i == len(calls) {
				close(results)
			}
		}()
	}

	return results
}

func (c *Client) Gets(table string, gets []*Get) ([]*ResultRow, error) {
	results := c.AsyncGets(table, gets)
	tbr := make([]*ResultRow, 0)

	for r := range results {
		tbr = append(tbr, r)
	}

	return tbr, nil
}

func (c *Client) Put(table string, put *Put) (bool, error) {
	cl := c.action([]byte(table), put.key, put)

	response := <-cl.responseCh
	switch r := (*response).(type) {
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

	c.multiaction([]byte(table), actions)

	return true, nil
}
