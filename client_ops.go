package hbase

import (
	"github.com/lazyshot/go-hbase/proto"

	"fmt"
	"sync"
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
	wg := new(sync.WaitGroup)
	wg.Add(len(calls))

	go func() {
		wg.Wait()
		close(results)
	}()

	for _, call := range calls {
		go func() {
			r := <-call.responseCh

			switch rs := (*r).(type) {
			case *proto.MultiResponse:
				log.Debug("Received region action result [n=%d]", len(rs.GetRegionActionResult()))
				for _, v := range rs.GetRegionActionResult() {
					for _, v2 := range v.GetResultOrException() {
						if res := v2.GetResult(); res != nil {
							results <- newResultRow(res)
						}
					}
				}
			}

			wg.Done()
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

	calls := c.multiaction([]byte(table), actions)

	wg := new(sync.WaitGroup)
	wg.Add(len(calls))

	for _, call := range calls {
		go func() {
			<-call.responseCh

			// Maybe handle these responses

			wg.Done()
		}()
	}

	wg.Wait()

	return true, nil
}

func (c *Client) Scan(table string) *Scan {
	return newScan([]byte(table), c)
}
