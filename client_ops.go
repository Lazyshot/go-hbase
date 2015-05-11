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

func (c *Client) AsyncGets(table string, results chan *ResultRow, gets []*Get) {
	actions := make([]multiaction, len(gets))

	for i, v := range gets {
		actions[i] = multiaction{
			row:    v.key,
			action: v,
		}
	}

	calls := c.multiaction([]byte(table), actions)
	wg := new(sync.WaitGroup)
	wg.Add(len(calls))

	go func() {
		log.Debug("Waiting to receive multiresponses")
		wg.Wait()
		log.Debug("Finished receiving multiresponses")
		close(results)
	}()

	for _, c := range calls {
		go func(c *call) {
			r := <-c.responseCh

			log.Debug("Received multiresponse: %#v", r)
			if r != nil {
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
			}

			wg.Done()
		}(c)
	}
}

func (c *Client) Gets(table string, gets []*Get) ([]*ResultRow, error) {
	results := make(chan *ResultRow, 100)
	c.AsyncGets(table, results, gets)
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

	for _, c := range calls {
		go func(c *call) {
			<-c.responseCh

			// Maybe handle these responses

			wg.Done()
		}(c)
	}

	wg.Wait()

	return true, nil
}

func (c *Client) Scan(table string) *Scan {
	return newScan([]byte(table), c)
}
