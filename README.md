go-hbase
========

UNMAINTAINED: See tsuna's implementation https://github.com/tsuna/gohbase

Implements a fully native HBase client for the Go programming language.

API Documentation: http://godoc.org/github.com/Lazyshot/go-hbase

Supported Versions
------------------

HBase >= 0.96.0 

This version of HBase has a backwards incompatible change, which takes full use of protocol buffers for client interactions.

Installation
------------

    go get github.com/lazyshot/go-hbase


Example
-------


```go
package main

import (
	"github.com/lazyshot/go-hbase"

	"fmt"
	"log"
)

func main() {
	client := hbase.NewClient([]string{"localhost"}, "/hbase")

	put := hbase.CreateNewPut([]byte("test1"))
	put.AddStringValue("info", "test_qual", "test_val")
	res, err := client.Put("test", put)

	if err != nil {
		panic(err)
	}

	if !res {
		panic("No put results")
	}
	log.Println("Completed put")

	get := hbase.CreateNewGet([]byte("test1"))
	result, err := client.Get("test", get)

	if err != nil {
		panic(err)
	}

	if !bytes.Equal(result.Row, []byte("test1")) {
		panic("No row")
	}

	if !bytes.Equal(result.Columns["info:test_qual"].Value, []byte("test_val")) {
		panic("Value doesn't match")
	}

	log.Println("Completed get")

	results, err := client.Gets("test", []*hbase.Get{get})

	if err != nil {
		panic(err)
	}

	log.Printf("%#v", results)
}
```

License
-------

> Copyright (c) 2014-2015 Bryan Peterson. All rights reserved.
> Use of this source code is governed by a BSD-style
> license that can be found in the LICENSE file.
