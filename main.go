//  Copyright (c) 2014 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.
package main

import (
	_ "expvar"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"path/filepath"
	"time"

	"github.com/blevesearch/bleve"
	bleveHttp "github.com/blevesearch/bleve/http"
	"github.com/couchbase/gomemcached/client"
	"github.com/couchbaselabs/go-couchbase"
)

var batchSize = flag.Int("batchSize", 100, "batch size for indexing")
var bindAddr = flag.String("addr", ":8094", "http listen address")
var jsonDir = flag.String("jsonDir", "data/", "json directory")
var indexPath = flag.String("index", "beer-search.bleve", "index path")
var staticEtag = flag.String("staticEtag", "", "A static etag value.")
var staticPath = flag.String("static", "static/", "Path to the static content")
var couchbaseUrl = flag.String("couchbase", "", "Path to couchbase server")

// to index bucket beer-sample from a couchbase server
// ./beer-search -couchbase="http://beer-sample@host:port/"

func main() {

	flag.Parse()

	// open the index
	beerIndex, err := bleve.Open(*indexPath)
	if err == bleve.ErrorIndexPathDoesNotExist {
		log.Printf("Creating new index...")
		// create a mapping
		indexMapping, err := buildIndexMapping()
		if err != nil {
			log.Fatal(err)
		}
		beerIndex, err = bleve.New(*indexPath, indexMapping)
		if err != nil {
			log.Fatal(err)
		}

		// index data in the background
		go func() {
			if len(*couchbaseUrl) > 0 {
				err = indexBeerCb(beerIndex, *couchbaseUrl)
			} else {
				err = indexBeer(beerIndex)
			}
			if err != nil {
				log.Fatal(err)
			}
		}()
	} else if err != nil {
		log.Fatal(err)
	} else {
		log.Printf("Opening existing index...")
	}

	// create a router to serve static files
	router := staticFileRouter()

	// add the API
	bleveHttp.RegisterIndexName("beer", beerIndex)
	searchHandler := bleveHttp.NewSearchHandler("beer")
	router.Handle("/api/search", searchHandler).Methods("POST")
	listFieldsHandler := bleveHttp.NewListFieldsHandler("beer")
	router.Handle("/api/fields", listFieldsHandler).Methods("GET")
	debugHandler := bleveHttp.NewDebugDocumentHandler("beer")
	router.Handle("/api/debug/{docID}", debugHandler).Methods("GET")

	// start the HTTP server
	http.Handle("/", router)
	log.Printf("Listening on %v", *bindAddr)
	log.Fatal(http.ListenAndServe(*bindAddr, nil))

}

func indexBeer(i bleve.Index) error {

	// open the directory
	dirEntries, err := ioutil.ReadDir(*jsonDir)
	if err != nil {
		return err
	}

	// walk the directory entries for indexing
	log.Printf("Indexing...")
	count := 0
	startTime := time.Now()
	batch := bleve.NewBatch()
	batchCount := 0
	for _, dirEntry := range dirEntries {
		filename := dirEntry.Name()
		// read the bytes
		jsonBytes, err := ioutil.ReadFile(*jsonDir + "/" + filename)
		if err != nil {
			return err
		}
		// // shred them into a document
		ext := filepath.Ext(filename)
		docId := filename[:(len(filename) - len(ext))]
		batch.Index(docId, jsonBytes)
		batchCount++

		if batchCount >= *batchSize {
			err = i.Batch(batch)
			if err != nil {
				return err
			}
			batch = bleve.NewBatch()
			batchCount = 0
		}
		count++
		if count%1000 == 0 {
			indexDuration := time.Since(startTime)
			indexDurationSeconds := float64(indexDuration) / float64(time.Second)
			timePerDoc := float64(indexDuration) / float64(count)
			log.Printf("Indexed %d documents, in %.2fs (average %.2fms/doc)", count, indexDurationSeconds, timePerDoc/float64(time.Millisecond))
		}
	}
	indexDuration := time.Since(startTime)
	indexDurationSeconds := float64(indexDuration) / float64(time.Second)
	timePerDoc := float64(indexDuration) / float64(count)
	log.Printf("Indexed %d documents, in %.2fs (average %.2fms/doc)", count, indexDurationSeconds, timePerDoc/float64(time.Millisecond))
	return nil
}

func indexBeerCb(i bleve.Index, couchbaseUrl string) error {

	// connect to the couchbase url/bucket
	u, err := url.Parse(couchbaseUrl)
	if err != nil {
		return err
	}

	if u.User == nil {
		return fmt.Errorf("Bucket name not specified")
	}

	bucket := u.User.Username()
	c, err := couchbase.Connect(u.String())
	if err != nil {
		return err
	}

	p, err := c.GetPool("default")
	if err != nil {
		return err
	}

	b, err := p.GetBucket(bucket)
	if err != nil {
		return err
	}

	feed, err := b.StartUprFeed("bleve", 0)
	if err != nil {
		return err
	}
	defer feed.Close()

	// ger the vbucket map for this bucket
	vbm := b.VBServerMap()

	// request stream for all vbuckets
	for i := 0; i < len(vbm.VBucketMap); i++ {
		if err := feed.UprRequestStream(uint16(i), 0, 0, 0, 0xFFFFFFFFFFFFFFFF, 0, 0); err != nil {
			fmt.Printf("%s", err.Error())
		}
	}

	log.Printf("Indexing...")
	count := 0
	startTime := time.Now()
	batch := bleve.NewBatch()
	batchCount := 0

	// observe and index mutations from the channel.
	var e *memcached.UprEvent
loop:
	for {
		select {
		case e = <-feed.C:
		case <-time.After(time.Second):
			break loop
		}

		if e.Opcode == memcached.UprMutation {
			//log.Printf(" got mutation %s", e.Value)
			batch.Index(string(e.Key), e.Value)
			batchCount++

			if batchCount >= *batchSize {
				err = i.Batch(batch)
				if err != nil {
					return err
				}
				batch = bleve.NewBatch()
				batchCount = 0
			}
			count++
			if count%1000 == 0 {
				indexDuration := time.Since(startTime)
				indexDurationSeconds := float64(indexDuration) / float64(time.Second)
				timePerDoc := float64(indexDuration) / float64(count)
				log.Printf("Indexed %d documents, in %.2fs (average %.2fms/doc)", count, indexDurationSeconds, timePerDoc/float64(time.Millisecond))
			}

		}

	}

	indexDuration := time.Since(startTime)
	indexDurationSeconds := float64(indexDuration) / float64(time.Second)
	timePerDoc := float64(indexDuration) / float64(count)
	log.Printf("Indexed %d documents, in %.2fs (average %.2fms/doc)", count, indexDurationSeconds, timePerDoc/float64(time.Millisecond))
	return nil
}
