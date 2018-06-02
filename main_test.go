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
	"encoding/json"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/blevesearch/bleve"
)

func TestBeerSearchAll(t *testing.T) {
	defer os.RemoveAll("beer-search-test.bleve")

	mapping, err := buildIndexMapping()
	if err != nil {
		t.Fatal(err)
	}
	index, err := bleve.New("beer-search-test.bleve", mapping)
	if err != nil {
		t.Fatal(err)
	}
	defer index.Close()

	// open the directory
	dirEntries, err := ioutil.ReadDir("data/")
	if err != nil {
		t.Fatal(err)
	}

	indexBatchSize := 100
	batch := index.NewBatch()
	batchCount := 0
	for _, dirEntry := range dirEntries {
		filename := dirEntry.Name()
		// read the bytes
		jsonBytes, err := ioutil.ReadFile("data/" + filename)
		if err != nil {
			t.Fatal(err)
		}
		// parse bytes as json
		var jsonDoc interface{}
		err = json.Unmarshal(jsonBytes, &jsonDoc)
		if err != nil {
			t.Fatal(err)
		}
		ext := filepath.Ext(filename)
		docId := filename[:(len(filename) - len(ext))]
		batch.Index(docId, jsonDoc)
		batchCount++

		if batchCount >= indexBatchSize {
			err = index.Batch(batch)
			if err != nil {
				t.Fatal(err)
			}
			batch = index.NewBatch()
			batchCount = 0
		}
	}
	// flush the last batch
	if batchCount > 0 {
		err = index.Batch(batch)
		if err != nil {
			t.Fatal(err)
		}
	}

	expectedCount := uint64(7303)
	actualCount, err := index.DocCount()
	if err != nil {
		t.Error(err)
	}
	if actualCount != expectedCount {
		t.Errorf("expected %d documents, got %d", expectedCount, actualCount)
	}

	// run a term search
	termQuery := bleve.NewTermQuery("shock")
	termQuery.SetField("name")
	termSearchRequest := bleve.NewSearchRequest(termQuery)
	termSearchResult, err := index.Search(termSearchRequest)
	if err != nil {
		t.Error(err)
	}
	expectedResultCount := uint64(1)
	if termSearchResult.Total != expectedResultCount {
		t.Errorf("expected %d hits, got %d", expectedResultCount, termSearchResult.Total)
	} else {
		expectedResultId := "anheuser_busch-shock_top"
		if termSearchResult.Hits[0].ID != expectedResultId {
			t.Errorf("expected top hit ID: %s, got %s", expectedResultId, termSearchResult.Hits[0].ID)
		}
	}

	// run a match phrase search
	matchPhraseQuery := bleve.NewMatchPhraseQuery("spicy mexican food")
	matchPhraseSearchRequest := bleve.NewSearchRequest(matchPhraseQuery)
	matchPhraseSearchResult, err := index.Search(matchPhraseSearchRequest)
	if err != nil {
		t.Error(err)
	}
	expectedResultCount = uint64(1)
	if matchPhraseSearchResult.Total != expectedResultCount {
		t.Errorf("expected %d hits, got %d", expectedResultCount, matchPhraseSearchResult.Total)
	} else {
		expectedResultId := "great_divide_brewing-wild_raspberry_ale"
		if matchPhraseSearchResult.Hits[0].ID != expectedResultId {
			t.Errorf("expected top hit ID: %s, got %s", expectedResultId, matchPhraseSearchResult.Hits[0].ID)
		}
	}

	// run a syntax query
	syntaxQuery := bleve.NewQueryStringQuery("+name:light +description:water -description:barley")
	syntaxSearchRequest := bleve.NewSearchRequest(syntaxQuery)
	syntaxSearchResult, err := index.Search(syntaxSearchRequest)
	if err != nil {
		t.Error(err)
	}
	expectedResultCount = uint64(1)
	if syntaxSearchResult.Total != expectedResultCount {
		t.Errorf("expected %d hits, got %d", expectedResultCount, syntaxSearchResult.Total)
	} else {
		expectedResultId := "iron_city_brewing_co-ic_light"
		if syntaxSearchResult.Hits[0].ID != expectedResultId {
			t.Errorf("expected top hit ID: %s, got %s", expectedResultId, syntaxSearchResult.Hits[0].ID)
		}
	}

	// run a numeric range search
	queryMin := 50.0
	numericRangeQuery := bleve.NewNumericRangeQuery(&queryMin, nil)
	numericRangeQuery.SetField("abv")
	numericSearchRequest := bleve.NewSearchRequest(numericRangeQuery)
	numericSearchResult, err := index.Search(numericSearchRequest)
	if err != nil {
		t.Error(err)
	}
	expectedResultCount = uint64(1)
	if numericSearchResult.Total != expectedResultCount {
		t.Errorf("expected %d hits, got %d", expectedResultCount, numericSearchResult.Total)
	} else {
		expectedResultId := "woodforde_s_norfolk_ales-norfolk_nog_old_dark_ale"
		if numericSearchResult.Hits[0].ID != expectedResultId {
			t.Errorf("expected top hit ID: %s, got %s", expectedResultId, numericSearchResult.Hits[0].ID)
		}
	}

	// run a date range search
	queryStartDateString := "2011-10-04"
	queryStartDate, err := time.Parse("2006-01-02", queryStartDateString)
	if err != nil {
		t.Fatal(err)
	}
	dateRangeQuery := bleve.NewDateRangeQuery(queryStartDate, time.Time{})
	dateRangeQuery.SetField("updated")
	dateSearchRequest := bleve.NewSearchRequest(dateRangeQuery)
	dateSearchResult, err := index.Search(dateSearchRequest)
	if err != nil {
		t.Error(err)
	}
	expectedResultCount = uint64(3)
	if dateSearchResult.Total != expectedResultCount {
		t.Errorf("expected %d hits, got %d", expectedResultCount, dateSearchResult.Total)
	} else {
		expectedResultId := "brasserie_du_bouffay"
		if dateSearchResult.Hits[0].ID != expectedResultId {
			t.Errorf("expected top hit ID: %s, got %s", expectedResultId, dateSearchResult.Hits[0].ID)
		}
	}

	// run a prefix search
	prefixQuery := bleve.NewPrefixQuery("adir")
	prefixQuery.SetField("name")
	prefixSearchRequest := bleve.NewSearchRequest(prefixQuery)
	prefixSearchResult, err := index.Search(prefixSearchRequest)
	if err != nil {
		t.Error(err)
	}
	expectedResultCount = uint64(1)
	if prefixSearchResult.Total != expectedResultCount {
		t.Errorf("expected %d hits, got %d", expectedResultCount, prefixSearchResult.Total)
	} else {
		expectedResultId := "f_x_matt_brewing-saranac_adirondack_lager"
		if prefixSearchResult.Hits[0].ID != expectedResultId {
			t.Errorf("expected top hit ID: %s, got %s", expectedResultId, prefixSearchResult.Hits[0].ID)
		}
	}
}

type jsonFile struct {
	filename string
	contents []byte
}

// this test reproduces bug #87
// https://github.com/blevesearch/bleve/issues/87
// because of which, it will deadlock
func TestBeerSearchBug87(t *testing.T) {
	defer os.RemoveAll("beer-search-test.bleve")

	mapping, err := buildIndexMapping()
	if err != nil {
		t.Fatal(err)
	}
	index, err := bleve.New("beer-search-test.bleve", mapping)
	if err != nil {
		t.Fatal(err)
	}
	defer index.Close()

	// start indexing documents in the background
	go func() {
		// open the directory
		dirEntries, err := ioutil.ReadDir("data/")
		if err != nil {
			t.Fatal(err)
		}

		for _, dirEntry := range dirEntries {
			filename := dirEntry.Name()
			jsonBytes, err := ioutil.ReadFile("data/" + filename)
			if err != nil {
				t.Fatal(err)
			}
			ext := filepath.Ext(filename)
			docId := filename[:(len(filename) - len(ext))]
			index.Index(docId, jsonBytes)
		}
	}()

	// give indexing a head start
	time.Sleep(1 * time.Second)

	// start querying
	for i := 0; i < 1000; i++ {
		time.Sleep(1 * time.Millisecond)
		termQuery := bleve.NewTermQuery("shock")
		termQuery.SetField("name")
		termSearchRequest := bleve.NewSearchRequest(termQuery)
		// termSearchRequest.AddFacet("styles", bleve.NewFacetRequest("style", 3))
		termSearchRequest.Fields = []string{"abv"}
		_, err := index.Search(termSearchRequest)
		if err != nil {
			t.Error(err)
		}
	}
}
