//  Copyright (c) 2014 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

//go:build !example1 && !example2
// +build !example1,!example2

package main

import (
	"github.com/blevesearch/bleve/v2"
	"github.com/blevesearch/bleve/v2/analysis/analyzer/keyword"
	"github.com/blevesearch/bleve/v2/analysis/lang/en"
	"github.com/blevesearch/bleve/v2/mapping"
)

func buildIndexMapping() (mapping.IndexMapping, error) {
	// a generic reusable mapping for english text
	englishTextFieldMapping := bleve.NewTextFieldMapping()
	englishTextFieldMapping.Analyzer = en.AnalyzerName

	// a generic reusable mapping for keyword text
	keywordFieldMapping := bleve.NewTextFieldMapping()
	keywordFieldMapping.Analyzer = keyword.Name

	beerMapping := bleve.NewDocumentMapping()

	// name
	beerMapping.AddFieldMappingsAt("name", englishTextFieldMapping)

	// description
	beerMapping.AddFieldMappingsAt("description",
		englishTextFieldMapping)

	beerMapping.AddFieldMappingsAt("type", keywordFieldMapping)
	beerMapping.AddFieldMappingsAt("style", keywordFieldMapping)
	beerMapping.AddFieldMappingsAt("category", keywordFieldMapping)

	breweryMapping := bleve.NewDocumentMapping()
	breweryMapping.AddFieldMappingsAt("name", englishTextFieldMapping)
	breweryMapping.AddFieldMappingsAt("description", englishTextFieldMapping)

	indexMapping := bleve.NewIndexMapping()
	indexMapping.AddDocumentMapping("beer", beerMapping)
	indexMapping.AddDocumentMapping("brewery", breweryMapping)

	indexMapping.TypeField = "type"
	indexMapping.DefaultAnalyzer = "en"

	return indexMapping, nil
}
