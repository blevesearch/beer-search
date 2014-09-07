//  Copyright (c) 2014 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

// +build example2

package main

import (
	"github.com/blevesearch/bleve"
)

const textFieldAnalyzer = "en"

func buildIndexMapping() (*bleve.IndexMapping, error) {

	// a custom field definition that uses our custom analyzer
	edgeNgram325FieldMapping := bleve.NewTextFieldMapping()
	edgeNgram325FieldMapping.Analyzer = "enWithEdgeNgram325"

	// a generic reusable mapping for english text
	englishTextFieldMapping := bleve.NewTextFieldMapping()
	englishTextFieldMapping.Analyzer = "en"

	// a generic reusable mapping for keyword text
	keywordFieldMapping := bleve.NewTextFieldMapping()
	keywordFieldMapping.Analyzer = "keyword"

	// a specific mapping to index the description fields
	// detected language
	descriptionLangFieldMapping := bleve.NewTextFieldMapping()
	descriptionLangFieldMapping.Name = "descriptionLang"
	descriptionLangFieldMapping.Analyzer = "detect_lang"
	descriptionLangFieldMapping.Store = false
	descriptionLangFieldMapping.IncludeTermVectors = false
	descriptionLangFieldMapping.IncludeInAll = false

	beerMapping := bleve.NewDocumentMapping()

	// name
	beerMapping.AddFieldMappingsAt("name", englishTextFieldMapping)

	// description
	beerMapping.AddFieldMappingsAt("description",
		edgeNgram325FieldMapping,
		descriptionLangFieldMapping)

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
	indexMapping.DefaultAnalyzer = textFieldAnalyzer

	err := indexMapping.AddCustomTokenFilter("edgeNgram325",
		map[string]interface{}{
			"type": "edge_ngram",
			"min":  3.0,
			"max":  25.0,
		})
	if err != nil {
		return nil, err
	}

	err = indexMapping.AddCustomAnalyzer("enWithEdgeNgram325",
		map[string]interface{}{
			"type":      "custom",
			"tokenizer": "unicode",
			"token_filters": []string{
				"possessive_en",
				"to_lower",
				"stop_en",
				"edgeNgram325",
			},
		})
	if err != nil {
		return nil, err
	}

	return indexMapping, nil
}
