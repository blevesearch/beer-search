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
	"github.com/blevesearch/bleve/analysis/analyzers/custom_analyzer"
	"github.com/blevesearch/bleve/analysis/analyzers/keyword_analyzer"
	"github.com/blevesearch/bleve/analysis/language/en"
	"github.com/blevesearch/bleve/analysis/token_filters/edge_ngram_filter"
	"github.com/blevesearch/bleve/analysis/token_filters/lower_case_filter"
	"github.com/blevesearch/bleve/analysis/tokenizers/unicode"
	"github.com/blevesearch/bleve/mapping"
	"github.com/blevesearch/blevex/detect_lang"
)

const textFieldAnalyzer = "en"

func buildIndexMapping() (mapping.IndexMapping, error) {

	// a custom field definition that uses our custom analyzer
	edgeNgram325FieldMapping := bleve.NewTextFieldMapping()
	edgeNgram325FieldMapping.Analyzer = "enWithEdgeNgram325"

	// a generic reusable mapping for english text
	englishTextFieldMapping := bleve.NewTextFieldMapping()
	englishTextFieldMapping.Analyzer = en.AnalyzerName

	// a generic reusable mapping for keyword text
	keywordFieldMapping := bleve.NewTextFieldMapping()
	keywordFieldMapping.Analyzer = keyword_analyzer.Name

	// a specific mapping to index the description fields
	// detected language
	descriptionLangFieldMapping := bleve.NewTextFieldMapping()
	descriptionLangFieldMapping.Name = "descriptionLang"
	descriptionLangFieldMapping.Analyzer = detect_lang.AnalyzerName
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
			"type": edge_ngram_filter.Name,
			"min":  3.0,
			"max":  25.0,
		})
	if err != nil {
		return nil, err
	}

	err = indexMapping.AddCustomAnalyzer("enWithEdgeNgram325",
		map[string]interface{}{
			"type":      custom_analyzer.Name,
			"tokenizer": unicode.Name,
			"token_filters": []string{
				en.PossessiveName,
				lower_case_filter.Name,
				en.StopName,
				"edgeNgram325",
			},
		})
	if err != nil {
		return nil, err
	}

	return indexMapping, nil
}
