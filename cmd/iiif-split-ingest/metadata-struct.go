package main

// SearchResult - a search result
type SearchResult struct {
	Groups []SearchItems `json:"group_list"`
}

// SearchItems - a list of items in the search result
type SearchItems struct {
	Records []Record `json:"record_list"`
}

// Record - a search result record
type Record struct {
	Fields []Field `json:"fields"`
}

// Field - a search result record field type
type Field struct {
	Name  string `json:"name"`
	Type  string `json:"type"`
	Label string `json:"label"`
	Value string `json:"value"`
}

//
// end of file
//
