package main

import (
	"encoding/json"
	"log"
	"net/http"
	"strings"
)

func generateMetadata(workerId int, config ServiceConfig, downloadName string) (*Metadata, error) {

	// set up the default fields
	var md Metadata
	md.Title = "<unspecified>"
	md.Author = "<unspecified>"
	md.Published = "<unspecified>"
	md.Description = "<unspecified>"
	md.Subjects = "<unspecified>"

	// if we do not have an endpoint to get the metadata configured, return the defaults
	if len(config.ManifestMetadataQueryEndpoint) == 0 {
		return &md, nil
	}

	// our query id
	id := idFromFilename(downloadName)

	// and our http client
	client := newHttpClient(1, config.ManifestMetadataQueryTimeout)

	// get our endpoint auth token
	auth, err := getMetadataAuthToken(workerId, config, client)
	if err != nil {
		return nil, err
	}

	// get the metadata
	qmd, err := getMetadata(workerId, config, client, id, auth)
	if err != nil {
		return nil, err
	}

	// apply the fields we got results for
	md.Title = defaultIfUnspecified(qmd.Title, md.Title)
	md.Author = defaultIfUnspecified(qmd.Author, md.Author)
	md.Published = defaultIfUnspecified(qmd.Published, md.Published)
	md.Description = defaultIfUnspecified(qmd.Description, md.Description)
	md.Subjects = defaultIfUnspecified(qmd.Subjects, md.Subjects)

	return &md, nil
}

// get metadata for the manifest from the configured endpoint
func getMetadata(workerId int, config ServiceConfig, client *http.Client, id string, auth string) (*Metadata, error) {

	// make the query template
	query := strings.Replace(config.ManifestMetadataQueryTemplate, config.IdPlaceHolder, id, 1)

	// and issue the query
	b, err := httpPost(workerId, config.ManifestMetadataQueryEndpoint, client, auth, []byte(query))
	if err != nil {
		return nil, err
	}

	log.Printf("[worker %d] DEBUG: received query response [%s]", workerId, string(b))

	sr := SearchResult{}
	err = json.Unmarshal(b, &sr)
	if err != nil {
		log.Printf("[worker %d] ERROR: json unmarshal error (%s)", workerId, err)
		return nil, err
	}

	// our result structure
	var md Metadata

	// if we did not get any results
	if len(sr.Groups) == 0 || len(sr.Groups[0].Records) == 0 {
		log.Printf("[worker %d] WARNING: received no results for id [%s]", workerId, id)
		// return an empty structure
		return &md, nil
	}

	// if we received more than 1 result, attempt a match by barcode but default to the first result in the event that we
	// cannot find the barcode to match
	resultIx := 0
	resultSize := len(sr.Groups[0].Records)
	if resultSize > 1 {
		log.Printf("[worker %d] INFO: received %d results for id [%s], attempting match by barcode", workerId, resultSize, id)
		var matched bool
		resultIx, matched = findByBarcode(sr.Groups[0].Records, id)
		if matched == true {
			log.Printf("[worker %d] DEBUG: matched by barcode [%s], using result # %d", workerId, id, resultIx+1)
		} else {
			log.Printf("[worker %d] WARNING: cannot match by barcode [%s], using the first of %d results", workerId, id, resultSize)
		}
	}

	fields := sr.Groups[0].Records[resultIx].Fields

	md.Title = getFirstField("title", fields)
	md.Author = getFirstField("author", fields)
	md.Published = getFirstField("published_date", fields)
	//md.Description = getFirstField("xxx", fields)
	//md.Subjects = getFirstField("xxx", fields)

	return &md, nil
}

// find a search result by matching the id with the barcode (if possible) and return the index if located
func findByBarcode(results []Record, id string) (int, bool) {

	// iterate through results and attempt to match the barcode
	for ix, r := range results {
		barcodes := getMultiField("barcode", r.Fields)
		for _, b := range barcodes {
			if b == id {
				return ix, true
			}
		}
	}

	// we did not match a barcode
	return -1, false
}

// use auth endpoint to get an auth token
func getMetadataAuthToken(workerId int, config ServiceConfig, client *http.Client) (string, error) {

	b, err := httpPost(workerId, config.ManifestMetadataAuthEndpoint, client, "", []byte(""))
	if err != nil {
		return "", err
	}
	auth := string(b)
	//log.Printf("[worker %d] INFO: received auth token [%s]", workerId, auth)
	return auth, nil
}

// helper to select a set or default value
func defaultIfUnspecified(value string, theDefault string) string {
	if len(value) != 0 {
		return value
	}
	return theDefault
}

// get the first named field value from a set of fields
func getFirstField(name string, fields []Field) string {
	for _, f := range fields {
		if f.Name == name || f.Type == name {
			log.Printf("DEBUG: located field [%s] -> [%s]", name, f.Value)
			return f.Value
		}
	}
	log.Printf("WARNING: cannot find field [%s] in search results", name)
	return ""
}

// get all named field values from a set of fields
func getMultiField(name string, fields []Field) []string {
	result := make([]string, 0)
	for _, f := range fields {
		if f.Name == name || f.Type == name {
			log.Printf("DEBUG: located field [%s] -> [%s]", name, f.Value)
			result = append(result, f.Value)
		}
	}
	if len(result) == 0 {
		log.Printf("WARNING: cannot find field [%s] in search results", name)
	}
	return result
}

//
// end of file
//
