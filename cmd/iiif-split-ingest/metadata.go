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

func getMetadata(workerId int, config ServiceConfig, client *http.Client, id string, auth string) (*Metadata, error) {

	// make the query template
	query := strings.Replace(config.ManifestMetadataQueryTemplate, config.IdPlaceHolder, id, 1)

	// and issue the query
	b, err := httpPost(workerId, config.ManifestMetadataQueryEndpoint, client, auth, []byte(query))
	if err != nil {
		return nil, err
	}

	//log.Printf("worker %d: DEBUG received query response [%s]", workerId, string(b))

	sr := SearchResult{}
	err = json.Unmarshal(b, &sr)
	if err != nil {
		log.Printf("worker %d: ERROR json unmarshal error (%s)", workerId, err)
		return nil, err
	}

	// our result structure
	var md Metadata

	// if we did not get any results
	if len(sr.Groups) == 0 {
		log.Printf("worker %d: WARNING received no results for id [%s]", workerId, id)
		// return an empty structure
		return &md, nil
	}

	if len(sr.Groups) > 1 {
		log.Printf("worker %d: WARNING received multiple results for id [%s], using the first one", workerId, id)
	}

	// more smarts later...
	fields := sr.Groups[0].Records[0].Fields

	md.Title = getField("title", fields)
	md.Author = getField("author", fields)
	md.Published = getField("published_date", fields)
	//md.Description = getField("xxx", fields)
	//md.Subjects = getField("xxx", fields)

	return &md, nil
}

func getMetadataAuthToken(workerId int, config ServiceConfig, client *http.Client) (string, error) {

	b, err := httpPost(workerId, config.ManifestMetadataAuthEndpoint, client, "", []byte(""))
	if err != nil {
		return "", err
	}
	auth := string(b)
	//log.Printf("worker %d: INFO received auth token [%s]", workerId, auth)
	return auth, nil
}

func defaultIfUnspecified(value string, theDefault string) string {
	if len(value) != 0 {
		return value
	}
	return theDefault
}

func getField(name string, fields []Field) string {

	for _, f := range fields {
		if f.Name == name || f.Type == name {
			log.Printf("DEBUG: located field [%s] -> [%s]", name, f.Value)
			return f.Value
		}
	}
	log.Printf("WARNING: cannot find field [%s] in search results", name)
	return ""
}

//
// end of file
//
