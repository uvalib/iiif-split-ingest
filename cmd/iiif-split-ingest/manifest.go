package main

import (
	"bytes"
	"fmt"
	"log"
	"strings"
	"text/template"

	"github.com/barasher/go-exiftool"
)

type Image struct {
	Id       string // file basename without extension
	Filename string // file basename
	Width    string // image width
	Height   string // image height
	Format   string // image format
}

type Metadata struct {
	Title       string // document title
	Author      string // document author
	Published   string // publication date
	Description string // the description
	Subjects    string // the subjects
}

type ManifestData struct {
	Metadata          // metadata received from external sources
	Copyright string  // the copyright text (staticly configured)
	URL       string  // the manifest URL (unused at the moment)
	IIIFUrl   string  // root URL of the iiif server
	Pages     []Image // image details for each page
}

func createManifest(workerId int, config ServiceConfig, inputFile string, convertedFiles []string) error {

	// generate the manifest data
	md, err := createManifestData(workerId, config, inputFile, convertedFiles)
	if err != nil {
		return err
	}

	// render the manifest template with the data
	b, err := renderTemplate(config.ManifestTemplateName, md)
	if err != nil {
		return err
	}

	// generate output file
	outfile := generateManifestFilename(config, inputFile)
	log.Printf("DEBUG: writing manifest (%s)", outfile)
	err = writeFile(outfile, b)
	if err != nil {
		log.Printf("ERROR: writing %s (%s)", outfile, err.Error())
		return err
	}

	return nil
}

func createManifestData(workerId int, config ServiceConfig, inputFile string, convertedFiles []string) (*ManifestData, error) {

	// get attributes of all the pages (images) in the manifest
	pages, err := createPageAttributes(workerId, config, convertedFiles)
	if err != nil {
		return nil, err
	}

	// get the metadata
	metadata, err := generateMetadata(workerId, config, inputFile)
	if err != nil {
		return nil, err
	}

	// populate the manifest data
	var manifestData ManifestData
	manifestData.URL = "THE URL"
	manifestData.Copyright = config.ManifestMetadataCopyrightText
	manifestData.Title = metadata.Title
	manifestData.Author = metadata.Author
	manifestData.Published = metadata.Published
	manifestData.Description = metadata.Description
	manifestData.Subjects = metadata.Subjects
	manifestData.IIIFUrl = config.IIIFServiceRoot
	manifestData.Pages = pages

	return &manifestData, nil
}

func createPageAttributes(workerId int, config ServiceConfig, convertedFiles []string) ([]Image, error) {

	// create our helper
	et, err := exiftool.NewExiftool()
	if err != nil {
		log.Printf("ERROR: initializing exiftool (%s)", err.Error())
		return nil, err
	}
	defer et.Close()

	// our list of page attributes
	pages := make([]Image, len(convertedFiles))

	// go through each page/file
	for ix, fn := range convertedFiles {

		// extract the metadata
		infos := et.ExtractMetadata(fn)
		if infos[0].Err == nil {
			ext, _ := infos[0].GetString("FileTypeExtension")
			suffix := fmt.Sprintf(".%s", ext)
			pages[ix].Filename, _ = infos[0].GetString("FileName")
			pages[ix].Id = strings.TrimSuffix(pages[ix].Filename, suffix)
			pages[ix].Height, _ = infos[0].GetString("ImageHeight")
			pages[ix].Width, _ = infos[0].GetString("ImageWidth")
			pages[ix].Format, _ = infos[0].GetString("MIMEType")
			//log.Printf("DEBUG: %s/%s (w %s, h %s, f %s)", pages[ix].Id, pages[ix].Filename, pages[ix].Width, pages[ix].Height, pages[ix].Format)
		} else {
			log.Printf("ERROR: extracting metadata (%s)", infos[0].Err)
			return nil, infos[0].Err
		}
	}
	return pages, nil
}

func renderTemplate(templateName string, manifestData *ManifestData) (string, error) {

	tmpl := template.Must(template.ParseFiles(templateName))
	var outBuffer bytes.Buffer
	err := tmpl.Execute(&outBuffer, manifestData)
	if err != nil {
		log.Printf("ERROR: unable to render template (%s)", err.Error())
		return "", err
	}
	return outBuffer.String(), nil
}

//
// end of file
//
