package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path"
	"strings"

	"github.com/uvalib/uva-aws-s3-sdk/uva-s3"
)

func fatalIfError(err error) {
	if err != nil {
		log.Fatalf("FATAL ERROR: %s", err.Error())
	}
}

// download a file from S3 and return the downloaded filename
func downloadS3File(workerId int, workDir string, s3Svc uva_s3.UvaS3, bucket string, key string) (string, error) {

	// create the download filename
	fileName := path.Base(key)
	downloadFile := fmt.Sprintf("%s/%s", workDir, fileName)

	// download the file
	o := uva_s3.NewUvaS3Object(bucket, key)
	err := s3Svc.GetToFile(o, downloadFile)
	if err != nil {
		log.Printf("[worker %d] ERROR: failed to download %s (%s)", workerId, key, err.Error())
		return "", err
	}

	return downloadFile, nil
}

// delete a file from S3
func deleteS3File(workerId int, s3Svc uva_s3.UvaS3, bucket string, key string) error {
	o := uva_s3.NewUvaS3Object(bucket, key)
	err := s3Svc.DeleteObject(o)
	if err != nil {
		log.Printf("[worker %d] ERROR: removing S3 object %s/%s (%s)", workerId, bucket, key, err.Error())
	}
	return err
}

// generate the names of the conversion and target files
func generateFilenames(workerId int, config ServiceConfig, inputName string) (string, string) {

	// split into interesting components
	dirName := path.Dir(inputName)
	baseName := path.Base(inputName)
	fileExt := path.Ext(baseName)
	baseNoExt := strings.TrimSuffix(baseName, fileExt)

	// this is a special case where we remove a leading character from the identifier to make the directory name
	id := baseNoExt
	if isLetter(id[0]) == true {
		id = id[1:]
	}

	// generate new components
	convertName := fmt.Sprintf("%s/%s.%s", dirName, baseNoExt, config.ConvertSuffix)
	outputName := fmt.Sprintf("%s/%s/%s.%s", config.ImageOutputRoot, outputDirName(workerId, config, id), baseNoExt, config.ConvertSuffix)
	return convertName, outputName
}

// make the target directory tree based on the id and configuration
func outputDirName(workerId int, config ServiceConfig, id string) string {

	// if we are not partitioning the output directory, just return the ID
	if config.PartitionOutputDir == false {
		return id
	}

	dirName := ""
	for ix, c := range id {

		// time to add a slash character?
		if ix > 0 && ix%2 == 0 {
			dirName = fmt.Sprintf("%s/", dirName)
		}
		// add the character from the id
		dirName = fmt.Sprintf("%s%c", dirName, c)
	}

	log.Printf("[worker %d] DEBUG: id: '%s' -> output dir: '%s'", workerId, id, dirName)
	return dirName
}

// make a working directory
func makeWorkDir(workerId int, baseDir string) (string, error) {

	workDir, err := ioutil.TempDir(baseDir, "")
	if err != nil {
		log.Printf("[worker %d] ERROR: failed to create work directory (%s)", workerId, err.Error())
		return "", err
	}
	return workDir, nil
}

// create a directory
func createDir(workerId int, dirName string) error {

	log.Printf("[worker %d] DEBUG: creating directory %s", workerId, dirName)

	// create the directory if appropriate
	err := os.MkdirAll(dirName, 0755)
	if err != nil {
		log.Printf("[worker %d] ERROR: failed to create output directory %s (%s)", workerId, dirName, err.Error())
		return err
	}

	return nil
}

// copy the file from the old location to the new one... we cannot use os.Rename as this only works withing a
// single device
func copyFile(workerId int, oldLocation string, newLocation string) error {

	log.Printf("[worker %d] INFO: copying '%s' -> '%s'", workerId, oldLocation, newLocation)

	i, err := os.Open(oldLocation)
	if err != nil {
		log.Printf("[worker %d] ERROR: failed to open '%s' (%s)", workerId, oldLocation, err.Error())
		return err
	}
	defer i.Close()
	o, err := os.Create(newLocation)
	if err != nil {
		log.Printf("[worker %d] ERROR: failed to create '%s' (%s)", workerId, newLocation, err.Error())
		return err
	}
	defer o.Close()
	_, err = o.ReadFrom(i)
	if err != nil {
		log.Printf("[worker %d] ERROR: failed to copy from '%s' -> '%s' (%s)", workerId, oldLocation, newLocation, err.Error())
		return err
	}

	return nil
}

func fileExists(filename string) bool {
	_, err := os.Stat(filename)
	return err == nil
}

func isLetter(c uint8) bool {
	return ('a' <= c && c <= 'z') || ('A' <= c && c <= 'Z')
}

//
// end of file
//
