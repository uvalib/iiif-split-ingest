package main

import (
	"fmt"
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
func generateImageFilenames(workerId int, config ServiceConfig, downloadName string, inputName string) (string, string) {

	// split into interesting components
	inputDirName := path.Dir(inputName)
	inputBaseName := path.Base(inputName)
	inputFileExt := path.Ext(inputBaseName)
	inputBaseNoExt := strings.TrimSuffix(inputBaseName, inputFileExt)

	// we use the original download name for the directory in some cases
	downloadBaseName := path.Base(downloadName)
	downloadFileExt := path.Ext(downloadBaseName)
	downloadBaseNoExt := strings.TrimSuffix(downloadBaseName, downloadFileExt)

	// generate new components
	convertName := fmt.Sprintf("%s/%s.%s", inputDirName, inputBaseNoExt, config.ConvertSuffix)
	outputName := fmt.Sprintf("%s/%s.%s", outputDirName(workerId, config, downloadBaseNoExt), inputBaseNoExt, config.ConvertSuffix)
	if len(config.OutputFSRoot) != 0 {
		outputName = fmt.Sprintf("%s/%s", config.OutputFSRoot, outputName)
	}

	log.Printf("[worker %d] DEBUG: convert name [%s], output name [%s]", workerId, convertName, outputName)
	return convertName, outputName
}

func generateManifestFilename(config ServiceConfig, downloadName string) string {

	// we use the original download name for the manifest id
	id := idFromFilename(downloadName)

	// do placeholder substitution
	filename := strings.Replace(config.ManifestOutputName, config.IdPlaceHolder, id, 1)
	return fmt.Sprintf("%s/%s", config.ManifestOutputDir, filename)
}

func idFromFilename(downloadName string) string {

	// we use the original download name for the manifest id
	downloadBaseName := path.Base(downloadName)
	downloadFileExt := path.Ext(downloadBaseName)
	return strings.TrimSuffix(downloadBaseName, downloadFileExt)
}

// make the target directory tree based on the id and configuration
func outputDirName(workerId int, config ServiceConfig, id string) string {

	// if we are not partitioning the output directory, just return the ID
	if config.PartitionOutputDir == false {
		return id
	}

	// this is a special case where we remove a leading character from the identifier to make the partitioned directory name
	if isLetter(id[0]) == true {
		id = id[1:]
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

	workDir, err := os.MkdirTemp(baseDir, "*")
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

func listFiles(workerId int, directory string, prefix string, suffix string) ([]string, error) {

	files, err := os.ReadDir(directory)
	if err != nil {
		log.Printf("[worker %d] ERROR: listing files in '%s' (%s)", workerId, directory, err.Error())
		return nil, err
	}

	// see if we found anything
	filesFound := make([]string, 0)
	for _, f := range files {
		if f.IsDir() == false {
			if strings.HasPrefix(f.Name(), prefix) == true && strings.HasSuffix(f.Name(), suffix) == true {
				log.Printf("[worker %d] DEBUG: found '%s'", workerId, f.Name())
				filesFound = append(filesFound, fmt.Sprintf("%s/%s", directory, f.Name()))
			}
		}
	}

	return filesFound, nil
}

func writeFile(filename string, buffer string) error {
	data := []byte(buffer)
	return os.WriteFile(filename, data, 0644)
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
