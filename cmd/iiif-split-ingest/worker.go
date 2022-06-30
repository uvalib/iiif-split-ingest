package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"path"
	"regexp"
	"strings"
	"time"

	"github.com/uvalib/uva-aws-s3-sdk/uva-s3"
	"github.com/uvalib/virgo4-sqs-sdk/awssqs"
)

// Notify - our worker notification structure
type Notify struct {
	SourceBucket  string               // the bucket name
	BucketKey     string               // the bucket key (file name)
	ExpectedSize  int64                // the expected size of the object
	ReceiptHandle awssqs.ReceiptHandle // the inbound message receipt handle (so we can delete it)
}

// special case handling name
var archivesName = "archives"

func worker(workerId int, config ServiceConfig, sqsSvc awssqs.AWS_SQS, s3Svc uva_s3.UvaS3, queue awssqs.QueueHandle, notifies <-chan Notify) {

	var notify Notify
	for {
		// wait for an inbound file
		notify = <-notifies

		start := time.Now()
		log.Printf("[worker %d] INFO: processing %s", workerId, notify.BucketKey)

		// validate the inbound file naming convention
		err := validateInputName(workerId, notify.BucketKey)
		if err != nil {
			log.Printf("[worker %d] ERROR: input name %s is invalid (%s)", workerId, notify.BucketKey, err.Error())
			continue
		}

		// create the output file name
		outputFile := generateOutputName(workerId, config, notify.BucketKey)

		// if we should fail when a converted file already exists
		if config.FailOnOverwrite == true {
			// check to see if the file already exists
			_, e := os.Stat(outputFile)
			if e == nil {
				log.Printf("[worker %d] ERROR: %s already exists", workerId, outputFile)
				continue
			}
		}

		// create the target directory tree
		err = createOutputDirectory(workerId, outputFile)
		if err != nil {
			continue
		}

		// create temp file
		tmp, err := ioutil.TempFile(config.LocalWorkDir, "")
		if err != nil {
			log.Printf("[worker %d] ERROR: failed to create temp file (%s)", workerId, err.Error())
			continue
		}

		tmp.Close()
		downloadFile := tmp.Name()

		// download the file
		o := uva_s3.NewUvaS3Object(notify.SourceBucket, notify.BucketKey)
		err = s3Svc.GetToFile(o, downloadFile)
		if err != nil {
			log.Printf("[worker %d] ERROR: failed to download %s (%s)", workerId, notify.BucketKey, err.Error())
			continue
		}

		// convert the file
		workFile, err := convertFile(workerId, config, notify.BucketKey, downloadFile)
		if err != nil {
			continue
		}

		// copy the file to the correct location and delete the original
		err = copyFile(workerId, workFile, outputFile)
		_ = os.Remove(workFile)
		if err != nil {
			log.Printf("[worker %d] ERROR: failed to copy %s to %s (%s)", workerId, workFile, outputFile, err.Error())
			continue
		}

		// should we delete the bucket contents
		if config.DeleteAfterConvert == true {
			// bucket file has been processed, remove it
			log.Printf("[worker %d] INFO: removing S3 object %s/%s", workerId, notify.SourceBucket, notify.BucketKey)
			err = s3Svc.DeleteObject(o)
			if err != nil {
				continue
			}
		}

		// delete the inbound message
		err = deleteMessage(workerId, sqsSvc, queue, notify.ReceiptHandle)
		if err != nil {
			log.Printf("[worker %d] ERROR: failed to delete a processed message (%s)", workerId, err.Error())
			continue
		}

		duration := time.Since(start)
		log.Printf("[worker %d] INFO: processing %s complete in %0.2f seconds", workerId, notify.BucketKey, duration.Seconds())
	}

	// should never get here
}

func convertFile(workerId int, config ServiceConfig, bucketKey string, inputFile string) (string, error) {

	// create a temp file
	f, err := ioutil.TempFile(config.LocalWorkDir, fmt.Sprintf("*.%s", config.ConvertSuffix))
	if err != nil {
		return "", err
	}
	_ = f.Close()
	outputFile := f.Name()

	// do the conversion
	params := strings.Split(config.ConvertOptions, " ")
	var cmd *exec.Cmd
	switch len(params) {
	case 0:
		cmd = exec.Command(config.ConvertBinary, inputFile, outputFile)
	case 1:
		cmd = exec.Command(config.ConvertBinary, inputFile, params[0], outputFile)
	case 2:
		cmd = exec.Command(config.ConvertBinary, inputFile, params[0], params[1], outputFile)
	case 3:
		cmd = exec.Command(config.ConvertBinary, inputFile, params[0], params[1], params[2], outputFile)
	case 4:
		cmd = exec.Command(config.ConvertBinary, inputFile, params[0], params[1], params[2], params[3], outputFile)
	case 5:
		cmd = exec.Command(config.ConvertBinary, inputFile, params[0], params[1], params[2], params[3], params[4], outputFile)
	default:
		fatalIfError(fmt.Errorf("excessive command options (%d), update code", len(params)))
	}
	log.Printf("[worker %d] DEBUG: convert command \"%s\"", workerId, cmd.String())
	start := time.Now()
	output, err := cmd.CombinedOutput()
	if err != nil {
		log.Printf("[worker %d] ERROR: processing %s (%s)", workerId, bucketKey, err.Error())
		if len(output) != 0 {
			log.Printf("[worker %d] ERROR: conversion output [%s]", workerId, output)
		}
		// remove the input and output files and ignore any errors
		_ = os.Remove(inputFile)
		_ = os.Remove(outputFile)

		// return the error
		return "", err
	}

	// cleanup and return
	duration := time.Since(start)
	log.Printf("[worker %d] INFO: conversion complete in %0.2f seconds", workerId, duration.Seconds())

	// if we have some output, log it
	if len(output) != 0 {
		log.Printf("[worker %d] DEBUG: conversion output [%s]", workerId, output)
	}

	// original file has been converted, remove it and ignore any errors
	log.Printf("[worker %d] INFO: removing downloaded file %s", workerId, inputFile)
	_ = os.Remove(inputFile)

	// all good
	return outputFile, nil
}

func deleteMessage(workerId int, aws awssqs.AWS_SQS, queue awssqs.QueueHandle, receiptHandle awssqs.ReceiptHandle) error {

	log.Printf("[worker %d] INFO: deleting queue message", workerId)

	delMessages := make([]awssqs.Message, 0, 1)
	delMessages = append(delMessages, awssqs.Message{ReceiptHandle: receiptHandle})
	opStatus, err := aws.BatchMessageDelete(queue, delMessages)
	if err != nil {
		if err != awssqs.ErrOneOrMoreOperationsUnsuccessful {
			return err
		}
	}

	// check the operation results
	for ix, op := range opStatus {
		if op == false {
			log.Printf("[worker %d] WARNING: message %d failed to delete", workerId, ix)
		}
	}

	// basically everything OK
	return nil
}

// generate the output file name based on the input file and configuration
func generateOutputName(workerId int, config ServiceConfig, inputName string) string {

	// split into path and filename components
	dirName := path.Dir(inputName)
	fileName := path.Base(inputName)

	// determine the converted filename
	fileExt := path.Ext(fileName)
	convertName := fmt.Sprintf("%s.%s", strings.TrimSuffix(fileName, fileExt), config.ConvertSuffix)

	// split the path components (we have already validated they are correct)
	dirs := strings.Split(dirName, "/")

	// special case
	if dirs[1] == archivesName {
		dirTree := makeDirTree(workerId, convertName)
		return fmt.Sprintf("%s/%s/%s/%s", config.ConvertDir, dirs[1], dirTree, convertName[1:])
	} else {
		return fmt.Sprintf("%s/%s/%s", config.ConvertDir, dirs[1], convertName)
	}
}

// validate the input file name
//
// the rules for validation are as follows:
// - must contain 2 path components
// - if second path component is "archive":
//   - filename must match regex xxx
// otherwise
//   - filename can be anything
func validateInputName(workerId int, inputName string) error {

	log.Printf("[worker %d] DEBUG: validating input name %s", workerId, inputName)

	// split into path and filename components
	dirName := path.Dir(inputName)
	fileName := path.Base(inputName)

	// ensure we have 2 path components
	dirs := strings.Split(dirName, "/")
	if len(dirs) != 2 {
		return fmt.Errorf("incorrect path specification for input file (must be 2 deep)")
	}

	// if we have specific filename validation rules
	if dirs[1] == archivesName {
		fileExt := path.Ext(fileName)
		noSuffix := strings.TrimSuffix(fileName, fileExt)
		matched, err := regexp.MatchString("^c\\d{4,7}$", noSuffix)
		if err != nil {
			return err
		}
		if matched == false {
			return fmt.Errorf("%s filename is invalid; must match regex ^c\\d{4,7}$", archivesName)
		}
	}

	// all is well
	return nil
}

// create the output directory
func createOutputDirectory(workerId int, outputName string) error {

	// split into path and filename components
	dirName := path.Dir(outputName)

	log.Printf("[worker %d] DEBUG: creating directory %s", workerId, dirName)

	// create the directory if appropriate
	err := os.MkdirAll(dirName, 0755)
	if err != nil {
		log.Printf("[worker %d] ERROR: failed to create output directory %s (%s)", workerId, dirName, err.Error())
		return err
	}

	return nil
}

// make the target directory tree, we have already validate the filename so know this is safe
func makeDirTree(workerId int, fileName string) string {
	fileExt := path.Ext(fileName)
	noSuffix := strings.TrimSuffix(fileName, fileExt)
	switch len(noSuffix) {
	case 5:
		return fmt.Sprintf("%c%c/%c%c",
			fileName[1], fileName[2], fileName[3], fileName[4])
	case 6:
		return fmt.Sprintf("%c%c/%c%c/%c",
			fileName[1], fileName[2], fileName[3], fileName[4], fileName[5])
	case 7:
		return fmt.Sprintf("%c%c/%c%c/%c%c",
			fileName[1], fileName[2], fileName[3], fileName[4], fileName[5], fileName[6])
	case 8:
		return fmt.Sprintf("%c%c/%c%c/%c%c/%c",
			fileName[1], fileName[2], fileName[3], fileName[4], fileName[5], fileName[6], fileName[7])
	}

	// should never happen
	fatalIfError(fmt.Errorf("violated invariant with file %s", fileName))
	return "" // should not need this for the compiler
}

// copy the file from the old location to the new one... we cannot use os.Rename as this only works withing a
// single device
func copyFile(workerId int, oldLocation, newLocation string) error {

	log.Printf("[worker %d] INFO: copying %s to %s", workerId, oldLocation, newLocation)

	i, err := os.Open(oldLocation)
	if err != nil {
		return err
	}
	defer i.Close()
	o, err := os.Create(newLocation)
	if err != nil {
		return err
	}
	defer o.Close()
	_, err = o.ReadFrom(i)
	return err
}

//
// end of file
//
