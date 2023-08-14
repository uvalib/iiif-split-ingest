package main

import (
	"fmt"
	"log"
	"os"
	"path"
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

func worker(workerId int, config ServiceConfig, sqsSvc awssqs.AWS_SQS, s3Svc uva_s3.UvaS3, queue awssqs.QueueHandle, notifies <-chan Notify) {

	var notify Notify
	for {
		// wait for an inbound file
		notify = <-notifies

		start := time.Now()
		log.Printf("[worker %d] INFO: processing %s", workerId, notify.BucketKey)

		// validate the inbound file naming convention
		//err := validateInputName(workerId, notify.BucketKey)
		//if err != nil {
		//	log.Printf("[worker %d] ERROR: input name %s is invalid (%s)", workerId, notify.BucketKey, err.Error())
		//	continue
		//}

		// create the working directory
		workDir, err := makeWorkDir(workerId, config.LocalWorkDir)
		if err != nil {
			continue
		}

		// download the file from S3 to the local work directory
		downloadedName, err := downloadS3File(workerId, workDir, s3Svc, notify.SourceBucket, notify.BucketKey)
		if err != nil {
			continue
		}

		// the list of files to convert
		var convertFiles = make([]string, 0)
		// the list of the target files
		var targetFiles = make([]string, 0)

		// are we splitting the inbound file before converting it
		if len(config.SplitBinary) != 0 {
			convertFiles, err = splitFile(workerId, config, downloadedName)
			if err != nil {
				continue
			}
		} else {
			convertFiles = append(convertFiles, downloadedName)
		}

		// for every file that needs to be converted
		for _, inputName := range convertFiles {

			// generate all the needed file names
			convertedName, targetName := generateImageFilenames(workerId, config, downloadedName, inputName)

			// convert the file
			err = convertFile(workerId, config, inputName, convertedName)
			if err != nil {
				break
			}

			// if we are outputting to a local filesystem
			if len(config.OutputFSRoot) != 0 {
				// create the target directory tree
				err = createDir(workerId, path.Dir(targetName))
				if err != nil {
					break
				}

				// copy the file to the correct location
				err = copyFile(workerId, convertedName, targetName)
				if err != nil {
					break
				}
			} else {
				// do we have a bucket root defined
				f := targetName
				if len(config.OutputBucketRoot) != 0 {
					f = fmt.Sprintf("%s/%s", config.OutputBucketRoot, f)
				}
				o := uva_s3.NewUvaS3Object(config.OutputBucket, f)
				err := s3Svc.PutFromFile(o, convertedName)
				if err != nil {
					break
				}
			}

			// and save the output file in case we need to make a manifest
			targetFiles = append(targetFiles, targetName)
		}

		// cleanup the work directory (does not matter if we failed or not)
		log.Printf("[worker %d] DEBUG: cleaning up %s", workerId, workDir)
		_ = os.RemoveAll(workDir)

		// if everything went well
		if err == nil {

			// should we create a manifest for the processed file(s)
			if len(config.ManifestTemplateName) != 0 {
				log.Printf("[worker %d] DEBUG: creating manifest", workerId)
				e := createManifest(workerId, config, downloadedName, targetFiles)
				if e != nil {
					log.Printf("[worker %d] ERROR: creating manifest (%s)", workerId, err.Error())
				}
			} else {
				log.Printf("[worker %d] DEBUG: no manifest required", workerId)
			}

			// should we delete the bucket contents
			if config.DeleteSource == true {
				_ = deleteS3File(workerId, s3Svc, notify.SourceBucket, notify.BucketKey)
			}

			// delete the inbound message
			_ = deleteMessage(workerId, sqsSvc, queue, notify.ReceiptHandle)
		}

		duration := time.Since(start)
		log.Printf("[worker %d] INFO: processing %s/%s complete in %0.2f seconds",
			workerId, notify.SourceBucket, notify.BucketKey, duration.Seconds())
	}

	// should never get here
}

func deleteMessage(workerId int, aws awssqs.AWS_SQS, queue awssqs.QueueHandle, receiptHandle awssqs.ReceiptHandle) error {

	log.Printf("[worker %d] INFO: deleting queue message", workerId)

	delMessages := make([]awssqs.Message, 0, 1)
	delMessages = append(delMessages, awssqs.Message{ReceiptHandle: receiptHandle})
	opStatus, err := aws.BatchMessageDelete(queue, delMessages)
	if err != nil {
		if err != awssqs.ErrOneOrMoreOperationsUnsuccessful {
			log.Printf("[worker %d] WARNING: failed to delete a processed message (%s)", workerId, err.Error())
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

//
// end of file
//
