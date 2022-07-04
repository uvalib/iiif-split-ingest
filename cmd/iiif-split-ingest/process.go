package main

import (
	"fmt"
	"log"
	"os/exec"
	"path"
	"strings"
	"time"
)

func splitFile(workerId int, config ServiceConfig, inputName string) ([]string, error) {

	// split into interesting components
	dirName := path.Dir(inputName)
	baseName := path.Base(inputName)
	fileExt := path.Ext(baseName)
	baseNoExt := strings.TrimSuffix(baseName, fileExt)

	// specify the output template
	outputTemplate := fmt.Sprintf("%s/%s", dirName, baseNoExt)

	// build the command line
	cmdLine := strings.Replace(config.SplitCommandLine, config.InFilePlaceHolder, inputName, 1)
	cmdLine = strings.Replace(cmdLine, config.OutFilePlaceHolder, outputTemplate, 1)

	// build the parameter structure
	params := strings.Split(cmdLine, " ")
	cmd := exec.Command(config.SplitBinary, params...)

	log.Printf("[worker %d] DEBUG: split command \"%s\"", workerId, cmd.String())

	start := time.Now()
	output, err := cmd.CombinedOutput()
	if err != nil {
		log.Printf("[worker %d] ERROR: splitting %s (%s)", workerId, inputName, err.Error())
		if len(output) != 0 {
			log.Printf("[worker %d] ERROR: split output [%s]", workerId, output)
		}
		// remove the input and output files and ignore any errors
		//_ = os.Remove(inputFile)
		//_ = os.Remove(outputFile)

		// return the error
		return nil, err
	}

	// cleanup and return
	duration := time.Since(start)
	log.Printf("[worker %d] INFO: split complete in %0.2f seconds", workerId, duration.Seconds())

	// if we have some output, log it
	if len(output) != 0 {
		log.Printf("[worker %d] DEBUG: split output [%s]", workerId, output)
	}

	// identify the files that were created (add the necessary delimiter, so we don't pick up the original)
	outputFiles, err := listFiles(workerId, dirName, baseNoExt, config.SplitSuffix)
	return outputFiles, err
}

func convertFile(workerId int, config ServiceConfig, inputFile string, outputFile string) error {

	// build the command line
	cmdLine := strings.Replace(config.ConvertCommandLine, config.InFilePlaceHolder, inputFile, 1)
	cmdLine = strings.Replace(cmdLine, config.OutFilePlaceHolder, outputFile, 1)

	// build the parameter structure
	params := strings.Split(cmdLine, " ")
	cmd := exec.Command(config.ConvertBinary, params...)

	log.Printf("[worker %d] DEBUG: convert command \"%s\"", workerId, cmd.String())
	start := time.Now()
	output, err := cmd.CombinedOutput()
	if err != nil {
		log.Printf("[worker %d] ERROR: converting %s (%s)", workerId, inputFile, err.Error())
		if len(output) != 0 {
			log.Printf("[worker %d] ERROR: conversion output [%s]", workerId, output)
		}
		// remove the input and output files and ignore any errors
		//_ = os.Remove(inputFile)
		//_ = os.Remove(outputFile)

		// return the error
		return err
	}

	// cleanup and return
	duration := time.Since(start)
	log.Printf("[worker %d] INFO: conversion complete in %0.2f seconds", workerId, duration.Seconds())

	// if we have some output, log it
	if len(output) != 0 {
		log.Printf("[worker %d] DEBUG: conversion output [%s]", workerId, output)
	}

	// all good
	return nil
}

//
// end of file
//
