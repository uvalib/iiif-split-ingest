package main

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"time"
)

// seem like reasonable defaults
var maxHttpRetries = 3
var retrySleepTime = 250 * time.Millisecond

func newHttpClient(maxConnections int, timeout int) *http.Client {

	return &http.Client{
		Transport: &http.Transport{
			MaxIdleConnsPerHost: maxConnections,
		},
		Timeout: time.Duration(timeout) * time.Second,
	}
}

func httpPost(workerId int, url string, client *http.Client, auth string, buffer []byte) ([]byte, error) {

	var response *http.Response
	count := 0

	log.Printf("[worker %d] INFO: post url [%s]", workerId, url)
	log.Printf("[worker %d] INFO: post payload [%s]", workerId, buffer)

	for {
		req, err := http.NewRequest("POST", url, bytes.NewBuffer(buffer))
		if err != nil {
			return nil, err
		}

		req.Header.Set("Content-Type", "application/json")

		// if we have an auth token
		if len(auth) != 0 {
			req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", auth))
		}

		response, err = client.Do(req)
		count++
		if err != nil {
			if canRetry(err) == false {
				return nil, err
			}

			// break when tried too many times
			if count >= maxHttpRetries {
				log.Printf("[worker %d] ERROR: POST failed with error, giving up (%s)", workerId, err)
				return nil, err
			}

			log.Printf("[worker %d] WARNING: POST failed with error, retrying (%s)", workerId, err)

			// sleep for a bit before retrying
			time.Sleep(retrySleepTime)
		} else {

			defer response.Body.Close()

			body, err := io.ReadAll(response.Body)

			// happy day, hopefully all is well
			if response.StatusCode == http.StatusOK {

				// if the body read failed
				if err != nil {
					log.Printf("[worker %d] ERROR: read failed with error (%s)", workerId, err)
					return nil, err
				}

				// everything went OK
				return body, nil
			}

			log.Printf("[worker %d] ERROR POST failed with status %d (%s)", workerId, response.StatusCode, body)
			return body, fmt.Errorf("request returns HTTP %d", response.StatusCode)
		}
	}
}

// examines the error and decides if it can be retried
func canRetry(err error) bool {

	if strings.Contains(err.Error(), "operation timed out") == true {
		return true
	}

	if strings.Contains(err.Error(), "Client.Timeout exceeded") == true {
		return true
	}

	if strings.Contains(err.Error(), "write: broken pipe") == true {
		return true
	}

	if strings.Contains(err.Error(), "no such host") == true {
		return true
	}

	if strings.Contains(err.Error(), "network is down") == true {
		return true
	}

	return false
}

//
// end of file
//
