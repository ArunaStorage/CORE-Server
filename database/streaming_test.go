package database

import (
	"net/url"
	"testing"
	"time"

	"github.com/ScienceObjectsDB/CORE-Server/signing"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

func TestCreateDatasetObjectGroupsURL(t *testing.T) {
	streamingHandler := Streaming{
		StreamingEndpoint: "http://testendpoint:9010",
		SigningSecret:     "signing-secret",
	}
	startTime := time.Now()
	endTime := time.Now()

	startTimeString, err := startTime.MarshalText()
	if err != nil {
		log.Println(err.Error())
		t.Log(err.Error())
	}

	endTimeString, err := endTime.MarshalText()
	if err != nil {
		log.Println(err.Error())
		t.Log(err.Error())
	}

	resultURL, err := streamingHandler.createResourceObjectGroupsURL(500, "/dataset", "starttime", string(startTimeString), "endtime", string(endTimeString))
	if err != nil {
		log.Println(err.Error())
		t.Log(err.Error())
	}

	parsedURL, err := url.Parse(resultURL)
	if err != nil {
		log.Println(err.Error())
		t.Log(err.Error())
	}

	startTimeFromQuery, err := url.QueryUnescape(parsedURL.Query().Get("starttime"))
	if err != nil {
		log.Println(err.Error())
		t.Log(err.Error())
	}
	parsedStartTime, err := time.Parse(time.RFC3339, startTimeFromQuery)
	if err != nil {
		log.Println(err.Error())
		t.Log(err.Error())
	}

	endTimeFromQuery, err := url.QueryUnescape(parsedURL.Query().Get("endtime"))
	if err != nil {
		log.Println(err.Error())
		t.Log(err.Error())
	}
	parsedEndTime, err := time.Parse(time.RFC3339, endTimeFromQuery)
	if err != nil {
		log.Println(err.Error())
		t.Log(err.Error())
	}

	queryStartTime, err := parsedStartTime.MarshalText()
	if err != nil {
		log.Println(err.Error())
		t.Log(err.Error())
	}

	queryEndTime, err := parsedEndTime.MarshalText()
	if err != nil {
		log.Println(err.Error())
		t.Log(err.Error())
	}

	assert.Equal(t, parsedURL.Host, "testendpoint:9010")
	assert.Equal(t, parsedURL.Scheme, "http")
	assert.Equal(t, parsedURL.Path, "/dataset")
	assert.Equal(t, parsedURL.Query().Get("id"), "500")
	assert.Equal(t, string(startTimeString), string(queryStartTime))
	assert.Equal(t, string(endTimeString), string(queryEndTime))

	verified, err := signing.VerifyHMAC_sha256("signing-secret", parsedURL)
	if err != nil {
		log.Println(err.Error())
		t.Fatal()
	}

	if !verified {
		t.Fatalf("could not verify signed link")
	}
}
