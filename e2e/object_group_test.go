package e2e

import (
	"bytes"
	"context"
	"io/ioutil"
	"log"
	"net/http"
	"testing"

	v1 "github.com/ScienceObjectsDB/go-api/api/models/v1"
	services "github.com/ScienceObjectsDB/go-api/api/services/v1"
	"github.com/stretchr/testify/assert"
)

func TestObjectGroup(t *testing.T) {
	createProjectRequest := &services.CreateProjectRequest{
		Name:        "testproject_dataset",
		Description: "test",
		Metadata: []*v1.Metadata{
			{
				Key:      "TestKey1",
				Metadata: []byte("mymetadata1"),
			},
			{
				Key:      "TestKey2",
				Metadata: []byte("mymetadata2"),
			},
		},
	}

	createResponse, err := ServerEndpoints.project.CreateProject(context.Background(), createProjectRequest)
	if err != nil {
		log.Fatalln(err.Error())
	}

	datasetMetadata := []*v1.Metadata{
		{
			Key:      "Key1",
			Metadata: []byte("dasddasd"),
		},
		{
			Key:      "Key2",
			Metadata: []byte("asdasd"),
		},
	}

	datasetLabel := []*v1.Label{
		{
			Key:   "Label1",
			Value: "LabelValue1",
		},
		{
			Key:   "Label2",
			Value: "LabelValue2",
		},
	}

	createDatasetRequest := &services.CreateDatasetRequest{
		Name:      "testdataset",
		ProjectId: createResponse.GetId(),
		Metadata:  datasetMetadata,
		Labels:    datasetLabel,
	}

	datasetCreateResponse, err := ServerEndpoints.dataset.CreateDataset(context.Background(), createDatasetRequest)
	if err != nil {
		log.Fatalln(err.Error())
	}

	objectGroupMetadata := []*v1.Metadata{
		{
			Key:      "Key1OG",
			Metadata: []byte("dasddasdOG"),
		},
		{
			Key:      "Key2OG",
			Metadata: []byte("asdasdOG"),
		},
	}

	objectGroupLabel := []*v1.Label{
		{
			Key:   "Label1OG",
			Value: "LabelValue1OG",
		},
		{
			Key:   "Label2OG",
			Value: "LabelValue2OG",
		},
	}

	object1Metadata := []*v1.Metadata{
		{
			Key:      "Key1O1",
			Metadata: []byte("dasddasdO1"),
		},
		{
			Key:      "Key2OG1",
			Metadata: []byte("asdasdO1"),
		},
	}

	object1Label := []*v1.Label{
		{
			Key:   "Label1O1",
			Value: "LabelValue1O1",
		},
		{
			Key:   "Label2O1",
			Value: "LabelValue2O1",
		},
	}

	object2Metadata := []*v1.Metadata{
		{
			Key:      "Key1O2",
			Metadata: []byte("dasddasdO2"),
		},
		{
			Key:      "Key2O2",
			Metadata: []byte("asdasdO2"),
		},
	}

	object2Label := []*v1.Label{
		{
			Key:   "Label1O2",
			Value: "LabelValue1O2",
		},
		{
			Key:   "Label2O2",
			Value: "LabelValue2O2",
		},
	}

	createObjectGroupRequest := &services.CreateObjectGroupRequest{
		Name:      "testog",
		DatasetId: datasetCreateResponse.GetId(),
		Labels:    objectGroupLabel,
		Metadata:  objectGroupMetadata,
		ObjectGroupRevision: &services.CreateObjectGroupRevisionRequest{
			Objects: []*services.CreateObjectRequest{
				{
					Filename:   "testfile1",
					Filetype:   "bin",
					Labels:     object1Label,
					Metadata:   object1Metadata,
					ContentLen: 3,
				},
				{
					Filename:   "testfile2",
					Filetype:   "bin",
					Labels:     object2Label,
					Metadata:   object2Metadata,
					ContentLen: 3,
				},
			},
		},
	}

	createObjectGroupResponse, err := ServerEndpoints.object.CreateObjectGroup(context.Background(), createObjectGroupRequest)
	if err != nil {
		log.Fatalln(err.Error())
	}

	assert.NotEqual(t, createObjectGroupResponse.ObjectGroupId, 0)
	assert.NotEqual(t, createObjectGroupResponse.RevisionId, 0)

	getObjectGroupResponse, err := ServerEndpoints.object.GetObjectGroup(context.Background(), &services.GetObjectGroupRequest{
		Id: createObjectGroupResponse.ObjectGroupId,
	})
	if err != nil {
		log.Fatalln(err.Error())
	}

	assert.Equal(t, createObjectGroupRequest.Name, getObjectGroupResponse.ObjectGroup.Name)
	assert.Equal(t, createObjectGroupRequest.DatasetId, getObjectGroupResponse.ObjectGroup.DatasetId)
	assert.Equal(t, createDatasetRequest.Description, getObjectGroupResponse.GetObjectGroup().Description)
	assert.ElementsMatch(t, createObjectGroupRequest.Labels, getObjectGroupResponse.ObjectGroup.Labels)
	assert.ElementsMatch(t, createObjectGroupRequest.Metadata, getObjectGroupResponse.ObjectGroup.Metadata)

	secondRevision := &services.CreateObjectGroupRevisionRequest{
		Objects: []*services.CreateObjectRequest{
			{
				Filename:   "testfile3",
				Filetype:   "bin",
				Labels:     object1Label,
				Metadata:   object1Metadata,
				ContentLen: 3,
			},
		},
	}

	_, err = ServerEndpoints.object.AddRevisionToObjectGroup(context.Background(), &services.AddRevisionToObjectGroupRequest{
		ObjectGroupId: getObjectGroupResponse.ObjectGroup.Id,
		GroupRevison:  secondRevision,
	})
	if err != nil {
		log.Fatalln(err.Error())
	}

	currentRevisionResponse, err := ServerEndpoints.object.GetCurrentObjectGroupRevision(context.Background(), &services.GetCurrentObjectGroupRevisionRequest{
		Id: getObjectGroupResponse.ObjectGroup.Id,
	})
	if err != nil {
		log.Fatalln(err.Error())
	}

	assert.Equal(t, "testfile3", currentRevisionResponse.ObjectGroupRevision.Objects[0].Filename)

	object := currentRevisionResponse.ObjectGroupRevision.Objects[0]

	uploadLink, err := ServerEndpoints.load.CreateUploadLink(context.Background(), &services.CreateUploadLinkRequest{
		Id: object.GetId(),
	})
	if err != nil {
		log.Fatalln(err.Error())
	}

	uploadHttpRequest, err := http.NewRequest("PUT", uploadLink.UploadLink, bytes.NewBufferString("foo"))
	if err != nil {
		log.Fatalln(err.Error())
	}

	response, err := http.DefaultClient.Do(uploadHttpRequest)
	if err != nil {
		log.Fatalln(err.Error())
	}

	if response.StatusCode != 200 {
		log.Fatalln(response.Status)
	}

	downloadLink, err := ServerEndpoints.load.CreateDownloadLink(context.Background(), &services.CreateDownloadLinkRequest{
		Id: object.GetId(),
	})
	if err != nil {
		log.Fatalln(err.Error())
	}

	dlResponse, err := http.DefaultClient.Get(downloadLink.GetDownloadLink())
	if err != nil {
		log.Fatalln(err.Error())
	}

	if response.StatusCode != 200 {
		log.Fatalln(response.Status)
	}

	data, err := ioutil.ReadAll(dlResponse.Body)
	if err != nil {
		log.Fatalln(err.Error())
	}

	assert.Equal(t, string(data), "foo")

}
