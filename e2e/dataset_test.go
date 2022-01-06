package e2e

import (
	"context"
	"fmt"
	"os"
	"testing"

	log "github.com/sirupsen/logrus"

	v1 "github.com/ScienceObjectsDB/go-api/api/models/v1"
	services "github.com/ScienceObjectsDB/go-api/api/services/v1"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

func TestDataset(t *testing.T) {
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

	streamer, err := ServerEndpoints.dataset.EventStreamMgmt.CreateMessageStreamHandler(&services.NotificationStreamRequest{
		Resource:           services.NotificationStreamRequest_EVENT_RESOURCES_PROJECT_RESOURCE,
		ResourceId:         createResponse.GetId(),
		IncludeSubresource: true,
		StreamType:         &services.NotificationStreamRequest_StreamAll{},
	})
	if err != nil {
		log.Fatalln(err.Error())
	}

	go streamer.StartMessageTransformation()

	datasetGetResponse, err := ServerEndpoints.dataset.GetDataset(context.Background(), &services.GetDatasetRequest{
		Id: datasetCreateResponse.GetId(),
	})
	if err != nil {
		log.Fatalln(err.Error())
	}

	assert.Equal(t, createDatasetRequest.Name, datasetGetResponse.Dataset.Name)
	assert.Equal(t, createDatasetRequest.Description, datasetGetResponse.GetDataset().Description)
	assert.ElementsMatch(t, createDatasetRequest.Labels, datasetGetResponse.Dataset.Labels)
	assert.ElementsMatch(t, createDatasetRequest.Metadata, datasetGetResponse.Dataset.Metadata)

	//_, err = ServerEndpoints.dataset.DeleteDataset(context.Background(), &services.DeleteDatasetRequest{
	//	Id: datasetCreateResponse.GetId(),
	//})
	//if err != nil {
	//	log.Fatalln(err.Error())
	//
	//}

	_, e2eComposeVar := os.LookupEnv("E2E_TEST_COMPOSE")

	if e2eComposeVar {
		msgChan := streamer.GetResponseMessageChan()
		notficationMsg := <-msgChan

		assert.Equal(t, datasetCreateResponse.GetId(), notficationMsg.Message.ResourceId)
		assert.Equal(t, v1.Resource_DATASET_RESOURCE, notficationMsg.Message.Resource)
	}
}

func TestDatasetObjectGroupsPagination(t *testing.T) {
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

	createDatasetRequest := &services.CreateDatasetRequest{
		Name:      "testdataset",
		ProjectId: createResponse.GetId(),
	}

	datasetCreateResponse, err := ServerEndpoints.dataset.CreateDataset(context.Background(), createDatasetRequest)
	if err != nil {
		log.Fatalln(err.Error())
	}

	for i := 0; i < 10; i++ {
		createObjectGroup := &services.CreateObjectGroupRequest{
			Name:        fmt.Sprintf("foo-%v", i),
			Description: "foo",
			DatasetId:   datasetCreateResponse.GetId(),
		}

		_, err := ServerEndpoints.object.CreateObjectGroup(context.Background(), createObjectGroup)
		if err != nil {
			log.Fatalln(err.Error())
		}
	}

	handledObjectGroups := make(map[string]struct{})

	objectGroups1, err := ServerEndpoints.dataset.ReadHandler.GetDatasetObjectGroups(uuid.MustParse(datasetCreateResponse.GetId()), &v1.PageRequest{
		LastUuid: "",
		PageSize: 4,
	})
	if err != nil {
		log.Fatalln(err.Error())
	}

	assert.Equal(t, 4, len(objectGroups1))

	var lastUUID uuid.UUID

	for _, objectGroup := range objectGroups1 {
		if _, ok := handledObjectGroups[objectGroup.Name]; !ok {
			handledObjectGroups[objectGroup.Name] = struct{}{}
			lastUUID = objectGroup.ID
		} else {
			log.Fatalln("found duplicate object group in pagination")
		}
	}

	objectGroups2, err := ServerEndpoints.dataset.ReadHandler.GetDatasetObjectGroups(uuid.MustParse(datasetCreateResponse.GetId()), &v1.PageRequest{
		LastUuid: lastUUID.String(),
		PageSize: 4,
	})
	if err != nil {
		log.Fatalln(err.Error())
	}

	assert.Equal(t, 4, len(objectGroups2))

	for _, objectGroup := range objectGroups2 {
		if _, ok := handledObjectGroups[objectGroup.Name]; !ok {
			handledObjectGroups[objectGroup.Name] = struct{}{}
			lastUUID = objectGroup.ID
		} else {
			log.Fatalln("found duplicate object group in pagination")
		}
	}

	objectGroups3, err := ServerEndpoints.dataset.ReadHandler.GetDatasetObjectGroups(uuid.MustParse(datasetCreateResponse.GetId()), &v1.PageRequest{
		LastUuid: lastUUID.String(),
		PageSize: 2,
	})
	if err != nil {
		log.Fatalln(err.Error())
	}

	assert.Equal(t, 2, len(objectGroups3))

	for _, objectGroup := range objectGroups3 {
		if _, ok := handledObjectGroups[objectGroup.Name]; !ok {
			handledObjectGroups[objectGroup.Name] = struct{}{}
		} else {
			log.Fatalln("found duplicate object group in pagination")
		}
	}
}
