package e2e

import (
	"context"
	"fmt"
	"testing"

	log "github.com/sirupsen/logrus"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"

	v1storagemodels "github.com/ScienceObjectsDB/go-api/sciobjsdb/api/storage/models/v1"
	v1storageservices "github.com/ScienceObjectsDB/go-api/sciobjsdb/api/storage/services/v1"
)

func TestDataset(t *testing.T) {
	createProjectRequest := &v1storageservices.CreateProjectRequest{
		Name:        "testproject_dataset",
		Description: "test",
		Metadata: []*v1storagemodels.Metadata{
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

	datasetMetadata := []*v1storagemodels.Metadata{
		{
			Key:      "Key1",
			Metadata: []byte("dasddasd"),
		},
		{
			Key:      "Key2",
			Metadata: []byte("asdasd"),
		},
	}

	datasetLabel := []*v1storagemodels.Label{
		{
			Key:   "Label1",
			Value: "LabelValue1",
		},
		{
			Key:   "Label2",
			Value: "LabelValue2",
		},
	}

	createDatasetRequest := &v1storageservices.CreateDatasetRequest{
		Name:      "testdataset",
		ProjectId: createResponse.GetId(),
		Metadata:  datasetMetadata,
		Labels:    datasetLabel,
	}

	datasetCreateResponse, err := ServerEndpoints.dataset.CreateDataset(context.Background(), createDatasetRequest)
	if err != nil {
		log.Fatalln(err.Error())
	}

	datasetGetResponse, err := ServerEndpoints.dataset.GetDataset(context.Background(), &v1storageservices.GetDatasetRequest{
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

}

func TestDatasetObjectGroupsPagination(t *testing.T) {
	createProjectRequest := &v1storageservices.CreateProjectRequest{
		Name:        "testproject_dataset",
		Description: "test",
		Metadata: []*v1storagemodels.Metadata{
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

	createDatasetRequest := &v1storageservices.CreateDatasetRequest{
		Name:      "testdataset",
		ProjectId: createResponse.GetId(),
	}

	datasetCreateResponse, err := ServerEndpoints.dataset.CreateDataset(context.Background(), createDatasetRequest)
	if err != nil {
		log.Fatalln(err.Error())
	}

	for i := 0; i < 10; i++ {
		createObjectGroup := &v1storageservices.CreateObjectGroupRequest{
			Name:        fmt.Sprintf("foobar-%v", i),
			Description: "foo",
			DatasetId:   datasetCreateResponse.GetId(),
		}

		_, err := ServerEndpoints.object.CreateObjectGroup(context.Background(), createObjectGroup)
		if err != nil {
			log.Fatalln(err.Error())
		}
	}

	handledObjectGroups := make(map[string]struct{})

	objectGroups1, err := ServerEndpoints.dataset.ReadHandler.GetDatasetObjectGroups(uuid.MustParse(datasetCreateResponse.GetId()), &v1storagemodels.PageRequest{
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

	objectGroups2, err := ServerEndpoints.dataset.ReadHandler.GetDatasetObjectGroups(uuid.MustParse(datasetCreateResponse.GetId()), &v1storagemodels.PageRequest{
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

	objectGroups3, err := ServerEndpoints.dataset.ReadHandler.GetDatasetObjectGroups(uuid.MustParse(datasetCreateResponse.GetId()), &v1storagemodels.PageRequest{
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
