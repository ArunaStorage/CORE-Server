package e2e

import (
	"context"
	"fmt"
	"testing"

	log "github.com/sirupsen/logrus"

	v1 "github.com/ScienceObjectsDB/go-api/api/models/v1"
	services "github.com/ScienceObjectsDB/go-api/api/services/v1"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

func TestDatasetVersion(t *testing.T) {
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
	}

	createObjectGroupResponse, err := ServerEndpoints.object.CreateObjectGroup(context.Background(), createObjectGroupRequest)
	if err != nil {
		log.Fatalln(err.Error())
	}

	assert.NotEqual(t, createObjectGroupResponse.ObjectGroupId, 0)

	getObjectGroupResponse, err := ServerEndpoints.object.GetObjectGroup(context.Background(), &services.GetObjectGroupRequest{
		Id: createObjectGroupResponse.ObjectGroupId,
	})
	if err != nil {
		log.Fatalln(err.Error())
	}

	versionMetadata := []*v1.Metadata{
		{
			Key:      "Key1V",
			Metadata: []byte("dasddasdV"),
		},
		{
			Key:      "Key2V",
			Metadata: []byte("asdasdV"),
		},
	}

	versionLabel := []*v1.Label{
		{
			Key:   "Label1",
			Value: "LabelValue1",
		},
		{
			Key:   "Label2",
			Value: "LabelValue2",
		},
	}

	releaseVersionRequest := &services.ReleaseDatasetVersionRequest{
		Name:      "foo",
		DatasetId: datasetCreateResponse.GetId(),
		Version: &v1.Version{
			Major:    1,
			Minor:    0,
			Patch:    2,
			Revision: 1,
			Stage:    v1.Version_STABLE,
		},
		Description:    "testrelease",
		ObjectGroupIds: []string{getObjectGroupResponse.ObjectGroup.Id},
		Labels:         versionLabel,
		Metadata:       versionMetadata,
	}

	versionResponse, err := ServerEndpoints.dataset.ReleaseDatasetVersion(context.Background(), releaseVersionRequest)
	if err != nil {
		log.Fatalln(err.Error())
	}

	response, err := ServerEndpoints.dataset.GetDatasetVersions(context.Background(), &services.GetDatasetVersionsRequest{
		Id: datasetCreateResponse.GetId(),
	})
	if err != nil {
		log.Fatalln(err.Error())
	}

	assert.Equal(t, len(response.GetDatasetVersions()), 1)

	versionRevisions, err := ServerEndpoints.dataset.GetDatasetVersionObjectGroups(context.Background(), &services.GetDatasetVersionObjectGroupsRequest{
		Id: versionResponse.Id,
	})
	if err != nil {
		log.Fatalln(err.Error())
	}

	assert.Equal(t, 1, len(versionRevisions.GetObjectGroup()))

	_, err = ServerEndpoints.dataset.DeleteDataset(context.Background(), &services.DeleteDatasetRequest{
		Id: datasetCreateResponse.GetId(),
	})
	if err != nil {
		log.Fatalln(err.Error())
	}
}

func TestDatasetVersionPaginated(t *testing.T) {
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

	var objectIDs []string
	for i := 0; i < 10; i++ {
		createObjectGroup := &services.CreateObjectGroupRequest{
			Name:        fmt.Sprintf("foo-%v", i),
			Description: "foo",
			DatasetId:   datasetCreateResponse.GetId(),
		}

		object, err := ServerEndpoints.object.CreateObjectGroup(context.Background(), createObjectGroup)
		if err != nil {
			log.Fatalln(err.Error())
		}

		objectIDs = append(objectIDs, object.ObjectGroupId)
	}

	handledObjectGroups := make(map[string]struct{})

	versionID, err := ServerEndpoints.dataset.CreateHandler.CreateDatasetVersion(&services.ReleaseDatasetVersionRequest{
		Name:           "foo",
		DatasetId:      datasetCreateResponse.GetId(),
		ObjectGroupIds: objectIDs,
		Version:        &v1.Version{},
	}, uuid.MustParse(createResponse.GetId()))
	if err != nil {
		log.Fatalln(err.Error())
	}

	objectGroups1, err := ServerEndpoints.dataset.ReadHandler.GetDatasetVersionWithObjectGroups(versionID, &v1.PageRequest{
		LastUuid: "",
		PageSize: 4,
	})
	if err != nil {
		log.Fatalln(err.Error())
	}

	assert.Equal(t, 4, len(objectGroups1.ObjectGroups))

	var lastUUID uuid.UUID

	for _, objectGroup := range objectGroups1.ObjectGroups {
		if _, ok := handledObjectGroups[objectGroup.Name]; !ok {
			handledObjectGroups[objectGroup.Name] = struct{}{}
			lastUUID = objectGroup.ID
		} else {
			log.Fatalln("found duplicate object group in pagination")
		}
	}

	objectGroups2, err := ServerEndpoints.dataset.ReadHandler.GetDatasetVersionWithObjectGroups(versionID, &v1.PageRequest{
		LastUuid: lastUUID.String(),
		PageSize: 4,
	})
	if err != nil {
		log.Fatalln(err.Error())
	}

	assert.Equal(t, 4, len(objectGroups2.ObjectGroups))

	for _, objectGroup := range objectGroups2.ObjectGroups {
		if _, ok := handledObjectGroups[objectGroup.Name]; !ok {
			handledObjectGroups[objectGroup.Name] = struct{}{}
			lastUUID = objectGroup.ID
		} else {
			log.Fatalln("found duplicate object group in pagination")
		}
	}

	objectGroups3, err := ServerEndpoints.dataset.ReadHandler.GetDatasetVersionWithObjectGroups(versionID, &v1.PageRequest{
		LastUuid: lastUUID.String(),
		PageSize: 2,
	})
	if err != nil {
		log.Fatalln(err.Error())
	}

	assert.Equal(t, 2, len(objectGroups3.ObjectGroups))

	for _, objectGroup := range objectGroups3.ObjectGroups {
		if _, ok := handledObjectGroups[objectGroup.Name]; !ok {
			handledObjectGroups[objectGroup.Name] = struct{}{}
		} else {
			log.Fatalln("found duplicate object group in pagination")
		}
	}
}
