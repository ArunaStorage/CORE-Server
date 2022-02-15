package e2e

import (
	"context"
	"testing"

	log "github.com/sirupsen/logrus"

	v1storagemodels "github.com/ScienceObjectsDB/go-api/sciobjsdb/api/storage/models/v1"
	v1storageservices "github.com/ScienceObjectsDB/go-api/sciobjsdb/api/storage/services/v1"
	"github.com/stretchr/testify/assert"
)

func TestProject(t *testing.T) {
	createRequest := &v1storageservices.CreateProjectRequest{
		Name:        "testproject1",
		Description: "test",
		Metadata: []*v1storagemodels.Metadata{
			{
				Key:      "TestKey1",
				Metadata: []byte("mymetadata1"),
			},
			{
				Key:      "TestKey2",
				Metadata: []byte("mymetadata3"),
			},
		},
	}

	createResponse, err := ServerEndpoints.project.CreateProject(context.Background(), createRequest)
	if err != nil {
		log.Fatalln(err.Error())
	}

	getResponse, err := ServerEndpoints.project.GetProject(context.Background(), &v1storageservices.GetProjectRequest{
		Id: createResponse.GetId(),
	})
	if err != nil {
		log.Fatalln(err.Error())
	}

	assert.Equal(t, createRequest.Name, getResponse.Project.Name)
	assert.Equal(t, createRequest.Description, getResponse.Project.Description)
	assert.ElementsMatch(t, createRequest.Labels, getResponse.Project.Labels)
	assert.ElementsMatch(t, createRequest.Metadata, getResponse.Project.Metadata)

	_, err = ServerEndpoints.project.DeleteProject(context.Background(), &v1storageservices.DeleteProjectRequest{
		Id: createResponse.GetId(),
	})
	if err != nil {
		log.Fatalln(err.Error())
	}
}
