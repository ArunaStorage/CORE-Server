package e2e

import (
	"context"
	"testing"

	log "github.com/sirupsen/logrus"

	v1 "github.com/ScienceObjectsDB/go-api/api/models/v1"
	services "github.com/ScienceObjectsDB/go-api/api/services/v1"
	"github.com/stretchr/testify/assert"
)

func TestProject(t *testing.T) {
	createRequest := &services.CreateProjectRequest{
		Name:        "testproject1",
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

	createResponse, err := ServerEndpoints.project.CreateProject(context.Background(), createRequest)
	if err != nil {
		log.Fatalln(err.Error())
	}

	getResponse, err := ServerEndpoints.project.GetProject(context.Background(), &services.GetProjectRequest{
		Id: createResponse.GetId(),
	})
	if err != nil {
		log.Fatalln(err.Error())
	}

	assert.Equal(t, createRequest.Name, getResponse.Project.Name)
	assert.Equal(t, createRequest.Description, getResponse.Project.Description)
	assert.ElementsMatch(t, createRequest.Labels, getResponse.Project.Labels)
	assert.ElementsMatch(t, createRequest.Metadata, getResponse.Project.Metadata)

	_, err = ServerEndpoints.project.DeleteProject(context.Background(), &services.DeleteProjectRequest{
		Id: createResponse.GetId(),
	})
	if err != nil {
		log.Fatalln(err.Error())
	}
}
