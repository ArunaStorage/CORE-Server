package e2e

import (
	"context"
	"testing"

	log "github.com/sirupsen/logrus"

	v1storageservices "github.com/ScienceObjectsDB/go-api/sciobjsdb/api/storage/services/v1"
	"github.com/stretchr/testify/assert"
)

func TestProject(t *testing.T) {
	createRequest := &v1storageservices.CreateProjectRequest{
		Name:        "testproject1",
		Description: "test",
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

	_, err = ServerEndpoints.project.DeleteProject(context.Background(), &v1storageservices.DeleteProjectRequest{
		Id: createResponse.GetId(),
	})
	if err != nil {
		log.Fatalln(err.Error())
	}
}
