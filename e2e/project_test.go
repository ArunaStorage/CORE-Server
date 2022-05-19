package e2e

import (
	"context"
	"testing"

	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"

	v1storagemodels "github.com/ScienceObjectsDB/go-api/sciobjsdb/api/storage/models/v1"
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

func TestDuplicateUserProject(t *testing.T) {
	// Create simple project with name and description
	createRequest := &v1storageservices.CreateProjectRequest{
		Name:        "Test Project 002",
		Description: "This project is used to test that users cannot be added as duplicate.",
	}

	createResponse, err := ServerEndpoints.project.CreateProject(context.Background(), createRequest)
	if err != nil {
		log.Fatalln(err.Error())
	}

	// Add two individual users to Project
	userId01 := uuid.New()
	scope := []v1storagemodels.Right{v1storagemodels.Right(v1storagemodels.Right_RIGHT_READ)}
	addUserResponse01, err := ServerEndpoints.project.AddUserToProject(
		context.Background(),
		&v1storageservices.AddUserToProjectRequest{
			UserId:    userId01.String(),
			Scope:     scope,
			ProjectId: createResponse.Id,
		})
	if err != nil {
		log.Fatalln(err.Error())
	}

	userId02 := uuid.New()
	scope = []v1storagemodels.Right{v1storagemodels.Right(v1storagemodels.Right_RIGHT_WRITE)}
	addUserResponse02, err := ServerEndpoints.project.AddUserToProject(
		context.Background(),
		&v1storageservices.AddUserToProjectRequest{
			UserId:    userId02.String(),
			Scope:     scope,
			ProjectId: createResponse.Id,
		})
	if err != nil {
		log.Fatalln(err.Error())
	}

	// Validate creation of Users
	assert.NotNil(t, addUserResponse01)
	assert.NotNil(t, addUserResponse02)

	// Try to add users with identical OAuth2IDs to project which should fail and return (nil, error)
	addIdenticalUserResponse01, err := ServerEndpoints.project.AddUserToProject(
		context.Background(),
		&v1storageservices.AddUserToProjectRequest{
			UserId:    userId01.String(),
			Scope:     scope,
			ProjectId: createResponse.Id,
		})

	assert.Nil(t, addIdenticalUserResponse01)
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "User already assigned to this project.")

	addIdenticalUserResponse02, err := ServerEndpoints.project.AddUserToProject(
		context.Background(),
		&v1storageservices.AddUserToProjectRequest{
			UserId:    userId02.String(),
			Scope:     scope,
			ProjectId: createResponse.Id,
		})

	assert.Nil(t, addIdenticalUserResponse02)
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "User already assigned to this project.")

	// Delete Project completely
	_, err = ServerEndpoints.project.DeleteProject(context.Background(), &v1storageservices.DeleteProjectRequest{
		Id: createResponse.Id,
	})
	if err != nil {
		log.Fatalln(err.Error())
	}

	// Validating Project deletion by trying to get deleted Project which should fail and return (nil, error)
	nilResponse, err := ServerEndpoints.project.GetProject(context.Background(), &v1storageservices.GetProjectRequest{
		Id: createResponse.Id,
	})

	assert.NotNil(t, err)
	assert.Nil(t, nilResponse)
}
