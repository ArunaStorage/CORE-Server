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
	// Create Project with
	createRequest := &v1storageservices.CreateProjectRequest{
		Name:        "Test Project 001",
		Description: "This is a test description.",
		Labels: []*v1storagemodels.Label{
			{
				Key:   "Label-01",
				Value: "Lorem Ipsum Dolor ... Sit?",
			},
			{
				Key:   "Label-02",
				Value: "Amet consetetur sadipscing, elitr!",
			},
		},
	}

	createResponse, err := ServerEndpoints.project.CreateProject(context.Background(), createRequest)
	if err != nil {
		log.Fatalln(err.Error())
	}

	// Get Project complete with all fields and validate correct creation
	getResponse, err := ServerEndpoints.project.GetProject(context.Background(), &v1storageservices.GetProjectRequest{
		Id: createResponse.GetId(),
	})
	if err != nil {
		log.Fatalln(err.Error())
	}

	assert.Equal(t, createRequest.Name, getResponse.Project.Name)
	assert.Equal(t, createRequest.Description, getResponse.Project.Description)
	assert.ElementsMatch(t, createRequest.Labels, getResponse.Project.Labels)

	// Delete Project completely (Created Labels stay)
	_, err = ServerEndpoints.project.DeleteProject(context.Background(), &v1storageservices.DeleteProjectRequest{
		Id: createResponse.GetId(),
	})
	if err != nil {
		log.Fatalln(err.Error())
	}

	// Validating Project deletion by trying to get deleted Project which should fail and return nil
	nilResponse, err := ServerEndpoints.project.GetProject(context.Background(), &v1storageservices.GetProjectRequest{
		Id: getResponse.Project.Id,
	})

	assert.NotNil(t, err)
	assert.Nil(t, nilResponse)
}

func TestProjectUsers(t *testing.T) {
	// Create simple project with name and description
	createRequest := &v1storageservices.CreateProjectRequest{
		Name:        "Test Project 002",
		Description: "This project is used to test that users cannot be added as duplicate.",
	}

	createResponse, err := ServerEndpoints.project.CreateProject(context.Background(), createRequest)
	if err != nil {
		log.Fatalln(err.Error())
	}

	projectID := uuid.MustParse(createResponse.Id)

	// Add two individual users to Project
	userId01 := uuid.New()
	scope := []v1storagemodels.Right{v1storagemodels.Right(v1storagemodels.Right_RIGHT_READ)}
	addUserResponse01, err := ServerEndpoints.project.AddUserToProject(
		context.Background(),
		&v1storageservices.AddUserToProjectRequest{
			UserId:    userId01.String(),
			Scope:     scope,
			ProjectId: projectID.String(),
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
			ProjectId: projectID.String(),
		})
	if err != nil {
		log.Fatalln(err.Error())
	}

	// Validate creation of Users
	projectUsers, err := ServerEndpoints.project.ReadHandler.GetProjectUsers(projectID)
	if err != nil {
		log.Fatalln(err.Error())
	}

	var oauth2Ids []string
	for _, user := range projectUsers {
		oauth2Ids = append(oauth2Ids, user.UserOauth2ID)
	}

	assert.Equal(t, 3, len(projectUsers))

	assert.NotNil(t, addUserResponse01)
	assert.Contains(t, oauth2Ids, userId01.String())
	assert.Equal(t, projectID, projectUsers[1].ProjectID)

	assert.NotNil(t, addUserResponse02)
	assert.Contains(t, oauth2Ids, userId02.String())
	assert.Equal(t, projectID, projectUsers[2].ProjectID)

	// Try to add users with identical OAuth2IDs to project which should fail and return (nil, error)
	addIdenticalUserResponse01, err := ServerEndpoints.project.AddUserToProject(
		context.Background(),
		&v1storageservices.AddUserToProjectRequest{
			UserId:    userId01.String(),
			Scope:     scope,
			ProjectId: projectID.String(),
		})

	assert.Nil(t, addIdenticalUserResponse01)
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "23505")

	addIdenticalUserResponse02, err := ServerEndpoints.project.AddUserToProject(
		context.Background(),
		&v1storageservices.AddUserToProjectRequest{
			UserId:    userId02.String(),
			Scope:     scope,
			ProjectId: projectID.String(),
		})

	assert.Nil(t, addIdenticalUserResponse02)
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "23505")

	projectUsers, err = ServerEndpoints.project.ReadHandler.GetProjectUsers(projectID)
	if err != nil {
		log.Fatalln(err.Error())
	}
	assert.Equal(t, 3, len(projectUsers))

	// Delete Project completely
	_, err = ServerEndpoints.project.DeleteProject(context.Background(), &v1storageservices.DeleteProjectRequest{
		Id: projectID.String(),
	})
	if err != nil {
		log.Fatalln(err.Error())
	}

	// Validating Project deletion by trying to get deleted Project which should fail and return (nil, error)
	nilResponse, err := ServerEndpoints.project.GetProject(context.Background(), &v1storageservices.GetProjectRequest{
		Id: projectID.String(),
	})

	assert.NotNil(t, err)
	assert.Nil(t, nilResponse)
}

func TestAddConcurrentProjectUsers(t *testing.T) {
	createRequest := &v1storageservices.CreateProjectRequest{
		Name:        "Test Project 003",
		Description: "This project is used to test the concurrent insert of user duplicates.",
	}

	createResponse, err := ServerEndpoints.project.CreateProject(context.Background(), createRequest)
	if err != nil {
		log.Fatalln(err.Error())
	}

	projectID := uuid.MustParse(createResponse.Id)

	for i := 0; i < 4; i++ {
		go addUsersToProject(t, projectID)
	}
}

func addUsersToProject(t *testing.T, projectID uuid.UUID) {
	const concurrentUserId01 string = "0a959fc8-151b-4818-a8e7-8f4325d1094e"
	const concurrentUserId02 string = "cc183d5f-ec0d-410e-be88-71f8d55de39f"
	const concurrentUserId03 string = "03747818-99b5-4828-a953-85174c1f1010"

	scope := []v1storagemodels.Right{v1storagemodels.Right(v1storagemodels.Right_RIGHT_READ)}
	addConcurrentUserResponse01, err := ServerEndpoints.project.AddUserToProject(
		context.Background(),
		&v1storageservices.AddUserToProjectRequest{
			UserId:    concurrentUserId01,
			Scope:     scope,
			ProjectId: projectID.String(),
		})

	if addConcurrentUserResponse01 != nil {
		assert.NotNil(t, addConcurrentUserResponse01)
		assert.Nil(t, err)
	} else {
		assert.Contains(t, err.Error(), "23505")
	}

	scope = []v1storagemodels.Right{v1storagemodels.Right(v1storagemodels.Right_RIGHT_WRITE)}
	addConcurrentUserResponse02, err := ServerEndpoints.project.AddUserToProject(
		context.Background(),
		&v1storageservices.AddUserToProjectRequest{
			UserId:    concurrentUserId02,
			Scope:     scope,
			ProjectId: projectID.String(),
		})

	if addConcurrentUserResponse02 != nil {
		assert.NotNil(t, addConcurrentUserResponse02)
		assert.Nil(t, err)
	} else {
		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), "23505")
	}

	scope = []v1storagemodels.Right{v1storagemodels.Right(v1storagemodels.Right_RIGHT_WRITE)}
	addConcurrentUserResponse03, err := ServerEndpoints.project.AddUserToProject(
		context.Background(),
		&v1storageservices.AddUserToProjectRequest{
			UserId:    concurrentUserId03,
			Scope:     scope,
			ProjectId: projectID.String(),
		})

	if addConcurrentUserResponse03 != nil {
		assert.NotNil(t, addConcurrentUserResponse03)
		assert.Nil(t, err)
	} else {
		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), "23505")
	}

	projectUsers, err := ServerEndpoints.project.ReadHandler.GetProjectUsers(projectID)
	if err != nil {
		log.Fatalln(err.Error())
	}
	assert.Equal(t, 4, len(projectUsers))
}
