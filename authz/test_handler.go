package authz

import (
	protoModels "github.com/ScienceObjectsDB/go-api/api/models/v1"
	"google.golang.org/grpc/metadata"
)

type TestHandler struct {
}

func (projectHandler *TestHandler) GetUserID(metadata metadata.MD) (string, error) {
	return "testuser1", nil
}

func (projectHandler *TestHandler) Authorize(projectID uint, requestedRight protoModels.Right, metadata metadata.MD) error {
	return nil
}

func (projectHandler *TestHandler) AuthorizeCreateProject(metadata metadata.MD) error {
	return nil
}
