package authz

import (
	protoModels "github.com/ScienceObjectsDB/go-api/api/models/v1"
	"github.com/google/uuid"
	"google.golang.org/grpc/metadata"
)

type TestHandler struct {
}

func (projectHandler *TestHandler) GetUserID(metadata metadata.MD) (uuid.UUID, error) {
	return uuid.UUID{}, nil
}

func (projectHandler *TestHandler) Authorize(projectID uuid.UUID, requestedRight protoModels.Right, metadata metadata.MD) error {
	return nil
}

func (projectHandler *TestHandler) AuthorizeCreateProject(metadata metadata.MD) error {
	return nil
}
