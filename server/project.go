package server

import (
	"context"

	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"

	"github.com/ScienceObjectsDB/CORE-Server/models"
	v1notficationservices "github.com/ScienceObjectsDB/go-api/sciobjsdb/api/notification/services/v1"
	v1storagemodels "github.com/ScienceObjectsDB/go-api/sciobjsdb/api/storage/models/v1"
	v1storageservices "github.com/ScienceObjectsDB/go-api/sciobjsdb/api/storage/services/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// Implements the gRPC project serivce.
type ProjectEndpoints struct {
	*Endpoints
}

// NewProjectEndpoints Returns a new ProjectEndpoint service
func NewProjectEndpoints(endpoints *Endpoints) (*ProjectEndpoints, error) {
	projectEndpoint := &ProjectEndpoints{
		Endpoints: endpoints,
	}

	return projectEndpoint, nil
}

//CreateProject creates a new projects
func (endpoint *ProjectEndpoints) CreateProject(ctx context.Context, request *v1storageservices.CreateProjectRequest) (*v1storageservices.CreateProjectResponse, error) {
	metadata, _ := metadata.FromIncomingContext(ctx)

	if err := endpoint.AuthzHandler.AuthorizeCreateProject(metadata); err != nil {
		log.Println(err.Error())
		return nil, err
	}

	userID, err := endpoint.AuthzHandler.GetUserID(metadata)
	if err != nil {
		log.Println(err.Error())
		return nil, err
	}

	projectID, err := endpoint.CreateHandler.CreateProject(request, userID.String())
	if err != nil {
		log.Println(err.Error())
		return nil, err
	}

	response := &v1storageservices.CreateProjectResponse{
		Id: projectID,
	}

	err = endpoint.EventStreamMgmt.PublishMessage(&v1notficationservices.EventNotificationMessage{
		Resource:    v1storagemodels.Resource(v1notficationservices.CreateEventStreamingGroupRequest_EVENT_RESOURCES_PROJECT_RESOURCE),
		ResourceId:  projectID,
		UpdatedType: v1notficationservices.EventNotificationMessage_UPDATE_TYPE_CREATED,
	})

	if err != nil {
		log.Errorln(err.Error())
		return nil, status.Error(codes.Internal, "could not publish notification event")
	}

	return response, nil
}

//AddUserToProject Adds a new user to a given project
func (endpoint *ProjectEndpoints) AddUserToProject(ctx context.Context, request *v1storageservices.AddUserToProjectRequest) (*v1storageservices.AddUserToProjectResponse, error) {
	projectID, err := uuid.Parse(request.GetProjectId())
	if err != nil {
		log.Debug(err.Error())
		return nil, status.Error(codes.InvalidArgument, "could not parse ID")
	}

	metadata, _ := metadata.FromIncomingContext(ctx)

	err = endpoint.AuthzHandler.Authorize(
		projectID,
		v1storagemodels.Right_RIGHT_READ,
		metadata)
	if err != nil {
		log.Println(err.Error())
		return nil, err
	}

	users, err := endpoint.ReadHandler.GetProjectUsers(projectID)
	if err != nil {
		log.Errorln(err.Error())
		return nil, err
	}

	for _, user := range users {
		if user.UserOauth2ID == request.UserId && user.ProjectID == projectID {
			err := status.Error(codes.AlreadyExists, "User already assigned to this project.")
			log.Errorln(err.Error())
			return nil, err
		}
	}

	err = endpoint.CreateHandler.AddUserToProject(request)
	if err != nil {
		log.Println(err.Error())
		return nil, err
	}

	response := &v1storageservices.AddUserToProjectResponse{}

	return response, nil
}

func (endpoint *ProjectEndpoints) CreateAPIToken(ctx context.Context, request *v1storageservices.CreateAPITokenRequest) (*v1storageservices.CreateAPITokenResponse, error) {
	requestID, err := uuid.Parse(request.GetId())
	if err != nil {
		log.Debug(err.Error())
		return nil, status.Error(codes.InvalidArgument, "could not parse ID")
	}

	metadata, _ := metadata.FromIncomingContext(ctx)

	err = endpoint.AuthzHandler.Authorize(
		requestID,
		v1storagemodels.Right_RIGHT_WRITE,
		metadata)
	if err != nil {
		log.Println(err.Error())
		return nil, err
	}

	userID, err := endpoint.AuthzHandler.GetUserID(metadata)
	if err != nil {
		log.Println(err.Error())
		return nil, err
	}

	token, err := endpoint.CreateHandler.CreateAPIToken(request, userID.String())
	if err != nil {
		log.Println(err.Error())
		return nil, err
	}

	response := &v1storageservices.CreateAPITokenResponse{
		Token: &v1storagemodels.APIToken{
			Token:     token,
			ProjectId: request.GetId(),
			Rights: []v1storagemodels.Right{
				v1storagemodels.Right_RIGHT_READ,
				v1storagemodels.Right_RIGHT_WRITE,
			},
		},
	}

	return response, nil
}

//GetProjectDatasets Returns all datasets that belong to a certain project
func (endpoint *ProjectEndpoints) GetProjectDatasets(ctx context.Context, request *v1storageservices.GetProjectDatasetsRequest) (*v1storageservices.GetProjectDatasetsResponse, error) {
	requestID, err := uuid.Parse(request.GetId())
	if err != nil {
		log.Debug(err.Error())
		return nil, status.Error(codes.InvalidArgument, "could not parse ID")
	}

	metadata, _ := metadata.FromIncomingContext(ctx)

	err = endpoint.AuthzHandler.Authorize(
		requestID,
		v1storagemodels.Right_RIGHT_READ,
		metadata)
	if err != nil {
		log.Println(err.Error())
		return nil, err
	}

	datasets, err := endpoint.ReadHandler.GetProjectDatasets(requestID)
	if err != nil {
		log.Println(err.Error())
		return nil, err
	}

	var protoDatasets []*v1storagemodels.Dataset
	for _, dataset := range datasets {
		stats, err := endpoint.StatsHandler.GetDatasetStats(dataset.ID)
		if err != nil {
			log.Errorln(err.Error())
			return nil, status.Error(codes.Internal, "error while reading dataset statistics")
		}

		protoDataset, err := dataset.ToProtoModel(stats)
		if err != nil {
			log.Errorln(err.Error())
			return nil, status.Error(codes.Internal, "could not create dataset protobuf representation")
		}
		protoDatasets = append(protoDatasets, protoDataset)
	}

	response := &v1storageservices.GetProjectDatasetsResponse{
		Datasets: protoDatasets,
	}

	return response, nil
}

//GetUserProjects Returns all projects that a specified user has access to
func (endpoint *ProjectEndpoints) GetUserProjects(ctx context.Context, request *v1storageservices.GetUserProjectsRequest) (*v1storageservices.GetUserProjectsResponse, error) {
	metadata, _ := metadata.FromIncomingContext(ctx)

	userOauth2ID, err := endpoint.AuthzHandler.GetUserID(metadata)
	if err != nil {
		log.Errorln(err.Error())
		return nil, err
	}

	projects, err := endpoint.ReadHandler.GetUserProjects(userOauth2ID.String())
	if err != nil {
		log.Errorln(err.Error())
		return nil, err
	}

	var protoProjects []*v1storagemodels.Project

	for _, project := range projects {
		stats, err := endpoint.StatsHandler.GetProjectStats(project.ID)
		if err != nil {
			log.Errorln(err.Error())
			return nil, err
		}

		project, err := project.ToProtoModel(stats)
		if err != nil {
			log.Errorln(err.Error())
			return nil, status.Error(codes.Internal, "could not create project protobuf representation")
		}

		protoProjects = append(protoProjects, project)
	}

	response := &v1storageservices.GetUserProjectsResponse{
		Projects: protoProjects,
	}

	return response, nil
}

func (endpoint *ProjectEndpoints) GetProject(ctx context.Context, request *v1storageservices.GetProjectRequest) (*v1storageservices.GetProjectResponse, error) {
	requestID, err := uuid.Parse(request.GetId())
	if err != nil {
		log.Debug(err.Error())
		return nil, status.Error(codes.InvalidArgument, "could not parse ID")
	}

	metadata, _ := metadata.FromIncomingContext(ctx)

	err = endpoint.AuthzHandler.Authorize(
		requestID,
		v1storagemodels.Right_RIGHT_READ,
		metadata)
	if err != nil {
		log.Println(err.Error())
		return nil, err
	}

	project, err := endpoint.ReadHandler.GetProject(requestID)
	if err != nil {
		log.Println(err.Error())
		return nil, err
	}

	stats, err := endpoint.StatsHandler.GetProjectStats(requestID)
	if err != nil {
		log.Errorln(err.Error())
		return nil, err
	}

	protoProject, err := project.ToProtoModel(stats)
	if err != nil {
		log.Errorln(err.Error())
		return nil, status.Error(codes.Internal, "could not create project protobuf representation")
	}

	response := v1storageservices.GetProjectResponse{
		Project: protoProject,
	}

	return &response, nil
}

func (endpoint *ProjectEndpoints) GetAPIToken(ctx context.Context, request *v1storageservices.GetAPITokenRequest) (*v1storageservices.GetAPITokenResponse, error) {
	metadata, _ := metadata.FromIncomingContext(ctx)
	userID, err := endpoint.AuthzHandler.GetUserID(metadata)
	if err != nil {
		log.Println(err.Error())
		return nil, err
	}

	tokens, err := endpoint.ReadHandler.GetAPIToken(userID)
	if err != nil {
		log.Println(err.Error())
		return nil, err
	}

	var protoTokens []*v1storagemodels.APIToken
	for _, token := range tokens {
		protoToken := token.ToProtoModel()
		protoTokens = append(protoTokens, protoToken)
	}

	response := &v1storageservices.GetAPITokenResponse{
		Token: protoTokens,
	}

	return response, nil
}

//DeleteProject Deletes a specific project
//Will also delete all associated resources (Datasets/Objects/etc...) both from objects storage and the database
func (endpoint *ProjectEndpoints) DeleteProject(ctx context.Context, request *v1storageservices.DeleteProjectRequest) (*v1storageservices.DeleteProjectResponse, error) {
	requestID, err := uuid.Parse(request.GetId())
	if err != nil {
		log.Debug(err.Error())
		return nil, status.Error(codes.InvalidArgument, "could not parse ID")
	}

	metadata, _ := metadata.FromIncomingContext(ctx)

	err = endpoint.AuthzHandler.Authorize(
		requestID,
		v1storagemodels.Right_RIGHT_WRITE,
		metadata)
	if err != nil {
		log.Println(err.Error())
		return nil, err
	}

	objects, err := endpoint.ReadHandler.GetAllProjectObjects(requestID)
	if err != nil {
		log.Println(err.Error())
		return nil, err
	}

	var locations []*models.Location
	for _, object := range objects {
		locations = append(locations, &object.Locations[0])
	}

	err = endpoint.ObjectHandler.DeleteObjects(locations)
	if err != nil {
		log.Println(err.Error())
		return nil, err
	}

	err = endpoint.EventStreamMgmt.PublishMessage(&v1notficationservices.EventNotificationMessage{
		Resource:    v1storagemodels.Resource_RESOURCE_PROJECT,
		ResourceId:  request.GetId(),
		UpdatedType: v1notficationservices.EventNotificationMessage_UPDATE_TYPE_DELETED,
	})

	if err != nil {
		log.Println(err.Error())
		return nil, err
	}

	err = endpoint.DeleteHandler.DeleteProject(requestID)
	if err != nil {
		log.Println(err.Error())
		return nil, err
	}

	return &v1storageservices.DeleteProjectResponse{}, nil
}

func (endpoint *ProjectEndpoints) DeleteAPIToken(ctx context.Context, request *v1storageservices.DeleteAPITokenRequest) (*v1storageservices.DeleteAPITokenResponse, error) {
	requestID, err := uuid.Parse(request.GetId())
	if err != nil {
		log.Debug(err.Error())
		return nil, status.Error(codes.InvalidArgument, "could not parse ID")
	}

	metadata, _ := metadata.FromIncomingContext(ctx)

	err = endpoint.AuthzHandler.Authorize(
		requestID,
		v1storagemodels.Right_RIGHT_WRITE,
		metadata)
	if err != nil {
		log.Println(err.Error())
		return nil, err
	}

	err = endpoint.DeleteHandler.DeleteAPIToken(requestID)
	if err != nil {
		log.Println(err.Error())
		return nil, err
	}

	return &v1storageservices.DeleteAPITokenResponse{}, nil
}
