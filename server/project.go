package server

import (
	"context"

	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"

	protoModels "github.com/ScienceObjectsDB/go-api/api/models/v1"
	services "github.com/ScienceObjectsDB/go-api/api/services/v1"
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
func (endpoint *ProjectEndpoints) CreateProject(ctx context.Context, request *services.CreateProjectRequest) (*services.CreateProjectResponse, error) {
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

	response := &services.CreateProjectResponse{
		Id: projectID,
	}

	return response, nil
}

//AddUserToProject Adds a new user to a given project
func (endpoint *ProjectEndpoints) AddUserToProject(ctx context.Context, request *services.AddUserToProjectRequest) (*services.AddUserToProjectResponse, error) {
	projectID, err := uuid.Parse(request.GetProjectId())
	if err != nil {
		log.Debug(err.Error())
		return nil, status.Error(codes.InvalidArgument, "could not parse ID")
	}

	metadata, _ := metadata.FromIncomingContext(ctx)

	err = endpoint.AuthzHandler.Authorize(
		projectID,
		protoModels.Right_READ,
		metadata)
	if err != nil {
		log.Println(err.Error())
		return nil, err
	}

	err = endpoint.CreateHandler.AddUserToProject(request)
	if err != nil {
		log.Println(err.Error())
		return nil, err
	}

	response := &services.AddUserToProjectResponse{}

	return response, nil
}

func (endpoint *ProjectEndpoints) CreateAPIToken(ctx context.Context, request *services.CreateAPITokenRequest) (*services.CreateAPITokenResponse, error) {
	requestID, err := uuid.Parse(request.GetId())
	if err != nil {
		log.Debug(err.Error())
		return nil, status.Error(codes.InvalidArgument, "could not parse ID")
	}

	metadata, _ := metadata.FromIncomingContext(ctx)

	err = endpoint.AuthzHandler.Authorize(
		requestID,
		protoModels.Right_WRITE,
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

	response := &services.CreateAPITokenResponse{
		Token: &protoModels.APIToken{
			Token:     token,
			ProjectId: request.GetId(),
			Rights: []protoModels.Right{
				protoModels.Right_READ,
				protoModels.Right_WRITE,
			},
		},
	}

	return response, nil
}

//GetProjectDatasets Returns all datasets that belong to a certain project
func (endpoint *ProjectEndpoints) GetProjectDatasets(ctx context.Context, request *services.GetProjectDatasetsRequest) (*services.GetProjectDatasetsResponse, error) {
	requestID, err := uuid.Parse(request.GetId())
	if err != nil {
		log.Debug(err.Error())
		return nil, status.Error(codes.InvalidArgument, "could not parse ID")
	}

	metadata, _ := metadata.FromIncomingContext(ctx)

	err = endpoint.AuthzHandler.Authorize(
		requestID,
		protoModels.Right_READ,
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

	var protoDatasets []*protoModels.Dataset
	for _, dataset := range datasets {
		protoDataset := dataset.ToProtoModel()
		protoDatasets = append(protoDatasets, &protoDataset)
	}

	response := &services.GetProjectDatasetsResponse{
		Dataset: protoDatasets,
	}

	return response, nil
}

//GetUserProjects Returns all projects that a specified user has access to
func (endpoint *ProjectEndpoints) GetUserProjects(ctx context.Context, request *services.GetUserProjectsRequest) (*services.GetUserProjectsResponse, error) {
	metadata, _ := metadata.FromIncomingContext(ctx)

	userOauth2ID, err := endpoint.AuthzHandler.GetUserID(metadata)
	if err != nil {
		log.Println(err.Error())
		return nil, err
	}

	projects, err := endpoint.ReadHandler.GetUserProjects(userOauth2ID.String())
	if err != nil {
		log.Println(err.Error())
		return nil, err
	}

	var protoProjects []*protoModels.Project

	for _, project := range projects {
		protoProjects = append(protoProjects, project.ToProtoModel())
	}

	response := &services.GetUserProjectsResponse{
		Projects: protoProjects,
	}

	return response, nil
}

func (endpoint *ProjectEndpoints) GetProject(ctx context.Context, request *services.GetProjectRequest) (*services.GetProjectResponse, error) {
	requestID, err := uuid.Parse(request.GetId())
	if err != nil {
		log.Debug(err.Error())
		return nil, status.Error(codes.InvalidArgument, "could not parse ID")
	}

	metadata, _ := metadata.FromIncomingContext(ctx)

	err = endpoint.AuthzHandler.Authorize(
		requestID,
		protoModels.Right_READ,
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

	protoProject := project.ToProtoModel()

	response := services.GetProjectResponse{
		Project: protoProject,
	}

	return &response, nil
}

func (endpoint *ProjectEndpoints) GetAPIToken(ctx context.Context, request *services.GetAPITokenRequest) (*services.GetAPITokenResponse, error) {
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

	var protoTokens []*protoModels.APIToken
	for _, token := range tokens {
		protoToken := token.ToProtoModel()
		protoTokens = append(protoTokens, protoToken)
	}

	response := &services.GetAPITokenResponse{
		Token: protoTokens,
	}

	return response, nil
}

//DeleteProject Deletes a specific project
//Will also delete all associated resources (Datasets/Objects/etc...) both from objects storage and the database
func (endpoint *ProjectEndpoints) DeleteProject(ctx context.Context, request *services.DeleteProjectRequest) (*services.DeleteProjectResponse, error) {
	requestID, err := uuid.Parse(request.GetId())
	if err != nil {
		log.Debug(err.Error())
		return nil, status.Error(codes.InvalidArgument, "could not parse ID")
	}

	metadata, _ := metadata.FromIncomingContext(ctx)

	err = endpoint.AuthzHandler.Authorize(
		requestID,
		protoModels.Right_WRITE,
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

	err = endpoint.ObjectHandler.DeleteObjects(objects)
	if err != nil {
		log.Println(err.Error())
		return nil, err
	}

	err = endpoint.DeleteHandler.DeleteProject(requestID)
	if err != nil {
		log.Println(err.Error())
		return nil, err
	}

	return &services.DeleteProjectResponse{}, nil
}

func (endpoint *ProjectEndpoints) DeleteAPIToken(ctx context.Context, request *services.DeleteAPITokenRequest) (*services.DeleteAPITokenResponse, error) {
	requestID, err := uuid.Parse(request.GetId())
	if err != nil {
		log.Debug(err.Error())
		return nil, status.Error(codes.InvalidArgument, "could not parse ID")
	}

	metadata, _ := metadata.FromIncomingContext(ctx)

	err = endpoint.AuthzHandler.Authorize(
		requestID,
		protoModels.Right_WRITE,
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

	return &services.DeleteAPITokenResponse{}, nil
}
