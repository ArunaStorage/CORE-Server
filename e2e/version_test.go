package e2e

import (
	"context"
	"fmt"
	"testing"

	log "github.com/sirupsen/logrus"

	v1storagemodels "github.com/ScienceObjectsDB/go-api/sciobjsdb/api/storage/models/v1"
	v1storageservices "github.com/ScienceObjectsDB/go-api/sciobjsdb/api/storage/services/v1"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

func TestDatasetVersion(t *testing.T) {
	createProjectRequest := &v1storageservices.CreateProjectRequest{
		Name:        "Test DatasetVersion - Project 001",
		Description: "Project containing a dataset to test the lifecycle of a dataset version.",
	}

	createResponse, err := ServerEndpoints.project.CreateProject(context.Background(), createProjectRequest)
	if err != nil {
		log.Fatalln(err.Error())
	}

	createDatasetRequest := &v1storageservices.CreateDatasetRequest{
		Name:      "Test DatasetVersion - Dataset 001",
		ProjectId: createResponse.GetId(),
	}

	datasetCreateResponse, err := ServerEndpoints.dataset.CreateDataset(context.Background(), createDatasetRequest)
	if err != nil {
		log.Fatalln(err.Error())
	}

	objects1, err := UploadObjects(ServerEndpoints.load, ServerEndpoints.object, 3, datasetCreateResponse.GetId(), "versiontest-")
	if err != nil {
		log.Fatalln(err.Error())
	}

	objectgroupuuidname := uuid.New().String()

	createObjectGroupRequest := &v1storageservices.CreateObjectGroupRequest{
		CreateRevisionRequest: &v1storageservices.CreateObjectGroupRevisionRequest{
			Name:              objectgroupuuidname,
			UpdateMetaObjects: &v1storageservices.UpdateObjectsRequests{},
			UpdateObjects: &v1storageservices.UpdateObjectsRequests{
				AddObjects: []*v1storageservices.AddObjectRequest{
					&v1storageservices.AddObjectRequest{Id: objects1[0].ID.String()},
					&v1storageservices.AddObjectRequest{Id: objects1[1].ID.String()},
					&v1storageservices.AddObjectRequest{Id: objects1[2].ID.String()},
				},
			},
		},
		DatasetId: datasetCreateResponse.GetId(),
	}

	createObjectGroupResponse, err := ServerEndpoints.object.CreateObjectGroup(context.Background(), createObjectGroupRequest)
	if err != nil {
		log.Fatalln(err.Error())
	}

	objects2, err := UploadObjects(ServerEndpoints.load, ServerEndpoints.object, 3, datasetCreateResponse.GetId(), "versiontest-")
	if err != nil {
		log.Fatalln(err.Error())
	}

	objectgroupuuidname2 := uuid.New().String()

	createObjectGroupRequest2 := &v1storageservices.CreateObjectGroupRequest{
		CreateRevisionRequest: &v1storageservices.CreateObjectGroupRevisionRequest{
			Name:              objectgroupuuidname2,
			UpdateMetaObjects: &v1storageservices.UpdateObjectsRequests{},
			UpdateObjects: &v1storageservices.UpdateObjectsRequests{
				AddObjects: []*v1storageservices.AddObjectRequest{
					&v1storageservices.AddObjectRequest{Id: objects2[0].ID.String()},
					&v1storageservices.AddObjectRequest{Id: objects2[1].ID.String()},
					&v1storageservices.AddObjectRequest{Id: objects2[2].ID.String()},
				},
			},
		},
		DatasetId: datasetCreateResponse.GetId(),
	}

	createObjectGroupResponse2, err := ServerEndpoints.object.CreateObjectGroup(context.Background(), createObjectGroupRequest2)
	if err != nil {
		log.Fatalln(err.Error())
	}

	_, err = ServerEndpoints.object.FinishObjectGroupRevisionUpload(context.Background(), &v1storageservices.FinishObjectGroupRevisionUploadRequest{
		Id: createObjectGroupResponse.CreateRevisionResponse.GetId(),
	})
	if err != nil {
		log.Fatalln(err.Error())
	}

	assert.NotEqual(t, createObjectGroupResponse.ObjectGroupId, 0)

	getObjectGroupResponse, err := ServerEndpoints.object.GetObjectGroup(context.Background(), &v1storageservices.GetObjectGroupRequest{
		Id: createObjectGroupResponse.ObjectGroupId,
	})
	if err != nil {
		log.Fatalln(err.Error())
	}

	versionLabel := []*v1storagemodels.Label{
		{
			Key:   "Label1",
			Value: "LabelValue1",
		},
		{
			Key:   "Label2",
			Value: "LabelValue2",
		},
	}

	releaseVersionRequest := &v1storageservices.ReleaseDatasetVersionRequest{
		Name:      "Dataset 001 Snapshot 1.0.2.1",
		DatasetId: datasetCreateResponse.GetId(),
		Version: &v1storagemodels.Version{
			Major:    1,
			Minor:    0,
			Patch:    2,
			Revision: 1,
			Stage:    v1storagemodels.Version_VERSION_STAGE_STABLE,
		},
		Description:            "Dataset 001 version release 001",
		ObjectGroupRevisionIds: []string{getObjectGroupResponse.ObjectGroup.CurrentRevision.Id},
		Labels:                 versionLabel,
	}

	versionResponse, err := ServerEndpoints.dataset.ReleaseDatasetVersion(context.Background(), releaseVersionRequest)
	if err != nil {
		log.Fatalln(err.Error())
	}

	releaseVersionRequest2 := &v1storageservices.ReleaseDatasetVersionRequest{
		Name:      "Dataset 001 Snapshot 1.0.2.2",
		DatasetId: datasetCreateResponse.GetId(),
		Version: &v1storagemodels.Version{
			Major:    1,
			Minor:    0,
			Patch:    2,
			Revision: 2,
			Stage:    v1storagemodels.Version_VERSION_STAGE_STABLE,
		},
		Description:            "Dataset 001 version release 002",
		ObjectGroupRevisionIds: []string{getObjectGroupResponse.ObjectGroup.CurrentRevision.Id, createObjectGroupResponse2.CreateRevisionResponse.GetId()},
		Labels:                 versionLabel,
	}

	version2Response, _ := ServerEndpoints.dataset.ReleaseDatasetVersion(context.Background(), releaseVersionRequest2)
	err = nil

	datasetVersions, err := ServerEndpoints.dataset.GetDatasetVersions(context.Background(), &v1storageservices.GetDatasetVersionsRequest{
		Id: datasetCreateResponse.GetId(),
	})
	if err != nil {
		log.Fatalln(err.Error())
	}

	assert.Equal(t, 2, len(datasetVersions.GetDatasetVersions()))

	datasetVersion, err := ServerEndpoints.dataset.GetDatasetVersion(context.Background(), &v1storageservices.GetDatasetVersionRequest{
		Id: versionResponse.GetId(),
	})
	if err != nil {
		log.Fatalln(err.Error())
	}

	assert.Equal(t, int64(3), datasetVersion.GetDatasetVersion().GetStats().GetObjectCount())
	assert.Equal(t, int64(57), datasetVersion.GetDatasetVersion().GetStats().GetAccSize())
	assert.Equal(t, float64(19), datasetVersion.GetDatasetVersion().GetStats().GetAvgObjectSize())

	versionRevisions, err := ServerEndpoints.dataset.GetDatasetVersionObjectGroups(context.Background(), &v1storageservices.GetDatasetVersionObjectGroupsRequest{
		Id: versionResponse.Id,
	})
	if err != nil {
		log.Fatalln(err.Error())
	}

	assert.Equal(t, 1, len(versionRevisions.GetObjectGroupRevisions()))

	// Delete latest dataset version
	_, err = ServerEndpoints.dataset.DeleteDatasetVersion(context.Background(), &v1storageservices.DeleteDatasetVersionRequest{
		Id: version2Response.Id,
	})
	if err != nil {
		log.Fatalln(err.Error())
	}

	// Validate dataset version deletion
	datasetVersion, err = ServerEndpoints.dataset.GetDatasetVersion(context.Background(), &v1storageservices.GetDatasetVersionRequest{
		Id: version2Response.GetId(),
	})

	assert.NotNil(t, err)
	assert.Nil(t, datasetVersion)

	datasetVersions, err = ServerEndpoints.dataset.GetDatasetVersions(context.Background(), &v1storageservices.GetDatasetVersionsRequest{
		Id: datasetCreateResponse.GetId(),
	})
	if err != nil {
		log.Fatalln(err.Error())
	}

	assert.Equal(t, 1, len(datasetVersions.GetDatasetVersions()))

	// Delete created Dataset
	_, err = ServerEndpoints.dataset.DeleteDataset(context.Background(), &v1storageservices.DeleteDatasetRequest{
		Id: datasetCreateResponse.GetId(),
	})
	if err != nil {
		log.Fatalln(err.Error())
	}

	// Validating Project deletion by trying to get deleted Project which should fail and return nil
	nilResponse, err := ServerEndpoints.dataset.GetDataset(context.Background(), &v1storageservices.GetDatasetRequest{
		Id: datasetCreateResponse.Id,
	})

	assert.NotNil(t, err)
	assert.Nil(t, nilResponse)
}

func TestDatasetVersionPaginated(t *testing.T) {
	createProjectRequest := &v1storageservices.CreateProjectRequest{
		Name:        "Test DatasetVersion - Project 002",
		Description: "Project containing a dataset to test the dataset version pagination.",
	}

	createResponse, err := ServerEndpoints.project.CreateProject(context.Background(), createProjectRequest)
	if err != nil {
		log.Fatalln(err.Error())
	}

	createDatasetRequest := &v1storageservices.CreateDatasetRequest{
		Name:      "testdataset",
		ProjectId: createResponse.GetId(),
	}

	datasetCreateResponse, err := ServerEndpoints.dataset.CreateDataset(context.Background(), createDatasetRequest)
	if err != nil {
		log.Fatalln(err.Error())
	}

	var objectIDs []string
	for i := 0; i < 10; i++ {
		createObjectGroup := &v1storageservices.CreateObjectGroupRequest{
			CreateRevisionRequest: &v1storageservices.CreateObjectGroupRevisionRequest{
				Name:              fmt.Sprintf("foo-%v", i),
				Description:       "foo",
				UpdateObjects:     &v1storageservices.UpdateObjectsRequests{},
				UpdateMetaObjects: &v1storageservices.UpdateObjectsRequests{},
			},
			DatasetId: datasetCreateResponse.GetId(),
		}

		object, err := ServerEndpoints.object.CreateObjectGroup(context.Background(), createObjectGroup)
		if err != nil {
			log.Fatalln(err.Error())
		}

		objectIDs = append(objectIDs, object.CreateRevisionResponse.GetId())
	}

	handledObjectGroups := make(map[string]struct{})

	versionID, err := ServerEndpoints.dataset.CreateHandler.CreateDatasetVersion(&v1storageservices.ReleaseDatasetVersionRequest{
		Name:                   "foo",
		DatasetId:              datasetCreateResponse.GetId(),
		ObjectGroupRevisionIds: objectIDs,
		Version:                &v1storagemodels.Version{},
	}, uuid.MustParse(createResponse.GetId()))
	if err != nil {
		log.Fatalln(err.Error())
	}

	objectGroups1, err := ServerEndpoints.dataset.ReadHandler.GetDatasetVersionWithObjectGroups(versionID, &v1storagemodels.PageRequest{
		LastUuid: "",
		PageSize: 4,
	})
	if err != nil {
		log.Fatalln(err.Error())
	}

	assert.Equal(t, 4, len(objectGroups1.ObjectGroupRevisions))

	var lastUUID uuid.UUID

	for _, objectGroup := range objectGroups1.ObjectGroupRevisions {
		if _, ok := handledObjectGroups[objectGroup.Name]; !ok {
			handledObjectGroups[objectGroup.Name] = struct{}{}
			lastUUID = objectGroup.ID
		} else {
			log.Fatalln("found duplicate object group in pagination")
		}
	}

	objectGroups2, err := ServerEndpoints.dataset.ReadHandler.GetDatasetVersionWithObjectGroups(versionID, &v1storagemodels.PageRequest{
		LastUuid: lastUUID.String(),
		PageSize: 4,
	})
	if err != nil {
		log.Fatalln(err.Error())
	}

	assert.Equal(t, 4, len(objectGroups2.ObjectGroupRevisions))

	for _, objectGroup := range objectGroups2.ObjectGroupRevisions {
		if _, ok := handledObjectGroups[objectGroup.Name]; !ok {
			handledObjectGroups[objectGroup.Name] = struct{}{}
			lastUUID = objectGroup.ID
		} else {
			log.Fatalln("found duplicate object group in pagination")
		}
	}

	objectGroups3, err := ServerEndpoints.dataset.ReadHandler.GetDatasetVersionWithObjectGroups(versionID, &v1storagemodels.PageRequest{
		LastUuid: lastUUID.String(),
		PageSize: 2,
	})
	if err != nil {
		log.Fatalln(err.Error())
	}

	assert.Equal(t, 2, len(objectGroups3.ObjectGroupRevisions))

	for _, objectGroup := range objectGroups3.ObjectGroupRevisions {
		if _, ok := handledObjectGroups[objectGroup.Name]; !ok {
			handledObjectGroups[objectGroup.Name] = struct{}{}
		} else {
			log.Fatalln("found duplicate object group in pagination")
		}
	}

	// Delete created Dataset
	_, err = ServerEndpoints.dataset.DeleteDataset(context.Background(), &v1storageservices.DeleteDatasetRequest{
		Id: datasetCreateResponse.GetId(),
	})
	if err != nil {
		log.Fatalln(err.Error())
	}

	// Validating dataset deletion by trying to get deleted dataset which should fail and return nil
	nilResponse, err := ServerEndpoints.dataset.GetDataset(context.Background(), &v1storageservices.GetDatasetRequest{
		Id: datasetCreateResponse.Id,
	})

	assert.NotNil(t, err)
	assert.Nil(t, nilResponse)
}

func TestDatasetVersionEmpty(t *testing.T) {
	createProjectRequest := &v1storageservices.CreateProjectRequest{
		Name:        "Test DatasetVersion - Project 003",
		Description: "Project containing a dataset to test an empty dataset version.",
	}

	createResponse, err := ServerEndpoints.project.CreateProject(context.Background(), createProjectRequest)
	if err != nil {
		log.Fatalln(err.Error())
	}

	datasetLabel := []*v1storagemodels.Label{
		{
			Key:   "Label1",
			Value: "LabelValue1",
		},
		{
			Key:   "Label2",
			Value: "LabelValue2",
		},
	}

	createDatasetRequest := &v1storageservices.CreateDatasetRequest{
		Name:      "testdataset",
		ProjectId: createResponse.GetId(),
		Labels:    datasetLabel,
	}

	datasetCreateResponse, err := ServerEndpoints.dataset.CreateDataset(context.Background(), createDatasetRequest)
	if err != nil {
		log.Fatalln(err.Error())
	}

	createVersionRequest := &v1storageservices.ReleaseDatasetVersionRequest{
		Name:      "testversion",
		DatasetId: datasetCreateResponse.GetId(),
		Version: &v1storagemodels.Version{
			Major:    0,
			Minor:    0,
			Patch:    1,
			Revision: 0,
			Stage:    v1storagemodels.Version_VERSION_STAGE_RC,
		},
	}

	versionCreateResponse, err := ServerEndpoints.dataset.ReleaseDatasetVersion(context.Background(), createVersionRequest)
	if err != nil {
		log.Fatalln(err.Error())
	}

	version, err := ServerEndpoints.dataset.GetDatasetVersion(context.Background(), &v1storageservices.GetDatasetVersionRequest{
		Id: versionCreateResponse.GetId(),
	})
	if err != nil {
		log.Fatalln(err.Error())
	}

	assert.Equal(t, int64(0), version.DatasetVersion.Stats.AccSize)
	assert.Equal(t, float64(0), version.DatasetVersion.Stats.AvgObjectSize)
	assert.Equal(t, int64(0), version.DatasetVersion.Stats.ObjectCount)
	assert.Equal(t, int64(0), version.DatasetVersion.Stats.ObjectGroupCount)

	// Delete created Dataset
	_, err = ServerEndpoints.dataset.DeleteDataset(context.Background(), &v1storageservices.DeleteDatasetRequest{
		Id: datasetCreateResponse.GetId(),
	})
	if err != nil {
		log.Fatalln(err.Error())
	}

	// Validating dataset deletion by trying to get deleted daatset which should fail and return nil
	nilResponse, err := ServerEndpoints.dataset.GetDataset(context.Background(), &v1storageservices.GetDatasetRequest{
		Id: datasetCreateResponse.Id,
	})

	assert.NotNil(t, err)
	assert.Nil(t, nilResponse)
}
