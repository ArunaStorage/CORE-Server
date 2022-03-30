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
		Name:        "testproject_dataset",
		Description: "test",
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

	objectGroupLabel := []*v1storagemodels.Label{
		{
			Key:   "Label1OG",
			Value: "LabelValue1OG",
		},
		{
			Key:   "Label2OG",
			Value: "LabelValue2OG",
		},
	}

	object1Label := []*v1storagemodels.Label{
		{
			Key:   "Label1O1",
			Value: "LabelValue1O1",
		},
		{
			Key:   "Label2O1",
			Value: "LabelValue2O1",
		},
	}

	object2Label := []*v1storagemodels.Label{
		{
			Key:   "Label1O2",
			Value: "LabelValue1O2",
		},
		{
			Key:   "Label2O2",
			Value: "LabelValue2O2",
		},
	}

	objectgroupuuidname := uuid.New().String()

	createObjectGroupRequest := &v1storageservices.CreateObjectGroupRequest{
		Name:      objectgroupuuidname,
		DatasetId: datasetCreateResponse.GetId(),
		Labels:    objectGroupLabel,
		Objects: []*v1storageservices.CreateObjectRequest{
			{
				Filename:   "testfile1",
				Filetype:   "bin",
				Labels:     object1Label,
				ContentLen: 3,
			},
			{
				Filename:   "testfile2",
				Filetype:   "bin",
				Labels:     object2Label,
				ContentLen: 3,
			},
		},
	}

	createObjectGroupResponse, err := ServerEndpoints.object.CreateObjectGroup(context.Background(), createObjectGroupRequest)
	if err != nil {
		log.Fatalln(err.Error())
	}

	objectgroupuuidname2 := uuid.New().String()

	createObjectGroupRequest2 := &v1storageservices.CreateObjectGroupRequest{
		Name:      objectgroupuuidname2,
		DatasetId: datasetCreateResponse.GetId(),
		Labels:    objectGroupLabel,
		Objects: []*v1storageservices.CreateObjectRequest{
			{
				Filename:   "testfile1",
				Filetype:   "bin",
				Labels:     object1Label,
				ContentLen: 3,
			},
			{
				Filename:   "testfile3",
				Filetype:   "bin",
				Labels:     object2Label,
				ContentLen: 3,
			},
		},
	}

	createObjectGroupResponse2, err := ServerEndpoints.object.CreateObjectGroup(context.Background(), createObjectGroupRequest2)
	if err != nil {
		log.Fatalln(err.Error())
	}

	_, err = ServerEndpoints.object.FinishObjectGroupUpload(context.Background(), &v1storageservices.FinishObjectGroupUploadRequest{
		Id: createObjectGroupResponse.ObjectGroupId,
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
		Name:      "foo",
		DatasetId: datasetCreateResponse.GetId(),
		Version: &v1storagemodels.Version{
			Major:    1,
			Minor:    0,
			Patch:    2,
			Revision: 1,
			Stage:    v1storagemodels.Version_VERSION_STAGE_STABLE,
		},
		Description:    "testrelease",
		ObjectGroupIds: []string{getObjectGroupResponse.ObjectGroup.Id},
		Labels:         versionLabel,
	}

	versionResponse, err := ServerEndpoints.dataset.ReleaseDatasetVersion(context.Background(), releaseVersionRequest)
	if err != nil {
		log.Fatalln(err.Error())
	}

	releaseVersionRequest2 := &v1storageservices.ReleaseDatasetVersionRequest{
		Name:      "foo",
		DatasetId: datasetCreateResponse.GetId(),
		Version: &v1storagemodels.Version{
			Major:    1,
			Minor:    0,
			Patch:    2,
			Revision: 1,
			Stage:    v1storagemodels.Version_VERSION_STAGE_STABLE,
		},
		Description:    "testrelease",
		ObjectGroupIds: []string{getObjectGroupResponse.ObjectGroup.Id, createObjectGroupResponse2.ObjectGroupId},
		Labels:         versionLabel,
	}

	_, err = ServerEndpoints.dataset.ReleaseDatasetVersion(context.Background(), releaseVersionRequest2)
	assert.Error(t, err)

	err = nil

	datasetVersions, err := ServerEndpoints.dataset.GetDatasetVersions(context.Background(), &v1storageservices.GetDatasetVersionsRequest{
		Id: datasetCreateResponse.GetId(),
	})
	if err != nil {
		log.Fatalln(err.Error())
	}

	assert.Equal(t, len(datasetVersions.GetDatasetVersions()), 1)

	datasetVersion, err := ServerEndpoints.dataset.GetDatasetVersion(context.Background(), &v1storageservices.GetDatasetVersionRequest{
		Id: versionResponse.GetId(),
	})
	if err != nil {
		log.Fatalln(err.Error())
	}

	assert.Equal(t, int64(2), datasetVersion.GetDatasetVersion().GetStats().GetObjectCount())
	assert.Equal(t, int64(6), datasetVersion.GetDatasetVersion().GetStats().GetAccSize())
	assert.Equal(t, float64(3), datasetVersion.GetDatasetVersion().GetStats().GetAvgObjectSize())

	versionRevisions, err := ServerEndpoints.dataset.GetDatasetVersionObjectGroups(context.Background(), &v1storageservices.GetDatasetVersionObjectGroupsRequest{
		Id: versionResponse.Id,
	})
	if err != nil {
		log.Fatalln(err.Error())
	}

	assert.Equal(t, 1, len(versionRevisions.GetObjectGroup()))

	//_, err = ServerEndpoints.dataset.DeleteDataset(context.Background(), &v1storageservices.DeleteDatasetRequest{
	//	Id: datasetCreateResponse.GetId(),
	//})
	//if err != nil {
	//	log.Fatalln(err.Error())
	//}
}

func TestDatasetVersionPaginated(t *testing.T) {
	createProjectRequest := &v1storageservices.CreateProjectRequest{
		Name:        "testproject_dataset",
		Description: "test",
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
			Name:        fmt.Sprintf("foo-%v", i),
			Description: "foo",
			DatasetId:   datasetCreateResponse.GetId(),
		}

		object, err := ServerEndpoints.object.CreateObjectGroup(context.Background(), createObjectGroup)
		if err != nil {
			log.Fatalln(err.Error())
		}

		objectIDs = append(objectIDs, object.ObjectGroupId)
	}

	handledObjectGroups := make(map[string]struct{})

	versionID, err := ServerEndpoints.dataset.CreateHandler.CreateDatasetVersion(&v1storageservices.ReleaseDatasetVersionRequest{
		Name:           "foo",
		DatasetId:      datasetCreateResponse.GetId(),
		ObjectGroupIds: objectIDs,
		Version:        &v1storagemodels.Version{},
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

	assert.Equal(t, 4, len(objectGroups1.ObjectGroups))

	var lastUUID uuid.UUID

	for _, objectGroup := range objectGroups1.ObjectGroups {
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

	assert.Equal(t, 4, len(objectGroups2.ObjectGroups))

	for _, objectGroup := range objectGroups2.ObjectGroups {
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

	assert.Equal(t, 2, len(objectGroups3.ObjectGroups))

	for _, objectGroup := range objectGroups3.ObjectGroups {
		if _, ok := handledObjectGroups[objectGroup.Name]; !ok {
			handledObjectGroups[objectGroup.Name] = struct{}{}
		} else {
			log.Fatalln("found duplicate object group in pagination")
		}
	}
}

func TestDatasetVersionEmpty(t *testing.T) {
	createProjectRequest := &v1storageservices.CreateProjectRequest{
		Name:        "testproject_dataset",
		Description: "test",
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

	assert.Equal(t, int64(0), version.DatasetVersion.Stats.AccSize)
	assert.Equal(t, float64(0), version.DatasetVersion.Stats.AvgObjectSize)
	assert.Equal(t, int64(0), version.DatasetVersion.Stats.ObjectCount)
	assert.Equal(t, int64(0), version.DatasetVersion.Stats.ObjectGroupCount)

}
