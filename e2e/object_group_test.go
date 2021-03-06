package e2e

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"testing"

	log "github.com/sirupsen/logrus"

	v1storagemodels "github.com/ScienceObjectsDB/go-api/sciobjsdb/api/storage/models/v1"
	v1storageservices "github.com/ScienceObjectsDB/go-api/sciobjsdb/api/storage/services/v1"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

func TestObjectGroup(t *testing.T) {
	projectID, err := ServerEndpoints.project.CreateProject(context.Background(), &v1storageservices.CreateProjectRequest{
		Name:        "testproject",
		Description: "test",
	})
	if err != nil {
		log.Fatalln(err.Error())
	}

	datasetID, err := ServerEndpoints.dataset.CreateDataset(context.Background(), &v1storageservices.CreateDatasetRequest{
		Name:        "testdataset",
		Description: "test",
		ProjectId:   projectID.GetId(),
	})

	dataObjects, err := UploadObjects(ServerEndpoints.load, ServerEndpoints.object, 5, datasetID.GetId(), "data")
	if err != nil {
		log.Fatalln(err.Error())
	}

	metaObjects, err := UploadObjects(ServerEndpoints.load, ServerEndpoints.object, 5, datasetID.GetId(), "meta")
	if err != nil {
		log.Fatalln(err.Error())
	}

	addDataRequest := make([]*v1storageservices.AddObjectRequest, 0)
	for _, dataObject := range dataObjects {
		addDataRequest = append(addDataRequest, &v1storageservices.AddObjectRequest{
			Id: dataObject.ID.String(),
		})
	}

	addMetaRequests := make([]*v1storageservices.AddObjectRequest, 0)
	for _, dataObject := range metaObjects {
		addMetaRequests = append(addMetaRequests, &v1storageservices.AddObjectRequest{
			Id: dataObject.ID.String(),
		})
	}

	objectGroupCreateRequest := &v1storageservices.CreateObjectGroupRequest{
		DatasetId: datasetID.GetId(),
		CreateRevisionRequest: &v1storageservices.CreateObjectGroupRevisionRequest{
			Name:              "testrevision",
			Description:       "revisiondescription",
			IncludeObjectLink: true,
			Labels: []*v1storagemodels.Label{
				&v1storagemodels.Label{Key: "testlabel1", Value: "testlabel1"},
				&v1storagemodels.Label{Key: "testlabel2", Value: "testlabel2"},
			},
			UpdateMetaObjects: &v1storageservices.UpdateObjectsRequests{
				AddObjects: addMetaRequests,
			},
			UpdateObjects: &v1storageservices.UpdateObjectsRequests{
				AddObjects: addDataRequest,
			},
		},
	}

	createObjectGroup, err := ServerEndpoints.object.CreateObjectGroup(context.Background(), objectGroupCreateRequest)
	if err != nil {
		log.Fatalln(err.Error())
	}

	objectGroup, err := ServerEndpoints.object.GetObjectGroup(context.Background(), &v1storageservices.GetObjectGroupRequest{
		Id: createObjectGroup.GetObjectGroupId(),
	})

	currentRevision := objectGroup.ObjectGroup.CurrentRevision

	assert.Equal(t, objectGroupCreateRequest.CreateRevisionRequest.Name, currentRevision.Name)
	assert.Equal(t, objectGroupCreateRequest.CreateRevisionRequest.Description, currentRevision.Description)
	assert.Equal(t, len(addDataRequest), len(currentRevision.Objects))
	assert.Equal(t, len(addMetaRequests), len(currentRevision.MetadataObjects))
}

func TestDatasetObjectsQuery(t *testing.T) {
	projectID, err := ServerEndpoints.project.CreateProject(context.Background(), &v1storageservices.CreateProjectRequest{
		Name:        "testproject",
		Description: "test",
	})
	if err != nil {
		log.Fatalln(err.Error())
	}

	datasetID, err := ServerEndpoints.dataset.CreateDataset(context.Background(), &v1storageservices.CreateDatasetRequest{
		Name:        "testdataset",
		Description: "test",
		ProjectId:   projectID.GetId(),
	})
	if err != nil {
		log.Fatalln(err.Error())
	}

	dataObjects, err := UploadObjects(ServerEndpoints.load, ServerEndpoints.object, 5, datasetID.GetId(), "data")
	if err != nil {
		log.Fatalln(err.Error())
	}

	queryProtoLabel := make([]*v1storagemodels.Label, 0)
	for _, label := range dataObjects[0].Labels {
		queryProtoLabel = append(queryProtoLabel, &v1storagemodels.Label{
			Key:   label.Key,
			Value: label.Value,
		})
	}

	response, err := ServerEndpoints.dataset.GetDatasetObjects(context.Background(), &v1storageservices.GetDatasetObjectsRequest{
		Id: datasetID.GetId(),
	})
	if err != nil {
		log.Fatalln(err.Error())
	}

	assert.Equal(t, 5, len(response.GetObjects()))

	responsePage1, err := ServerEndpoints.dataset.GetDatasetObjects(context.Background(), &v1storageservices.GetDatasetObjectsRequest{
		Id: datasetID.GetId(),
		PageRequest: &v1storagemodels.PageRequest{
			PageSize: 3,
		},
	})
	if err != nil {
		log.Fatalln(err.Error())
	}

	assert.Equal(t, 3, len(responsePage1.GetObjects()))

	responsePage2, err := ServerEndpoints.dataset.GetDatasetObjects(context.Background(), &v1storageservices.GetDatasetObjectsRequest{
		Id: datasetID.GetId(),
		PageRequest: &v1storagemodels.PageRequest{
			PageSize: 3,
			LastUuid: responsePage1.Objects[2].GetId(),
		},
	})
	if err != nil {
		log.Fatalln(err.Error())
	}

	assert.Equal(t, 2, len(responsePage2.GetObjects()))

	objects := make([]string, 0)
	for _, object := range responsePage1.Objects {
		assert.NotContains(t, objects, object.GetId())
		objects = append(objects, object.GetId())
	}

	for _, object := range responsePage2.Objects {
		assert.NotContains(t, objects, object.GetId())
		objects = append(objects, object.GetId())
	}

}

func TestDatasetObjectsQueryWithLabel(t *testing.T) {
	projectID, err := ServerEndpoints.project.CreateProject(context.Background(), &v1storageservices.CreateProjectRequest{
		Name:        "testproject",
		Description: "test",
	})
	if err != nil {
		log.Fatalln(err.Error())
	}

	datasetID, err := ServerEndpoints.dataset.CreateDataset(context.Background(), &v1storageservices.CreateDatasetRequest{
		Name:        "testdataset",
		Description: "test",
		ProjectId:   projectID.GetId(),
	})
	if err != nil {
		log.Fatalln(err.Error())
	}

	dataObjects, err := UploadObjects(ServerEndpoints.load, ServerEndpoints.object, 5, datasetID.GetId(), "data")
	if err != nil {
		log.Fatalln(err.Error())
	}

	queryProtoLabel := make([]*v1storagemodels.Label, 0)
	for _, label := range dataObjects[0].Labels {
		queryProtoLabel = append(queryProtoLabel, &v1storagemodels.Label{
			Key:   label.Key,
			Value: label.Value,
		})
	}

	response, err := ServerEndpoints.dataset.GetDatasetObjects(context.Background(), &v1storageservices.GetDatasetObjectsRequest{
		Id: datasetID.GetId(),
		LabelFilter: &v1storagemodels.LabelFilter{
			Labels: queryProtoLabel,
		},
	})
	if err != nil {
		log.Fatalln(err.Error())
	}

	assert.Equal(t, 1, len(response.GetObjects()))
}

func TestObjectGroupUpdate(t *testing.T) {
	projectID, err := ServerEndpoints.project.CreateProject(context.Background(), &v1storageservices.CreateProjectRequest{
		Name:        "testproject",
		Description: "test",
	})
	if err != nil {
		log.Fatalln(err.Error())
	}

	datasetID, err := ServerEndpoints.dataset.CreateDataset(context.Background(), &v1storageservices.CreateDatasetRequest{
		Name:        "testdataset",
		Description: "test",
		ProjectId:   projectID.GetId(),
	})
	if err != nil {
		log.Fatalln(err.Error())
	}

	dataObjects, err := UploadObjects(ServerEndpoints.load, ServerEndpoints.object, 5, datasetID.GetId(), "data")
	if err != nil {
		log.Fatalln(err.Error())
	}

	metaObjects, err := UploadObjects(ServerEndpoints.load, ServerEndpoints.object, 5, datasetID.GetId(), "meta")
	if err != nil {
		log.Fatalln(err.Error())
	}

	addDataRequest := make([]*v1storageservices.AddObjectRequest, 0)
	for _, dataObject := range dataObjects {
		addDataRequest = append(addDataRequest, &v1storageservices.AddObjectRequest{
			Id: dataObject.ID.String(),
		})
	}

	addMetaRequests := make([]*v1storageservices.AddObjectRequest, 0)
	for _, dataObject := range metaObjects {
		addMetaRequests = append(addMetaRequests, &v1storageservices.AddObjectRequest{
			Id: dataObject.ID.String(),
		})
	}

	objectGroupCreateRequest := &v1storageservices.CreateObjectGroupRequest{
		DatasetId: datasetID.GetId(),
		CreateRevisionRequest: &v1storageservices.CreateObjectGroupRevisionRequest{
			Name:        "revision-1",
			Description: "revision-1-description",
			UpdateObjects: &v1storageservices.UpdateObjectsRequests{
				AddObjects: addDataRequest,
			},
			UpdateMetaObjects: &v1storageservices.UpdateObjectsRequests{
				AddObjects: addMetaRequests,
			},
		},
	}

	objectGroup, err := ServerEndpoints.object.CreateObjectGroup(context.Background(), objectGroupCreateRequest)
	if err != nil {
		log.Fatalln(err.Error())
	}

	objectGroupFromGet, err := ServerEndpoints.object.GetObjectGroup(context.Background(), &v1storageservices.GetObjectGroupRequest{
		Id: objectGroup.ObjectGroupId,
	})

	assert.Equal(t, objectGroup.CreateRevisionResponse.GetId(), objectGroupFromGet.ObjectGroup.CurrentRevision.Id)
	assert.Equal(t, objectGroupCreateRequest.CreateRevisionRequest.GetName(), objectGroupFromGet.ObjectGroup.CurrentRevision.GetName())
	assert.Equal(t, objectGroupCreateRequest.CreateRevisionRequest.GetDescription(), objectGroupFromGet.ObjectGroup.CurrentRevision.GetDescription())
	assert.Equal(t, 5, len(objectGroupFromGet.ObjectGroup.CurrentRevision.Objects))
	assert.Equal(t, 5, len(objectGroupFromGet.ObjectGroup.CurrentRevision.MetadataObjects))

	newDataObjects, err := UploadObjects(ServerEndpoints.load, ServerEndpoints.object, 5, datasetID.GetId(), "data")
	if err != nil {
		log.Fatalln(err.Error())
	}

	newMetaObjects, err := UploadObjects(ServerEndpoints.load, ServerEndpoints.object, 5, datasetID.GetId(), "meta")
	if err != nil {
		log.Fatalln(err.Error())
	}

	newAddDataRequest := make([]*v1storageservices.AddObjectRequest, 0)
	for _, dataObject := range newDataObjects {
		newAddDataRequest = append(newAddDataRequest, &v1storageservices.AddObjectRequest{
			Id: dataObject.ID.String(),
		})
	}

	newAddMetaRequests := make([]*v1storageservices.AddObjectRequest, 0)
	for _, dataObject := range newMetaObjects {
		newAddMetaRequests = append(newAddMetaRequests, &v1storageservices.AddObjectRequest{
			Id: dataObject.ID.String(),
		})
	}

	updateObjectGroupRequest := &v1storageservices.UpdateObjectGroupRequest{
		Id: objectGroup.GetObjectGroupId(),
		CreateRevisionRequest: &v1storageservices.CreateObjectGroupRevisionRequest{
			Name:        "updated-revision",
			Description: "updated-revision",
			UpdateObjects: &v1storageservices.UpdateObjectsRequests{
				AddObjects: newAddDataRequest,
				DeleteObjects: []*v1storageservices.DeleteObjectRequest{&v1storageservices.DeleteObjectRequest{
					Id: addDataRequest[0].GetId(),
				}},
			},
			UpdateMetaObjects: &v1storageservices.UpdateObjectsRequests{
				AddObjects: newAddMetaRequests,
				DeleteObjects: []*v1storageservices.DeleteObjectRequest{
					&v1storageservices.DeleteObjectRequest{
						Id: addMetaRequests[0].GetId(),
					},
				},
			},
		},
	}

	_, err = ServerEndpoints.object.UpdateObjectGroup(context.Background(), updateObjectGroupRequest)
	if err != nil {
		log.Fatalln(err.Error())
	}

	objectGroupNewRevision, err := ServerEndpoints.object.GetObjectGroup(context.Background(), &v1storageservices.GetObjectGroupRequest{
		Id: objectGroup.ObjectGroupId,
	})
	if err != nil {
		log.Fatalln(err.Error())
	}

	newCurrentRevision := objectGroupNewRevision.ObjectGroup.CurrentRevision

	var objectNames []string
	var objectIDs []string

	for _, object := range newCurrentRevision.Objects {
		objectNames = append(objectNames, object.Filename)
		objectIDs = append(objectIDs, object.Id)
	}

	assert.Equal(t, 9, len(newCurrentRevision.Objects))

	dataObjects2, err := UploadObjects(ServerEndpoints.load, ServerEndpoints.object, 5, datasetID.GetId(), "data")
	if err != nil {
		log.Fatalln(err.Error())
	}

	metaObjects2, err := UploadObjects(ServerEndpoints.load, ServerEndpoints.object, 5, datasetID.GetId(), "meta")
	if err != nil {
		log.Fatalln(err.Error())
	}

	addDataRequest2 := make([]*v1storageservices.AddObjectRequest, 0)
	for _, dataObject := range dataObjects2 {
		addDataRequest2 = append(addDataRequest2, &v1storageservices.AddObjectRequest{
			Id: dataObject.ID.String(),
		})
	}

	addMetaRequests2 := make([]*v1storageservices.AddObjectRequest, 0)
	for _, dataObject := range metaObjects2 {
		addMetaRequests2 = append(addMetaRequests2, &v1storageservices.AddObjectRequest{
			Id: dataObject.ID.String(),
		})
	}

	updateObjectGroupRequest2 := &v1storageservices.UpdateObjectGroupRequest{
		Id: objectGroup.GetObjectGroupId(),
		CreateRevisionRequest: &v1storageservices.CreateObjectGroupRevisionRequest{
			Name:        "updated-revision",
			Description: "updated-revision",
			UpdateObjects: &v1storageservices.UpdateObjectsRequests{
				AddObjects: addDataRequest2,
				DeleteObjects: []*v1storageservices.DeleteObjectRequest{&v1storageservices.DeleteObjectRequest{
					Id: addDataRequest[1].GetId(),
				}},
			},
			UpdateMetaObjects: &v1storageservices.UpdateObjectsRequests{
				AddObjects: addMetaRequests2,
				DeleteObjects: []*v1storageservices.DeleteObjectRequest{
					&v1storageservices.DeleteObjectRequest{
						Id: addMetaRequests[1].GetId(),
					},
				},
			},
		},
	}

	_, err = ServerEndpoints.object.UpdateObjectGroup(context.Background(), updateObjectGroupRequest2)
	if err != nil {
		log.Fatalln(err.Error())
	}

	objectGroupNewRevision2, err := ServerEndpoints.object.GetObjectGroup(context.Background(), &v1storageservices.GetObjectGroupRequest{
		Id: objectGroup.ObjectGroupId,
	})
	if err != nil {
		log.Fatalln(err.Error())
	}

	newCurrentRevision2 := objectGroupNewRevision2.ObjectGroup.CurrentRevision

	var objectNames2 []string
	var objectIDs2 []string

	for _, object := range newCurrentRevision2.Objects {
		objectNames2 = append(objectNames2, object.Filename)
		objectIDs2 = append(objectIDs2, object.Id)
	}

	assert.Equal(t, 13, len(newCurrentRevision2.Objects))
}

func TestObjectGroupBatch(t *testing.T) {
	t.Skip()
	project, err := ServerEndpoints.project.CreateProject(context.Background(), &v1storageservices.CreateProjectRequest{
		Name: "foo",
	})
	if err != nil {
		log.Fatalln(err.Error())
	}

	dataset, err := ServerEndpoints.dataset.CreateDataset(context.Background(), &v1storageservices.CreateDatasetRequest{
		Name:      "foo",
		ProjectId: project.GetId(),
	})
	if err != nil {
		log.Fatalln(err.Error())
	}

	var requests []*v1storageservices.CreateObjectGroupRequest

	for i := 0; i < 10; i++ {
		objects, err := UploadObjects(ServerEndpoints.load, ServerEndpoints.object, 1, dataset.GetId(), "batch-")
		if err != nil {
			log.Fatalln(err.Error())
		}

		createObjectGroupRequest := &v1storageservices.CreateObjectGroupRequest{
			DatasetId: dataset.GetId(),
			CreateRevisionRequest: &v1storageservices.CreateObjectGroupRevisionRequest{
				Name:              fmt.Sprintf("foo-%v", i),
				UpdateMetaObjects: &v1storageservices.UpdateObjectsRequests{},
				UpdateObjects: &v1storageservices.UpdateObjectsRequests{AddObjects: []*v1storageservices.AddObjectRequest{
					&v1storageservices.AddObjectRequest{Id: objects[0].ID.String()},
				}},
			},
		}
		requests = append(requests, createObjectGroupRequest)
	}

	result, err := ServerEndpoints.object.CreateObjectGroupBatch(context.Background(), &v1storageservices.CreateObjectGroupBatchRequest{
		Requests:          requests,
		IncludeObjectLink: true,
	})
	if err != nil {
		log.Fatalln(err.Error())
	}

	assert.Equal(t, len(requests), len(result.Responses))

	for _, objectgroup := range result.GetResponses() {
		if len(objectgroup.CreateRevisionResponse.ObjectLinks) != 2 {
			log.Fatalln(fmt.Sprintf("wrong number of upload links found: found %v expected 2", len(objectgroup.CreateRevisionResponse.ObjectLinks)))
		}
		for _, object := range objectgroup.CreateRevisionResponse.ObjectLinks {
			uploadHttpRequest, err := http.NewRequest("PUT", object.Link, bytes.NewBufferString("foo"))
			if err != nil {
				log.Fatalln(err.Error())
			}

			response, err := http.DefaultClient.Do(uploadHttpRequest)
			if err != nil {
				log.Fatalln(err.Error())
			}

			if response.StatusCode != 200 {
				log.Fatalln(response.Status)
			}
		}
	}

	projectFromGet, err := ServerEndpoints.project.GetProject(context.Background(), &v1storageservices.GetProjectRequest{
		Id: project.GetId(),
	})

	if err != nil {
		log.Fatalln(err.Error())
	}

	assert.Equal(t, int64(20), projectFromGet.GetProject().GetStats().ObjectCount)
	assert.Equal(t, int64(10), projectFromGet.GetProject().GetStats().ObjectGroupCount)
	assert.Equal(t, float64(3), projectFromGet.GetProject().GetStats().AvgObjectSize)
	assert.Equal(t, int64(60), projectFromGet.GetProject().GetStats().GetAccSize())
	assert.Equal(t, int64(1), projectFromGet.GetProject().GetStats().GetUserCount())

	datasetFromGet, err := ServerEndpoints.dataset.GetDataset(context.Background(), &v1storageservices.GetDatasetRequest{
		Id: dataset.GetId(),
	})

	if err != nil {
		log.Fatalln(err.Error())
	}

	assert.Equal(t, int64(20), datasetFromGet.GetDataset().GetStats().ObjectCount)
	assert.Equal(t, int64(10), datasetFromGet.GetDataset().GetStats().ObjectGroupCount)
	assert.Equal(t, float64(3), datasetFromGet.GetDataset().GetStats().AvgObjectSize)
	assert.Equal(t, int64(60), datasetFromGet.GetDataset().GetStats().GetAccSize())

	datasetobjectGroups, err := ServerEndpoints.dataset.GetDatasetObjectGroups(context.Background(), &v1storageservices.GetDatasetObjectGroupsRequest{
		Id: dataset.GetId(),
	})

	if err != nil {
		log.Fatalln(err.Error())
	}

	for _, objectGroup := range datasetobjectGroups.ObjectGroups {
		objectGroupStats := objectGroup.CurrentRevision.GetStats()
		assert.Equal(t, int64(2), objectGroupStats.GetObjectCount())
		assert.Equal(t, float64(3), objectGroupStats.GetAvgObjectSize())
		assert.Equal(t, int64(6), objectGroupStats.GetAccSize())
	}
}

func TestObjectGroupDuplicates(t *testing.T) {
	projectID, err := ServerEndpoints.project.CreateProject(context.Background(), &v1storageservices.CreateProjectRequest{
		Name: "foo",
	})
	if err != nil {
		log.Fatalln(err.Error())
	}

	datasetID1, err := ServerEndpoints.dataset.CreateDataset(context.Background(), &v1storageservices.CreateDatasetRequest{
		Name:      "foo-1",
		ProjectId: projectID.GetId(),
	})
	if err != nil {
		log.Fatalln(err.Error())
	}

	datasetID2, err := ServerEndpoints.dataset.CreateDataset(context.Background(), &v1storageservices.CreateDatasetRequest{
		Name:      "foo-2",
		ProjectId: projectID.GetId(),
	})
	if err != nil {
		log.Fatalln(err.Error())
	}

	_, err = ServerEndpoints.object.CreateObjectGroup(context.Background(), &v1storageservices.CreateObjectGroupRequest{
		DatasetId: datasetID1.GetId(),
		CreateRevisionRequest: &v1storageservices.CreateObjectGroupRevisionRequest{
			Name:              uuid.New().String(),
			UpdateObjects:     &v1storageservices.UpdateObjectsRequests{},
			UpdateMetaObjects: &v1storageservices.UpdateObjectsRequests{},
		},
	})
	if err != nil {
		log.Fatalln(err.Error())
	}

	_, err = ServerEndpoints.object.CreateObjectGroup(context.Background(), &v1storageservices.CreateObjectGroupRequest{
		CreateRevisionRequest: &v1storageservices.CreateObjectGroupRevisionRequest{
			Name:              "test-1",
			UpdateObjects:     &v1storageservices.UpdateObjectsRequests{},
			UpdateMetaObjects: &v1storageservices.UpdateObjectsRequests{},
		},
		DatasetId: datasetID1.GetId(),
	})
	if err != nil {
		log.Fatalln(err.Error())
	}

	_, err = ServerEndpoints.object.CreateObjectGroup(context.Background(), &v1storageservices.CreateObjectGroupRequest{
		CreateRevisionRequest: &v1storageservices.CreateObjectGroupRevisionRequest{
			Name:              uuid.New().String(),
			UpdateObjects:     &v1storageservices.UpdateObjectsRequests{},
			UpdateMetaObjects: &v1storageservices.UpdateObjectsRequests{},
		},
		DatasetId: datasetID2.GetId(),
	})
	if err != nil {
		log.Fatalln(err.Error())
	}
}
