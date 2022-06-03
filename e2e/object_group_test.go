package e2e

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
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
				AddObjects: []*v1storageservices.CreateObjectRequest{
					&v1storageservices.CreateObjectRequest{
						Filename:   "metatest1.txt",
						Filetype:   "txt",
						ContentLen: 3,
						Labels: []*v1storagemodels.Label{
							&v1storagemodels.Label{Key: "testlabel1", Value: "testlabel1"},
							&v1storagemodels.Label{Key: "testlabel2", Value: "testlabel2"},
						},
					},
				},
				ExistingObjects: []*v1storageservices.ExistingObjectRequest{},
				UpdateObjects:   []*v1storageservices.UpdateObjectRequest{},
			},
			UpdateObjects: &v1storageservices.UpdateObjectsRequests{
				AddObjects: []*v1storageservices.CreateObjectRequest{
					&v1storageservices.CreateObjectRequest{
						Filename:   "test1.txt",
						Filetype:   "txt",
						ContentLen: 3,
						Labels: []*v1storagemodels.Label{
							&v1storagemodels.Label{Key: "testlabelobject1-1", Value: "testlabelobject1-1"},
							&v1storagemodels.Label{Key: "testlabelobject2-1", Value: "testlabelobject2-1"},
						},
					},
					&v1storageservices.CreateObjectRequest{
						Filename:   "test2.txt",
						Filetype:   "txt",
						ContentLen: 3,
						Labels: []*v1storagemodels.Label{
							&v1storagemodels.Label{Key: "testlabelobject1-2", Value: "testlabelobject1-2"},
							&v1storagemodels.Label{Key: "testlabelobject2-2", Value: "testlabelobject2-2"},
						},
					},
				},
				UpdateObjects:   []*v1storageservices.UpdateObjectRequest{},
				ExistingObjects: []*v1storageservices.ExistingObjectRequest{},
			},
		},
	}

	createObjectGroup, err := ServerEndpoints.object.CreateObjectGroup(context.Background(), objectGroupCreateRequest)
	if err != nil {
		log.Fatalln(err.Error())
	}

	for _, objectLink := range createObjectGroup.CreateRevisionResponse.ObjectLinks {
		if objectLink != nil {
			uploadHttpRequest, err := http.NewRequest("PUT", objectLink.Link, bytes.NewBufferString(objectLink.ObjectId))
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

			_, err = ServerEndpoints.object.FinishObjectUpload(context.Background(), &v1storageservices.FinishObjectUploadRequest{
				Id: objectLink.ObjectId,
			})
			if err != nil {
				log.Fatalln(err.Error())
			}
		}
	}

	for _, metaObjectLink := range createObjectGroup.CreateRevisionResponse.MetadataObjectLinks {
		if metaObjectLink != nil {
			uploadHttpRequest, err := http.NewRequest("PUT", metaObjectLink.Link, bytes.NewBufferString("test2"))
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

			_, err = ServerEndpoints.object.FinishObjectUpload(context.Background(), &v1storageservices.FinishObjectUploadRequest{
				Id: metaObjectLink.ObjectId,
			})
			if err != nil {
				log.Fatalln(err.Error())
			}
		}
	}

	_, err = ServerEndpoints.object.FinishObjectGroupRevisionUpload(context.Background(), &v1storageservices.FinishObjectGroupRevisionUploadRequest{
		Id: createObjectGroup.GetCreateRevisionResponse().GetId(),
	})

	objectGroup, err := ServerEndpoints.object.GetObjectGroup(context.Background(), &v1storageservices.GetObjectGroupRequest{
		Id: createObjectGroup.GetObjectGroupId(),
	})

	currentRevision := objectGroup.ObjectGroup.CurrentRevision

	assert.Equal(t, objectGroupCreateRequest.CreateRevisionRequest.Name, currentRevision.Name)
	assert.Equal(t, objectGroupCreateRequest.CreateRevisionRequest.Description, currentRevision.Description)

	foundObjects := make(map[string]*v1storagemodels.Object, 0)

	for _, object := range currentRevision.Objects {
		foundObjects[object.Filename] = object
	}

	for _, createObjectRequest := range objectGroupCreateRequest.GetCreateRevisionRequest().UpdateObjects.AddObjects {
		if foundObject, ok := foundObjects[createObjectRequest.Filename]; ok {
			assert.Equal(t, createObjectRequest.ContentLen, foundObject.ContentLen)
			assert.Equal(t, createObjectRequest.Filetype, foundObject.Filetype)
			assert.Equal(t, len(createObjectRequest.Labels), len(foundObject.Labels))
		}
	}

	for _, object := range objectGroup.GetObjectGroup().GetCurrentRevision().GetObjects() {
		link, err := ServerEndpoints.load.CreateDownloadLink(context.Background(), &v1storageservices.CreateDownloadLinkRequest{
			Id: object.Id,
		})
		if err != nil {
			log.Fatalln(err.Error())
		}

		response, err := http.Get(link.GetDownloadLink())
		if err != nil {
			log.Fatalln(err.Error())
		}

		data, err := ioutil.ReadAll(response.Body)
		if err != nil {
			log.Fatalln(err.Error())
		}

		datastring := string(data)
		assert.Equal(t, object.GetId(), datastring)
	}
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

	objectGroupCreateRequest := &v1storageservices.CreateObjectGroupRequest{
		DatasetId: datasetID.GetId(),
		CreateRevisionRequest: &v1storageservices.CreateObjectGroupRevisionRequest{
			UpdateMetaObjects: &v1storageservices.UpdateObjectsRequests{
				AddObjects: []*v1storageservices.CreateObjectRequest{
					&v1storageservices.CreateObjectRequest{
						Filename:   "metatest1.txt",
						Filetype:   "txt",
						ContentLen: 3,
					},
				},
				ExistingObjects: []*v1storageservices.ExistingObjectRequest{},
				UpdateObjects:   []*v1storageservices.UpdateObjectRequest{},
			},
			UpdateObjects: &v1storageservices.UpdateObjectsRequests{
				AddObjects: []*v1storageservices.CreateObjectRequest{
					&v1storageservices.CreateObjectRequest{
						Filename:   "test1.txt",
						Filetype:   "txt",
						ContentLen: 3,
					},
					&v1storageservices.CreateObjectRequest{
						Filename:   "test2.txt",
						Filetype:   "txt",
						ContentLen: 3,
					},
				},
				UpdateObjects:   []*v1storageservices.UpdateObjectRequest{},
				ExistingObjects: []*v1storageservices.ExistingObjectRequest{},
			},
		},
	}

	objectGroup, err := ServerEndpoints.object.CreateObjectGroup(context.Background(), objectGroupCreateRequest)
	if err != nil {
		log.Fatalln(err.Error())
	}

	for _, object := range objectGroup.CreateRevisionResponse.ObjectLinks {
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

		_, err = ServerEndpoints.object.FinishObjectUpload(context.Background(), &v1storageservices.FinishObjectUploadRequest{
			Id: object.ObjectId,
		})
		if err != nil {
			log.Fatalln(err.Error())
		}
	}

	for _, metaobject := range objectGroup.CreateRevisionResponse.GetMetadataObjectLinks() {
		uploadHttpRequest, err := http.NewRequest("PUT", metaobject.Link, bytes.NewBufferString("bar"))
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

		_, err = ServerEndpoints.object.FinishObjectUpload(context.Background(), &v1storageservices.FinishObjectUploadRequest{
			Id: metaobject.ObjectId,
		})
		if err != nil {
			log.Fatalln(err.Error())
		}
	}

	_, err = ServerEndpoints.object.FinishObjectGroupRevisionUpload(context.Background(), &v1storageservices.FinishObjectGroupRevisionUploadRequest{
		Id: objectGroup.CreateRevisionResponse.GetId(),
	})
	if err != nil {
		log.Fatalln(err.Error())
	}

	objectGroupFromGet, err := ServerEndpoints.object.GetObjectGroup(context.Background(), &v1storageservices.GetObjectGroupRequest{
		Id: objectGroup.ObjectGroupId,
	})

	assert.Equal(t, objectGroup.CreateRevisionResponse.GetId(), objectGroupFromGet.ObjectGroup.CurrentRevision.Id)
	assert.Equal(t, objectGroupCreateRequest.CreateRevisionRequest.GetName(), objectGroupFromGet.ObjectGroup.CurrentRevision.GetName())
	assert.Equal(t, objectGroupCreateRequest.CreateRevisionRequest.GetDescription(), objectGroupFromGet.ObjectGroup.CurrentRevision.GetDescription())
	assert.Equal(t, 2, len(objectGroupFromGet.ObjectGroup.CurrentRevision.Objects))
	assert.Equal(t, 1, len(objectGroupFromGet.ObjectGroup.CurrentRevision.MetadataObjects))

	objectGroupRevisionRequestForUpdate := &v1storageservices.CreateObjectGroupRevisionRequest{
		Name:              objectGroupFromGet.GetObjectGroup().GetCurrentRevision().Name,
		Description:       objectGroupFromGet.GetObjectGroup().GetCurrentRevision().Description,
		Labels:            objectGroupFromGet.ObjectGroup.CurrentRevision.Labels,
		ObjectGroupId:     objectGroup.ObjectGroupId,
		Annotations:       objectGroupFromGet.ObjectGroup.CurrentRevision.Annotations,
		IncludeObjectLink: true,
		UpdateObjects: &v1storageservices.UpdateObjectsRequests{
			AddObjects: []*v1storageservices.CreateObjectRequest{
				&v1storageservices.CreateObjectRequest{
					Filename:   "updatedfile.txt",
					Filetype:   "txt",
					ContentLen: 5,
				},
			},
			ExistingObjects: []*v1storageservices.ExistingObjectRequest{
				&v1storageservices.ExistingObjectRequest{
					Id: objectGroupFromGet.ObjectGroup.CurrentRevision.Objects[1].Id,
				},
			},
			UpdateObjects: []*v1storageservices.UpdateObjectRequest{},
		},
		UpdateMetaObjects: &v1storageservices.UpdateObjectsRequests{
			AddObjects:      []*v1storageservices.CreateObjectRequest{},
			UpdateObjects:   []*v1storageservices.UpdateObjectRequest{},
			ExistingObjects: []*v1storageservices.ExistingObjectRequest{},
		},
	}

	objectGroupRevisionResponse, err := ServerEndpoints.object.CreateObjectGroupRevision(context.Background(), objectGroupRevisionRequestForUpdate)
	if err != nil {
		log.Fatalln(err.Error())
	}

	for _, objectLink := range objectGroupRevisionResponse.ObjectLinks {
		if objectLink != nil {
			uploadHttpRequest, err := http.NewRequest("PUT", objectLink.Link, bytes.NewBufferString("test2"))
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

			_, err = ServerEndpoints.object.FinishObjectUpload(context.Background(), &v1storageservices.FinishObjectUploadRequest{
				Id: objectLink.ObjectId,
			})
			if err != nil {
				log.Fatalln(err.Error())
			}
		}
	}

	for _, metaObjectLink := range objectGroupRevisionResponse.MetadataObjectLinks {
		if metaObjectLink != nil {
			uploadHttpRequest, err := http.NewRequest("PUT", metaObjectLink.Link, bytes.NewBufferString("test2"))
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

			_, err = ServerEndpoints.object.FinishObjectUpload(context.Background(), &v1storageservices.FinishObjectUploadRequest{
				Id: metaObjectLink.ObjectId,
			})
			if err != nil {
				log.Fatalln(err.Error())
			}
		}
	}

	_, err = ServerEndpoints.object.FinishObjectGroupRevisionUpload(context.Background(), &v1storageservices.FinishObjectGroupRevisionUploadRequest{
		Id: objectGroupRevisionResponse.GetId(),
	})
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

	assert.Contains(t, objectNames, objectGroupRevisionRequestForUpdate.UpdateObjects.AddObjects[0].Filename)
	assert.Contains(t, objectNames, objectGroupFromGet.ObjectGroup.CurrentRevision.Objects[1].Filename)
	assert.NotContains(t, objectNames, objectGroupFromGet.ObjectGroup.CurrentRevision.Objects[0].Filename)

	assert.Contains(t, objectIDs, objectGroupFromGet.ObjectGroup.CurrentRevision.Objects[1].Id)

}

func TestObjectGroupBatch(t *testing.T) {
	projectID, err := ServerEndpoints.project.CreateProject(context.Background(), &v1storageservices.CreateProjectRequest{
		Name: "foo",
	})
	if err != nil {
		log.Fatalln(err.Error())
	}

	datasetID, err := ServerEndpoints.dataset.CreateDataset(context.Background(), &v1storageservices.CreateDatasetRequest{
		Name:      "foo",
		ProjectId: projectID.GetId(),
	})
	if err != nil {
		log.Fatalln(err.Error())
	}

	var requests []*v1storageservices.CreateObjectGroupRequest

	for i := 0; i < 10; i++ {
		createObjectGroupRequest := &v1storageservices.CreateObjectGroupRequest{
			DatasetId: datasetID.GetId(),
			CreateRevisionRequest: &v1storageservices.CreateObjectGroupRevisionRequest{
				Name:              fmt.Sprintf("foo-%v", i),
				UpdateMetaObjects: &v1storageservices.UpdateObjectsRequests{},
				UpdateObjects: &v1storageservices.UpdateObjectsRequests{
					AddObjects: []*v1storageservices.CreateObjectRequest{
						{
							Filename:   "ff.bin",
							ContentLen: 3,
						},
						{
							Filename:   "fu.bin",
							ContentLen: 3,
						},
					},
				},
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

	if len(result.Responses) != len(requests) {
		t.Fatalf("wrong number of result found")
	}

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

	project, err := ServerEndpoints.project.GetProject(context.Background(), &v1storageservices.GetProjectRequest{
		Id: projectID.GetId(),
	})

	if err != nil {
		log.Fatalln(err.Error())
	}

	assert.Equal(t, int64(20), project.GetProject().GetStats().ObjectCount)
	assert.Equal(t, int64(10), project.GetProject().GetStats().ObjectGroupCount)
	assert.Equal(t, float64(3), project.GetProject().GetStats().AvgObjectSize)
	assert.Equal(t, int64(60), project.GetProject().GetStats().GetAccSize())
	assert.Equal(t, int64(1), project.GetProject().GetStats().GetUserCount())

	dataset, err := ServerEndpoints.dataset.GetDataset(context.Background(), &v1storageservices.GetDatasetRequest{
		Id: datasetID.GetId(),
	})

	if err != nil {
		log.Fatalln(err.Error())
	}

	assert.Equal(t, int64(20), dataset.GetDataset().GetStats().ObjectCount)
	assert.Equal(t, int64(10), dataset.GetDataset().GetStats().ObjectGroupCount)
	assert.Equal(t, float64(3), dataset.GetDataset().GetStats().AvgObjectSize)
	assert.Equal(t, int64(60), dataset.GetDataset().GetStats().GetAccSize())

	datasetobjectGroups, err := ServerEndpoints.dataset.GetDatasetObjectGroups(context.Background(), &v1storageservices.GetDatasetObjectGroupsRequest{
		Id: datasetID.GetId(),
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
