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

	name := fmt.Sprintf("objectgroup-%v", uuid.New())

	createObjectGroupRequest := &v1storageservices.CreateObjectGroupRequest{
		DatasetId: datasetCreateResponse.GetId(),
		CreateRevisionRequest: &v1storageservices.CreateObjectGroupRevisionRequest{
			Name:              name,
			Labels:            objectGroupLabel,
			IncludeObjectLink: true,
			UpdateObjects: &v1storageservices.UpdateObjectsRequests{
				AddObjects: []*v1storageservices.CreateObjectRequest{
					{
						Filename:   "testfile1",
						Filetype:   "bin",
						Labels:     object1Label,
						ContentLen: 6,
					},
					{
						Filename:   "testfile2",
						Filetype:   "txt",
						Labels:     object2Label,
						ContentLen: 24,
					},
				},
			},
			UpdateMetaObjects: &v1storageservices.UpdateObjectsRequests{
				AddObjects: []*v1storageservices.CreateObjectRequest{
					{
						Filename:   "metadata1",
						Filetype:   "meta",
						ContentLen: 8,
					},
				},
			},
		},
	}

	createObjectGroupResponse, err := ServerEndpoints.object.CreateObjectGroup(context.Background(), createObjectGroupRequest)
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

	// Validate general ObjectGroup fields
	assert.Equal(t, createObjectGroupRequest.CreateRevisionRequest.Name, getObjectGroupResponse.ObjectGroup.CurrentRevision.Name)
	assert.Equal(t, createObjectGroupRequest.DatasetId, getObjectGroupResponse.ObjectGroup.DatasetId)
	assert.Equal(t, createDatasetRequest.Description, getObjectGroupResponse.GetObjectGroup().CurrentRevision.Description)
	assert.ElementsMatch(t, createObjectGroupRequest.CreateRevisionRequest.Labels, getObjectGroupResponse.ObjectGroup.CurrentRevision.Labels)

	// Validate ObjectGroup stats
	assert.Equal(t, int64(2), getObjectGroupResponse.ObjectGroup.CurrentRevision.GetStats().GetObjectCount())
	assert.Equal(t, int64(1), getObjectGroupResponse.ObjectGroup.CurrentRevision.GetStats().GetMetaObjectCount())
	assert.Equal(t, int64(30), getObjectGroupResponse.ObjectGroup.CurrentRevision.GetStats().GetAccSize())
	assert.Equal(t, float64(15), getObjectGroupResponse.ObjectGroup.CurrentRevision.GetStats().GetAvgObjectSize())

	// Validate ObjectGroup data Objects creation
	assert.Equal(t, "testfile1", getObjectGroupResponse.ObjectGroup.CurrentRevision.Objects[0].Filename)
	assert.Equal(t, "bin", getObjectGroupResponse.ObjectGroup.CurrentRevision.Objects[0].Filetype)
	assert.Equal(t, int64(6), getObjectGroupResponse.ObjectGroup.CurrentRevision.Objects[0].ContentLen)

	assert.Equal(t, "testfile2", getObjectGroupResponse.ObjectGroup.CurrentRevision.Objects[1].Filename)
	assert.Equal(t, "txt", getObjectGroupResponse.ObjectGroup.CurrentRevision.Objects[1].Filetype)
	assert.Equal(t, int64(24), getObjectGroupResponse.ObjectGroup.CurrentRevision.Objects[1].ContentLen)

	// Validate ObjectGroup meta Objects creation
	assert.Equal(t, "metadata1", getObjectGroupResponse.ObjectGroup.CurrentRevision.MetadataObjects[0].Filename)
	assert.Equal(t, "meta", getObjectGroupResponse.ObjectGroup.CurrentRevision.MetadataObjects[0].Filetype)
	assert.Equal(t, int64(8), getObjectGroupResponse.ObjectGroup.CurrentRevision.MetadataObjects[0].ContentLen)

	object := getObjectGroupResponse.ObjectGroup.CurrentRevision.Objects[0]

	err = UploadObjects(getObjectGroupResponse.ObjectGroup.CurrentRevision.GetObjects(), []string{"foo", "baa"}, ServerEndpoints.load, ServerEndpoints.object)
	if err != nil {
		log.Fatalln(err.Error())
	}

	err = UploadObjects(getObjectGroupResponse.ObjectGroup.CurrentRevision.GetMetadataObjects(), []string{"metadata"}, ServerEndpoints.load, ServerEndpoints.object)
	if err != nil {
		log.Fatalln(err.Error())
	}

	_, err = ServerEndpoints.object.FinishObjectGroupRevisionUpload(context.Background(), &v1storageservices.FinishObjectGroupRevisionUploadRequest{
		Id: createObjectGroupResponse.CreateRevisionResponse.GetId(),
	})
	if err != nil {
		log.Fatalln(err.Error())
	}

	err = DownloadObjects(t, getObjectGroupResponse.ObjectGroup.CurrentRevision.Objects, []string{"foo", "baa"}, ServerEndpoints.load, ServerEndpoints.object)
	if err != nil {
		log.Fatalln(err.Error())
	}

	downloadLinkRange, err := ServerEndpoints.load.CreateDownloadLink(context.Background(), &v1storageservices.CreateDownloadLinkRequest{
		Id: object.GetId(),
		Range: &v1storageservices.CreateDownloadLinkRequest_Range{
			StartByte: 0,
			EndByte:   1,
		},
	})
	if err != nil {
		log.Fatalln(err.Error())
	}

	rangeDLRequest, err := http.NewRequest("GET", downloadLinkRange.GetDownloadLink(), &bytes.Reader{})
	if err != nil {
		log.Fatalln(err.Error())
	}

	rangeDLRequest.Header.Add("Range", "bytes=0-1")

	dlResponseRange, err := http.DefaultClient.Do(rangeDLRequest)
	if err != nil {
		log.Fatalln(err.Error())
	}

	if dlResponseRange.StatusCode != 206 {
		log.Fatalln(dlResponseRange.Status)
	}

	dataRange, err := ioutil.ReadAll(dlResponseRange.Body)
	if err != nil {
		log.Fatalln(err.Error())
	}

	assert.Equal(t, string(dataRange), "fo")
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

	objectGroup, err := ServerEndpoints.object.CreateObjectGroup(context.Background(), &v1storageservices.CreateObjectGroupRequest{
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
			},
		},
	})
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

	objectGroupRevisionRequest := &v1storageservices.CreateObjectGroupRevisionRequest{
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
		},
	}

	objectGroupRevisionResponse, err := ServerEndpoints.object.CreateObjectGroupRevision(context.Background(), objectGroupRevisionRequest)
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
		Id: objectGroup.ObjectGroupId,
	})

	objectGroupNewRevision, err := ServerEndpoints.object.GetObjectGroup(context.Background(), &v1storageservices.GetObjectGroupRequest{
		Id: objectGroup.ObjectGroupId,
	})

	newCurrentRevision := objectGroupNewRevision.ObjectGroup.CurrentRevision

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
