package handler

import (
	"encoding/json"
	"testing"

	log "github.com/sirupsen/logrus"

	"github.com/ScienceObjectsDB/CORE-Server/objectstorage"
	"github.com/ScienceObjectsDB/CORE-Server/util"
	protomodels "github.com/ScienceObjectsDB/go-api/api/models/v1"
	services "github.com/ScienceObjectsDB/go-api/api/services/v1"
)

func TestDataset(t *testing.T) {
	database := util.TestDatabase{}
	database.New()

	s3Handler := objectstorage.S3ObjectStorageHandler{}
	s3Handler.New("testbucket")

	createHandler := Create{
		Common: &Common{
			DB:        database.DB,
			S3Handler: &s3Handler,
		},
	}

	readHandler := Read{
		Common: &Common{
			DB:        database.DB,
			S3Handler: &s3Handler,
		},
	}

	createProjectReq := services.CreateProjectRequest{
		Name:        "foo",
		Description: "baa",
		Metadata: []*protomodels.Metadata{
			{
				Key:      "test",
				Metadata: []byte("foobarbaz"),
			},
		},
	}

	projectID, err := createHandler.CreateProject(&createProjectReq, "foouser")
	if err != nil {
		log.Fatalln(err.Error())
	}

	create_DatasetReq := services.CreateDatasetRequest{
		Name:      "foo",
		ProjectId: uint64(projectID),
		Labels: []*protomodels.Label{
			{
				Key:   "foo",
				Value: "baa",
			},
		},
		Metadata: []*protomodels.Metadata{
			{
				Key:      "fo",
				Metadata: []byte("foo"),
			},
		},
	}

	datasetID, err := createHandler.CreateDataset(&create_DatasetReq)
	if err != nil {
		log.Fatalln(err.Error())
	}

	createObjectGroupRequest := services.CreateObjectGroupRequest{
		Name:      "foo",
		DatasetId: uint64(datasetID),
		Labels: []*protomodels.Label{
			&protomodels.Label{
				Key:   "fii",
				Value: "baa",
			},
			&protomodels.Label{
				Key:   "baa",
				Value: "fii",
			},
		},
		Metadata: []*protomodels.Metadata{
			&protomodels.Metadata{
				Key: "fii",
				Labels: []*protomodels.Label{
					&protomodels.Label{
						Key:   "oo",
						Value: "fdaa",
					},
				},
				Metadata: []byte("fooo"),
			},
			&protomodels.Metadata{
				Key: "asffii",
				Labels: []*protomodels.Label{
					&protomodels.Label{
						Key:   "asf",
						Value: "asf",
					},
				},
				Metadata: []byte("asfa"),
			},
		},
		ObjectGroupRevision: &services.CreateObjectGroupRevisionRequest{
			Objects: []*services.CreateObjectRequest{
				&services.CreateObjectRequest{
					Filename:   "foo.bar",
					Filetype:   "bar",
					ContentLen: 9,
				},
				&services.CreateObjectRequest{
					Filename:   "bar.baz",
					Filetype:   "baz",
					ContentLen: 15,
				},
			},
		},
	}

	id, _, err := createHandler.CreateObjectGroup(&createObjectGroupRequest)
	if err != nil {
		log.Fatalln(err.Error())
	}

	createObjectGroupRequest2 := services.CreateObjectGroupRequest{
		Name:      "foo",
		DatasetId: uint64(datasetID),
		Labels: []*protomodels.Label{
			&protomodels.Label{
				Key:   "fii",
				Value: "baa",
			},
			&protomodels.Label{
				Key:   "baa",
				Value: "fii",
			},
		},
		Metadata: []*protomodels.Metadata{
			&protomodels.Metadata{
				Key: "fii",
				Labels: []*protomodels.Label{
					&protomodels.Label{
						Key:   "oo",
						Value: "fdaa",
					},
				},
				Metadata: []byte("fooo"),
			},
			&protomodels.Metadata{
				Key: "asffii",
				Labels: []*protomodels.Label{
					&protomodels.Label{
						Key:   "asf",
						Value: "asf",
					},
				},
				Metadata: []byte("asfa"),
			},
		},
		ObjectGroupRevision: &services.CreateObjectGroupRevisionRequest{
			Objects: []*services.CreateObjectRequest{
				&services.CreateObjectRequest{
					Filename:   "foo.bar",
					Filetype:   "bar",
					ContentLen: 9,
				},
				&services.CreateObjectRequest{
					Filename:   "bar.baz",
					Filetype:   "baz",
					ContentLen: 15,
				},
			},
		},
	}

	_, _, err = createHandler.CreateObjectGroup(&createObjectGroupRequest2)
	if err != nil {
		log.Fatalln(err.Error())
	}

	grps, err := readHandler.GetDatasetObjectGroups(id)
	if err != nil {
		log.Fatalln(err.Error())
	}

	createHandler.AddObjectGroupRevision(&services.AddRevisionToObjectGroupRequest{
		ObjectGroupId: uint64(id),
		GroupRevison: &services.CreateObjectGroupRevisionRequest{
			Objects: []*services.CreateObjectRequest{
				&services.CreateObjectRequest{
					Filename:   "foo2.bar",
					Filetype:   "bar2",
					ContentLen: 9,
				},
				&services.CreateObjectRequest{
					Filename:   "bar2.baz",
					Filetype:   "baz2",
					ContentLen: 15,
				},
			},
		},
	})

	objectGroupRevisions, err := readHandler.GetCurrentObjectGroupRevisions(datasetID)
	if err != nil {
		log.Fatalln(err.Error())
	}

	log.Fatalln(len(objectGroupRevisions))

	marshalledGrp, err := json.MarshalIndent(grps, "", " ")
	if err != nil {
		log.Fatalln(err.Error())
	}

	log.Fatalln(string(marshalledGrp))

}
