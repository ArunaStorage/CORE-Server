# CORE-Server
**WIP**

## Description
This is a golang based implementation of the CORE-Server API described here: https://github.com/ScienceObjectsDB/API. It is designed to provide a data management solution for data objects (e.g. files) along with associated metadata and a management system that allows easy bulk access to the objects. It uses an underlaying SQL based database for storing some common metadata and uses an object storage based storage system for the data itself. It can be used as a standalone data management platform but is intended to be used with a metadata store that handles dataset specific metadata (currently under development) and an optional multi-side module that can connect multiple individual CORE-Servers to allow for easy data sharing and efficient caching.

## Deployment
### Local insecure (for testing)
This is a setup for testing the API and is not intended for production use.

1. Start a local cockroachdb: 
```
docker run -d -p 15000:8080 -p 26257:26257 cockroachdb/cockroach:latest start-single-node --cluster-name=example-single-node --insecure
```

2. Start a local minio:
```
docker run -d -p 9000:9000 -p 9001:9001 minio/minio server /data --console-address ":9001"
```

3. Configure minio: Create a bucket in the UI from localhost:9000