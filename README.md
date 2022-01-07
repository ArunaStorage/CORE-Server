# CORE-Server

**WIP**

## Description

This is a golang based implementation of the CORE-Server API described here: https://github.com/ScienceObjectsDB/API. It is designed to provide a data management solution for data objects (e.g. files) along with associated metadata and a management system that allows easy bulk access to the objects. It uses an underlaying SQL based database for storing some common metadata and uses an object storage based storage system for the data itself. It can be used as a standalone data management platform but is intended to be used with a metadata store that handles dataset specific metadata (currently under development) and an optional multi-side module that can connect multiple individual CORE-Servers to allow for easy data sharing and efficient caching.

## Configuration parameters

The configuration for the CORE-Server has to be described as yaml file. It has to be places under ./config.

### Server parameters

| Name          | Description                  | Value     |
| ------------- | ---------------------------- | --------- |
| `Server.Host` | Server IP address to bind to | `0.0.0.0` |
| `Server.Port` | Server port                  | `50051`   |

### Database parameters

| Name                          | Description                                                         | Value             |
| ----------------------------- | ------------------------------------------------------------------- | ----------------- |
| `DB.Databasetype`             | Backend type of the database, one of: [`"Cockroach", "Postgres"`]   | `"Cockroach"`     |
| `DB.Cockroach.Hostname`       | Hostname of the CockroachDB                                         | `"localhost"`     |
| `DB.Cockroach.Port`           | Port of the CockroachDB                                             | `26257`           |
| `DB.Cockroach.Username`       | Username for the CockroachDB                                        | `"root"`          |
| `DB.Cockroach.Databasename`   | Name of the database in CockroachDB to use (will not be created)    | `"defaultdb"`     |
| `DB.Cockroach.PasswordEnvVar` | Name of the environment variable to find the database user password | `"PSQL_PASSWORD"` |
| `DB.Postgres.Hostname`        | Hostname of the Postgres                                            | `"localhost"`     |
| `DB.Postgres.Port`            | Port of the Postgres                                                | `26257`           |
| `DB.Postgres.Username`        | Username for the Postgres                                           | `"root"`          |
| `DB.Postgres.Databasename`    | Name of the database in Postgres to use (will not be created)       | `"defaultdb"`     |
| `DB.Postgres.PasswordEnvVar`  | Name of the environment variable to find the database user password | `"PSQL_PASSWORD"` |

### Objectstorage parameters

| Name                | Description                                                              | Value                     |
| ------------------- | ------------------------------------------------------------------------ | ------------------------- |
| `S3.BucketPrefix`   | Prefix of the buckets that are created for the individual dataset        | `"scienceobjectsdb"`      |
| `S3.Endpoint`       | S3 endpoint to use for data storage                                      | `"http://localhost:9000"` |
| `S3.Implementation` | Name of the implementation that is used for S3 storage, e.g. minio, ceph | `"generic"`               |

### Eventnotification parameters

| Name                                       | Description                                                | Value                     |
| ------------------------------------------ | ---------------------------------------------------------- | ------------------------- |
| `EventNotifications.Backend`               | Event streaming backend type: [`"Empty", "NATS"`]          | `"Empty"`                 |
| `EventNotifications.NATS.URL`              | Hostname of the NATS cluster                               | `"http://localhost:4222"` |
| `EventNotifications.NATS.SubjectPrefix`    | The Subject prefix that should be used on the NATS cluster | `"UPDATES"`               |
| `EventNotifications.NATS.NKeySeedFileName` | Nkey file for autentication                                | None                      |

### Streaming parameters

| Name                     | Description                             | Value                |
| ------------------------ | --------------------------------------- | -------------------- |
| `Streaming.Endpoint`     | Endpoint of the data streaming function | `"localhost"`        |
| `Streaming.Port`         | Hostname of the NATS cluster            | `"443"`              |
| `Streaming.SecretEnvVar` | Hostname of the NATS cluster            | `"STREAMING_SECRET"` |

### Authentication parameters

| Name                                    | Description                                | Value                                                                        |
| --------------------------------------- | ------------------------------------------ | ---------------------------------------------------------------------------- |
| `Authentication.Type`                   | Authentication type [`"INSECURE", "OIDC"`] | `"INSECURE"`                                                                 |
| `Authentication.OIDC.UserInfoEndpoint`  | OAuth2 user info endpoint                  | `"localhost:9051/auth/realms/DEFAULTREALM/protocol/openid-connect/userinfo"` |
| `Authentication.OIDC.RealmInfoEndpoint` | OAuth2 realm info endpoint                 | `"localhost:9051/auth/realms/DEFAULTREALM"`                                  |

### Environment variables

| Name                    | Description                                                                                         |
| ----------------------- | --------------------------------------------------------------------------------------------------- |
| `PSQL_PASSWORD`         | Database password; can be set via `"DB.Postgres.PasswordEnvVar"` or `"DB.Cockroach.PasswordEnvVar"` |
| `AWS_ACCESS_KEY_ID`     | Access key for the object storage                                                                   |
| `AWS_SECRET_ACCESS_KEY` | Secret key for the object storage                                                                   |

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
4. Create the config, an example can be found under config/local/config.yaml
5. Start server:

```
docker run -d -p 50051:50051 -p 9011:9011 --mount type=bind,source=<path/to/configdir>,target=/config harbor.computational.bio.uni-giessen.de/scienceobjectsdb/core-server:latest
```
