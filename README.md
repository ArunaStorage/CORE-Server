# CORE-Server
**WIP**

## Description
This is a golang based implementation of the CORE-Server API described here: https://github.com/ScienceObjectsDB/API. It is designed to provide a data management solution for data objects (e.g. files) along with associated metadata and a management system that allows easy bulk access to the objects. It uses an underlaying SQL based database for storing some common metadata and uses an object storage based storage system for the data itself. It can be used as a standalone data management platform but is intended to be used with a metadata store that handles dataset specific metadata (currently under development) and an optional multi-side module that can connect multiple individual CORE-Servers to allow for easy data sharing and efficient caching.