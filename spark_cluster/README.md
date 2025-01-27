# Spark Cluster with Docker & docker-compose

# General

A simple spark standalone cluster for your testing environment purposses. A *docker-compose up* away from you solution for your spark development environment.

# Installation

The following steps will make you run your spark cluster's containers.

## Pre requisites

* Docker installed

* Docker compose installed

## Build the docker images

The first step to deploy the cluster will be the build of the custom images, these builds can be performed with the *build-images.sh* script. 

The executions is as simple as the following steps:

For Linux/MacOS users:

```sh
chmod +x build-images.sh
./build-images.sh
```

For Windows users:

```sh
./build-images.bat
```

This will create the following docker images:

* spark-base:3.5.4: A base image based on jupyter/base-notebook wich ships conda software to run the Jupyter Notebook service. The full stack of spark 3.5.4 and scala 2.13 are installed on the top of jupyter/base-notebook.

* spark-master:3.5.4: An image based on the previously created spark image, used to create a spark master containers.

* spark-worker:3.5.4: An image based on the previously created spark image, used to create spark worker containers.

* spark-submit:3.5.4: An image based on the previously created Spark image, used to start the notebook service and serve as the entry point for communicating with the Spark cluster created using the images described above.

## Run the docker-compose

The final step to create your test cluster will be to run the compose file:

```sh
docker compose up --scale spark-worker=3 -d
```

## Validate your cluster

Just validate your cluster accessing the spark UI on [master URL](http://localhost:9090)


### Binded Volumes

To make app running easier I've shipped two volume mounts described in the following chart:

Host Mount|Container Mount|Purposse
---|---|---
./notebooks|/home/jovyan/notebooks|Used to make available your notebook's code and jars on all workers & master nodes.
./data|/home/jovyan/notebooks/data| Used to make available your notebook's data on all workers & master nodes.


