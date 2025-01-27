#!/bin/bash

docker run -d --name=jupyter-iteso --network spark_cluster_default \
--volumes-from spark_cluster-spark-master-1 \
-p 8888:8888 \
spark-submit
