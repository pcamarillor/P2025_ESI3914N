#!/bin/bash

export SPARK_MASTER_HOST=`hostname`
export SPARK_HOME=/opt/conda/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}-scala${SCALA_VERSION_BASE}

. "${SPARK_HOME}/sbin/spark-config.sh"

. "${SPARK_HOME}/bin/load-spark-env.sh"

mkdir -p $SPARK_WORKER_LOG
ln -sf /dev/stdout $SPARK_WORKER_LOG/spark-worker.out

cd ${SPARK_HOME}/bin/ && ${SPARK_HOME}/sbin/../bin/spark-class org.apache.spark.deploy.worker.Worker --webui-port $SPARK_WORKER_WEBUI_PORT $SPARK_MASTER >> $SPARK_WORKER_LOG/spark-worker.out
