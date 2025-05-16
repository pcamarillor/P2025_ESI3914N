FROM jupyter/base-notebook:latest
LABEL author="Pablo Camarillo" email="pablo.camarillo@iteso.mx"
LABEL version="0.3"

ENV DAEMON_RUN=true
ENV SPARK_VERSION=3.5.4
ENV HADOOP_VERSION=3
ENV SCALA_VERSION_BASE=2.13
ENV SCALA_VERSION=2.13.12
ENV SCALA_HOME=/usr/share/scala
ENV CONDA_HOME=/opt/conda
ENV SPARK_HOME=/opt/conda/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}-scala${SCALA_VERSION_BASE}


USER root
RUN wget --no-verbose https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}-scala${SCALA_VERSION_BASE}.tgz && tar -xvzf spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}-scala${SCALA_VERSION_BASE}.tgz \
      && mv spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}-scala${SCALA_VERSION_BASE} ${CONDA_HOME} \
      && rm spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}-scala${SCALA_VERSION_BASE}.tgz

RUN apt-get update && apt-get install -y openjdk-17-jdk curl vim software-properties-common ssh net-tools ca-certificates wget tar jq dbus-x11
RUN echo exit 0 > /usr/sbin/policy-rc.d

ENV PATH=${SPARK_HOME}/bin:$PATH
RUN pip install findspark
RUN export PATH="/usr/local/sbt/bin:$PATH" && mkdir -p "/usr/local/sbt" && wget -qO - --no-check-certificate "https://github.com/sbt/sbt/releases/download/v1.9.6/sbt-1.9.6.tgz" | tar xz -C /usr/local/sbt --strip-components=1 && sbt sbtVersion -Dsbt.rootdir=true

RUN cd "/tmp" && \
    wget --no-verbose "https://downloads.typesafe.com/scala/${SCALA_VERSION}/scala-${SCALA_VERSION}.tgz" && \
    tar xzf "scala-${SCALA_VERSION}.tgz" && \
    rm "/tmp/scala-${SCALA_VERSION}/bin/"*.bat && \
    rm -rf "${SCALA_HOME}" && \
    mkdir -p "${SCALA_HOME}/bin" && \
    mkdir -p "${SCALA_HOME}/lib" && \ 
    mv "/tmp/scala-${SCALA_VERSION}/bin" "/tmp/scala-${SCALA_VERSION}/lib" "${SCALA_HOME}" && \
    ln -sf "${SCALA_HOME}/bin/"* "/usr/bin/" && \
    rm -rf "/tmp/"*

# Fix the value of PYTHONHASHSEED
# Note: this is needed when you use Python 3.3 or greater
ENV PYTHONHASHSEED 1
