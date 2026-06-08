#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
ARG spark_base_image_prefix=apache/spark
ARG spark_base_image_tag=3.3.3-scala2.12-java11-ubuntu
# TARGETARCH is auto-populated by buildx (amd64 or arm64). scripts/createImage.sh
# tags the locally-built base as `<prefix>:<tag>-<arch>` per platform.
ARG TARGETARCH

FROM ${spark_base_image_prefix}:${spark_base_image_tag}-${TARGETARCH}

ARG scala_binary_version=2.13
ARG spark_version=3.3.3
ARG include_python=false

COPY target/armada-cluster-manager_${scala_binary_version}-*-all.jar /opt/spark/jars/
COPY target/armada-cluster-manager_${scala_binary_version}-*-connect-auth.jar /opt/spark/jars/
COPY extraFiles /opt/spark/extraFiles
COPY extraJars/* /opt/spark/jars
COPY docker/jupyter-entrypoint.sh /opt/spark/bin/jupyter-entrypoint.sh
COPY docker/armada-entrypoint.sh /opt/armada-entrypoint.sh


USER 0

# Move spark-core jar to a separate directory to ensure armada jar is first in the classpath
RUN mkdir -p /opt/spark/coreJars && \
    mv /opt/spark/jars/spark-core_*.jar /opt/spark/coreJars/

# Create a stable name for the examples jar
RUN rm -f /opt/spark/examples/jars/spark-examples.jar
RUN cp /opt/spark/examples/jars/spark-examples_*.jar /opt/spark/examples/jars/spark-examples.jar

ENV SPARK_DIST_CLASSPATH=/opt/spark/coreJars/*

# Install Jupyter, PySpark, and Python dependencies (only if include_python is true)
RUN if [ "$include_python" = "true" ]; then \
        apt-get update && \
        apt-get install -y python3-pip && \
        pip3 install --no-cache-dir \
            jupyter \
            notebook \
            ipykernel \
            pandas \
            pyarrow && \
        pip3 install --no-cache-dir --upgrade "pyspark[connect]==${spark_version}" && \
        apt-get clean && \
        rm -rf /var/lib/apt/lists/*; \
    fi


RUN if [ "$include_python" = "true" ]; then \
        mkdir -p /home/spark/workspace && \
        mkdir -p /home/spark/.local/share/jupyter && \
        mkdir -p /home/spark/.jupyter && \
        chown -R 185:185 /home/spark/workspace && \
        chown -R 185:185 /home/spark/.local && \
        chown -R 185:185 /home/spark/.jupyter; \
    fi && \
    chmod +x /opt/spark/bin/jupyter-entrypoint.sh

RUN chmod +x /opt/armada-entrypoint.sh

ARG spark_uid=185
USER ${spark_uid}

# Install ipykernel (only if include_python is true)
RUN if [ "$include_python" = "true" ]; then \
        HOME=/home/spark python3 -m ipykernel install --user --name python3 --display-name "Python 3"; \
    fi

ENV HOME=/home/spark
ENV SPARK_HOME=/opt/spark
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3
ENV PYTHONPATH=${SPARK_HOME}/python:${SPARK_HOME}/python/lib/py4j-*src.zip
ENV JUPYTER_RUNTIME_DIR=/home/spark/.local/share/jupyter/runtime

# Prevent config maps from being created
ENV HADOOP_CONF_DIR=

# Use Armada entrypoint wrapper that forwards ARMADA_GANG* env vars as
# SPARK_EXECUTOR_ATTRIBUTE_* for dynamic client mode, then delegates to
# the original Spark entrypoint.
ENTRYPOINT ["/opt/armada-entrypoint.sh"]
