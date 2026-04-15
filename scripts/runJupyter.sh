#!/bin/bash
set -euo pipefail

# Parse -C (Spark Connect) flag before sourcing init.sh
USE_SPARK_CONNECT=false
for arg in "$@"; do
    if [ "$arg" = "-C" ]; then
        USE_SPARK_CONNECT=true
        break
    fi
done
# Remove -C from args so init.sh doesn't see it
filtered_args=()
for arg in "$@"; do
    if [ "$arg" != "-C" ]; then
        filtered_args+=("$arg")
    fi
done
set -- "${filtered_args[@]+"${filtered_args[@]}"}"

if [ "$USE_SPARK_CONNECT" = true ]; then
    # Spark Connect mode: driver runs in cluster, Jupyter is a thin gRPC client
    export DEPLOY_MODE=cluster
    export ALLOCATION_MODE=dynamic
else
    # Client mode: driver runs inside the Jupyter container
    export DEPLOY_MODE="${DEPLOY_MODE:-client}"
fi

# init environment variables
scripts="$(cd "$(dirname "$0")"; pwd)"
root="$(cd "$scripts/.."; pwd)"
source "$scripts/init.sh"

# Jupyter-specific defaults
JUPYTER_PORT="${JUPYTER_PORT:-8888}"
CONNECT_PORT="${CONNECT_PORT:-15002}"
SPARK_BLOCK_MANAGER_PORT="${SPARK_BLOCK_MANAGER_PORT:-10061}"
SPARK_DRIVER_PORT="${SPARK_DRIVER_PORT:-7078}"

# Memory limits (shared with submitArmadaSpark.sh)
EXECUTOR_MEMORY_LIMIT="${EXECUTOR_MEMORY_LIMIT:-1Gi}"
DRIVER_MEMORY_LIMIT="${DRIVER_MEMORY_LIMIT:-1Gi}"

if [ "$USE_SPARK_CONNECT" = false ]; then
    # SPARK_DRIVER_HOST is required - must be reachable from Kubernetes executors
    if [ -z "${SPARK_DRIVER_HOST:-}" ]; then
        echo "Error: SPARK_DRIVER_HOST must be set."
        echo ""
        exit 1
    fi
fi

if [ "${USE_KIND}" == "true" ]; then
    # Ensure queue exists on Armada
    if ! armadactl get queue $ARMADA_QUEUE >& /dev/null; then
        armadactl create queue $ARMADA_QUEUE
    fi

    # needed by kind load docker-image (if docker is installed via snap)
    # https://github.com/kubernetes-sigs/kind/issues/2535
    export TMPDIR="$scripts/.tmp"
    mkdir -p "$TMPDIR"
    kind load docker-image $IMAGE_NAME --name armada
fi

# Setup workspace directory
notebooks_dir="$root/example/jupyter/notebooks"
workspace_dir="$root/example/jupyter/workspace"

# Create workspace directory if it doesn't exist
mkdir -p "$workspace_dir"

# Copy example notebooks to workspace only if they don't already exist
if [ -d "$notebooks_dir" ]; then
    for notebook in "$notebooks_dir"/*.ipynb; do
        [ -f "$notebook" ] || break
        notebook_name=$(basename "$notebook")
        if [ ! -f "$workspace_dir/$notebook_name" ]; then
            echo "Copying $notebook_name to workspace..."
            cp "$notebook" "$workspace_dir/"
        fi
    done
fi

# ── Spark Connect: submit driver + executors to Armada ──
if [ "$USE_SPARK_CONNECT" = true ]; then
    CONNECT_JAR_NAME="spark-connect_${SCALA_BIN_VERSION}-${SPARK_VERSION}.jar"
    CONNECT_JAR_LOCAL="$root/extraJars/$CONNECT_JAR_NAME"
    CONNECT_JAR_REMOTE="local:///opt/spark/jars/$CONNECT_JAR_NAME"
    MAVEN_URL="https://repo1.maven.org/maven2/org/apache/spark/spark-connect_${SCALA_BIN_VERSION}/${SPARK_VERSION}/${CONNECT_JAR_NAME}"

    if [ ! -f "$CONNECT_JAR_LOCAL" ]; then
        echo "spark-connect JAR not found in extraJars/. Downloading..."
        if ! curl -sfL -o "$CONNECT_JAR_LOCAL" "$MAVEN_URL"; then
            echo "Error: Failed to download $CONNECT_JAR_NAME"
            rm -f "$CONNECT_JAR_LOCAL"
            exit 1
        fi
        echo "Downloaded $CONNECT_JAR_NAME"
        echo ">>> Rebuild the image: ./scripts/createImage.sh"
        exit 0
    fi

    echo "Submitting Spark Connect server to Armada (cluster mode)..."

    # Build spark-submit args (same pattern as submitArmadaSpark.sh)
    SPARK_SUBMIT_ARGS=(
        --master $ARMADA_MASTER
        --deploy-mode cluster
        --name spark-connect-server
        --class org.apache.spark.sql.connect.service.SparkConnectServer
        ${S3_CONF[@]+"${S3_CONF[@]}"}
        --conf spark.connect.grpc.binding.port=$CONNECT_PORT
        --conf spark.home=/opt/spark
        --conf spark.armada.container.image=$IMAGE_NAME
        --conf spark.armada.queue=$ARMADA_QUEUE
        --conf spark.armada.lookouturl=${ARMADA_LOOKOUT_URL:-http://localhost:30000}
        --conf spark.armada.scheduling.namespace=${ARMADA_NAMESPACE:-default}
        --conf spark.armada.scheduling.nodeUniformity=${ARMADA_NODE_UNIFORMITY_LABEL:-armada-spark}
        --conf spark.armada.executor.limit.memory=$EXECUTOR_MEMORY_LIMIT
        --conf spark.armada.executor.request.memory=$EXECUTOR_MEMORY_LIMIT
        --conf spark.armada.driver.limit.memory=$DRIVER_MEMORY_LIMIT
        --conf spark.armada.driver.request.memory=$DRIVER_MEMORY_LIMIT
        --conf spark.kubernetes.executor.disableConfigMap=true
        --conf spark.local.dir=/tmp
        --conf spark.jars.ivy=/tmp/.ivy
        --conf spark.dynamicAllocation.enabled=true
        --conf spark.dynamicAllocation.minExecutors=0
        --conf spark.dynamicAllocation.maxExecutors=5
        --conf spark.dynamicAllocation.executorIdleTimeout=60
        --conf spark.dynamicAllocation.schedulerBacklogTimeout=5
        --conf spark.dynamicAllocation.initialExecutors=1
        --conf spark.dynamicAllocation.shuffleTracking.enabled=false
        --conf spark.decommission.enabled=true
        --conf spark.storage.decommission.enabled=true
        --conf spark.storage.decommission.shuffleBlocks.enabled=true
    )

    # Add deploy mode args (internalUrl for cluster mode)
    SPARK_SUBMIT_ARGS+=(${DEPLOY_MODE_ARGS[@]+"${DEPLOY_MODE_ARGS[@]}"})

    # Add auth args
    SPARK_SUBMIT_ARGS+=(${ARMADA_AUTH_ARGS[@]+"${ARMADA_AUTH_ARGS[@]}"})

    # Add event log conf
    SPARK_SUBMIT_ARGS+=(${EVENT_LOG_CONF[@]+"${EVENT_LOG_CONF[@]}"})

    # Add primary resource
    SPARK_SUBMIT_ARGS+=($CONNECT_JAR_REMOTE)

    # Cluster-mode submit: submits the job and exits (non-zero exit is OK)
    docker run \
      --rm --network host \
      "${DOCKER_ENV_ARGS[@]}" \
      $IMAGE_NAME \
      /opt/spark/bin/spark-submit "${SPARK_SUBMIT_ARGS[@]}" || true

    echo ""
    echo "Spark Connect server submitted."
    echo ""
    echo "Port-forward to the driver pod before using Jupyter:"
    echo "  kubectl port-forward \$(kubectl get pods -n default -o name | head -1) $CONNECT_PORT:$CONNECT_PORT"
    echo ""
fi

# ── Start Jupyter container ──

# Remove existing container if it exists
if docker ps -a --format '{{.Names}}' | grep -q "^armada-jupyter$"; then
    echo "Removing existing armada-jupyter container..."
    docker rm -f armada-jupyter >/dev/null 2>&1 || true
fi

if [ "$USE_SPARK_CONNECT" = true ]; then
    # Spark Connect mode: thin client, no driver ports needed
    docker run -d \
      --name armada-jupyter \
      -p ${JUPYTER_PORT}:8888 \
      -v "$workspace_dir:/home/spark/workspace" \
      --rm \
      ${IMAGE_NAME} \
      /opt/spark/bin/jupyter-entrypoint.sh
else
    # Client mode: driver runs inside, needs ports exposed
    docker run -d \
      --name armada-jupyter \
      -p ${JUPYTER_PORT}:8888 \
      -p ${SPARK_BLOCK_MANAGER_PORT}:${SPARK_BLOCK_MANAGER_PORT} \
      -p ${SPARK_DRIVER_PORT}:${SPARK_DRIVER_PORT} \
      -e ARMADA_MASTER=${ARMADA_MASTER} \
      -e ARMADA_COMMON_CONF="${ARMADA_COMMON_CONF[*]:-}" \
      -e ARMADA_AUTH_ARGS="${ARMADA_AUTH_ARGS[*]:-}" \
      -e DEPLOY_MODE_ARGS="${DEPLOY_MODE_ARGS[*]:-}" \
      -e DYNAMIC_ALLOC_CONF="${DYNAMIC_ALLOC_CONF[*]:-}" \
      -e STATIC_ALLOC_CONF="${STATIC_ALLOC_CONF[*]:-}" \
      -e DISTRIBUTED_SHUFFLE_STORAGE_CONF="${DISTRIBUTED_SHUFFLE_STORAGE_CONF[*]:-}" \
      -e ALLOCATION_MODE=${ALLOCATION_MODE} \
      -v "$workspace_dir:/home/spark/workspace" \
      -v "$root/conf:/opt/spark/conf:ro" \
      --rm \
      ${IMAGE_NAME} \
      /opt/spark/bin/jupyter-entrypoint.sh
fi

# Wait for Jupyter server to be reachable
for i in {1..10}; do
    if curl -s -f -o /dev/null "http://localhost:${JUPYTER_PORT}" 2>/dev/null; then
        echo "Jupyter notebook is running at http://localhost:${JUPYTER_PORT}"
        echo "Workspace is available in the container at /home/spark/workspace"
        echo "Notebooks are persisted in $workspace_dir"
        if [ "$USE_SPARK_CONNECT" = true ]; then
            echo ""
            echo "Using Spark Connect mode."
            echo "Connect in notebook with: SparkSession.builder.remote('sc://host.docker.internal:$CONNECT_PORT').getOrCreate()"
        fi
        exit 0
    fi
    sleep 1
done

echo "Error: Jupyter server is not reachable. The container may have exited."
echo "This likely means Python/Jupyter is not installed in the image (INCLUDE_PYTHON=false)."
exit 1
