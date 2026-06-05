#!/bin/bash
set -euo pipefail

# Parse our flags before sourcing init.sh:
#   -C  Spark Connect mode
#   -J  Jupyter-only: skip the Connect server submit; just restart the Jupyter
#       container (reads ingress hostname from the existing K8s Ingress). Use after
#       your token expires or after editing the notebook.
USE_SPARK_CONNECT=false
SKIP_SUBMIT=false
for arg in "$@"; do
    case "$arg" in
        -C) USE_SPARK_CONNECT=true ;;
        -J) SKIP_SUBMIT=true ;;
    esac
done
# Remove our flags so init.sh doesn't see them
filtered_args=()
for arg in "$@"; do
    case "$arg" in
        -C|-J) ;;
        *) filtered_args+=("$arg") ;;
    esac
done
set -- "${filtered_args[@]+"${filtered_args[@]}"}"

if [ "$USE_SPARK_CONNECT" = true ]; then
    # Spark Connect mode: driver runs in cluster, Jupyter is a thin gRPC client.
    # Allocation mode comes from -A (init.sh defaults it to dynamic); the connect
    # submit below honors static vs dynamic when building spark-submit args.
    export DEPLOY_MODE=cluster
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
if [ "$USE_SPARK_CONNECT" = true ] && [ "$SKIP_SUBMIT" = false ]; then
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

    echo "Submitting Spark Connect server to Armada (cluster mode, $ALLOCATION_MODE allocation)..."

    # Build spark-submit args (same pattern as submitArmadaSpark.sh).
    # ARMADA_COMMON_CONF (from init.sh) supplies spark.home, spark.local.dir,
    # container image, queue, lookout URL, and disableConfigMap; only the
    # connect-specific confs are listed explicitly here.
    SPARK_SUBMIT_ARGS=(
        --master $ARMADA_MASTER
        --deploy-mode cluster
        --name spark-connect-server
        --class org.apache.spark.sql.connect.service.SparkConnectServer
        ${S3_CONF[@]+"${S3_CONF[@]}"}
        ${ARMADA_COMMON_CONF[@]+"${ARMADA_COMMON_CONF[@]}"}
        --conf spark.connect.grpc.binding.port=$CONNECT_PORT
        --conf spark.armada.scheduling.namespace=${ARMADA_NAMESPACE:-default}
        --conf spark.armada.scheduling.nodeUniformity=${ARMADA_NODE_UNIFORMITY_LABEL:-armada-spark}
        --conf spark.armada.executor.limit.memory=$EXECUTOR_MEMORY_LIMIT
        --conf spark.armada.executor.request.memory=$EXECUTOR_MEMORY_LIMIT
        --conf spark.armada.driver.limit.memory=$DRIVER_MEMORY_LIMIT
        --conf spark.armada.driver.request.memory=$DRIVER_MEMORY_LIMIT
        --conf spark.jars.ivy=/tmp/.ivy
        --conf spark.armada.driver.watchEnabled=false
    )

    # Allocation mode (-A static|dynamic, same contract as submitArmadaSpark.sh).
    # Dynamic is the usual choice for a Connect server: it is a long-lived service,
    # so minExecutors=0 lets it scale to zero while idle and back up on demand.
    # Static pins a fixed executor count for the server's entire lifetime.
    if [ "$ALLOCATION_MODE" = "static" ]; then
        SPARK_SUBMIT_ARGS+=(
            --conf spark.executor.instances=${EXECUTOR_INSTANCES:-2}
        )
    else
        SPARK_SUBMIT_ARGS+=(
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
    fi

    # Add deploy mode args (internalUrl for cluster mode)
    SPARK_SUBMIT_ARGS+=(${DEPLOY_MODE_ARGS[@]+"${DEPLOY_MODE_ARGS[@]}"})

    # Add auth args
    SPARK_SUBMIT_ARGS+=(${ARMADA_AUTH_ARGS[@]+"${ARMADA_AUTH_ARGS[@]}"})

    # Add event log conf
    SPARK_SUBMIT_ARGS+=(${EVENT_LOG_CONF[@]+"${EVENT_LOG_CONF[@]}"})

    # Auto-enable JWT auth when SPARK_ARMADA_CONNECT_OWNER is set.
    # Skip auth for one run with: SPARK_ARMADA_CONNECT_OWNER= ./scripts/runJupyter.sh -C
    if [ -n "${SPARK_ARMADA_CONNECT_OWNER:-}" ]; then
        SPARK_SUBMIT_ARGS+=(
            --conf spark.connect.grpc.interceptor.classes=io.armadaproject.spark.connect.auth.JwtAuthInterceptor
            --conf spark.kubernetes.driverEnv.SPARK_ARMADA_CONNECT_OWNER=$SPARK_ARMADA_CONNECT_OWNER
        )
        [ -n "${OIDC_ISSUER_URL:-}" ] && SPARK_SUBMIT_ARGS+=(--conf spark.kubernetes.driverEnv.OIDC_ISSUER_URL=$OIDC_ISSUER_URL)
        [ -n "${OIDC_USER_CLAIM:-}" ] && SPARK_SUBMIT_ARGS+=(--conf spark.kubernetes.driverEnv.OIDC_USER_CLAIM=$OIDC_USER_CLAIM)
        [ -n "${OIDC_AUDIENCE:-}" ]   && SPARK_SUBMIT_ARGS+=(--conf spark.kubernetes.driverEnv.OIDC_AUDIENCE=$OIDC_AUDIENCE)
        [ -n "${OIDC_JWKS_URL:-}" ]   && SPARK_SUBMIT_ARGS+=(--conf spark.kubernetes.driverEnv.OIDC_JWKS_URL=$OIDC_JWKS_URL)
    fi

    # Auto-enable Armada ingress for the gRPC port when SPARK_CONNECT_INGRESS=true.
    # Skip with: SPARK_CONNECT_INGRESS=false ./scripts/runJupyter.sh -C
    # SPARK_CONNECT_INGRESS_TLS=true + SPARK_CONNECT_INGRESS_CERT=<secret-name> turns
    # on TLS termination at the ingress, using the named K8s TLS Secret.
    if [ "${SPARK_CONNECT_INGRESS:-false}" = "true" ]; then
        SPARK_SUBMIT_ARGS+=(
            --conf spark.armada.driver.ingress.enabled=true
            --conf spark.armada.driver.ingress.port=$CONNECT_PORT
            --conf spark.armada.driver.ingress.tls.enabled=${SPARK_CONNECT_INGRESS_TLS:-false}
            --conf spark.armada.driver.ingress.annotations=nginx.ingress.kubernetes.io/backend-protocol=GRPC
        )
        [ -n "${SPARK_CONNECT_INGRESS_CERT:-}" ] && \
            SPARK_SUBMIT_ARGS+=(--conf spark.armada.driver.ingress.certName=$SPARK_CONNECT_INGRESS_CERT)
    fi

    # Add primary resource
    SPARK_SUBMIT_ARGS+=($CONNECT_JAR_REMOTE)

    # Cluster-mode submit: submits the driver + executor jobs to Armada and exits.
    docker run \
      --rm --network host \
      "${DOCKER_ENV_ARGS[@]}" \
      -v "$root/conf:/opt/spark/conf" \
      $IMAGE_NAME \
      /opt/spark/bin/spark-submit "${SPARK_SUBMIT_ARGS[@]}"

    echo ""
    echo "Spark Connect server submitted."
    echo ""
fi

# Capture the ingress hostname so the Jupyter container's DNS can be wired to reach it.
# Runs for both `-C` (after submit, polls up to 60s) and `-C -J` (reads existing Ingress).
# Without this, the container resolves the hostname via the host's /etc/hosts -> 127.0.0.1,
# which inside the container is the container's own loopback (not the host).
if [ "$USE_SPARK_CONNECT" = true ] && [ "${SPARK_CONNECT_INGRESS:-false}" = "true" ]; then
    if [ "${SPARK_CONNECT_INGRESS_TLS:-false}" = "true" ]; then
        INGRESS_PORT_DEFAULT="${SPARK_CONNECT_INGRESS_PORT:-9443}"
    else
        INGRESS_PORT_DEFAULT="${SPARK_CONNECT_INGRESS_PORT:-9999}"
    fi
    INGRESS_HOST=""
    if [ "$SKIP_SUBMIT" = true ]; then
        INGRESS_HOST=$(kubectl get ingress -n "${ARMADA_NAMESPACE:-default}" \
            -o jsonpath='{.items[?(@.metadata.labels.armada_job_id)].spec.rules[0].host}' \
            2>/dev/null | awk '{print $1}')
        if [ -z "$INGRESS_HOST" ]; then
            echo "Error: -J skips submit but no existing driver Ingress was found."
            echo "Run './scripts/runJupyter.sh -C' first to submit the Spark Connect server."
            exit 1
        fi
    else
        # Timeout configurable via SPARK_CONNECT_INGRESS_WAIT (default 120s). Override
        # with SPARK_CONNECT_INGRESS_WAIT=300 ./scripts/runJupyter.sh -C if the cluster
        # is slow to schedule the driver pod.
        WAIT_SECS="${SPARK_CONNECT_INGRESS_WAIT:-120}"
        echo "Waiting up to ${WAIT_SECS}s for Armada to create the driver Ingress..."
        ATTEMPTS=$((WAIT_SECS / 2))
        for _i in $(seq 1 "$ATTEMPTS"); do
            INGRESS_HOST=$(kubectl get ingress -n "${ARMADA_NAMESPACE:-default}" \
                -o jsonpath='{.items[?(@.metadata.labels.armada_job_id)].spec.rules[0].host}' \
                2>/dev/null | awk '{print $1}')
            [ -n "$INGRESS_HOST" ] && break
            sleep 2
        done
        [ -z "$INGRESS_HOST" ] && \
            echo "WARNING: No Ingress appeared within ${WAIT_SECS}s. Set SPARK_CONNECT_HOST manually, or re-run with SPARK_CONNECT_INGRESS_WAIT=<more>."
    fi
    if [ -n "$INGRESS_HOST" ]; then
        echo "Ingress hostname: $INGRESS_HOST"
        export SPARK_CONNECT_HOST="$INGRESS_HOST"
        export SPARK_CONNECT_PORT="$INGRESS_PORT_DEFAULT"
    fi
elif [ "$USE_SPARK_CONNECT" = true ] && [ "$SKIP_SUBMIT" = false ]; then
    echo "Port-forward to the driver pod before using Jupyter:"
    echo "  kubectl port-forward -n ${ARMADA_NAMESPACE:-default} \$(kubectl get pod -n ${ARMADA_NAMESPACE:-default} -l spark-role=driver,spark-app-name=spark-connect-server -o name | head -1) $CONNECT_PORT:$CONNECT_PORT"
fi
echo ""

# ── Start Jupyter container ──

# Remove existing container if it exists
if docker ps -a --format '{{.Names}}' | grep -q "^armada-jupyter$"; then
    echo "Removing existing armada-jupyter container..."
    docker rm -f armada-jupyter >/dev/null 2>&1 || true
fi

if [ "$USE_SPARK_CONNECT" = true ]; then
    # Spark Connect mode: thin client, no driver ports needed.
    # SPARK_CONNECT_TOKEN / HOST / PORT are forwarded so the notebook can read
    # them from os.environ and assemble the remote URL (token = bearer auth,
    # host/port = ingress vs. port-forward target).
    #
    # When an ingress hostname is known (SPARK_CONNECT_HOST is the per-job FQDN),
    # add a docker --add-host mapping to host-gateway. Otherwise the container's
    # DNS resolves the hostname via the host's /etc/hosts -> 127.0.0.1, which inside
    # the container is the container's own loopback (not the host) -> connection refused.
    ADD_HOST_ARGS=()
    if [ -n "${SPARK_CONNECT_HOST:-}" ] && [ "$SPARK_CONNECT_HOST" != "host.docker.internal" ]; then
        ADD_HOST_ARGS=(--add-host "${SPARK_CONNECT_HOST}:host-gateway")
    fi
    # gRPC inside the container uses its own trust store and does NOT see CAs from the
    # laptop's system keychain. When the ingress uses a custom/self-signed cert (mkcert,
    # cert-manager local CA), mount the CA into the container and point gRPC at it.
    # Default to mkcert's rootCA.pem; override with SPARK_CONNECT_TLS_CA_PATH.
    TLS_CA_ARGS=()
    DEFAULT_MKCERT_CA="${HOME}/Library/Application Support/mkcert/rootCA.pem"
    TLS_CA_PATH="${SPARK_CONNECT_TLS_CA_PATH:-$DEFAULT_MKCERT_CA}"
    if [ "${SPARK_CONNECT_INGRESS_TLS:-false}" = "true" ] && [ -f "$TLS_CA_PATH" ]; then
        TLS_CA_ARGS=(
            -v "${TLS_CA_PATH}:/etc/ssl/certs/spark-connect-ca.pem:ro"
            -e GRPC_DEFAULT_SSL_ROOTS_FILE_PATH=/etc/ssl/certs/spark-connect-ca.pem
        )
    fi
    docker run -d \
      --name armada-jupyter \
      -p ${JUPYTER_PORT}:8888 \
      -e SPARK_CONNECT_TOKEN="${SPARK_CONNECT_TOKEN:-}" \
      -e SPARK_CONNECT_HOST="${SPARK_CONNECT_HOST:-host.docker.internal}" \
      -e SPARK_CONNECT_PORT="${SPARK_CONNECT_PORT:-$CONNECT_PORT}" \
      "${ADD_HOST_ARGS[@]+"${ADD_HOST_ARGS[@]}"}" \
      "${TLS_CA_ARGS[@]+"${TLS_CA_ARGS[@]}"}" \
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
