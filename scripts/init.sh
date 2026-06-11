#!/bin/bash

# Utility functions

root="$(cd "$(dirname "$0")/.."; pwd)"
scripts="$(cd "$(dirname "$0")"; pwd)"

if [ -e "$scripts/config.sh" ]; then
    source "$scripts/config.sh"
fi

# Default values for deploy mode and allocation
DEPLOY_MODE="${DEPLOY_MODE:-cluster}"
ALLOCATION_MODE="${ALLOCATION_MODE:-dynamic}"

# s3
ARMADA_S3_BUCKET_NAME=${ARMADA_S3_BUCKET_NAME:-kafka-s3}
ARMADA_S3_BUCKET_ENDPOINT=${ARMADA_S3_BUCKET_ENDPOINT:-http://192.168.59.6}
ARMADA_S3_USER_DIR=${ARMADA_USER_DIR:-s3a://$ARMADA_S3_BUCKET_NAME/$USER}

# benchmark
ARMADA_BENCHMARK_DATA=${ARMADA_BENCHMARK_DATA:-s3a://kafka-s3/data/benchmark/data/10t}
ARMADA_BENCHMARK_CLASS=${ARMADA_BENCHMARK_CLASS:-com.amazonaws.eks.tpcds.BenchmarkSQL}
ARMADA_BENCHMARK_TOOLS=${ARMADA_BENCHMARK_TOOLS:-/opt/tools/tpcds-kit/tools}

# Parse long options (--mode, --allocation) before getopts
ARGS=()
while [[ $# -gt 0 ]]; do
    case "$1" in
        --mode)
            DEPLOY_MODE="$2"
            shift 2
            ;;
        --allocation)
            ALLOCATION_MODE="$2"
            shift 2
            ;;
        *)
            ARGS+=("$1")
            shift
            ;;
    esac
done

# Restore remaining arguments for getopts
set -- "${ARGS[@]+"${ARGS[@]}"}"

print_usage () {
    echo ' Usage:'
    echo '   -h  help'
    echo '   -k  "use kind cluster"'
    echo '   -p  "build image with python"'
    echo '   -M, --mode <client|cluster>     "deploy mode (default: cluster)"'
    echo '   -A, --allocation <static|dynamic> "allocation type (default: dynamic)"'
    echo '   -i  <image-name>'
    echo '   -m  <armada-master-url>'
    echo '   -q  <armada-queue>'
    echo '   -P  <python script to run>'
    echo '   -s  <scala class to run>'
    echo '   -c  <class path to use>'
    echo '   -e  running e2e tests'
    echo ''
    echo 'Examples:'
    echo '   --mode cluster --allocation dynamic'
    echo '   --mode cluster --allocation static'
    echo '   --mode client --allocation dynamic'
    echo '   --mode client --allocation static'
    echo ''
    echo 'You also can specify those parameters in scripts/config.sh, like so:'
    echo '   IMAGE_NAME=spark:armada'
    echo '   ARMADA_MASTER=armada://localhost:30002'
    echo '   ARMADA_QUEUE=test'
    echo '   USE_KIND=true'
    echo '   INCLUDE_PYTHON=true'
    echo '   DEPLOY_MODE=cluster'
    echo '   ALLOCATION_MODE=dynamic'
    echo '   PYTHON_SCRIPT=/opt/spark/examples/src/main/python/pi.py'
    echo '   SCALA_CLASS=org.apache.spark.examples.SparkPi'
    echo '   CLASS_PATH=local:///opt/spark/extraFiles/spark-examples_2.12-3.5.3.jar'
    echo '   # Auth: Set ARMADA_AUTH_SCRIPT_PATH for authentication'
    exit 1
}

while getopts "hekpi:m:P:s:c:q:M:A:ef" opt; do
  case "$opt" in
    h) print_usage ;;
    k) USE_KIND=true ;;
    p) INCLUDE_PYTHON=true ;;
    i) IMAGE_NAME=$OPTARG ;;
    m) ARMADA_MASTER=$OPTARG ;;
    q) ARMADA_QUEUE=$OPTARG ;;
    P) PYTHON_SCRIPT=$OPTARG ;;
    s) SCALA_CLASS=$OPTARG ;;
    c) CLASSPATH=$OPTARG ;;
    e) RUNNING_E2E_TESTS=true ;;
    M) DEPLOY_MODE=$OPTARG ;;
    A) ALLOCATION_MODE=$OPTARG ;;
    f) USE_DISTRIBUTED_SHUFFLE_STORAGE=true ;;
  esac
done

export INCLUDE_PYTHON="${INCLUDE_PYTHON:-false}"
export USE_KIND="${USE_KIND:-false}"
export IMAGE_NAME="${IMAGE_NAME:-spark:armada}"
export ARMADA_MASTER="${ARMADA_MASTER:-armada://localhost:30002}"
export ARMADA_LOOKOUT_URL="${ARMADA_LOOKOUT_URL:-http://localhost:30000}"
export ARMADA_INTERNAL_URL="${ARMADA_INTERNAL_URL:-armada://armada-server.armada:50051}"
export ARMADA_QUEUE="${ARMADA_QUEUE:-test}"
export ARMADA_AUTH_TOKEN=${ARMADA_AUTH_TOKEN:-}
export ARMADA_AUTH_SCRIPT_PATH=${ARMADA_AUTH_SCRIPT_PATH:-}
export ARMADA_EVENT_WATCHER_USE_TLS=${ARMADA_EVENT_WATCHER_USE_TLS:-false}
export SPARK_BLOCK_MANAGER_PORT=${SPARK_BLOCK_MANAGER_PORT:-}
export SCALA_CLASS="${SCALA_CLASS:-org.apache.spark.examples.SparkPi}"
export RUNNING_E2E_TESTS="${RUNNING_E2E_TESTS:-false}"
export INIT_CONTAINER_IMAGE="${INIT_CONTAINER_IMAGE:-busybox:latest}"
export USE_DISTRIBUTED_SHUFFLE_STORAGE="${USE_DISTRIBUTED_SHUFFLE_STORAGE:-false}"
export SPARK_SECRET_KEY="${SPARK_SECRET_KEY:-armada-secret}"

# 'client' deployment mode requires SPARK_LOCAL_IP for Executor connectivity
# back to the Driver. Attempt to extract that by examining network interface
# addresses, and use the first one that is not the loopback interface, or a
# Docker/K8S virtual interface.
if [ -z "${SPARK_LOCAL_IP:-}" ]; then
  SPARK_LOCAL_IP=$(ifconfig -a | grep -w 'inet' | awk '{print $2}' | grep -Ev '^(127\.0\.0\.1|172\.)' | sed -n '1p')
  if [ "$SPARK_LOCAL_IP" = "" ] ; then
    echo ""
    echo "ERROR: could not determine external network interface address. In order to run Spark"
    echo "applications in client deployment mode, you may need to set SPARK_LOCAL_IP in your"
    echo "scripts/config.sh to the external IP address of this system, for example:"
    echo "  export SPARK_LOCAL_IP=\"192.168.1.555\""
    echo ""
    exit 1
  fi

  export SPARK_LOCAL_IP
fi

# Common Armada spark-submit conf args shared across all scripts
ARMADA_COMMON_CONF=(
    --conf spark.home=/opt/spark
    --conf spark.local.dir=/tmp
    --conf spark.armada.container.image=$IMAGE_NAME
    --conf spark.armada.queue=$ARMADA_QUEUE
    --conf spark.armada.lookouturl=${ARMADA_LOOKOUT_URL:-http://localhost:30000}
    --conf spark.kubernetes.executor.disableConfigMap=true
    --conf spark.kubernetes.driver.disableConfigMap=true
)

ARMADA_AUTH_ARGS=()
# Add auth script path if configured
if [ "$ARMADA_AUTH_SCRIPT_PATH" != "" ]; then
    ARMADA_AUTH_ARGS+=("--conf" "spark.armada.auth.script.path=$ARMADA_AUTH_SCRIPT_PATH")
fi

if [ "$ARMADA_EVENT_WATCHER_USE_TLS" != "" ]; then
    ARMADA_AUTH_ARGS+=("--conf" "spark.armada.eventWatcher.useTls=$ARMADA_EVENT_WATCHER_USE_TLS")
fi

# Forward the JWT to the driver pod so getAuthToken.sh can echo it from there.
# Without this, the driver pod's ArmadaClusterManagerBackend can't authenticate
# to armada-server, and the dynamic executor allocator never initializes.
if [ "${ARMADA_AUTH_TOKEN:-}" != "" ]; then
    ARMADA_AUTH_ARGS+=("--conf" "spark.kubernetes.driverEnv.ARMADA_AUTH_TOKEN=$ARMADA_AUTH_TOKEN")
fi

# OAuth proxy for the Spark UI. Requires cluster deploy mode + ingress.
# OAUTH_ENABLED follows the master AUTH_ENABLED switch unless set explicitly.
OAUTH_ENABLED="${OAUTH_ENABLED:-${AUTH_ENABLED:-false}}"
OAUTH_CONF=()
if [ "${OAUTH_ENABLED}" == "true" ]; then
    OAUTH_CONF=(
        --conf spark.armada.driver.ui.ingress.enabled=true
        --conf spark.armada.driver.ui.ingress.tls.enabled=${OAUTH_INGRESS_TLS:-false}
        --conf "spark.armada.driver.ui.ingress.annotations=nginx.ingress.kubernetes.io/server-alias=${OAUTH_INGRESS_HOST:-spark-ui.test}"
        --conf spark.armada.oauth.enabled=true
        --conf spark.armada.oauth.clientId="${OAUTH_CLIENT_ID:-spark-ui}"
        --conf spark.armada.oauth.clientSecret="${OAUTH_CLIENT_SECRET:-dex-spark-ui-secret}"
        --conf spark.armada.oauth.issuerUrl="${OAUTH_ISSUER_URL:-http://10.0.0.109:5556/dex}"
        --conf spark.armada.oauth.redirectUrl="${OAUTH_REDIRECT_URL:-http://spark-ui.test:9999/oauth2/callback}"
        --conf spark.armada.oauth.emailDomain="${OAUTH_EMAIL_DOMAIN:-*}"
        --conf spark.armada.oauth.cookieSecure="${OAUTH_COOKIE_SECURE:-false}"
        --conf spark.armada.oauth.passHostHeader="${OAUTH_PASS_HOST_HEADER:-true}"
    )
    [ -n "${OAUTH_INGRESS_CERT:-}" ] && \
        OAUTH_CONF+=(--conf spark.armada.driver.ui.ingress.certName=$OAUTH_INGRESS_CERT)
fi

# Build deploy-mode specific arguments array
DEPLOY_MODE_ARGS=()
if [ "$DEPLOY_MODE" = "client" ]; then
    DEPLOY_MODE_ARGS=(
        --conf spark.driver.host=$SPARK_DRIVER_HOST
        --conf spark.driver.port=$SPARK_DRIVER_PORT
        --conf spark.driver.bindAddress=0.0.0.0
    )
else
    DEPLOY_MODE_ARGS=(
        --conf spark.armada.internalUrl=$ARMADA_INTERNAL_URL
    )
fi

# Add block manager port if configured
if [ "$SPARK_BLOCK_MANAGER_PORT" != "" ]; then
    DEPLOY_MODE_ARGS+=("--conf" "spark.blockManager.port=$SPARK_BLOCK_MANAGER_PORT")
fi

DOCKER_ENV_ARGS=(-e SPARK_PRINT_LAUNCH_COMMAND=true)
if [ "$ARMADA_AUTH_TOKEN" != "" ]; then
    DOCKER_ENV_ARGS+=(-e "ARMADA_AUTH_TOKEN=$ARMADA_AUTH_TOKEN")
fi
if [ "${AWS_ACCESS_KEY_ID:-}" != "" ]; then
    DOCKER_ENV_ARGS+=(-e "AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID" -e "AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY")
fi

# Validation

if [[ "$DEPLOY_MODE" != "client" && "$DEPLOY_MODE" != "cluster" ]]; then
    echo "Error: --mode/-M must be either 'client' or 'cluster'"
    exit 1
fi
export DEPLOY_MODE

if [[ "$ALLOCATION_MODE" != "static" && "$ALLOCATION_MODE" != "dynamic" ]]; then
    echo "Error: --allocation/-A must be either 'static' or 'dynamic'"
    exit 1
fi
export ALLOCATION_MODE

if [ "$ALLOCATION_MODE" = "static" ]; then
    STATIC_MODE=true
else
    STATIC_MODE=false
fi
export STATIC_MODE

# Memory limits (overridable via config.sh or env)
EXECUTOR_MEMORY_LIMIT="${EXECUTOR_MEMORY_LIMIT:-1Gi}"
DRIVER_MEMORY_LIMIT="${DRIVER_MEMORY_LIMIT:-1Gi}"
ARMADA_NODE_UNIFORMITY_LABEL="${ARMADA_NODE_UNIFORMITY_LABEL:-armada-spark}"

# Allocation-mode conf args
STATIC_ALLOC_CONF=(
    --conf spark.executor.instances=2
    --conf spark.armada.executor.limit.memory=$EXECUTOR_MEMORY_LIMIT
    --conf spark.armada.executor.request.memory=$EXECUTOR_MEMORY_LIMIT
    --conf spark.armada.driver.limit.memory=$DRIVER_MEMORY_LIMIT
    --conf spark.armada.driver.request.memory=$DRIVER_MEMORY_LIMIT
)

DYNAMIC_ALLOC_CONF=(
    --conf spark.armada.scheduling.namespace=${ARMADA_NAMESPACE:-default}
    --conf spark.armada.executor.limit.memory=$EXECUTOR_MEMORY_LIMIT
    --conf spark.armada.executor.request.memory=$EXECUTOR_MEMORY_LIMIT
    --conf spark.armada.driver.limit.memory=$DRIVER_MEMORY_LIMIT
    --conf spark.armada.driver.request.memory=$DRIVER_MEMORY_LIMIT
    --conf spark.default.parallelism=10
    --conf spark.executor.instances=1
    --conf spark.sql.shuffle.partitions=5
    --conf spark.dynamicAllocation.enabled=true
    --conf spark.dynamicAllocation.minExecutors=2
    --conf spark.dynamicAllocation.maxExecutors=10
    --conf spark.dynamicAllocation.initialExecutors=2
    --conf spark.dynamicAllocation.executorIdleTimeout=5
    --conf spark.dynamicAllocation.schedulerBacklogTimeout=5
    --conf spark.armada.scheduling.nodeUniformity=$ARMADA_NODE_UNIFORMITY_LABEL
    --conf spark.armada.allocation.batchSize=4
    --conf spark.decommission.enabled=true
    --conf spark.storage.decommission.enabled=true
    --conf spark.storage.decommission.shuffleBlocks.enabled=true
)

if [ -z "${PYTHON_SCRIPT:-}" ]; then
    PYTHON_SCRIPT="/opt/spark/examples/src/main/python/pi.py"
else
    INCLUDE_PYTHON=true
fi

# derive Scala and Spark versions from pom.xml, set via ./scripts/set-version.sh
if [[ -z "${SCALA_VERSION:-}" ]]; then
  export SCALA_VERSION=$(cd "$scripts/.."; mvn help:evaluate -Dexpression=scala.version -q -DforceStdout)
  export SCALA_BIN_VERSION=$(cd "$scripts/.."; mvn help:evaluate -Dexpression=scala.binary.version -q -DforceStdout)
fi
if [[ -z "${SPARK_VERSION:-}" ]]; then
  export SPARK_VERSION=$(cd "$scripts/.."; mvn help:evaluate -Dexpression=spark.version -q -DforceStdout)
  export SPARK_BIN_VERSION=$(cd "$scripts/.."; mvn help:evaluate -Dexpression=spark.binary.version -q -DforceStdout)
fi

# When using DSS, validate Spark version + Scala version against known DSS base
# images and set DSS_PREFIX/DSS_TAG defaults
if [ "$USE_DISTRIBUTED_SHUFFLE_STORAGE" = "true" ]; then
    case "${SPARK_VERSION}:${SCALA_BIN_VERSION}" in
        3.3.4:2.12)
            DSS_PREFIX=${DSS_PREFIX:-gbj262/dss-334-1}
            DSS_BRANCH=${DSS_BRANCH:-fallback-storage-multithread-read-v3.3.4}
            ;;
        3.5.3:2.12)
            DSS_PREFIX=${DSS_PREFIX:-gbj262/dss-353-1}
            DSS_BRANCH=${DSS_BRANCH:-armada/push-task-result-to-driver-bm-v3.5.3}
            ;;
        4.1.1:2.13)
            DSS_PREFIX=${DSS_PREFIX:-gbj262/dss-411-1}
            DSS_BRANCH=${DSS_BRANCH:-fallback-storage-proactive}
            ;;
        *)
            echo "Error: unsupported Spark/Scala combination for DSS: ${SPARK_VERSION} / ${SCALA_BIN_VERSION}"
            exit 1
            ;;
    esac
    DSS_TAG=${DSS_TAG:-latest}
    export DSS_PREFIX DSS_TAG
fi

# Benchmark jar and download ID, keyed by Spark major version
if [[ "$SPARK_VERSION" == "4."* ]]; then
    ARMADA_BENCHMARK_JAR=${ARMADA_BENCHMARK_JAR:-local:///opt/spark/jars/armada-eks-spark-benchmark-assembly-411-1.0.jar}
    ARMADA_BENCHMARK_JAR_ID=${ARMADA_BENCHMARK_JAR_ID:-1fxbOFli52VQQyK2IX2WxEW1XAjWRU4XX}
else
    # The 353 benchmark jar works for 334 as well
    ARMADA_BENCHMARK_JAR=${ARMADA_BENCHMARK_JAR:-local:///opt/spark/jars/armada-eks-spark-benchmark-assembly-353-1.0.jar}
    ARMADA_BENCHMARK_JAR_ID=${ARMADA_BENCHMARK_JAR_ID:-1fjGRrLmbLygqdP-ugoTHLUbNMkTTxvcO}
fi
export ARMADA_BENCHMARK_JAR ARMADA_BENCHMARK_JAR_ID
export CLASS_PATH="${CLASS_PATH:-local:///opt/spark/examples/jars/spark-examples.jar}"

# check the Spark version is supported
if ! [ -e "$root/src/main/scala-spark-$SPARK_BIN_VERSION" ]; then
    echo "This tool does not support Spark version ${SPARK_VERSION}."
    exit 1
fi

# Distributed shuffle storage / fallback storage conf args
ARMADA_DSS_PATH="${ARMADA_DSS_PATH:-${ARMADA_S3_USER_DIR}/shuffle/}"
DISTRIBUTED_SHUFFLE_STORAGE_CONF=()
if [ "$USE_DISTRIBUTED_SHUFFLE_STORAGE" = "true" ]; then
    DISTRIBUTED_SHUFFLE_STORAGE_CONF=(
        --conf spark.storage.decommission.shuffleBlocks.maxDiskSize=0
        --conf spark.storage.decommission.fallbackStorage.path=$ARMADA_DSS_PATH
        --conf spark.storage.decommission.fallbackStorage.cleanUp=true
        --conf spark.storage.decommission.fallbackStorage.proactive.enabled=true
        --conf spark.storage.decommission.fallbackStorage.proactive.reliable=true
        --conf spark.shuffle.io.connectionCreationTimeout=10s
        --conf spark.shuffle.netty.connect.maxThreads=10
    )
fi

# DSS + client mode requires spark.app.id to be pre-set, otherwise
# FallbackStorage's `require(conf.contains("spark.app.id"))` fails before
# SparkContext gets a chance to assign it. See FallbackStorage.scala:53.
APP_ID_CONF=()
if  [ "$USE_DISTRIBUTED_SHUFFLE_STORAGE" = "true" ] && [ "$DEPLOY_MODE" = "client" ]; then
    APP_ID_CONF=(--conf "spark.app.id=armada-spark-job-$(uuidgen)")
fi

shift $((OPTIND - 1))
FINAL_ARGS=("${@:-}")   

if [ ${#FINAL_ARGS[@]} -eq 0 ]; then
    FINAL_ARGS+=("100")
fi

if [ "$INCLUDE_PYTHON" == "true" ]; then WITH_PYTHON="-python3"; else WITH_PYTHON=""; fi
image_tag="$SPARK_VERSION-scala$SCALA_BIN_VERSION-java${JAVA_VERSION:-17}$WITH_PYTHON-ubuntu"

S3_CONF=()
if [[ ${AWS_ACCESS_KEY_ID:-} != "" ]]; then
    S3_CONF=(
        --conf spark.hadoop.fs.s3a.access.key=$AWS_ACCESS_KEY_ID
        --conf spark.hadoop.fs.s3a.secret.key=$AWS_SECRET_ACCESS_KEY
    )
elif [[ ${ARMADA_SPARK_SECRET_KEY:-} != "" ]]; then
    S3_CONF=(
        --conf spark.kubernetes.driver.secretKeyRef.AWS_SECRET_ACCESS_KEY=$ARMADA_SPARK_SECRET_KEY:secret_key
        --conf spark.kubernetes.executor.secretKeyRef.AWS_SECRET_ACCESS_KEY=$ARMADA_SPARK_SECRET_KEY:secret_key
        --conf spark.kubernetes.driver.secretKeyRef.AWS_ACCESS_KEY_ID=$ARMADA_SPARK_SECRET_KEY:access_key
        --conf spark.kubernetes.executor.secretKeyRef.AWS_ACCESS_KEY_ID=$ARMADA_SPARK_SECRET_KEY:access_key
    )
fi
if [[ ${#S3_CONF[@]} -gt 0 && ${ARMADA_S3_BUCKET_ENDPOINT:-} != "" ]]; then
    S3_CONF+=(
        --conf spark.hadoop.fs.s3a.endpoint=$ARMADA_S3_BUCKET_ENDPOINT
        --conf spark.hadoop.fs.s3a.path.style.access=true
    )
fi

EVENT_LOG_CONF=()
if [ ${#S3_CONF[@]} -gt 0 ]; then
    ARMADA_EVENT_LOG_DIR="$ARMADA_S3_USER_DIR/eventLog"
    EVENT_LOG_CONF=(
        --conf spark.eventLog.enabled=true
        --conf spark.eventLog.createDir=true
        --conf spark.eventLog.dir=$ARMADA_EVENT_LOG_DIR
        --conf spark.history.fs.logDirectory=$ARMADA_EVENT_LOG_DIR
    )
fi

