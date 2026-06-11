# All About `armada-spark`

# Quickstart

Make sure you have [Java](https://openjdk.org/)(8, 11, or 17),
[Scala](https://www.scala-lang.org/)(2.12 or 2.13),
and [Apache Maven](https://maven.apache.org/)(3.9.6+) installed.
You also need Docker to build the `armada-spark` container image for use with
Armada.

Clone `armada-spark` to a local directory, then run:
```bash
./scripts/set-version.sh 3.5.5 2.13.8
```
This sets up `armada-spark` to be built with Spark 3.5.5 and Scala 2.13.8.

Then build with:
```bash
mvn clean package
```

Assuming everything built, you can then create an image to be used with
Kubernetes with `./scripts/createImage.sh`. Here's an example:

```bash
./scripts/createImage.sh -i armada-spark -m armada://localhost:30002 -q default -l http://localhost:30000
```

This builds the necessary image that runs armada-spark capable Spark Drivers
and Executors. The image will be called `armada-spark`. The armada server URL
will be `armada://localhost:30002`. The Armada queue used will be `default`.
And finally, the lookout URL will be set to `http://localhost:30000`.

To use the image you'll need an Armada instance along with the ability to load
the image to whatever clusters Armada is overseeing.

Armada requires certain configurations to be set. An example configuration 
of Armada services with all the necessary options configured can be found by 
applying this [patch](../e2e/armada-operator.patch)
to the [Armada Operator Dev Quickstart Config](https://github.com/armadaproject/armada-operator/blob/main/dev/quickstart/armada-crs.yaml)

The [README](../README.md) covers all this in more detail.

## Running `armada-spark` Scala example

`armada-spark`'s main entry point is a class called
[`ArmadaSparkSubmit`](../src/main/scala-spark-4.1/org/apache/spark/deploy/ArmadaSparkSubmit.scala).
It's designed to be a drop-in replacement/extension for `spark-submit`.
Upstream integration is not quite there yet, but we're working on getting
it [accepted](https://github.com/apache/spark/pull/50770); for now,
we must load it via the
[`spark-class`](https://github.com/apache/spark/blob/master/bin/spark-class)
helper program instead of using `spark-submit` directly.

Here's an example of calling `ArmadaSparkSubmit`:

```bash
 $ /opt/spark/bin/spark-class org.apache.spark.deploy.ArmadaSparkSubmit \
    --master armada://localhost:50051 --deploy-mode cluster \
    --name spark-pi \
    --class org.apache.spark.examples.SparkPi \
    --conf spark.executor.instances=4 \
    --conf spark.armada.container.image=armada-spark \
    --conf spark.armada.lookouturl=http://localhost:30000 \
    --conf spark.armada.scheduling.nodeSelectors="armada-spark=true" \
    "local:///opt/spark/examples/jars/spark-examples_${SCALA_BIN_VERSION}-${SPARK_VERSION}.jar"
```

Quite a handful, but let's break down some of the important bits:

The first argument to `spark-class` is the fully-qualified Java classpath to
`ArmadaSparkSubmit`. This will load and execute `ArmadaSparkSubmit.main` which
will behave almost exactly like the base `spark-submit`, but
`ArmadaSparkSubmit` knows how to parse and handle `armada` protocol URLs.

The `--master` flag specifies the master URL for spark to use to submit work.
If we break down the URL we have the protocol `armada`, and a (hostname, port)
tuple, in this case: (`localhost`, `50051`). So `ArmadaSparkSubmit` will try to submit
Armada jobs to `localhost` on port `50051`.

The next important flag comes from `--class` which tells `ArmadaSparkSubmit` to
load `org.apache.spark.examples.SparkPi` as the main workload program.

Then comes several configuration options specified with `--conf` flags:
- `--conf spark.executor.instances=4` The number of Executor jobs/instances to
  launch. `armada-spark` only supports static Executors at the moment.
- `--conf spark.armada.container.image=armada-spark` lets us know what
  container image `ArmadaSparkSubmit` should use for the Driver and Executors.
- `--conf spark.armada.lookouturl=http://localhost:30000` gives the Lookout
    URL so we can easily find the results of jobs submitted to Armada.
- `--conf spark.armada.scheduling.nodeSelectors="armada-spark=true"` helps Armada pick
    a specific kubernetes cluster to use in order to schedule Executors and
    Drivers to the same cluster.

And last, but not least: `"local:///opt/spark/examples/jars/spark-examples_${SCALA_BIN_VERSION}-${SPARK_VERSION}.jar"`
A jar which contains the Java class the Driver and Executors will load. This
will be located within the container image specified earlier.

Once called, `armada-spark` will submit a Driver job and a job for each of the
Executors. Through some magic we'll discuss a bit later, the Driver and
Executors will coordinate to accomplish whatever work is laid out in the loaded
`--class`. Here's output from a run of `ArmadaSparkSubmit`:

```
Armada selected as cluster manager.
ArmadaClientApplication.start() called!
host is localhost, port is 50051
Submit health good!
Executor JobID: 01jt6bmrszmnk4apxby6e895d8  Error: None
Executor JobID: 01jt6bmrszmnk4apxby6xrwer6  Error: None
Executor JobID: 01jt6bmrt0bvv5dh3k5k1qc8ta  Error: None
Executor JobID: 01jt6bmrt0bvv5dh3k5mqkeqms  Error: None
Driver JobID: 01jt6bmrthrtzgdwn7tad307nk  Error: None
Lookout URL for this job is http://localhost:30000/?page=0&sort[id]=jobId&sort[desc]=true&ps=50&sb=01jt6bmrthrtzgdwn7tad307nk&active=false&refresh=true
25/05/01 16:42:27 INFO ShutdownHookManager: Shutdown hook called
25/05/01 16:42:27 INFO ShutdownHookManager: Deleting directory /tmp/spark-a5c32faa-d3e4-4d5d-82a6-cdb79bac7984
```

Note the Lookout URL. This will let us easily view results for the Driver and
its Executors. If all goes well the jobs will succeed and produce a result!

### Running Python example

`armada-spark` also supports running Python jobs instead of Java.

The invocation is mostly the same:
```bash
 $ /opt/spark/bin/spark-class org.apache.spark.deploy.ArmadaSparkSubmit \
    --master armada://localhost:50051 --deploy-mode cluster \
    --name python-pi \
    --conf spark.executor.instances=4 \
    --conf spark.armada.container.image=armada-spark \
    --conf spark.armada.lookouturl=http://localhost:30000 \
    --conf spark.armada.scheduling.nodeSelectors="armada-spark=true" \
    /opt/spark/examples/src/main/python/pi.py
```

With a couple of critical differences:
- The `--class` flag is absent.
- Instead of passing a `jar` file as the final argument, a Python script is specified.

Execution will proceed similarly as in the Java case.

### submitArmadaSpark.sh Convenience script

There is a convience script `./scripts/submitArmadaSpark.sh`, that makes it easier to submit jobs to the cluster.  There are many options; run it with `-h` to see them all.  By default, it will run the sparkPi example jobs.  To run your own jobs, you can run it like so:

```
./scripts/submitArmadaSpark.sh -P /opt/spark/extraFiles/src/main/python/pi.py 1000
```
Option `-P` allows you to run your own Python scripts.  Run Scala / Java programs like so:
```
./scripts/submitArmadaSpark.sh -s org.apache.spark.examples.SparkPi -c local:///opt/spark/extraFiles/jars/spark-examples_2.12-3.5.3.jar 102
```


### Running the Spark UI
To run the spark ui, you must get access to port 4040 on the driver, like so:
```
kubectl --context <cluster> -n <namespace> port-forward <spark-driver-pod-name> 4040:4040
```
You can get the specific cluster, namespace, podName details on the driver job's "Commands" page in the Lookout UI. The "driver" container on the Lookout UI "Details" page refers to the driver job.

## Configuration Options

`armada-spark` has a slew of configuration options that can be set with the
`--conf` flag.

Each option consists of a `String` unless otherwise noted.

They can be set in the [conf](../conf/spark-defaults.conf) file.

- `ARMADA_AUTH_TOKEN` - Environment variable for Armada auth token (for initial submission). For further authentication between driver and executors, configure `spark.armada.auth.script.path` to point to an authentication script.
- `spark.armada.auth.script.path` - Path to an executable script for obtaining authentication tokens in cluster mode. The script should output the token to stdout. If not specified, no script-based authentication will be attempted.

- `spark.armada.container.image` - Container image to use for Spark containers.
- `spark.armada.driver.limit.cores` - Specify the hard cpu limit for the driver pod
- `spark.armada.driver.request.cores` - Specify the cpu request for the driver pod
- `spark.armada.driver.limit.memory` - Specify the hard memory limit for the driver pod
- `spark.armada.driver.request.memory` - Specify the memory request for the driver pod
- `spark.armada.executor.limit.cores` - Specify the hard cpu limit for each executor pod.  (k8s syntax)
- `spark.armada.executor.request.cores` - Specify the cpu request for each executor pod.   (k8s syntax)
- `spark.armada.executor.limit.memory` - Specify the hard memory limit for each executor pod.  (k8s syntax)
- `spark.armada.executor.request.memory` - Specify the memory request for each executor pod.   (k8s syntax)
- `spark.armada.executor.connectionTimeout` - Time to wait for the executor to connect to the driver. (Duration string)
- `spark.armada.executor.initContainer.image` - The container image to use for executor init containers. Defaults to busybox.
- `spark.armada.executor.initContainer.cpu` - CPU request and limit for executor init containers (e.g., '100m', '0.1'). In Armada, requests must equal limits. Defaults to 100m.
- `spark.armada.executor.initContainer.memory` - Memory request and limit for executor init containers (e.g., '128Mi', '1Gi'). In Armada, requests must equal limits. Defaults to 128Mi.
- `spark.armada.queue` - The name of the job queue to use for the Armada job.
- `spark.armada.jobSetId` - The JobSet ID for which the driver and executor pods will be part of. 
          If not set, it will be derived from the Spark application name.
- `spark.armada.internalUrl` - The Kubernetes DNS or IP address of the Armada Server.
          This URL is used by the Driver when running in Cluster mode.
          If not specified, 'spark.master' will be used.
- `spark.armada.scheduling.namespace` - The namespace to use for the job. If not set, the default namespace will be used.
- `spark.armada.scheduling.priority` - The priority to use for the job. If not set, the default priority will be used. (Double)
- `spark.armada.executor.trackerPollingInterval` - Specifies
    the interval between polls to check the state of Executors. (Duration string)
- `spark.armada.executor.trackerTimeout` - Specifies the time to
    wait for the minimum number of Executors. (Duration string)
- `spark.armada.lookouturl` - Sets the base URL to use for Armada's Lookout
    service.
- `spark.armada.health.checkTimeout` - Time to wait for Armada's health check
    to succeed in seconds. (Duration string)
- `spark.armada.scheduling.nodeSelectors` - A comma-separated list of kubernetes
    label selectors (in key=value format) to ensure the spark Driver and
    its Executors are deployed to the same cluster.
- `spark.armada.scheduling.nodeUniformityLabel` - A single kubernetes label to
    apply to both Driver and Executors.
- `spark.armada.driver.serviceNamePrefix` - Defines the Driver's service name
    prefix within Armada. Must consist of lowercase a-z and '-' characters only.
- `spark.armada.pod.labels` - A comma-separated list of kubernetes labels
    (in key=value format) to be added to both the Driver and Executor pods.
- `spark.armada.driver.labels` - A comma-separated list of kubernetes labels
    (in key=value format) to be added to the Driver pod.
- `spark.armada.executor.labels` - A comma-separated list of kubernetes labels
    (in key=value format) to be added to all Executor pods.
- `spark.armada.runAsUser` - the numeric id of user to run job as, usually 185 for the spark user on the apache spark images.
- `spark.executor.memory` - Amount of memory to use per executor process, in the same format as JVM memory
    strings with a size unit suffix ("k", "m", "g" or "t") (e.g. 512m, 2g).
- `spark.armada.jobTemplate` - URL or file path to a job template YAML file. 
    Supports local files (with or without file:// prefix) and HTTP/HTTPS URLs. 
    The template should contain JobSubmitRequest configuration in YAML format.
- `spark.armada.driver.jobItemTemplate` - URL or file path to a job item template YAML file for the driver. 
    Supports local files (with or without file:// prefix) and HTTP/HTTPS URLs. 
    The template should contain a JobSubmitRequestItem configuration in YAML format.
- `spark.armada.executor.jobItemTemplate` - URL or file path to a job item template YAML file for the executors.
    Supports local files (with or without file:// prefix) and HTTP/HTTPS URLs.
    The template should contain a JobSubmitRequestItem configuration in YAML format.
- `spark.armada.driver.ui.ingress.enabled` - If set to true, the Spark UI is
    exposed via an Ingress resource. The Ingress targets `spark.ui.port`
    (default 4040), or the OAuth proxy port (default 4180) when
    `spark.armada.oauth.enabled=true`.
- `spark.armada.driver.ui.ingress.tls.enabled` - If set to true, the Spark UI Ingress
    resource is configured to use TLS.
- `spark.armada.driver.ui.ingress.annotations` - A comma-separated list of annotations to apply to the Spark UI Ingress resource.
    Annotations should be in the format key=value, e.g. `nginx.ingress.kubernetes.io/rewrite-target=/`.
- `spark.armada.driver.ui.ingress.certName` - The name of the TLS certificate to use for the Spark UI Ingress resource.
    This is used when `spark.armada.driver.ui.ingress.tls.enabled` is set to true.
- `spark.armada.driver.connect.ingress.enabled` - If set to true, the Spark Connect
    gRPC endpoint is exposed via its own Ingress resource. The port is read from
    `spark.connect.grpc.binding.port` (default 15002). Can be enabled together with
    the UI ingress; each Ingress object carries its own annotations.
- `spark.armada.driver.connect.ingress.tls.enabled` - If set to true, the Spark Connect
    Ingress resource is configured to use TLS.
- `spark.armada.driver.connect.ingress.annotations` - A comma-separated list of annotations to apply to the Spark Connect Ingress resource
    (e.g. `nginx.ingress.kubernetes.io/backend-protocol=GRPC`).
- `spark.armada.driver.connect.ingress.certName` - The name of the TLS certificate to use for the Spark Connect Ingress resource.
    This is used when `spark.armada.driver.connect.ingress.tls.enabled` is set to true.

### OAuth2 Authentication Configuration

`armada-spark` supports OAuth2-based authentication for the Spark Driver WebUI via an oauth2-proxy native sidecar. The full configuration reference (all `spark.armada.oauth.*` keys, defaults, examples) lives in [`docs/oauth/configuration.md`](./oauth/configuration.md). For setup, runtime flow, security model, and troubleshooting, start at [`docs/oauth/`](./oauth/README.md).

Key settings:

- `spark.armada.oauth.enabled` - Enable OAuth2 authentication for Spark UI.
- `spark.armada.oauth.clientId` - OAuth2 client ID.
- `spark.armada.oauth.clientSecret` / `clientSecretK8s` - Client secret (inline or K8s secret reference).
- `spark.armada.oauth.issuerUrl` - OIDC issuer URL (for discovery).
- `spark.armada.oauth.proxy.port` - Port for oauth2-proxy to listen on (default 4180).
- `spark.armada.oauth.proxy.image` - oauth2-proxy container image.

# Building `armada-spark`

## Scala Source

`armada-spark` can be built from source using maven. See the [Building Armada Spark](../README.md#build)
section of the README for more information.

## Docker image

`armada-spark` uses docker to produce container images that serve as spark
Drivers and Executors. See the [Building Docker Images](../README.md#build-docker-images)
section of the README for more information.

Any files you wish to be included in the image, (e.g. Python scripts or Scala / Java jar files) should be copied into the [extraFiles](../extraFiles) directory.  They will appear on the image in the /opt/spark/extraFiles directory.

Any jars you want added to the classpath should be copied into the [extraJars](../extraJars) directory.

# Architecture & Design

## Overview

`armada-spark` is implemented as a new
[Spark Cluster Manager Type](https://spark.apache.org/docs/latest/cluster-overview.html#cluster-manager-types)
It leverages much of the existing Spark Kubernetes cluster manager, but with
the added layer of Armada and its job system on top.

A great overview of what a "cluster" is from Spark's perspective can be found
[here](https://spark.apache.org/docs/latest/cluster-overview.html).

The basic flow of an `armada-spark` execution is as follows:

1. End-user calls `ArmadaSparkSubmit.main` via `spark-submit` or `spark-class`.
2. `ArmadaSparkSubmit`:
    - Constructs an `ArmadaClient` to communicate with Armada Server via gRPC.
    - Verifies connectivity and service health of specified Armada URL.
    - Submits a Driver job to Armada.
    - Submits `n` Executor jobs to Armada.
3. After job submissions, `ArmadaSparkSubmit` prints a Lookout link and exits.
4. Armada schedules Driver and Executor jobs submitted by `ArmadaSparkSubmit`.
   Each job submits a pod with a single container.
5. Asynchronously:
    - Driver starts up and waits for each expected Executor to establish
    connections.
    - Each Executor starts and establishes connections to the Driver.
6. Driver and Executors proceed as any normal Spark program would.
7. Driver and Executors finish their work.

## Integrating with Spark, or "What is a Cluster Manager?"

A "Cluster Manager" is a concept within Spark which allows Spark to submit and
manage Drivers and Executors to a compute cluster like YARN, Mesos,
Kubernetes, and now Armada. Something to note: cluster managers are also sometimes
referred to as "resource-managers" within the Spark codebase.

### How Armada-Spark Becomes a Cluster Manager

To define a new cluster manager within Spark one would inherit the `trait`
The `armada-spark` cluster manager is defined by inheriting the
[`ExternalClusterManager`](https://github.com/apache/spark/blob/master/core/src/main/scala/org/apache/spark/scheduler/ExternalClusterManager.scala)
trait and implementing the required methods. Mostly those methods center around
the creation, management, and deletion of Spark Executors tied to a particular
instance of Driver program .

### Integrating with `spark-submit`

While Spark provides a defined "plugin" interface to help create
new Cluster Managers, (like Armada), there's still a fair bit of cluster-specific code
within `SparkSubmit` code. 
Therefore, `armada-spark` maintains and
provides it's own `ArmadaSparkSubmit` implementation which serves as a drop-in
replacement.

Currently the following versions of Spark are supported:
- 3.3
- 3.5
- 4.1

### Using Armada as a "cluster"

While Armada can be treated as a cluster for the purposes of Spark, some care
and consideration must be taken in order to meet Spark's
expectations.

### Driver and Executor (and Cluster Manager) Network Communications

One major requirement that **must** be met is network communication between
Drivers and Executors. This allows Drivers to submit tasks to the Executors
and collate their results. `armada-spark` achieves this through the use of
[kubernetes labels](https://kubernetes.io/docs/concepts/overview/working-with-objects/labels/).
Used correctly, these labels induce the Armada scheduler to schedule Drivers
and their accompanying Executors to the same kubernetes cluster managed by
Armada. Kubernetes cluster networking topology guarantees that nodes within the
cluster will be able to communicate. Scheduling to the same cluster also has
the benefit of increasing the probability of compute resources being
"clustered" closely together. Perhaps servers on the same rack, or even a single
server.

If you refer back to the ["Cluster Manager Overview"](https://spark.apache.org/docs/latest/cluster-overview.html),
the first image you'll find is a diagram showing a complete, directed graph between the:

- Driver Program
- Worker Node / Executors
- Cluster Manager

and while the diagram appears to separate the "Cluster Manager" out as a separate
program or node, in reality it "lives" in the same process as the
Driver. So by ensuring connectivity between the Driver and Executors, we also
get connectivity with the "Cluster Manager".

Drivers depend on Armada's custom service names to
make it easy for Executors to "find" their respective Drivers.

### Driver and Executors as Armada Jobs

Armada, being a multi-cluster Kubernetes manager, takes on the role of
scheduling "jobs" to the clusters it oversees. For now, it makes the most sense
to separate each Driver and Executor into their own Armada job. There are
several considerations that must be observed when submitting these jobs.

#### Armada Gang Scheduling

`armada-spark` uses gang scheduling annotations to schedule the driver and
its executors together. By using the gang scheduling “Node Uniformity Label”
we can guarantee that a driver and its executors are scheduled to the same
cluster.

### Armada Job Limitations

One important limitation it remember is that Armada jobs may only have one Pod
per job. This necessitates the separation of drivers and executors into their own
jobs so their corresponding Pods can be freely scheduled by Armada.

# `armada-spark` Deployment Considerations

## Necessary Kubernetes Configuration

## Necessary Armada Configuration

### Armada Scheduler

The Armada Scheduler indexes whatever label is assigned to cluster nodes that
denote they accept `armada-spark` jobs through their `scheduling.indexedNodeLabels`
configuration option. Gang scheduling will only consider indexed node labels!

### Armada Executor

Armada Executors must track whatever label is assigned to cluster nodes
that denote they accept `armada-spark` jobs through their `kubernetes.trackedNodeLabels`
configuration option.

### Example Configuration

An example configuration of Armada services with all the necessary options
configured can be found by applying this [patch](../e2e/armada-operator.patch)
to the [Armada Operator Dev Quickstart Config](https://github.com/armadaproject/armada-operator/blob/main/dev/quickstart/armada-crs.yaml)

# Unsupported features

Currently unsupported features include Spark Connect.

