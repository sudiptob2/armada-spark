/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.deploy.armada

import org.apache.spark.deploy.armada.validators.K8sValidator

import java.util.concurrent.TimeUnit
import org.apache.spark.internal.config.{ConfigBuilder, ConfigEntry, OptionalConfigEntry}

import scala.util.{Failure, Success, Try}

private[spark] object Config {
  private val invalidLabelListErrorMessage =
    "Must be a comma-separated list of valid Kubernetes labels (each as key=value)"
  private val invalidAnnotationListErrorMessage =
    "Must be a comma-separated list of valid Kubernetes annotations (each as key=value)"

  val ARMADA_SERVER_INTERNAL_URL: OptionalConfigEntry[String] =
    ConfigBuilder("spark.armada.internalUrl")
      .doc(
        "The Kubernetes DNS or IP address of the Armada Server. " +
          "This URL is used by the Driver when running in Cluster mode. " +
          "If not specified, 'spark.master' will be used."
      )
      .stringConf
      .createOptional

  val ARMADA_JOB_QUEUE: OptionalConfigEntry[String] =
    ConfigBuilder("spark.armada.queue")
      .doc(
        "The name of the job queue to use for the Armada job."
      )
      .stringConf
      .createOptional

  val ARMADA_JOB_SET_ID: OptionalConfigEntry[String] =
    ConfigBuilder("spark.armada.jobSetId")
      .doc(
        "The JobSet ID for which the driver and executor pods will be part of. " +
          "If not set, it will be derived from the Spark application name."
      )
      .stringConf
      .createOptional

  /** Configuration for specifying a job template file to customize Armada job submissions.
    *
    * The template file should contain a JobSubmitRequest structure in YAML format that will be used
    * as a base for job submission. This allows for advanced customization of queue and job set ID.
    * The jobRequestsItems field is ignored, as there is a separate configuration option for the
    * driver and executor job submit item template.
    *
    * Supported template sources:
    *   - File URI: "[file://]/absolute/path/to/template.yaml" (absolute or relative)
    *   - HTTP/HTTPS: "http(s)://config-server.example.com/spark-template.yaml"
    */
  val ARMADA_JOB_TEMPLATE: OptionalConfigEntry[String] =
    ConfigBuilder("spark.armada.jobTemplate")
      .doc(
        "URL or file path to a job template YAML file. " +
          "Supports local files (with or without file:// prefix) and HTTP/HTTPS URLs. " +
          "The template should contain JobSubmitRequest configuration in YAML format."
      )
      .stringConf
      .checkValue(
        path => if (path.isEmpty) false else isValidFilePath(path),
        "Must be a valid local file path, file://, http:// or https:// URL"
      )
      .createOptional

  /** Configuration for specifying a driver job item template file to customize driver pods.
    *
    * The template file should contain a JobSubmitRequestItem structure in YAML format that will be
    * used as a base for driver pod configuration. This allows for advanced customization of
    * driver-specific pod specifications, resources, labels, annotations, and other Kubernetes
    * settings while preserving runtime-configured values.
    *
    * CLI configuration flags override template values. Some fields (containers, services,
    * restartPolicy) are always overridden to ensure correct Spark behavior.
    *
    * Supported template sources:
    *   - File URI: "[file://]/absolute/path/to/template.yaml" (absolute or relative)
    *   - HTTP/HTTPS: "http(s)://config-server.example.com/driver-template.yaml"
    */
  val ARMADA_DRIVER_JOB_ITEM_TEMPLATE: OptionalConfigEntry[String] =
    ConfigBuilder("spark.armada.driver.jobItemTemplate")
      .doc(
        "URL or file path to a job item template YAML file for the driver. " +
          "Supports local files (with or without file:// prefix) and HTTP/HTTPS URLs. " +
          "The template should contain a JobSubmitRequestItem configuration in YAML format."
      )
      .stringConf
      .checkValue(
        path => if (path.isEmpty) false else isValidFilePath(path),
        "Must be a valid local file path, file://, http://, https:// URL"
      )
      .createOptional

  val ARMADA_SPARK_DRIVER_UI_INGRESS_ENABLED: ConfigEntry[Boolean] =
    ConfigBuilder("spark.armada.driver.ui.ingress.enabled")
      .doc(
        "If set to true, the Spark UI is exposed via an Ingress resource. " +
          "The ingress targets the Spark UI port, or the OAuth proxy port when " +
          "spark.armada.oauth.enabled is true."
      )
      .booleanConf
      .createWithDefault(false)

  val ARMADA_SPARK_DRIVER_UI_INGRESS_TLS_ENABLED: OptionalConfigEntry[Boolean] =
    ConfigBuilder("spark.armada.driver.ui.ingress.tls.enabled")
      .doc(
        "Whether to enable TLS for the Spark UI Ingress resource. " +
          "If not set, TLS is disabled by default."
      )
      .booleanConf
      .createOptional

  val ARMADA_SPARK_DRIVER_UI_INGRESS_ANNOTATIONS: OptionalConfigEntry[String] =
    ConfigBuilder("spark.armada.driver.ui.ingress.annotations")
      .doc(
        "A comma-separated list of annotations (in key=value format) to be added to the Spark UI Ingress resource."
      )
      .stringConf
      .checkValue(k8sAnnotationListValidator, invalidAnnotationListErrorMessage)
      .createOptional

  val ARMADA_SPARK_DRIVER_UI_INGRESS_CERT_NAME: OptionalConfigEntry[String] =
    ConfigBuilder("spark.armada.driver.ui.ingress.certName")
      .doc(
        "The name of the TLS certificate to use for the Spark UI Ingress resource."
      )
      .stringConf
      .createOptional

  val ARMADA_SPARK_DRIVER_CONNECT_INGRESS_ENABLED: ConfigEntry[Boolean] =
    ConfigBuilder("spark.armada.driver.connect.ingress.enabled")
      .doc(
        "If set to true, the Spark Connect gRPC endpoint is exposed via an Ingress resource. " +
          "The ingress targets spark.connect.grpc.binding.port (default 15002)."
      )
      .booleanConf
      .createWithDefault(false)

  val ARMADA_SPARK_DRIVER_CONNECT_INGRESS_TLS_ENABLED: OptionalConfigEntry[Boolean] =
    ConfigBuilder("spark.armada.driver.connect.ingress.tls.enabled")
      .doc(
        "Whether to enable TLS for the Spark Connect Ingress resource. " +
          "If not set, TLS is disabled by default."
      )
      .booleanConf
      .createOptional

  val ARMADA_SPARK_DRIVER_CONNECT_INGRESS_ANNOTATIONS: OptionalConfigEntry[String] =
    ConfigBuilder("spark.armada.driver.connect.ingress.annotations")
      .doc(
        "A comma-separated list of annotations (in key=value format) to be added to the " +
          "Spark Connect Ingress resource. Kept separate from the UI ingress annotations so " +
          "the Connect ingress can use gRPC backend annotations while the UI ingress uses HTTP."
      )
      .stringConf
      .checkValue(k8sAnnotationListValidator, invalidAnnotationListErrorMessage)
      .createOptional

  val ARMADA_SPARK_DRIVER_CONNECT_INGRESS_CERT_NAME: OptionalConfigEntry[String] =
    ConfigBuilder("spark.armada.driver.connect.ingress.certName")
      .doc(
        "The name of the TLS certificate to use for the Spark Connect Ingress resource."
      )
      .stringConf
      .createOptional

  /** Configuration for specifying an executor job item template file to customize executor pods.
    *
    * The template file should contain a JobSubmitRequestItem structure in YAML format that will be
    * used as a base for executor pod configuration. This allows for advanced customization of
    * executor-specific pod specifications, resources, labels, annotations, init containers, and
    * other Kubernetes settings while preserving runtime-configured values.
    *
    * CLI configuration flags override template values. Some fields (containers, initContainers,
    * restartPolicy) are always overridden to ensure correct Spark behavior.
    *
    * Supported template sources:
    *   - File URI: "[file://]/absolute/path/to/template.yaml" (absolute or relative)
    *   - HTTP/HTTPS: "http(s)://config-server.example.com/executor-template.yaml"
    */
  val ARMADA_EXECUTOR_JOB_ITEM_TEMPLATE: OptionalConfigEntry[String] =
    ConfigBuilder("spark.armada.executor.jobItemTemplate")
      .doc(
        "URL or file path to a job item template YAML file for executors. " +
          "Supports local files (with or without file:// prefix) and HTTP/HTTPS URLs. " +
          "The template should contain a JobSubmitRequestItem configuration in YAML format."
      )
      .stringConf
      .checkValue(
        path => if (path.isEmpty) false else isValidFilePath(path),
        "Must be a valid local file path, file://, http:// or https:// URL"
      )
      .createOptional

  private def isValidFilePath(path: String): Boolean = {
    Seq("file://", "http://", "https://").exists(path.toLowerCase.startsWith(_)) ||
    (!path.contains("://") && path.nonEmpty) // Local file without protocol
  }

  val ARMADA_EXECUTOR_CONNECTION_TIMEOUT: ConfigEntry[Long] =
    ConfigBuilder("spark.armada.executor.connectionTimeout")
      .doc("Time to wait for the executor to connect to the driver.")
      .timeConf(TimeUnit.SECONDS)
      .checkValue(
        _ > 0,
        s"Connection timeout must be a positive time value."
      )
      .createWithDefaultString("300s")

  val ARMADA_EXECUTOR_INIT_CONTAINER_IMAGE: ConfigEntry[String] =
    ConfigBuilder("spark.armada.executor.initContainer.image")
      .doc("Container image for the executor init container.")
      .stringConf
      .createWithDefaultString("busybox")

  val ARMADA_LOOKOUTURL: ConfigEntry[String] =
    ConfigBuilder("spark.armada.lookouturl")
      .doc("URL base for the Armada Lookout UI.")
      .stringConf
      .checkValue(
        urlPrefix => urlPrefix.nonEmpty && urlPrefix.startsWith("http", 0),
        s"Value must be a valid URL, like http://host:8080 or https://host:443"
      )
      .createWithDefaultString("http://localhost:30000")

  val ARMADA_HEALTH_CHECK_TIMEOUT: ConfigEntry[Long] =
    ConfigBuilder("spark.armada.health.checkTimeout")
      .doc("Number of seconds to wait for an Armada health check result")
      .timeConf(TimeUnit.SECONDS)
      .checkValue(_ > 0, s"Timeout must be a positive time value.")
      .createWithDefaultString("5")

  val ARMADA_DRIVER_WATCH_ENABLED: ConfigEntry[Boolean] =
    ConfigBuilder("spark.armada.driver.watchEnabled")
      .doc(
        "Whether to watch the driver job event stream after cluster-mode submission " +
          "and block until it reaches a terminal state. When true, the process exits " +
          "with code 0 on success and non-zero on failure/cancellation."
      )
      .booleanConf
      .createWithDefault(true)

  val ARMADA_DRIVER_WATCH_TIMEOUT: ConfigEntry[Long] =
    ConfigBuilder("spark.armada.driver.watchTimeout")
      .doc(
        "Maximum time in seconds to wait for the driver job to reach a terminal state. " +
          "A value of 0 means wait indefinitely."
      )
      .timeConf(TimeUnit.SECONDS)
      .checkValue(_ >= 0, "Watch timeout must be non-negative.")
      .createWithDefaultString("0s")

  val ARMADA_JOB_NODE_SELECTORS: OptionalConfigEntry[String] =
    ConfigBuilder("spark.armada.scheduling.nodeSelectors")
      .doc(
        "A comma-separated list of kubernetes label selectors (in key=value format) to ensure " +
          "the spark driver and its executors are deployed to the same cluster."
      )
      .stringConf
      .checkValue(k8sLabelListValidator, invalidLabelListErrorMessage)
      .createOptional

  val ARMADA_JOB_GANG_SCHEDULING_NODE_UNIFORMITY: OptionalConfigEntry[String] =
    ConfigBuilder("spark.armada.scheduling.nodeUniformity")
      .doc(
        "Constrains the jobs that make up a gang to be scheduled across a uniform set of nodes. " +
          "Specifically, if set, all gang jobs are scheduled onto nodes for which the value of the provided label is equal."
      )
      .stringConf
      .checkValue(
        v => if (v.nonEmpty) K8sValidator.Label.isValidKey(v) else true,
        "Only a valid RFC 1123 label key is allowed!"
      )
      .createOptional

  val ARMADA_INTERNAL_GANG_NODE_LABEL_NAME: OptionalConfigEntry[String] =
    ConfigBuilder("spark.armada.internal.gangNodeLabelName")
      .doc("Internal: gang node uniformity label name captured from executor attributes.")
      .internal()
      .stringConf
      .createOptional

  val ARMADA_INTERNAL_GANG_NODE_LABEL_VALUE: OptionalConfigEntry[String] =
    ConfigBuilder("spark.armada.internal.gangNodeLabelValue")
      .doc("Internal: gang node uniformity label value captured from executor attributes.")
      .internal()
      .stringConf
      .createOptional

  val ARMADA_EXECUTOR_INIT_CONTAINER_CPU: ConfigEntry[String] =
    ConfigBuilder("spark.armada.executor.initContainer.cpu")
      .doc(
        "CPU request and limit for executor init containers (e.g., '100m', '0.1'). " +
          "In Armada, requests must equal limits. Defaults to 100m."
      )
      .stringConf
      .createWithDefault("100m")

  val ARMADA_EXECUTOR_INIT_CONTAINER_MEMORY: ConfigEntry[String] =
    ConfigBuilder("spark.armada.executor.initContainer.memory")
      .doc(
        "Memory request and limit for executor init containers (e.g., '128Mi', '1Gi'). " +
          "In Armada, requests must equal limits. Defaults to 128Mi."
      )
      .stringConf
      .createWithDefault("128Mi")

  val ARMADA_SPARK_POD_LABELS: OptionalConfigEntry[String] =
    ConfigBuilder("spark.armada.pod.labels")
      .doc(
        "A comma-separated list of kubernetes labels (in key=value format) to be added to " +
          "both the driver and executor pods."
      )
      .stringConf
      .checkValue(k8sLabelListValidator, invalidLabelListErrorMessage)
      .createOptional

  val ARMADA_SPARK_DRIVER_LABELS: OptionalConfigEntry[String] =
    ConfigBuilder("spark.armada.driver.labels")
      .doc(
        "A comma-separated list of kubernetes labels (in key=value format) to be added to the " +
          "driver pod."
      )
      .stringConf
      .checkValue(k8sLabelListValidator, invalidLabelListErrorMessage)
      .createOptional

  val ARMADA_SPARK_JOB_NAMESPACE: OptionalConfigEntry[String] =
    ConfigBuilder("spark.armada.scheduling.namespace")
      .doc(
        "The namespace to use for the job. If not set, the default namespace will be used."
      )
      .stringConf
      .createOptional

  val ARMADA_SPARK_JOB_PRIORITY: OptionalConfigEntry[Double] =
    ConfigBuilder("spark.armada.scheduling.priority")
      .doc(
        "The priority to use for the job. If not set, the default priority will be used."
      )
      .doubleConf
      .checkValue(
        _ >= 0,
        "Priority must be equal or greater than 0."
      )
      .createOptional

  val ARMADA_SCHEDULING_INITIAL_PRIORITY_CLASS: OptionalConfigEntry[String] =
    ConfigBuilder("spark.armada.scheduling.initialPriorityClass")
      .doc(
        "Armada PriorityClassName for pods in the initial gang " +
          "(driver + initial executors). If not set, no priorityClassName " +
          "is applied and the pod inherits from its template or cluster default."
      )
      .stringConf
      .checkValue(_.nonEmpty, "Priority class name must not be empty")
      .createOptional

  val ARMADA_SCHEDULING_SCALE_UP_PRIORITY_CLASS: OptionalConfigEntry[String] =
    ConfigBuilder("spark.armada.scheduling.scaleUpPriorityClass")
      .doc(
        "Armada PriorityClassName for executor pods allocated after " +
          "the initial gang (scale-up executors). If not set, no " +
          "priorityClassName is applied and the pod inherits from its " +
          "template or cluster default.  NOTE: this only gets used in " +
          "dynamic cluster mode."
      )
      .stringConf
      .checkValue(_.nonEmpty, "Priority class name must not be empty")
      .createOptional

  val ARMADA_SPARK_EXECUTOR_LABELS: OptionalConfigEntry[String] =
    ConfigBuilder("spark.armada.executor.labels")
      .doc(
        "A comma-separated list of kubernetes labels (in key=value format) to be added to all " +
          "executor pods."
      )
      .stringConf
      .checkValue(k8sLabelListValidator, invalidLabelListErrorMessage)
      .createOptional

  val CONTAINER_IMAGE: OptionalConfigEntry[String] =
    ConfigBuilder("spark.armada.container.image")
      .doc("Container image to use for Spark containers.")
      .stringConf
      .createOptional

  val DEFAULT_SPARK_EXECUTOR_CORES = "1"
  val DEFAULT_CORES                = "1"
  val ARMADA_DRIVER_LIMIT_CORES: OptionalConfigEntry[String] =
    ConfigBuilder("spark.armada.driver.limit.cores")
      .doc("Specify the hard cpu limit for the driver pod")
      .version("1.0.0")
      .stringConf
      .createOptional

  val ARMADA_DRIVER_REQUEST_CORES: OptionalConfigEntry[String] =
    ConfigBuilder("spark.armada.driver.request.cores")
      .doc("Specify the cpu request for the driver pod")
      .version("1.0.0")
      .stringConf
      .createOptional

  val ARMADA_EXECUTOR_LIMIT_CORES: OptionalConfigEntry[String] =
    ConfigBuilder("spark.armada.executor.limit.cores")
      .doc("Specify the hard cpu limit for each executor pod")
      .version("1.0.0")
      .stringConf
      .createOptional

  val ARMADA_EXECUTOR_REQUEST_CORES: OptionalConfigEntry[String] =
    ConfigBuilder("spark.armada.executor.request.cores")
      .doc("Specify the cpu request for each executor pod")
      .version("1.0.0")
      .stringConf
      .createOptional

  val DEFAULT_MEM                   = "1Gi"
  val DEFAULT_SPARK_EXECUTOR_MEMORY = "1g"
  val ARMADA_DRIVER_LIMIT_MEMORY: OptionalConfigEntry[String] =
    ConfigBuilder("spark.armada.driver.limit.memory")
      .doc("Specify the hard memory limit for the driver pod")
      .version("1.0.0")
      .stringConf
      .createOptional

  val ARMADA_DRIVER_REQUEST_MEMORY: OptionalConfigEntry[String] =
    ConfigBuilder("spark.armada.driver.request.memory")
      .doc("Specify the memory request for the driver pod")
      .version("1.0.0")
      .stringConf
      .createOptional

  val ARMADA_EXECUTOR_LIMIT_MEMORY: OptionalConfigEntry[String] =
    ConfigBuilder("spark.armada.executor.limit.memory")
      .doc("Specify the hard memory limit for each executor pod")
      .version("1.0.0")
      .stringConf
      .createOptional

  val ARMADA_EXECUTOR_REQUEST_MEMORY: OptionalConfigEntry[String] =
    ConfigBuilder("spark.armada.executor.request.memory")
      .doc("Specify the memory request for each executor pod")
      .version("1.0.0")
      .stringConf
      .createOptional

  val ARMADA_RUN_AS_USER: OptionalConfigEntry[Long] =
    ConfigBuilder("spark.armada.runAsUser")
      .doc("Specify the numeric id of the Unix/OS user running inside the docker container.")
      .version("1.0.0")
      .longConf
      .createOptional

  val ARMADA_AUTH_SCRIPT_PATH: OptionalConfigEntry[String] =
    ConfigBuilder("spark.armada.auth.script.path")
      .doc(
        "Path to an executable script for obtaining authentication tokens. "
      )
      .stringConf
      .createOptional

  val ARMADA_DELETE_EXECUTORS: ConfigEntry[Boolean] =
    ConfigBuilder("spark.armada.deleteExecutors")
      .doc("Whether to delete executor jobs when the backend stops.")
      .booleanConf
      .createWithDefault(true)

  val ARMADA_KILL_GRACE_PERIOD: ConfigEntry[Long] =
    ConfigBuilder("spark.armada.killGracePeriod")
      .doc("Grace period in milliseconds before forcefully killing executors.")
      .timeConf(TimeUnit.MILLISECONDS)
      .checkValue(_ > 0, "Grace period must be positive")
      .createWithDefaultString("5s")

  val ARMADA_ALLOCATION_BATCH_SIZE: ConfigEntry[Int] =
    ConfigBuilder("spark.armada.allocation.batchSize")
      .doc("Maximum number of executors to allocate in a single batch.")
      .intConf
      .checkValue(_ > 0, "Batch size must be positive")
      .createWithDefault(10)

  val ARMADA_MAX_PENDING_JOBS: ConfigEntry[Int] =
    ConfigBuilder("spark.armada.allocation.maxPendingJobs")
      .doc("Maximum number of pending executor jobs allowed.")
      .intConf
      .checkValue(_ > 0, "Max pending jobs must be positive")
      .createWithDefault(100)

  val ARMADA_ALLOCATION_CHECK_INTERVAL: ConfigEntry[Long] =
    ConfigBuilder("spark.armada.allocation.checkInterval")
      .doc("Interval for checking executor allocation needs.")
      .timeConf(TimeUnit.MILLISECONDS)
      .checkValue(_ > 0, "Check interval must be positive")
      .createWithDefaultString("10s")

  val ARMADA_EVENT_WATCHER_USE_TLS: ConfigEntry[Boolean] =
    ConfigBuilder("spark.armada.eventWatcher.useTls")
      .doc(
        "Whether to use TLS for the event watcher gRPC channel. " +
          "Set to true to use TLS connections. " +
          "Defaults to false (plaintext)"
      )
      .booleanConf
      .createWithDefault(false)

  val ARMADA_EXECUTOR_PREEMPTION_GRACE_PERIOD: ConfigEntry[Long] =
    ConfigBuilder("spark.armada.executor.preemptionGracePeriod")
      .doc(
        "Grace period between Armada preemption signal and actual pod termination. " +
          "This is configured in the Armada job spec's terminationGracePeriodSeconds."
      )
      .timeConf(TimeUnit.SECONDS)
      .checkValue(_ > 0, "Preemption grace period must be positive")
      .createWithDefault(180)

  val ARMADA_OAUTH_ENABLED: ConfigEntry[Boolean] =
    ConfigBuilder("spark.armada.oauth.enabled")
      .doc("Enable OAuth2 authentication for Spark Driver UI")
      .booleanConf
      .createWithDefault(false)

  val ARMADA_OAUTH_CLIENT_ID: OptionalConfigEntry[String] =
    ConfigBuilder("spark.armada.oauth.clientId")
      .doc("OAuth2 client ID")
      .stringConf
      .createOptional

  val ARMADA_OAUTH_CLIENT_SECRET: OptionalConfigEntry[String] =
    ConfigBuilder("spark.armada.oauth.clientSecret")
      .doc("OAuth2 client secret")
      .stringConf
      .createOptional

  val ARMADA_OAUTH_CLIENT_SECRET_K8S: OptionalConfigEntry[String] =
    ConfigBuilder("spark.armada.oauth.clientSecretK8s")
      .doc("Kubernetes secret name containing OAuth2 client secret")
      .stringConf
      .createOptional

  val ARMADA_OAUTH_CLIENT_SECRET_KEY: ConfigEntry[String] =
    ConfigBuilder("spark.armada.oauth.clientSecretKey")
      .doc("Kubernetes secret key name for OAuth2 client secret")
      .stringConf
      .createWithDefault("client-secret")

  val ARMADA_OAUTH_ISSUER_URL: OptionalConfigEntry[String] =
    ConfigBuilder("spark.armada.oauth.issuerUrl")
      .doc("OIDC issuer URL")
      .stringConf
      .createOptional

  val ARMADA_OAUTH_REDIRECT_URL: OptionalConfigEntry[String] =
    ConfigBuilder("spark.armada.oauth.redirectUrl")
      .doc("OAuth2 redirect URL (auto-detected if not set)")
      .stringConf
      .createOptional

  val ARMADA_OAUTH_PROXY_IMAGE: ConfigEntry[String] =
    ConfigBuilder("spark.armada.oauth.proxy.image")
      .doc("oauth2-proxy Docker image")
      .stringConf
      .createWithDefault("quay.io/oauth2-proxy/oauth2-proxy:v7.5.1")

  val ARMADA_OAUTH_PROXY_PORT: ConfigEntry[Int] =
    ConfigBuilder("spark.armada.oauth.proxy.port")
      .doc("oauth2-proxy listen port")
      .intConf
      .checkValue(_ > 0, "Port must be positive")
      .createWithDefault(4180)

  val ARMADA_OAUTH_PROVIDER_DISPLAY_NAME: ConfigEntry[String] =
    ConfigBuilder("spark.armada.oauth.providerDisplayName")
      .doc("Provider name shown in OAuth UI")
      .stringConf
      .createWithDefault("OAuth Provider")

  val ARMADA_OAUTH_EMAIL_DOMAIN: ConfigEntry[String] =
    ConfigBuilder("spark.armada.oauth.emailDomain")
      .doc("Allowed email domains (* for all)")
      .stringConf
      .createWithDefault("*")

  val ARMADA_OAUTH_SKIP_PROVIDER_DISCOVERY: ConfigEntry[Boolean] =
    ConfigBuilder("spark.armada.oauth.skipProviderDiscovery")
      .doc("Skip OIDC discovery and use explicit endpoints")
      .booleanConf
      .createWithDefault(false)

  val ARMADA_OAUTH_LOGIN_URL: OptionalConfigEntry[String] =
    ConfigBuilder("spark.armada.oauth.loginUrl")
      .doc("OIDC authorization endpoint (required if skipProviderDiscovery=true)")
      .stringConf
      .createOptional

  val ARMADA_OAUTH_REDEEM_URL: OptionalConfigEntry[String] =
    ConfigBuilder("spark.armada.oauth.redeemUrl")
      .doc("OIDC token endpoint (required if skipProviderDiscovery=true)")
      .stringConf
      .createOptional

  val ARMADA_OAUTH_VALIDATE_URL: OptionalConfigEntry[String] =
    ConfigBuilder("spark.armada.oauth.validateUrl")
      .doc("OIDC userinfo endpoint (required if skipProviderDiscovery=true)")
      .stringConf
      .createOptional

  val ARMADA_OAUTH_JWKS_URL: OptionalConfigEntry[String] =
    ConfigBuilder("spark.armada.oauth.jwksUrl")
      .doc("OIDC JWKS endpoint (required if skipProviderDiscovery=true)")
      .stringConf
      .createOptional

  val ARMADA_OAUTH_INSECURE_SKIP_ISSUER_VERIFICATION: ConfigEntry[Boolean] =
    ConfigBuilder("spark.armada.oauth.insecureSkipIssuerVerification")
      .doc("Skip OIDC issuer verification (dev/test only)")
      .booleanConf
      .createWithDefault(false)

  val ARMADA_OAUTH_INSECURE_ALLOW_UNVERIFIED_EMAIL: ConfigEntry[Boolean] =
    ConfigBuilder("spark.armada.oauth.insecureAllowUnverifiedEmail")
      .doc("Allow unverified email addresses (dev/test only)")
      .booleanConf
      .createWithDefault(false)

  val ARMADA_OAUTH_SKIP_JWT_BEARER_TOKENS: ConfigEntry[Boolean] =
    ConfigBuilder("spark.armada.oauth.skipJwtBearerTokens")
      .doc("Skip JWT bearer token validation")
      .booleanConf
      .createWithDefault(false)

  val ARMADA_OAUTH_SKIP_VERIFY: ConfigEntry[Boolean] =
    ConfigBuilder("spark.armada.oauth.skipVerify")
      .doc("Skip TLS certificate verification for OIDC provider")
      .booleanConf
      .createWithDefault(false)

  val ARMADA_OAUTH_SSL_UPSTREAM_INSECURE_SKIP_VERIFY: ConfigEntry[Boolean] =
    ConfigBuilder("spark.armada.oauth.sslUpstreamInsecureSkipVerify")
      .doc("Skip TLS certificate verification for upstream (Spark UI)")
      .booleanConf
      .createWithDefault(false)

  val ARMADA_OAUTH_EXTRA_AUDIENCES: OptionalConfigEntry[String] =
    ConfigBuilder("spark.armada.oauth.extraAudiences")
      .doc("Comma-separated list of additional OIDC audiences")
      .stringConf
      .createOptional

  val ARMADA_OAUTH_CODE_CHALLENGE_METHOD: ConfigEntry[String] =
    ConfigBuilder("spark.armada.oauth.codeChallengeMethod")
      .doc("PKCE code challenge method")
      .stringConf
      .createWithDefault("S256")

  val ARMADA_OAUTH_COOKIE_NAME: ConfigEntry[String] =
    ConfigBuilder("spark.armada.oauth.cookieName")
      .doc("OAuth session cookie name")
      .stringConf
      .createWithDefault("_oauth2_proxy")

  val ARMADA_OAUTH_COOKIE_PATH: ConfigEntry[String] =
    ConfigBuilder("spark.armada.oauth.cookiePath")
      .doc("OAuth cookie path")
      .stringConf
      .createWithDefault("/")

  val ARMADA_OAUTH_COOKIE_SECURE: ConfigEntry[Boolean] =
    ConfigBuilder("spark.armada.oauth.cookieSecure")
      .doc("Require HTTPS for cookies")
      .booleanConf
      .createWithDefault(false)

  val ARMADA_OAUTH_COOKIE_SAMESITE: OptionalConfigEntry[String] =
    ConfigBuilder("spark.armada.oauth.cookieSamesite")
      .doc("SameSite cookie attribute (none/lax/strict)")
      .stringConf
      .createOptional

  val ARMADA_OAUTH_COOKIE_CSRF_PER_REQUEST: ConfigEntry[Boolean] =
    ConfigBuilder("spark.armada.oauth.cookieCsrfPerRequest")
      .doc("Enable CSRF per request")
      .booleanConf
      .createWithDefault(false)

  val ARMADA_OAUTH_COOKIE_CSRF_EXPIRE: OptionalConfigEntry[String] =
    ConfigBuilder("spark.armada.oauth.cookieCsrfExpire")
      .doc("CSRF cookie expiration duration")
      .stringConf
      .createOptional

  val ARMADA_OAUTH_SKIP_PROVIDER_BUTTON: ConfigEntry[Boolean] =
    ConfigBuilder("spark.armada.oauth.skipProviderButton")
      .doc("Skip provider selection button")
      .booleanConf
      .createWithDefault(false)

  val ARMADA_OAUTH_SKIP_AUTH_PREFLIGHT: ConfigEntry[Boolean] =
    ConfigBuilder("spark.armada.oauth.skipAuthPreflight")
      .doc("Skip authentication for OPTIONS requests")
      .booleanConf
      .createWithDefault(false)

  val ARMADA_OAUTH_PASS_HOST_HEADER: ConfigEntry[Boolean] =
    ConfigBuilder("spark.armada.oauth.passHostHeader")
      .doc("Pass Host header to upstream")
      .booleanConf
      .createWithDefault(false)

  val ARMADA_OAUTH_WHITELIST_DOMAIN: OptionalConfigEntry[String] =
    ConfigBuilder("spark.armada.oauth.whitelistDomain")
      .doc("Whitelist redirect domains")
      .stringConf
      .createOptional

  val ARMADA_OAUTH_TLS_CA_CERT_PATH: OptionalConfigEntry[String] =
    ConfigBuilder("spark.armada.oauth.tls.caCertPath")
      .doc("Mount path for CA certificate")
      .stringConf
      .createOptional

  val ARMADA_OAUTH_TLS_CA_CERT_SUBPATH: OptionalConfigEntry[String] =
    ConfigBuilder("spark.armada.oauth.tls.caCertSubPath")
      .doc("SubPath for CA certificate volume mount")
      .stringConf
      .createOptional

  val ARMADA_OAUTH_TLS_CA_BUNDLE_PATH: OptionalConfigEntry[String] =
    ConfigBuilder("spark.armada.oauth.tls.caBundlePath")
      .doc("Mount path for CA bundle (e.g., /etc/ssl/certs/ca-certificates.crt)")
      .stringConf
      .createOptional

  val ARMADA_OAUTH_TLS_CA_BUNDLE_SUBPATH: OptionalConfigEntry[String] =
    ConfigBuilder("spark.armada.oauth.tls.caBundleSubPath")
      .doc("SubPath for CA bundle volume mount (e.g., ca-bundle.crt)")
      .stringConf
      .createOptional

  val ARMADA_OAUTH_RESOURCES_CPU: ConfigEntry[String] =
    ConfigBuilder("spark.armada.oauth.resources.cpu")
      .doc("CPU resource limit/request for oauth2-proxy")
      .stringConf
      .createWithDefault("100m")

  val ARMADA_OAUTH_RESOURCES_MEMORY: ConfigEntry[String] =
    ConfigBuilder("spark.armada.oauth.resources.memory")
      .doc("Memory resource limit/request for oauth2-proxy")
      .stringConf
      .createWithDefault("128Mi")

  def commaSeparatedLabelsToMap(labelList: String): Map[String, String] = {
    parseCommaSeparatedK8sValue(labelList, K8sValidator.Label).map(_.get).toMap
  }

  private def k8sLabelListValidator(labelList: String): Boolean = {
    parseCommaSeparatedK8sValue(labelList, K8sValidator.Label).forall(_.isSuccess)
  }

  def commaSeparatedAnnotationsToMap(annotationList: String): Map[String, String] = {
    parseCommaSeparatedK8sValue(annotationList, K8sValidator.Annotation).map(_.get).toMap
  }

  private def k8sAnnotationListValidator(annotationList: String): Boolean = {
    parseCommaSeparatedK8sValue(annotationList, K8sValidator.Annotation).forall(_.isSuccess)
  }

  private def parseCommaSeparatedK8sValue(
      str: String,
      validator: K8sValidator
  ): Seq[Try[(String, String)]] = {
    parseCommaSeparatedString(str)
      .map { entry =>
        entry.split("=", 2).map(_.trim) match {
          case Array(key, value) if validator.isValidKey(key) && validator.isValidValue(value) =>
            Success(key -> value)
          case _ =>
            Failure(
              new IllegalArgumentException(
                s"Invalid label format: '$entry' (expected key=value)"
              )
            )
        }
      }
  }

  private def parseCommaSeparatedString(str: String): Seq[String] = {
    str.trim
      .split(",")
      .filter(_.nonEmpty)
      .map(_.trim)
      .toSeq
  }
}
