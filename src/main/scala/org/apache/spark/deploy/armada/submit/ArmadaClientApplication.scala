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
package org.apache.spark.deploy.armada.submit

import api.event.EventMessage
import api.submit.JobSubmitRequestItem
import org.apache.spark.deploy.armada.Config.{
  ARMADA_DRIVER_JOB_ITEM_TEMPLATE,
  ARMADA_DRIVER_LIMIT_CORES,
  ARMADA_DRIVER_LIMIT_MEMORY,
  ARMADA_DRIVER_REQUEST_CORES,
  ARMADA_DRIVER_REQUEST_MEMORY,
  ARMADA_EXECUTOR_CONNECTION_TIMEOUT,
  ARMADA_EXECUTOR_INIT_CONTAINER_CPU,
  ARMADA_EXECUTOR_INIT_CONTAINER_IMAGE,
  ARMADA_EXECUTOR_INIT_CONTAINER_MEMORY,
  ARMADA_EXECUTOR_JOB_ITEM_TEMPLATE,
  ARMADA_EXECUTOR_LIMIT_CORES,
  ARMADA_EXECUTOR_LIMIT_MEMORY,
  ARMADA_EXECUTOR_PREEMPTION_GRACE_PERIOD,
  ARMADA_EXECUTOR_REQUEST_CORES,
  ARMADA_EXECUTOR_REQUEST_MEMORY,
  ARMADA_DRIVER_WATCH_ENABLED,
  ARMADA_DRIVER_WATCH_TIMEOUT,
  ARMADA_HEALTH_CHECK_TIMEOUT,
  ARMADA_JOB_GANG_SCHEDULING_NODE_UNIFORMITY,
  ARMADA_JOB_NODE_SELECTORS,
  ARMADA_JOB_QUEUE,
  ARMADA_JOB_SET_ID,
  ARMADA_JOB_TEMPLATE,
  ARMADA_LOOKOUTURL,
  ARMADA_OAUTH_ENABLED,
  ARMADA_RUN_AS_USER,
  ARMADA_SERVER_INTERNAL_URL,
  ARMADA_SPARK_DRIVER_CONNECT_INGRESS_ANNOTATIONS,
  ARMADA_SPARK_DRIVER_CONNECT_INGRESS_CERT_NAME,
  ARMADA_SPARK_DRIVER_CONNECT_INGRESS_ENABLED,
  ARMADA_SPARK_DRIVER_CONNECT_INGRESS_TLS_ENABLED,
  ARMADA_SPARK_DRIVER_UI_INGRESS_ANNOTATIONS,
  ARMADA_SPARK_DRIVER_UI_INGRESS_CERT_NAME,
  ARMADA_SPARK_DRIVER_UI_INGRESS_ENABLED,
  ARMADA_SPARK_DRIVER_UI_INGRESS_TLS_ENABLED,
  ARMADA_SPARK_DRIVER_LABELS,
  ARMADA_SPARK_EXECUTOR_LABELS,
  ARMADA_SPARK_JOB_NAMESPACE,
  ARMADA_SPARK_JOB_PRIORITY,
  ARMADA_SPARK_POD_LABELS,
  ARMADA_SCHEDULING_INITIAL_PRIORITY_CLASS,
  ARMADA_SCHEDULING_SCALE_UP_PRIORITY_CLASS,
  CONTAINER_IMAGE,
  DEFAULT_CORES,
  DEFAULT_MEM,
  DEFAULT_SPARK_EXECUTOR_CORES,
  DEFAULT_SPARK_EXECUTOR_MEMORY,
  commaSeparatedAnnotationsToMap,
  commaSeparatedLabelsToMap
}
import org.apache.spark.deploy.armada.DeploymentModeHelper
import io.armadaproject.armada.ArmadaClient
import io.fabric8.kubernetes.api.model
import io.fabric8.kubernetes.api.model.PodBuilder
import k8s.io.api.core.v1.generated._
import k8s.io.apimachinery.pkg.api.resource.generated.Quantity
import org.apache.spark.deploy.SparkApplication
import org.apache.spark.deploy.k8s.submit.{
  JavaMainAppResource,
  KubernetesDriverBuilder,
  MainAppResource,
  PythonMainAppResource,
  RMainAppResource
}
import org.apache.spark.deploy.k8s.{KubernetesDriverConf, KubernetesExecutorConf}
import org.apache.spark.deploy.k8s.Config.{
  CONTAINER_IMAGE => KUBERNETES_CONTAINER_IMAGE,
  KUBERNETES_FILE_UPLOAD_PATH,
  KUBERNETES_SUBMIT_GRACE_PERIOD
}
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.{SecurityManager, SparkConf}
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.internal.config.{DRIVER_PORT, DRIVER_HOST_ADDRESS, DYN_ALLOCATION_ENABLED}
import org.apache.spark.resource.ResourceProfile
import org.apache.spark.scheduler.cluster.k8s.KubernetesExecutorBuilder
import io.fabric8.kubernetes.client.DefaultKubernetesClient

import scala.util.control.NonFatal
import scala.collection.mutable
import scala.jdk.CollectionConverters._
import scala.concurrent.Await
import scala.concurrent.duration._

/** Encapsulates arguments to the submission client.
  *
  * @param mainAppResource
  *   the main application resource if any
  * @param mainClass
  *   the main class of the application to run
  * @param driverArgs
  *   arguments to the driver
  */
private[spark] case class ClientArguments(
    mainAppResource: MainAppResource,
    mainClass: String,
    driverArgs: Array[String],
    proxyUser: Option[String]
)

private[spark] object ClientArguments {

  def fromCommandLineArgs(args: Array[String]): ClientArguments = {
    var mainAppResource: MainAppResource = JavaMainAppResource(None)
    var mainClass: Option[String]        = None
    val driverArgs                       = mutable.ArrayBuffer.empty[String]
    var proxyUser: Option[String]        = None

    args.sliding(2, 2).toList.foreach {
      case Array("--primary-java-resource", primaryJavaResource: String) =>
        mainAppResource = JavaMainAppResource(Some(primaryJavaResource))
      case Array("--primary-py-file", primaryPythonResource: String) =>
        mainAppResource = PythonMainAppResource(primaryPythonResource)
      case Array("--primary-r-file", primaryRFile: String) =>
        mainAppResource = RMainAppResource(primaryRFile)
      case Array("--main-class", clazz: String) =>
        mainClass = Some(clazz)
      case Array("--arg", arg: String) =>
        driverArgs += arg
      case Array("--proxy-user", user: String) =>
        proxyUser = Some(user)
      case other =>
        val invalid = other.mkString(" ")
        throw new RuntimeException(s"Unknown arguments: $invalid")
    }

    require(
      mainClass.isDefined,
      "Main class must be specified via --main-class"
    )

    ClientArguments(
      mainAppResource,
      mainClass.get,
      driverArgs.toArray,
      proxyUser
    )
  }
}

private[spark] object ArmadaClientApplication {
  private[submit] val DRIVER_PORT              = 7078
  private val DEFAULT_PRIORITY                 = 0.0
  private val DEFAULT_NAMESPACE                = "default"
  private val DEFAULT_RUN_AS_USER              = 185
  private val DEFAULT_SPARK_UI_PORT            = 4040
  private val DEFAULT_SPARK_CONNECT_PORT       = 15002
  private val DEFAULT_DRIVER_GRACE_PERIOD_SECS = 30L

  /** Returns the effective UI port - OAuth proxy port if enabled, otherwise Spark UI port. */
  private[submit] def getEffectiveUIPort(conf: SparkConf): Int = {
    OAuthSidecarBuilder.getOAuthProxyPort(conf).getOrElse {
      conf.getInt("spark.ui.port", DEFAULT_SPARK_UI_PORT)
    }
  }

  /** Returns the port the Spark Connect ingress targets: Spark Connect's own binding port. */
  private[submit] def getConnectPort(conf: SparkConf): Int = {
    conf.getInt("spark.connect.grpc.binding.port", DEFAULT_SPARK_CONNECT_PORT)
  }
}

private[submit] class DriverWatchException(msg: String) extends Exception(msg)

private[submit] sealed trait DriverTerminalState
private[submit] object DriverTerminalState {
  case object Succeeded                extends DriverTerminalState
  case class Failed(reason: String)    extends DriverTerminalState
  case class Cancelled(reason: String) extends DriverTerminalState
  case object Preempted                extends DriverTerminalState
}

/** Main class and entry point of application submission in KUBERNETES mode.
  */
private[spark] class ArmadaClientApplication extends SparkApplication {

  private val gangId = Some(java.util.UUID.randomUUID.toString)

  private def getApplicationId(conf: SparkConf): String =
    ArmadaUtils.getApplicationId(conf)

  private def log(msg: String): Unit = {
    // scalastyle:off println
    System.err.println(msg)
    // scalastyle:on println
  }

  override def start(args: Array[String], conf: SparkConf): Unit = {
    val parsedArguments = ClientArguments.fromCommandLineArgs(args)
    run(parsedArguments, conf)
  }

  private def run(
      clientArguments: ClientArguments,
      sparkConf: SparkConf
  ): Unit = {
    val armadaJobConfig = validateArmadaJobConfig(sparkConf, Some(clientArguments))

    val (host, port) = ArmadaUtils.parseMasterUrl(sparkConf.get("spark.master"))
    log(s"Connecting to Armada Server - host: $host, port: $port")

    val armadaClient =
      ArmadaClient(host, port, useSsl = false, ArmadaUtils.getAuthToken(Some(sparkConf)))
    val healthTimeout =
      Duration(sparkConf.get(ARMADA_HEALTH_CHECK_TIMEOUT), SECONDS)

    log(s"Checking Armada Server health (timeout: $healthTimeout)")
    val healthResp = Await.result(armadaClient.submitHealth(), healthTimeout)

    if (healthResp.status.isServing) {
      log("Armada Server is serving requests!")
    } else {
      throw new RuntimeException(
        "Armada health check failed - Armada Server is not serving requests!"
      )
    }

    val modeHelper = DeploymentModeHelper(sparkConf)
    val (driverJobId, _) =
      submitArmadaJob(armadaClient, clientArguments, armadaJobConfig, sparkConf)

    val lookoutBaseURL = sparkConf.get(ARMADA_LOOKOUTURL)
    val lookoutURL =
      s"$lookoutBaseURL/?page=0&sort[id]=jobId&sort[desc]=true&" +
        s"ps=50&active=false&refresh=true&" +
        s"f[0][id]=queue&f[0][value][0]=${armadaJobConfig.queue}&f[0][match]=anyOf&" +
        s"f[1][id]=jobSet&f[1][value]=${armadaJobConfig.jobSetId}&f[1][match]=startsWith"
    log(s"Lookout URL for the driver job is $lookoutURL")

    if (modeHelper.isDriverInCluster && sparkConf.get(ARMADA_DRIVER_WATCH_ENABLED)) {
      val timeout = sparkConf.get(ARMADA_DRIVER_WATCH_TIMEOUT)
      watchDriverJob(
        armadaClient,
        armadaJobConfig.queue,
        armadaJobConfig.jobSetId,
        driverJobId,
        if (timeout > 0) Some(Duration(timeout, SECONDS)) else None
      )
    }
  }

  private[submit] def watchDriverJob(
      armadaClient: ArmadaClient,
      queue: String,
      jobSetId: String,
      driverJobId: String,
      timeout: Option[Duration]
  ): Unit = {
    log(s"Watching driver job $driverJobId in job set $jobSetId...")

    var lastMessageId: String = ""
    val startTime             = System.currentTimeMillis()
    var watching              = true

    def checkTimeout(): Unit = timeout.foreach { t =>
      val elapsed = System.currentTimeMillis() - startTime
      if (elapsed > t.toMillis) {
        throw new DriverWatchException(
          s"Timed out after $t waiting for driver job $driverJobId " +
            "to reach a terminal state"
        )
      }
    }

    while (watching) {
      checkTimeout()

      try {
        val watcher = armadaClient.jobWatcher(queue, jobSetId, lastMessageId)

        while (watching && watcher.hasNext) {
          checkTimeout()

          try {
            val streamMessage = watcher.next()
            if (streamMessage != null) {
              lastMessageId = streamMessage.id
              streamMessage.message.foreach { eventMessage =>
                processDriverEvent(eventMessage, driverJobId) match {
                  case Some(DriverTerminalState.Succeeded) =>
                    log(s"Driver job $driverJobId SUCCEEDED")
                    watching = false

                  case Some(DriverTerminalState.Failed(reason)) =>
                    throw new DriverWatchException(
                      s"Driver job $driverJobId FAILED: $reason"
                    )

                  case Some(DriverTerminalState.Cancelled(reason)) =>
                    throw new DriverWatchException(
                      s"Driver job $driverJobId CANCELLED: $reason"
                    )

                  case Some(DriverTerminalState.Preempted) =>
                    throw new DriverWatchException(
                      s"Driver job $driverJobId was PREEMPTED"
                    )

                  case None => // Non-terminal or non-driver event
                }
              }
            }
          } catch {
            case e: DriverWatchException => throw e
            case NonFatal(e) =>
              log(s"Error processing event: ${e.getMessage}")
          }
        }

        if (watching) {
          log("Event stream ended, reconnecting...")
          Thread.sleep(1000)
        }
      } catch {
        case e: DriverWatchException => throw e
        case NonFatal(e) =>
          log(s"Error in event stream: ${e.getMessage}, reconnecting...")
          Thread.sleep(5000)
      }
    }
  }

  private[submit] def processDriverEvent(
      eventMessage: EventMessage,
      driverJobId: String
  ): Option[DriverTerminalState] = {
    eventMessage.events match {
      case EventMessage.Events.Submitted(event) if event.jobId == driverJobId =>
        log(s"Driver job submitted")
        None

      case EventMessage.Events.Queued(event) if event.jobId == driverJobId =>
        log(s"Driver job queued")
        None

      case EventMessage.Events.Pending(event) if event.jobId == driverJobId =>
        log(s"Driver job pending")
        None

      case EventMessage.Events.Running(event) if event.jobId == driverJobId =>
        log(s"Driver job running")
        None

      case EventMessage.Events.Succeeded(event) if event.jobId == driverJobId =>
        Some(DriverTerminalState.Succeeded)

      case EventMessage.Events.Failed(event) if event.jobId == driverJobId =>
        val reason = Option(event.reason)
          .filter(_.nonEmpty)
          .getOrElse("Unknown failure")
        Some(DriverTerminalState.Failed(reason))

      case EventMessage.Events.Cancelled(event) if event.jobId == driverJobId =>
        Some(DriverTerminalState.Cancelled("Job cancelled"))

      case EventMessage.Events.Preempted(event) if event.jobId == driverJobId =>
        Some(DriverTerminalState.Preempted)

      case _ => None
    }
  }

  private[spark] def validateArmadaJobConfig(
      conf: SparkConf,
      clientArguments: Option[ClientArguments] = None
  ): ArmadaJobConfig = {
    // Disable ConfigMap creation as we do not have support for them in Armada
    conf.set("spark.kubernetes.executor.disableConfigMap", "true")
    // Signal to SparkSubmit inside the driver pod that it should download
    // remote files (from spark.kubernetes.file.upload.path) to working directory
    conf.set("spark.kubernetes.submitInDriver", "true")

    val jobTemplate: Option[api.submit.JobSubmitRequest] = conf
      .get(ARMADA_JOB_TEMPLATE)
      .filter(_.nonEmpty)
      .map(JobTemplateLoader.loadJobTemplate)

    val driverJobItemTemplate: Option[api.submit.JobSubmitRequestItem] = conf
      .get(ARMADA_DRIVER_JOB_ITEM_TEMPLATE)
      .filter(_.nonEmpty)
      .map(JobTemplateLoader.loadJobItemTemplate)

    val executorJobItemTemplate: Option[api.submit.JobSubmitRequestItem] = conf
      .get(ARMADA_EXECUTOR_JOB_ITEM_TEMPLATE)
      .filter(_.nonEmpty)
      .map(JobTemplateLoader.loadJobItemTemplate)

    val cliConfig = parseCLIConfig(conf)

    validateRequiredConfig(
      cliConfig,
      jobTemplate,
      driverJobItemTemplate,
      executorJobItemTemplate,
      conf
    )

    val finalQueue = cliConfig.queue
      .filter(_.nonEmpty)
      .orElse(jobTemplate.map(_.queue).filter(_.nonEmpty))
      .get // Safe to use .get because validation ensures queue exists

    val finalJobSetId = cliConfig.jobSetId
      .filter(_.nonEmpty)
      .orElse(jobTemplate.map(_.jobSetId).filter(_.nonEmpty))
      .getOrElse(getApplicationId(conf))

    // Get basic feature steps from Spark's Kubernetes integration
    val (driverJobItem, driverContainer, driverSysProps) =
      getDriverFeatureSteps(conf, clientArguments)
    val (executorJobItem, executorContainer) = getExecutorFeatureSteps(conf)

    ArmadaJobConfig(
      queue = finalQueue,
      jobSetId = finalJobSetId,
      jobTemplate = jobTemplate,
      driverJobItemTemplate = driverJobItemTemplate,
      executorJobItemTemplate = executorJobItemTemplate,
      cliConfig = cliConfig,
      applicationId = getApplicationId(conf),
      driverFeatureStepJobItem = driverJobItem,
      driverFeatureStepContainer = driverContainer,
      executorFeatureStepJobItem = executorJobItem,
      executorFeatureStepContainer = executorContainer,
      driverSystemProperties = driverSysProps
    )
  }

  private case class DriverData(
      jobItem: Option[api.submit.JobSubmitRequestItem],
      configGenerator: ConfigGenerator,
      templateAnnotations: Map[String, String],
      templateLabels: Map[String, String],
      confSeq: Seq[String]
  )

  /** Get file-related system properties from driver feature steps. BasicDriverFeatureStep uploads
    * local files to spark.kubernetes.file.upload.path and returns updated remote URIs for these
    * keys.
    */
  private def applyFileUploadProperties(
      driverSystemProperties: Map[String, String],
      conf: SparkConf
  ): Unit = {
    val fileUploadKeys = Set(
      "spark.jars",
      "spark.files",
      "spark.archives",
      "spark.submit.pyFiles"
    )
    driverSystemProperties
      .filter { case (k, _) => fileUploadKeys.contains(k) }
      .foreach { case (k, v) => conf.set(k, v) }
  }

  private def buildDriverData(
      clientArguments: Option[ClientArguments],
      armadaJobConfig: ArmadaJobConfig,
      conf: SparkConf
  ): DriverData = {
    applyFileUploadProperties(armadaJobConfig.driverSystemProperties, conf)
    val confSeq         = buildSparkConfArgs(conf)
    val configGenerator = new ConfigGenerator("armada-spark-config", conf)

    val (templateAnnotations, templateLabels) = extractTemplateMetadata(armadaJobConfig.jobTemplate)

    val runtimeAnnotations = buildAnnotations(
      configGenerator,
      templateAnnotations,
      armadaJobConfig.cliConfig.nodeUniformityLabel,
      conf
    )
    val runtimeLabels = buildLabels(
      armadaJobConfig.cliConfig.podLabels,
      templateLabels,
      armadaJobConfig.cliConfig.driverLabels
    )

    val resolvedConfig = resolveJobConfig(
      armadaJobConfig.cliConfig,
      armadaJobConfig.driverJobItemTemplate,
      runtimeAnnotations,
      runtimeLabels,
      conf
    )

    // Only create the actual job item if clientArguments is provided
    val jobItem = clientArguments.map { args =>
      val primaryResource = extractPrimaryResource(args.mainAppResource).map(r =>
        resolveLocalAppResource(r, armadaJobConfig.driverFeatureStepContainer, conf)
      )
      createDriverJob(
        armadaJobConfig,
        resolvedConfig,
        configGenerator,
        args,
        primaryResource,
        confSeq,
        conf
      )
    }

    DriverData(
      jobItem = jobItem,
      configGenerator = configGenerator,
      templateAnnotations = templateAnnotations,
      templateLabels = templateLabels,
      confSeq = confSeq
    )
  }

  private[submit] def submitDriverJob(
      armadaClient: ArmadaClient,
      clientArguments: ClientArguments,
      armadaJobConfig: ArmadaJobConfig,
      conf: SparkConf
  ): String = {

    val result = buildDriverData(Some(clientArguments), armadaJobConfig, conf)
    val jobItem = result.jobItem.getOrElse(
      throw new IllegalStateException("Driver job item must be present when submitting driver job")
    )
    submitDriver(armadaClient, armadaJobConfig.queue, armadaJobConfig.jobSetId, jobItem)
  }

  private[spark] def submitExecutorJobs(
      armadaClient: ArmadaClient,
      armadaJobConfig: ArmadaJobConfig,
      conf: SparkConf,
      driverJobId: String,
      executorCount: Int
  ): Seq[String] = {
    val driverData = buildDriverData(None, armadaJobConfig, conf)

    val executorLabels = buildLabels(
      armadaJobConfig.cliConfig.podLabels,
      driverData.templateLabels,
      armadaJobConfig.cliConfig.executorLabels
    )

    val executorRuntimeAnnotations = buildAnnotations(
      driverData.configGenerator,
      driverData.templateAnnotations,
      armadaJobConfig.cliConfig.nodeUniformityLabel,
      conf
    )

    val executorResolvedConfig = resolveJobConfig(
      armadaJobConfig.cliConfig,
      armadaJobConfig.executorJobItemTemplate,
      executorRuntimeAnnotations,
      executorLabels,
      conf
    )

    // Derive driver hostname from deployment mode
    val modeHelper             = DeploymentModeHelper(conf)
    val resolvedDriverHostname = modeHelper.getDriverHostName(driverJobId)
    val executors = createExecutorJobs(
      armadaJobConfig,
      executorResolvedConfig,
      driverData.configGenerator,
      resolvedDriverHostname,
      executorCount,
      conf
    )
    submitExecutors(
      armadaClient,
      armadaJobConfig.queue,
      armadaJobConfig.jobSetId,
      executors
    )

  }

  /** Validates Armada job configuration and submits executor jobs.
    *
    * This is a convenience method that combines validateArmadaJobConfig() and submitExecutorJobs()
    * for use in dynamic allocation.
    * @return
    *   Sequence of submitted executor job IDs
    */
  private[spark] def validateAndSubmitExecutorJobs(
      armadaClient: ArmadaClient,
      conf: SparkConf,
      driverJobId: String,
      executorCount: Int
  ): Seq[String] = {
    val armadaJobConfig = validateArmadaJobConfig(conf)

    submitExecutorJobs(
      armadaClient,
      armadaJobConfig,
      conf,
      driverJobId,
      executorCount
    )
  }

  private[submit] def submitArmadaJob(
      armadaClient: ArmadaClient,
      clientArguments: ClientArguments,
      armadaJobConfig: ArmadaJobConfig,
      conf: SparkConf
  ): (String, Seq[String]) = {
    val modeHelper    = DeploymentModeHelper(conf)
    val executorCount = modeHelper.getInitialExecutorCount
    val isDynamic     = conf.getBoolean(DYN_ALLOCATION_ENABLED.key, false)

    // Allow minExecutors=0 for dynamic allocation
    if (!isDynamic && executorCount <= 0) {
      throw new IllegalArgumentException(
        s"Executor count must be greater than 0 for static allocation, but got: $executorCount"
      )
    }

    val driverJobId = if (modeHelper.isDriverInCluster) {
      submitDriverJob(armadaClient, clientArguments, armadaJobConfig, conf)
    } else {
      armadaJobConfig.applicationId
    }

    val executorJobIds = submitExecutorJobs(
      armadaClient,
      armadaJobConfig,
      conf,
      driverJobId,
      executorCount
    )
    (driverJobId, executorJobIds)
  }

  private[spark] case class CLIConfig(
      queue: Option[String],
      jobSetId: Option[String],
      namespace: Option[String],
      priority: Option[Double],
      containerImage: Option[String],
      podLabels: Map[String, String],
      driverLabels: Map[String, String],
      executorLabels: Map[String, String],
      armadaClusterUrl: Option[String],
      nodeSelectors: Map[String, String],
      nodeUniformityLabel: Option[String],
      executorConnectionTimeout: Option[Duration],
      runAsUser: Option[Long],
      driverResources: ResourceConfig,
      executorResources: ResourceConfig,
      uiIngress: Option[IngressConfig] = None,
      connectIngress: Option[IngressConfig] = None
  )

  private[spark] case class ResourceConfig(
      limitCores: Option[String],
      requestCores: Option[String],
      limitMemory: Option[String],
      requestMemory: Option[String]
  )

  private[spark] case class IngressConfig(
      annotations: Map[String, String],
      tls: Option[Boolean],
      certName: Option[String]
  )

  private[spark] def parseCLIConfig(conf: SparkConf): CLIConfig = {
    // Extract CLI values only - validation handles defaults later
    val queue          = conf.get(ARMADA_JOB_QUEUE)
    val jobSetId       = conf.get(ARMADA_JOB_SET_ID)
    val runAsUser      = conf.get(ARMADA_RUN_AS_USER)
    val containerImage = conf.get(CONTAINER_IMAGE)
    containerImage.foreach { image =>
      conf.set(KUBERNETES_CONTAINER_IMAGE.key, image)
    }

    val nodeSelectors       = conf.get(ARMADA_JOB_NODE_SELECTORS).map(commaSeparatedLabelsToMap)
    val gangUniformityLabel = conf.get(ARMADA_JOB_GANG_SCHEDULING_NODE_UNIFORMITY)

    val podLabels =
      conf.get(ARMADA_SPARK_POD_LABELS).map(commaSeparatedLabelsToMap).getOrElse(Map.empty)
    val driverLabels =
      conf.get(ARMADA_SPARK_DRIVER_LABELS).map(commaSeparatedLabelsToMap).getOrElse(Map.empty)
    val executorLabels =
      conf.get(ARMADA_SPARK_EXECUTOR_LABELS).map(commaSeparatedLabelsToMap).getOrElse(Map.empty)

    val armadaClientUrl = conf.get("spark.master")
    val armadaClusterUrl = conf
      .get(ARMADA_SERVER_INTERNAL_URL)
      .filter(_.nonEmpty)
      .map { internalUrl =>
        s"$internalUrl"
      }
      .getOrElse(armadaClientUrl)

    val driverResources = ResourceConfig(
      limitCores = conf.get(ARMADA_DRIVER_LIMIT_CORES),
      requestCores = conf.get(ARMADA_DRIVER_REQUEST_CORES),
      limitMemory = conf.get(ARMADA_DRIVER_LIMIT_MEMORY),
      requestMemory = conf.get(ARMADA_DRIVER_REQUEST_MEMORY)
    )

    val executorResources = ResourceConfig(
      limitCores = conf.get(ARMADA_EXECUTOR_LIMIT_CORES),
      requestCores = conf.get(ARMADA_EXECUTOR_REQUEST_CORES),
      limitMemory = conf.get(ARMADA_EXECUTOR_LIMIT_MEMORY),
      requestMemory = conf.get(ARMADA_EXECUTOR_REQUEST_MEMORY)
    )

    val uiIngress = if (conf.get(ARMADA_SPARK_DRIVER_UI_INGRESS_ENABLED)) {
      Some(
        IngressConfig(
          annotations = conf
            .get(ARMADA_SPARK_DRIVER_UI_INGRESS_ANNOTATIONS)
            .map(commaSeparatedAnnotationsToMap)
            .getOrElse(Map.empty),
          tls = conf.get(ARMADA_SPARK_DRIVER_UI_INGRESS_TLS_ENABLED),
          certName = conf.get(ARMADA_SPARK_DRIVER_UI_INGRESS_CERT_NAME)
        )
      )
    } else {
      None
    }

    val connectIngress = if (conf.get(ARMADA_SPARK_DRIVER_CONNECT_INGRESS_ENABLED)) {
      Some(
        IngressConfig(
          annotations = conf
            .get(ARMADA_SPARK_DRIVER_CONNECT_INGRESS_ANNOTATIONS)
            .map(commaSeparatedAnnotationsToMap)
            .getOrElse(Map.empty),
          tls = conf.get(ARMADA_SPARK_DRIVER_CONNECT_INGRESS_TLS_ENABLED),
          certName = conf.get(ARMADA_SPARK_DRIVER_CONNECT_INGRESS_CERT_NAME)
        )
      )
    } else {
      None
    }

    CLIConfig(
      queue = queue,
      jobSetId = jobSetId,
      namespace = conf.get(ARMADA_SPARK_JOB_NAMESPACE),
      priority = conf.get(ARMADA_SPARK_JOB_PRIORITY),
      containerImage = containerImage,
      podLabels = podLabels,
      driverLabels = driverLabels,
      executorLabels = executorLabels,
      nodeSelectors = nodeSelectors.getOrElse(Map.empty),
      nodeUniformityLabel = gangUniformityLabel,
      armadaClusterUrl = Some(armadaClusterUrl),
      executorConnectionTimeout =
        Some(Duration(conf.get(ARMADA_EXECUTOR_CONNECTION_TIMEOUT), SECONDS)),
      runAsUser = runAsUser,
      driverResources = driverResources,
      executorResources = executorResources,
      uiIngress = uiIngress,
      connectIngress = connectIngress
    )
  }

  /** Validates required configuration values and throws IllegalArgumentException if invalid.
    *
    * @param cliConfig
    *   CLI configuration parsed from --conf options
    * @param jobTemplate
    *   Optional job template for validation
    * @param driverJobItemTemplate
    *   Optional driver job item template for validation
    * @param executorJobItemTemplate
    *   Optional executor job item template for validation
    * @param conf
    *   Spark configuration
    * @throws IllegalArgumentException
    *   if required values are missing or invalid
    */
  private[submit] def validateRequiredConfig(
      cliConfig: CLIConfig,
      jobTemplate: Option[api.submit.JobSubmitRequest],
      driverJobItemTemplate: Option[api.submit.JobSubmitRequestItem],
      executorJobItemTemplate: Option[api.submit.JobSubmitRequestItem],
      conf: SparkConf
  ): Unit = {
    validateContainerImage(cliConfig, driverJobItemTemplate, executorJobItemTemplate)

    if (executorJobItemTemplate.exists(_.ingress.nonEmpty)) {
      throw new IllegalArgumentException(
        "Executor job item template must not contain ingress definition."
      )
    }

    if (driverJobItemTemplate.exists(_.ingress.size > 1)) {
      throw new IllegalArgumentException(
        "Driver job item template can contain only 1 ingress definition."
      )
    }

    val hasValidQueue = cliConfig.queue.exists(_.nonEmpty) ||
      jobTemplate.exists(_.queue.nonEmpty)
    if (!hasValidQueue) {
      throw new IllegalArgumentException(
        s"Queue name must be set via ${ARMADA_JOB_QUEUE.key} or in job template."
      )
    }

    if (cliConfig.jobSetId.exists(_.isEmpty)) {
      throw new IllegalArgumentException(
        s"Empty jobSetId is not allowed. Please set a valid jobSetId via ${ARMADA_JOB_SET_ID.key}"
      )
    }
  }

  private def validateContainerImage(
      cliConfig: CLIConfig,
      driverTemplate: Option[api.submit.JobSubmitRequestItem],
      executorTemplate: Option[api.submit.JobSubmitRequestItem]
  ): Unit = {
    cliConfig.containerImage match {
      case Some(image) if image.isEmpty =>
        throw new IllegalArgumentException(
          s"Empty container image is not allowed. Please set a valid container image via ${CONTAINER_IMAGE.key}"
        )
      case None =>
        val driverImage   = extractContainerImageFromTemplate(driverTemplate)
        val executorImage = extractContainerImageFromTemplate(executorTemplate)

        if (driverImage.isEmpty || executorImage.isEmpty) {
          throw new IllegalArgumentException(
            s"Container image must be set via ${CONTAINER_IMAGE.key} or provided in BOTH driver and executor job item templates " +
              s"(found driver: ${driverImage.isDefined}, executor: ${executorImage.isDefined})"
          )
        }
      case Some(_) => // Valid non-empty image provided via CLI
    }
  }

  /** Comprehensive resolved configuration holding all resolved values after applying precedence */
  private[submit] case class ResolvedJobConfig(
      namespace: String,
      priority: Double,
      containerImage: String,
      armadaClusterUrl: String,
      executorConnectionTimeout: Duration,
      annotations: Map[String, String],
      labels: Map[String, String],
      nodeSelectors: Map[String, String],
      runAsUser: Long,
      driverResources: ResolvedResourceConfig,
      executorResources: ResolvedResourceConfig,
      uiIngress: Option[api.submit.IngressConfig] = None,
      connectIngress: Option[api.submit.IngressConfig] = None
  )

  /** Resolved resource configuration for driver and executor pods */
  private[submit] case class ResolvedResourceConfig(
      limitCores: Option[String],
      requestCores: Option[String],
      limitMemory: Option[String],
      requestMemory: Option[String]
  )

  /** Resolves all configuration values by applying precedence hierarchy.
    *
    * @param cliConfig
    *   CLI configuration values
    * @param template
    *   Optional template containing default values
    * @param annotations
    *   Runtime annotations to merge with template
    * @param labels
    *   Runtime labels to merge with template
    * @param conf
    *   Spark configuration for defaults
    * @return
    *   Comprehensive resolved configuration
    */
  private[submit] def resolveJobConfig(
      cliConfig: CLIConfig,
      template: Option[api.submit.JobSubmitRequestItem],
      annotations: Map[String, String],
      labels: Map[String, String],
      conf: SparkConf
  ): ResolvedJobConfig = {
    val templateAnnotations = template.map(_.annotations).getOrElse(Map.empty)
    val templateLabels      = template.map(_.labels).getOrElse(Map.empty)

    val mergedAnnotations = templateAnnotations ++ annotations
    val mergedLabels      = templateLabels ++ labels

    val resolvedPriority = resolveValue(
      cliConfig.priority,
      template.map(_.priority),
      ArmadaClientApplication.DEFAULT_PRIORITY
    )
    val resolvedNamespace = resolveValue(
      cliConfig.namespace,
      template.map(_.namespace),
      ArmadaClientApplication.DEFAULT_NAMESPACE
    )

    val resolvedRunAsUser: Long = resolveValue(
      cliConfig.runAsUser,
      extractRunAsUserFromTemplate(template),
      ArmadaClientApplication.DEFAULT_RUN_AS_USER
    )

    val containerImage = cliConfig.containerImage
      .orElse(extractContainerImageFromTemplate(template))
      .get // Safe to use .get because validation ensures container image exists
    val armadaClusterUrl = resolveValue(
      cliConfig.armadaClusterUrl,
      None,
      conf.get("spark.master")
    )
    val executorConnectionTimeout = resolveValue(
      cliConfig.executorConnectionTimeout,
      None,
      Duration(conf.get(ARMADA_EXECUTOR_CONNECTION_TIMEOUT), SECONDS)
    )

    val resolvedUIIngressConfig =
      if (cliConfig.uiIngress.isDefined || template.flatMap(_.ingress.headOption).isDefined) {
        Some(
          resolveUIIngressConfig(
            cliConfig.uiIngress,
            template.flatMap(_.ingress.headOption),
            conf
          )
        )
      } else {
        None
      }

    val resolvedConnectIngressConfig =
      cliConfig.connectIngress.map(ci => resolveConnectIngressConfig(ci, conf))

    ResolvedJobConfig(
      namespace = resolvedNamespace,
      priority = resolvedPriority,
      containerImage = containerImage,
      armadaClusterUrl = armadaClusterUrl,
      executorConnectionTimeout = executorConnectionTimeout,
      annotations = mergedAnnotations,
      labels = mergedLabels,
      nodeSelectors = cliConfig.nodeSelectors,
      runAsUser = resolvedRunAsUser,
      driverResources = ResolvedResourceConfig(
        cliConfig.driverResources.limitCores,
        cliConfig.driverResources.requestCores,
        cliConfig.driverResources.limitMemory,
        cliConfig.driverResources.requestMemory
      ),
      executorResources = ResolvedResourceConfig(
        cliConfig.executorResources.limitCores,
        cliConfig.executorResources.requestCores,
        cliConfig.executorResources.limitMemory,
        cliConfig.executorResources.requestMemory
      ),
      uiIngress = resolvedUIIngressConfig,
      connectIngress = resolvedConnectIngressConfig
    )
  }

  /** Configuration object for Armada job submission parameters.
    *
    * @param jobTemplate
    *   Optional loaded job template for advanced job customization
    * @param driverJobItemTemplate
    *   Optional loaded driver job item template
    * @param executorJobItemTemplate
    *   Optional loaded executor job item template
    * @param cliConfig
    *   CLI configuration parameters parsed from --conf options
    * @param driverFeatureStepContainer
    *   Container from basic driver feature steps
    * @param executorFeatureStepContainer
    *   Container from basic executor feature steps
    */
  private[spark] case class ArmadaJobConfig(
      queue: String,
      jobSetId: String,
      jobTemplate: Option[api.submit.JobSubmitRequest],
      driverJobItemTemplate: Option[api.submit.JobSubmitRequestItem],
      executorJobItemTemplate: Option[api.submit.JobSubmitRequestItem],
      cliConfig: CLIConfig,
      applicationId: String,
      driverFeatureStepJobItem: Option[api.submit.JobSubmitRequestItem],
      driverFeatureStepContainer: Option[Container],
      executorFeatureStepJobItem: Option[api.submit.JobSubmitRequestItem],
      executorFeatureStepContainer: Option[Container],
      driverSystemProperties: Map[String, String]
  )

  private[submit] def createDriverJob(
      armadaJobConfig: ArmadaJobConfig,
      resolvedConfig: ResolvedJobConfig,
      configGenerator: ConfigGenerator,
      clientArguments: ClientArguments,
      primaryResource: Seq[String],
      confSeq: Seq[String],
      conf: SparkConf
  ): api.submit.JobSubmitRequestItem = {
    val driverArgs = confSeq ++ primaryResource ++ clientArguments.driverArgs

    val driverJobItem = mergeDriverTemplate(
      armadaJobConfig.driverJobItemTemplate,
      resolvedConfig,
      armadaJobConfig,
      conf.getInt(DRIVER_PORT.key, ArmadaClientApplication.DRIVER_PORT),
      clientArguments.mainClass,
      configGenerator.getVolumes,
      configGenerator.getVolumeMounts,
      driverArgs,
      conf
    )

    driverJobItem
  }

  private[submit] def createExecutorJobs(
      armadaJobConfig: ArmadaJobConfig,
      resolvedConfig: ResolvedJobConfig,
      configGenerator: ConfigGenerator,
      driverHostname: String,
      executorCount: Int,
      conf: SparkConf
  ): Seq[api.submit.JobSubmitRequestItem] = {
    ArmadaUtils.getExecutorRange(executorCount).map { _ =>
      val executorJobItem = mergeExecutorTemplate(
        armadaJobConfig.executorJobItemTemplate,
        resolvedConfig,
        armadaJobConfig,
        javaOptEnvVars(conf),
        driverHostname,
        conf.getInt(DRIVER_PORT.key, ArmadaClientApplication.DRIVER_PORT),
        configGenerator.getVolumes,
        conf
      )

      executorJobItem
    }
  }

  private def submitDriver(
      armadaClient: ArmadaClient,
      queue: String,
      jobSetId: String,
      driver: api.submit.JobSubmitRequestItem
  ): String = {
    val driverResponse = armadaClient.submitJobs(queue, jobSetId, Seq(driver))
    val driverJobId    = driverResponse.jobResponseItems.head.jobId
    val error = Some(driverResponse.jobResponseItems.head.error)
      .filter(_.nonEmpty)
      .getOrElse("none")
    log(
      s"Submitted driver job with ID: $jobSetId:$driverJobId, Error: $error"
    )
    driverJobId
  }

  private def submitExecutors(
      armadaClient: ArmadaClient,
      queue: String,
      jobSetId: String,
      executors: Seq[api.submit.JobSubmitRequestItem]
  ): Seq[String] = {
    val executorsResponse = armadaClient.submitJobs(queue, jobSetId, executors)
    executorsResponse.jobResponseItems.map { item =>
      val error = Some(item.error).filter(_.nonEmpty).getOrElse("none")
      log(s"Submitted executor job with ID: $jobSetId:${item.jobId}, Error: $error")
      item.jobId
    }
  }

  /** Merges a driver job item template with runtime configuration.
    *
    * Merge order (later overrides earlier):
    *   1. Feature Steps (base from Spark)
    *   2. Template (user-provided YAML)
    *   3. CLI config (command-line flags)
    *   4. Armada requirements (hardcoded, always wins)
    */
  private[submit] def mergeDriverTemplate(
      template: Option[api.submit.JobSubmitRequestItem],
      resolvedConfig: ResolvedJobConfig,
      armadaJobConfig: ArmadaJobConfig,
      driverPort: Int,
      mainClass: String,
      volumes: Seq[Volume],
      volumeMounts: Seq[VolumeMount],
      additionalDriverArgs: Seq[String],
      conf: SparkConf
  ): api.submit.JobSubmitRequestItem = {

    val baseJobItem = armadaJobConfig.driverFeatureStepJobItem.getOrElse(createBlankTemplate())
    val basePodSpec = baseJobItem.podSpec.getOrElse(PodSpec())

    val afterTemplate = template.flatMap(_.podSpec) match {
      case Some(templatePodSpec) =>
        PodMerger.mergePodSpecs(base = basePodSpec, overriding = templatePodSpec)
      case None =>
        basePodSpec
    }
    val afterTemplatePod = PodSpecConverter.protobufPodSpecToFabric8Pod(afterTemplate)

    import scala.jdk.CollectionConverters._
    val afterCLIVolumes = if (volumes.nonEmpty) {
      val currentSpec =
        Option(afterTemplatePod.getSpec).getOrElse(new io.fabric8.kubernetes.api.model.PodSpec())
      val currentVolumes = Option(currentSpec.getVolumes)
        .map(_.asScala.toSeq)
        .getOrElse(Seq.empty)

      val fabric8CLIVolumes = volumes.flatMap(PodSpecConverter.convertVolumeToFabric8)

      val mergedVolumes = PodMerger.mergeByName(
        currentVolumes,
        fabric8CLIVolumes
      )(v => Option(v.getName))

      new io.fabric8.kubernetes.api.model.PodBuilder(afterTemplatePod)
        .editOrNewSpec()
        .withVolumes(mergedVolumes.asJava)
        .endSpec()
        .build()
    } else {
      afterTemplatePod
    }

    val templateVolumeMounts = Option(afterCLIVolumes.getSpec)
      .flatMap(spec => Option(spec.getContainers))
      .map(_.asScala.toSeq)
      .getOrElse(Seq.empty)
      .flatMap { c =>
        Option(c.getVolumeMounts).map(_.asScala.toSeq).getOrElse(Seq.empty)
      }
      .flatMap { vm =>
        PodSpecConverter.convertVolumeMount(vm)
      }

    val mergedVolumeMounts = PodMerger.mergeByName(
      templateVolumeMounts,
      volumeMounts
    )(_.name)

    val driverContainer = newDriverContainer(
      resolvedConfig.armadaClusterUrl,
      driverPort,
      mainClass,
      mergedVolumeMounts,
      additionalDriverArgs,
      armadaJobConfig,
      conf
    )

    val currentPodSpec = PodSpecConverter.fabric8PodToProtobufPodSpec(afterCLIVolumes)
    val sidecars       = extractSidecarContainers(Some(currentPodSpec))
    // Append OAuth sidecar after user-supplied init containers so templates cannot override it
    val oauthSidecar      = OAuthSidecarBuilder.buildOAuthSidecar(conf)
    val allInitContainers = currentPodSpec.initContainers ++ oauthSidecar.toSeq

    // Set termination grace period for graceful decommissioning
    val gracePeriodSeconds = conf
      .get(KUBERNETES_SUBMIT_GRACE_PERIOD)
      .getOrElse(ArmadaClientApplication.DEFAULT_DRIVER_GRACE_PERIOD_SECS)
      .toInt

    val baseFinalPodSpec = currentPodSpec
      .withRestartPolicy("Never")
      .withTerminationGracePeriodSeconds(gracePeriodSeconds)
      .withContainers(Seq(driverContainer) ++ sidecars)
      .withInitContainers(allInitContainers)
      .withSecurityContext(new PodSecurityContext().withRunAsUser(resolvedConfig.runAsUser))
      .withNodeSelector(
        if (resolvedConfig.nodeSelectors.nonEmpty) resolvedConfig.nodeSelectors
        else currentPodSpec.nodeSelector
      )

    val finalPodSpec = resolvePriorityClassName(conf, isDriver = true)
      .map(baseFinalPodSpec.withPriorityClassName)
      .getOrElse(baseFinalPodSpec)

    val services = buildServiceConfig(driverPort, conf)

    val finalJobItem = JobSubmitRequestItem(
      priority = if (resolvedConfig.priority != ArmadaClientApplication.DEFAULT_PRIORITY) {
        resolvedConfig.priority
      } else {
        template.map(_.priority).filter(_ != 0.0).getOrElse(baseJobItem.priority)
      },
      namespace = if (resolvedConfig.namespace != ArmadaClientApplication.DEFAULT_NAMESPACE) {
        resolvedConfig.namespace
      } else {
        template
          .map(_.namespace)
          .filter(_.nonEmpty)
          .orElse(Option(baseJobItem.namespace).filter(_.nonEmpty))
          .getOrElse(ArmadaClientApplication.DEFAULT_NAMESPACE)
      },
      labels =
        baseJobItem.labels ++ template.map(_.labels).getOrElse(Map.empty) ++ resolvedConfig.labels,
      annotations = baseJobItem.annotations ++ template
        .map(_.annotations)
        .getOrElse(Map.empty) ++ resolvedConfig.annotations,
      podSpec = Some(finalPodSpec),
      services = services
    )

    val ingresses = resolvedConfig.uiIngress.toSeq ++ resolvedConfig.connectIngress.toSeq
    if (ingresses.nonEmpty) finalJobItem.withIngress(ingresses) else finalJobItem
  }

  private def createBlankTemplate(): JobSubmitRequestItem = {
    api.submit
      .JobSubmitRequestItem()
      .withPriority(ArmadaClientApplication.DEFAULT_PRIORITY)
      .withNamespace(ArmadaClientApplication.DEFAULT_NAMESPACE)
      .withLabels(Map.empty)
      .withAnnotations(Map.empty)
      .withPodSpec(PodSpec().withNodeSelector(Map.empty))
  }

  /*
    hadoop config will create configmaps which are not supported in armada.
    the code below strips them out.
   */
  private val HADOOP_CONF_VOLUME = "hadoop-properties"

  private[submit] def stripHadoopConfVolume(
      pod: io.fabric8.kubernetes.api.model.Pod
  ): io.fabric8.kubernetes.api.model.Pod = {
    val spec = pod.getSpec
    if (spec == null) return pod
    val volumes  = Option(spec.getVolumes).map(_.asScala).getOrElse(Nil)
    val filtered = volumes.filterNot(_.getName == HADOOP_CONF_VOLUME).asJava
    spec.setVolumes(filtered)
    pod
  }

  private[submit] def stripHadoopConfMount(
      container: io.fabric8.kubernetes.api.model.Container
  ): io.fabric8.kubernetes.api.model.Container = {
    val mounts   = Option(container.getVolumeMounts).map(_.asScala).getOrElse(Nil)
    val filtered = mounts.filterNot(_.getName == HADOOP_CONF_VOLUME).asJava
    container.setVolumeMounts(filtered)
    val envVars     = Option(container.getEnv).map(_.asScala).getOrElse(Nil)
    val filteredEnv = envVars.filterNot(_.getName == "HADOOP_CONF_DIR").asJava
    container.setEnv(filteredEnv)
    container
  }

  /** Converts a fabric8 Kubernetes Pod to an Armada JobSubmitRequestItem.
    *
    * Extracts metadata (labels, annotations) and converts the PodSpec to protobuf format.
    */
  private def fabric8PodToJobItem(
      fabric8Pod: io.fabric8.kubernetes.api.model.Pod
  ): JobSubmitRequestItem = {
    val labels = Option(fabric8Pod.getMetadata)
      .flatMap(m => Option(m.getLabels))
      .map(_.asScala.toMap)
      .getOrElse(Map.empty)
    val annotations = Option(fabric8Pod.getMetadata)
      .flatMap(m => Option(m.getAnnotations))
      .map(_.asScala.toMap)
      .getOrElse(Map.empty)

    val podSpec = PodSpecConverter.fabric8ToProtobuf(fabric8Pod.getSpec)

    api.submit
      .JobSubmitRequestItem()
      .withLabels(labels)
      .withAnnotations(annotations)
      .withPodSpec(podSpec)
  }

  /** Extract non-Spark containers (sidecars) from a PodSpec. Filters out the main Spark
    * driver/executor container added by buildFromFeatures(). Spark's basic feature steps use
    * reserved names "driver" and "executor" for main containers. All other containers are
    * considered sidecars and preserved.
    */
  private def extractSidecarContainers(podSpec: Option[PodSpec]): Seq[Container] = {
    podSpec
      .map(_.containers)
      .getOrElse(Seq.empty)
      .filterNot(c =>
        // Spark's basic feature steps use these exact names for main containers
        c.name.exists { name => Set("driver", "executor").contains(name.toLowerCase) }
      )
  }

  /** Apply basic Kubernetes feature steps from Spark's KubernetesDriverBuilder.
    *
    * This method calls buildFromFeatures() to apply Spark's standard driver feature steps
    * (credentials, volumes, secrets, env vars, etc.) which are needed for proper Kubernetes
    * integration.
    *
    * @param conf
    *   Spark configuration
    * @param clientArguments
    *   Client arguments with application details
    * @return
    *   A tuple of (Option[JobSubmitRequestItem], Option[Container], Map[String, String]) with basic
    *   feature steps applied. JobSubmitRequestItem contains labels, annotations, and PodSpec with
    *   init containers/sidecars. Container is the main Spark driver container with env vars and
    *   volume mounts. The Map contains system properties from the driver spec.
    */
  private[spark] def getDriverFeatureSteps(
      conf: SparkConf,
      clientArguments: Option[ClientArguments]
  ): (Option[JobSubmitRequestItem], Option[Container], Map[String, String]) = {
    clientArguments match {
      case None => (None, None, Map.empty)
      case Some(args) =>
        val appId = getApplicationId(conf)

        // We want to use the driver featuresteps to upload the local files, but
        // they will throw if the appResource is defined and
        // KUBERNETES_FILE_UPLOAD_PATH is not.  This hack works around that problem,
        // by temporarily removing the app resource during feature step processing.
        val appResource = if (conf.get(KUBERNETES_FILE_UPLOAD_PATH).isDefined) {
          args.mainAppResource
        } else {
          JavaMainAppResource(None)
        }

        // Clone conf to prevent feature step builders from mutating the original
        val driverSpec = new KubernetesDriverBuilder().buildFromFeatures(
          new KubernetesDriverConf(
            sparkConf = conf.clone(),
            appId = appId,
            mainAppResource = appResource,
            mainClass = args.mainClass,
            appArgs = args.driverArgs,
            proxyUser = args.proxyUser
          ),
          new DefaultKubernetesClient()
        )

        val cleanedPod       = stripHadoopConfVolume(driverSpec.pod.pod)
        val cleanedContainer = stripHadoopConfMount(driverSpec.pod.container)

        val jobItem   = fabric8PodToJobItem(cleanedPod)
        val container = PodSpecConverter.convertContainer(cleanedContainer)

        (Some(jobItem), Some(container), driverSpec.systemProperties)
    }
  }

  /** Apply basic Kubernetes feature steps from Spark's KubernetesExecutorBuilder.
    *
    * This method calls buildFromFeatures() to apply Spark's standard executor feature steps
    * (credentials, volumes, secrets, env vars, etc.) which are needed for proper Kubernetes
    * integration.
    *
    * @param conf
    *   Spark configuration
    * @return
    *   A tuple of (Some(JobSubmitRequestItem), Some(Container)) with basic feature steps applied.
    *   JobSubmitRequestItem contains labels, annotations, and PodSpec with init
    *   containers/sidecars. Container is the main Spark executor container with env vars and volume
    *   mounts.
    */
  private[spark] def getExecutorFeatureSteps(
      conf: SparkConf
  ): (Option[JobSubmitRequestItem], Option[Container]) = {
    val appId = getApplicationId(conf)

    // Clone conf to prevent feature step builders from mutating the original
    val clonedConf = conf.clone()
    val executorConf = new KubernetesExecutorConf(
      sparkConf = clonedConf,
      appId = appId,
      executorId = "0",
      driverPod = None
    )

    val executorSpec = new KubernetesExecutorBuilder().buildFromFeatures(
      executorConf,
      new SecurityManager(clonedConf),
      new DefaultKubernetesClient(),
      ResourceProfile.getOrCreateDefaultProfile(clonedConf)
    )

    val cleanedPod       = stripHadoopConfVolume(executorSpec.pod.pod)
    val cleanedContainer = stripHadoopConfMount(executorSpec.pod.container)

    val jobItem   = fabric8PodToJobItem(cleanedPod)
    val container = PodSpecConverter.convertContainer(cleanedContainer)

    (Some(jobItem), Some(container))
  }

  /** Merges an executor job item template with runtime configuration.
    *
    * Merge order (later overrides earlier):
    *   1. Feature Steps (base from Spark)
    *   2. Template (user-provided YAML)
    *   3. CLI config (command-line flags)
    *   4. Armada requirements (hardcoded, always wins)
    */
  private[submit] def mergeExecutorTemplate(
      template: Option[api.submit.JobSubmitRequestItem],
      resolvedConfig: ResolvedJobConfig,
      armadaJobConfig: ArmadaJobConfig,
      javaOptEnvVars: Seq[EnvVar],
      driverHostname: String,
      driverPort: Int,
      volumes: Seq[Volume],
      conf: SparkConf
  ): api.submit.JobSubmitRequestItem = {

    val baseJobItem = armadaJobConfig.executorFeatureStepJobItem.getOrElse(createBlankTemplate())
    val basePodSpec = baseJobItem.podSpec.getOrElse(PodSpec())

    val afterTemplate = template.flatMap(_.podSpec) match {
      case Some(templatePodSpec) =>
        PodMerger.mergePodSpecs(base = basePodSpec, overriding = templatePodSpec)
      case None =>
        basePodSpec
    }
    val afterTemplatePod = PodSpecConverter.protobufPodSpecToFabric8Pod(afterTemplate)

    import scala.jdk.CollectionConverters._
    val afterCLIVolumes = if (volumes.nonEmpty) {
      val currentSpec =
        Option(afterTemplatePod.getSpec).getOrElse(new model.PodSpec())
      val currentVolumes = Option(currentSpec.getVolumes)
        .map(_.asScala.toSeq)
        .getOrElse(Seq.empty)

      val fabric8CLIVolumes = volumes.flatMap(PodSpecConverter.convertVolumeToFabric8)

      val mergedVolumes = PodMerger.mergeByName(
        currentVolumes,
        fabric8CLIVolumes
      )(v => Option(v.getName))

      new PodBuilder(afterTemplatePod)
        .editOrNewSpec()
        .withVolumes(mergedVolumes.asJava)
        .endSpec()
        .build()
    } else {
      afterTemplatePod
    }

    val featureStepVolumeMounts = armadaJobConfig.executorFeatureStepContainer
      .map(_.volumeMounts)
      .getOrElse(Seq.empty)

    val templateVolumeMounts = Option(afterCLIVolumes.getSpec)
      .flatMap(spec => Option(spec.getContainers))
      .map(_.asScala.toSeq)
      .getOrElse(Seq.empty)
      .flatMap { c =>
        Option(c.getVolumeMounts).map(_.asScala.toSeq).getOrElse(Seq.empty)
      }
      .flatMap { vm =>
        PodSpecConverter.convertVolumeMount(vm)
      }

    val mergedVolumeMounts = PodMerger.mergeByName(
      featureStepVolumeMounts,
      templateVolumeMounts
    )(_.name)

    val executorContainer = newExecutorContainer(
      driverHostname,
      driverPort,
      armadaJobConfig.cliConfig.nodeUniformityLabel,
      javaOptEnvVars,
      armadaJobConfig,
      conf
    ).withVolumeMounts(mergedVolumeMounts)

    val sidecars = extractSidecarContainers(baseJobItem.podSpec)

    val executorInitContainer = newExecutorInitContainer(
      driverHostname,
      driverPort,
      resolvedConfig.executorConnectionTimeout,
      conf.get(ARMADA_EXECUTOR_INIT_CONTAINER_IMAGE),
      conf.get(ARMADA_EXECUTOR_INIT_CONTAINER_CPU),
      conf.get(ARMADA_EXECUTOR_INIT_CONTAINER_MEMORY)
    )

    val currentPodSpec = PodSpecConverter.fabric8PodToProtobufPodSpec(afterCLIVolumes)
    val allInitContainers =
      PodMerger.mergeByName(currentPodSpec.initContainers, Seq(executorInitContainer))(_.name)

    // Set termination grace period for graceful decommissioning
    val gracePeriodSeconds = conf.get(ARMADA_EXECUTOR_PREEMPTION_GRACE_PERIOD).toInt

    val baseFinalPodSpec = currentPodSpec
      .withRestartPolicy("Never")
      .withTerminationGracePeriodSeconds(gracePeriodSeconds)
      .withContainers(Seq(executorContainer) ++ sidecars)
      .withInitContainers(allInitContainers)
      .withSecurityContext(new PodSecurityContext().withRunAsUser(resolvedConfig.runAsUser))
      .withNodeSelector({
        val gangSelector = DeploymentModeHelper(conf).getGangNodeSelector
        if (resolvedConfig.nodeSelectors.nonEmpty)
          resolvedConfig.nodeSelectors ++ gangSelector
        else currentPodSpec.nodeSelector ++ gangSelector
      })

    val finalPodSpec = resolvePriorityClassName(conf, isDriver = false)
      .map(baseFinalPodSpec.withPriorityClassName)
      .getOrElse(baseFinalPodSpec)

    JobSubmitRequestItem(
      priority = if (resolvedConfig.priority != ArmadaClientApplication.DEFAULT_PRIORITY) {
        resolvedConfig.priority
      } else {
        template.map(_.priority).filter(_ != 0.0).getOrElse(baseJobItem.priority)
      },
      namespace = if (resolvedConfig.namespace != ArmadaClientApplication.DEFAULT_NAMESPACE) {
        resolvedConfig.namespace
      } else {
        template
          .map(_.namespace)
          .filter(_.nonEmpty)
          .orElse(Option(baseJobItem.namespace).filter(_.nonEmpty))
          .getOrElse(ArmadaClientApplication.DEFAULT_NAMESPACE)
      },
      labels =
        baseJobItem.labels ++ template.map(_.labels).getOrElse(Map.empty) ++ resolvedConfig.labels,
      annotations = baseJobItem.annotations ++ template
        .map(_.annotations)
        .getOrElse(Map.empty) ++ resolvedConfig.annotations,
      podSpec = Some(finalPodSpec)
    )
  }

  private def newDriverContainer(
      master: String,
      port: Int,
      mainClass: String,
      volumeMounts: Seq[VolumeMount],
      additionalDriverArgs: Seq[String],
      armadaJobConfig: ArmadaJobConfig,
      conf: SparkConf
  ): Container = {
    val source = EnvVarSource().withFieldRef(
      ObjectFieldSelector()
        .withApiVersion("v1")
        .withFieldPath("status.podIP")
    )
    val armadaJobIdSource = EnvVarSource().withFieldRef(
      ObjectFieldSelector()
        .withApiVersion("v1")
        .withFieldPath("metadata.labels['armada_job_id']")
    )
    val armadaJobSetIdSource = EnvVarSource().withFieldRef(
      ObjectFieldSelector()
        .withApiVersion("v1")
        .withFieldPath("metadata.annotations['armada_jobset_id']")
    )

    val newEnvVars = Seq(
      EnvVar().withName("SPARK_DRIVER_BIND_ADDRESS").withValueFrom(source),
      EnvVar()
        .withName(ConfigGenerator.ENV_SPARK_CONF_DIR)
        .withValue(ConfigGenerator.REMOTE_CONF_DIR_NAME),
      EnvVar().withName("ARMADA_JOB_ID").withValueFrom(armadaJobIdSource),
      EnvVar().withName("ARMADA_JOB_SET_ID").withValueFrom(armadaJobSetIdSource)
    )

    val featureStepEnvVars = armadaJobConfig.driverFeatureStepContainer
      .map(_.env)
      .getOrElse(Seq.empty)
    val envVars = featureStepEnvVars ++ newEnvVars

    val templateResources = extractResourcesFromTemplate(armadaJobConfig.driverJobItemTemplate)

    val driverLimits = Map(
      "memory" -> {
        armadaJobConfig.cliConfig.driverResources.limitMemory
          .map(value => Quantity(Option(value)))
          .orElse(templateResources.flatMap(_.limits.get("memory")))
          .getOrElse(Quantity(Option(DEFAULT_MEM)))
      },
      "cpu" -> {
        armadaJobConfig.cliConfig.driverResources.limitCores
          .map(value => Quantity(Option(value)))
          .orElse(templateResources.flatMap(_.limits.get("cpu")))
          .getOrElse(Quantity(Option(DEFAULT_CORES)))
      }
    ) ++ extractAdditionalTemplateResources(templateResources, "limits")

    val driverRequests = Map(
      "memory" -> {
        armadaJobConfig.cliConfig.driverResources.requestMemory
          .map(value => Quantity(Option(value)))
          .orElse(templateResources.flatMap(_.requests.get("memory")))
          .getOrElse(Quantity(Option(DEFAULT_MEM)))
      },
      "cpu" -> {
        armadaJobConfig.cliConfig.driverResources.requestCores
          .map(value => Quantity(Option(value)))
          .orElse(templateResources.flatMap(_.requests.get("cpu")))
          .getOrElse(Quantity(Option(DEFAULT_CORES)))
      }
    ) ++ extractAdditionalTemplateResources(templateResources, "requests")

    val containerImage = armadaJobConfig.cliConfig.containerImage
      .orElse(extractContainerImageFromTemplate(armadaJobConfig.driverJobItemTemplate))
      .get // Safe to use .get because validation ensures container image exists
    Container()
      .withName("driver")
      .withImage(containerImage)
      .withImagePullPolicy("IfNotPresent")
      .withArgs(
        Seq(
          "driver",
          "--verbose",
          "--master",
          master,
          "--class",
          mainClass,
          "--conf",
          DRIVER_PORT.key + s"=$port",
          "--conf",
          s"spark.app.id=${armadaJobConfig.applicationId}",
          "--conf",
          DRIVER_HOST_ADDRESS.key + "=$(SPARK_DRIVER_BIND_ADDRESS)"
        ) ++ additionalDriverArgs
      )
      .withVolumeMounts(
        armadaJobConfig.driverFeatureStepContainer
          .map(_.volumeMounts)
          .getOrElse(Seq.empty) ++ volumeMounts
      )
      .withEnv(envVars)
      .withResources(
        ResourceRequirements(
          requests = driverRequests,
          limits = driverLimits
        )
      )
      .withPorts(buildDriverContainerPorts(port, conf))
  }

  /** Builds container ports for the driver.
    *
    * These declarations are load-bearing: Armada only creates Service and Ingress entries for ports
    * declared on a container (or native sidecar), so every ingress-targeted port the driver JVM
    * owns must be declared here. The UI port is skipped when OAuth is enabled because the OAuth
    * proxy sidecar declares its own port and the UI stays loopback-only.
    */
  private[submit] def buildDriverContainerPorts(
      driverPort: Int,
      conf: SparkConf
  ): Seq[ContainerPort] = {
    val driverPortSpec = ContainerPort(
      containerPort = Some(driverPort),
      name = Some("driver"),
      protocol = Some("TCP")
    )

    val uiPortSpec =
      if (conf.get(ARMADA_SPARK_DRIVER_UI_INGRESS_ENABLED) && !conf.get(ARMADA_OAUTH_ENABLED)) {
        val sparkUIPort =
          conf.getInt("spark.ui.port", ArmadaClientApplication.DEFAULT_SPARK_UI_PORT)
        Some(
          ContainerPort(
            containerPort = Some(sparkUIPort),
            name = Some("ui"),
            protocol = Some("TCP")
          )
        )
      } else {
        None
      }

    val connectPortSpec =
      if (conf.get(ARMADA_SPARK_DRIVER_CONNECT_INGRESS_ENABLED)) {
        Some(
          ContainerPort(
            containerPort = Some(ArmadaClientApplication.getConnectPort(conf)),
            name = Some("connect"),
            protocol = Some("TCP")
          )
        )
      } else {
        None
      }

    Seq(driverPortSpec) ++ uiPortSpec ++ connectPortSpec
  }

  private def newExecutorContainer(
      driverHostname: String,
      driverPort: Int,
      nodeUniformityLabel: Option[String],
      javaOptEnvVars: Seq[EnvVar],
      armadaJobConfig: ArmadaJobConfig,
      conf: SparkConf
  ): Container = {
    val driverURL = s"spark://CoarseGrainedScheduler@$driverHostname:$driverPort"
    val source = EnvVarSource().withFieldRef(
      ObjectFieldSelector()
        .withApiVersion("v1")
        .withFieldPath("status.podIP")
    )
    val armadaJobIdSource = EnvVarSource().withFieldRef(
      ObjectFieldSelector()
        .withApiVersion("v1")
        .withFieldPath("metadata.labels['armada_job_id']")
    )
    val sparkExecutorMemory =
      conf.getOption("spark.executor.memory").getOrElse(DEFAULT_SPARK_EXECUTOR_MEMORY)
    val sparkExecutorCores =
      conf.getOption("spark.executor.cores").getOrElse(DEFAULT_SPARK_EXECUTOR_CORES)

    val templateResources = extractResourcesFromTemplate(armadaJobConfig.executorJobItemTemplate)

    val executorLimits = Map(
      "memory" -> {
        armadaJobConfig.cliConfig.executorResources.limitMemory
          .map(value => Quantity(Option(value)))
          .orElse(templateResources.flatMap(_.limits.get("memory")))
          .getOrElse(Quantity(Option(DEFAULT_MEM)))
      },
      "cpu" -> {
        armadaJobConfig.cliConfig.executorResources.limitCores
          .map(value => Quantity(Option(value)))
          .orElse(templateResources.flatMap(_.limits.get("cpu")))
          .getOrElse(Quantity(Option(DEFAULT_CORES)))
      }
    ) ++ extractAdditionalTemplateResources(templateResources, "limits")

    val executorRequests = Map(
      "memory" -> {
        armadaJobConfig.cliConfig.executorResources.requestMemory
          .map(value => Quantity(Option(value)))
          .orElse(templateResources.flatMap(_.requests.get("memory")))
          .getOrElse(Quantity(Option(DEFAULT_MEM)))
      },
      "cpu" -> {
        armadaJobConfig.cliConfig.executorResources.requestCores
          .map(value => Quantity(Option(value)))
          .orElse(templateResources.flatMap(_.requests.get("cpu")))
          .getOrElse(Quantity(Option(DEFAULT_CORES)))
      }
    ) ++ extractAdditionalTemplateResources(templateResources, "requests")

    val newEnvVars = Seq(
      EnvVar().withName("SPARK_EXECUTOR_ID").withValue("EXECID"),
      EnvVar().withName("SPARK_RESOURCE_PROFILE_ID").withValue("0"),
      // Ensure executor pod name is based on Armada job id label by referencing ARMADA_JOB_ID
      EnvVar().withName("SPARK_EXECUTOR_POD_NAME").withValueFrom(armadaJobIdSource),
      EnvVar().withName("ARMADA_JOB_ID").withValueFrom(armadaJobIdSource),
      EnvVar()
        .withName("SPARK_APPLICATION_ID")
        .withValue(armadaJobConfig.applicationId),
      EnvVar().withName("SPARK_EXECUTOR_CORES").withValue(sparkExecutorCores),
      EnvVar().withName("SPARK_EXECUTOR_MEMORY").withValue(sparkExecutorMemory),
      EnvVar().withName("SPARK_DRIVER_URL").withValue(driverURL),
      EnvVar().withName("SPARK_EXECUTOR_POD_IP").withValueFrom(source)
    ) ++ nodeUniformityLabel
      .map(label => EnvVar().withName("ARMADA_SPARK_GANG_NODE_UNIFORMITY_LABEL").withValue(label))

    val featureStepEnvVars = armadaJobConfig.executorFeatureStepContainer
      .map(_.env)
      .getOrElse(Seq.empty)
    val envVars = featureStepEnvVars ++ newEnvVars

    val containerImage = armadaJobConfig.cliConfig.containerImage
      .orElse(extractContainerImageFromTemplate(armadaJobConfig.executorJobItemTemplate))
      .get // Safe to use .get because validation ensures container image exists
    Container()
      .withName("executor")
      .withImage(containerImage)
      .withImagePullPolicy("IfNotPresent")
      .withArgs(
        Seq(
          "executor",
          "--cores",
          sparkExecutorCores,
          "--app-id",
          armadaJobConfig.applicationId,
          "--hostname",
          "$(SPARK_EXECUTOR_POD_IP)"
        )
      )
      .withEnv(envVars ++ javaOptEnvVars)
      .withResources(
        ResourceRequirements(
          requests = executorRequests,
          limits = executorLimits
        )
      )
  }

  private def newExecutorInitContainer(
      driverHost: String,
      driverPort: Int,
      connectionTimeout: Duration,
      image: String,
      cpu: String,
      memory: String
  ) = {
    val initContainerResources = ResourceRequirements(
      requests = Map(
        "cpu"    -> Quantity(Option(cpu)),
        "memory" -> Quantity(Option(memory))
      ),
      limits = Map(
        "cpu"    -> Quantity(Option(cpu)),
        "memory" -> Quantity(Option(memory))
      )
    )

    Container()
      .withName("wait-for-driver")
      .withImagePullPolicy("IfNotPresent")
      .withImage(image)
      .withResources(initContainerResources)
      .withEnv(
        Seq(
          EnvVar().withName("SPARK_DRIVER_HOST").withValue(driverHost),
          EnvVar().withName("SPARK_DRIVER_PORT").withValue(driverPort.toString),
          EnvVar()
            .withName("SPARK_EXECUTOR_CONNECTION_TIMEOUT")
            .withValue(connectionTimeout.toSeconds.toString)
        )
      )
      .withCommand(Seq("sh", "-c"))
      .withArgs(
        Seq(ArmadaUtils.initContainerCommand)
      )
      .withResources(initContainerResources)
  }

  // Resolves a value based on precedence: CLI > Template > Default
  private[submit] def resolveValue[T](
      cliValue: Option[T],
      templateValue: => Option[T],
      defaultValue: => T
  ): T = {
    cliValue.orElse(templateValue).getOrElse(defaultValue)
  }

  /** Builds service configuration for the driver pod. Driver port is listed first so executors can
    * connect to service-0.
    */
  private[submit] def buildServiceConfig(
      driverPort: Int,
      conf: SparkConf
  ): Seq[api.submit.ServiceConfig] = {
    val uiPort = ArmadaClientApplication.getEffectiveUIPort(conf)
    val connectPort =
      if (conf.get(ARMADA_SPARK_DRIVER_CONNECT_INGRESS_ENABLED)) {
        Seq(ArmadaClientApplication.getConnectPort(conf))
      } else {
        Seq.empty
      }
    // Headless service exposes the driver port, the UI port, and the Spark Connect port when
    // its ingress is enabled. Armada drops Service and Ingress ports that are not declared as
    // container ports, so these stay in sync with buildDriverContainerPorts.
    val ports = (Seq(driverPort, uiPort) ++ connectPort).distinct
    Seq(
      api.submit.ServiceConfig(
        `type` = api.submit.ServiceType.Headless,
        ports = ports,
        name = ""
      )
    )
  }

  /** Resolves the Spark UI ingress with CLI > Template > Default precedence. Job template ingress
    * entries mean the UI ingress. Routes to the OAuth proxy port if enabled, otherwise routes to
    * the Spark UI port.
    */
  private[submit] def resolveUIIngressConfig(
      cliIngress: Option[IngressConfig],
      templateIngress: Option[api.submit.IngressConfig],
      conf: SparkConf
  ): api.submit.IngressConfig = {
    val uiPort = ArmadaClientApplication.getEffectiveUIPort(conf)
    api.submit.IngressConfig(
      `type` = api.submit.IngressType.Ingress,
      ports = Seq(uiPort),
      annotations = templateIngress.map(_.annotations).getOrElse(Map.empty) ++
        cliIngress.map(_.annotations).getOrElse(Map.empty),
      tlsEnabled = resolveValue(
        cliIngress.flatMap(_.tls),
        templateIngress.map(_.tlsEnabled),
        false
      ),
      certName = resolveValue(
        cliIngress.flatMap(_.certName),
        templateIngress.map(_.certName),
        ""
      ),
      useClusterIP = true
    )
  }

  /** Resolves the Spark Connect ingress from spark.armada.driver.connect.ingress.*. Conf-only: job
    * template ingress entries continue to mean the Spark UI ingress.
    */
  private[submit] def resolveConnectIngressConfig(
      cliIngress: IngressConfig,
      conf: SparkConf
  ): api.submit.IngressConfig = {
    api.submit.IngressConfig(
      `type` = api.submit.IngressType.Ingress,
      ports = Seq(ArmadaClientApplication.getConnectPort(conf)),
      annotations = cliIngress.annotations,
      tlsEnabled = cliIngress.tls.getOrElse(false),
      certName = cliIngress.certName.getOrElse(""),
      useClusterIP = true
    )
  }

  private def extractContainerImageFromTemplate(
      template: Option[api.submit.JobSubmitRequestItem]
  ): Option[String] = {
    for {
      t         <- template
      podSpec   <- t.podSpec
      container <- podSpec.containers.headOption
      image     <- container.image
    } yield image
  }

  private def extractRunAsUserFromTemplate(
      template: Option[api.submit.JobSubmitRequestItem]
  ): Option[Long] = {
    for {
      t               <- template
      podSpec         <- t.podSpec
      securityContext <- podSpec.securityContext
      runAsUser       <- securityContext.runAsUser
    } yield runAsUser
  }

  // Convert the space-delimited "spark.executor.extraJavaOptions" into env vars that can be used by entrypoint.sh
  private def javaOptEnvVars(conf: SparkConf) = {
    // The executor's java opts are handled as env vars in the docker entrypoint.sh here:
    // https://github.com/apache/spark/blob/v3.5.3/resource-managers/kubernetes/docker/src/main/dockerfiles/spark/entrypoint.sh#L44-L46

    // entrypoint.sh then adds those to the jvm command line here:
    // https://github.com/apache/spark/blob/v3.5.3/resource-managers/kubernetes/docker/src/main/dockerfiles/spark/entrypoint.sh#L96
    val javaOpts =
      conf
        .getOption("spark.executor.extraJavaOptions")
        .map(_.split(" ").toSeq)
        .getOrElse(
          Seq()
        ) ++ "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.lang.invoke=ALL-UNNAMED --add-opens=java.base/java.lang.reflect=ALL-UNNAMED --add-opens=java.base/java.io=ALL-UNNAMED --add-opens=java.base/java.net=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.util.concurrent=ALL-UNNAMED --add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED --add-opens=java.base/jdk.internal.ref=ALL-UNNAMED --add-opens=java.base/sun.nio.cs=ALL-UNNAMED --add-opens=java.base/sun.security.action=ALL-UNNAMED --add-opens=java.base/sun.util.calendar=ALL-UNNAMED --add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED"
        .split(" ")
        .toSeq

    javaOpts.zipWithIndex.map { case (value: String, index) =>
      EnvVar().withName("SPARK_JAVA_OPT_" + index).withValue(value)
    }.toVector
  }

  /** Merges two sequences of environment variables, with values from secondSeq taking precedence
    * for duplicate names
    *
    * @param firstSeq
    *   First sequence of environment variables
    * @param secondSeq
    *   Second sequence of environment variables
    * @return
    *   Merged sequence with duplicates resolved from secondSeq
    */
  private[spark] def mergeEnvVars(firstSeq: Seq[EnvVar], secondSeq: Seq[EnvVar]): Seq[EnvVar] = {
    val index: EnvVar => (String, EnvVar) = env => env.getName -> env
    val merged                            = firstSeq.map(index).toMap ++ secondSeq.map(index).toMap
    merged.values.toSeq
  }

  /** Extracts resource values from a job item template's pod spec.
    *
    * @param template
    *   Optional job item template containing pod spec with resources
    * @return
    *   ResourceRequirements if found in template, None otherwise
    */
  private def extractResourcesFromTemplate(
      template: Option[api.submit.JobSubmitRequestItem]
  ): Option[ResourceRequirements] = {
    for {
      t         <- template
      podSpec   <- t.podSpec
      container <- podSpec.containers.headOption
      res       <- container.resources
    } yield res
  }

  /** Extracts additional resource types from template resources, excluding memory and CPU.
    *
    * This function filters out the standard memory and CPU resources that are handled explicitly
    * with CLI > Template > Default precedence, and returns all other resource types (like GPU,
    * ephemeral-storage, etc.) that are defined in the template.
    *
    * @param templateResources
    *   Optional resource requirements from a job item template
    * @param resourceType
    *   Either "limits" or "requests" to specify which resource map to extract from
    * @return
    *   Map of additional resource types (excluding memory/cpu) with their Quantity values
    */
  private def extractAdditionalTemplateResources(
      templateResources: Option[ResourceRequirements],
      resourceType: String
  ): Map[String, Quantity] = {
    val resourceMap = resourceType match {
      case "limits"   => templateResources.map(_.limits).getOrElse(Map.empty)
      case "requests" => templateResources.map(_.requests).getOrElse(Map.empty)
      case _ =>
        throw new IllegalArgumentException(
          s"Invalid resource type: $resourceType. Must be 'limits' or 'requests'"
        )
    }

    resourceMap.filter { case (key, _) => key != "memory" && key != "cpu" }
  }

  private def extractPrimaryResource(mainAppResource: MainAppResource): Seq[String] = {
    mainAppResource match {
      case JavaMainAppResource(Some(resource)) => Seq(resource)
      case PythonMainAppResource(resource)     => Seq(resource)
      case RMainAppResource(resource)          => Seq(resource)
      case _                                   => Seq()
    }
  }

  /** Returns true if the arg represents a local file path rather than a remote resource. Bare paths
    * (no scheme) are resolved against the default filesystem from hadoop config.
    */
  private[submit] def isLocalFile(arg: String, conf: SparkConf): Boolean = {
    try {
      val uri = new java.net.URI(arg)
      val scheme = Option(uri.getScheme).map(_.toLowerCase).getOrElse {
        val hadoopConf = SparkHadoopUtil.get.newConfiguration(conf)
        Option(FileSystem.getDefaultUri(hadoopConf).getScheme).map(_.toLowerCase).getOrElse("file")
      }
      scheme == "file" || scheme == "local"
    } catch {
      case _: java.net.URISyntaxException => false
    }
  }

  /** Resolves a local app resource path using the feature step container's args.
    *
    * When Spark's feature steps process the driver pod, they upload local files (e.g., to S3) and
    * put the resolved remote URI in the container args. The app resource is always the arg
    * immediately after the --class value in the DriverCommandFeatureStep output: ["driver", ...,
    * "--class", "<mainClass>", "<appResource>", ...appArgs]
    */
  private[submit] def resolveLocalAppResource(
      appResource: String,
      featureStepContainer: Option[Container],
      conf: SparkConf
  ): String = {
    if (!isLocalFile(appResource, conf)) return appResource
    featureStepContainer match {
      case None => appResource
      case Some(container) =>
        val args     = container.args
        val classIdx = args.indexOf("--class")
        if (classIdx >= 0 && classIdx + 2 < args.length) {
          val resolved         = args(classIdx + 2)
          val originalBasename = new java.io.File(appResource).getName
          val resolvedBasename =
            new java.net.URI(resolved).getPath.split('/').lastOption.getOrElse("")
          if (resolvedBasename == originalBasename) resolved else appResource
        } else {
          appResource
        }
    }
  }

  private def buildSparkConfArgs(conf: SparkConf): Seq[String] = {
    conf.getAll.flatMap { case (k, v) =>
      Seq("--conf", s"$k=$v")
    }
  }

  private def extractTemplateMetadata(
      jobTemplate: Option[api.submit.JobSubmitRequest]
  ): (Map[String, String], Map[String, String]) = {
    jobTemplate match {
      case Some(template) =>
        template.jobRequestItems.headOption match {
          case Some(firstItem) => (firstItem.annotations, firstItem.labels)
          case None            => (Map.empty, Map.empty)
        }
      case None => (Map.empty, Map.empty)
    }
  }

  private def buildAnnotations(
      configGenerator: ConfigGenerator,
      templateAnnotations: Map[String, String],
      nodeUniformityLabel: Option[String],
      conf: SparkConf
  ): Map[String, String] = {
    val modeHelper      = DeploymentModeHelper(conf)
    val gangCardinality = modeHelper.getGangCardinality
    configGenerator.getAnnotations ++ templateAnnotations ++ nodeUniformityLabel
      .filter(_ => gangCardinality > 0) // Only add gang annotations if cardinality > 0
      .map(label =>
        GangSchedulingAnnotations(
          gangId,
          gangCardinality,
          label
        )
      )
      .getOrElse(Map.empty)
  }

  private def resolvePriorityClassName(
      conf: SparkConf,
      isDriver: Boolean
  ): Option[String] = {
    val isGang = isDriver || DeploymentModeHelper(conf).getGangCardinality > 0
    if (isGang) conf.get(ARMADA_SCHEDULING_INITIAL_PRIORITY_CLASS)
    else conf.get(ARMADA_SCHEDULING_SCALE_UP_PRIORITY_CLASS)
  }

  private def buildLabels(
      podLabels: Map[String, String],
      templateLabels: Map[String, String],
      roleSpecificLabels: Map[String, String]
  ): Map[String, String] = {
    podLabels ++ templateLabels ++ roleSpecificLabels
  }

}
