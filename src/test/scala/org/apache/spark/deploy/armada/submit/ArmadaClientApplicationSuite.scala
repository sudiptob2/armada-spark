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

import api.event.{
  EventMessage,
  JobCancelledEvent,
  JobFailedEvent,
  JobPendingEvent,
  JobPreemptedEvent,
  JobQueuedEvent,
  JobRunningEvent,
  JobSubmittedEvent,
  JobSucceededEvent
}
import api.submit.{IngressConfig, JobSubmitRequestItem}
import k8s.io.api.core.v1.generated.{
  Container,
  EnvVar,
  PodSecurityContext,
  PodSpec,
  Volume,
  VolumeMount
}
import org.apache.spark.SparkConf
import org.apache.spark.deploy.armada.Config
import org.apache.spark.deploy.k8s.submit.JavaMainAppResource
import org.scalatest.BeforeAndAfter
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.io.{File, FileWriter}
import java.nio.file.{Files, Path}
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._

class ArmadaClientApplicationSuite extends AnyFunSuite with BeforeAndAfter with Matchers {

  private var sparkConf: SparkConf       = _
  private val armadaClientApp            = new ArmadaClientApplication()
  private var tempDir: Path              = _
  private val RUNTIME_PRIORITY           = 1.0
  private val TEMPLATE_PRIORITY          = 0.5
  private val EXECUTOR_TEMPLATE_PRIORITY = 2.5
  private val RUNTIME_RUN_AS_USER        = 385
  private val TEMPLATE_RUN_AS_USER       = 285
  private val DEFAULT_RUN_AS_USER        = 185

  // Constants for container and image names
  private val EXECUTOR_CONTAINER_NAME = "executor"
  private val DRIVER_CONTAINER_NAME   = "driver"
  private val DEFAULT_IMAGE_NAME      = "spark:3.5.0"
  private val CUSTOM_IMAGE_NAME       = "custom-spark:latest"

  // Constants for environment variables
  private val SPARK_EXECUTOR_ID = "SPARK_EXECUTOR_ID"
  private val SPARK_DRIVER_URL  = "SPARK_DRIVER_URL"

  // Constants for paths
  private val clientArguments = ClientArguments(
    mainAppResource = JavaMainAppResource(Some("app.jar")),
    mainClass = "org.example.SparkApp",
    driverArgs = Array("--input", "data.txt"),
    proxyUser = None
  )
  private var executorPodSpec: Option[PodSpec]     = _
  private var driverPodSpec: Option[PodSpec]       = _
  private var executorContainer: Option[Container] = _
  private var driverContainer: Option[Container]   = _
  before {
    tempDir = Files.createTempDirectory("armada-client-test-")
    sparkConf = new SparkConf()
      .set("spark.master", "armada://localhost:50051")
      .set("spark.app.name", "test-app")
      .set(Config.ARMADA_JOB_QUEUE.key, "test-queue")
      .set(Config.ARMADA_JOB_SET_ID.key, "test-job-set")
      .set(Config.ARMADA_SPARK_JOB_NAMESPACE.key, "test-namespace")
      .set(Config.ARMADA_SPARK_JOB_PRIORITY.key, RUNTIME_PRIORITY.toString)
      .set(Config.CONTAINER_IMAGE.key, DEFAULT_IMAGE_NAME)
      .set(Config.ARMADA_JOB_NODE_SELECTORS.key, "node-type=compute")
      .set("spark.kubernetes.container.image", DEFAULT_IMAGE_NAME)
    executorPodSpec = None
    executorContainer = None
    driverPodSpec = None
    driverContainer = None

  }

  after {
    if (tempDir != null) {
      Files
        .walk(tempDir)
        .sorted(java.util.Comparator.reverseOrder())
        .forEach(Files.deleteIfExists(_))
    }
  }

  // Feature step methods have been removed - feature steps are now applied as a final step
  // after all configuration is complete

  test("Feature steps are applied as final transformation") {
    // Feature steps are no longer tested separately since they are applied
    // as the last step in job creation

    // Feature steps are now applied as the final transformation during job creation
  }

  test("Driver system properties from feature steps are included in driver args") {
    // The driverSystemProperties field in ArmadaJobConfig should be applied
    // to the SparkConf before building driver args. This is critical for
    // spark.kubernetes.file.upload.path support where BasicDriverFeatureStep
    // uploads local files and returns remote URIs in systemProperties.

    val conf     = sparkConf.clone()
    val testFile = Files.createTempFile(tempDir, "test-file", ".txt")
    conf.set("spark.files", testFile.toAbsolutePath.toString)
    conf.set("spark.kubernetes.file.upload.path", tempDir.toAbsolutePath.toString)
    val armadaJobConfig = armadaClientApp.validateArmadaJobConfig(
      conf,
      Some(clientArguments)
    )

    // BasicDriverFeatureStep uploads local files to the upload path and returns
    // the remote URI in systemProperties under spark.files
    armadaJobConfig.driverSystemProperties should contain key "spark.files"
    armadaJobConfig.driverSystemProperties("spark.files") should not be
      testFile.toAbsolutePath.toString
    armadaJobConfig.driverSystemProperties("spark.files") should startWith(
      tempDir.toAbsolutePath.toString
    )
  }

  test("K8s pod templates are correctly read and applied") {
    // Define test variables to be used in both template and assertions
    val POD_NAME                 = "driver-pod-template"
    val APP_LABEL                = "spark-driver"
    val CUSTOM_LABEL_KEY         = "custom-label"
    val CUSTOM_LABEL_VALUE       = "custom-value"
    val CONTAINER_NAME           = "driver-container"
    val TEMPLATE_IMAGE           = "custom-spark:template"
    val VOLUME_NAME              = "test-volume"
    val MOUNT_PATH               = "/mnt/test"
    val NODE_SELECTOR_KEY        = "kubernetes.io/hostname"
    val NODE_SELECTOR_VALUE      = "worker-node-1"
    val RESTART_POLICY           = "Never"
    val TERMINATION_GRACE_PERIOD = 30

    // Create a pod template with specific properties using the defined variables
    val podTemplateContent =
      s"""apiVersion: v1
        |kind: Pod
        |metadata:
        |  name: $POD_NAME
        |  labels:
        |    app: $APP_LABEL
        |    $CUSTOM_LABEL_KEY: $CUSTOM_LABEL_VALUE
        |spec:
        |  containers:
        |  - name: $CONTAINER_NAME
        |    image: $TEMPLATE_IMAGE
        |    volumeMounts:
        |    - name: $VOLUME_NAME
        |      mountPath: $MOUNT_PATH
        |  volumes:
        |  - name: $VOLUME_NAME
        |    emptyDir: {}
        |  nodeSelector:
        |    $NODE_SELECTOR_KEY: $NODE_SELECTOR_VALUE
        |  restartPolicy: $RESTART_POLICY
        |  terminationGracePeriodSeconds: $TERMINATION_GRACE_PERIOD
        |""".stripMargin

    val templateFile = createPodTemplateFile("driver-pod-template.yaml", podTemplateContent)

    // Set the pod template in the SparkConf
    val templateConf = sparkConf.clone()
    templateConf.set("spark.kubernetes.driver.podTemplateFile", templateFile.getAbsolutePath)

    // K8s pod templates are now handled during job creation, not separately
    // Feature steps are applied as the final transformation step
  }

  private def createPodTemplateFile(filename: String, content: String): File = {
    val file   = tempDir.resolve(filename).toFile
    val writer = new FileWriter(file)
    try {
      writer.write(content)
    } finally {
      writer.close()
    }
    file
  }

  private def createJobTemplateFile(queue: String, jobSetId: String): File = {
    val templateContent =
      s"""queue: $queue
         |jobSetId: $jobSetId
         |jobRequestItems:
         |  - priority: $TEMPLATE_PRIORITY
         |    namespace: template-namespace
         |    labels:
         |      template-label: template-value
         |    annotations:
         |      template-annotation: template-value
         |""".stripMargin

    val file   = tempDir.resolve("job-template.yaml").toFile
    val writer = new FileWriter(file)
    try {
      writer.write(templateContent)
    } finally {
      writer.close()
    }
    file
  }

  private def createJobItemTemplateFile(
      priority: Double,
      namespace: String,
      filename: String = "job-item-template.yaml"
  ): File = {
    val templateContent =
      s"""priority: $priority
         |namespace: $namespace
         |labels:
         |  item-template-label: item-template-value
         |annotations:
         |  item-template-annotation: item-template-value
         |""".stripMargin

    val file   = tempDir.resolve(filename).toFile
    val writer = new FileWriter(file)
    try {
      writer.write(templateContent)
    } finally {
      writer.close()
    }
    file
  }

  test("validateArmadaJobConfig should create config without templates") {
    val config = armadaClientApp.validateArmadaJobConfig(sparkConf, Some(clientArguments))

    config.cliConfig.queue shouldBe Some("test-queue")
    config.cliConfig.jobSetId shouldBe Some("test-job-set")
    config.cliConfig.namespace shouldBe Some("test-namespace")
    config.cliConfig.priority shouldBe Some(RUNTIME_PRIORITY)
    config.cliConfig.containerImage shouldBe Some(DEFAULT_IMAGE_NAME)
    config.cliConfig.runAsUser shouldBe None
    config.jobTemplate shouldBe None
    config.driverJobItemTemplate shouldBe None
    config.executorJobItemTemplate shouldBe None
  }

  test("should use template values when CLI config not provided") {
    val templateFile = createJobTemplateFile("template-queue", "template-job-set")
    sparkConf.set(Config.ARMADA_JOB_TEMPLATE.key, templateFile.getAbsolutePath)

    sparkConf.remove(Config.ARMADA_JOB_QUEUE.key)
    val configWithTemplateQueue =
      armadaClientApp.validateArmadaJobConfig(sparkConf, Some(clientArguments))
    configWithTemplateQueue.queue shouldBe "template-queue"
    configWithTemplateQueue.jobSetId shouldBe "test-job-set"

    sparkConf.set(Config.ARMADA_JOB_QUEUE.key, "test-queue")
    sparkConf.remove(Config.ARMADA_JOB_SET_ID.key)
    val configWithTemplateJobSetId =
      armadaClientApp.validateArmadaJobConfig(sparkConf, Some(clientArguments))
    configWithTemplateJobSetId.queue shouldBe "test-queue"
    configWithTemplateJobSetId.jobSetId shouldBe "template-job-set"
  }

  test("should use app ID fallback when no template and no CLI jobSetId") {
    sparkConf.remove(Config.ARMADA_JOB_SET_ID.key)
    sparkConf.set("spark.app.id", "test-app-id")

    val config = armadaClientApp.validateArmadaJobConfig(sparkConf, Some(clientArguments))
    config.jobSetId shouldBe "test-app-id"
  }

  test("should validate required configuration values") {
    sparkConf.set(Config.ARMADA_JOB_QUEUE.key, "")
    val emptyQueueException = intercept[IllegalArgumentException] {
      armadaClientApp.validateArmadaJobConfig(sparkConf, Some(clientArguments))
    }
    emptyQueueException.getMessage should include("Queue name must be set")

    sparkConf.set(Config.ARMADA_JOB_QUEUE.key, "test-queue")
    sparkConf.set(Config.ARMADA_JOB_SET_ID.key, "")
    val emptyJobSetIdException = intercept[IllegalArgumentException] {
      armadaClientApp.validateArmadaJobConfig(sparkConf, Some(clientArguments))
    }
    emptyJobSetIdException.getMessage should include("Empty jobSetId is not allowed")

    sparkConf.set(Config.ARMADA_JOB_SET_ID.key, "test-job-set")
    sparkConf.remove(Config.CONTAINER_IMAGE.key)
    val missingImageException = intercept[IllegalArgumentException] {
      armadaClientApp.validateArmadaJobConfig(sparkConf, Some(clientArguments))
    }
    missingImageException.getMessage should include("Container image must be set")

    sparkConf.set(Config.CONTAINER_IMAGE.key, "")
    val emptyImageException = intercept[IllegalArgumentException] {
      armadaClientApp.validateArmadaJobConfig(sparkConf, Some(clientArguments))
    }
    emptyImageException.getMessage should include("Empty container image is not allowed")
  }

  test("validateArmadaJobConfig should correctly parse gangUniformityLabel configuration") {
    sparkConf.remove(Config.ARMADA_JOB_NODE_SELECTORS.key)
    sparkConf.set(Config.ARMADA_JOB_GANG_SCHEDULING_NODE_UNIFORMITY.key, "zone")

    val config = armadaClientApp.validateArmadaJobConfig(sparkConf, Some(clientArguments))

    config.cliConfig.nodeSelectors shouldBe Map.empty
    config.cliConfig.nodeUniformityLabel shouldBe Some("zone")
  }

  test("validateArmadaJobConfig should load all template types with correct precedence") {
    val jobTemplateFile = createJobTemplateFile("all-template-queue", "all-template-job-set")
    val driverTemplateFile =
      createJobItemTemplateFile(TEMPLATE_PRIORITY, "all-driver-namespace", "driver-template.yaml")
    val executorTemplateFile =
      createJobItemTemplateFile(
        EXECUTOR_TEMPLATE_PRIORITY,
        "all-executor-namespace",
        "executor-template.yaml"
      )

    sparkConf.set(Config.ARMADA_JOB_TEMPLATE.key, jobTemplateFile.getAbsolutePath)
    sparkConf.set(Config.ARMADA_DRIVER_JOB_ITEM_TEMPLATE.key, driverTemplateFile.getAbsolutePath)
    sparkConf.set(
      Config.ARMADA_EXECUTOR_JOB_ITEM_TEMPLATE.key,
      executorTemplateFile.getAbsolutePath
    )

    val config = armadaClientApp.validateArmadaJobConfig(sparkConf, Some(clientArguments))

    config.jobTemplate should not be empty
    config.jobTemplate.get.queue shouldBe "all-template-queue"
    config.driverJobItemTemplate should not be empty
    config.driverJobItemTemplate.get.namespace shouldBe "all-driver-namespace"
    config.driverJobItemTemplate.get.priority shouldBe TEMPLATE_PRIORITY
    config.driverJobItemTemplate.get.labels should contain(
      "item-template-label" -> "item-template-value"
    )
    config.executorJobItemTemplate should not be empty
    config.executorJobItemTemplate.get.namespace shouldBe "all-executor-namespace"
    config.executorJobItemTemplate.get.priority shouldBe EXECUTOR_TEMPLATE_PRIORITY
    config.executorJobItemTemplate.get.labels should contain(
      "item-template-label" -> "item-template-value"
    )
    config.cliConfig.queue shouldBe Some("test-queue")
    config.cliConfig.jobSetId shouldBe Some("test-job-set")
  }

  test("mergeExecutorTemplate should merge template with runtime configuration") {
    val template: JobSubmitRequestItem = JobSubmitRequestItem(
      priority = TEMPLATE_PRIORITY,
      namespace = "template-namespace",
      annotations = Map("template-annotation" -> "template-value"),
      labels = Map("template-label" -> "template-value"),
      podSpec = Some(
        PodSpec()
          .withTolerations(
            Seq(
              k8s.io.api.core.v1.generated
                .Toleration()
                .withKey("gpu")
                .withOperator("Equal")
                .withValue("true")
                .withEffect("NoSchedule")
            )
          )
          .withHostAliases(
            Seq(
              k8s.io.api.core.v1.generated
                .HostAlias()
                .withIp("10.0.0.1")
                .withHostnames(Seq("custom-host"))
            )
          )
          .withDnsPolicy("ClusterFirst")
          .withNodeSelector(Map("template-node-type" -> "gpu-enabled", "zone" -> "us-west"))
          .withAffinity(
            k8s.io.api.core.v1.generated
              .Affinity()
              .withNodeAffinity(
                k8s.io.api.core.v1.generated
                  .NodeAffinity()
                  .withRequiredDuringSchedulingIgnoredDuringExecution(
                    k8s.io.api.core.v1.generated
                      .NodeSelector()
                      .withNodeSelectorTerms(
                        Seq(
                          k8s.io.api.core.v1.generated
                            .NodeSelectorTerm()
                            .withMatchExpressions(
                              Seq(
                                k8s.io.api.core.v1.generated
                                  .NodeSelectorRequirement()
                                  .withKey("kubernetes.io/arch")
                                  .withOperator("In")
                                  .withValues(Seq("amd64"))
                              )
                            )
                        )
                      )
                  )
              )
          )
          .withVolumes(
            Seq(
              k8s.io.api.core.v1.generated
                .Volume()
                .withName("template-volume")
            )
          )
          .withSecurityContext(new PodSecurityContext().withRunAsUser(TEMPLATE_RUN_AS_USER))
          .withContainers(
            Seq(
              k8s.io.api.core.v1.generated
                .Container()
                .withName("template-container")
                .withImage("executor-template-image:1.0")
                .withVolumeMounts(
                  Seq(
                    k8s.io.api.core.v1.generated
                      .VolumeMount()
                      .withName("template-volume")
                      .withMountPath("/tmp/template-data")
                  )
                )
            )
          )
      )
    )

    val cliConfig = armadaClientApp.CLIConfig(
      queue = Some("test-queue"),
      jobSetId = Some("test-job-set"),
      namespace = Some("runtime-namespace"),
      priority = Some(RUNTIME_PRIORITY),
      containerImage = Some(DEFAULT_IMAGE_NAME),
      podLabels = Map.empty,
      driverLabels = Map.empty,
      executorLabels = Map.empty,
      armadaClusterUrl = Some("armada://localhost:50051"),
      nodeSelectors = Map("node-type" -> "compute"),
      nodeUniformityLabel = Some("zone"),
      executorConnectionTimeout = Some(300.seconds),
      runAsUser = Some(RUNTIME_RUN_AS_USER),
      driverResources = armadaClientApp.ResourceConfig(
        limitCores = Some("1"),
        requestCores = Some("1"),
        limitMemory = Some("1Gi"),
        requestMemory = Some("1Gi")
      ),
      executorResources = armadaClientApp.ResourceConfig(
        limitCores = Some("1"),
        requestCores = Some("1"),
        limitMemory = Some("1Gi"),
        requestMemory = Some("1Gi")
      )
    )

    val armadaJobConfig = armadaClientApp.ArmadaJobConfig(
      queue = "test-queue",
      jobSetId = "test-job-set",
      jobTemplate = None,
      driverJobItemTemplate = None,
      executorJobItemTemplate = Some(template),
      cliConfig = cliConfig,
      applicationId = "armada-spark-app-id",
      driverFeatureStepJobItem = None,
      driverFeatureStepContainer = None,
      executorFeatureStepJobItem = None,
      executorFeatureStepContainer = None,
      driverSystemProperties = Map.empty
    )

    val javaOptEnvVars = Seq(EnvVar().withName("SPARK_JAVA_OPT_0").withValue("-Xmx1g"))

    val resolvedConfig = armadaClientApp.ResolvedJobConfig(
      namespace = "runtime-namespace",
      priority = RUNTIME_PRIORITY,
      containerImage = DEFAULT_IMAGE_NAME,
      armadaClusterUrl = "armada://localhost:50051",
      executorConnectionTimeout = 300.seconds,
      runAsUser = RUNTIME_RUN_AS_USER,
      annotations =
        Map("runtime-annotation" -> "runtime-value", "template-annotation" -> "template-value"),
      labels = Map("runtime-label" -> "runtime-value", "template-label" -> "template-value"),
      nodeSelectors = Map("node-type" -> "compute"),
      driverResources =
        armadaClientApp.ResolvedResourceConfig(Some("1"), Some("1"), Some("1Gi"), Some("1Gi")),
      executorResources =
        armadaClientApp.ResolvedResourceConfig(Some("1"), Some("1"), Some("1Gi"), Some("1Gi"))
    )

    val result = armadaClientApp.mergeExecutorTemplate(
      Some(template),
      resolvedConfig,
      armadaJobConfig,
      javaOptEnvVars,
      "driver-service",
      7078,
      Seq.empty[Volume],
      sparkConf
    )

    result.priority shouldBe RUNTIME_PRIORITY
    result.namespace shouldBe "runtime-namespace"
    result.annotations should contain("template-annotation" -> "template-value")
    result.annotations should contain("runtime-annotation" -> "runtime-value")
    result.labels should contain("template-label" -> "template-value")
    result.labels should contain("runtime-label" -> "runtime-value")
    result.podSpec should not be empty
    val podSpec = result.podSpec.get

    podSpec.restartPolicy shouldBe Some("Never")
    podSpec.securityContext.get.runAsUser shouldBe Some(RUNTIME_RUN_AS_USER)
    podSpec.terminationGracePeriodSeconds shouldBe Some(180)
    podSpec.nodeSelector shouldBe Map("node-type" -> "compute")

    podSpec.initContainers should have size 1
    val initContainer = podSpec.initContainers.head
    initContainer.name shouldBe Some("wait-for-driver")
    initContainer.image shouldBe Some("busybox")
    initContainer.command.take(2) shouldBe Seq("sh", "-c")

    podSpec.containers should have size 1
    val container = podSpec.containers.head
    container.name shouldBe Some(EXECUTOR_CONTAINER_NAME)
    container.image shouldBe Some(DEFAULT_IMAGE_NAME)
    container.args should contain("executor")
    container.args should contain allOf (
      "--cores",
      "1",
      "--app-id",
      "armada-spark-app-id",
      "--hostname",
      "$(SPARK_EXECUTOR_POD_IP)"
    )

    container.env should not be empty
    val envVars = container.env
      .filter(e => e.name.isDefined && e.value.isDefined)
      .map(e => e.name.get -> e.value.get)
      .toMap
    envVars should contain key "SPARK_EXECUTOR_ID"
    envVars should contain(
      "SPARK_DRIVER_URL" -> "spark://CoarseGrainedScheduler@driver-service:7078"
    )

    container.resources should not be empty
    val resources = container.resources.get
    resources.limits should contain key "memory"
    resources.limits should contain key "cpu"
    resources.requests should contain key "memory"
    resources.requests should contain key "cpu"

    podSpec.tolerations should have size 1
    val toleration = podSpec.tolerations.head
    toleration.key shouldBe Some("gpu")
    toleration.operator shouldBe Some("Equal")
    toleration.value shouldBe Some("true")
    toleration.effect shouldBe Some("NoSchedule")

    podSpec.hostAliases should have size 1
    val hostAlias = podSpec.hostAliases.head
    hostAlias.ip shouldBe Some("10.0.0.1")
    hostAlias.hostnames shouldBe Seq("custom-host")

    podSpec.dnsPolicy shouldBe Some("ClusterFirst")

    podSpec.nodeSelector shouldBe Map("node-type" -> "compute")
    podSpec.nodeSelector should not contain ("template-node-type" -> "gpu-enabled")
    podSpec.nodeSelector should not contain ("zone"               -> "us-west")

    podSpec.affinity should not be empty
    val affinity = podSpec.affinity.get
    affinity.nodeAffinity should not be empty
    val nodeAffinity = affinity.nodeAffinity.get
    nodeAffinity.requiredDuringSchedulingIgnoredDuringExecution should not be empty

    podSpec.volumes should not be empty
    val volumeNames = podSpec.volumes.map(_.name.getOrElse("")).toSet
    volumeNames should contain("template-volume")

    val containerWithMounts = podSpec.containers.find(_.volumeMounts.nonEmpty)
    containerWithMounts should not be empty
    val volumeMounts = containerWithMounts.get.volumeMounts
    val mountNames   = volumeMounts.map(_.name.getOrElse("")).toSet
    mountNames should contain("template-volume")
  }

  test("mergeExecutorTemplate should handle template without podSpec") {
    val template: JobSubmitRequestItem = JobSubmitRequestItem(
      priority = TEMPLATE_PRIORITY,
      namespace = "template-namespace",
      annotations = Map.empty,
      labels = Map.empty,
      podSpec = None
    )

    val cliConfig = armadaClientApp.CLIConfig(
      queue = Some("test-queue"),
      jobSetId = Some("test-job-set"),
      namespace = Some("runtime-namespace"),
      priority = Some(RUNTIME_PRIORITY),
      containerImage = Some(DEFAULT_IMAGE_NAME),
      podLabels = Map.empty,
      driverLabels = Map.empty,
      executorLabels = Map.empty,
      armadaClusterUrl = Some("armada://localhost:50051"),
      nodeSelectors = Map.empty,
      nodeUniformityLabel = None,
      executorConnectionTimeout = Some(300.seconds),
      runAsUser = None,
      driverResources = armadaClientApp.ResourceConfig(
        limitCores = Some("1"),
        requestCores = Some("1"),
        limitMemory = Some("1Gi"),
        requestMemory = Some("1Gi")
      ),
      executorResources = armadaClientApp.ResourceConfig(
        limitCores = Some("1"),
        requestCores = Some("1"),
        limitMemory = Some("1Gi"),
        requestMemory = Some("1Gi")
      )
    )

    val armadaJobConfig = armadaClientApp.ArmadaJobConfig(
      queue = "test-queue",
      jobSetId = "test-job-set",
      jobTemplate = None,
      driverJobItemTemplate = None,
      executorJobItemTemplate = Some(template),
      cliConfig = cliConfig,
      applicationId = "armada-spark-app-id",
      driverFeatureStepJobItem = None,
      driverFeatureStepContainer = None,
      executorFeatureStepJobItem = None,
      executorFeatureStepContainer = None,
      driverSystemProperties = Map.empty
    )

    val resolvedConfig = armadaClientApp.ResolvedJobConfig(
      namespace = "runtime-namespace",
      priority = RUNTIME_PRIORITY,
      containerImage = DEFAULT_IMAGE_NAME,
      armadaClusterUrl = "armada://localhost:50051",
      executorConnectionTimeout = 300.seconds,
      runAsUser = DEFAULT_RUN_AS_USER,
      annotations = Map.empty,
      labels = Map.empty,
      nodeSelectors = Map.empty,
      driverResources =
        armadaClientApp.ResolvedResourceConfig(Some("1"), Some("1"), Some("1Gi"), Some("1Gi")),
      executorResources =
        armadaClientApp.ResolvedResourceConfig(Some("1"), Some("1"), Some("1Gi"), Some("1Gi"))
    )

    val result = armadaClientApp.mergeExecutorTemplate(
      Some(template),
      resolvedConfig,
      armadaJobConfig,
      Seq.empty[EnvVar],
      "driver-service",
      7078,
      Seq.empty[Volume],
      sparkConf
    )

    result.podSpec should not be empty
    val podSpec = result.podSpec.get

    podSpec.restartPolicy shouldBe Some("Never")
    podSpec.terminationGracePeriodSeconds shouldBe Some(180)
    podSpec.nodeSelector shouldBe Map.empty[String, String]

    podSpec.initContainers should have size 1
    val initContainer = podSpec.initContainers.head
    initContainer.name shouldBe Some("wait-for-driver")
    initContainer.image shouldBe Some("busybox")
    initContainer.command.take(2) shouldBe Seq("sh", "-c")

    podSpec.containers should have size 1
    val container = podSpec.containers.head
    container.name shouldBe Some(EXECUTOR_CONTAINER_NAME)
    container.image shouldBe Some(DEFAULT_IMAGE_NAME)
    container.args should contain("executor")
    container.args should contain allOf (
      "--cores",
      "1",
      "--app-id",
      "armada-spark-app-id",
      "--hostname",
      "$(SPARK_EXECUTOR_POD_IP)"
    )

    container.env should not be empty
    val envVars = container.env
      .filter(e => e.name.isDefined && e.value.isDefined)
      .map(e => e.name.get -> e.value.get)
      .toMap
    envVars should contain key "SPARK_EXECUTOR_ID"
    envVars should contain(
      "SPARK_DRIVER_URL" -> "spark://CoarseGrainedScheduler@driver-service:7078"
    )

    container.resources should not be empty
    val resources = container.resources.get
    resources.limits should contain key "memory"
    resources.limits should contain key "cpu"
    resources.requests should contain key "memory"
    resources.requests should contain key "cpu"
  }

  test("validateRequiredConfig should validate all requirements") {
    val cliConfig = armadaClientApp.CLIConfig(
      queue = Some("test-queue"),
      jobSetId = Some("test-job-set"),
      namespace = Some("test-namespace"),
      priority = Some(RUNTIME_PRIORITY),
      containerImage = Some(DEFAULT_IMAGE_NAME),
      podLabels = Map.empty,
      driverLabels = Map.empty,
      executorLabels = Map.empty,
      armadaClusterUrl = Some("armada://localhost:50051"),
      nodeSelectors = Map.empty,
      nodeUniformityLabel = None,
      executorConnectionTimeout = Some(300.seconds),
      runAsUser = None,
      driverResources = armadaClientApp.ResourceConfig(None, None, None, None),
      executorResources = armadaClientApp.ResourceConfig(None, None, None, None)
    )

    armadaClientApp.validateRequiredConfig(cliConfig, None, None, None, sparkConf)

    val invalidConfig = cliConfig.copy(containerImage = None)
    val exception = intercept[IllegalArgumentException] {
      armadaClientApp.validateRequiredConfig(invalidConfig, None, None, None, sparkConf)
    }
    exception.getMessage should include("Container image must be set")

    val emptyImageConfig = cliConfig.copy(containerImage = Some(""))
    val emptyException = intercept[IllegalArgumentException] {
      armadaClientApp.validateRequiredConfig(emptyImageConfig, None, None, None, sparkConf)
    }
    emptyException.getMessage should include("Empty container image is not allowed")

    // Test executor template with ingress should fail
    val executorTemplateWithIngress = Some(
      api.submit.JobSubmitRequestItem(
        priority = 1.0,
        namespace = "test",
        ingress = Seq(
          api.submit.IngressConfig(
            ports = Seq(7078),
            annotations = Map.empty,
            tlsEnabled = false,
            certName = ""
          )
        )
      )
    )
    val executorIngressException = intercept[IllegalArgumentException] {
      armadaClientApp.validateRequiredConfig(
        cliConfig,
        None,
        None,
        executorTemplateWithIngress,
        sparkConf
      )
    }
    executorIngressException.getMessage should include(
      "Executor job item template must not contain ingress definition"
    )

    // Test driver template with multiple ingresses should fail
    val driverTemplateWithMultipleIngresses = Some(
      api.submit.JobSubmitRequestItem(
        priority = 1.0,
        namespace = "test",
        ingress = Seq(
          api.submit.IngressConfig(
            ports = Seq(7078),
            annotations = Map.empty,
            tlsEnabled = false,
            certName = ""
          ),
          api.submit.IngressConfig(
            ports = Seq(8080),
            annotations = Map.empty,
            tlsEnabled = false,
            certName = ""
          )
        )
      )
    )
    val multipleIngressException = intercept[IllegalArgumentException] {
      armadaClientApp.validateRequiredConfig(
        cliConfig,
        None,
        driverTemplateWithMultipleIngresses,
        None,
        sparkConf
      )
    }
    multipleIngressException.getMessage should include(
      "Driver job item template can contain only 1 ingress definition"
    )
  }

  test("resolveValue should follow precedence correctly") {
    armadaClientApp.resolveValue(Some("cli"), Some("template"), "default") shouldBe "cli"
    armadaClientApp.resolveValue(None, Some("template"), "default") shouldBe "template"
    armadaClientApp.resolveValue(None, None, "default") shouldBe "default"

    armadaClientApp.resolveValue(Some(1.0), Some(2.0), 3.0) shouldBe 1.0
    armadaClientApp.resolveValue(None, Some(2.0), 3.0) shouldBe 2.0
    armadaClientApp.resolveValue(None, None, 3.0) shouldBe 3.0
  }

  test("mergeDriverTemplate should merge template with runtime configuration") {
    val template: JobSubmitRequestItem = JobSubmitRequestItem(
      priority = TEMPLATE_PRIORITY,
      namespace = "template-namespace",
      annotations = Map("template-annotation" -> "template-value"),
      labels = Map("template-label" -> "template-value"),
      podSpec = Some(
        PodSpec()
          .withTolerations(
            Seq(
              k8s.io.api.core.v1.generated
                .Toleration()
                .withKey("dedicated")
                .withOperator("Equal")
                .withValue("spark-driver")
                .withEffect("NoSchedule")
            )
          )
          .withActiveDeadlineSeconds(3600)
          .withPriorityClassName("high-priority")
          .withNodeSelector(Map("driver-node-type" -> "memory-optimized", "tier" -> "production"))
          .withVolumes(
            Seq(
              k8s.io.api.core.v1.generated
                .Volume()
                .withName("driver-template-volume")
            )
          )
          .withSecurityContext(new PodSecurityContext().withRunAsUser(TEMPLATE_RUN_AS_USER))
          .withContainers(
            Seq(
              k8s.io.api.core.v1.generated
                .Container()
                .withName("driver-template-container")
                .withImage("driver-template-image:1.0")
                .withVolumeMounts(
                  Seq(
                    k8s.io.api.core.v1.generated
                      .VolumeMount()
                      .withName("driver-template-volume")
                      .withMountPath("/driver/template-data")
                  )
                )
            )
          )
      )
    )

    val resolvedConfig = armadaClientApp.ResolvedJobConfig(
      namespace = "runtime-namespace",
      priority = RUNTIME_PRIORITY,
      containerImage = DEFAULT_IMAGE_NAME,
      armadaClusterUrl = "armada://localhost:50051",
      executorConnectionTimeout = 300.seconds,
      runAsUser = RUNTIME_RUN_AS_USER,
      annotations =
        Map("runtime-annotation" -> "runtime-value", "template-annotation" -> "template-value"),
      labels = Map("runtime-label" -> "runtime-value", "template-label" -> "template-value"),
      nodeSelectors = Map.empty,
      driverResources = armadaClientApp.ResolvedResourceConfig(None, None, None, None),
      executorResources = armadaClientApp.ResolvedResourceConfig(None, None, None, None),
      uiIngress = Some(
        IngressConfig(
          ports = Seq(7078),
          annotations = Map("nginx.ingress.kubernetes.io/rewrite-target" -> "/"),
          tlsEnabled = true,
          certName = "driver-cert"
        )
      )
    )

    val cliConfig = armadaClientApp.CLIConfig(
      queue = Some("test-queue"),
      jobSetId = Some("test-job-set"),
      namespace = Some("runtime-namespace"),
      priority = Some(RUNTIME_PRIORITY),
      containerImage = Some(DEFAULT_IMAGE_NAME),
      podLabels = Map.empty,
      driverLabels = Map.empty,
      executorLabels = Map.empty,
      armadaClusterUrl = Some("armada://localhost:50051"),
      nodeSelectors = Map.empty,
      nodeUniformityLabel = Some("zone"),
      executorConnectionTimeout = Some(300.seconds),
      runAsUser = Some(RUNTIME_RUN_AS_USER),
      driverResources = armadaClientApp.ResourceConfig(None, None, None, None),
      executorResources = armadaClientApp.ResourceConfig(None, None, None, None)
    )

    val armadaJobConfig = armadaClientApp.ArmadaJobConfig(
      queue = "test-queue",
      jobSetId = "test-job-set",
      jobTemplate = None,
      driverJobItemTemplate = Some(template),
      executorJobItemTemplate = None,
      cliConfig = cliConfig,
      applicationId = "armada-spark-app-id",
      driverFeatureStepJobItem = None,
      driverFeatureStepContainer = None,
      executorFeatureStepJobItem = None,
      executorFeatureStepContainer = None,
      driverSystemProperties = Map.empty
    )

    val result = armadaClientApp.mergeDriverTemplate(
      Some(template),
      resolvedConfig,
      armadaJobConfig,
      7078,
      "org.example.TestClass",
      Seq.empty[Volume],
      Seq.empty[VolumeMount],
      Seq("--arg1", "--arg2"),
      sparkConf
    )

    result.priority shouldBe RUNTIME_PRIORITY
    result.namespace shouldBe "runtime-namespace"
    result.annotations should contain("template-annotation" -> "template-value")
    result.annotations should contain("runtime-annotation" -> "runtime-value")
    result.labels should contain("template-label" -> "template-value")
    result.labels should contain("runtime-label" -> "runtime-value")
    result.podSpec should not be empty
    val podSpec = result.podSpec.get

    podSpec.restartPolicy shouldBe Some("Never")
    podSpec.securityContext.get.runAsUser shouldBe Some(RUNTIME_RUN_AS_USER)
    // Driver uses KUBERNETES_SUBMIT_GRACE_PERIOD (default 30s), not executor's preemption period
    podSpec.terminationGracePeriodSeconds shouldBe Some(30)
    podSpec.nodeSelector shouldBe Map(
      "driver-node-type" -> "memory-optimized",
      "tier"             -> "production"
    )

    // After OAuth integration, template sidecars are properly preserved alongside the main driver container
    podSpec.containers should have size 2
    val driverContainer = podSpec.containers.find(_.name.contains(DRIVER_CONTAINER_NAME)).get
    driverContainer.name shouldBe Some(DRIVER_CONTAINER_NAME)
    driverContainer.image shouldBe Some(DEFAULT_IMAGE_NAME)
    driverContainer.args should contain("driver")
    driverContainer.args should contain("--class")
    driverContainer.args should contain("org.example.TestClass")
    driverContainer.args should contain allOf ("--arg1", "--arg2")

    driverContainer.env should not be empty
    val envVars = driverContainer.env
      .filter(e => e.name.isDefined && e.value.isDefined)
      .map(e => e.name.get -> e.value.get)
      .toMap
    envVars should contain("SPARK_CONF_DIR" -> "/opt/spark/conf")

    // Driver container defines ports for listening
    driverContainer.ports should not be empty
    driverContainer.ports should have size 1
    driverContainer.ports.head.containerPort shouldBe Some(7078)

    result.services should have size 1
    val service = result.services.head
    service.ports should contain(7078)

    podSpec.tolerations should have size 1
    val toleration = podSpec.tolerations.head
    toleration.key shouldBe Some("dedicated")
    toleration.operator shouldBe Some("Equal")
    toleration.value shouldBe Some("spark-driver")
    toleration.effect shouldBe Some("NoSchedule")

    podSpec.activeDeadlineSeconds shouldBe Some(3600)
    podSpec.priorityClassName shouldBe Some("high-priority")

    podSpec.nodeSelector shouldBe Map(
      "driver-node-type" -> "memory-optimized",
      "tier"             -> "production"
    )

    podSpec.volumes should not be empty
    val volumeNames = podSpec.volumes.map(_.name.getOrElse("")).toSet
    volumeNames should contain("driver-template-volume")

    val containerWithMounts = podSpec.containers.find(_.volumeMounts.nonEmpty)
    containerWithMounts should not be empty
    val volumeMounts = containerWithMounts.get.volumeMounts
    val mountNames   = volumeMounts.map(_.name.getOrElse("")).toSet
    mountNames should contain("driver-template-volume")

    result.ingress should have size 1
    val ingress = result.ingress.head
    ingress.ports shouldBe Seq(7078)
    ingress.annotations should contain("nginx.ingress.kubernetes.io/rewrite-target" -> "/")
    ingress.tlsEnabled shouldBe true
    ingress.certName shouldBe "driver-cert"
  }

  test("mergeDriverTemplate should handle template without podSpec") {
    val template: JobSubmitRequestItem = JobSubmitRequestItem(
      priority = TEMPLATE_PRIORITY,
      namespace = "template-namespace",
      annotations = Map.empty,
      labels = Map.empty,
      podSpec = None
    )

    val resolvedConfig = armadaClientApp.ResolvedJobConfig(
      namespace = "runtime-namespace",
      priority = RUNTIME_PRIORITY,
      containerImage = DEFAULT_IMAGE_NAME,
      armadaClusterUrl = "armada://localhost:50051",
      executorConnectionTimeout = 300.seconds,
      runAsUser = DEFAULT_RUN_AS_USER,
      annotations = Map.empty,
      labels = Map.empty,
      nodeSelectors = Map.empty,
      driverResources = armadaClientApp.ResolvedResourceConfig(None, None, None, None),
      executorResources = armadaClientApp.ResolvedResourceConfig(None, None, None, None)
    )

    val cliConfig = armadaClientApp.CLIConfig(
      queue = Some("test-queue"),
      jobSetId = Some("test-job-set"),
      namespace = Some("runtime-namespace"),
      priority = Some(RUNTIME_PRIORITY),
      containerImage = Some(DEFAULT_IMAGE_NAME),
      podLabels = Map.empty,
      driverLabels = Map.empty,
      executorLabels = Map.empty,
      armadaClusterUrl = Some("armada://localhost:50051"),
      nodeSelectors = Map.empty,
      nodeUniformityLabel = None,
      runAsUser = None,
      executorConnectionTimeout = Some(300.seconds),
      driverResources = armadaClientApp.ResourceConfig(None, None, None, None),
      executorResources = armadaClientApp.ResourceConfig(None, None, None, None)
    )

    val armadaJobConfig = armadaClientApp.ArmadaJobConfig(
      queue = "test-queue",
      jobSetId = "test-job-set",
      jobTemplate = None,
      driverJobItemTemplate = Some(template),
      executorJobItemTemplate = None,
      cliConfig = cliConfig,
      applicationId = "armada-spark-app-id",
      driverFeatureStepJobItem = None,
      driverFeatureStepContainer = None,
      executorFeatureStepJobItem = None,
      executorFeatureStepContainer = None,
      driverSystemProperties = Map.empty
    )

    val result = armadaClientApp.mergeDriverTemplate(
      Some(template),
      resolvedConfig,
      armadaJobConfig,
      7078,
      "org.example.TestClass",
      Seq.empty[Volume],
      Seq.empty[VolumeMount],
      Seq.empty[String],
      sparkConf
    )

    result.podSpec should not be empty
    val podSpec = result.podSpec.get

    podSpec.restartPolicy shouldBe Some("Never")
    // Driver uses KUBERNETES_SUBMIT_GRACE_PERIOD (default 30s), not executor's preemption period
    podSpec.terminationGracePeriodSeconds shouldBe Some(30)
    podSpec.nodeSelector shouldBe Map.empty[String, String]

    podSpec.containers should have size 1
    val container = podSpec.containers.head
    container.name shouldBe Some(DRIVER_CONTAINER_NAME)
    container.image shouldBe Some(DEFAULT_IMAGE_NAME)
    container.args should contain("driver")
    container.args should contain("--class")
    container.args should contain("org.example.TestClass")

    container.env should not be empty
    val envVars = container.env
      .filter(e => e.name.isDefined && e.value.isDefined)
      .map(e => e.name.get -> e.value.get)
      .toMap
    envVars should contain("SPARK_CONF_DIR" -> "/opt/spark/conf")

    // Driver container defines ports for listening (OAuth disabled by default)
    container.ports should have size 1
    container.ports.head.containerPort shouldBe Some(7078)

    result.services should have size 1
    val service = result.services.head
    service.ports should contain(7078)
  }

  test("resolveConfig should resolve values with correct precedence") {
    val cliConfig = armadaClientApp.CLIConfig(
      queue = Some("test-queue"),
      jobSetId = Some("test-job-set"),
      namespace = Some("cli-namespace"),
      priority = Some(RUNTIME_PRIORITY),
      containerImage = Some(DEFAULT_IMAGE_NAME),
      podLabels = Map.empty,
      driverLabels = Map.empty,
      executorLabels = Map.empty,
      armadaClusterUrl = Some("armada://cli-url:50051"),
      nodeSelectors = Map.empty,
      nodeUniformityLabel = None,
      runAsUser = None,
      executorConnectionTimeout = Some(120.seconds),
      driverResources = armadaClientApp.ResourceConfig(None, None, None, None),
      executorResources = armadaClientApp.ResourceConfig(None, None, None, None)
    )

    val result = armadaClientApp.resolveJobConfig(
      cliConfig,
      None,
      Map.empty,
      Map.empty,
      sparkConf
    )

    result.namespace shouldBe "cli-namespace"
    result.priority shouldBe RUNTIME_PRIORITY
    result.containerImage shouldBe DEFAULT_IMAGE_NAME
    result.armadaClusterUrl shouldBe "armada://cli-url:50051"
    result.executorConnectionTimeout shouldBe 120.seconds
  }

  test("resolveJobConfig should merge template and runtime values") {
    val template = JobSubmitRequestItem(
      priority = TEMPLATE_PRIORITY,
      namespace = "template-namespace",
      annotations = Map("template-key" -> "template-value"),
      labels = Map("template-label" -> "template-value")
    )

    val cliConfig = armadaClientApp.CLIConfig(
      queue = Some("test-queue"),
      jobSetId = Some("test-job-set"),
      namespace = Some("runtime-namespace"),
      priority = Some(RUNTIME_PRIORITY),
      containerImage = Some(DEFAULT_IMAGE_NAME),
      podLabels = Map.empty,
      driverLabels = Map.empty,
      executorLabels = Map.empty,
      armadaClusterUrl = Some("armada://localhost:50051"),
      nodeSelectors = Map.empty,
      nodeUniformityLabel = None,
      runAsUser = None,
      executorConnectionTimeout = Some(300.seconds),
      driverResources = armadaClientApp.ResourceConfig(None, None, None, None),
      executorResources = armadaClientApp.ResourceConfig(None, None, None, None)
    )

    val armadaJobConfig = armadaClientApp.ArmadaJobConfig(
      queue = "test-queue",
      jobSetId = "test-job-set",
      jobTemplate = None,
      driverJobItemTemplate = None,
      executorJobItemTemplate = None,
      cliConfig = cliConfig,
      applicationId = "armada-spark-app-id",
      driverFeatureStepJobItem = None,
      driverFeatureStepContainer = None,
      executorFeatureStepJobItem = None,
      executorFeatureStepContainer = None,
      driverSystemProperties = Map.empty
    )

    val runtimeAnnotations = Map("runtime-key" -> "runtime-value")
    val runtimeLabels      = Map("runtime-label" -> "runtime-value")

    val result = armadaClientApp.resolveJobConfig(
      armadaJobConfig.cliConfig,
      Some(template),
      runtimeAnnotations,
      runtimeLabels,
      sparkConf
    )

    result.priority shouldBe RUNTIME_PRIORITY
    result.namespace shouldBe "runtime-namespace"
    result.annotations should contain("template-key" -> "template-value")
    result.annotations should contain("runtime-key" -> "runtime-value")
    result.labels should contain("template-label" -> "template-value")
    result.labels should contain("runtime-label" -> "runtime-value")
  }

  test("resolveUIIngressConfig should follow CLI > template > default precedence") {
    sparkConf.set("spark.ui.port", "7078")
    val templateIngress = IngressConfig(
      ports = Seq(8080),
      annotations = Map("foo" -> "template"),
      tlsEnabled = true,
      certName = "template-cert"
    )

    val cliIngress = armadaClientApp.IngressConfig(
      annotations = Map("bazz" -> "cli"),
      tls = Some(false),
      certName = None
    )

    val result = armadaClientApp.resolveUIIngressConfig(
      Some(cliIngress),
      Some(templateIngress),
      sparkConf
    )

    // Port should use Spark UI port since OAuth is disabled
    result.ports shouldBe Seq(7078)

    result.annotations should contain("foo" -> "template")
    result.annotations should contain("bazz" -> "cli")
    result.tlsEnabled shouldBe false
    result.certName shouldBe "template-cert"
  }

  test("resolveUIIngressConfig should use defaults when no CLI or template values") {
    val result = armadaClientApp.resolveUIIngressConfig(None, None, sparkConf)

    result.ports shouldBe Seq(4040)
    result.annotations shouldBe Map.empty
    result.tlsEnabled shouldBe false
    result.certName shouldBe ""
  }

  test(
    "mergeDriverTemplate should create valid driver job specification when no template provided"
  ) {
    val cliConfig = armadaClientApp.CLIConfig(
      queue = Some("test-queue"),
      jobSetId = Some("test-job-set"),
      namespace = Some("test-namespace"),
      priority = Some(RUNTIME_PRIORITY),
      containerImage = Some(DEFAULT_IMAGE_NAME),
      podLabels = Map.empty,
      driverLabels = Map.empty,
      executorLabels = Map.empty,
      armadaClusterUrl = Some("armada://localhost:50051"),
      nodeSelectors = Map("node-type" -> "compute"),
      nodeUniformityLabel = None,
      executorConnectionTimeout = Some(300.seconds),
      runAsUser = None,
      driverResources = armadaClientApp.ResourceConfig(
        limitCores = Some("2"),
        requestCores = Some("1"),
        limitMemory = Some("2Gi"),
        requestMemory = Some("1Gi")
      ),
      executorResources = armadaClientApp.ResourceConfig(None, None, None, None)
    )

    val armadaJobConfig = armadaClientApp.ArmadaJobConfig(
      queue = "test-queue",
      jobSetId = "test-job-set",
      jobTemplate = None,
      driverJobItemTemplate = None,
      executorJobItemTemplate = None,
      cliConfig = cliConfig,
      applicationId = "armada-spark-app-id",
      driverFeatureStepJobItem = None,
      driverFeatureStepContainer = None,
      executorFeatureStepJobItem = None,
      executorFeatureStepContainer = None,
      driverSystemProperties = Map.empty
    )

    val resolvedConfig = armadaClientApp.ResolvedJobConfig(
      namespace = "test-namespace",
      priority = RUNTIME_PRIORITY,
      containerImage = DEFAULT_IMAGE_NAME,
      armadaClusterUrl = "spark://driver:7077",
      executorConnectionTimeout = 300.seconds,
      runAsUser = DEFAULT_RUN_AS_USER,
      annotations = Map("app" -> "spark-test"),
      labels = Map("component" -> "driver"),
      nodeSelectors = Map("node-type" -> "compute"),
      driverResources = armadaClientApp.ResolvedResourceConfig(
        limitCores = Some("2"),
        requestCores = Some("1"),
        limitMemory = Some("2Gi"),
        requestMemory = Some("1Gi")
      ),
      executorResources = armadaClientApp.ResolvedResourceConfig(None, None, None, None)
    )

    val result = armadaClientApp.mergeDriverTemplate(
      template = None, // No template - will create blank one internally
      resolvedConfig = resolvedConfig,
      armadaJobConfig = armadaJobConfig,
      driverPort = 7078,
      mainClass = "org.example.SparkApp",
      volumes = Seq.empty,
      volumeMounts = Seq.empty,
      additionalDriverArgs = Seq("--arg1", "value1"),
      conf = sparkConf
    )

    result.priority shouldBe RUNTIME_PRIORITY
    result.namespace shouldBe "test-namespace"
    result.annotations should contain("app" -> "spark-test")
    result.labels should contain("component" -> "driver")
    result.podSpec should not be empty
    val podSpec = result.podSpec.get

    podSpec.restartPolicy shouldBe Some("Never")
    // Driver uses KUBERNETES_SUBMIT_GRACE_PERIOD (default 30s), not executor's preemption period
    podSpec.terminationGracePeriodSeconds shouldBe Some(30)
    podSpec.nodeSelector shouldBe Map("node-type" -> "compute")

    podSpec.containers should have size 1
    val container = podSpec.containers.head
    container.name shouldBe Some(DRIVER_CONTAINER_NAME)
    container.image shouldBe Some(DEFAULT_IMAGE_NAME)
    container.args should contain("driver")
    container.args should contain("--class")
    container.args should contain("org.example.SparkApp")
    container.args should contain allOf ("--arg1", "value1")

    container.env should not be empty
    val envVars = container.env
      .filter(e => e.name.isDefined && e.value.isDefined)
      .map(e => e.name.get -> e.value.get)
      .toMap
    envVars should contain("SPARK_CONF_DIR" -> "/opt/spark/conf")

    // Driver container defines ports for listening (OAuth disabled by default)
    container.ports should have size 1
    container.ports.head.containerPort shouldBe Some(7078)

    container.resources should not be empty
    val resources = container.resources.get
    resources.limits should contain key "memory"
    resources.limits should contain key "cpu"
    resources.requests should contain key "memory"
    resources.requests should contain key "cpu"

    result.services should have size 1
    result.services.head.ports should contain(7078)
  }

  test(
    "mergeExecutorTemplate should create valid executor job specification when no template provided"
  ) {
    val cliConfig = armadaClientApp.CLIConfig(
      queue = Some("test-queue"),
      jobSetId = Some("test-job-set"),
      namespace = Some("test-namespace"),
      priority = Some(RUNTIME_PRIORITY),
      containerImage = Some(DEFAULT_IMAGE_NAME),
      podLabels = Map.empty,
      driverLabels = Map.empty,
      executorLabels = Map.empty,
      armadaClusterUrl = Some("armada://localhost:50051"),
      nodeSelectors = Map.empty,
      nodeUniformityLabel = Some("zone"),
      executorConnectionTimeout = Some(300.seconds),
      runAsUser = None,
      driverResources = armadaClientApp.ResourceConfig(None, None, None, None),
      executorResources = armadaClientApp.ResourceConfig(
        limitCores = Some("1"),
        requestCores = Some("1"),
        limitMemory = Some("1Gi"),
        requestMemory = Some("1Gi")
      )
    )

    val armadaJobConfig = armadaClientApp.ArmadaJobConfig(
      queue = "test-queue",
      jobSetId = "test-job-set",
      jobTemplate = None,
      driverJobItemTemplate = None,
      executorJobItemTemplate = None, // No template provided
      cliConfig = cliConfig,
      applicationId = "armada-spark-app-id",
      driverFeatureStepJobItem = None,
      driverFeatureStepContainer = None,
      executorFeatureStepJobItem = None,
      executorFeatureStepContainer = None,
      driverSystemProperties = Map.empty
    )

    val resolvedConfig = armadaClientApp.ResolvedJobConfig(
      namespace = "test-namespace",
      priority = RUNTIME_PRIORITY,
      containerImage = DEFAULT_IMAGE_NAME,
      armadaClusterUrl = "armada://localhost:50051",
      executorConnectionTimeout = 300.seconds,
      runAsUser = DEFAULT_RUN_AS_USER,
      annotations = Map("app" -> "spark-test"),
      labels = Map("component" -> "executor"),
      nodeSelectors = Map.empty,
      driverResources = armadaClientApp.ResolvedResourceConfig(None, None, None, None),
      executorResources = armadaClientApp.ResolvedResourceConfig(
        limitCores = Some("1"),
        requestCores = Some("1"),
        limitMemory = Some("1Gi"),
        requestMemory = Some("1Gi")
      )
    )

    val result = armadaClientApp.mergeExecutorTemplate(
      template = None,
      resolvedConfig = resolvedConfig,
      armadaJobConfig = armadaJobConfig,
      javaOptEnvVars = Seq(EnvVar().withName("SPARK_JAVA_OPT_0").withValue("-Xmx1g")),
      driverHostname = "driver-service",
      driverPort = 7078,
      volumes = Seq.empty,
      conf = sparkConf
    )

    result.priority shouldBe RUNTIME_PRIORITY
    result.namespace shouldBe "test-namespace"
    result.annotations should contain("app" -> "spark-test")
    result.labels should contain("component" -> "executor")
    result.podSpec should not be empty
    val podSpec = result.podSpec.get

    podSpec.restartPolicy shouldBe Some("Never")
    podSpec.terminationGracePeriodSeconds shouldBe Some(180)
    podSpec.nodeSelector shouldBe Map.empty[String, String]

    podSpec.initContainers should have size 1
    val initContainer = podSpec.initContainers.head
    initContainer.name shouldBe Some("wait-for-driver")
    initContainer.image shouldBe Some("busybox")
    initContainer.command.take(2) shouldBe Seq("sh", "-c")

    podSpec.containers should have size 1
    val container = podSpec.containers.head
    container.name shouldBe Some(EXECUTOR_CONTAINER_NAME)
    container.image shouldBe Some(DEFAULT_IMAGE_NAME)
    container.args should contain("executor")
    container.args should contain allOf (
      "--cores",
      "1",
      "--app-id",
      "armada-spark-app-id",
      "--hostname",
      "$(SPARK_EXECUTOR_POD_IP)"
    )

    container.env should not be empty
    val envVars = container.env
      .filter(e => e.name.isDefined && e.value.isDefined)
      .map(e => e.name.get -> e.value.get)
      .toMap
    envVars should contain key "SPARK_EXECUTOR_ID"
    envVars should contain(
      "SPARK_DRIVER_URL" -> "spark://CoarseGrainedScheduler@driver-service:7078"
    )
    envVars should contain("SPARK_JAVA_OPT_0" -> "-Xmx1g")

    container.resources should not be empty
    val resources = container.resources.get
    resources.limits should contain key "memory"
    resources.limits should contain key "cpu"
    resources.requests should contain key "memory"
    resources.requests should contain key "cpu"
  }

  test("createDriverJob should create driver job without templates") {
    val cliConfig = armadaClientApp.CLIConfig(
      queue = Some("test-queue"),
      jobSetId = Some("test-job-set"),
      namespace = Some("test-namespace"),
      priority = Some(RUNTIME_PRIORITY),
      containerImage = Some(DEFAULT_IMAGE_NAME),
      podLabels = Map.empty,
      driverLabels = Map.empty,
      executorLabels = Map.empty,
      armadaClusterUrl = Some("armada://localhost:50051"),
      nodeSelectors = Map.empty,
      nodeUniformityLabel = None,
      executorConnectionTimeout = Some(300.seconds),
      runAsUser = None,
      driverResources = armadaClientApp.ResourceConfig(None, None, None, None),
      executorResources = armadaClientApp.ResourceConfig(None, None, None, None)
    )

    val resolvedConfig = armadaClientApp.ResolvedJobConfig(
      namespace = "test-namespace",
      priority = RUNTIME_PRIORITY,
      containerImage = DEFAULT_IMAGE_NAME,
      armadaClusterUrl = "armada://localhost:50051",
      executorConnectionTimeout = 300.seconds,
      runAsUser = DEFAULT_RUN_AS_USER,
      annotations = Map("app" -> "spark-test"),
      labels = Map("component" -> "driver"),
      nodeSelectors = Map.empty,
      driverResources = armadaClientApp.ResolvedResourceConfig(None, None, None, None),
      executorResources = armadaClientApp.ResolvedResourceConfig(None, None, None, None)
    )

    val armadaJobConfig = armadaClientApp.ArmadaJobConfig(
      queue = "test-queue",
      jobSetId = "test-job-set",
      jobTemplate = None,
      driverJobItemTemplate = None,
      executorJobItemTemplate = None,
      cliConfig = cliConfig,
      applicationId = "armada-spark-app-id",
      driverFeatureStepJobItem = None,
      driverFeatureStepContainer = None,
      executorFeatureStepJobItem = None,
      executorFeatureStepContainer = None,
      driverSystemProperties = Map.empty
    )

    val configGenerator = new ConfigGenerator(tempDir.toString, sparkConf)

    val result = armadaClientApp.createDriverJob(
      armadaJobConfig = armadaJobConfig,
      resolvedConfig = resolvedConfig,
      configGenerator = configGenerator,
      clientArguments = clientArguments,
      primaryResource = Seq("app.jar"),
      confSeq = Seq("--conf", "spark.executor.memory=1g"),
      conf = sparkConf
    )

    result.priority shouldBe RUNTIME_PRIORITY
    result.namespace shouldBe "test-namespace"
    result.podSpec should not be empty
    val podSpec = result.podSpec.get

    podSpec.restartPolicy shouldBe Some("Never")
    // Driver uses KUBERNETES_SUBMIT_GRACE_PERIOD (default 30s), not executor's preemption period
    podSpec.terminationGracePeriodSeconds shouldBe Some(30)
    podSpec.nodeSelector shouldBe Map.empty[String, String]

    podSpec.containers should have size 1
    val container = podSpec.containers.head
    container.name shouldBe Some(DRIVER_CONTAINER_NAME)
    container.image shouldBe Some(DEFAULT_IMAGE_NAME)

    container.env should not be empty
    val envVars = container.env
      .filter(e => e.name.isDefined && e.value.isDefined)
      .map(e => e.name.get -> e.value.get)
      .toMap
    envVars should contain("SPARK_CONF_DIR" -> "/opt/spark/conf")

    // Driver container defines ports for listening (OAuth disabled by default)
    container.ports should have size 1
    container.ports.head.containerPort shouldBe Some(7078)

    result.services should have size 1
  }

  test("resolveLocalAppResource returns feature step app resource for local path") {
    val featureStepContainer = Container()
      .withArgs(
        Seq(
          "driver",
          "--properties-file",
          "/opt/spark/conf/spark.properties",
          "--class",
          "org.apache.spark.deploy.PythonRunner",
          "local:///opt/spark/work/pi.py",
          "100"
        )
      )

    val result = armadaClientApp.resolveLocalAppResource(
      "/opt/files/pi.py",
      Some(featureStepContainer),
      sparkConf
    )

    result shouldBe "local:///opt/spark/work/pi.py"
  }

  test("resolveLocalAppResource returns original when no container") {
    val result = armadaClientApp.resolveLocalAppResource("script.py", None, sparkConf)

    result shouldBe "script.py"
  }

  test("resolveLocalAppResource resolves to s3a:// uploaded path") {
    val featureStepContainer = Container()
      .withArgs(
        Seq(
          "driver",
          "--properties-file",
          "/opt/spark/conf/spark.properties",
          "--class",
          "org.apache.spark.deploy.PythonRunner",
          "s3a://kafka-s3/gbj/tmp/spark-upload-abc123/read_lines.py"
        )
      )

    val result = armadaClientApp.resolveLocalAppResource(
      "/opt/files/read_lines.py",
      Some(featureStepContainer),
      sparkConf
    )

    result shouldBe "s3a://kafka-s3/gbj/tmp/spark-upload-abc123/read_lines.py"
  }

  test("resolveLocalAppResource does not resolve s3a:// remote resource") {
    val featureStepContainer = Container()
      .withArgs(
        Seq("driver", "--class", "Main", "local:///opt/spark/work/data.csv")
      )

    val result = armadaClientApp.resolveLocalAppResource(
      "s3a://bucket/path/data.csv",
      Some(featureStepContainer),
      sparkConf
    )

    result shouldBe "s3a://bucket/path/data.csv"
  }

  test("resolveLocalAppResource returns original when no --class in args") {
    val featureStepContainer = Container()
      .withArgs(Seq("driver", "local:///opt/spark/work/app.jar"))

    val result = armadaClientApp.resolveLocalAppResource(
      "/home/user/app.jar",
      Some(featureStepContainer),
      sparkConf
    )

    result shouldBe "/home/user/app.jar"
  }

  test("resolveLocalAppResource returns original when basenames do not match") {
    val featureStepContainer = Container()
      .withArgs(
        Seq(
          "driver",
          "--properties-file",
          "/opt/spark/conf/spark.properties",
          "--class",
          "org.apache.spark.deploy.PythonRunner",
          "s3a://kafka-s3/gbj/tmp/spark-upload-abc123/other_script.py"
        )
      )

    val result = armadaClientApp.resolveLocalAppResource(
      "/opt/files/read_lines.py",
      Some(featureStepContainer),
      sparkConf
    )

    result shouldBe "/opt/files/read_lines.py"
  }

  test("isLocalFile treats bare path as remote when defaultFS is s3a") {
    val s3Conf = sparkConf.clone()
    s3Conf.set("spark.hadoop.fs.defaultFS", "s3a://my-bucket")

    armadaClientApp.isLocalFile("/data/app.jar", s3Conf) shouldBe false
  }

  test("isLocalFile treats bare path as local when defaultFS is file") {
    armadaClientApp.isLocalFile("/data/app.jar", sparkConf) shouldBe true
  }

  test("createExecutorJobs should create multiple executor jobs") {
    val cliConfig = armadaClientApp.CLIConfig(
      queue = Some("test-queue"),
      jobSetId = Some("test-job-set"),
      namespace = Some("test-namespace"),
      priority = Some(RUNTIME_PRIORITY),
      containerImage = Some(DEFAULT_IMAGE_NAME),
      podLabels = Map.empty,
      driverLabels = Map.empty,
      executorLabels = Map.empty,
      armadaClusterUrl = Some("armada://localhost:50051"),
      nodeSelectors = Map.empty,
      nodeUniformityLabel = None,
      executorConnectionTimeout = Some(300.seconds),
      runAsUser = None,
      driverResources = armadaClientApp.ResourceConfig(None, None, None, None),
      executorResources = armadaClientApp.ResourceConfig(None, None, None, None)
    )

    val resolvedConfig = armadaClientApp.ResolvedJobConfig(
      namespace = "test-namespace",
      priority = RUNTIME_PRIORITY,
      containerImage = DEFAULT_IMAGE_NAME,
      armadaClusterUrl = "armada://localhost:50051",
      executorConnectionTimeout = 300.seconds,
      runAsUser = DEFAULT_RUN_AS_USER,
      annotations = Map("app" -> "spark-test"),
      labels = Map("component" -> "executor"),
      nodeSelectors = Map.empty,
      driverResources = armadaClientApp.ResolvedResourceConfig(None, None, None, None),
      executorResources = armadaClientApp.ResolvedResourceConfig(None, None, None, None)
    )

    val armadaJobConfig = armadaClientApp.ArmadaJobConfig(
      queue = "test-queue",
      jobSetId = "test-job-set",
      jobTemplate = None,
      driverJobItemTemplate = None,
      executorJobItemTemplate = None,
      cliConfig = cliConfig,
      applicationId = "armada-spark-app-id",
      driverFeatureStepJobItem = None,
      driverFeatureStepContainer = None,
      executorFeatureStepJobItem = None,
      executorFeatureStepContainer = None,
      driverSystemProperties = Map.empty
    )

    val configGenerator = new ConfigGenerator(tempDir.toString, sparkConf)

    // Create a dummy driver JobItem for the test
    val driverJobItem = api.submit
      .JobSubmitRequestItem()
      .withLabels(Map("spark-role" -> "driver"))
      .withPodSpec(PodSpec())

    val results = armadaClientApp.createExecutorJobs(
      armadaJobConfig = armadaJobConfig,
      resolvedConfig = resolvedConfig,
      configGenerator = configGenerator,
      driverHostname = "driver-service",
      executorCount = 2,
      conf = sparkConf
    )

    results should have size 2
    results.zipWithIndex.foreach { case (job, _) =>
      job.priority shouldBe RUNTIME_PRIORITY
      job.namespace shouldBe "test-namespace"
      job.podSpec should not be empty
      val podSpec = job.podSpec.get

      podSpec.restartPolicy shouldBe Some("Never")
      podSpec.terminationGracePeriodSeconds shouldBe Some(180)
      podSpec.nodeSelector shouldBe Map.empty[String, String]

      podSpec.initContainers should have size 1
      val initContainer = podSpec.initContainers.head
      initContainer.name shouldBe Some("wait-for-driver")
      initContainer.image shouldBe Some("busybox")
      initContainer.command.take(2) shouldBe Seq("sh", "-c")

      podSpec.containers should have size 1
      val container = podSpec.containers.head
      container.name shouldBe Some(EXECUTOR_CONTAINER_NAME)
      container.image shouldBe Some(DEFAULT_IMAGE_NAME)
      container.args should contain("executor")
      container.args should contain allOf (
        "--cores",
        "1",
        "--app-id",
        "armada-spark-app-id",
        "--hostname",
        "$(SPARK_EXECUTOR_POD_IP)"
      )

      val envVars = container.env
        .filter(e => e.name.isDefined && e.value.isDefined)
        .map(e => e.name.get -> e.value.get)
        .toMap
      envVars should contain key "SPARK_EXECUTOR_ID"
      envVars should contain(
        "SPARK_DRIVER_URL" -> "spark://CoarseGrainedScheduler@driver-service:7078"
      )
    }
  }

  test(
    "submitArmadaJob should validate executor count is greater than zero for static allocation"
  ) {
    val armadaJobConfig = armadaClientApp.ArmadaJobConfig(
      queue = "test-queue",
      jobSetId = "test-job-set",
      jobTemplate = None,
      driverJobItemTemplate = None,
      executorJobItemTemplate = None,
      cliConfig = armadaClientApp.CLIConfig(
        queue = Some("test-queue"),
        jobSetId = Some("test-job-set"),
        namespace = Some("test-namespace"),
        priority = Some(RUNTIME_PRIORITY),
        containerImage = Some(DEFAULT_IMAGE_NAME),
        podLabels = Map.empty,
        driverLabels = Map.empty,
        executorLabels = Map.empty,
        armadaClusterUrl = Some("armada://localhost:50051"),
        nodeSelectors = Map.empty,
        nodeUniformityLabel = None,
        executorConnectionTimeout = Some(300.seconds),
        runAsUser = None,
        driverResources = armadaClientApp.ResourceConfig(None, None, None, None),
        executorResources = armadaClientApp.ResourceConfig(None, None, None, None)
      ),
      applicationId = "armada-spark-app-id",
      driverFeatureStepJobItem = None,
      driverFeatureStepContainer = None,
      executorFeatureStepJobItem = None,
      executorFeatureStepContainer = None,
      driverSystemProperties = Map.empty
    )

    sparkConf.set("spark.executor.instances", "0")
    sparkConf.set("spark.dynamicAllocation.enabled", "false")

    val exception = intercept[IllegalArgumentException] {
      armadaClientApp.submitArmadaJob(null, clientArguments, armadaJobConfig, sparkConf)
    }
    exception.getMessage should include("Executor count must be greater than 0")
  }

  test("submitArmadaJob should allow minExecutors=0 for dynamic allocation") {
    val armadaJobConfig = armadaClientApp.ArmadaJobConfig(
      queue = "test-queue",
      jobSetId = "test-job-set",
      jobTemplate = None,
      driverJobItemTemplate = None,
      executorJobItemTemplate = None,
      cliConfig = armadaClientApp.CLIConfig(
        queue = Some("test-queue"),
        jobSetId = Some("test-job-set"),
        namespace = Some("test-namespace"),
        priority = Some(RUNTIME_PRIORITY),
        containerImage = Some(DEFAULT_IMAGE_NAME),
        podLabels = Map.empty,
        driverLabels = Map.empty,
        executorLabels = Map.empty,
        armadaClusterUrl = Some("armada://localhost:50051"),
        nodeSelectors = Map.empty,
        nodeUniformityLabel = Some("armada-spark"),
        executorConnectionTimeout = Some(300.seconds),
        runAsUser = None,
        driverResources = armadaClientApp.ResourceConfig(None, None, None, None),
        executorResources = armadaClientApp.ResourceConfig(None, None, None, None)
      ),
      applicationId = "armada-spark-app-id",
      driverFeatureStepJobItem = None,
      driverFeatureStepContainer = None,
      executorFeatureStepJobItem = None,
      executorFeatureStepContainer = None,
      driverSystemProperties = Map.empty
    )

    sparkConf.set("spark.armada.scheduling.nodeUniformity", "armada-spark")
    sparkConf.set("spark.dynamicAllocation.initialExecutors", "2")
    sparkConf.set("spark.submit.deployMode", "cluster")
    sparkConf.set("spark.dynamicAllocation.enabled", "true")
    sparkConf.set("spark.dynamicAllocation.minExecutors", "0")

    // Should not throw executor count validation exception
    val exception = intercept[Throwable] {
      armadaClientApp.submitArmadaJob(null, clientArguments, armadaJobConfig, sparkConf)
    }
    Option(exception.getMessage).getOrElse(
      ""
    ) should not include "Executor count must be greater than 0"
  }

  test("JobTemplateLoader should handle malformed YAML gracefully") {
    val malformedYaml = "invalid: yaml: content: ]]]["
    val templateFile  = tempDir.resolve("malformed-template.yaml").toFile
    val writer        = new FileWriter(templateFile)
    try {
      writer.write(malformedYaml)
    } finally {
      writer.close()
    }

    val exception = intercept[RuntimeException] {
      JobTemplateLoader.loadJobTemplate(templateFile.getAbsolutePath)
    }
    exception.getMessage should include("Failed to parse template as YAML")
    exception.getMessage should include("malformed-template.yaml")
  }

  test("container image should follow precedence: CLI > template > error") {
    val templateWithImage = JobSubmitRequestItem(
      priority = TEMPLATE_PRIORITY,
      namespace = "template-namespace",
      annotations = Map.empty,
      labels = Map.empty,
      podSpec = Some(
        PodSpec()
          .withContainers(
            Seq(
              k8s.io.api.core.v1.generated
                .Container()
                .withName("template-container")
                .withImage("template-image:1.0")
            )
          )
      )
    )

    val cliConfig = armadaClientApp.CLIConfig(
      queue = Some("test-queue"),
      jobSetId = Some("test-job-set"),
      namespace = Some("test-namespace"),
      priority = Some(RUNTIME_PRIORITY),
      containerImage = Some("cli-image:2.0"),
      podLabels = Map.empty,
      driverLabels = Map.empty,
      executorLabels = Map.empty,
      armadaClusterUrl = Some("armada://localhost:50051"),
      nodeSelectors = Map.empty,
      nodeUniformityLabel = None,
      executorConnectionTimeout = Some(300.seconds),
      runAsUser = None,
      driverResources = armadaClientApp.ResourceConfig(None, None, None, None),
      executorResources = armadaClientApp.ResourceConfig(None, None, None, None)
    )

    val resolvedConfig = armadaClientApp.resolveJobConfig(
      cliConfig,
      Some(templateWithImage),
      Map.empty,
      Map.empty,
      sparkConf
    )

    resolvedConfig.containerImage shouldBe "cli-image:2.0"
    resolvedConfig.priority shouldBe RUNTIME_PRIORITY

    val cliConfigWithoutImage = cliConfig.copy(containerImage = None)
    val resolvedConfigWithTemplateImage = armadaClientApp.resolveJobConfig(
      cliConfigWithoutImage,
      Some(templateWithImage),
      Map.empty,
      Map.empty,
      sparkConf
    )

    resolvedConfigWithTemplateImage.containerImage shouldBe "template-image:1.0"
  }

  test(
    "validateRequiredConfig should require both templates to have container image when CLI image not provided"
  ) {
    val driverTemplateWithImage = JobSubmitRequestItem(
      priority = TEMPLATE_PRIORITY,
      namespace = "template-namespace",
      annotations = Map.empty,
      labels = Map.empty,
      podSpec = Some(
        PodSpec()
          .withContainers(
            Seq(
              k8s.io.api.core.v1.generated
                .Container()
                .withName("driver")
                .withImage("driver-template-image:1.0")
            )
          )
      )
    )

    val executorTemplateWithImage = JobSubmitRequestItem(
      priority = EXECUTOR_TEMPLATE_PRIORITY,
      namespace = "template-namespace",
      annotations = Map.empty,
      labels = Map.empty,
      podSpec = Some(
        PodSpec()
          .withContainers(
            Seq(
              k8s.io.api.core.v1.generated
                .Container()
                .withName("executor")
                .withImage("executor-template-image:1.0")
            )
          )
      )
    )

    val cliConfigWithoutImage = armadaClientApp.CLIConfig(
      queue = Some("test-queue"),
      jobSetId = Some("test-job-set"),
      namespace = Some("test-namespace"),
      priority = Some(RUNTIME_PRIORITY),
      containerImage = None,
      podLabels = Map.empty,
      driverLabels = Map.empty,
      executorLabels = Map.empty,
      armadaClusterUrl = Some("armada://localhost:50051"),
      nodeSelectors = Map.empty,
      nodeUniformityLabel = None,
      executorConnectionTimeout = Some(300.seconds),
      runAsUser = None,
      driverResources = armadaClientApp.ResourceConfig(None, None, None, None),
      executorResources = armadaClientApp.ResourceConfig(None, None, None, None)
    )

    // Should succeed when both templates have images
    armadaClientApp.validateRequiredConfig(
      cliConfigWithoutImage,
      None,
      Some(driverTemplateWithImage),
      Some(executorTemplateWithImage),
      sparkConf
    )

    // Should fail when only driver template has image
    val exception1 = intercept[IllegalArgumentException] {
      armadaClientApp.validateRequiredConfig(
        cliConfigWithoutImage,
        None,
        Some(driverTemplateWithImage),
        None,
        sparkConf
      )
    }
    exception1.getMessage should include("BOTH driver and executor")

    // Should fail when only executor template has image
    val exception2 = intercept[IllegalArgumentException] {
      armadaClientApp.validateRequiredConfig(
        cliConfigWithoutImage,
        None,
        None,
        Some(executorTemplateWithImage),
        sparkConf
      )
    }
    exception2.getMessage should include("BOTH driver and executor")
  }

  test("missing CLI values should follow precedence rules with templates") {
    val templateFile = createJobTemplateFile("template-queue", "template-job-set")
    sparkConf.set(Config.ARMADA_JOB_TEMPLATE.key, templateFile.getAbsolutePath)

    // Missing queue from CLI - should use template value
    sparkConf.remove(Config.ARMADA_JOB_QUEUE.key)
    sparkConf.set(Config.ARMADA_JOB_SET_ID.key, "cli-job-set")
    val configWithMissingQueue =
      armadaClientApp.validateArmadaJobConfig(sparkConf, Some(clientArguments))
    configWithMissingQueue.queue shouldBe "template-queue"
    configWithMissingQueue.jobSetId shouldBe "cli-job-set"

    // Missing jobSetId from CLI - should use template value
    sparkConf.set(Config.ARMADA_JOB_QUEUE.key, "cli-queue")
    sparkConf.remove(Config.ARMADA_JOB_SET_ID.key)
    val configWithMissingJobSetId =
      armadaClientApp.validateArmadaJobConfig(sparkConf, Some(clientArguments))
    configWithMissingJobSetId.queue shouldBe "cli-queue"
    configWithMissingJobSetId.jobSetId shouldBe "template-job-set"
  }

  test(
    "mergeExecutorTemplate should not introduce framework-level duplicates with feature steps"
  ) {
    // This test verifies that the framework doesn't duplicate resources when merging
    // templates with feature steps. User mistakes (same resource in both) are NOT our concern.
    // We only check that the framework doesn't add the same thing twice.

    val template: JobSubmitRequestItem = JobSubmitRequestItem(
      priority = TEMPLATE_PRIORITY,
      namespace = "template-namespace",
      annotations = Map("template-annotation" -> "template-value"),
      labels = Map("template-label" -> "template-value"),
      podSpec = Some(
        PodSpec()
          .withVolumes(
            Seq(
              Volume()
                .withName("template-volume")
                .withVolumeSource(
                  k8s.io.api.core.v1.generated.VolumeSource(
                    emptyDir = Some(k8s.io.api.core.v1.generated.EmptyDirVolumeSource())
                  )
                )
            )
          )
          .withContainers(
            Seq(
              Container()
                .withName("template-container")
                .withImage("executor-template-image:1.0")
                .withVolumeMounts(
                  Seq(
                    VolumeMount()
                      .withName("template-volume")
                      .withMountPath("/tmp/template-data")
                  )
                )
            )
          )
      )
    )

    // Feature step with DIFFERENT volumes/mounts (not duplicating template - that's user's fault)
    val featureStepJobItem = JobSubmitRequestItem(
      priority = 1.0,
      namespace = "default",
      podSpec = Some(
        PodSpec()
          .withVolumes(
            Seq(
              Volume()
                .withName("feature-step-volume")
                .withVolumeSource(
                  k8s.io.api.core.v1.generated.VolumeSource(
                    emptyDir = Some(k8s.io.api.core.v1.generated.EmptyDirVolumeSource())
                  )
                )
            )
          )
      )
    )

    val featureStepContainer = Container()
      .withName(EXECUTOR_CONTAINER_NAME)
      .withVolumeMounts(
        Seq(
          VolumeMount()
            .withName("feature-step-volume")
            .withMountPath("/tmp/feature-step-data")
        )
      )

    val cliConfig = armadaClientApp.CLIConfig(
      queue = Some("test-queue"),
      jobSetId = Some("test-job-set"),
      namespace = Some("runtime-namespace"),
      priority = Some(RUNTIME_PRIORITY),
      containerImage = Some(DEFAULT_IMAGE_NAME),
      podLabels = Map.empty,
      driverLabels = Map.empty,
      executorLabels = Map.empty,
      armadaClusterUrl = Some("armada://localhost:50051"),
      nodeSelectors = Map("node-type" -> "compute"),
      nodeUniformityLabel = Some("zone"),
      executorConnectionTimeout = Some(300.seconds),
      runAsUser = Some(RUNTIME_RUN_AS_USER),
      driverResources = armadaClientApp.ResourceConfig(
        limitCores = Some("1"),
        requestCores = Some("1"),
        limitMemory = Some("1Gi"),
        requestMemory = Some("1Gi")
      ),
      executorResources = armadaClientApp.ResourceConfig(
        limitCores = Some("1"),
        requestCores = Some("1"),
        limitMemory = Some("1Gi"),
        requestMemory = Some("1Gi")
      )
    )

    val armadaJobConfig = armadaClientApp.ArmadaJobConfig(
      queue = "test-queue",
      jobSetId = "test-job-set",
      jobTemplate = None,
      driverJobItemTemplate = None,
      executorJobItemTemplate = Some(template),
      cliConfig = cliConfig,
      applicationId = "armada-spark-app-id",
      driverFeatureStepJobItem = None,
      driverFeatureStepContainer = None,
      executorFeatureStepJobItem = Some(featureStepJobItem),
      executorFeatureStepContainer = Some(featureStepContainer),
      driverSystemProperties = Map.empty
    )

    val javaOptEnvVars = Seq(EnvVar().withName("SPARK_JAVA_OPT_0").withValue("-Xmx1g"))

    val resolvedConfig = armadaClientApp.ResolvedJobConfig(
      namespace = "runtime-namespace",
      priority = RUNTIME_PRIORITY,
      containerImage = DEFAULT_IMAGE_NAME,
      armadaClusterUrl = "armada://localhost:50051",
      executorConnectionTimeout = 300.seconds,
      runAsUser = RUNTIME_RUN_AS_USER,
      annotations = Map("runtime-annotation" -> "runtime-value"),
      labels = Map("runtime-label" -> "runtime-value"),
      nodeSelectors = Map("node-type" -> "compute"),
      driverResources =
        armadaClientApp.ResolvedResourceConfig(Some("1"), Some("1"), Some("1Gi"), Some("1Gi")),
      executorResources =
        armadaClientApp.ResolvedResourceConfig(Some("1"), Some("1"), Some("1Gi"), Some("1Gi"))
    )

    val result = armadaClientApp.mergeExecutorTemplate(
      Some(template),
      resolvedConfig,
      armadaJobConfig,
      javaOptEnvVars,
      "driver-service",
      7078,
      Seq.empty, // volumes
      sparkConf
    )

    val podSpec = result.podSpec.get

    // Verify volumes: should have both template-volume and feature-step-volume, no duplicates
    val volumeNames = podSpec.volumes.map(_.name).flatten
    volumeNames should contain("template-volume")
    volumeNames should contain("feature-step-volume")
    volumeNames.size shouldBe volumeNames.distinct.size // No duplicates

    // Verify volume mounts in the executor container
    val executorContainer =
      podSpec.containers.find(_.name.contains(EXECUTOR_CONTAINER_NAME)).get
    val volumeMountNames = executorContainer.volumeMounts.map(_.name).flatten
    volumeMountNames should contain("template-volume")
    volumeMountNames should contain("feature-step-volume")

    // CRITICAL: Verify no duplicates introduced by framework
    volumeMountNames.size shouldBe volumeMountNames.distinct.size

    // Additional verification: count each specific mount
    volumeMountNames.count(_ == "template-volume") shouldBe 1
    volumeMountNames.count(_ == "feature-step-volume") shouldBe 1
  }

  test("mergeDriverTemplate should not introduce framework-level duplicates with feature steps") {
    // Similar test for driver template merging

    val template: JobSubmitRequestItem = JobSubmitRequestItem(
      priority = TEMPLATE_PRIORITY,
      namespace = "template-namespace",
      annotations = Map("template-annotation" -> "template-value"),
      labels = Map("template-label" -> "template-value"),
      podSpec = Some(
        PodSpec()
          .withVolumes(
            Seq(
              Volume()
                .withName("template-volume")
                .withVolumeSource(
                  k8s.io.api.core.v1.generated.VolumeSource(
                    emptyDir = Some(k8s.io.api.core.v1.generated.EmptyDirVolumeSource())
                  )
                )
            )
          )
          .withContainers(
            Seq(
              Container()
                .withName("template-container")
                .withImage("driver-template-image:1.0")
                .withVolumeMounts(
                  Seq(
                    VolumeMount()
                      .withName("template-volume")
                      .withMountPath("/tmp/template-data")
                  )
                )
            )
          )
      )
    )

    // Feature step with DIFFERENT volumes/mounts
    val featureStepJobItem = JobSubmitRequestItem(
      priority = 1.0,
      namespace = "default",
      podSpec = Some(
        PodSpec()
          .withVolumes(
            Seq(
              Volume()
                .withName("feature-step-volume")
                .withVolumeSource(
                  k8s.io.api.core.v1.generated.VolumeSource(
                    emptyDir = Some(k8s.io.api.core.v1.generated.EmptyDirVolumeSource())
                  )
                )
            )
          )
      )
    )

    val featureStepContainer = Container()
      .withName(DRIVER_CONTAINER_NAME)
      .withVolumeMounts(
        Seq(
          VolumeMount()
            .withName("feature-step-volume")
            .withMountPath("/tmp/feature-step-data")
        )
      )

    val cliConfig = armadaClientApp.CLIConfig(
      queue = Some("test-queue"),
      jobSetId = Some("test-job-set"),
      namespace = Some("runtime-namespace"),
      priority = Some(RUNTIME_PRIORITY),
      containerImage = Some(DEFAULT_IMAGE_NAME),
      podLabels = Map.empty,
      driverLabels = Map.empty,
      executorLabels = Map.empty,
      armadaClusterUrl = Some("armada://localhost:50051"),
      nodeSelectors = Map("node-type" -> "compute"),
      nodeUniformityLabel = Some("zone"),
      executorConnectionTimeout = Some(300.seconds),
      runAsUser = Some(RUNTIME_RUN_AS_USER),
      driverResources = armadaClientApp.ResourceConfig(
        limitCores = Some("1"),
        requestCores = Some("1"),
        limitMemory = Some("1Gi"),
        requestMemory = Some("1Gi")
      ),
      executorResources = armadaClientApp.ResourceConfig(
        limitCores = Some("1"),
        requestCores = Some("1"),
        limitMemory = Some("1Gi"),
        requestMemory = Some("1Gi")
      )
    )

    val armadaJobConfig = armadaClientApp.ArmadaJobConfig(
      queue = "test-queue",
      jobSetId = "test-job-set",
      jobTemplate = None,
      driverJobItemTemplate = Some(template),
      executorJobItemTemplate = None,
      cliConfig = cliConfig,
      applicationId = "armada-spark-app-id",
      driverFeatureStepJobItem = Some(featureStepJobItem),
      driverFeatureStepContainer = Some(featureStepContainer),
      executorFeatureStepJobItem = None,
      executorFeatureStepContainer = None,
      driverSystemProperties = Map.empty
    )

    val javaOptEnvVars = Seq(EnvVar().withName("SPARK_JAVA_OPT_0").withValue("-Xmx1g"))

    val resolvedConfig = armadaClientApp.ResolvedJobConfig(
      namespace = "runtime-namespace",
      priority = RUNTIME_PRIORITY,
      containerImage = DEFAULT_IMAGE_NAME,
      armadaClusterUrl = "armada://localhost:50051",
      executorConnectionTimeout = 300.seconds,
      runAsUser = RUNTIME_RUN_AS_USER,
      annotations = Map("runtime-annotation" -> "runtime-value"),
      labels = Map("runtime-label" -> "runtime-value"),
      nodeSelectors = Map("node-type" -> "compute"),
      driverResources =
        armadaClientApp.ResolvedResourceConfig(Some("1"), Some("1"), Some("1Gi"), Some("1Gi")),
      executorResources =
        armadaClientApp.ResolvedResourceConfig(Some("1"), Some("1"), Some("1Gi"), Some("1Gi"))
    )

    val result = armadaClientApp.mergeDriverTemplate(
      Some(template),
      resolvedConfig,
      armadaJobConfig,
      7078, // driverPort
      clientArguments.mainClass,
      Seq.empty, // volumes
      Seq.empty, // volumeMounts
      Seq.empty, // additionalDriverArgs
      new SparkConf()
    )

    val podSpec = result.podSpec.get

    // Verify volumes: should have both template-volume and feature-step-volume, no duplicates
    val volumeNames = podSpec.volumes.map(_.name).flatten
    volumeNames should contain("template-volume")
    volumeNames should contain("feature-step-volume")
    volumeNames.size shouldBe volumeNames.distinct.size // No duplicates

    // Verify volume mounts in the driver container
    val driverContainer =
      podSpec.containers.find(_.name.contains(DRIVER_CONTAINER_NAME)).get
    val volumeMountNames = driverContainer.volumeMounts.map(_.name).flatten
    volumeMountNames should contain("template-volume")
    volumeMountNames should contain("feature-step-volume")

    // CRITICAL: Verify no duplicates introduced by framework
    volumeMountNames.size shouldBe volumeMountNames.distinct.size

    // Additional verification: count each specific mount
    volumeMountNames.count(_ == "template-volume") shouldBe 1
    volumeMountNames.count(_ == "feature-step-volume") shouldBe 1
  }

  test("PodMerger should preserve base fields when overriding pod doesn't set them") {
    // Test for field preservation during pod merging
    import io.fabric8.kubernetes.api.model.{Pod, PodBuilder}

    // Base pod has serviceAccount and hostname set
    val basePod = new PodBuilder()
      .withNewMetadata()
      .withName("base")
      .endMetadata()
      .withNewSpec()
      .withServiceAccount("base-service-account")
      .withHostname("base-hostname")
      .addNewVolume()
      .withName("base-volume")
      .withNewEmptyDir()
      .endEmptyDir()
      .endVolume()
      .endSpec()
      .build()

    // Overriding pod has NO serviceAccount or hostname but has volumes
    val overridingPod = new PodBuilder()
      .withNewMetadata()
      .withName("overriding")
      .endMetadata()
      .withNewSpec()
      .addNewVolume()
      .withName("template-volume")
      .withNewEmptyDir()
      .endEmptyDir()
      .endVolume()
      .endSpec()
      .build()

    val merged = PodMerger.mergePods(base = basePod, overriding = overridingPod)

    // Base fields should be preserved when not set in overriding
    merged.getSpec.getServiceAccount shouldBe "base-service-account"
    merged.getSpec.getHostname shouldBe "base-hostname"

    // Arrays should be strategically merged
    val volumeNames = merged.getSpec.getVolumes.asScala.map(_.getName).toSet
    volumeNames should contain("base-volume")
    volumeNames should contain("template-volume")
    volumeNames.size shouldBe 2
  }

  test("PodMerger should override base fields when overriding pod sets them") {
    import io.fabric8.kubernetes.api.model.{Pod, PodBuilder}

    val basePod = new PodBuilder()
      .withNewMetadata()
      .withName("base")
      .endMetadata()
      .withNewSpec()
      .withServiceAccount("base-service-account")
      .withHostname("base-hostname")
      .endSpec()
      .build()

    val overridingPod = new PodBuilder()
      .withNewMetadata()
      .withName("overriding")
      .endMetadata()
      .withNewSpec()
      .withServiceAccount("template-service-account")
      .withHostname("template-hostname")
      .endSpec()
      .build()

    val merged = PodMerger.mergePods(base = basePod, overriding = overridingPod)

    // Overriding values should win when explicitly set
    merged.getSpec.getServiceAccount shouldBe "template-service-account"
    merged.getSpec.getHostname shouldBe "template-hostname"
  }

  test("parseCLIConfig should set uiIngress when ingress enabled via SparkConf") {
    val ingressConf = new SparkConf()
      .set("spark.master", "armada://localhost:50051")
      .set("spark.app.name", "test-app")
      .set(Config.ARMADA_JOB_QUEUE.key, "test-queue")
      .set(Config.CONTAINER_IMAGE.key, DEFAULT_IMAGE_NAME)
      .set(Config.ARMADA_SPARK_DRIVER_UI_INGRESS_ENABLED.key, "true")
      .set(
        Config.ARMADA_SPARK_DRIVER_UI_INGRESS_ANNOTATIONS.key,
        "nginx.ingress.kubernetes.io/rewrite-target=/,nginx.ingress.kubernetes.io/backend-protocol=HTTP"
      )
      .set(Config.ARMADA_SPARK_DRIVER_UI_INGRESS_TLS_ENABLED.key, "false")

    val cliConfig = armadaClientApp.parseCLIConfig(ingressConf)

    cliConfig.uiIngress shouldBe defined
    val ingress = cliConfig.uiIngress.get
    ingress.annotations should contain("nginx.ingress.kubernetes.io/rewrite-target" -> "/")
    ingress.annotations should contain("nginx.ingress.kubernetes.io/backend-protocol" -> "HTTP")
    ingress.tls shouldBe Some(false)
  }

  test("resolveJobConfig should create uiIngress when CLI has ingress enabled") {
    val ingressConf = new SparkConf()
      .set("spark.master", "armada://localhost:50051")
      .set("spark.app.name", "test-app")
      .set(Config.ARMADA_JOB_QUEUE.key, "test-queue")
      .set(Config.CONTAINER_IMAGE.key, DEFAULT_IMAGE_NAME)
      .set(Config.ARMADA_SPARK_DRIVER_UI_INGRESS_ENABLED.key, "true")
      .set(
        Config.ARMADA_SPARK_DRIVER_UI_INGRESS_ANNOTATIONS.key,
        "nginx.ingress.kubernetes.io/rewrite-target=/"
      )

    val cliConfig = armadaClientApp.parseCLIConfig(ingressConf)

    val resolvedConfig = armadaClientApp.resolveJobConfig(
      cliConfig = cliConfig,
      template = None,
      annotations = Map.empty,
      labels = Map.empty,
      conf = ingressConf
    )

    resolvedConfig.uiIngress shouldBe defined
    val ingress = resolvedConfig.uiIngress.get
    ingress.ports shouldBe Seq(4040) // Default Spark UI port
    ingress.annotations should contain("nginx.ingress.kubernetes.io/rewrite-target" -> "/")
  }

  test("validateArmadaJobConfig should set uiIngress when ingress enabled") {
    val ingressConf = new SparkConf()
      .set("spark.master", "armada://localhost:50051")
      .set("spark.app.name", "test-app")
      .set(Config.ARMADA_JOB_QUEUE.key, "test-queue")
      .set(Config.CONTAINER_IMAGE.key, DEFAULT_IMAGE_NAME)
      .set(Config.ARMADA_SPARK_DRIVER_UI_INGRESS_ENABLED.key, "true")
      .set(
        Config.ARMADA_SPARK_DRIVER_UI_INGRESS_ANNOTATIONS.key,
        "nginx.ingress.kubernetes.io/rewrite-target=/"
      )
      .set(Config.ARMADA_SPARK_DRIVER_UI_INGRESS_TLS_ENABLED.key, "false")
      .set("spark.kubernetes.container.image", DEFAULT_IMAGE_NAME)

    val armadaJobConfig =
      armadaClientApp.validateArmadaJobConfig(ingressConf, Some(clientArguments))

    // Verify CLI config has ingress enabled
    armadaJobConfig.cliConfig.uiIngress shouldBe defined
    val cliIngress = armadaJobConfig.cliConfig.uiIngress.get
    cliIngress.annotations should contain("nginx.ingress.kubernetes.io/rewrite-target" -> "/")
    cliIngress.tls shouldBe Some(false)
  }

  test("resolveConnectIngressConfig builds connect ingress from conf") {
    sparkConf.set("spark.connect.grpc.binding.port", "15999")
    val cliIngress = armadaClientApp.IngressConfig(
      annotations = Map("nginx.ingress.kubernetes.io/backend-protocol" -> "GRPC"),
      tls = Some(true),
      certName = Some("wildcard-tls")
    )

    val result = armadaClientApp.resolveConnectIngressConfig(cliIngress, sparkConf)

    result.ports shouldBe Seq(15999)
    result.annotations should contain("nginx.ingress.kubernetes.io/backend-protocol" -> "GRPC")
    result.tlsEnabled shouldBe true
    result.certName shouldBe "wildcard-tls"
    result.useClusterIP shouldBe true
  }

  test("resolveConnectIngressConfig defaults port to 15002 and tls off") {
    val cliIngress = armadaClientApp.IngressConfig(Map.empty, None, None)

    val result = armadaClientApp.resolveConnectIngressConfig(cliIngress, sparkConf)

    result.ports shouldBe Seq(15002)
    result.tlsEnabled shouldBe false
    result.certName shouldBe ""
  }

  test("parseCLIConfig parses both ui and connect ingress groups") {
    sparkConf
      .set(Config.ARMADA_SPARK_DRIVER_UI_INGRESS_ENABLED.key, "true")
      .set(
        Config.ARMADA_SPARK_DRIVER_UI_INGRESS_ANNOTATIONS.key,
        "nginx.ingress.kubernetes.io/backend-protocol=HTTP"
      )
      .set(Config.ARMADA_SPARK_DRIVER_CONNECT_INGRESS_ENABLED.key, "true")
      .set(
        Config.ARMADA_SPARK_DRIVER_CONNECT_INGRESS_ANNOTATIONS.key,
        "nginx.ingress.kubernetes.io/backend-protocol=GRPC"
      )

    val cliConfig = armadaClientApp.parseCLIConfig(sparkConf)

    cliConfig.uiIngress shouldBe defined
    cliConfig.uiIngress.get.annotations should contain(
      "nginx.ingress.kubernetes.io/backend-protocol" -> "HTTP"
    )
    cliConfig.connectIngress shouldBe defined
    cliConfig.connectIngress.get.annotations should contain(
      "nginx.ingress.kubernetes.io/backend-protocol" -> "GRPC"
    )
  }

  test("parseCLIConfig leaves connectIngress empty when disabled") {
    sparkConf.set(Config.ARMADA_SPARK_DRIVER_UI_INGRESS_ENABLED.key, "true")

    val cliConfig = armadaClientApp.parseCLIConfig(sparkConf)

    cliConfig.uiIngress shouldBe defined
    cliConfig.connectIngress shouldBe None
  }

  test("buildDriverContainerPorts declares only driver port by default") {
    val ports = armadaClientApp.buildDriverContainerPorts(7078, sparkConf)

    ports.map(_.name) shouldBe Seq(Some("driver"))
    ports.head.containerPort shouldBe Some(7078)
  }

  test("buildDriverContainerPorts declares ui port when ui ingress on and oauth off") {
    sparkConf.set(Config.ARMADA_SPARK_DRIVER_UI_INGRESS_ENABLED.key, "true")

    val ports = armadaClientApp.buildDriverContainerPorts(7078, sparkConf)

    ports.map(_.name.get) shouldBe Seq("driver", "ui")
    ports(1).containerPort shouldBe Some(4040)
  }

  test("buildDriverContainerPorts skips ui port when oauth enabled") {
    sparkConf
      .set(Config.ARMADA_SPARK_DRIVER_UI_INGRESS_ENABLED.key, "true")
      .set(Config.ARMADA_OAUTH_ENABLED.key, "true")

    val ports = armadaClientApp.buildDriverContainerPorts(7078, sparkConf)

    ports.map(_.name.get) shouldBe Seq("driver")
  }

  test("buildDriverContainerPorts declares connect port when connect ingress enabled") {
    sparkConf.set(Config.ARMADA_SPARK_DRIVER_CONNECT_INGRESS_ENABLED.key, "true")

    val ports = armadaClientApp.buildDriverContainerPorts(7078, sparkConf)

    ports.map(_.name.get) should contain("connect")
    ports.find(_.name.contains("connect")).get.containerPort shouldBe Some(15002)
  }

  test("buildDriverContainerPorts declares connect port even when oauth enabled") {
    sparkConf
      .set(Config.ARMADA_SPARK_DRIVER_UI_INGRESS_ENABLED.key, "true")
      .set(Config.ARMADA_OAUTH_ENABLED.key, "true")
      .set(Config.ARMADA_SPARK_DRIVER_CONNECT_INGRESS_ENABLED.key, "true")

    val ports = armadaClientApp.buildDriverContainerPorts(7078, sparkConf)

    ports.map(_.name.get) shouldBe Seq("driver", "connect")
    ports(1).containerPort shouldBe Some(15002)
  }

  test("buildServiceConfig includes connect port only when connect ingress enabled") {
    val withoutConnect = armadaClientApp.buildServiceConfig(7078, sparkConf)
    withoutConnect.head.ports shouldBe Seq(7078, 4040)

    sparkConf.set(Config.ARMADA_SPARK_DRIVER_CONNECT_INGRESS_ENABLED.key, "true")
    val withConnect = armadaClientApp.buildServiceConfig(7078, sparkConf)
    withConnect.head.ports shouldBe Seq(7078, 4040, 15002)
  }

  test("mergeDriverTemplate emits ui and connect ingresses") {
    sparkConf
      .set(Config.ARMADA_SPARK_DRIVER_UI_INGRESS_ENABLED.key, "true")
      .set(
        Config.ARMADA_SPARK_DRIVER_UI_INGRESS_ANNOTATIONS.key,
        "nginx.ingress.kubernetes.io/backend-protocol=HTTP"
      )
      .set(Config.ARMADA_SPARK_DRIVER_CONNECT_INGRESS_ENABLED.key, "true")
      .set(
        Config.ARMADA_SPARK_DRIVER_CONNECT_INGRESS_ANNOTATIONS.key,
        "nginx.ingress.kubernetes.io/backend-protocol=GRPC"
      )

    val cliConfig = armadaClientApp.parseCLIConfig(sparkConf)
    val resolvedConfig = armadaClientApp.resolveJobConfig(
      cliConfig = cliConfig,
      template = None,
      annotations = Map.empty,
      labels = Map.empty,
      conf = sparkConf
    )

    val armadaJobConfig = armadaClientApp.ArmadaJobConfig(
      queue = "test-queue",
      jobSetId = "test-job-set",
      jobTemplate = None,
      driverJobItemTemplate = None,
      executorJobItemTemplate = None,
      cliConfig = cliConfig,
      applicationId = "armada-spark-app-id",
      driverFeatureStepJobItem = None,
      driverFeatureStepContainer = None,
      executorFeatureStepJobItem = None,
      executorFeatureStepContainer = None,
      driverSystemProperties = Map.empty
    )

    val result = armadaClientApp.mergeDriverTemplate(
      template = None,
      resolvedConfig = resolvedConfig,
      armadaJobConfig = armadaJobConfig,
      driverPort = 7078,
      mainClass = "org.example.SparkApp",
      volumes = Seq.empty,
      volumeMounts = Seq.empty,
      additionalDriverArgs = Seq.empty,
      conf = sparkConf
    )

    result.ingress should have size 2
    val uiIngress = result.ingress.head
    uiIngress.ports shouldBe Seq(4040)
    uiIngress.annotations shouldBe Map("nginx.ingress.kubernetes.io/backend-protocol" -> "HTTP")
    val connectIngress = result.ingress(1)
    connectIngress.ports shouldBe Seq(15002)
    connectIngress.annotations shouldBe Map(
      "nginx.ingress.kubernetes.io/backend-protocol" -> "GRPC"
    )
  }

  // --- Priority class tests ---

  private def minimalCLIConfig() = armadaClientApp.CLIConfig(
    queue = Some("test-queue"),
    jobSetId = Some("test-job-set"),
    namespace = Some("test-namespace"),
    priority = Some(RUNTIME_PRIORITY),
    containerImage = Some(DEFAULT_IMAGE_NAME),
    podLabels = Map.empty,
    driverLabels = Map.empty,
    executorLabels = Map.empty,
    armadaClusterUrl = Some("armada://localhost:50051"),
    nodeSelectors = Map.empty,
    nodeUniformityLabel = None,
    executorConnectionTimeout = Some(300.seconds),
    runAsUser = Some(RUNTIME_RUN_AS_USER),
    driverResources =
      armadaClientApp.ResourceConfig(Some("1"), Some("1"), Some("1Gi"), Some("1Gi")),
    executorResources =
      armadaClientApp.ResourceConfig(Some("1"), Some("1"), Some("1Gi"), Some("1Gi"))
  )

  private def minimalResolvedConfig = armadaClientApp.ResolvedJobConfig(
    namespace = "test-namespace",
    priority = RUNTIME_PRIORITY,
    containerImage = DEFAULT_IMAGE_NAME,
    armadaClusterUrl = "armada://localhost:50051",
    executorConnectionTimeout = 300.seconds,
    runAsUser = RUNTIME_RUN_AS_USER,
    annotations = Map.empty,
    labels = Map.empty,
    nodeSelectors = Map.empty,
    driverResources =
      armadaClientApp.ResolvedResourceConfig(Some("1"), Some("1"), Some("1Gi"), Some("1Gi")),
    executorResources =
      armadaClientApp.ResolvedResourceConfig(Some("1"), Some("1"), Some("1Gi"), Some("1Gi"))
  )

  private def minimalArmadaJobConfig(conf: SparkConf) = armadaClientApp.ArmadaJobConfig(
    queue = "test-queue",
    jobSetId = "test-job-set",
    jobTemplate = None,
    driverJobItemTemplate = None,
    executorJobItemTemplate = None,
    cliConfig = minimalCLIConfig(),
    applicationId = "armada-spark-app-id",
    driverFeatureStepJobItem = None,
    driverFeatureStepContainer = None,
    executorFeatureStepJobItem = None,
    executorFeatureStepContainer = None,
    driverSystemProperties = Map.empty
  )

  test("mergeDriverTemplate sets initialPriorityClass on driver pod") {
    sparkConf.set(Config.ARMADA_SCHEDULING_INITIAL_PRIORITY_CLASS.key, "gang-priority")

    val result = armadaClientApp.mergeDriverTemplate(
      None,
      minimalResolvedConfig,
      minimalArmadaJobConfig(sparkConf),
      7078,
      "org.example.TestClass",
      Seq.empty[Volume],
      Seq.empty[VolumeMount],
      Seq.empty[String],
      sparkConf
    )

    result.podSpec.get.priorityClassName shouldBe Some("gang-priority")
  }

  test("mergeExecutorTemplate sets initialPriorityClass for initial gang executors") {
    // Static mode: gangCardinality is always > 0
    sparkConf.set(Config.ARMADA_SCHEDULING_INITIAL_PRIORITY_CLASS.key, "gang-priority")

    val result = armadaClientApp.mergeExecutorTemplate(
      None,
      minimalResolvedConfig,
      minimalArmadaJobConfig(sparkConf),
      Seq.empty[EnvVar],
      "driver-service",
      7078,
      Seq.empty[Volume],
      sparkConf
    )

    result.podSpec.get.priorityClassName shouldBe Some("gang-priority")
  }

  test("mergeExecutorTemplate sets scaleUpPriorityClass for scale-up executors") {
    // Dynamic mode after gang attributes captured: gangCardinality returns 0
    sparkConf.set(Config.ARMADA_SCHEDULING_SCALE_UP_PRIORITY_CLASS.key, "non-gang-priority")
    sparkConf.set("spark.dynamicAllocation.enabled", "true")
    sparkConf.set("spark.dynamicAllocation.initialExecutors", "2")
    sparkConf.set(Config.ARMADA_JOB_GANG_SCHEDULING_NODE_UNIFORMITY.key, "zone")
    // Simulate post-capture: internal labels set so getGangCardinality returns 0
    sparkConf.set(Config.ARMADA_INTERNAL_GANG_NODE_LABEL_NAME.key, "zone")
    sparkConf.set(Config.ARMADA_INTERNAL_GANG_NODE_LABEL_VALUE.key, "cluster-1")

    val result = armadaClientApp.mergeExecutorTemplate(
      None,
      minimalResolvedConfig,
      minimalArmadaJobConfig(sparkConf),
      Seq.empty[EnvVar],
      "driver-service",
      7078,
      Seq.empty[Volume],
      sparkConf
    )

    result.podSpec.get.priorityClassName shouldBe Some("non-gang-priority")
  }

  test("mergeExecutorTemplate does not set priorityClassName when configs not set") {
    val result = armadaClientApp.mergeExecutorTemplate(
      None,
      minimalResolvedConfig,
      minimalArmadaJobConfig(sparkConf),
      Seq.empty[EnvVar],
      "driver-service",
      7078,
      Seq.empty[Volume],
      sparkConf
    )

    result.podSpec.get.priorityClassName shouldBe None
  }

  test("mergeDriverTemplate preserves template priorityClassName when no config override") {
    val template = JobSubmitRequestItem(
      podSpec = Some(PodSpec().withPriorityClassName("template-priority"))
    )

    val result = armadaClientApp.mergeDriverTemplate(
      Some(template),
      minimalResolvedConfig,
      minimalArmadaJobConfig(sparkConf),
      7078,
      "org.example.TestClass",
      Seq.empty[Volume],
      Seq.empty[VolumeMount],
      Seq.empty[String],
      sparkConf
    )

    result.podSpec.get.priorityClassName shouldBe Some("template-priority")
  }

  test("mergeDriverTemplate overrides template priorityClassName with initialPriorityClass") {
    sparkConf.set(Config.ARMADA_SCHEDULING_INITIAL_PRIORITY_CLASS.key, "config-priority")

    val template = JobSubmitRequestItem(
      podSpec = Some(PodSpec().withPriorityClassName("template-priority"))
    )

    val result = armadaClientApp.mergeDriverTemplate(
      Some(template),
      minimalResolvedConfig,
      minimalArmadaJobConfig(sparkConf),
      7078,
      "org.example.TestClass",
      Seq.empty[Volume],
      Seq.empty[VolumeMount],
      Seq.empty[String],
      sparkConf
    )

    result.podSpec.get.priorityClassName shouldBe Some("config-priority")
  }

  test("mergeExecutorTemplate preserves template priorityClassName when no config override") {
    val template = JobSubmitRequestItem(
      podSpec = Some(PodSpec().withPriorityClassName("template-priority"))
    )

    val result = armadaClientApp.mergeExecutorTemplate(
      Some(template),
      minimalResolvedConfig,
      minimalArmadaJobConfig(sparkConf),
      Seq.empty[EnvVar],
      "driver-service",
      7078,
      Seq.empty[Volume],
      sparkConf
    )

    result.podSpec.get.priorityClassName shouldBe Some("template-priority")
  }

  test(
    "mergeExecutorTemplate overrides template priorityClassName with initialPriorityClass"
  ) {
    sparkConf.set(Config.ARMADA_SCHEDULING_INITIAL_PRIORITY_CLASS.key, "config-priority")

    val template = JobSubmitRequestItem(
      podSpec = Some(PodSpec().withPriorityClassName("template-priority"))
    )

    val result = armadaClientApp.mergeExecutorTemplate(
      Some(template),
      minimalResolvedConfig,
      minimalArmadaJobConfig(sparkConf),
      Seq.empty[EnvVar],
      "driver-service",
      7078,
      Seq.empty[Volume],
      sparkConf
    )

    result.podSpec.get.priorityClassName shouldBe Some("config-priority")
  }

  // -- processDriverEvent tests --

  private val DRIVER_JOB_ID   = "driver-job-123"
  private val EXECUTOR_JOB_ID = "executor-job-456"

  test("processDriverEvent returns Succeeded for matching driver job") {
    val event = EventMessage(
      events = EventMessage.Events.Succeeded(
        JobSucceededEvent(jobId = DRIVER_JOB_ID)
      )
    )
    val result = armadaClientApp.processDriverEvent(event, DRIVER_JOB_ID)
    result shouldBe Some(DriverTerminalState.Succeeded)
  }

  test("processDriverEvent returns Failed with reason for matching driver job") {
    val event = EventMessage(
      events = EventMessage.Events.Failed(
        JobFailedEvent(jobId = DRIVER_JOB_ID, reason = "OOMKilled")
      )
    )
    val result = armadaClientApp.processDriverEvent(event, DRIVER_JOB_ID)
    result shouldBe Some(DriverTerminalState.Failed("OOMKilled"))
  }

  test("processDriverEvent returns Failed with default reason when empty") {
    val event = EventMessage(
      events = EventMessage.Events.Failed(
        JobFailedEvent(jobId = DRIVER_JOB_ID, reason = "")
      )
    )
    val result = armadaClientApp.processDriverEvent(event, DRIVER_JOB_ID)
    result shouldBe Some(DriverTerminalState.Failed("Unknown failure"))
  }

  test("processDriverEvent returns Cancelled for matching driver job") {
    val event = EventMessage(
      events = EventMessage.Events.Cancelled(
        JobCancelledEvent(jobId = DRIVER_JOB_ID)
      )
    )
    val result = armadaClientApp.processDriverEvent(event, DRIVER_JOB_ID)
    result shouldBe Some(DriverTerminalState.Cancelled("Job cancelled"))
  }

  test("processDriverEvent returns Preempted for matching driver job") {
    val event = EventMessage(
      events = EventMessage.Events.Preempted(
        JobPreemptedEvent(jobId = DRIVER_JOB_ID)
      )
    )
    val result = armadaClientApp.processDriverEvent(event, DRIVER_JOB_ID)
    result shouldBe Some(DriverTerminalState.Preempted)
  }

  test("processDriverEvent returns None for non-driver succeeded event") {
    val event = EventMessage(
      events = EventMessage.Events.Succeeded(
        JobSucceededEvent(jobId = EXECUTOR_JOB_ID)
      )
    )
    val result = armadaClientApp.processDriverEvent(event, DRIVER_JOB_ID)
    result shouldBe None
  }

  test("processDriverEvent returns None for non-driver failed event") {
    val event = EventMessage(
      events = EventMessage.Events.Failed(
        JobFailedEvent(jobId = EXECUTOR_JOB_ID, reason = "OOMKilled")
      )
    )
    val result = armadaClientApp.processDriverEvent(event, DRIVER_JOB_ID)
    result shouldBe None
  }

  test("processDriverEvent returns None for driver progress events") {
    val progressEvents = Seq(
      EventMessage(
        events = EventMessage.Events.Submitted(JobSubmittedEvent(jobId = DRIVER_JOB_ID))
      ),
      EventMessage(
        events = EventMessage.Events.Queued(JobQueuedEvent(jobId = DRIVER_JOB_ID))
      ),
      EventMessage(
        events = EventMessage.Events.Pending(JobPendingEvent(jobId = DRIVER_JOB_ID))
      ),
      EventMessage(
        events = EventMessage.Events.Running(JobRunningEvent(jobId = DRIVER_JOB_ID))
      )
    )
    progressEvents.foreach { event =>
      val result = armadaClientApp.processDriverEvent(event, DRIVER_JOB_ID)
      result shouldBe None
    }
  }

  test("stripHadoopConfVolume removes hadoop-properties volume") {
    import io.fabric8.kubernetes.api.model.{PodBuilder, VolumeBuilder}

    val pod = new PodBuilder()
      .withNewMetadata()
      .withName("test-pod")
      .endMetadata()
      .withNewSpec()
      .withVolumes(
        new VolumeBuilder().withName("hadoop-properties").withNewEmptyDir().endEmptyDir().build(),
        new VolumeBuilder().withName("spark-conf").withNewEmptyDir().endEmptyDir().build(),
        new VolumeBuilder().withName("data-vol").withNewEmptyDir().endEmptyDir().build()
      )
      .endSpec()
      .build()

    val result      = armadaClientApp.stripHadoopConfVolume(pod)
    val volumeNames = result.getSpec.getVolumes.asScala.map(_.getName)
    volumeNames should contain("spark-conf")
    volumeNames should contain("data-vol")
    volumeNames should not contain "hadoop-properties"
    volumeNames should have size 2
  }

  test("stripHadoopConfMount removes hadoop-properties mount and HADOOP_CONF_DIR env") {
    import io.fabric8.kubernetes.api.model.{ContainerBuilder, EnvVarBuilder, VolumeMountBuilder}

    val container = new ContainerBuilder()
      .withName("driver")
      .withVolumeMounts(
        new VolumeMountBuilder()
          .withName("hadoop-properties")
          .withMountPath("/etc/hadoop")
          .build(),
        new VolumeMountBuilder()
          .withName("spark-conf")
          .withMountPath("/opt/spark/conf")
          .build()
      )
      .withEnv(
        new EnvVarBuilder().withName("HADOOP_CONF_DIR").withValue("/etc/hadoop").build(),
        new EnvVarBuilder().withName("SPARK_HOME").withValue("/opt/spark").build()
      )
      .build()

    val result     = armadaClientApp.stripHadoopConfMount(container)
    val mountNames = result.getVolumeMounts.asScala.map(_.getName)
    mountNames should contain("spark-conf")
    mountNames should not contain "hadoop-properties"
    mountNames should have size 1

    val envNames = result.getEnv.asScala.map(_.getName)
    envNames should contain("SPARK_HOME")
    envNames should not contain "HADOOP_CONF_DIR"
    envNames should have size 1
  }

}
