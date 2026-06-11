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

package org.apache.spark.deploy.armada.e2e

import org.apache.spark.deploy.armada.e2e.K8sClient

import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatest.concurrent.{Eventually, ScalaFutures}
import org.scalatest.time.{Seconds, Span}

import java.io.File
import java.util.Properties
import scala.concurrent.ExecutionContext.Implicits.global
import TestConstants._

class ArmadaSparkE2E
    extends AnyFunSuite
    with BeforeAndAfterAll
    with Matchers
    with ScalaFutures
    with Eventually {

  implicit override val patienceConfig: PatienceConfig = PatienceConfig(
    timeout = Span(300, Seconds),
    interval = Span(2, Seconds)
  )

  private val baseQueueName = "e2e-template"

  private var armadaClient: ArmadaClient      = _
  private var k8sClient: K8sClient            = _
  implicit private var orch: TestOrchestrator = _

  private var baseConfig: TestConfig = _

  private val templateServer =
    new TemplateFileServer(new File("src/test/resources/e2e/templates"))

  override def beforeAll(): Unit = {
    super.beforeAll()

    val port = templateServer.start()
    println(s"[TEMPLATE-SERVER] Started on port $port")

    val props = loadProperties()

    val armadaApiUrl = props.getProperty("armada.master", "armada://localhost:30002")
    armadaClient = new ArmadaClient(armadaApiUrl)
    k8sClient = new K8sClient(props)
    orch = new TestOrchestrator(armadaClient, k8sClient)

    // Get Scala binary version - either from system property or derive from full version
    // This should be "2.12" or "2.13", not the full version like "2.12.15"
    val scalaBinaryVersion = props.getProperty("scala.binary.version") match {
      case null | "" =>
        val fullVersion = props.getProperty("scala.version", "2.13.8")
        fullVersion.split('.').take(2).mkString(".")
      case binaryVersion => binaryVersion
    }

    val finalSparkVersion = props.getProperty("spark.version", "3.5.5")

    baseConfig = TestConfig(
      baseQueueName = props.getProperty("armada.queue", baseQueueName),
      imageName = props.getProperty("container.image", "spark:armada"),
      masterUrl = props.getProperty("armada.master", "armada://localhost:30002"),
      lookoutUrl = props.getProperty("armada.lookout.url", "http://localhost:30000"),
      scalaVersion = scalaBinaryVersion,
      sparkVersion = finalSparkVersion,
      sparkConfs = Map(
        "spark.armada.driver.labels"   -> "spark-role=driver",
        "spark.armada.executor.labels" -> "spark-role=executor"
      )
    )

    println(s"Test configuration loaded: $baseConfig")

    // Verify Armada cluster is ready before running tests
    val clusterReadyTimeout = ClusterReadyTimeout.toSeconds.toInt
    val testQueueName = s"${baseConfig.baseQueueName}-cluster-check-${System.currentTimeMillis()}"

    println(s"[CLUSTER-CHECK] Verifying Armada cluster readiness...")
    println(s"[CLUSTER-CHECK] Will retry for up to $clusterReadyTimeout seconds")

    val startTime                    = System.currentTimeMillis()
    var clusterReady                 = false
    var lastError: Option[Throwable] = None
    var attempts                     = 0

    while (!clusterReady && (System.currentTimeMillis() - startTime) < clusterReadyTimeout * 1000) {
      attempts += 1
      println(
        s"[CLUSTER-CHECK] Attempt #$attempts - Creating and verifying test queue: $testQueueName"
      )

      try {
        armadaClient.ensureQueueExists(testQueueName).futureValue
        println(s"[CLUSTER-CHECK] Queue creation and verification succeeded - cluster is ready!")
        clusterReady = true

        try {
          armadaClient.deleteQueue(testQueueName).futureValue
          println(s"[CLUSTER-CHECK] Test queue cleaned up")
        } catch {
          case _: Exception => // Ignore cleanup failures
        }
      } catch {
        case ex: Exception =>
          lastError = Some(ex)
          val elapsed = (System.currentTimeMillis() - startTime) / 1000
          println(s"[CLUSTER-CHECK] Attempt #$attempts failed after ${elapsed}s: ${ex.getMessage}")

          if ((System.currentTimeMillis() - startTime) < clusterReadyTimeout * 1000) {
            println(
              s"[CLUSTER-CHECK] Waiting ${ClusterCheckRetryDelay.toSeconds} seconds before retry..."
            )
            Thread.sleep(ClusterCheckRetryDelay.toMillis)
          }
      }
    }

    val totalTime = (System.currentTimeMillis() - startTime) / 1000
    if (!clusterReady) {
      throw new RuntimeException(
        s"Armada cluster not ready after $totalTime seconds ($attempts attempts). " +
          s"Last error: ${lastError.map(_.getMessage).getOrElse("Unknown")}"
      )
    }

    println(
      s"[CLUSTER-CHECK] Cluster verified ready after $totalTime seconds ($attempts attempts)"
    )
  }

  override def afterAll(): Unit = {
    templateServer.stop()
    println("[TEMPLATE-SERVER] Stopped")
    super.afterAll()
  }

  private def loadProperties(): Properties = {
    val props = new Properties()

    // Check system properties first, then environment variables
    def getPropertyOrEnv(propName: String, envName: String): Option[String] = {
      sys.props.get(propName).orElse(sys.env.get(envName))
    }

    // Helper to set property if it exists in either system properties or environment variables
    def set(property: String, envvar: String): Unit = {
      getPropertyOrEnv(property, envvar).foreach(
        props.setProperty(property, _)
      )
    }

    // Set properties using the same precedence as the scripts
    set("container.image", "IMAGE_NAME")
    set("armada.master", "ARMADA_MASTER")
    set("armada.lookout.url", "ARMADA_LOOKOUT_URL")
    set("scala.version", "SCALA_VERSION")
    set("spark.version", "SPARK_VERSION")
    set("armada.queue", "ARMADA_QUEUE")

    // Also check for binary versions
    set("scala.binary.version", "SCALA_BIN_VERSION")
    set("spark.binary.version", "SPARK_BIN_VERSION")

    // Optionally-set TLS certs for using remote Armada cluster
    set("client_cert_file", "CLIENT_CERT_FILE")
    set("client_key_file", "CLIENT_KEY_FILE")
    set("cluster_ca_file", "CLUSTER_CA_FILE")

    props
  }

  // ========================================================================
  // Base helper method
  // ========================================================================

  /** Base builder for basic SparkPi job tests */
  private def baseSparkPiTest(
      testName: String,
      deployMode: String,
      allocation: String,
      labels: Map[String, String] = Map.empty
  ): E2ETestBuilder = {
    val test = E2ETestBuilder(testName)
      .withBaseConfig(baseConfig)
      .withDeployMode(deployMode)
      .withPodLabels(labels)

    val testWithModeSpecificAsserts = if (deployMode == "cluster") {
      test
        .assertDriverExists()
        .assertPodLabels(labels)
    } else {
      test
        .assertExecutorsHaveLabels(labels)
    }

    if (allocation == "dynamic") {
      // Dynamic allocation uses 500 slices (vs 100 for static) to generate enough
      // work to trigger executor scale-up. The higher workload causes OOMKilled with
      // the default 510Mi memory, so we increase the container memory to 1Gi.
      // spark.executor.memory (JVM heap) is set to 768m because Spark adds
      // memoryOverhead (default min 384MiB) on top, and 768m + overhead fits in 1Gi.
      testWithModeSpecificAsserts
        .withAppArgs("500")
        .withSparkConf(
          Map(
            "spark.executor.instances"             -> "1",
            "spark.armada.executor.limit.memory"   -> "1Gi",
            "spark.armada.executor.request.memory" -> "1Gi",
            "spark.executor.memory"                -> "768m"
          )
        )
    } else {
      testWithModeSpecificAsserts
    }
  }

  // ========================================================================
  // Gang Scheduling Tests
  // ========================================================================

  private def baseSparkPiGangTest(
      deployMode: String,
      allocation: String,
      executorCount: Int
  ): E2ETestBuilder = {
    val suffix = s"$allocation-$deployMode"
    val base = baseSparkPiTest(
      s"basic-spark-pi-gang-$suffix",
      deployMode,
      allocation,
      Map("test-type" -> "basic-gang")
    )
      .withGangJob("armada-spark")

    base.withAllocationMode(allocation, executorCount)
  }

  test("Basic SparkPi job with gang scheduling - staticCluster", E2ETest) {
    baseSparkPiGangTest("cluster", "static", 3)
      .assertGangJob("armada-spark", 4) // 1 driver + 3 executors
      .run()
  }

  test("Basic SparkPi job with gang scheduling - staticClient", E2ETest) {
    baseSparkPiGangTest("client", "static", 3)
      .assertExecutorGangJob("armada-spark", 3) // Only 3 executors, no driver
      .run()
  }

  test("Basic SparkPi job with gang scheduling - dynamicCluster", E2ETest) {
    baseSparkPiGangTest("cluster", "dynamic", 2)
      .assertGangJobForDynamic("armada-spark", initialGangPods = 2)
      .assertExecutorCountMaxReachedAtLeast(
        3
      ) // at least 3 executor pods (2 min + 1 scaled) with gang annotations seen
      .run()
  }

  test("Basic SparkPi job with gang scheduling - dynamicClient", E2ETest) {
    baseSparkPiGangTest("client", "dynamic", 2)
      .assertGangJobForDynamic("armada-spark", initialGangPods = 2)
      .assertExecutorCountMaxReachedAtLeast(
        3
      ) // at least 3 executor pods (2 min + 1 scaled) with gang annotations seen
      .run()
  }

  // ========================================================================
  // Node Selector Tests
  // ========================================================================

  private def baseNodeSelectorTest(
      deployMode: String,
      allocation: String,
      executorCount: Int
  ): E2ETestBuilder = {
    val suffix = s"$allocation-$deployMode"
    // Pin to armada-worker2 because nodeUniformity=armada-spark is set globally
    // for dynamic allocation, and only armada-worker2 has the armada-spark label
    // in our kind cluster (added via e2e/armada-operator.patch).
    val base = baseSparkPiTest(
      s"spark-pi-node-selectors-$suffix",
      deployMode,
      allocation,
      Map("test-type" -> "node-selector")
    )
      .withNodeSelectors(Map("kubernetes.io/hostname" -> "armada-worker2"))
      .assertNodeSelectors(Map("kubernetes.io/hostname" -> "armada-worker2"))

    val configured = base.withAllocationMode(allocation, executorCount)
    if (allocation == "dynamic")
      configured.assertExecutorCountMaxReachedAtLeast(executorCount + 1)
    else configured
  }

  test("SparkPi job with node selectors - staticCluster", E2ETest) {
    baseNodeSelectorTest("cluster", "static", 2)
      .run()
  }

  test("SparkPi job with node selectors - staticClient", E2ETest) {
    baseNodeSelectorTest("client", "static", 2)
      .run()
  }

  test("SparkPi job with node selectors - dynamicCluster", E2ETest) {
    baseNodeSelectorTest("cluster", "dynamic", 2)
      .run()
  }

  // ========================================================================
  // Python Tests
  // ========================================================================

  private def basePythonSparkPiTest(
      deployMode: String,
      allocation: String,
      executorCount: Int
  ): E2ETestBuilder = {
    val suffix = s"$allocation-$deployMode"
    val base = baseSparkPiTest(
      s"python-spark-pi-$suffix",
      deployMode,
      allocation,
      Map("test-type" -> "python")
    )
      .withPythonScript("/opt/spark/examples/src/main/python/pi.py")
      .withSparkConf(
        Map(
          "spark.kubernetes.executor.disableConfigMap" -> "true"
        )
      )

    val configured = base.withAllocationMode(allocation, executorCount)
    if (allocation == "dynamic")
      configured.assertExecutorCountMaxReachedAtLeast(executorCount + 1)
    else configured
  }

  test("Basic python SparkPi job - staticCluster", E2ETest) {
    basePythonSparkPiTest("cluster", "static", 2)
      .run()
  }

  test("Basic python SparkPi job - staticClient", E2ETest) {
    basePythonSparkPiTest("client", "static", 2)
      .run()
  }

  test("Basic python SparkPi job - dynamicCluster", E2ETest) {
    basePythonSparkPiTest("cluster", "dynamic", 2)
      .run()
  }

  // ========================================================================
  // Template Tests
  // ========================================================================

  private val templateLabels = Map(
    "app"             -> "spark-pi",
    "component"       -> "executor",
    "template-source" -> "e2e-test"
  )

  private val templateAnnotations = Map(
    "armada/component" -> "spark-executor",
    "armada/template"  -> "spark-pi-executor"
  )

  private def baseTemplateTest(
      deployMode: String,
      allocation: String,
      executorCount: Int
  ): E2ETestBuilder = {
    val suffix = s"$allocation-$deployMode"
    val base =
      baseSparkPiTest(
        s"spark-pi-templates-$suffix",
        deployMode,
        allocation,
        Map("test-type" -> "template")
      )
        .withJobTemplate(templateServer.url("spark-pi-job-template.yaml"))
        .withSparkConf(
          Map(
            "spark.armada.driver.jobItemTemplate" -> templateServer.url(
              "spark-pi-driver-template.yaml"
            ),
            "spark.armada.executor.jobItemTemplate" -> templateServer.url(
              "spark-pi-executor-template.yaml"
            )
          )
        )

    val builder = if (allocation == "dynamic") {
      base
        .withStandardDynamicAllocation(executorCount)
        .assertDynamicExecutorsHaveLabels(templateLabels, executorCount + 1)
        .assertDynamicExecutorsHaveAnnotations(templateAnnotations, executorCount + 1)
    } else {
      base
        .withExecutors(executorCount)
        .assertExecutorCount(executorCount)
        .assertExecutorsHaveLabels(templateLabels)
        .assertExecutorsHaveAnnotations(templateAnnotations)
    }

    if (deployMode == "cluster") {
      builder
        .assertDriverHasLabels(
          Map(
            "app"             -> "spark-pi",
            "component"       -> "driver",
            "template-source" -> "e2e-test"
          )
        )
        .assertDriverHasAnnotations(
          Map(
            "armada/component" -> "spark-driver",
            "armada/template"  -> "spark-pi-driver"
          )
        )
    } else {
      builder
    }
  }

  test("SparkPi job using job templates - staticCluster", E2ETest) {
    baseTemplateTest("cluster", "static", 2)
      .run()
  }

  test("SparkPi job using job templates - staticClient", E2ETest) {
    baseTemplateTest("client", "static", 2)
      .run()
  }

  test("SparkPi job using job templates - dynamicCluster", E2ETest) {
    baseTemplateTest("cluster", "dynamic", 2)
      .run()
  }

  // ========================================================================
  // Feature Step Tests
  // ========================================================================

  private def baseFeatureStepTest(
      deployMode: String,
      allocation: String,
      executorCount: Int
  ): E2ETestBuilder = {
    val featureSteps = if (deployMode == "cluster") {
      Map(
        "spark.kubernetes.driver.pod.featureSteps" ->
          "org.apache.spark.deploy.armada.e2e.featurestep.DriverFeatureStep",
        "spark.kubernetes.executor.pod.featureSteps" ->
          "org.apache.spark.deploy.armada.e2e.featurestep.ExecutorFeatureStep"
      )
    } else {
      Map(
        "spark.kubernetes.executor.pod.featureSteps" ->
          "org.apache.spark.deploy.armada.e2e.featurestep.ExecutorFeatureStep"
      )
    }
    val featureStepLabels = Map(
      "feature-step"      -> "executor-applied",
      "feature-step-role" -> "executor"
    )

    val suffix = s"$allocation-$deployMode"
    val base = baseSparkPiTest(
      s"spark-pi-feature-steps-$suffix",
      deployMode,
      allocation,
      Map("test-type" -> "feature-step")
    )
      .withSparkConf(featureSteps)

    val builder = if (allocation == "static") {
      base
        .withExecutors(executorCount)
        .assertExecutorCount(executorCount)
        .assertExecutorsHaveLabels(featureStepLabels)
        .assertExecutorsHaveAnnotation("executor-feature-step", "configured")
        .withExecutorPodAssertion { pod =>
          Option(pod.getSpec.getActiveDeadlineSeconds).map(_.longValue).contains(1800L)
        }
    } else {
      base
        .withStandardDynamicAllocation(executorCount)
        .assertDynamicExecutorsHaveLabels(featureStepLabels, executorCount + 1)
        .assertDynamicExecutorsHaveAnnotations(
          Map("executor-feature-step" -> "configured"),
          executorCount + 1
        )
        .assertDynamicExecutorPod(
          pod =>
            Option(pod.getSpec.getActiveDeadlineSeconds)
              .map(_.longValue)
              .contains(1800L),
          "activeDeadlineSeconds=1800",
          executorCount + 1
        )
    }

    if (deployMode == "cluster") {
      builder
        .assertDriverHasLabels(
          Map(
            "feature-step"      -> "driver-applied",
            "feature-step-role" -> "driver"
          )
        )
        .assertDriverHasAnnotation("driver-feature-step", "configured")
        .withDriverPodAssertion { pod =>
          Option(pod.getSpec.getActiveDeadlineSeconds).map(_.longValue).contains(3600L)
        }
    } else {
      builder
    }
  }

  test("SparkPi job with custom feature steps - staticCluster", E2ETest) {
    baseFeatureStepTest("cluster", "static", 2)
      .run()
  }

  test("SparkPi job with custom feature steps - staticClient", E2ETest) {
    baseFeatureStepTest("client", "static", 2)
      .run()
  }

  test("SparkPi job with custom feature steps - dynamicCluster", E2ETest) {
    baseFeatureStepTest("cluster", "dynamic", 2)
      .run()
  }

  // ========================================================================
  // Ingress Tests
  // ========================================================================

  private val ingressAnnotations = Map(
    "nginx.ingress.kubernetes.io/rewrite-target"   -> "/",
    "nginx.ingress.kubernetes.io/backend-protocol" -> "HTTP",
    "kubernetes.io/ingress.class"                  -> "nginx"
  )

  private def baseIngressCLITest(executorCount: Int): E2ETestBuilder = {
    baseSparkPiTest("spark-pi-ingress", "cluster", "static", Map("test-type" -> "ingress"))
      .withDriverIngress(ingressAnnotations)
      .withSparkConf("spark.armada.driver.ui.ingress.tls.enabled", "false")
      .withExecutors(executorCount)
      .assertExecutorCount(executorCount)
      .assertIngressAnnotations(ingressAnnotations)
  }

  private def baseIngressTemplateTest(executorCount: Int): E2ETestBuilder = {
    baseSparkPiTest(
      "spark-pi-ingress-template",
      "cluster",
      "static",
      Map("test-type" -> "ingress-template")
    )
      .withJobTemplate(templateServer.url("spark-pi-job-template.yaml"))
      .withSparkConf(
        Map(
          "spark.armada.driver.ui.ingress.enabled" -> "true",
          "spark.armada.driver.jobItemTemplate" -> templateServer.url(
            "spark-pi-driver-ingress-template.yaml"
          ),
          "spark.armada.executor.jobItemTemplate" -> templateServer.url(
            "spark-pi-executor-template.yaml"
          )
        )
      )
      .withExecutors(executorCount)
      .assertExecutorCount(executorCount)
      .assertIngressAnnotations(ingressAnnotations)
  }

  test("SparkPi job with driver ingress using cli - staticCluster", E2ETest) {
    baseIngressCLITest(2).run()
  }

  test("SparkPi job with driver ingress using template - staticCluster", E2ETest) {
    baseIngressTemplateTest(2).run()
  }
}
