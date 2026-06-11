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

import io.fabric8.kubernetes.api.model.Pod
import org.apache.spark.deploy.armada.submit.GangSchedulingAnnotations
import scala.jdk.CollectionConverters._
import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._
import org.scalatest.Assertions._

/** Fluent API for building E2E tests. Makes it easy for developers to create comprehensive tests
  * for their features.
  *
  * Example usage:
  * {{{
  * E2ETestBuilder("My new feature")
  *   .withSparkConf("spark.my.feature.enabled", "true")
  *   .withExecutors(3)
  *   .assertExecutorCount(3)
  *   .assertDriverHasLabel("feature", "enabled")
  *   .run()
  * }}}
  */
class E2ETestBuilder(testName: String) {
  private var sparkConfs                            = Map.empty[String, String]
  private var assertions                            = Seq.empty[TestAssertion]
  private var baseQueueName                         = "e2e-template"
  private var imageName                             = "spark:armada"
  private var masterUrl                             = "armada://localhost:30002"
  private var lookoutUrl                            = "http://localhost:30000"
  private var scalaVersion                          = "2.13"
  private var sparkVersion                          = "3.5.5"
  private var failFastOnPodFailure                  = true
  private var pythonScript: Option[String]          = None
  private var appArgs: Seq[String]                  = Seq("100")
  private var assertionPollInterval: FiniteDuration = 5.seconds

  def withSparkConf(key: String, value: String): E2ETestBuilder = {
    sparkConfs += (key -> value)
    this
  }

  def withSparkConf(confs: Map[String, String]): E2ETestBuilder = {
    sparkConfs ++= confs
    this
  }

  def withExecutors(count: Int): E2ETestBuilder = {
    withSparkConf("spark.executor.instances", count.toString)
  }

  def withDeployMode(mode: String): E2ETestBuilder = {
    withSparkConf("spark.submit.deployMode", mode)
  }

  /** Use Python script instead of Scala class */
  def withPythonScript(script: String): E2ETestBuilder = {
    pythonScript = Some(script)
    this
  }

  /** Application arguments. */
  def withAppArgs(args: String*): E2ETestBuilder = {
    appArgs = args.toSeq
    this
  }

  /** Enable driver ingress with annotations */
  def withDriverIngress(annotations: Map[String, String] = Map.empty): E2ETestBuilder = {
    withSparkConf("spark.armada.driver.ui.ingress.enabled", "true")
    if (annotations.nonEmpty) {
      withSparkConf(
        "spark.armada.driver.ui.ingress.annotations",
        annotations.map { case (k, v) => s"$k=$v" }.mkString(",")
      )
    }
    this
  }

  /** Use job templates */
  def withJobTemplate(path: String): E2ETestBuilder = {
    withSparkConf("spark.armada.jobTemplate", path)
  }

  def withPodLabels(labels: Map[String, String]): E2ETestBuilder = {
    val labelString = labels.map { case (k, v) => s"$k=$v" }.mkString(",")
    withSparkConf("spark.armada.pod.labels", labelString)
  }

  def withNodeSelectors(selectors: Map[String, String]): E2ETestBuilder = {
    val selectorString = selectors.map { case (k, v) => s"$k=$v" }.mkString(",")
    withSparkConf("spark.armada.scheduling.nodeSelectors", selectorString)
  }

  /** Submit the job as a gang-scheduled job with the specified node uniformity label */
  def withGangJob(nodeUniformityLabel: String): E2ETestBuilder = {
    withSparkConf("spark.armada.scheduling.nodeUniformity", nodeUniformityLabel)
  }

  /** Enable dynamic allocation. */
  def withDynamicAllocation(
      initialExecutors: Int,
      maxExecutors: Int,
      schedulerBacklogTimeoutSeconds: Int = 3,
      executorIdleTimeoutSeconds: Int = 90
  ): E2ETestBuilder = {
    withSparkConf("spark.dynamicAllocation.enabled", "true")
    // Required for Spark < 3.4.0 (shuffle tracking is enabled by default from 3.4.0+)
    // This allows dynamic allocation without an external shuffle service
    // Ref: https://issues.apache.org/jira/browse/SPARK-39846
    withSparkConf("spark.dynamicAllocation.shuffleTracking.enabled", "true")
    withSparkConf("spark.dynamicAllocation.minExecutors", "0")
    withSparkConf("spark.dynamicAllocation.maxExecutors", maxExecutors.toString)
    withSparkConf("spark.dynamicAllocation.initialExecutors", initialExecutors.toString)
    // nodeUniformity is required for all dynamic modes in Armada
    withSparkConf("spark.armada.scheduling.nodeUniformity", "armada-spark")
    withSparkConf(
      "spark.dynamicAllocation.schedulerBacklogTimeout",
      s"${schedulerBacklogTimeoutSeconds}s"
    )
    withSparkConf(
      "spark.dynamicAllocation.executorIdleTimeout",
      s"${executorIdleTimeoutSeconds}s"
    )
    withSparkConf("spark.armada.allocation.checkInterval", "1s")
    assertionPollInterval = 10.seconds
    this
  }

  /** Enable dynamic allocation with standard defaults (maxExecutors = 4). */
  def withStandardDynamicAllocation(executorCount: Int): E2ETestBuilder = {
    withDynamicAllocation(initialExecutors = executorCount, maxExecutors = 4)
  }

  /** Apply static or dynamic allocation config. */
  def withAllocationMode(
      allocation: String,
      executorCount: Int
  ): E2ETestBuilder = {
    allocation match {
      case "static"  => withExecutors(executorCount).assertExecutorCount(executorCount)
      case "dynamic" => withStandardDynamicAllocation(executorCount)
      case other     => throw new IllegalArgumentException(s"Unknown allocation mode: $other")
    }
  }

  /** Assert exact executor count */
  def assertExecutorCount(expected: Int): E2ETestBuilder = {
    assertions :+= new ExecutorCountAssertion(expected)
    this
  }

  def assertExecutorCountMaxReachedAtLeast(expectedMin: Int): E2ETestBuilder = {
    assertions :+= new ExecutorCountMaxReachedAssertion(expectedMin)
    this
  }

  def assertDriverExists(): E2ETestBuilder = {
    assertions :+= new DriverExistsAssertion()
    this
  }

  /** Assert driver has specific label */
  def assertDriverHasLabel(key: String, value: String): E2ETestBuilder = {
    assertions :+= new DriverLabelAssertion(Map(key -> value))
    this
  }

  /** Assert executors have specific label */
  def assertExecutorsHaveLabel(key: String, value: String): E2ETestBuilder = {
    assertions :+= new ExecutorLabelAssertion(Map(key -> value))
    this
  }

  /** Assert driver ingress exists with specific annotations (key-value pairs) */
  def assertIngressAnnotations(
      requiredAnnotations: Map[String, String] = Map.empty
  ): E2ETestBuilder = {
    assertions :+= new IngressAssertion(requiredAnnotations)
    this
  }

  /** Assert pods are scheduled with node selectors */
  def assertNodeSelectors(selectors: Map[String, String]): E2ETestBuilder = {
    // We need to create a custom assertion that uses the test-id from context
    assertions :+= new TestAssertion {
      override val name = "Node selector verification"
      override def assert(context: AssertionContext)(implicit
          ec: ExecutionContext
      ): Future[AssertionResult] = {
        // Use test-id from context to find all pods for this test
        val podSelector = s"test-id=${context.testId}"
        new NodeSelectorAssertion(podSelector, selectors).assert(context)
      }
    }
    this
  }

  /** Assert all pods have specific labels */
  def assertPodLabels(labels: Map[String, String]): E2ETestBuilder = {
    labels.foreach { case (key, value) =>
      assertDriverHasLabel(key, value)
      assertExecutorsHaveLabel(key, value)
    }
    this
  }

  /** Assert driver pod has all specified labels */
  def assertDriverHasLabels(labels: Map[String, String]): E2ETestBuilder = {
    labels.foreach { case (key, value) =>
      assertDriverHasLabel(key, value)
    }
    this
  }

  /** Assert executor pods have all specified labels */
  def assertExecutorsHaveLabels(labels: Map[String, String]): E2ETestBuilder = {
    labels.foreach { case (key, value) =>
      assertExecutorsHaveLabel(key, value)
    }
    this
  }

  /** Assert driver has specific annotation */
  def assertDriverHasAnnotation(key: String, value: String): E2ETestBuilder = {
    assertions :+= new DriverAnnotationAssertion(Map(key -> value))
    this
  }

  /** Assert executors have specific annotation */
  def assertExecutorsHaveAnnotation(key: String, value: String): E2ETestBuilder = {
    assertions :+= new ExecutorAnnotationAssertion(Map(key -> value))
    this
  }

  /** Assert driver pod has all specified annotations */
  def assertDriverHasAnnotations(annotations: Map[String, String]): E2ETestBuilder = {
    annotations.foreach { case (key, value) =>
      assertDriverHasAnnotation(key, value)
    }
    this
  }

  /** Assert executor pods have all specified annotations */
  def assertExecutorsHaveAnnotations(annotations: Map[String, String]): E2ETestBuilder = {
    annotations.foreach { case (key, value) =>
      assertExecutorsHaveAnnotation(key, value)
    }
    this
  }

  /** Assert dynamic executor pods have all specified labels (tracks across polls) */
  def assertDynamicExecutorsHaveLabels(
      labels: Map[String, String],
      expectedMinPods: Int
  ): E2ETestBuilder = {
    assertions :+= new DynamicExecutorTrackingAssertion(
      s"has labels $labels",
      pod => {
        val podLabels = Option(pod.getMetadata.getLabels)
          .map(_.asScala.toMap)
          .getOrElse(Map.empty)
        labels.forall { case (k, v) => podLabels.get(k).contains(v) }
      },
      expectedMinPods
    )
    this
  }

  /** Assert dynamic executor pods have all specified annotations (tracks across polls) */
  def assertDynamicExecutorsHaveAnnotations(
      annotations: Map[String, String],
      expectedMinPods: Int
  ): E2ETestBuilder = {
    assertions :+= new DynamicExecutorTrackingAssertion(
      s"has annotations $annotations",
      pod => {
        val podAnnotations = Option(pod.getMetadata.getAnnotations)
          .map(_.asScala.toMap)
          .getOrElse(Map.empty)
        annotations.forall { case (k, v) => podAnnotations.get(k).contains(v) }
      },
      expectedMinPods
    )
    this
  }

  /** Assert dynamic executor pods match a predicate, tracking across polls */
  def assertDynamicExecutorPod(
      predicate: Pod => Boolean,
      predicateDescription: String,
      expectedMinPods: Int
  ): E2ETestBuilder = {
    assertions :+= new DynamicExecutorTrackingAssertion(
      predicateDescription,
      predicate,
      expectedMinPods
    )
    this
  }

  /** Assert driver pod matches a predicate */
  def withDriverPodAssertion(predicate: Pod => Boolean): E2ETestBuilder = {
    assertions :+= new DriverPodAssertion(predicate)
    this
  }

  /** Assert executor pods match a predicate */
  def withExecutorPodAssertion(predicate: Pod => Boolean): E2ETestBuilder = {
    assertions :+= new ExecutorPodAssertion(predicate)
    this
  }

  private def hasGangEnvVars(
      nodeUniformityLabel: String
  ): Pod => Boolean = { pod =>
    val envVars = pod.getSpec.getContainers.asScala
      .flatMap(c => Option(c.getEnv).map(_.asScala).getOrElse(Seq.empty))
      .map(e => e.getName -> e.getValue)
      .toMap
    envVars.get("ARMADA_GANG_NODE_UNIFORMITY_LABEL_NAME").contains(nodeUniformityLabel)
  }

  /** Helper method to create a predicate that validates gang scheduling annotations */
  private def hasValidGangAnnotations(
      nodeUniformityLabel: String,
      expectedCardinality: Int
  ): Pod => Boolean = { pod =>
    val annotations = pod.getMetadata.getAnnotations
    Option(annotations.get(GangSchedulingAnnotations.GANG_ID)).exists(_.nonEmpty) &&
    Option(annotations.get(GangSchedulingAnnotations.GANG_CARDINALITY))
      .contains(expectedCardinality.toString) &&
    Option(annotations.get(GangSchedulingAnnotations.GANG_NODE_UNIFORMITY_LABEL))
      .contains(nodeUniformityLabel)
  }

  /** Assert that driver and all executors have correct gang scheduling annotations */
  def assertGangJob(nodeUniformityLabel: String, expectedCardinality: Int): E2ETestBuilder = {
    val validator = hasValidGangAnnotations(nodeUniformityLabel, expectedCardinality)
    withDriverPodAssertion(validator)
    withExecutorPodAssertion(validator)
  }

  /** Assert gang scheduling for dynamic allocation. Adds three tracking assertions:
    *   1. At least `initialGangPods` pods have Armada-injected gang env vars
    *   2. At least `initialGangPods` pods have gang annotations (only the initial batch gets gang
    *      annotations; scale-up batches use cardinality=0 and rely on node selectors)
    *   3. Every scale-up pod (no gang annotations) must have a node selector for the uniformity
    *      label. Initial gang pods pass unconditionally. Uses `initialGangPods` as min count so the
    *      test passes even if the job completes before scale-up, but if a scale-up pod appears
    *      without a node selector it fails.
    */
  def assertGangJobForDynamic(
      nodeUniformityLabel: String,
      initialGangPods: Int
  ): E2ETestBuilder = {
    // Initial gang executors must have Armada-injected env vars
    assertions :+= new DynamicExecutorTrackingAssertion(
      s"gang env vars ($nodeUniformityLabel)",
      hasGangEnvVars(nodeUniformityLabel),
      initialGangPods
    )
    // Only initial batch executors have gang annotations (scale-up uses cardinality=0)
    assertions :+= new DynamicExecutorTrackingAssertion(
      s"gang annotations ($nodeUniformityLabel)",
      pod => {
        val annotations = pod.getMetadata.getAnnotations
        annotations != null &&
        Option(annotations.get(GangSchedulingAnnotations.GANG_ID)).exists(_.nonEmpty) &&
        Option(annotations.get(GangSchedulingAnnotations.GANG_NODE_UNIFORMITY_LABEL))
          .contains(nodeUniformityLabel)
      },
      initialGangPods
    )
    // Scale-up pods must have a node selector for the uniformity label.
    // Initial gang pods (which have gang annotations) pass unconditionally.
    assertions :+= new DynamicExecutorTrackingAssertion(
      s"scale-up node selector ($nodeUniformityLabel)",
      pod => {
        val annotations = Option(pod.getMetadata.getAnnotations)
        val hasGangAnnotation = annotations.exists(a =>
          Option(a.get(GangSchedulingAnnotations.GANG_ID)).exists(_.nonEmpty)
        )
        if (hasGangAnnotation) true // initial gang pod — no node selector expected
        else {
          val nodeSelector = Option(pod.getSpec.getNodeSelector)
            .map(_.asScala.toMap)
            .getOrElse(Map.empty)
          nodeSelector.contains(nodeUniformityLabel)
        }
      },
      initialGangPods
    )
    this
  }

  /** Assert that executors have correct gang scheduling annotations (for client mode where driver
    * is external)
    */
  def assertExecutorGangJob(
      nodeUniformityLabel: String,
      expectedCardinality: Int
  ): E2ETestBuilder = {
    val validator = hasValidGangAnnotations(nodeUniformityLabel, expectedCardinality)
    withExecutorPodAssertion(validator)
  }

  def build(): TestConfig = {
    TestConfig(
      baseQueueName = baseQueueName,
      imageName = imageName,
      masterUrl = masterUrl,
      lookoutUrl = lookoutUrl,
      scalaVersion = scalaVersion,
      sparkVersion = sparkVersion,
      sparkConfs = sparkConfs,
      assertions = assertions,
      failFastOnPodFailure = failFastOnPodFailure,
      pythonScript = pythonScript,
      appArgs = appArgs,
      assertionPollInterval = assertionPollInterval
    )
  }

  /** Configure base settings from existing config */
  def withBaseConfig(config: TestConfig): E2ETestBuilder = {
    this.baseQueueName = config.baseQueueName
    this.imageName = config.imageName
    this.masterUrl = config.masterUrl
    this.lookoutUrl = config.lookoutUrl
    this.scalaVersion = config.scalaVersion
    this.sparkVersion = config.sparkVersion
    this.sparkConfs ++= config.sparkConfs
    this.failFastOnPodFailure = config.failFastOnPodFailure
    this.appArgs = config.appArgs
    this
  }

  def run()(implicit orchestrator: TestOrchestrator): TestResult = {
    import scala.concurrent.Await
    import TestConstants.RunTimeout
    val future = orchestrator.runTest(testName, build())
    val result = Await.result(future, RunTimeout)

    assert(result.status == JobSetStatus.Success, s"Job failed with status: ${result.status}")

    result.assertionResults.foreach { case (name, assertionResult) =>
      assertionResult match {
        case AssertionResult.Success => // Success, no need to log
        case AssertionResult.Failure(msg) =>
          fail(s"Assertion '$name' failed: $msg")
      }
    }

    result
  }
}

/** Companion object with factory methods */
object E2ETestBuilder {
  def apply(testName: String): E2ETestBuilder = new E2ETestBuilder(testName)
}
