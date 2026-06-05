/*
 * Copyright (C) 2017-2019 Dremio Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.dremio.resource.elastic;

import com.dremio.service.coordinator.ListenableSet;
import com.dremio.telemetry.api.metrics.MeterProviders;
import io.fabric8.kubernetes.api.model.apps.StatefulSet;
import io.fabric8.kubernetes.client.KubernetesClient;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Kubernetes implementation of ResourcePlatform.
 *
 * <p>Manages executor scaling by adjusting the replica count on pre-existing Kubernetes
 * StatefulSets. Two tiers are supported: SMALL and LARGE, each backed by a separate StatefulSet
 * (e.g., dremio-executor-small and dremio-executor-large). The StatefulSets and their ConfigMaps
 * must be created by the operator before enabling elastic scaling — see
 * docs/elastic-scaling-deployment.md.
 *
 * <p>StatefulSets are preferred over Deployments for Dremio executors because they provide stable
 * network identity (required for ZooKeeper registration) and support shared PersistentVolumeClaims
 * for paths.dist across multiple nodes.
 *
 * <p>This class only manages {@code .spec.replicas}; it does not create ConfigMaps, StatefulSets,
 * or Services. This avoids resource leaks and keeps the Java code decoupled from deployment
 * specifics.
 */
public class K8sPlatform implements ResourcePlatform, Closeable {
  private static final Logger logger = LoggerFactory.getLogger(K8sPlatform.class);

  private final KubernetesClient k8sClient;
  private final String namespace;
  private final String deploymentNameSmall;
  private final String deploymentNameLarge;
  private final ListenableSet executorSet;
  private final int maxExecutorsSmall;
  private final int maxExecutorsLarge;
  private final AtomicInteger desiredSmall = new AtomicInteger(0);
  private final AtomicInteger desiredLarge = new AtomicInteger(0);

  // Idle-reset: resets desiredSmall/desiredLarge to 0 when both jobs_active and
  // maestro_active have been 0 for IDLE_RESET_THRESHOLD consecutive polls (30 s).
  // This prevents stale elastic_desired_* metrics from keeping executors running
  // after a query completes or is cancelled, since ElasticResourceAllocator has
  // no query-completion hook to call scaleExecutors(-N).
  private static final int METRICS_PORT = 45679;
  private static final int IDLE_RESET_THRESHOLD = 3; // 3 × 10 s = 30 s
  private final AtomicInteger idlePollCount = new AtomicInteger(0);
  // Armed to false by scaleDeployment() when a scale-up is requested.
  // Set to true by checkAndResetIfIdle() when jobs_active or maestro_active > 0.
  // The idle countdown is suppressed while this is false to protect the executor
  // cold-start window (typically 60–90 s) during which jobs_active=0 even though
  // executors are being provisioned and the coordinator is blocking in waitForExecutors().
  private volatile boolean jobSeenSinceScaleUp = true;
  private final ScheduledExecutorService idleResetScheduler =
      Executors.newSingleThreadScheduledExecutor(
          r -> {
            Thread t = new Thread(r, "elastic-idle-reset");
            t.setDaemon(true);
            return t;
          });

  public K8sPlatform(
      KubernetesClient k8sClient,
      String namespace,
      String deploymentNameSmall,
      String deploymentNameLarge,
      ListenableSet executorSet,
      int maxExecutorsSmall,
      int maxExecutorsLarge) {
    this.k8sClient = k8sClient;
    this.namespace = namespace;
    this.deploymentNameSmall = deploymentNameSmall;
    this.deploymentNameLarge = deploymentNameLarge;
    this.executorSet = executorSet;
    this.maxExecutorsSmall = maxExecutorsSmall;
    this.maxExecutorsLarge = maxExecutorsLarge;
    MeterProviders.newGauge(
        "elastic_desired_small",
        "Desired small executor replicas as requested by ElasticResourceAllocator",
        desiredSmall::get);
    MeterProviders.newGauge(
        "elastic_desired_large",
        "Desired large executor replicas as requested by ElasticResourceAllocator",
        desiredLarge::get);
    idleResetScheduler.scheduleAtFixedRate(this::checkAndResetIfIdle, 10, 10, TimeUnit.SECONDS);
  }

  /**
   * Polls the coordinator's own liveness /metrics endpoint every 10 s. When both
   * {@code jobs_active} and {@code maestro_active} have been 0 for
   * {@link #IDLE_RESET_THRESHOLD} consecutive polls, resets desiredSmall and
   * desiredLarge to 0 so that stale elastic_desired_* gauges do not keep executors
   * running indefinitely after a query completes or is cancelled.
   */
  private void checkAndResetIfIdle() {
    try {
      URL url = new URL("http://localhost:" + METRICS_PORT + "/metrics");
      String metrics;
      try (InputStream is = url.openStream()) {
        metrics = new String(is.readAllBytes(), StandardCharsets.UTF_8);
      }
      double jobsActive = parsePrometheusGauge(metrics, "jobs_active");
      double maestroActive = parsePrometheusGauge(metrics, "maestro_active");

      if (jobsActive > 0.0 || maestroActive > 0.0) {
        // Work is active — record that at least one job has run since the last scale-up,
        // which makes it safe to reset desired counts once we go idle again.
        jobSeenSinceScaleUp = true;
        idlePollCount.set(0);
      } else {
        // jobs_active=0 and maestro_active=0.
        // Guard: if a scale-up was requested but no job has been seen yet, this is the
        // executor cold-start window — suppress the countdown to avoid prematurely
        // resetting elastic_desired_* while executors are still starting up.
        if (!jobSeenSinceScaleUp) {
          idlePollCount.set(0);
          return;
        }
        if (idlePollCount.incrementAndGet() >= IDLE_RESET_THRESHOLD) {
          int prevSmall = desiredSmall.getAndSet(0);
          int prevLarge = desiredLarge.getAndSet(0);
          if (prevSmall > 0 || prevLarge > 0) {
            logger.info(
                "Idle reset: jobs_active=0 maestro_active=0 for {}s — "
                    + "resetting elastic_desired_small={} elastic_desired_large={} to 0",
                IDLE_RESET_THRESHOLD * 10, prevSmall, prevLarge);
          }
          jobSeenSinceScaleUp = false; // re-arm for the next scale-up cycle
          idlePollCount.set(0);
        }
      }
    } catch (Exception e) {
      logger.debug("Idle-reset metrics poll failed (non-fatal): {}", e.getMessage());
    }
  }

  /** Parses a bare {@code name <value>} line from a Prometheus text-format scrape. */
  private static double parsePrometheusGauge(String metrics, String name) {
    for (String line : metrics.split("\n")) {
      if (line.startsWith(name + " ") && !line.startsWith("#")) {
        try {
          return Double.parseDouble(line.substring(name.length()).trim());
        } catch (NumberFormatException ignored) {
          // fall through → return 0
        }
      }
    }
    return 0.0;
  }

  @Override
  public int getAvailableExecutors() {
    if (executorSet == null) {
      return 0;
    }
    return executorSet.getAvailableEndpoints().size();
  }

  @Override
  public boolean waitForExecutors(int requiredExecutors, long timeout, TimeUnit unit)
      throws InterruptedException {
    long deadline = System.currentTimeMillis() + unit.toMillis(timeout);
    while (System.currentTimeMillis() < deadline) {
      if (getAvailableExecutors() >= requiredExecutors) {
        return true;
      }
      Thread.sleep(2000);
    }
    return false;
  }

  @Override
  public boolean scaleExecutors(int scaleDelta) {
    return scaleDeployment(deploymentNameSmall, scaleDelta, maxExecutorsSmall);
  }

  @Override
  public boolean scaleExecutors(int scaleDelta, ElasticAdmissionCalculator.ExecutorTier tier) {
    String deploymentName =
        (tier == ElasticAdmissionCalculator.ExecutorTier.LARGE)
            ? deploymentNameLarge
            : deploymentNameSmall;
    int cap =
        (tier == ElasticAdmissionCalculator.ExecutorTier.LARGE)
            ? maxExecutorsLarge
            : maxExecutorsSmall;
    return scaleDeployment(deploymentName, scaleDelta, cap);
  }

  @Override
  public int getAvailableExecutors(ElasticAdmissionCalculator.ExecutorTier tier) {
    if (executorSet == null) {
      return 0;
    }
    String tag = (tier == ElasticAdmissionCalculator.ExecutorTier.LARGE) ? "large" : "small";
    return (int)
        executorSet.getAvailableEndpoints().stream()
            .filter(ep -> tag.equals(ep.getNodeTag()))
            .count();
  }

  @Override
  public boolean waitForExecutors(
      int requiredExecutors,
      ElasticAdmissionCalculator.ExecutorTier tier,
      long timeout,
      TimeUnit unit)
      throws InterruptedException {
    long deadline = System.currentTimeMillis() + unit.toMillis(timeout);
    while (System.currentTimeMillis() < deadline) {
      if (getAvailableExecutors(tier) >= requiredExecutors) {
        return true;
      }
      Thread.sleep(2000);
    }
    return false;
  }

  private boolean scaleDeployment(String deploymentName, int scaleDelta, int maxReplicas) {
    if (scaleDelta == 0) {
      return true;
    }

    try {
      StatefulSet sts =
          k8sClient.apps().statefulSets().inNamespace(namespace).withName(deploymentName).get();

      if (sts == null) {
        logger.error(
            "Executor StatefulSet {} not found in namespace {}. "
                + "Ensure the StatefulSet is created before enabling elastic scaling.",
            deploymentName,
            namespace);
        return false;
      }

      int currentReplicas = sts.getSpec().getReplicas();
      int newReplicas = Math.max(0, currentReplicas + scaleDelta);

      // Enforce max replicas cap for this tier
      if (newReplicas > maxReplicas) {
        logger.warn(
            "Scale cap: requested {} replicas exceeds max {}, capping at {}",
            newReplicas,
            maxReplicas,
            maxReplicas);
        newReplicas = maxReplicas;
      }

      logger.info(
          "Scaling {} StatefulSet from {} to {} replicas",
          deploymentName,
          currentReplicas,
          newReplicas);

      // Publish desired replica count as a Prometheus gauge. The KEDA metrics exporter
      // reads elastic_desired_small/large from the coordinator liveness endpoint and
      // propagates them to KEDA, which then sets .spec.replicas on the StatefulSet.
      if (deploymentName.equals(deploymentNameSmall)) {
        desiredSmall.set(newReplicas);
      } else {
        desiredLarge.set(newReplicas);
      }
      // Arm the cold-start guard so the idle-reset does not fire while executors
      // are starting up (jobs_active=0 during the ~60-90s provisioning window).
      if (newReplicas > 0) {
        jobSeenSinceScaleUp = false;
      }

      logger.info(
          "Published elastic_desired_{}={} to Prometheus (KEDA exporter will apply via KEDA)",
          deploymentName.equals(deploymentNameSmall) ? "small" : "large",
          newReplicas);

      return true;
    } catch (Exception e) {
      logger.error("Failed to scale {}: {}", deploymentName, e.getMessage(), e);
      return false;
    }
  }

  @Override
  public void close() throws IOException {
    idleResetScheduler.shutdownNow();
    k8sClient.close();
  }
}
