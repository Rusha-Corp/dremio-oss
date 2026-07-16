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

import com.dremio.config.DremioConfig;
import com.dremio.exec.proto.CoordinationProtos.NodeEndpoint;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.resource.GroupResourceInformation;
import com.dremio.resource.ResourceSchedulingProperties;
import com.dremio.resource.basic.BasicResourceAllocator;
import com.dremio.resource.basic.QueueType;
import com.dremio.resource.common.ResourceSchedulingContext;
import com.dremio.resource.exception.ResourceAllocationException;
import com.dremio.resource.exception.ResourceUnavailableException;
import com.dremio.service.coordinator.ClusterCoordinator;
import com.dremio.service.coordinator.ListenableSet;
import com.dremio.telemetry.api.metrics.MeterProviders;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Metrics;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Elastic Resource Allocator that scales executors based on query requirements.
 *
 * <p>This class extends BasicResourceAllocator to add elastic scaling via KEDA. It publishes
 * {@code elastic_desired_small} and {@code elastic_desired_large} Prometheus gauges that KEDA
 * reads to scale the corresponding StatefulSets. It does not directly interact with the Kubernetes
 * API.
 *
 * <p>The single source of truth for tier classification is the overridden {@link
 * #getQueueNameFromSchedulingProperties}, which adds a {@code routingQueue} check on top of the
 * cost-based classification in BasicResourceAllocator. This ensures that both the scaling logic and
 * the downstream tag filter ({@code ExecutorSelectionServiceImpl.applyTagFilter}) agree on the
 * tier.
 *
 * <ol>
 *   <li>Classify query tier via {@code getQueueNameFromSchedulingProperties} (routingQueue + cost)
 *   <li>Calculate required executors from query cost
 *   <li>Publish {@code elastic_desired_*} gauge so KEDA scales the StatefulSet
 *   <li>Wait for enough tier-tagged executors to register via ZooKeeper
 *   <li>Delegate to BasicResourceAllocator for queue/semaphore allocation
 * </ol>
 */
@Singleton
public class ElasticResourceAllocator extends BasicResourceAllocator {

  private static final Logger logger = LoggerFactory.getLogger(ElasticResourceAllocator.class);

  private final Provider<ClusterCoordinator> clusterCoordinatorProvider;
  private final ElasticAdmissionCalculator calculator;
  private final DremioConfig config;
  private final int scaleTimeoutMinutes;
  private final int maxExecutorsSmall;
  private final int maxExecutorsLarge;

  // Prometheus gauges read by KEDA ScaledObjects to set StatefulSet replica counts.
  private final AtomicInteger desiredSmall = new AtomicInteger(0);
  private final AtomicInteger desiredLarge = new AtomicInteger(0);

  // Idle-reset: resets desiredSmall/desiredLarge to 0 when all activity metrics
  // (jobs.active, maestro.active) have been 0 for IDLE_RESET_THRESHOLD consecutive
  // polls (30 min). This prevents stale elastic_desired_* metrics from keeping executors
  // running after a query completes or is cancelled, since ElasticResourceAllocator has
  // no query-completion hook to call scaleExecutors(-N).
  private static final int IDLE_RESET_THRESHOLD = 180; // 180 x 10 s = 1800 s = 30 min
  private final AtomicInteger idlePollCount = new AtomicInteger(0);
  private volatile boolean jobSeenSinceScaleUp = false;

  // Wall-clock backstop: if elastic_desired_* has been non-zero for longer than
  // MAX_STALE_MS, force-reset to 0 regardless of what the gauges say.
  private static final long MAX_STALE_MS = TimeUnit.MINUTES.toMillis(90);
  private volatile long lastScaleUpTime = 0;
  private final ScheduledExecutorService idleResetScheduler =
      Executors.newSingleThreadScheduledExecutor(
          r -> {
            Thread t = new Thread(r, "elastic-idle-reset");
            t.setDaemon(true);
            return t;
          });

  @Inject
  public ElasticResourceAllocator(
      Provider<ClusterCoordinator> clusterCoordinatorProvider,
      Provider<GroupResourceInformation> clusterResourceInformationProvider,
      DremioConfig config) {
    super(clusterCoordinatorProvider, clusterResourceInformationProvider);
    this.clusterCoordinatorProvider = clusterCoordinatorProvider;
    this.config = config;
    this.scaleTimeoutMinutes = config.getInt(DremioConfig.ELASTIC_SCALE_TIMEOUT);
    this.maxExecutorsSmall = config.getInt(DremioConfig.ELASTIC_MAX_EXECUTORS_SMALL);
    this.maxExecutorsLarge = config.getInt(DremioConfig.ELASTIC_MAX_EXECUTORS_LARGE);
    this.calculator =
        new ElasticAdmissionCalculator(
            config.getInt(DremioConfig.ELASTIC_SMALL_QUERY_THRESHOLD),
            config.getInt(DremioConfig.ELASTIC_MEDIUM_QUERY_THRESHOLD));
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
   * Single source of truth for tier classification. Adds a {@code routingQueue} check on top of
   * {@code BasicResourceAllocator.getQueueNameFromSchedulingProperties} (which uses cost only).
   * When {@code routingQueue} contains "large" (case-insensitive), returns LARGE regardless of
   * cost. This ensures that both the scaling logic and the downstream tag filter agree on the tier.
   */
  @Override
  protected QueueType getQueueNameFromSchedulingProperties(
      ResourceSchedulingContext queryContext,
      ResourceSchedulingProperties resourceSchedulingProperties) {
    String routingQueue = resourceSchedulingProperties.getRoutingQueue();
    if (routingQueue != null && routingQueue.toLowerCase().contains("large")) {
      boolean isBackground =
          queryContext
              .getQueryContextInfo()
              .getPriority()
              .getWorkloadClass()
              .equals(UserBitShared.WorkloadClass.BACKGROUND);
      return isBackground ? QueueType.REFLECTION_LARGE : QueueType.LARGE;
    }
    return super.getQueueNameFromSchedulingProperties(queryContext, resourceSchedulingProperties);
  }

  @Override
  public com.dremio.resource.ResourceSchedulingResult allocate(
      ResourceSchedulingContext queryContext,
      ResourceSchedulingProperties resourceSchedulingProperties,
      com.dremio.resource.ResourceSchedulingObserver resourceSchedulingObserver,
      Consumer<com.dremio.resource.ResourceSchedulingDecisionInfo> schedulingDecisionInfoConsumer)
      throws ResourceAllocationException {

    if (!config.getBoolean(DremioConfig.ELASTIC_ENABLED)) {
      return super.allocate(
          queryContext,
          resourceSchedulingProperties,
          resourceSchedulingObserver,
          schedulingDecisionInfoConsumer);
    }

    // Null-safe: getQueryCost() returns Double (nullable)
    Double rawCost = resourceSchedulingProperties.getQueryCost();
    double planCost = rawCost != null ? rawCost : 0.0;

    // Step 1: Classify query using the single source of truth (QueueType).
    // This override adds the routingQueue check on top of cost-based classification.
    QueueType queueType =
        getQueueNameFromSchedulingProperties(queryContext, resourceSchedulingProperties);
    boolean isLargeTier = isLargeTier(queueType);
    String tierTag = isLargeTier ? "large" : "small";

    // Step 2: Calculate required executors from cost.
    int requiredExecutors = calculator.calculateRequiredExecutors(planCost);
    int maxForTier = isLargeTier ? maxExecutorsLarge : maxExecutorsSmall;
    int desired = Math.min(requiredExecutors, maxForTier);

    // Step 3: Publish desired count as Prometheus gauge. KEDA reads this and scales
    // the StatefulSet. No Kubernetes API interaction from Dremio.
    publishDesired(isLargeTier, desired);
    armIdleGuard();

    logger.info(
        "Elastic scaling: query cost {} classified as {} ({}), desired {} executors",
        planCost,
        queueType,
        tierTag,
        desired);

    // Step 4: Wait for enough tier-tagged executors to register via ZooKeeper.
    ListenableSet executorSet =
        clusterCoordinatorProvider.get().getServiceSet(ClusterCoordinator.Role.EXECUTOR);
    int available = countTierExecutors(executorSet, tierTag);
    if (available < desired) {
      logger.info(
          "Waiting for {} {}-tagged executors (have {})", desired - available, tierTag, available);
      boolean ready;
      try {
        ready = waitForTierExecutors(executorSet, desired, tierTag, scaleTimeoutMinutes);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        ready = false;
      }
      if (!ready) {
        throw new ResourceUnavailableException(
            String.format(
                "%s executors did not become available within %d minute(s). "
                    + "Scaling is in progress — the query has been cancelled; please retry.",
                tierTag, scaleTimeoutMinutes));
      }
      logger.info("Elastic scaling complete: {} {} executors available", desired, tierTag);
    }

    // Step 5: Delegate to BasicResourceAllocator. super.allocate() calls
    // getQueueNameFromSchedulingProperties() which hits our override, ensuring
    // the same QueueType is used for queue assignment and tag filtering.
    return super.allocate(
        queryContext,
        resourceSchedulingProperties,
        resourceSchedulingObserver,
        schedulingDecisionInfoConsumer);
  }

  /** Returns true if the QueueType maps to the large executor tier. */
  private static boolean isLargeTier(QueueType queueType) {
    return queueType == QueueType.LARGE || queueType == QueueType.REFLECTION_LARGE;
  }

  /** Publishes the desired replica count as a Prometheus gauge for KEDA. */
  private void publishDesired(boolean isLargeTier, int desired) {
    if (isLargeTier) {
      desiredLarge.set(desired);
    } else {
      desiredSmall.set(desired);
    }
  }

  /** Counts registered executors with the given node tag. */
  private static int countTierExecutors(ListenableSet executorSet, String tag) {
    if (executorSet == null) {
      return 0;
    }
    return (int)
        executorSet.getAvailableEndpoints().stream()
            .filter(ep -> tag.equals(ep.getNodeTag()))
            .count();
  }

  /** Waits until enough tier-tagged executors are registered, or timeout. */
  private static boolean waitForTierExecutors(
      ListenableSet executorSet, int desired, String tag, int timeoutMinutes)
      throws InterruptedException {
    long deadline = System.currentTimeMillis() + TimeUnit.MINUTES.toMillis(timeoutMinutes);
    while (System.currentTimeMillis() < deadline) {
      if (countTierExecutors(executorSet, tag) >= desired) {
        return true;
      }
      Thread.sleep(2000);
    }
    return false;
  }

  /** Arms the cold-start guard to prevent premature idle-reset during executor provisioning. */
  private void armIdleGuard() {
    jobSeenSinceScaleUp = false;
    idlePollCount.set(0);
    lastScaleUpTime = System.currentTimeMillis();
  }

  /**
   * Resets desiredSmall/desiredLarge to 0 when all activity metrics have been 0 for
   * IDLE_RESET_THRESHOLD consecutive polls. Also includes a wall-clock backstop that force-resets
   * after MAX_STALE_MS if jobs.active is 0 (ignoring potentially stale maestro.active).
   */
  private void checkAndResetIfIdle() {
    try {
      boolean desiredNonZero = desiredSmall.get() > 0 || desiredLarge.get() > 0;
      if (desiredNonZero
          && lastScaleUpTime > 0
          && System.currentTimeMillis() - lastScaleUpTime > MAX_STALE_MS) {
        double jobsActive = getGaugeValue("jobs.active");
        if (jobsActive > 0.0) {
          lastScaleUpTime = System.currentTimeMillis();
          return;
        }
        int prevSmall = desiredSmall.getAndSet(0);
        int prevLarge = desiredLarge.getAndSet(0);
        double maestroActive = getGaugeValue("maestro.active");
        logger.warn(
            "Wall-clock backstop: elastic_desired non-zero for >{}min with jobs.active=0 "
                + "but maestro.active={}. Stale gauge suspected. "
                + "Force-resetting (small={}, large={}).",
            MAX_STALE_MS / 60_000,
            maestroActive,
            prevSmall,
            prevLarge);
        jobSeenSinceScaleUp = false;
        idlePollCount.set(0);
        lastScaleUpTime = 0;
        return;
      }

      double jobsActive = getGaugeValue("jobs.active");
      double maestroActive = getGaugeValue("maestro.active");
      boolean active = jobsActive > 0.0 || maestroActive > 0.0;

      if (active) {
        jobSeenSinceScaleUp = true;
        idlePollCount.set(0);
      } else {
        if (!jobSeenSinceScaleUp) {
          idlePollCount.set(0);
          return;
        }
        if (idlePollCount.incrementAndGet() >= IDLE_RESET_THRESHOLD) {
          int prevSmall = desiredSmall.getAndSet(0);
          int prevLarge = desiredLarge.getAndSet(0);
          if (prevSmall > 0 || prevLarge > 0) {
            logger.info(
                "Idle reset: all activity metrics=0 for {}s — "
                    + "resetting elastic_desired_small={} elastic_desired_large={} to 0",
                IDLE_RESET_THRESHOLD * 10,
                prevSmall,
                prevLarge);
          }
          jobSeenSinceScaleUp = false;
          idlePollCount.set(0);
        }
      }
    } catch (Exception e) {
      logger.debug("Idle-reset gauge read failed (non-fatal): {}", e.getMessage());
    }
  }

  /** Reads a gauge value from the Micrometer globalRegistry. Returns 0.0 if not found. */
  static double getGaugeValue(String name) {
    Gauge gauge = Metrics.globalRegistry.find(name).gauge();
    return gauge != null ? gauge.value() : 0.0;
  }
}
