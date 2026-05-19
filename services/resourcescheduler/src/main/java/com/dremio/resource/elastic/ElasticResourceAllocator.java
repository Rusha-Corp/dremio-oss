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
import com.dremio.resource.basic.BasicResourceAllocator;
import com.dremio.resource.common.ResourceSchedulingContext;
import com.dremio.service.coordinator.ClusterCoordinator;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Elastic Resource Allocator that scales executors based on query requirements.
 *
 * <p>This class wraps BasicResourceAllocator to add elastic scaling:
 *
 * <ol>
 *   <li>Calculate required executors from query cost
 *   <li>Check current available executors
 *   <li>If not enough, scale nodes/pods via ResourcePlatform
 *   <li>Wait for executors to register with Dremio Coordinator
 *   <li>Proceed with normal allocation
 * </ol>
 */
@Singleton
public class ElasticResourceAllocator extends BasicResourceAllocator {

  private static final Logger logger = LoggerFactory.getLogger(ElasticResourceAllocator.class);

  private final Provider<ResourcePlatform> resourcePlatformProvider;
  private final ElasticAdmissionCalculator calculator;
  private final DremioConfig config;

  @Inject
  public ElasticResourceAllocator(
      Provider<ClusterCoordinator> clusterCoordinatorProvider,
      Provider<com.dremio.resource.GroupResourceInformation> clusterResourceInformationProvider,
      Provider<ResourcePlatform> resourcePlatformProvider,
      DremioConfig config) {
    super(clusterCoordinatorProvider, clusterResourceInformationProvider);
    this.resourcePlatformProvider = resourcePlatformProvider;
    this.config = config;
    this.calculator =
        new ElasticAdmissionCalculator(
            config.getInt(DremioConfig.ELASTIC_SMALL_QUERY_THRESHOLD),
            config.getInt(DremioConfig.ELASTIC_MEDIUM_QUERY_THRESHOLD));
  }

  @Override
  public com.dremio.resource.ResourceSchedulingResult allocate(
      ResourceSchedulingContext queryContext,
      com.dremio.resource.ResourceSchedulingProperties resourceSchedulingProperties,
      com.dremio.resource.ResourceSchedulingObserver resourceSchedulingObserver,
      Consumer<com.dremio.resource.ResourceSchedulingDecisionInfo> schedulingDecisionInfoConsumer) {

    // Check if elastic scaling is enabled
    if (!config.getBoolean(DremioConfig.ELASTIC_ENABLED)) {
      // Elastic not enabled, use basic allocation
      return super.allocate(
          queryContext,
          resourceSchedulingProperties,
          resourceSchedulingObserver,
          schedulingDecisionInfoConsumer);
    }

    // Get ResourcePlatform - this will throw if elastic is enabled but platform is misconfigured
    ResourcePlatform resourcePlatform = resourcePlatformProvider.get();

    // If we get here with NoOp, something is wrong - fail fast
    if (resourcePlatform == NoOpResourcePlatform.INSTANCE) {
      throw new IllegalStateException(
          "Elastic executor is enabled but no valid platform is configured. "
              + "Please configure services.executor.elastic.platform.type and associated settings.");
    }

    // Get query cost
    double planCost = resourceSchedulingProperties.getQueryCost();
    long executorMemory = getExecutorMemoryBytes(queryContext);

    // Step 1: Calculate required executors
    int requiredExecutors = calculator.calculateRequiredExecutors(planCost, executorMemory);

    // Step 2: Get current available executors via platform
    int availableExecutors = resourcePlatform.getAvailableExecutors();

    // Step 3: Check if we need to scale
    int scaleDelta = calculator.calculateScaleDelta(requiredExecutors, availableExecutors);

    if (scaleDelta > 0) {
      logger.info(
          "Elastic scaling: query cost {} requires {} executors, have {}. Scaling by {}",
          planCost,
          requiredExecutors,
          availableExecutors,
          scaleDelta);

      // Scale the executors (create pods)
      boolean podsCreated = resourcePlatform.scaleExecutors(scaleDelta);
      if (!podsCreated) {
        throw new RuntimeException(
            "Elastic scaling failed: could not provision required executors");
      }

      // Calculate required nodes
      long nodeMemory = getNodeMemoryBytes(queryContext);
      int requiredNodes =
          calculator.calculateRequiredNodes(requiredExecutors, nodeMemory, executorMemory);

      // Wait for scaling with timeout (5 minutes)
      try {
        boolean scaled = resourcePlatform.waitForExecutors(requiredExecutors, 5, TimeUnit.MINUTES);

        if (!scaled) {
          throw new RuntimeException(
              "Elastic scaling timeout: could not provision required executors within 5 minutes");
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException("Elastic scaling interrupted", e);
      }

      logger.info(
          "Elastic scaling complete: {} executors available",
          resourcePlatform.getAvailableExecutors());
    }

    // Step 4: Proceed with normal allocation
    return super.allocate(
        queryContext,
        resourceSchedulingProperties,
        resourceSchedulingObserver,
        schedulingDecisionInfoConsumer);
  }

  private long getExecutorMemoryBytes(ResourceSchedulingContext context) {
    return config.getInt(DremioConfig.ELASTIC_EXECUTOR_MEMORY_GB) * 1024L * 1024L * 1024L;
  }

  private long getNodeMemoryBytes(ResourceSchedulingContext context) {
    return config.getInt(DremioConfig.ELASTIC_NODE_MEMORY_GB) * 1024L * 1024L * 1024L;
  }
}
