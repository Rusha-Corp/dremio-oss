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
 *   <li>If not enough, scale executors via ResourcePlatform
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
  private final int scaleTimeoutMinutes;

  @Inject
  public ElasticResourceAllocator(
      Provider<ClusterCoordinator> clusterCoordinatorProvider,
      Provider<com.dremio.resource.GroupResourceInformation> clusterResourceInformationProvider,
      Provider<ResourcePlatform> resourcePlatformProvider,
      DremioConfig config) {
    super(clusterCoordinatorProvider, clusterResourceInformationProvider);
    this.resourcePlatformProvider = resourcePlatformProvider;
    this.config = config;
    this.scaleTimeoutMinutes = config.getInt(DremioConfig.ELASTIC_SCALE_TIMEOUT);
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

    if (!config.getBoolean(DremioConfig.ELASTIC_ENABLED)) {
      return super.allocate(
          queryContext,
          resourceSchedulingProperties,
          resourceSchedulingObserver,
          schedulingDecisionInfoConsumer);
    }

    ResourcePlatform resourcePlatform = resourcePlatformProvider.get();

    if (resourcePlatform == NoOpResourcePlatform.INSTANCE) {
      throw new IllegalStateException(
          "Elastic executor is enabled but no valid platform is configured. "
              + "Please configure services.executor.elastic.kubernetes settings.");
    }

    // Null-safe: getQueryCost() returns Double (nullable)
    Double rawCost = resourceSchedulingProperties.getQueryCost();
    double planCost = rawCost != null ? rawCost : 0.0;

    // Step 1: Calculate required executors and tier
    int requiredExecutors = calculator.calculateRequiredExecutors(planCost);
    ElasticAdmissionCalculator.ExecutorTier tier = calculator.getTier(planCost);
    // Use tier-aware count so a running small executor does not satisfy a LARGE query's requirement
    int availableExecutors = resourcePlatform.getAvailableExecutors(tier);
    int scaleDelta = calculator.calculateScaleDelta(requiredExecutors, availableExecutors);

    if (scaleDelta > 0) {
      logger.info(
          "Elastic scaling: query cost {} requires {} {} executors, have {}. Scaling by {}",
          planCost,
          requiredExecutors,
          tier,
          availableExecutors,
          scaleDelta);

      // Step 2: Scale the executors for the appropriate tier
      boolean scaled = resourcePlatform.scaleExecutors(scaleDelta, tier);
      if (!scaled) {
        throw new RuntimeException(
            "Elastic scaling failed: could not provision "
                + scaleDelta
                + " executors for query with cost "
                + planCost);
      }

      // Step 3: Wait for executors of the correct tier to become ready
      boolean ready = false;
      try {
        ready =
            resourcePlatform.waitForExecutors(
                requiredExecutors, tier, scaleTimeoutMinutes, TimeUnit.MINUTES);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }

      if (!ready) {
        logger.warn(
            "Elastic scaling timeout after {}min for {} executors, proceeding with available executors",
            scaleTimeoutMinutes,
            requiredExecutors);
      } else {
        logger.info(
            "Elastic scaling complete: {} executors available",
            resourcePlatform.getAvailableExecutors());
      }
    }

    // Step 4: Delegate to BasicResourceAllocator
    return super.allocate(
        queryContext,
        resourceSchedulingProperties,
        resourceSchedulingObserver,
        schedulingDecisionInfoConsumer);
  }
}
