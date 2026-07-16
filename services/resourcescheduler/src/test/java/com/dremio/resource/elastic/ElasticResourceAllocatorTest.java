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

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.dremio.config.DremioConfig;
import com.dremio.exec.proto.CoordExecRPC;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.options.OptionManager;
import com.dremio.resource.GroupResourceInformation;
import com.dremio.resource.ResourceSchedulingProperties;
import com.dremio.resource.basic.BasicResourceConstants;
import com.dremio.resource.basic.QueueType;
import com.dremio.resource.common.ResourceSchedulingContext;
import com.dremio.service.coordinator.ClusterCoordinator;
import javax.inject.Provider;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests for ElasticResourceAllocator tier classification logic.
 *
 * <p>The single source of truth is {@code getQueueNameFromSchedulingProperties}, which adds a
 * routingQueue check on top of BasicResourceAllocator's cost-based classification. These tests
 * verify that routingQueue="large" overrides cost, and that cost-based classification delegates to
 * the parent.
 */
public class ElasticResourceAllocatorTest {

  private Provider<ClusterCoordinator> clusterCoordinatorProvider;
  private Provider<GroupResourceInformation> groupResourceInfoProvider;
  private DremioConfig config;
  private OptionManager optionManager;

  @Before
  public void setUp() {
    clusterCoordinatorProvider = mock(Provider.class);
    groupResourceInfoProvider = mock(Provider.class);

    optionManager = mock(OptionManager.class);
    when(optionManager.getOption(BasicResourceConstants.QUEUE_THRESHOLD_SIZE))
        .thenReturn(30_000_000L);

    config =
        DremioConfig.create()
            .withValue(DremioConfig.ELASTIC_ENABLED, true)
            .withValue(DremioConfig.ELASTIC_SCALE_TIMEOUT, 1)
            .withValue(DremioConfig.ELASTIC_SMALL_QUERY_THRESHOLD, 10_000_000)
            .withValue(DremioConfig.ELASTIC_MEDIUM_QUERY_THRESHOLD, 30_000_000)
            .withValue(DremioConfig.ELASTIC_MAX_EXECUTORS_SMALL, 10)
            .withValue(DremioConfig.ELASTIC_MAX_EXECUTORS_LARGE, 8);
  }

  /**
   * Creates a mock ResourceSchedulingContext with the shared optionManager and a non-background
   * workload class.
   */
  private ResourceSchedulingContext mockContext() {
    ResourceSchedulingContext ctx = mock(ResourceSchedulingContext.class);
    when(ctx.getOptions()).thenReturn(optionManager);
    CoordExecRPC.QueryContextInformation queryContextInfo =
        mock(CoordExecRPC.QueryContextInformation.class);
    CoordExecRPC.FragmentPriority priority = mock(CoordExecRPC.FragmentPriority.class);
    when(priority.getWorkloadClass()).thenReturn(UserBitShared.WorkloadClass.GENERAL);
    when(queryContextInfo.getPriority()).thenReturn(priority);
    when(ctx.getQueryContextInfo()).thenReturn(queryContextInfo);
    return ctx;
  }

  // ---- Tier classification: routingQueue overrides cost ----

  @Test
  public void testLargeQueueOverridesLowCost() {
    ElasticResourceAllocator allocator =
        new ElasticResourceAllocator(
            clusterCoordinatorProvider, groupResourceInfoProvider, config);
    ResourceSchedulingProperties props = new ResourceSchedulingProperties();
    props.setQueryCost(100.0);
    props.setRoutingQueue("query.large");
    assertEquals(
        QueueType.LARGE,
        allocator.getQueueNameFromSchedulingProperties(mockContext(), props));
  }

  @Test
  public void testLargeQueueCaseInsensitive() {
    ElasticResourceAllocator allocator =
        new ElasticResourceAllocator(
            clusterCoordinatorProvider, groupResourceInfoProvider, config);
    ResourceSchedulingProperties props = new ResourceSchedulingProperties();
    props.setQueryCost(100.0);
    props.setRoutingQueue("WS.LARGE");
    assertEquals(
        QueueType.LARGE,
        allocator.getQueueNameFromSchedulingProperties(mockContext(), props));
  }

  // ---- Tier classification: cost-based fallback when no routingQueue ----

  @Test
  public void testLowCostWithoutRoutingQueueIsSmall() {
    ElasticResourceAllocator allocator =
        new ElasticResourceAllocator(
            clusterCoordinatorProvider, groupResourceInfoProvider, config);
    ResourceSchedulingProperties props = new ResourceSchedulingProperties();
    props.setQueryCost(1_000_000.0);
    assertEquals(
        QueueType.SMALL,
        allocator.getQueueNameFromSchedulingProperties(mockContext(), props));
  }

  @Test
  public void testHighCostWithoutRoutingQueueIsLarge() {
    ElasticResourceAllocator allocator =
        new ElasticResourceAllocator(
            clusterCoordinatorProvider, groupResourceInfoProvider, config);
    ResourceSchedulingProperties props = new ResourceSchedulingProperties();
    props.setQueryCost(50_000_000.0);
    assertEquals(
        QueueType.LARGE,
        allocator.getQueueNameFromSchedulingProperties(mockContext(), props));
  }

  @Test
  public void testCostAtThresholdIsSmall() {
    // cost == threshold should be SMALL (cost > threshold is LARGE)
    ElasticResourceAllocator allocator =
        new ElasticResourceAllocator(
            clusterCoordinatorProvider, groupResourceInfoProvider, config);
    ResourceSchedulingProperties props = new ResourceSchedulingProperties();
    props.setQueryCost(30_000_000.0);
    assertEquals(
        QueueType.SMALL,
        allocator.getQueueNameFromSchedulingProperties(mockContext(), props));
  }

  // ---- Tier classification: routingQueue without "large" falls back to cost ----

  @Test
  public void testSmallQueueNameFallsBackToCost() {
    ElasticResourceAllocator allocator =
        new ElasticResourceAllocator(
            clusterCoordinatorProvider, groupResourceInfoProvider, config);
    ResourceSchedulingProperties props = new ResourceSchedulingProperties();
    props.setQueryCost(1_000_000.0);
    props.setRoutingQueue("query.small");
    assertEquals(
        QueueType.SMALL,
        allocator.getQueueNameFromSchedulingProperties(mockContext(), props));
  }

  // ---- Required executors (from ElasticAdmissionCalculator) ----

  @Test
  public void testSmallQueryNeedsOneExecutor() {
    ElasticAdmissionCalculator calc = new ElasticAdmissionCalculator();
    assertEquals(1, calc.calculateRequiredExecutors(1_000_000));
  }

  @Test
  public void testMediumQueryNeedsTwoExecutors() {
    ElasticAdmissionCalculator calc = new ElasticAdmissionCalculator();
    assertEquals(2, calc.calculateRequiredExecutors(20_000_000));
  }

  @Test
  public void testLargeQueryNeedsThreeExecutors() {
    ElasticAdmissionCalculator calc = new ElasticAdmissionCalculator();
    assertEquals(3, calc.calculateRequiredExecutors(50_000_000));
  }
}
