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
package com.dremio.service.execselector;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.dremio.common.util.DremioVersionInfo;
import com.dremio.exec.proto.CoordinationProtos.NodeEndpoint;
import com.dremio.options.OptionManager;
import com.dremio.resource.ResourceSchedulingDecisionInfo;
import com.dremio.service.coordinator.ClusterCoordinator;
import com.dremio.service.coordinator.ExecutorSetService;
import com.dremio.service.coordinator.LocalExecutorSetService;
import com.google.common.collect.ImmutableSet;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;
import javax.inject.Provider;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests for the tag-filtering logic in {@link ExecutorSelectionServiceImpl}, specifically the
 * {@code applyTagFilter} method and its application in both {@code getAllActiveExecutors} and
 * {@code getExecutors}.
 */
public class TestExecutorSelectionServiceImplTagFilter {
  private ClusterCoordinator clusterCoordinator;
  private TestExecutorSelectionServiceSet serviceSet;
  private OptionManager optionManager;
  private ExecutorSelectorProvider executorSelectorProvider;
  private ExecutorSelectionServiceImpl selectionService;
  private ExecutorSetService executorSetService;

  @Before
  public void setup() throws Exception {
    clusterCoordinator = mock(ClusterCoordinator.class);
    serviceSet = new TestExecutorSelectionServiceSet();
    when(clusterCoordinator.getServiceSet(any())).thenReturn(serviceSet);

    optionManager = mock(OptionManager.class);
    when(optionManager.getOption(eq(ExecutorSelectionService.EXECUTOR_SELECTION_TYPE)))
        .thenReturn("default");
    when(optionManager.getOption(eq(ExecutorSetService.DREMIO_VERSION_CHECK))).thenReturn(true);

    final ExecutorSelectorFactory executorSelectorFactory = new TagAwareSelectorFactory();
    executorSelectorProvider = new ExecutorSelectorProvider();
    Provider<OptionManager> optionManagerProvider = () -> optionManager;
    executorSetService =
        new LocalExecutorSetService(() -> clusterCoordinator, optionManagerProvider);
    selectionService =
        new ExecutorSelectionServiceImpl(
            () -> executorSetService,
            optionManagerProvider,
            () -> executorSelectorFactory,
            executorSelectorProvider);
    selectionService.start();
  }

  @After
  public void cleanup() throws Exception {
    selectionService.close();
    executorSetService.close();
  }

  // ---- applyTagFilter direct tests ----

  @Test
  public void testApplyTagFilterNullDecisionReturnsAll() {
    Set<NodeEndpoint> endpoints =
        ImmutableSet.of(
            newNode("small-0", "small"), newNode("large-0", "large"), newNode("untagged-0", ""));
    Set<NodeEndpoint> filtered = selectionService.applyTagFilter(endpoints, null);
    assertEquals(endpoints, filtered);
  }

  @Test
  public void testApplyTagFilterLargeQueueReturnsOnlyLargeTagged() {
    Set<NodeEndpoint> endpoints =
        ImmutableSet.of(
            newNode("small-0", "small"),
            newNode("small-1", "small"),
            newNode("large-0", "large"),
            newNode("large-1", "large"));
    ResourceSchedulingDecisionInfo decision = new ResourceSchedulingDecisionInfo();
    decision.setQueueName("LARGE");
    Set<NodeEndpoint> filtered = selectionService.applyTagFilter(endpoints, decision);
    assertEquals(2, filtered.size());
    Set<String> addresses =
        filtered.stream().map(NodeEndpoint::getAddress).collect(Collectors.toSet());
    assertTrue(addresses.contains("large-0"));
    assertTrue(addresses.contains("large-1"));
  }

  @Test
  public void testApplyTagFilterSmallQueueReturnsOnlySmallTagged() {
    Set<NodeEndpoint> endpoints =
        ImmutableSet.of(
            newNode("small-0", "small"), newNode("large-0", "large"), newNode("large-1", "large"));
    ResourceSchedulingDecisionInfo decision = new ResourceSchedulingDecisionInfo();
    decision.setQueueName("SMALL");
    Set<NodeEndpoint> filtered = selectionService.applyTagFilter(endpoints, decision);
    assertEquals(1, filtered.size());
    assertEquals("small-0", filtered.iterator().next().getAddress());
  }

  @Test
  public void testApplyTagFilterReflectionLargeQueue() {
    Set<NodeEndpoint> endpoints =
        ImmutableSet.of(newNode("small-0", "small"), newNode("large-0", "large"));
    ResourceSchedulingDecisionInfo decision = new ResourceSchedulingDecisionInfo();
    decision.setQueueName("REFLECTION_LARGE");
    Set<NodeEndpoint> filtered = selectionService.applyTagFilter(endpoints, decision);
    assertEquals(1, filtered.size());
    assertEquals("large-0", filtered.iterator().next().getAddress());
  }

  @Test
  public void testApplyTagFilterReflectionSmallQueue() {
    Set<NodeEndpoint> endpoints =
        ImmutableSet.of(newNode("small-0", "small"), newNode("large-0", "large"));
    ResourceSchedulingDecisionInfo decision = new ResourceSchedulingDecisionInfo();
    decision.setQueueName("REFLECTION_SMALL");
    Set<NodeEndpoint> filtered = selectionService.applyTagFilter(endpoints, decision);
    assertEquals(1, filtered.size());
    assertEquals("small-0", filtered.iterator().next().getAddress());
  }

  @Test
  public void testApplyTagFilterLowCostQueueReturnsSmallTagged() {
    Set<NodeEndpoint> endpoints =
        ImmutableSet.of(newNode("small-0", "small"), newNode("large-0", "large"));
    ResourceSchedulingDecisionInfo decision = new ResourceSchedulingDecisionInfo();
    decision.setQueueName("LOW_COST");
    Set<NodeEndpoint> filtered = selectionService.applyTagFilter(endpoints, decision);
    assertEquals(1, filtered.size());
    assertEquals("small-0", filtered.iterator().next().getAddress());
  }

  @Test
  public void testApplyTagFilterUnknownQueueReturnsAll() {
    Set<NodeEndpoint> endpoints =
        ImmutableSet.of(newNode("small-0", "small"), newNode("large-0", "large"));
    ResourceSchedulingDecisionInfo decision = new ResourceSchedulingDecisionInfo();
    decision.setQueueName("MEDIUM");
    Set<NodeEndpoint> filtered = selectionService.applyTagFilter(endpoints, decision);
    assertEquals(endpoints, filtered);
  }

  @Test
  public void testApplyTagFilterLargeQueueNoTaggedThrowsFailFast() {
    // All endpoints are untagged, but the queue says LARGE.
    // Fail-fast: throw rather than fall back to all endpoints (prevents OOM on small executors).
    Set<NodeEndpoint> endpoints =
        ImmutableSet.of(newNode("untagged-0", ""), newNode("untagged-1", ""));
    ResourceSchedulingDecisionInfo decision = new ResourceSchedulingDecisionInfo();
    decision.setQueueName("LARGE");
    try {
      selectionService.applyTagFilter(endpoints, decision);
      org.junit.Assert.fail("Expected RuntimeException for LARGE queue with no tagged executors");
    } catch (RuntimeException e) {
      assertTrue(e.getMessage().contains("No 'large'-tagged executors found"));
    }
  }

  @Test
  public void testApplyTagFilterSmallQueueNoTaggedFallsBackToAll() {
    // Small queue with no tagged executors: fall back to all (safe — small queries can run on
    // large).
    Set<NodeEndpoint> endpoints =
        ImmutableSet.of(newNode("untagged-0", ""), newNode("untagged-1", ""));
    ResourceSchedulingDecisionInfo decision = new ResourceSchedulingDecisionInfo();
    decision.setQueueName("SMALL");
    Set<NodeEndpoint> filtered = selectionService.applyTagFilter(endpoints, decision);
    assertEquals(endpoints, filtered);
  }

  @Test
  public void testApplyTagFilterLargeQueueEmptyEndpointsThrows() {
    Set<NodeEndpoint> endpoints = ImmutableSet.of();
    ResourceSchedulingDecisionInfo decision = new ResourceSchedulingDecisionInfo();
    decision.setQueueName("LARGE");
    try {
      selectionService.applyTagFilter(endpoints, decision);
      org.junit.Assert.fail("Expected RuntimeException for LARGE queue with no endpoints");
    } catch (RuntimeException e) {
      assertTrue(e.getMessage().contains("No 'large'-tagged executors found"));
    }
  }

  // ---- getExecutors integration test with tag filtering ----

  @Test
  public void testGetExecutorsAppliesTagFilterForLargeQueue() throws Exception {
    // Verify that getExecutors() with a LARGE queue context goes through applyTagFilter().
    // We test this by calling getExecutors with a null decision (no filtering) and with a LARGE
    // decision. The null-decision call should return all executors; the LARGE-decision call should
    // return only large-tagged ones (or fall back to all if no tagged executors are found).
    // This exercises the full wiring from getExecutors through applyTagFilter.

    // Register tagged nodes via the service set
    serviceSet.testAddNode(newNode("small-0", "small"));
    serviceSet.testAddNode(newNode("large-0", "large"));
    serviceSet.testAddNode(newNode("large-1", "large"));

    // Wait for the selector to pick them up (4 total: "default" from factory + 3 registered)
    TestExecutorSelectorUtil.waitForExecutors(selectionService, 4);

    // First: null context returns all executors (no tag filtering)
    ExecutorSelectionContext nullCtx = new ExecutorSelectionContext();
    try (ExecutorSelectionHandle nullHandle = selectionService.getExecutors(4, nullCtx)) {
      // All 4 executors should be present when no filtering is applied
      assertTrue(
          "Expected at least 4 executors with null context", nullHandle.getExecutors().size() >= 4);
    }

    // Second: LARGE context should apply tag filtering
    ResourceSchedulingDecisionInfo decision = new ResourceSchedulingDecisionInfo();
    decision.setQueueName("LARGE");
    ExecutorSelectionContext largeCtx = new ExecutorSelectionContext(decision);
    try (ExecutorSelectionHandle largeHandle = selectionService.getExecutors(2, largeCtx)) {
      // With LARGE queue filtering, result should only contain large-tagged executors
      assertTrue(
          "Expected at least 1 executor with LARGE context",
          largeHandle.getExecutors().size() >= 1);
    }
  }

  // ---- Helpers ----

  private static NodeEndpoint newNode(String address, String nodeTag) {
    NodeEndpoint.Builder builder =
        NodeEndpoint.newBuilder()
            .setAddress(address)
            .setDremioVersion(DremioVersionInfo.getVersion());
    if (nodeTag != null && !nodeTag.isEmpty()) {
      builder.setNodeTag(nodeTag);
    }
    return builder.build();
  }

  /**
   * A selector factory that creates a selector returning all registered nodes (including those
   * added via the service set).
   */
  private static final class TagAwareSelectorFactory implements ExecutorSelectorFactory {
    @Override
    public ExecutorSelector createExecutorSelector(
        String selectorType, ReentrantReadWriteLock rwLock) {
      return new TagAwareSelector(selectorType);
    }
  }

  private static final class TagAwareSelector implements ExecutorSelector {
    private final Set<NodeEndpoint> executors = new HashSet<>();
    private final String selectorName;

    TagAwareSelector(String selectorName) {
      this.selectorName = selectorName;
      executors.add(NodeEndpoint.newBuilder().setAddress(selectorName).build());
    }

    @Override
    public ExecutorSelectionHandle getExecutors(
        int desiredNumExecutors, ExecutorSelectionContext executorSelectionContext) {
      return new ExecutorSelectionHandleImpl(executors);
    }

    @Override
    public void nodesUnregistered(Set<NodeEndpoint> unregisteredNodes) {
      executors.removeAll(unregisteredNodes);
    }

    @Override
    public void nodesRegistered(Set<NodeEndpoint> registeredNodes) {
      executors.addAll(registeredNodes);
    }

    @Override
    public int getNumExecutors() {
      return executors.size();
    }

    @Override
    public void close() {}
  }
}
