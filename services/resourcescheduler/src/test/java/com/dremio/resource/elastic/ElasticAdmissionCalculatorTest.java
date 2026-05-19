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

import org.junit.Test;

/**
 * Tests for ElasticAdmissionCalculator - calculates required executors based on query cost.
 *
 * <p>RED test: This test defines the expected behavior for admission calculation. GREEN: Minimal
 * implementation will be added to pass.
 */
public class ElasticAdmissionCalculatorTest {

  private final ElasticAdmissionCalculator calculator = new ElasticAdmissionCalculator();

  /**
   * Small queries (cost <= 10M) should require only 1 executor. These can share capacity with other
   * small queries.
   */
  @Test
  public void testSmallQueryRequiresOneExecutor() {
    assertEquals(1, calculator.calculateRequiredExecutors(1_000_000, 8_000_000_000L));
    assertEquals(1, calculator.calculateRequiredExecutors(5_000_000, 8_000_000_000L));
    assertEquals(1, calculator.calculateRequiredExecutors(10_000_000, 8_000_000_000L));
  }

  /** Medium queries (10M < cost <= 30M) should require 2 executors. */
  @Test
  public void testMediumQueryRequiresTwoExecutors() {
    assertEquals(2, calculator.calculateRequiredExecutors(10_000_001, 8_000_000_000L));
    assertEquals(2, calculator.calculateRequiredExecutors(20_000_000, 8_000_000_000L));
    assertEquals(2, calculator.calculateRequiredExecutors(30_000_000, 8_000_000_000L));
  }

  /** Large queries (cost > 30M) should require 3 executors. These need dedicated capacity. */
  @Test
  public void testLargeQueryRequiresThreeExecutors() {
    assertEquals(3, calculator.calculateRequiredExecutors(30_000_001, 8_000_000_000L));
    assertEquals(3, calculator.calculateRequiredExecutors(50_000_000, 8_000_000_000L));
    assertEquals(3, calculator.calculateRequiredExecutors(100_000_000, 8_000_000_000L));
  }

  /** Edge case: zero cost query. */
  @Test
  public void testZeroCostQueryRequiresOneExecutor() {
    assertEquals(1, calculator.calculateRequiredExecutors(0, 8_000_000_000L));
  }

  /** Edge case: very small executor memory. */
  @Test
  public void testSmallExecutorMemory() {
    // Even with small memory, calculation should be based on cost thresholds
    assertEquals(1, calculator.calculateRequiredExecutors(5_000_000, 1_000_000_000L));
    assertEquals(2, calculator.calculateRequiredExecutors(20_000_000, 1_000_000_000L));
  }

  /** When current capacity exceeds required, no scale needed. */
  @Test
  public void testNoScaleWhenAlreadyEnough() {
    // Current: 5, Required: 3 -> Delta: 0
    assertEquals(0, calculator.calculateScaleDelta(3, 5));
    // Current: 2, Required: 1 -> Delta: 0
    assertEquals(0, calculator.calculateScaleDelta(1, 2));
  }

  /** When current capacity is insufficient, scale up by delta. */
  @Test
  public void testScaleDeltaWhenNotEnough() {
    // Current: 2, Required: 5 -> Delta: 3
    assertEquals(3, calculator.calculateScaleDelta(5, 2));
    // Current: 1, Required: 2 -> Delta: 1
    assertEquals(1, calculator.calculateScaleDelta(2, 1));
  }

  /** When current capacity exactly matches required, no scale needed. */
  @Test
  public void testNoScaleWhenExactMatch() {
    // Current: 3, Required: 3 -> Delta: 0
    assertEquals(0, calculator.calculateScaleDelta(3, 3));
    // Current: 1, Required: 1 -> Delta: 0
    assertEquals(0, calculator.calculateScaleDelta(1, 1));
  }

  /** Edge case: scale from zero executors. */
  @Test
  public void testScaleFromZero() {
    // Current: 0, Required: 3 -> Delta: 3
    assertEquals(3, calculator.calculateScaleDelta(3, 0));
  }

  /** 5 executors at 8GB each = 40GB needed, 32GB nodes = 2 nodes needed. */
  @Test
  public void testCalculateNodesForExecutors() {
    // 5 executors * 8GB = 40GB needed
    // 32GB node can hold 3 executors (24GB), leaving 8GB headroom
    // Need second node for remaining 2 executors (16GB)
    assertEquals(2, calculator.calculateRequiredNodes(5, 32_000_000_000L, 8_000_000_000L));
  }

  /** 1 executor fits in 1 node. */
  @Test
  public void testSingleExecutorFitsInOneNode() {
    assertEquals(1, calculator.calculateRequiredNodes(1, 32_000_000_000L, 8_000_000_000L));
  }

  /** 4 executors exactly fit in one 32GB node (4 * 8GB = 32GB). */
  @Test
  public void testExactFitNoWastedNodes() {
    assertEquals(1, calculator.calculateRequiredNodes(4, 32_000_000_000L, 8_000_000_000L));
  }

  /** Zero executors should require zero nodes. */
  @Test
  public void testZeroExecutorsRequiresZeroNodes() {
    assertEquals(0, calculator.calculateRequiredNodes(0, 32_000_000_000L, 8_000_000_000L));
  }

  /** Edge case: very small node, many executors. */
  @Test
  public void testManyExecutorsNeedManyNodes() {
    // 10 executors * 8GB = 80GB needed
    // 32GB nodes: 3 nodes hold 80GB (3 * 32 = 96GB capacity)
    assertEquals(3, calculator.calculateRequiredNodes(10, 32_000_000_000L, 8_000_000_000L));
  }
}
