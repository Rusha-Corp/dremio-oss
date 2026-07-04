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

/** Tests for ElasticAdmissionCalculator - calculates required executors based on query cost. */
public class ElasticAdmissionCalculatorTest {

  private final ElasticAdmissionCalculator calculator = new ElasticAdmissionCalculator();

  @Test
  public void testSmallQueryRequiresOneExecutor() {
    assertEquals(1, calculator.calculateRequiredExecutors(1_000_000));
    assertEquals(1, calculator.calculateRequiredExecutors(5_000_000));
    assertEquals(1, calculator.calculateRequiredExecutors(10_000_000));
  }

  @Test
  public void testMediumQueryRequiresTwoExecutors() {
    assertEquals(2, calculator.calculateRequiredExecutors(10_000_001));
    assertEquals(2, calculator.calculateRequiredExecutors(20_000_000));
    assertEquals(2, calculator.calculateRequiredExecutors(30_000_000));
  }

  @Test
  public void testLargeQueryRequiresThreeExecutors() {
    assertEquals(3, calculator.calculateRequiredExecutors(30_000_001));
    assertEquals(3, calculator.calculateRequiredExecutors(50_000_000));
    assertEquals(3, calculator.calculateRequiredExecutors(100_000_000));
  }

  @Test
  public void testZeroCostQueryRequiresOneExecutor() {
    assertEquals(1, calculator.calculateRequiredExecutors(0));
  }

  @Test
  public void testNoScaleWhenAlreadyEnough() {
    assertEquals(0, calculator.calculateScaleDelta(3, 5));
    assertEquals(0, calculator.calculateScaleDelta(1, 2));
  }

  @Test
  public void testScaleDeltaWhenNotEnough() {
    assertEquals(3, calculator.calculateScaleDelta(5, 2));
    assertEquals(1, calculator.calculateScaleDelta(2, 1));
  }

  @Test
  public void testNoScaleWhenExactMatch() {
    assertEquals(0, calculator.calculateScaleDelta(3, 3));
    assertEquals(0, calculator.calculateScaleDelta(1, 1));
  }

  @Test
  public void testScaleFromZero() {
    assertEquals(3, calculator.calculateScaleDelta(3, 0));
  }

  @Test
  public void testGetTierSmall() {
    assertEquals(ElasticAdmissionCalculator.ExecutorTier.SMALL, calculator.getTier(1_000_000));
    assertEquals(ElasticAdmissionCalculator.ExecutorTier.SMALL, calculator.getTier(10_000_000));
  }

  @Test
  public void testGetTierLarge() {
    assertEquals(ElasticAdmissionCalculator.ExecutorTier.LARGE, calculator.getTier(10_000_001));
    assertEquals(ElasticAdmissionCalculator.ExecutorTier.LARGE, calculator.getTier(50_000_000));
  }

  @Test
  public void testGetTierWithLargeQueueName() {
    // queue name overrides plan cost
    assertEquals(
        ElasticAdmissionCalculator.ExecutorTier.LARGE,
        calculator.getTier(1_000, "query.large"));
    assertEquals(
        ElasticAdmissionCalculator.ExecutorTier.LARGE, calculator.getTier(1_000, "LARGE"));
    assertEquals(
        ElasticAdmissionCalculator.ExecutorTier.LARGE, calculator.getTier(0, "WS.large"));
  }

  @Test
  public void testGetTierWithSmallOrNullQueue() {
    // falls back to cost-based when no "large" keyword
    assertEquals(
        ElasticAdmissionCalculator.ExecutorTier.SMALL,
        calculator.getTier(1_000, "query.small"));
    assertEquals(
        ElasticAdmissionCalculator.ExecutorTier.SMALL, calculator.getTier(1_000, null));
    assertEquals(
        ElasticAdmissionCalculator.ExecutorTier.LARGE,
        calculator.getTier(50_000_000, null));
  }

  // ---- 3-arg getTier with queue threshold override ----

  @Test
  public void testGetTierWithOverrideThresholdSmall() {
    // cost 20M with override threshold 30M -> SMALL (20M <= 30M)
    // even though the default smallQueryThreshold (10M) would classify it as LARGE
    assertEquals(
        ElasticAdmissionCalculator.ExecutorTier.SMALL,
        calculator.getTier(20_000_000, null, 30_000_000));
  }

  @Test
  public void testGetTierWithOverrideThresholdLarge() {
    // cost 35M with override threshold 30M -> LARGE (35M > 30M)
    assertEquals(
        ElasticAdmissionCalculator.ExecutorTier.LARGE,
        calculator.getTier(35_000_000, null, 30_000_000));
  }

  @Test
  public void testGetTierWithOverrideThresholdLargeQueueOverrides() {
    // "large" queue name overrides regardless of cost or threshold
    assertEquals(
        ElasticAdmissionCalculator.ExecutorTier.LARGE,
        calculator.getTier(100, "query.large", 30_000_000));
  }

  @Test
  public void testGetTierWithOverrideThresholdAlignsWithQueueThreshold() {
    // Key alignment test: cost 15M with threshold 30M -> SMALL
    // but same cost with threshold 10M -> LARGE
    assertEquals(
        ElasticAdmissionCalculator.ExecutorTier.SMALL,
        calculator.getTier(15_000_000, null, 30_000_000));
    assertEquals(
        ElasticAdmissionCalculator.ExecutorTier.LARGE,
        calculator.getTier(15_000_000, null, 10_000_000));
  }
}
