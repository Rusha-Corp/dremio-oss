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
 */
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
    assertEquals(
        ElasticAdmissionCalculator.ExecutorTier.SMALL,
        calculator.getTier(1_000_000));
    assertEquals(
        ElasticAdmissionCalculator.ExecutorTier.SMALL,
        calculator.getTier(10_000_000));
  }

  @Test
  public void testGetTierLarge() {
    assertEquals(
        ElasticAdmissionCalculator.ExecutorTier.LARGE,
        calculator.getTier(10_000_001));
    assertEquals(
        ElasticAdmissionCalculator.ExecutorTier.LARGE,
        calculator.getTier(50_000_000));
  }
}
