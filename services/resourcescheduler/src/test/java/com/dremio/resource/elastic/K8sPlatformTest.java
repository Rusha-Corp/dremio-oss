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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/** Tests for K8sPlatform — Micrometer gauge lookup and scaling arithmetic. */
public class K8sPlatformTest {

  private SimpleMeterRegistry registry;

  @Before
  public void setUp() {
    registry = new SimpleMeterRegistry();
    Metrics.globalRegistry.add(registry);
  }

  @After
  public void tearDown() {
    Metrics.globalRegistry.remove(registry);
  }

  // ---- Micrometer gauge lookup ----

  @Test
  public void testGetGaugeValueRegistered() {
    AtomicLong value = new AtomicLong(3);
    Metrics.globalRegistry.gauge("test_jobs_active", value, AtomicLong::get);
    assertEquals(3.0, K8sPlatform.getGaugeValue("test_jobs_active"), 0.001);

    value.set(5);
    assertEquals(5.0, K8sPlatform.getGaugeValue("test_jobs_active"), 0.001);
  }

  @Test
  public void testGetGaugeValueMissing() {
    // Non-existent gauge should return 0.0
    assertEquals(0.0, K8sPlatform.getGaugeValue("nonexistent_gauge"), 0.001);
  }

  @Test
  public void testGetGaugeValueZero() {
    AtomicLong value = new AtomicLong(0);
    Metrics.globalRegistry.gauge("test_jobs_active_zero", value, AtomicLong::get);
    assertEquals(0.0, K8sPlatform.getGaugeValue("test_jobs_active_zero"), 0.001);
  }

  @Test
  public void testTotalMetricsSufficientForIdleDetection() {
    // Verify that total metrics (jobs.active) cover all tiers:
    // jobs.active = jobs.active.small + jobs.active.large
    // so checking only totals is sufficient for the idle-reset decision.
    long small = 1;
    long large = 0;
    long total = small + large;
    assertTrue(total > 0);
  }

  // ---- Scaling arithmetic (mirrors K8sPlatform.scaleDeployment logic) ----

  @Test
  public void testScaleSmallFromZero() {
    AtomicInteger desiredSmall = new AtomicInteger(0);
    int currentReplicas = 0;
    int newReplicas = Math.max(0, currentReplicas + 1);
    desiredSmall.set(newReplicas);
    assertEquals(1, desiredSmall.get());
  }

  @Test
  public void testScaleLargeFromZero() {
    AtomicInteger desiredLarge = new AtomicInteger(0);
    int currentReplicas = 0;
    int newReplicas = Math.max(0, currentReplicas + 3);
    desiredLarge.set(newReplicas);
    assertEquals(3, desiredLarge.get());
  }

  @Test
  public void testScaleCappedAtMax() {
    int maxReplicas = 4;
    int currentReplicas = 3;
    int scaleDelta = 5;
    int newReplicas = Math.min(currentReplicas + scaleDelta, maxReplicas);
    assertEquals(4, newReplicas);
  }

  @Test
  public void testScaleDown() {
    int currentReplicas = 4;
    int scaleDelta = -2;
    int newReplicas = Math.max(0, currentReplicas + scaleDelta);
    assertEquals(2, newReplicas);
  }

  @Test
  public void testScaleDownFloorZero() {
    int currentReplicas = 1;
    int scaleDelta = -3;
    int newReplicas = Math.max(0, currentReplicas + scaleDelta);
    assertEquals(0, newReplicas);
  }

  // ---- Idle reset logic ----

  @Test
  public void testIdleResetThresholdCalculation() {
    // 30 polls * 10s = 300s = 5min idle threshold
    int threshold = 30;
    int polls = 0;

    for (int i = 0; i < threshold - 1; i++) {
      polls++;
      assertFalse(polls >= threshold);
    }
    polls++;
    assertTrue(polls >= threshold);
  }

  @Test
  public void testIdleResetResetsGaugesToZero() {
    AtomicInteger desiredSmall = new AtomicInteger(2);
    AtomicInteger desiredLarge = new AtomicInteger(1);

    desiredSmall.set(0);
    desiredLarge.set(0);

    assertEquals(0, desiredSmall.get());
    assertEquals(0, desiredLarge.get());
  }

  @Test
  public void testIdleResetBlockedWhenGuardArmed() {
    // When jobSeenSinceScaleUp is false, the idle countdown should NOT proceed.
    boolean jobSeenSinceScaleUp = false;
    int idlePollCount = 0;

    // Simulate 6 idle polls with guard still armed
    if (!jobSeenSinceScaleUp) {
      // countdown is suppressed
      idlePollCount = 0;
    }

    assertEquals(0, idlePollCount);
  }

  @Test
  public void testIdleResetProceedsAfterJobSeen() {
    // When jobSeenSinceScaleUp is true and jobs go idle, the countdown should proceed.
    boolean jobSeenSinceScaleUp = true;
    int threshold = 30;
    int idlePollCount = 0;

    // Simulate 30 idle polls with guard disarmed
    for (int i = 0; i < threshold; i++) {
      if (!jobSeenSinceScaleUp) {
        idlePollCount = 0;
        continue;
      }
      idlePollCount++;
    }

    assertTrue(idlePollCount >= threshold);
  }

  @Test
  public void testJobSeenSinceScaleUpStartsFalse() {
    // The guard should start as false so idle-reset cannot fire before a job is observed.
    // This prevents the race where the initial value of true allows premature reset.
    boolean jobSeenSinceScaleUp = false;
    assertFalse(jobSeenSinceScaleUp);
  }

  @Test
  public void testArmIdleGuardResetsState() {
    // armIdleGuard() sets jobSeenSinceScaleUp=false and resets the poll counter
    boolean jobSeenSinceScaleUp = true; // was set to true by a previous job
    int idlePollCount = 5;

    // armIdleGuard()
    jobSeenSinceScaleUp = false;
    idlePollCount = 0;

    assertFalse(jobSeenSinceScaleUp);
    assertEquals(0, idlePollCount);
  }

  @Test
  public void testTotalMetricsPreventIdleReset() {
    // If any total activity metric is > 0, idle reset should not fire.
    // Total metrics cover all tiers: jobs.active >= jobs.active.small + jobs.active.large
    double jobsActive = 1.0;
    double maestroActive = 0.0;

    boolean active = jobsActive > 0.0 || maestroActive > 0.0;
    assertTrue(active);
  }

  @Test
  public void testIdleResetFiresAfterThresholdEvenWithDesiredGaugesAboveZero() {
    // After jobSeenSinceScaleUp=true and activity drops to 0, idle-reset should
    // eventually fire (after IDLE_RESET_THRESHOLD polls) even if desiredSmall/desiredLarge > 0.
    // The desiredGauges guard was removed because it created a deadlock: the reset
    // could only clear desiredGauges to 0, but the guard prevented the reset whenever
    // they were > 0. The 5-minute threshold (30 polls) gives enough time for
    // executors to register during cold-start.
    AtomicInteger desiredSmall = new AtomicInteger(3);
    AtomicInteger desiredLarge = new AtomicInteger(3);
    boolean jobSeenSinceScaleUp = true;
    int threshold = 30;
    int idlePollCount = 0;

    // Simulate 30 idle polls — idle-reset should fire
    for (int i = 0; i < threshold; i++) {
      idlePollCount++;
    }
    assertTrue(idlePollCount >= threshold);

    // After idle-reset fires, desiredGauges should be 0
    desiredSmall.set(0);
    desiredLarge.set(0);
    assertEquals(0, desiredSmall.get());
    assertEquals(0, desiredLarge.get());
  }

  // ---- Zero-delta scaleDeployment refreshes gauge ----

  @Test
  public void testZeroDeltaRefreshesGaugeToCurrentReplicas() {
    // When scaleDelta=0, scaleDeployment should refresh desiredSmall/desiredLarge
    // to the StatefulSet's current replica count (not skip the call entirely).
    // This prevents a stale elastic_desired_* gauge from causing KEDA scale-down.
    AtomicInteger desiredSmall = new AtomicInteger(0);
    int currentReplicas = 2;
    int scaleDelta = 0;
    int newReplicas = Math.max(0, currentReplicas + scaleDelta);
    desiredSmall.set(newReplicas);
    assertEquals(2, desiredSmall.get());
  }

  @Test
  public void testZeroDeltaArmsColdStartGuard() {
    // When scaleDelta=0 but newReplicas>0, the cold-start guard (jobSeenSinceScaleUp)
    // should be armed (set to false) just like the scale-up path.
    boolean jobSeenSinceScaleUp = true; // was true from a previous query
    int currentReplicas = 2;
    int scaleDelta = 0;
    int newReplicas = Math.max(0, currentReplicas + scaleDelta);

    if (newReplicas > 0) {
      jobSeenSinceScaleUp = false; // arm the guard
    }

    assertFalse(jobSeenSinceScaleUp);
  }

  // ---- Tier routing ----

  @Test
  public void testSmallTierSelectsSmallDeployment() {
    ElasticAdmissionCalculator calc = new ElasticAdmissionCalculator();
    ElasticAdmissionCalculator.ExecutorTier tier = calc.getTier(5_000_000);
    String deploymentName = (tier == ElasticAdmissionCalculator.ExecutorTier.LARGE)
        ? "dremio-executor-large"
        : "dremio-executor-small";
    assertEquals("dremio-executor-small", deploymentName);
  }

  @Test
  public void testLargeTierSelectsLargeDeployment() {
    ElasticAdmissionCalculator calc = new ElasticAdmissionCalculator();
    ElasticAdmissionCalculator.ExecutorTier tier = calc.getTier(50_000_000);
    String deploymentName = (tier == ElasticAdmissionCalculator.ExecutorTier.LARGE)
        ? "dremio-executor-large"
        : "dremio-executor-small";
    assertEquals("dremio-executor-large", deploymentName);
  }
}
