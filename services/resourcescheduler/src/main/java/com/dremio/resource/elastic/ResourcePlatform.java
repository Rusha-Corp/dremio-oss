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

import java.util.concurrent.TimeUnit;

/**
 * Platform-agnostic interface for elastic executor scaling.
 *
 * <p>Implementations manage executor lifecycle on a specific platform (e.g., Kubernetes). The
 * interface is intentionally minimal: it exposes observation (pod count, executor count), scaling
 * (scale up/down), and synchronization (wait for readiness).
 */
public interface ResourcePlatform extends AutoCloseable {

  /**
   * Gets the count of Dremio executors registered and available.
   *
   * @return number of available executors from Dremio Coordinator
   */
  int getAvailableExecutors();

  /**
   * Waits for the required number of executors to be registered.
   *
   * <p>This waits for Dremio executors to register with the coordinator via ZooKeeper.
   *
   * @param requiredExecutors number of executors needed
   * @param timeout maximum time to wait
   * @param unit time unit for timeout
   * @return true if required executors are available, false if timeout
   * @throws InterruptedException if thread is interrupted while waiting
   */
  boolean waitForExecutors(int requiredExecutors, long timeout, TimeUnit unit)
      throws InterruptedException;

  /**
   * Scales the executor count by the given delta for a specific tier.
   *
   * <p>This allows implementations to route scaling requests to the correct Deployment (e.g.,
   * dremio-executor-small vs dremio-executor-large).
   *
   * @param scaleDelta positive to scale up, negative to scale down
   * @param tier the executor tier (SMALL or LARGE)
   * @return true if scaling succeeded, false if failed
   */
  boolean scaleExecutors(int scaleDelta, ElasticAdmissionCalculator.ExecutorTier tier);

  /**
   * Gets the count of available executors for a specific tier.
   *
   * <p>Defaults to the tier-blind count for backward compatibility.
   *
   * @param tier the executor tier (SMALL or LARGE)
   * @return number of available executors for that tier
   */
  default int getAvailableExecutors(ElasticAdmissionCalculator.ExecutorTier tier) {
    return getAvailableExecutors();
  }

  /**
   * Waits for the required number of executors of a specific tier to be ready.
   *
   * <p>Defaults to the tier-blind wait for backward compatibility.
   *
   * @param requiredExecutors number of executors needed
   * @param tier the executor tier (SMALL or LARGE)
   * @param timeout maximum time to wait
   * @param unit time unit for timeout
   * @return true if required executors are available, false if timeout
   * @throws InterruptedException if thread is interrupted while waiting
   */
  default boolean waitForExecutors(
      int requiredExecutors,
      ElasticAdmissionCalculator.ExecutorTier tier,
      long timeout,
      TimeUnit unit)
      throws InterruptedException {
    return waitForExecutors(requiredExecutors, timeout, unit);
  }

  /**
   * Arms the idle-reset guard to prevent premature scale-down during executor cold-start.
   *
   * <p>Call this before any scale-up request to ensure the idle-reset scheduler does not clear the
   * {@code elastic_desired_*} gauges while executors are still starting up. The guard is
   * automatically disarmed when {@code jobs_active > 0 || maestro_active > 0} is observed.
   *
   * <p>Default no-op for implementations that do not use an idle-reset scheduler.
   */
  default void armIdleGuard() {}

  @Override
  default void close() throws Exception {}
}
