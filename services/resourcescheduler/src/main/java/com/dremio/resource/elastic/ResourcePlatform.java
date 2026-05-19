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

/**
 * Platform-agnostic interface for resource management.
 *
 * <p>This abstraction allows the elastic admission logic to work with different container
 * orchestration platforms (Kubernetes, AWS, GCP, etc.) without coupling to any specific platform.
 */
public interface ResourcePlatform {

  /**
   * Gets the count of ready worker nodes in the cluster.
   *
   * @return number of ready nodes
   */
  int getReadyNodeCount();

  /**
   * Gets the count of ready executor pods.
   *
   * @return number of ready pods
   */
  int getReadyPodCount();

  /**
   * Gets the count of Dremio executors registered and available.
   *
   * @return number of available executors from Dremio Coordinator
   */
  int getAvailableExecutors();

  /**
   * Waits for the required number of executors to be ready.
   *
   * <p>This waits for: nodes ready + pods ready + executors registered
   *
   * @param requiredExecutors number of executors needed
   * @param timeout maximum time to wait
   * @param unit time unit for timeout
   * @return true if required executors are available, false if timeout
   * @throws InterruptedException if thread is interrupted while waiting
   */
  boolean waitForExecutors(int requiredExecutors, long timeout, java.util.concurrent.TimeUnit unit)
      throws InterruptedException;

  /**
   * Scales the executor pods to the specified count.
   *
   * <p>This creates new pods if scaleUp > 0, or deletes pods if scaleDown < 0.
   *
   * @param scaleDelta positive to scale up, negative to scale down
   * @return true if scaling succeeded, false if failed
   */
  boolean scaleExecutors(int scaleDelta);
}
