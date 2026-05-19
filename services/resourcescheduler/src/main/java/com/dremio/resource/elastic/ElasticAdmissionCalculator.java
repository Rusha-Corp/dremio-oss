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
 * Platform-agnostic admission calculator for elastic executor scaling.
 *
 * <p>This class contains the core admission logic that is independent of any specific platform
 * (Kubernetes, AWS, GCP, etc.). Platform-specific implementations are provided by {@link
 * ResourcePlatform}.
 */
public class ElasticAdmissionCalculator {

  /** Small query threshold - queries below this can share executors. */
  private final double smallQueryThreshold;

  /** Medium query threshold - queries above this need dedicated capacity. */
  private final double mediumQueryThreshold;

  /** Default constructor with standard thresholds. */
  public ElasticAdmissionCalculator() {
    this(10_000_000, 30_000_000);
  }

  /**
   * Constructor with custom thresholds.
   *
   * @param smallQueryThreshold threshold for small queries
   * @param mediumQueryThreshold threshold for medium queries
   */
  public ElasticAdmissionCalculator(double smallQueryThreshold, double mediumQueryThreshold) {
    this.smallQueryThreshold = smallQueryThreshold;
    this.mediumQueryThreshold = mediumQueryThreshold;
  }

  /**
   * Calculates the number of executors required to run a query.
   *
   * @param planCost the estimated query cost from PhysicalPlan.getCost()
   * @param avgExecutorMemoryBytes average memory per executor (unused for now, reserved for future
   *     refinement)
   * @return number of executors needed
   */
  public int calculateRequiredExecutors(double planCost, long avgExecutorMemoryBytes) {
    if (planCost <= smallQueryThreshold) {
      return 1;
    }
    if (planCost <= mediumQueryThreshold) {
      return 2;
    }
    return 3;
  }

  /**
   * Calculates how many additional executors need to be scaled up.
   *
   * @param requiredExecutors total executors needed
   * @param currentExecutors currently available executors
   * @return number of executors to add (0 if already sufficient)
   */
  public int calculateScaleDelta(int requiredExecutors, int currentExecutors) {
    return Math.max(0, requiredExecutors - currentExecutors);
  }

  /**
   * Calculates the number of nodes needed to run the requested executors.
   *
   * <p>This is platform-agnostic - it simply calculates how many nodes are needed based on memory.
   * The actual node provisioning is handled by the {@link ResourcePlatform} implementation.
   *
   * @param executors number of executors needed
   * @param nodeMemoryBytes total memory per node (e.g., 32GB)
   * @param executorMemoryBytes memory needed per executor (e.g., 8GB)
   * @return number of nodes required
   */
  public int calculateRequiredNodes(int executors, long nodeMemoryBytes, long executorMemoryBytes) {
    if (executors <= 0) {
      return 0;
    }
    return (int) Math.ceil((double) executors * executorMemoryBytes / nodeMemoryBytes);
  }
}
