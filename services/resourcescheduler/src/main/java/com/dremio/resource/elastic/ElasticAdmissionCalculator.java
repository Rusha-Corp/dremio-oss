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
 * <p>This class contains the core admission logic that determines how many executors a query needs
 * based on its estimated cost.
 */
public class ElasticAdmissionCalculator {

  /** Executor tier assignment based on query cost. */
  public enum ExecutorTier {
    SMALL,
    LARGE
  }

  private final double smallQueryThreshold;
  private final double mediumQueryThreshold;

  public ElasticAdmissionCalculator() {
    this(10_000_000, 30_000_000);
  }

  public ElasticAdmissionCalculator(double smallQueryThreshold, double mediumQueryThreshold) {
    this.smallQueryThreshold = smallQueryThreshold;
    this.mediumQueryThreshold = mediumQueryThreshold;
  }

  /**
   * Calculates the number of executors required to run a query.
   *
   * @param planCost the estimated query cost from PhysicalPlan.getCost()
   * @return number of executors needed
   */
  public int calculateRequiredExecutors(double planCost) {
    if (planCost <= smallQueryThreshold) {
      return 1;
    }
    if (planCost <= mediumQueryThreshold) {
      return 2;
    }
    return 3;
  }

  /**
   * Returns the executor tier for a given query cost.
   *
   * @param planCost the estimated query cost from PhysicalPlan.getCost()
   * @return ExecutorTier (SMALL for cost &le; threshold, LARGE otherwise)
   */
  public ExecutorTier getTier(double planCost) {
    return planCost <= smallQueryThreshold ? ExecutorTier.SMALL : ExecutorTier.LARGE;
  }

  /**
   * Returns the executor tier for a given query cost and routing queue. If the routing queue
   * contains "large" (case-insensitive), returns LARGE regardless of cost.
   *
   * @param planCost the estimated query cost from PhysicalPlan.getCost()
   * @param routingQueue the routing queue name (e.g. "query.large", "query.small")
   * @return ExecutorTier (LARGE if queue contains "large", otherwise based on cost)
   */
  public ExecutorTier getTier(double planCost, String routingQueue) {
    if (routingQueue != null && routingQueue.toLowerCase().contains("large")) {
      return ExecutorTier.LARGE;
    }
    return getTier(planCost);
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
}
