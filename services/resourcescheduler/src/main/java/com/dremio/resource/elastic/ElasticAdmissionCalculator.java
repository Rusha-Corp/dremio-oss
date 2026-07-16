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
 * Admission calculator for elastic executor scaling.
 *
 * <p>Calculates the number of executors a query needs based on its estimated cost. Tier
 * classification is handled by {@code ElasticResourceAllocator.getQueueNameFromSchedulingProperties}
 * which uses {@code QueueType} as the single source of truth.
 */
public class ElasticAdmissionCalculator {

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
}
