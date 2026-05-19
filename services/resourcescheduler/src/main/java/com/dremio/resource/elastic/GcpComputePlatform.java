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

import com.dremio.service.coordinator.ListenableSet;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * GCP Compute Engine implementation of ResourcePlatform.
 *
 * <p>This implementation manages elastic executors on GCP Compute Engine instances.
 */
public class GcpComputePlatform implements ResourcePlatform {
  private static final Logger logger = LoggerFactory.getLogger(GcpComputePlatform.class);

  private final String instanceType;
  private final String zone;
  private final String projectId;
  private final ListenableSet executorSet;

  public GcpComputePlatform(String instanceType, String zone, String projectId) {
    this(instanceType, zone, projectId, null);
  }

  public GcpComputePlatform(String instanceType, String zone, String projectId, ListenableSet executorSet) {
    this.instanceType = instanceType;
    this.zone = zone;
    this.projectId = projectId;
    this.executorSet = executorSet;
    logger.info("Initialized GcpComputePlatform: instanceType={}, zone={}", instanceType, zone);
  }

  @Override
  public int getReadyNodeCount() {
    // TODO: Query GCP for ready instances
    logger.debug("getReadyNodeCount not implemented for GCP");
    return 0;
  }

  @Override
  public int getReadyPodCount() {
    return getReadyNodeCount();
  }

  @Override
  public int getAvailableExecutors() {
    if (executorSet == null) {
      return 0;
    }
    return executorSet.getAvailableEndpoints().size();
  }

  @Override
  public boolean waitForExecutors(int requiredExecutors, long timeout, TimeUnit unit)
      throws InterruptedException {
    long deadline = System.currentTimeMillis() + unit.toMillis(timeout);
    while (System.currentTimeMillis() < deadline) {
      if (getAvailableExecutors() >= requiredExecutors) {
        return true;
      }
      Thread.sleep(2000);
    }
    return false;
  }

  @Override
  public boolean scaleExecutors(int scaleDelta) {
    if (scaleDelta == 0) {
      return true;
    }

    if (scaleDelta > 0) {
      logger.info("Scaling up GCP Compute by {} instances (instanceType={}, zone={})",
          scaleDelta, instanceType, zone);
      // TODO: Use GCP SDK to create Compute Engine instances
      // - Create instances with machineType
      // - Use zone for location
      // - Wait for instances to be RUNNING
      // - Register with Dremio coordinator
      logger.warn("GCP Compute scaling not yet implemented - instances would be created here");
      return true;
    } else {
      logger.info("Scaling down GCP Compute by {} instances", -scaleDelta);
      // TODO: Use GCP SDK to delete Compute Engine instances
      logger.warn("GCP Compute scaling not yet implemented - instances would be deleted here");
      return true;
    }
  }
}
