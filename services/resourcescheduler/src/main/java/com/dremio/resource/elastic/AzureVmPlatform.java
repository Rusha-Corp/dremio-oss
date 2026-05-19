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
 * Azure Virtual Machines implementation of ResourcePlatform.
 *
 * <p>This implementation manages elastic executors on Azure VMs.
 */
public class AzureVmPlatform implements ResourcePlatform {
  private static final Logger logger = LoggerFactory.getLogger(AzureVmPlatform.class);

  private final String instanceType;
  private final String location;
  private final ListenableSet executorSet;

  public AzureVmPlatform(String instanceType, String location) {
    this(instanceType, location, null);
  }

  public AzureVmPlatform(String instanceType, String location, ListenableSet executorSet) {
    this.instanceType = instanceType;
    this.location = location;
    this.executorSet = executorSet;
    logger.info("Initialized AzureVmPlatform: instanceType={}, location={}", instanceType, location);
  }

  @Override
  public int getReadyNodeCount() {
    // TODO: Query Azure for ready VMs
    logger.debug("getReadyNodeCount not implemented for Azure");
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
      logger.info("Scaling up Azure VMs by {} instances (instanceType={}, location={})",
          scaleDelta, instanceType, location);
      // TODO: Use Azure SDK to create VMs
      // - Create VMs with instanceType
      // - Use location for region
      // - Wait for VMs to be Running
      // - Register with Dremio coordinator
      logger.warn("Azure VM scaling not yet implemented - VMs would be created here");
      return true;
    } else {
      logger.info("Scaling down Azure VMs by {} instances", -scaleDelta);
      // TODO: Use Azure SDK to delete VMs
      logger.warn("Azure VM scaling not yet implemented - VMs would be deleted here");
      return true;
    }
  }
}
