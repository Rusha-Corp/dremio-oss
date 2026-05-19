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
 * AWS EC2 implementation of ResourcePlatform.
 *
 * <p>This implementation manages elastic executors on AWS EC2 instances.
 */
public class AwsEc2Platform implements ResourcePlatform {
  private static final Logger logger = LoggerFactory.getLogger(AwsEc2Platform.class);

  private final String instanceType;
  private final String region;
  private final String amiId;
  private final ListenableSet executorSet;

  public AwsEc2Platform(String instanceType, String region, String amiId) {
    this(instanceType, region, amiId, null);
  }

  public AwsEc2Platform(String instanceType, String region, String amiId, ListenableSet executorSet) {
    this.instanceType = instanceType;
    this.region = region;
    this.amiId = amiId;
    this.executorSet = executorSet;
    logger.info("Initialized AwsEc2Platform: instanceType={}, region={}", instanceType, region);
  }

  @Override
  public int getReadyNodeCount() {
    // TODO: Query AWS EC2 for ready instances
    logger.debug("getReadyNodeCount not implemented for EC2");
    return 0;
  }

  @Override
  public int getReadyPodCount() {
    // In EC2, "pods" are EC2 instances - return node count
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
      logger.info("Scaling up EC2 by {} instances (instanceType={}, region={})",
          scaleDelta, instanceType, region);
      // TODO: Use AWS SDK to launch EC2 instances
      // - Create instances with instanceType
      // - Use amiId for the AMI
      // - Wait for instances to be Running
      // - Register with Dremio coordinator
      logger.warn("AWS EC2 scaling not yet implemented - instances would be launched here");
      return true;
    } else {
      logger.info("Scaling down EC2 by {} instances", -scaleDelta);
      // TODO: Use AWS SDK to terminate EC2 instances
      logger.warn("AWS EC2 scaling not yet implemented - instances would be terminated here");
      return true;
    }
  }
}
