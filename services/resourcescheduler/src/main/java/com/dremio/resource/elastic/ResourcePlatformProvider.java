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

import com.dremio.config.DremioConfig;
import com.dremio.service.coordinator.ClusterCoordinator;
import com.dremio.service.coordinator.ListenableSet;
import com.google.common.base.Strings;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;
import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provider for ResourcePlatform that returns the appropriate implementation based on configuration.
 *
 * <p>When elastic scaling is enabled, this provider requires a valid platform to be configured.
 * It will throw an error if elastic is enabled but no valid platform is configured.
 */
@Singleton
public class ResourcePlatformProvider implements Provider<ResourcePlatform> {

  private static final Logger logger = LoggerFactory.getLogger(ResourcePlatformProvider.class);

  private final DremioConfig config;
  private final Provider<ClusterCoordinator> clusterCoordinatorProvider;

  @Inject
  public ResourcePlatformProvider(
      DremioConfig config, Provider<ClusterCoordinator> clusterCoordinatorProvider) {
    this.config = config;
    this.clusterCoordinatorProvider = clusterCoordinatorProvider;
  }

  @Override
  public ResourcePlatform get() {
    // Check if elastic is enabled
    boolean elasticEnabled = config.getBoolean(DremioConfig.ELASTIC_ENABLED);
    if (!elasticEnabled) {
      logger.debug("Elastic scaling disabled, using NoOpResourcePlatform");
      return NoOpResourcePlatform.INSTANCE;
    }

    // Elastic is enabled - require platform configuration
    String platform = config.getString(DremioConfig.ELASTIC_PLATFORM);
    if (Strings.isNullOrEmpty(platform)) {
      throw new IllegalStateException(
          "Elastic executor is enabled but services.executor.elastic.platform is not configured. "
              + "Please set platform type (e.g., 'kubernetes', 'aws-ec2', 'gcp-compute', 'azure-vm') in dremio.conf");
    }

    // Create platform based on type
    switch (platform.toLowerCase()) {
      case "kubernetes":
        return createKubernetesPlatform();
      case "aws-ec2":
        return createAwsEc2Platform();
      case "gcp-compute":
        return createGcpComputePlatform();
      case "azure-vm":
        return createAzureVmPlatform();
      default:
        throw new IllegalStateException(
            "Unknown elastic platform: " + platform
                + ". Supported platforms: kubernetes, aws-ec2, gcp-compute, azure-vm");
    }
  }

  private ResourcePlatform createKubernetesPlatform() {
    String namespace = config.getString(DremioConfig.ELASTIC_K8S_NAMESPACE);
    String podTemplate = config.getString(DremioConfig.ELASTIC_K8S_POD_TEMPLATE);

    if (Strings.isNullOrEmpty(namespace)) {
      throw new IllegalStateException(
          "Elastic executor platform is 'kubernetes' but "
              + "services.executor.elastic.kubernetes.namespace is not configured. "
              + "Please set the Kubernetes namespace in dremio.conf");
    }

    try {
      KubernetesClient k8sClient = new KubernetesClientBuilder().build();

      // Test connection by getting namespaces (simple API call)
      k8sClient.namespaces().list();

      // Get executor set from coordinator
      ListenableSet executorSet =
          clusterCoordinatorProvider.get().getServiceSet(ClusterCoordinator.Role.EXECUTOR);
      String labelSelector = Strings.isNullOrEmpty(podTemplate) ? "dremio-executor" : podTemplate;
      String image = config.getString(DremioConfig.ELASTIC_K8S_IMAGE);
      if (Strings.isNullOrEmpty(image)) {
        image = "dremio/dremio:latest";
      }
      String zookeeperAddress = config.getString(DremioConfig.ELASTIC_K8S_ZOOKEEPER_ADDRESS);
      logger.info("Creating K8sPlatform for namespace: {} with image: {} and ZK: {}", namespace, image, zookeeperAddress);
      return new K8sPlatform(k8sClient, namespace, labelSelector, image, zookeeperAddress, executorSet);
    } catch (Exception e) {
      throw new IllegalStateException(
          "Failed to connect to Kubernetes for elastic executor scaling: " + e.getMessage(), e);
    }
  }

  private ResourcePlatform createAwsEc2Platform() {
    String instanceType = config.getString(DremioConfig.ELASTIC_AWS_EC2_INSTANCE_TYPE);
    String region = config.getString(DremioConfig.ELASTIC_AWS_EC2_REGION);
    String amiId = config.getString(DremioConfig.ELASTIC_AWS_EC2_AMI_ID);

    if (Strings.isNullOrEmpty(instanceType)) {
      throw new IllegalStateException(
          "Elastic executor platform is 'aws-ec2' but "
              + "services.executor.elastic.aws-ec2.instance_type is not configured. "
              + "Please set the EC2 instance type in dremio.conf");
    }

    logger.info("Creating AwsEc2Platform for region: {}, instance type: {}", region, instanceType);
    return new AwsEc2Platform(instanceType, region, amiId);
  }

  private ResourcePlatform createGcpComputePlatform() {
    String instanceType = config.getString(DremioConfig.ELASTIC_GCP_INSTANCE_TYPE);
    String zone = config.getString(DremioConfig.ELASTIC_GCP_ZONE);
    String projectId = config.getString(DremioConfig.ELASTIC_GCP_PROJECT_ID);

    if (Strings.isNullOrEmpty(instanceType)) {
      throw new IllegalStateException(
          "Elastic executor platform is 'gcp-compute' but "
              + "services.executor.elastic.gcp-compute.instance_type is not configured. "
              + "Please set the GCP instance type in dremio.conf");
    }

    logger.info("Creating GcpComputePlatform for zone: {}, instance type: {}", zone, instanceType);
    return new GcpComputePlatform(instanceType, zone, projectId);
  }

  private ResourcePlatform createAzureVmPlatform() {
    String instanceType = config.getString(DremioConfig.ELASTIC_AZURE_VM_INSTANCE_TYPE);
    String location = config.getString(DremioConfig.ELASTIC_AZURE_LOCATION);

    if (Strings.isNullOrEmpty(instanceType)) {
      throw new IllegalStateException(
          "Elastic executor platform is 'azure-vm' but "
              + "services.executor.elastic.azure-vm.instance_type is not configured. "
              + "Please set the Azure VM instance type in dremio.conf");
    }

    logger.info("Creating AzureVmPlatform for location: {}, instance type: {}", location, instanceType);
    return new AzureVmPlatform(instanceType, location);
  }
}
