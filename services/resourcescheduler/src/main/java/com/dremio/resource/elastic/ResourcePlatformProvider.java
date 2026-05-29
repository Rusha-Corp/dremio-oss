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
import java.io.Closeable;
import java.io.IOException;
import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provider for ResourcePlatform that creates a K8sPlatform when elastic scaling is enabled.
 *
 * <p>The resolved platform is lazily initialized and cached on first access. Implements Closeable
 * to clean up the KubernetesClient on coordinator shutdown.
 */
@Singleton
public class ResourcePlatformProvider implements Provider<ResourcePlatform>, Closeable {

  private static final Logger logger = LoggerFactory.getLogger(ResourcePlatformProvider.class);
  private static final String DEFAULT_DEPLOYMENT_NAME = "dremio-executor";

  private final DremioConfig config;
  private final Provider<ClusterCoordinator> clusterCoordinatorProvider;

  private volatile ResourcePlatform cachedPlatform;
  private final Object lock = new Object();

  @Inject
  public ResourcePlatformProvider(
      DremioConfig config, Provider<ClusterCoordinator> clusterCoordinatorProvider) {
    this.config = config;
    this.clusterCoordinatorProvider = clusterCoordinatorProvider;
  }

  @Override
  public ResourcePlatform get() {
    ResourcePlatform platform = cachedPlatform;
    if (platform != null) {
      return platform;
    }

    synchronized (lock) {
      platform = cachedPlatform;
      if (platform != null) {
        return platform;
      }

      platform = createPlatform();
      cachedPlatform = platform;
      return platform;
    }
  }

  private ResourcePlatform createPlatform() {
    boolean elasticEnabled = config.getBoolean(DremioConfig.ELASTIC_ENABLED);
    if (!elasticEnabled) {
      logger.debug("Elastic scaling disabled, using NoOpResourcePlatform");
      return NoOpResourcePlatform.INSTANCE;
    }

    return createKubernetesPlatform();
  }

  private ResourcePlatform createKubernetesPlatform() {
    String namespace = config.getString(DremioConfig.ELASTIC_K8S_NAMESPACE);

    if (Strings.isNullOrEmpty(namespace)) {
      throw new IllegalStateException(
          "Elastic executor is enabled but "
              + "services.executor.elastic.kubernetes.namespace is not configured. "
              + "Please set the Kubernetes namespace in dremio.conf");
    }

    try {
      KubernetesClient k8sClient = new KubernetesClientBuilder().build();

      // Test connection by listing pods in the target namespace
      k8sClient.pods().inNamespace(namespace).list();

      ListenableSet executorSet =
          clusterCoordinatorProvider.get().getServiceSet(ClusterCoordinator.Role.EXECUTOR);

      String deploymentNameSmall = config.getString(DremioConfig.ELASTIC_K8S_POD_TEMPLATE);
      if (Strings.isNullOrEmpty(deploymentNameSmall)) {
        deploymentNameSmall = DEFAULT_DEPLOYMENT_NAME;
      }

      String deploymentNameLarge;
      if (config.hasPath(DremioConfig.ELASTIC_K8S_POD_TEMPLATE_LARGE)) {
        deploymentNameLarge = config.getString(DremioConfig.ELASTIC_K8S_POD_TEMPLATE_LARGE);
      } else {
        deploymentNameLarge = "";
      }
      if (Strings.isNullOrEmpty(deploymentNameLarge)) {
        deploymentNameLarge = deploymentNameSmall + "-large";
      }

      int maxExecutorsSmall = config.getInt(DremioConfig.ELASTIC_MAX_EXECUTORS);
      int maxExecutorsLarge = config.getInt(DremioConfig.ELASTIC_MAX_EXECUTORS_LARGE);

      logger.info(
          "Creating K8sPlatform for namespace: {}, small deployment: {}, large deployment: {}, "
              + "maxExecutorsSmall: {}, maxExecutorsLarge: {}",
          namespace,
          deploymentNameSmall,
          deploymentNameLarge,
          maxExecutorsSmall,
          maxExecutorsLarge);
      return new K8sPlatform(
          k8sClient,
          namespace,
          deploymentNameSmall,
          deploymentNameLarge,
          executorSet,
          maxExecutorsSmall,
          maxExecutorsLarge);
    } catch (Exception e) {
      throw new IllegalStateException(
          "Failed to connect to Kubernetes for elastic executor scaling: " + e.getMessage(), e);
    }
  }

  @Override
  public void close() throws IOException {
    ResourcePlatform platform = cachedPlatform;
    if (platform instanceof Closeable) {
      try {
        ((Closeable) platform).close();
      } catch (IOException e) {
        logger.warn("Error closing ResourcePlatform", e);
      }
    }
  }
}
