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
import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import io.fabric8.kubernetes.api.model.ContainerBuilder;
import io.fabric8.kubernetes.api.model.Node;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodBuilder;
import io.fabric8.kubernetes.api.model.VolumeBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Kubernetes implementation of ResourcePlatform.
 *
 * <p>This implementation uses the Kubernetes API to manage elastic resources. It can be replaced
 * with other platform implementations (AWS, GCP, etc.) as needed.
 */
public class K8sPlatform implements ResourcePlatform {
  private static final Logger logger = LoggerFactory.getLogger(K8sPlatform.class);

  private final KubernetesClient k8sClient;
  private final String namespace;
  private final String executorLabel;
  private final String image;
  private final String zookeeperAddress;
  private final ListenableSet executorSet;

  /**
   * Creates a K8sPlatform with the given configuration.
   *
   * @param k8sClient the Kubernetes client
   * @param namespace the namespace to search for pods
   * @param executorLabel the label selector for executor pods
   * @param image the Docker image to use for executor pods
   * @param zookeeperAddress the ZK address to connect to coordinator
   * @param executorSet the Dremio executor set from Coordinator
   */
  public K8sPlatform(
      KubernetesClient k8sClient,
      String namespace,
      String executorLabel,
      String image,
      String zookeeperAddress,
      ListenableSet executorSet) {
    this.k8sClient = k8sClient;
    this.namespace = namespace;
    this.executorLabel = executorLabel;
    this.image = image;
    this.zookeeperAddress = zookeeperAddress;
    this.executorSet = executorSet;
  }

  @Override
  public int getReadyNodeCount() {
    return (int) k8sClient.nodes().list().getItems().stream().filter(this::isReadyNode).count();
  }

  @Override
  public int getReadyPodCount() {
    return (int)
        k8sClient.pods().inNamespace(namespace).withLabel(executorLabel).list().getItems().stream()
            .filter(this::isReadyPod)
            .count();
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
      // Check all three conditions: nodes, pods, Dremio registration
      if (getReadyNodeCount() > 0
          && getReadyPodCount() >= requiredExecutors
          && getAvailableExecutors() >= requiredExecutors) {
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
      // Scale up: create new executor pods
      logger.info("Scaling up by {} executors in namespace {}", scaleDelta, namespace);
      try {
        // First create a ConfigMap with the executor config
        String configMapName = "dremio-executor-config-" + System.currentTimeMillis();
        String dremioConfig = "paths: {\n"
            + "  local: \"/opt/dremio/data\"\n"
            + "  dist: \"file:///opt/dremio/data/dist\"\n"
            + "}\n"
            + "services: {\n"
            + "  coordinator.enabled: false\n"
            + "  coordinator.master.enabled: false\n"
            + "  coordinator.address: \"dremio-coordinator.dremio.svc.cluster.local\"\n"
            + "  coordinator.port: 45678\n"
            + "  executor.enabled: true\n"
            + "  executor.cache.enabled: true\n"
            + "}\n"
            + "zookeeper: \"" + zookeeperAddress + "\"\n";

        ConfigMap configMap = new ConfigMapBuilder()
            .withNewMetadata()
            .withName(configMapName)
            .endMetadata()
            .addToData("dremio.conf", dremioConfig)
            .build();
        k8sClient.configMaps().inNamespace(namespace).resource(configMap).create();
        logger.info("Created ConfigMap: {}", configMapName);

        for (int i = 0; i < scaleDelta; i++) {
          String podName = "dremio-executor-" + System.currentTimeMillis() + "-" + i;
          Pod pod = new PodBuilder()
              .withNewMetadata()
              .withName(podName)
              .addToLabels("app", "dremio")
              .addToLabels("role", "executor")
              .addToLabels("dremio", executorLabel)
              .endMetadata()
              .withNewSpec()
              .addNewHostAlias()
              .withHostnames("dremio-coordinator-0")
              .withIp("dremio-coordinator.dremio.svc.cluster.local")
              .endHostAlias()
              .addNewContainer()
              .withName("executor")
              .withImage(image)
              .addNewVolumeMount()
              .withName("dremio-config")
              .withMountPath("/opt/dremio/conf")
              .endVolumeMount()
              .addNewEnv()
              .withName("DREMIO_CONF_DIR")
              .withValue("/opt/dremio/conf")
              .endEnv()
              .endContainer()
              .addNewVolume()
              .withName("dremio-config")
              .withNewConfigMap()
              .withName(configMapName)
              .endConfigMap()
              .endVolume()
              .addNewImagePullSecret()
              .withName("ecr-secret")
              .endImagePullSecret()
              .endSpec()
              .build();

          k8sClient.pods().inNamespace(namespace).resource(pod).create();
          logger.info("Created pod: {}", podName);
        }
        return true;
      } catch (Exception e) {
        logger.error("Failed to scale up executors: {}", e.getMessage(), e);
        return false;
      }
    } else {
      // Scale down: delete executor pods
      int scaleDown = -scaleDelta;
      logger.info("Scaling down by {} executors in namespace {}", scaleDown, namespace);
      try {
        List<Pod> executorPods = k8sClient.pods()
            .inNamespace(namespace)
            .withLabel(executorLabel)
            .list()
            .getItems();

        int deleted = 0;
        for (Pod pod : executorPods) {
          if (deleted >= scaleDown) {
            break;
          }
          String podName = pod.getMetadata().getName();
          k8sClient.pods().inNamespace(namespace).withName(podName).delete();
          logger.info("Deleted pod: {}", podName);
          deleted++;
        }
        return true;
      } catch (Exception e) {
        logger.error("Failed to scale down executors: {}", e.getMessage(), e);
        return false;
      }
    }
  }

  /**
   * Checks if a node is in Ready condition.
   *
   * @param node the node to check
   * @return true if node is ready
   */
  private boolean isReadyNode(Node node) {
    if (node.getStatus() == null || node.getStatus().getConditions() == null) {
      return false;
    }
    return node.getStatus().getConditions().stream()
        .anyMatch(c -> "Ready".equals(c.getType()) && "True".equals(c.getStatus()));
  }

  /**
   * Checks if a pod is Running and all its containers are Ready.
   *
   * @param pod the pod to check
   * @return true if pod is ready
   */
  private boolean isReadyPod(Pod pod) {
    if (pod.getStatus() == null) {
      return false;
    }

    String phase = pod.getStatus().getPhase();
    if (!"Running".equals(phase)) {
      return false;
    }

    if (pod.getStatus().getContainerStatuses() == null) {
      return false;
    }

    return pod.getStatus().getContainerStatuses().stream()
        .allMatch(status -> Boolean.TRUE.equals(status.getReady()));
  }
}
