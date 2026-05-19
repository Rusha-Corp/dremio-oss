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

import static org.junit.jupiter.api.Assertions.*;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Integration test for K8sPlatform with kubeconfig authentication.
 *
 * <p>This test uses kubeconfig to connect to a Kubernetes cluster. The kubeconfig file is typically
 * located at ~/.kube/config or can be specified via KUBECONFIG environment variable.
 */
public class K8sPlatformKubeconfigIntegrationTest {

  private static final Logger logger =
      LoggerFactory.getLogger(K8sPlatformKubeconfigIntegrationTest.class);

  private KubernetesClient k8sClient;
  private Properties testProperties;

  @BeforeEach
  void setUp() throws IOException {
    testProperties = loadEnvProperties();
  }

  @AfterEach
  void tearDown() {
    if (k8sClient != null) {
      k8sClient.close();
    }
  }

  @Test
  void testKubeconfigAuthenticationAndK8sConnection() throws Exception {
    String namespace =
        testProperties != null ? testProperties.getProperty("RUSHA_K8S_NAMESPACE") : null;

    // Use a default namespace if not configured
    if (namespace == null || namespace.isEmpty()) {
      namespace = "default";
    }

    logger.info("Using kubeconfig for K8s authentication");
    logger.info("Target namespace: {}", namespace);

    // Step 1: Create K8s client using kubeconfig
    // The KubernetesClientBuilder automatically reads from:
    // - KUBECONFIG environment variable
    // - ~/.kube/config
    // - In-cluster service account (when running in K8s)
    logger.info("Using kubeconfig (default or KUBECONFIG env var)");
    k8sClient = new KubernetesClientBuilder().build();

    // Step 2: Get the actual server URL being used
    String actualServer = k8sClient.getConfiguration().getMasterUrl();
    logger.info("Connected to K8s cluster at: {}", actualServer);
    assertNotNull(actualServer, "Expected to be connected to a K8s cluster");

    // Step 3: Verify connection by listing namespaces
    var namespaces = k8sClient.namespaces().list().getItems();
    assertNotNull(namespaces, "Failed to list namespaces");
    logger.info("Connected to K8s cluster. Found {} namespaces", namespaces.size());

    // Step 3: Query for pods in the specified namespace
    var pods = k8sClient.pods().inNamespace(namespace).list().getItems();
    logger.info("Found {} pods in namespace '{}'", pods.size(), namespace);

    // Step 4: Check node status
    var nodes = k8sClient.nodes().list().getItems();
    logger.info("Found {} nodes in cluster", nodes.size());

    long readyNodes =
        nodes.stream()
            .filter(n -> n.getStatus() != null && n.getStatus().getConditions() != null)
            .filter(
                n ->
                    n.getStatus().getConditions().stream()
                        .anyMatch(c -> "Ready".equals(c.getType()) && "True".equals(c.getStatus())))
            .count();
    logger.info("Found {} ready nodes", readyNodes);

    // Step 5: Create K8sPlatform and test its methods
    K8sPlatform platform = new K8sPlatform(k8sClient, namespace, "dremio/executor", null);

    int readyNodeCount = platform.getReadyNodeCount();
    logger.info("K8sPlatform.getReadyNodeCount() = {}", readyNodeCount);
    assertTrue(readyNodeCount >= 0, "Expected non-negative ready node count");

    int readyPodCount = platform.getReadyPodCount();
    logger.info("K8sPlatform.getReadyPodCount() = {}", readyPodCount);
    assertTrue(readyPodCount >= 0, "Expected non-negative ready pod count");

    logger.info("K8sPlatform kubeconfig integration test completed successfully!");
  }

  /** Loads environment properties from .env file. */
  private Properties loadEnvProperties() throws IOException {
    Properties props = new Properties();

    // Try multiple possible locations for .env file
    Path[] possiblePaths = {
      Paths.get("/home/dev/dremio-oss/.env"),
      Paths.get(System.getProperty("user.home"), ".env"),
      Paths.get(".env")
    };

    Path envFile = null;
    for (Path path : possiblePaths) {
      if (java.nio.file.Files.exists(path)) {
        envFile = path;
        break;
      }
    }

    if (envFile != null) {
      logger.info("Loading properties from: {}", envFile.toAbsolutePath());
      props.load(java.nio.file.Files.newInputStream(envFile));
    } else {
      logger.warn(".env file not found in any location");
    }

    return props;
  }
}
