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

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.dremio.config.DremioConfig;
import com.dremio.service.coordinator.ClusterCoordinator;
import javax.inject.Provider;
import org.junit.Test;

/** Tests for ResourcePlatformProvider — lazy initialization and NoOp fallback. */
public class ResourcePlatformProviderTest {

  @Test
  public void testReturnsNoOpPlatformWhenElasticDisabled() {
    DremioConfig config =
        DremioConfig.create().withValue(DremioConfig.ELASTIC_ENABLED, false);

    Provider<ClusterCoordinator> ccProvider = mock(Provider.class);

    ResourcePlatformProvider provider = new ResourcePlatformProvider(config, ccProvider);

    ResourcePlatform platform = provider.get();
    assertNotNull(platform);
    assertTrue(platform instanceof NoOpResourcePlatform);
  }

  @Test
  public void testCachesPlatformInstance() {
    DremioConfig config =
        DremioConfig.create().withValue(DremioConfig.ELASTIC_ENABLED, false);

    Provider<ClusterCoordinator> ccProvider = mock(Provider.class);

    ResourcePlatformProvider provider = new ResourcePlatformProvider(config, ccProvider);

    ResourcePlatform first = provider.get();
    ResourcePlatform second = provider.get();
    assertSame(first, second);
  }

  @Test
  public void testThrowsIllegalStateWhenElasticEnabledButNoNamespace() {
    DremioConfig config =
        DremioConfig.create()
            .withValue(DremioConfig.ELASTIC_ENABLED, true)
            .withValue(DremioConfig.ELASTIC_K8S_NAMESPACE, "");

    Provider<ClusterCoordinator> ccProvider = mock(Provider.class);

    ResourcePlatformProvider provider = new ResourcePlatformProvider(config, ccProvider);

    try {
      provider.get();
      assertTrue("Expected IllegalStateException", false);
    } catch (IllegalStateException e) {
      assertTrue(e.getMessage().contains("namespace"));
    }
  }

  @Test
  public void testCloseOnNoOpPlatform() throws Exception {
    DremioConfig config =
        DremioConfig.create().withValue(DremioConfig.ELASTIC_ENABLED, false);

    Provider<ClusterCoordinator> ccProvider = mock(Provider.class);

    ResourcePlatformProvider provider = new ResourcePlatformProvider(config, ccProvider);
    provider.get(); // initialize

    // Closing should not throw
    provider.close();
  }

  @Test
  public void testNoOpPlatformArmIdleGuardIsNoOp() {
    // armIdleGuard() on NoOpResourcePlatform should be a safe no-op
    ResourcePlatform platform = NoOpResourcePlatform.INSTANCE;
    platform.armIdleGuard(); // should not throw
  }
}
