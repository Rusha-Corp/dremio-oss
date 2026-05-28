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

import java.util.concurrent.TimeUnit;

/**
 * No-op ResourcePlatform implementation used when elastic scaling is disabled.
 *
 * <p>Returns 0 for all counts and false for wait/scale operations.
 */
public final class NoOpResourcePlatform implements ResourcePlatform {

  public static final NoOpResourcePlatform INSTANCE = new NoOpResourcePlatform();

  private NoOpResourcePlatform() {}

  @Override
  public int getReadyPodCount() {
    return 0;
  }

  @Override
  public int getAvailableExecutors() {
    return 0;
  }

  @Override
  public boolean waitForExecutors(int requiredExecutors, long timeout, TimeUnit unit) {
    return false;
  }

  @Override
  public boolean scaleExecutors(int scaleDelta) {
    return false;
  }
}
