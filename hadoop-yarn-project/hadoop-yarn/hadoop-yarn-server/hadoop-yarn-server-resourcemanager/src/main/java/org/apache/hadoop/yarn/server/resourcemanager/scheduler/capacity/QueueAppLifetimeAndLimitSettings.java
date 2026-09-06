/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity;

import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;

/**
 * This class determines application lifetime and max parallel apps settings based on the
 * {@link CapacitySchedulerConfiguration} and other queue
 * properties.
 **/
public class QueueAppLifetimeAndLimitSettings {
  // -1 indicates lifetime is disabled
  private final long maxApplicationLifetime;
  private final long defaultApplicationLifetime;

  // Indicates if this queue's default lifetime was set by a config property,
  // either at this level or anywhere in the queue's hierarchy.
  private boolean defaultAppLifetimeWasSpecifiedInConfig = false;

  private int maxParallelApps;

  public QueueAppLifetimeAndLimitSettings(CapacitySchedulerConfiguration configuration,
      AbstractCSQueue q, QueuePath queuePath) {
    // Store max parallel apps property
    this.maxParallelApps = configuration.getMaxParallelAppsForQueue(queuePath);
    long configuredMaximum = configuration.getMaximumLifetimePerQueue(queuePath);
    long configuredDefault = configuration.getDefaultLifetimePerQueue(queuePath);
    LifetimeSettings resolved = resolve(queuePath.isRoot(), configuredMaximum,
        configuredDefault,
        q.getParent() == null ? -1 : q.getParent().getMaximumApplicationLifetime(),
        q.getParent() == null ? -1 : q.getParent().getDefaultApplicationLifetime(),
        q.getParent() != null
            && q.getParent().getDefaultAppLifetimeWasSpecifiedInConfig());
    this.maxApplicationLifetime = resolved.maximumLifetime();
    this.defaultApplicationLifetime = resolved.defaultLifetime();
    this.defaultAppLifetimeWasSpecifiedInConfig =
        resolved.defaultLifetimeConfigured();
  }

  /** Immutable application-lifetime inheritance result. */
  public record LifetimeSettings(long maximumLifetime, long defaultLifetime,
      boolean defaultLifetimeConfigured) {
  }

  /** Resolves application lifetimes without consulting a live queue. */
  public static LifetimeSettings resolve(boolean root, long configuredMaximum,
      long configuredDefault, long parentMaximum, long parentDefault,
      boolean parentDefaultConfigured) {
    long maximum = root || configuredMaximum >= 0
        ? configuredMaximum : parentMaximum;
    boolean defaultConfigured = configuredDefault >= 0
        || !root && parentDefaultConfigured;
    long defaultLifetime = configuredDefault;
    if (!root && defaultLifetime < 0) {
      defaultLifetime = defaultConfigured
          ? Math.min(parentDefault, maximum) : maximum;
    }
    if (!root && maximum > 0 && defaultLifetime > maximum) {
      throw new YarnRuntimeException(
          "Default lifetime " + defaultLifetime
              + " can't exceed maximum lifetime " + maximum);
    }
    if (!root && defaultLifetime <= 0) {
      defaultLifetime = maximum;
    }
    return new LifetimeSettings(maximum, defaultLifetime, defaultConfigured);
  }

  public int getMaxParallelApps() {
    return maxParallelApps;
  }

  public void setMaxParallelApps(int maxParallelApps) {
    this.maxParallelApps = maxParallelApps;
  }

  public long getMaxApplicationLifetime() {
    return maxApplicationLifetime;
  }

  public long getDefaultApplicationLifetime() {
    return defaultApplicationLifetime;
  }

  public boolean isDefaultAppLifetimeWasSpecifiedInConfig() {
    return defaultAppLifetimeWasSpecifiedInConfig;
  }
}
