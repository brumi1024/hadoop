/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.plan;

import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.plan.CompiledQueueSettings.ResourceSetting;

/** Immutable scheduler-wide values consumed by queue construction. */
public final class CompiledSchedulerSettings {
  private final ResourceSetting minimumAllocation;
  private final ResourceSetting maximumAllocation;
  private final boolean reservationsContinueLooking;
  private final int nodeLocalityDelay;
  private final int rackLocalityAdditionalDelay;
  private final boolean rackLocalityFullReset;

  CompiledSchedulerSettings(ResourceSetting minimumAllocation,
      ResourceSetting maximumAllocation,
      boolean reservationsContinueLooking, int nodeLocalityDelay,
      int rackLocalityAdditionalDelay, boolean rackLocalityFullReset) {
    this.minimumAllocation = minimumAllocation;
    this.maximumAllocation = maximumAllocation;
    this.reservationsContinueLooking = reservationsContinueLooking;
    this.nodeLocalityDelay = nodeLocalityDelay;
    this.rackLocalityAdditionalDelay = rackLocalityAdditionalDelay;
    this.rackLocalityFullReset = rackLocalityFullReset;
  }

  public ResourceSetting getMinimumAllocation() {
    return minimumAllocation;
  }

  public ResourceSetting getMaximumAllocation() {
    return maximumAllocation;
  }

  public boolean isReservationsContinueLooking() {
    return reservationsContinueLooking;
  }

  public int getNodeLocalityDelay() {
    return nodeLocalityDelay;
  }

  public int getRackLocalityAdditionalDelay() {
    return rackLocalityAdditionalDelay;
  }

  public boolean isRackLocalityFullReset() {
    return rackLocalityFullReset;
  }
}
