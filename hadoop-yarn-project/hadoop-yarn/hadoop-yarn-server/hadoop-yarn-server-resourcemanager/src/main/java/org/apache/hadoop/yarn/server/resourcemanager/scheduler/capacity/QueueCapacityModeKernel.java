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
package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.classification.InterfaceAudience;

/** Shared pure legacy queue-capacity mode classification. */
@InterfaceAudience.Private
public final class QueueCapacityModeKernel {
  /** One queue/label's explicitly configured capacity modes. */
  public record ModeUse(String queuePath, String label, boolean percentage,
      boolean weight, boolean absolute) {
  }

  private QueueCapacityModeKernel() {
  }

  /**
   * Resolves the constructor capacity type, distinct from sibling mode.
   * Weights and mixed vectors use the percentage constructor type.
   * @param legacy whether legacy capacity mode is enabled
   * @param absoluteSyntax whether the legacy absolute pattern matches
   * @param types normalized vector types, needed only outside legacy mode
   * @return the type used by queue construction and managed templates
   */
  public static AbstractCSQueue.CapacityConfigType constructorCapacityType(
      boolean legacy, boolean absoluteSyntax,
      Set<QueueCapacityVector.ResourceUnitCapacityType> types) {
    if (legacy) {
      return absoluteSyntax ? AbstractCSQueue.CapacityConfigType.ABSOLUTE_RESOURCE
          : AbstractCSQueue.CapacityConfigType.PERCENTAGE;
    }
    return types.equals(Set.of(QueueCapacityVector.ResourceUnitCapacityType.ABSOLUTE))
        ? AbstractCSQueue.CapacityConfigType.ABSOLUTE_RESOURCE
        : AbstractCSQueue.CapacityConfigType.PERCENTAGE;
  }

  /**
   * Preserves the legacy sibling-mode classification and diagnostic meaning.
   * @throws IOException when sibling queues mix incompatible modes
   */
  public static AbstractParentQueue.QueueCapacityType classify(
      String parentPath, List<ModeUse> uses, boolean firstQueueIsRoot,
      boolean queuesEmpty) throws IOException {
    boolean percentageIsSet = false;
    boolean weightIsSet = false;
    boolean absoluteIsSet = false;
    StringBuilder diagnostics = new StringBuilder();
    for (ModeUse use : uses) {
      if (use.percentage()) {
        percentageIsSet = true;
      }
      if (use.weight()) {
        weightIsSet = true;
        diagnostics.append("{Queue=").append(use.queuePath())
            .append(", label=").append(use.label())
            .append(" uses weight mode}. ");
      }
      if (use.absolute()) {
        absoluteIsSet = true;
        percentageIsSet = false;
        diagnostics.append("{Queue=").append(use.queuePath())
            .append(", label=").append(use.label())
            .append(" uses absolute mode}. ");
      }
      if (percentageIsSet) {
        diagnostics.append("{Queue=").append(use.queuePath())
            .append(", label=").append(use.label())
            .append(" uses percentage mode}. ");
      }
    }
    int configuredModes = (percentageIsSet ? 1 : 0)
        + (weightIsSet ? 1 : 0) + (absoluteIsSet ? 1 : 0);
    if (!queuesEmpty && !firstQueueIsRoot && configuredModes > 1) {
      throw new IOException("Parent queue '" + parentPath
          + "' have children queue used mixed of "
          + " weight mode, percentage and absolute mode, it is not allowed, "
          + "please double check, details:" + diagnostics);
    }
    if (weightIsSet || queuesEmpty) {
      return AbstractParentQueue.QueueCapacityType.WEIGHT;
    }
    if (absoluteIsSet) {
      return AbstractParentQueue.QueueCapacityType.ABSOLUTE_RESOURCE;
    }
    return AbstractParentQueue.QueueCapacityType.PERCENT;
  }
}
