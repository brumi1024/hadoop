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

import java.util.Set;

import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueCapacityVector.ResourceUnitCapacityType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.conf.model.CSConfigModel;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.conf.model.QueueConfigNode;

/** Compatibility probes for legacy queue-mode detection. */
final class LegacyQueueModeProbes {
  private LegacyQueueModeProbes() {
  }

  static boolean isPercentageConfigured(CSQueue queue, String label,
      CSConfigModel model) {
    if (usesRuntimeCapacities(queue)) {
      return queue.getQueueCapacities().getCapacity(label) > 0;
    }
    return hasSingletonVectorTypeForMemoryProbe(queue, label, model,
        ResourceUnitCapacityType.PERCENTAGE, true);
  }

  static boolean isWeightConfigured(CSQueue queue, String label,
      CSConfigModel model) {
    if (usesRuntimeCapacities(queue)) {
      return queue.getQueueCapacities().getWeight(label) >= 0;
    }
    return hasSingletonVectorTypeForMemoryProbe(queue, label, model,
        ResourceUnitCapacityType.WEIGHT, false);
  }

  static boolean isAbsoluteConfigured(CSQueue queue, String label,
      CapacitySchedulerConfiguration configuration) {
    return configuration.checkConfigTypeIsAbsoluteResource(label,
        queue.getQueuePathObject());
  }

  private static boolean usesRuntimeCapacities(CSQueue queue) {
    return queue.isDynamicQueue()
        || queue instanceof AbstractAutoCreatedLeafQueue
        || queue instanceof ReservationQueue
        || queue instanceof PlanQueue;
  }

  private static boolean hasSingletonVectorTypeForMemoryProbe(
      CSQueue queue, String label,
      CSConfigModel model, ResourceUnitCapacityType type,
      boolean requirePositiveValue) {
    if (model == null) {
      return false;
    }
    boolean isLeaf = !(queue instanceof AbstractParentQueue);
    QueueConfigNode node = model.effectiveConfigFor(
        queue.getQueuePathObject(), isLeaf);
    QueueConfigNode.CapacityValue value = node.getCapacity(label);
    if (value == null || !value.getVector().getDefinedCapacityTypes()
        .equals(Set.of(type))) {
      return false;
    }
    double memory = value.getVector().getMemory();
    return requirePositiveValue ? memory > 0 : memory >= 0;
  }
}
