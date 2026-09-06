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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.yarn.api.records.QueueState;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.ValidatedQueuePlan.OldQueueSnapshot;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.ValidatedQueuePlan.QueueKind;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.ValidatedQueuePlan.QueuePlanNode;

/**
 * Predicts reinitialization failures that fresh-tree validation cannot observe.
 * Such candidates keep legacy activation and its existing exception behavior.
 */
final class CompiledQueuePlanActivationValidator {
  private CompiledQueuePlanActivationValidator() {
  }

  static List<LegacyFallbackReason> fallbackReasons(ValidatedQueuePlan plan) {
    List<LegacyFallbackReason> reasons = new ArrayList<>();
    visit(plan, plan.getRoot(), null, false, false, reasons);
    reasons.sort(Comparator.comparing(LegacyFallbackReason::queuePath)
        .thenComparing(LegacyFallbackReason::propertyKey));
    return List.copyOf(reasons);
  }

  private static void visit(ValidatedQueuePlan plan, QueuePlanNode node,
      QueueState parentState, boolean stoppedByAncestor,
      boolean replacedAncestor, List<LegacyFallbackReason> reasons) {
    OldQueueSnapshot old = plan.getFacts().oldHierarchy().get(node.queuePath());
    boolean retained = !replacedAncestor && old != null
        && old.kind().name().equals(node.kind().name());
    QueueState effective = node.initialState();
    boolean stopsChildren = stoppedByAncestor;
    if (retained) {
      if (!old.configuredNodeLabels().equals(node.configuredNodeLabels())) {
        reasons.add(reason(node, CapacitySchedulerConfiguration.ACCESSIBLE_NODE_LABELS,
            "Retained queue label-set changes require legacy quota initialization"));
      }
      if (node.kind() != QueueKind.LEAF && old.allowZeroCapacitySum()
          != node.settings().scheduling().allowZeroCapacitySum()) {
        reasons.add(reason(node, CapacitySchedulerConfiguration.ALLOW_ZERO_CAPACITY_SUM,
            "Retained parents keep their constructor-only zero-capacity setting"));
      }
      if (node.kind() == QueueKind.MANAGED_PARENT
          && !old.constructorCapacityType().equals(
          CompiledQueuePlanCapacityValidator.constructorCapacityType(plan, node).name())) {
        reasons.add(reason(node, CapacitySchedulerConfiguration.CAPACITY,
            "Retained managed-parent template refresh depends on its previous capacity type"));
      }
      QueueState previous = stoppedByAncestor ? QueueState.STOPPED : old.state();
      effective = previous;
      if (previous == QueueState.RUNNING) {
        if (node.configuredState() == QueueState.STOPPED) {
          effective = QueueState.STOPPED;
          stopsChildren = true;
        }
      } else if (node.configuredState() == QueueState.RUNNING) {
        if (parentState != null && parentState != QueueState.RUNNING) {
          reasons.add(reason(node, CapacitySchedulerConfiguration.STATE,
              "Retained queue activation requires a running live parent"));
        }
        effective = QueueState.RUNNING;
      }
      if (node.kind() == QueueKind.LEAF) {
        for (Map.Entry<String, Long> resource
            : old.maximumAllocation().values().entrySet()) {
          if (resource.getValue() > node.settings().maximumAllocation()
              .value(resource.getKey())) {
            reasons.add(reason(node,
                CapacitySchedulerConfiguration.MAXIMUM_ALLOCATION,
                "Retained leaf maximum allocation cannot decrease"));
            break;
          }
        }
      }
    }
    for (String child : node.childPaths()) {
      visit(plan, plan.getQueue(child), effective, stopsChildren,
          !retained, reasons);
    }
  }

  private static LegacyFallbackReason reason(QueuePlanNode node,
      String property, String message) {
    return new LegacyFallbackReason(
        LegacyFallbackReason.Code.LIVE_QUEUE_REINITIALIZATION,
        node.queuePath(), CapacitySchedulerConfiguration.PREFIX
            + node.queuePath() + "." + property, null, message);
  }
}
