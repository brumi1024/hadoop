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
package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.rules;

import java.util.Set;
import java.util.function.Consumer;

import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueCapacityVector.ResourceUnitCapacityType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.conf.model.LegacyCapacityDerivations;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.conf.model.QueueConfigNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.ValidationContext;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.ValidationIssue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.ValidationRule;

/** Checks the legacy percentage sum for each configured parent. */
public final class ChildrenCapacitySumRule implements ValidationRule {
  private static final float EPSILON = 0.05f;

  @Override
  public String id() {
    return "children-capacity-sum";
  }
  @Override
  public Stage stage() {
    return Stage.MODEL;
  }

  @Override
  public void run(ValidationContext context,
      Consumer<ValidationIssue> sink) {
    if (!context.getModel().isLegacyQueueMode()) {
      return;
    }
    for (QueueConfigNode parent : context.getModel().getNodes().values()) {
      if (parent.getChildren().isEmpty()) {
        continue;
      }
      QueueConfigNode.CapacityValue parentCapacity = parent.getCapacity("");
      if (!parent.getQueuePath().isRoot() && parentCapacity != null
          && LegacyCapacityDerivations.capacity(parent.getQueuePath(),
              parentCapacity, 0) == 0) {
        // A zero-capacity parent may keep zero-capacity children. This is a
        // supported staging shape in the live queue constructors.
        continue;
      }
      float sum = 0;
      boolean percentage = true;
      boolean configuredChild = false;
      for (QueueConfigNode child : parent.getChildren().values()) {
        QueueConfigNode.CapacityValue value = child.getCapacity("");
        if (value == null || value.getVector().isMixedCapacityVector()
            || !value.getVector().getDefinedCapacityTypes()
                .equals(Set.of(ResourceUnitCapacityType.PERCENTAGE))) {
          percentage = false;
          break;
        }
        configuredChild = true;
        sum += LegacyCapacityDerivations.capacity(child.getQueuePath(),
            value, 0);
      }
      boolean allowedZeroSum = Math.abs(sum) <= EPSILON
          && context.getPlan().getQueue(parent.getQueuePath())
              .getSettings().isAllowZeroCapacitySum();
      if (percentage && configuredChild && !allowedZeroSum
          && Math.abs(sum - 100) > EPSILON) {
        sink.accept(new ValidationIssue(parent.getQueuePath(), null, id(),
            ValidationIssue.Severity.ERROR,
            "Illegal capacity sum of " + sum + " for children of queue "
                + parent.getQueuePath().getFullPath()
                + ". The capacity sum should be 100.0"));
      }
    }
  }
}
