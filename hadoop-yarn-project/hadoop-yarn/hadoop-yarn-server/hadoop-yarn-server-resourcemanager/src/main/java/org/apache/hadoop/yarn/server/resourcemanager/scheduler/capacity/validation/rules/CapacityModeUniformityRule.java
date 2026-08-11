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

import java.util.HashSet;
import java.util.Set;
import java.util.function.Consumer;

import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueCapacityVector.ResourceUnitCapacityType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.conf.model.QueueConfigNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.ValidationContext;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.ValidationIssue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.ValidationRule;

/** Requires siblings to use a compatible capacity mode. */
public final class CapacityModeUniformityRule implements ValidationRule {
  @Override
  public String id() {
    return "capacity-mode-uniformity";
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
      Set<ResourceUnitCapacityType> modes = new HashSet<>();
      for (QueueConfigNode child : parent.getChildren().values()) {
        QueueConfigNode.CapacityValue value = child.getCapacity("");
        if (value != null) {
          modes.addAll(value.getVector().getDefinedCapacityTypes());
        }
      }
      if (modes.size() > 1) {
        sink.accept(new ValidationIssue(parent.getQueuePath(), null, id(),
            ValidationIssue.Severity.ERROR,
            "Queue children use mixed capacity configuration modes"));
      }
    }
  }
}
