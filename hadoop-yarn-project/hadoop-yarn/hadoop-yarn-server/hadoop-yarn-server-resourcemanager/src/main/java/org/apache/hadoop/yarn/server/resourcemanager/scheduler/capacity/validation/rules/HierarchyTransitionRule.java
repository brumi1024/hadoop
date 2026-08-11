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

import java.util.function.Consumer;
import java.util.Map;

import org.apache.hadoop.yarn.api.records.QueueState;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueuePath;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueuePrefixes;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.conf.model.QueueConfigNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.ClusterFacts;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.ValidationContext;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.ValidationIssue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.ValidationRule;

/** Enforces queue hierarchy transition and deletion invariants. */
public final class HierarchyTransitionRule implements ValidationRule {
  @Override
  public String id() {
    return "queue-deletion-requires-stopped";
  }
  @Override
  public Stage stage() {
    return Stage.HIERARCHY;
  }

  @Override
  public void run(ValidationContext context,
      Consumer<ValidationIssue> sink) {
    if (context.getFacts().isHierarchyValidationSkipped()) {
      return;
    }
    for (Map.Entry<QueuePath, ClusterFacts.OldQueue> entry
        : context.getFacts().getOldHierarchy().entrySet()) {
      QueuePath path = entry.getKey();
      ClusterFacts.OldQueue old = entry.getValue();
      if (old.isAutoCreatedLeaf()) {
        continue;
      }
      QueueConfigNode proposed = context.getModel().getNode(path);
      QueueState proposedState = configuredStateCaseSensitive(context, path,
          proposed);
      if (proposed == null) {
        if (!old.isDynamic() && old.getState() != QueueState.STOPPED
            && proposedState != QueueState.STOPPED) {
          sink.accept(issue(path, id(), path.getFullPath()
              + " cannot be deleted from the capacity scheduler "
              + "configuration, as the queue is not yet in stopped state. "
              + "Current State : " + old.getState()));
        }
        continue;
      }
      ClusterFacts.QueueKind next = kind(proposed);
      if (old.getKind() == ClusterFacts.QueueKind.PARENT
          && next == ClusterFacts.QueueKind.MANAGED_PARENT) {
        sink.accept(issue(path, "parent-managedparent-conversion",
            "Can not convert parent queue: " + path.getFullPath()
                + " to auto create enabled parent queue since it could have "
                + "other pre-configured queues which is not supported"));
      }
      if (old.getKind() == ClusterFacts.QueueKind.MANAGED_PARENT
          && next != ClusterFacts.QueueKind.MANAGED_PARENT) {
        sink.accept(issue(path, "parent-managedparent-conversion",
            "Cannot convert auto create enabled parent queue: "
                + path.getFullPath() + " to leaf queue. Please check parent "
                + "queue's configuration "
                + CapacitySchedulerConfiguration.AUTO_CREATE_CHILD_QUEUE_ENABLED
                + " is set to true"));
      }
      if (old.getKind() == ClusterFacts.QueueKind.LEAF
          && next != ClusterFacts.QueueKind.LEAF
          && old.getState() != QueueState.STOPPED
          && proposedState != QueueState.STOPPED) {
        sink.accept(issue(path, "leaf-parent-conversion-requires-stopped",
            "Can not convert the leaf queue: " + path.getFullPath()
                + " to parent queue since it is not yet in stopped state. "
                + "Current State : " + old.getState()));
      }
    }
  }

  private ClusterFacts.QueueKind kind(QueueConfigNode node) {
    if (node.isAutoCreateChildQueueEnabled()) {
      return ClusterFacts.QueueKind.MANAGED_PARENT;
    }
    return node.getChildren().isEmpty() ? ClusterFacts.QueueKind.LEAF
        : ClusterFacts.QueueKind.PARENT;
  }

  private QueueState configuredStateCaseSensitive(ValidationContext context,
      QueuePath path, QueueConfigNode node) {
    String state = node == null
        ? context.getModel().getRawProperties().get(
            QueuePrefixes.getQueuePrefix(path)
                + CapacitySchedulerConfiguration.STATE)
        : node.getRawProperty(CapacitySchedulerConfiguration.STATE);
    if (state == null) {
      return null;
    }
    try {
      return QueueState.valueOf(state);
    } catch (IllegalArgumentException ignored) {
      return null;
    }
  }

  private ValidationIssue issue(QueuePath path, String ruleId,
      String message) {
    return new ValidationIssue(path, null, ruleId,
        ValidationIssue.Severity.ERROR, message);
  }
}
