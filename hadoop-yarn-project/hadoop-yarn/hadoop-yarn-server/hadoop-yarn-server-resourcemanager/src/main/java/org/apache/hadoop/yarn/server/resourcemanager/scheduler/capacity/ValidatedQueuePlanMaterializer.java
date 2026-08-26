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
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.util.Preconditions;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.plan.ValidatedQueuePlan;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.plan.ValidatedQueuePlan.QueueKind;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.plan.ValidatedQueuePlan.QueuePlanNode;

/**
 * Materializes a validated, configuration-only plan using live scheduler
 * adapters and existing queue state.
 */
@Private
@Unstable
public final class ValidatedQueuePlanMaterializer {

  /**
   * Builds one live queue hierarchy from an already compiled plan.
   *
   * @param plan validated immutable queue plan
   * @param liveContext live scheduler adapters used by queue constructors
   * @param existingQueues existing queues used to preserve mutable queue state
   * @return materialized root queue
   * @throws IOException when a live queue cannot be constructed
   */
  public CSQueue materialize(ValidatedQueuePlan plan,
      CapacitySchedulerQueueContext liveContext,
      CSQueueStore existingQueues) throws IOException {
    Preconditions.checkNotNull(plan, "plan");
    Preconditions.checkNotNull(liveContext, "liveContext");
    Preconditions.checkNotNull(existingQueues, "existingQueues");
    return materializeNode(plan, plan.getRoot(), liveContext, existingQueues,
        null);
  }

  private CSQueue materializeNode(ValidatedQueuePlan plan,
      QueuePlanNode node, CapacitySchedulerQueueContext liveContext,
      CSQueueStore existingQueues, CSQueue parent) throws IOException {
    QueuePath path = node.getQueuePath();
    String queueName = path.getLeafName();
    CSQueue existingQueue = existingQueues.get(path.getFullPath());
    QueueKind kind = materializedKind(node, existingQueue);

    if (kind == QueueKind.LEAF || kind == QueueKind.PLAN) {
      validateParent(parent, queueName);
    }

    CSQueue queue;
    switch (kind) {
    case LEAF:
      queue = new LeafQueue(liveContext, queueName, parent, existingQueue);
      break;
    case PLAN:
      PlanQueue planQueue = new PlanQueue(liveContext, queueName, parent,
          existingQueue);
      planQueue.initializeDefaultInternalQueue();
      queue = planQueue;
      break;
    case MANAGED_PARENT:
      queue = new ManagedParentQueue(liveContext, queueName, parent,
          existingQueue);
      break;
    case PARENT:
      queue = new ParentQueue(liveContext, queueName, parent, existingQueue);
      break;
    default:
      throw new IllegalStateException("Unsupported queue kind " + kind
          + " for " + path.getFullPath());
    }

    if (queue instanceof AbstractParentQueue) {
      List<CSQueue> children = new ArrayList<>();
      for (QueuePath childPath : node.getChildPaths()) {
        QueuePlanNode child = plan.getQueue(childPath);
        if (child == null) {
          throw new IllegalStateException("Compiled queue plan is missing "
              + childPath.getFullPath());
        }
        children.add(materializeNode(plan, child, liveContext,
            existingQueues, queue));
      }
      if (!children.isEmpty()) {
        ((AbstractParentQueue) queue).setChildQueues(children);
      }
    }
    return queue;
  }

  private QueueKind materializedKind(QueuePlanNode node,
      CSQueue existingQueue) {
    boolean existingDynamicParent = existingQueue instanceof AbstractParentQueue
        && existingQueue.isDynamicQueue();
    if (!existingDynamicParent) {
      return node.getKind();
    }
    if (node.isReservable()) {
      throw new IllegalStateException("Only Leaf Queues can be reservable for "
          + node.getQueuePath().getFullPath());
    }
    if (node.getKind() == QueueKind.LEAF
        || node.getKind() == QueueKind.PLAN) {
      return QueueKind.PARENT;
    }
    return node.getKind();
  }

  private void validateParent(CSQueue parent, String queueName) {
    if (parent == null) {
      throw new IllegalStateException(
          "Queue configuration missing child queue names for " + queueName);
    }
  }
}
