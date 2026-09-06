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
import java.util.Objects;

import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.CSConfigValidationEngine;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.ValidatedQueuePlan;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.ValidatedQueuePlan.QueuePlanNode;

/** Builds one live hierarchy from a validated, candidate-bound queue plan. */
public final class ValidatedQueuePlanMaterializer {
  private final CapacitySchedulerQueueManager.QueueHook hook;

  public ValidatedQueuePlanMaterializer() {
    this(new CapacitySchedulerQueueManager.QueueHook());
  }

  ValidatedQueuePlanMaterializer(CapacitySchedulerQueueManager.QueueHook hook) {
    this.hook = Objects.requireNonNull(hook);
  }

  /**
   * Materializes without publishing the resulting hierarchy.
   * The caller holds the scheduler and node-label snapshot scope, and the live
   * context must already expose the candidate configuration.
   * Existing queue constructors remain the source of live initialization.
   *
   * @param plan validated immutable plan
   * @param liveContext live adapters exposing the candidate
   * @param existingQueues existing queue identities used for metric reuse
   * @return newly materialized root
   * @throws IOException when inputs no longer match or construction fails
   */
  public CSQueue materialize(ValidatedQueuePlan plan,
      CapacitySchedulerQueueContext liveContext, CSQueueStore existingQueues)
      throws IOException {
    Objects.requireNonNull(plan);
    Objects.requireNonNull(liveContext);
    Objects.requireNonNull(existingQueues);
    if (!new CSConfigValidationEngine().matchesInputs(plan,
        liveContext.getConfigModel(),
        liveContext.captureClusterFacts(existingQueues))) {
      throw new IOException("Compiled queue plan does not match candidate "
          + "configuration and current cluster facts");
    }
    return build(plan, plan.getRoot(), null, liveContext, existingQueues);
  }

  private CSQueue build(ValidatedQueuePlan plan, QueuePlanNode node,
      CSQueue parent, CapacitySchedulerQueueContext context,
      CSQueueStore existingQueues) throws IOException {
    String name = new QueuePath(node.queuePath()).getLeafName();
    CSQueue old = existingQueues.get(node.queuePath());
    CSQueue queue = switch (node.kind()) {
    case LEAF -> new LeafQueue(context, name, parent, old);
    case PARENT -> new ParentQueue(context, name, parent, old);
    case MANAGED_PARENT -> new ManagedParentQueue(context, name, parent, old);
    case PLAN -> throw new IOException(
        "Reservation plans require legacy materialization");
    };
    queue = hook.hook(queue);
    if (queue instanceof AbstractParentQueue parentQueue) {
      List<CSQueue> children = new ArrayList<>(node.childPaths().size());
      for (String child : node.childPaths()) {
        children.add(build(plan, plan.getQueue(child), queue, context,
            existingQueues));
      }
      if (!children.isEmpty()) {
        parentQueue.setChildQueues(children);
      }
    }
    return queue;
  }
}
