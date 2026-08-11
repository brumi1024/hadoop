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

import java.util.Locale;
import java.util.function.Consumer;

import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerQueueCapacityHandler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueCapacityUpdateContext;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueuePath;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueUpdateWarning;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueUpdateWarning.QueueUpdateWarningType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.ValidationContext;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.ValidationIssue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.ValidationRule;
import org.apache.hadoop.yarn.util.resource.Resources;

/**
 * Applies the capacity-vector calculations to the isolated validation tree.
 *
 * <p>The validation context uses a fresh preemption manager, so it reports no
 * killable resources. A live resource manager can have active preemption and
 * therefore produce different warning outcomes on refresh. This rule is
 * warning-only for those outcomes, and the fresh-boot state is the documented
 * validation baseline.</p>
 */
public final class CapacityVectorUpdateRule implements ValidationRule {
  @Override
  public String id() {
    return "capacity-vector-update";
  }

  @Override
  public Stage stage() {
    return Stage.HIERARCHY;
  }

  @Override
  public void run(ValidationContext context,
      Consumer<ValidationIssue> sink) {
    if (context.getProposedRoot() == null
        || context.getFacts().isHierarchyValidationSkipped()
        || Resources.isNone(context.getFacts().getClusterResource())) {
      return;
    }
    try {
      CapacitySchedulerQueueCapacityHandler handler =
          new CapacitySchedulerQueueCapacityHandler(
              context.getBuildContext().getLabelManager(),
              context.getBuildContext().getConfiguration());
      handler.updateRoot(context.getProposedRoot(),
          context.getFacts().getClusterResource());
      QueueCapacityUpdateContext updateContext = handler.updateChildren(
          context.getFacts().getClusterResource(),
          context.getProposedRoot());
      emitWarnings(updateContext, sink);
    } catch (Exception failure) {
      sink.accept(new ValidationIssue(null, null, "capacity-update-failure",
          ValidationIssue.Severity.ERROR, message(failure)));
    }
  }

  private void emitWarnings(QueueCapacityUpdateContext updateContext,
      Consumer<ValidationIssue> sink) {
    for (QueueUpdateWarning warning : updateContext.getUpdateWarnings()) {
      QueueUpdateWarningType warningType = warning.getWarningType();
      String ruleId = "capacity-update-" + warningType.name()
          .toLowerCase(Locale.ROOT).replace('_', '-');
      QueuePath queuePath = warning.getQueue() == null
          ? null : new QueuePath(warning.getQueue());
      sink.accept(new ValidationIssue(queuePath, null, ruleId,
          ValidationIssue.Severity.WARNING, warning.toString()));
    }
  }

  private static String message(Throwable failure) {
    return failure.getMessage() == null
        ? failure.getClass().getSimpleName() : failure.getMessage();
  }
}
