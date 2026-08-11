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

import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.conf.model.QueueConfigNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.ValidationContext;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.ValidationIssue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.ValidationRule;

/** Rejects a managed parent configured below another managed parent. */
public final class NestedManagedParentRule implements ValidationRule {
  @Override
  public String id() {
    return "nested-managed-parent";
  }

  @Override
  public Stage stage() {
    return Stage.MODEL;
  }

  @Override
  public void run(ValidationContext context,
      Consumer<ValidationIssue> sink) {
    for (QueueConfigNode node : context.getModel().getNodes().values()) {
      QueueConfigNode parent = node.getParent();
      if (node.isAutoCreateChildQueueEnabled() && parent != null
          && parent.isAutoCreateChildQueueEnabled()) {
        sink.accept(new ValidationIssue(node.getQueuePath(), null, id(),
            ValidationIssue.Severity.ERROR,
            "Auto creation enabled parent queue "
                + node.getQueuePath().getFullPath()
                + " cannot be configured below auto creation enabled parent "
                + parent.getQueuePath().getFullPath()));
      }
    }
  }
}
