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

import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.ValidationContext;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.ValidationIssue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.ValidationRule;

/** Performs syntax checks for the configured placement-rule representation. */
public final class PlacementRulesParseRule implements ValidationRule {
  @Override
  public String id() {
    return "placement-rules-parse";
  }
  @Override
  public Stage stage() {
    return Stage.HIERARCHY;
  }

  @Override
  public void run(ValidationContext context,
      Consumer<ValidationIssue> sink) {
    String format = context.getModel().getMappingRuleFormat();
    if (!CapacitySchedulerConfiguration.MAPPING_RULE_FORMAT_LEGACY.equals(format)
        && !CapacitySchedulerConfiguration.MAPPING_RULE_FORMAT_JSON.equals(format)) {
      sink.accept(issue("Unknown mapping rule format '" + format + "'"));
      return;
    }
    if (!CapacitySchedulerConfiguration.MAPPING_RULE_FORMAT_LEGACY.equals(format)) {
      return;
    }
    String rules = context.getModel().getRawProperties().get(
        CapacitySchedulerConfiguration.QUEUE_MAPPING);
    if (rules == null || rules.trim().isEmpty()) {
      return;
    }
    for (String rule : rules.split(",")) {
      String[] fields = rule.trim().split(":", -1);
      if (fields.length != 3) {
        sink.accept(issue("Illegal queue mapping " + rule.trim()));
        return;
      }
      String target = fields[2].trim();
      if (!target.contains("%") && !queueExists(context, target)) {
        sink.accept(issue("Path root '" + target
            + "' does not exist. Path '" + target + "' is invalid"));
        return;
      }
    }
  }

  private boolean queueExists(ValidationContext context, String target) {
    boolean configured = context.getModel().getNodes().keySet().stream()
        .anyMatch(path ->
        path.getFullPath().equals(target)
            || path.getLeafName().equals(target));
    if (configured) {
      return true;
    }
    int separator = target.lastIndexOf('.');
    if (separator > 0) {
      String parentTarget = target.substring(0, separator);
      boolean dynamicParent = context.getModel().getNodes().values().stream()
          .anyMatch(node -> node.isAutoCreateChildQueueEnabled()
              && (node.getQueuePath().getFullPath().equals(parentTarget)
                  || node.getQueuePath().getFullPath()
                      .equals("root." + parentTarget)
                  || node.getQueuePath().getLeafName()
                      .equals(parentTarget)));
      if (dynamicParent) {
        return true;
      }
    }
    return context.getFacts().getOldHierarchy().entrySet().stream()
        .filter(entry -> entry.getValue().isDynamic()
            || entry.getValue().isAutoCreatedLeaf())
        .map(entry -> entry.getKey().getFullPath())
        .anyMatch(path -> path.equals(target) || path.endsWith("." + target));
  }

  private ValidationIssue issue(String message) {
    return new ValidationIssue(null,
        CapacitySchedulerConfiguration.QUEUE_MAPPING, id(),
        ValidationIssue.Severity.ERROR, message);
  }
}
