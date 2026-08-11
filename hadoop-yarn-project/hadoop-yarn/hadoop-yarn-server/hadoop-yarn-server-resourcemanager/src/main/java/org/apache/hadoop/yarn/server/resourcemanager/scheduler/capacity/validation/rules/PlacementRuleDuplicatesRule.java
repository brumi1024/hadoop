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

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.function.Consumer;

import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.ValidationContext;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.ValidationIssue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.ValidationRule;

/** Rejects duplicate legacy placement-rule strings. */
public final class PlacementRuleDuplicatesRule implements ValidationRule {
  public static Set<String> validateRuleClassNames(
      Collection<String> placementRules) throws IOException {
    Set<String> distinct = new LinkedHashSet<>();
    for (String rule : placementRules) {
      if (!distinct.add(rule)) {
        throw new IOException("Invalid PlacementRule inputs which contains "
            + "duplicate rule strings");
      }
    }
    return distinct;
  }

  @Override
  public String id() {
    return "placement-rule-duplicates";
  }
  @Override
  public Stage stage() {
    return Stage.MODEL;
  }

  @Override
  public void run(ValidationContext context,
      Consumer<ValidationIssue> sink) {
    String configured = context.getModel().getRawProperties().get(
        CapacitySchedulerConfiguration.QUEUE_MAPPING);
    if (configured == null) {
      return;
    }
    Set<String> distinct = new HashSet<>();
    for (String rule : configured.split(",")) {
      if (!distinct.add(rule.trim())) {
        sink.accept(new ValidationIssue(null,
            CapacitySchedulerConfiguration.QUEUE_MAPPING, id(),
            ValidationIssue.Severity.ERROR,
            "Invalid PlacementRule inputs which contains duplicate rule strings"));
        return;
      }
    }
  }
}
