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
import java.util.regex.Pattern;

import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueuePath;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.ValidationContext;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.ValidationIssue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.ValidationRule;

/** Enforces the queue path grammar and warns about nonportable names. */
public final class QueueNameRule implements ValidationRule {
  private static final Pattern PORTABLE = Pattern.compile("[a-zA-Z0-9_-]+");

  @Override
  public String id() {
    return "queue-name";
  }
  @Override
  public Stage stage() {
    return Stage.MODEL;
  }

  @Override
  public void run(ValidationContext context,
      Consumer<ValidationIssue> sink) {
    context.getModel().getRawProperties().forEach((key, value) -> {
      if (key.startsWith(CapacitySchedulerConfiguration.PREFIX)
          && key.endsWith("." + CapacitySchedulerConfiguration.QUEUES)) {
        for (String rawComponent : value.split(",", -1)) {
          String component = rawComponent.trim();
          if (component.isEmpty()) {
            sink.accept(new ValidationIssue(null, key, id(),
                ValidationIssue.Severity.ERROR,
                "Queue list contains an empty component"));
          } else if (component.contains(".")) {
            sink.accept(new ValidationIssue(null, key, id(),
                ValidationIssue.Severity.ERROR,
                "Queue list component '" + component
                    + "' contains an embedded dot"));
          }
        }
      }
    });
    for (QueuePath path : context.getModel().getNodes().keySet()) {
      if (path.hasEmptyPart()) {
        sink.accept(issue(path, ValidationIssue.Severity.ERROR,
            "Queue path contains an empty component"));
        continue;
      }
      for (String component : path.getFullPath().split("\\.", -1)) {
        if (!PORTABLE.matcher(component).matches()) {
          sink.accept(issue(path, ValidationIssue.Severity.WARNING,
              "Queue name component '" + component
                  + "' contains characters outside [a-zA-Z0-9_-]"));
        }
      }
    }
  }

  private ValidationIssue issue(QueuePath path,
      ValidationIssue.Severity severity, String message) {
    return new ValidationIssue(path, null, id(), severity, message);
  }
}
