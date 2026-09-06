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
package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation;

import java.util.Map;

import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueuePath;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.conf.model.ConfigDiagnostic;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.ValidatedQueuePlan.PlanDiagnostic;

/** Shared stable conversion from parser diagnostics to validation issues. */
final class ValidationDiagnosticSupport {
  private static final Map<String, ValidationIssue.Severity> SEVERITIES =
      Map.of(
          "invalid-boolean", ValidationIssue.Severity.WARNING,
          "invalid-float", ValidationIssue.Severity.ERROR,
          "invalid-integer", ValidationIssue.Severity.ERROR,
          "invalid-capacity", ValidationIssue.Severity.ERROR,
          "invalid-queue-state", ValidationIssue.Severity.ERROR,
          "deprecated-key", ValidationIssue.Severity.WARNING);

  private ValidationDiagnosticSupport() {
  }

  static ValidationIssue issue(ConfigDiagnostic diagnostic) {
    return new ValidationIssue(diagnostic.getQueuePath(),
        diagnostic.getPropertyKey(), diagnostic.getCode(),
        severity(diagnostic.getCode()), diagnostic.getMessage());
  }

  static ValidationIssue issue(PlanDiagnostic diagnostic) {
    QueuePath path = diagnostic.queuePath() == null
        ? null : new QueuePath(diagnostic.queuePath());
    return new ValidationIssue(path, diagnostic.propertyKey(),
        diagnostic.code(), severity(diagnostic.code()), diagnostic.message());
  }

  private static ValidationIssue.Severity severity(String code) {
    return SEVERITIES.getOrDefault(code, ValidationIssue.Severity.ERROR);
  }
}
