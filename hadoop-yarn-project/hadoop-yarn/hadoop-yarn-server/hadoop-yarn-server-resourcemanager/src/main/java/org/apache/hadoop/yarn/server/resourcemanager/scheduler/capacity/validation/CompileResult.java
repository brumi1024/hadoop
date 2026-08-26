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

import java.util.List;

import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.plan.ValidatedQueuePlan;

/** Immutable compiled queue plan and its structured validation issues. */
public final class CompileResult {
  private final ValidatedQueuePlan plan;
  private final List<ValidationIssue> issues;

  CompileResult(ValidatedQueuePlan plan, List<ValidationIssue> issues) {
    this.plan = plan;
    this.issues = List.copyOf(issues);
  }

  public ValidatedQueuePlan getPlan() {
    return plan;
  }

  public List<ValidationIssue> getIssues() {
    return issues;
  }

  public boolean isValid() {
    return issues.stream().noneMatch(issue ->
        issue.getSeverity() == ValidationIssue.Severity.ERROR);
  }

  /**
   * Returns the structured validation result associated with this plan.
   *
   * @return immutable validation result
   */
  public ValidationResult asValidationResult() {
    return new ValidationResult(issues);
  }
}
