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

/** Immutable result of compatibility classification and plan compilation. */
public final class CompileResult {
  private final ValidatedQueuePlan plan;
  private final List<ValidationIssue> issues;
  private final List<LegacyFallbackReason> fallbackReasons;

  CompileResult(ValidatedQueuePlan plan, List<ValidationIssue> issues,
      List<LegacyFallbackReason> fallbackReasons) {
    this.plan = plan;
    this.issues = List.copyOf(issues);
    this.fallbackReasons = List.copyOf(fallbackReasons);
  }

  public ValidatedQueuePlan getPlan() {
    return plan;
  }

  public List<ValidationIssue> getIssues() {
    return issues;
  }

  public List<LegacyFallbackReason> getFallbackReasons() {
    return fallbackReasons;
  }

  public boolean isValid() {
    return fallbackReasons.isEmpty() && issues.stream().noneMatch(issue ->
        issue.getSeverity() == ValidationIssue.Severity.ERROR);
  }

  public boolean isCompiledActivationEligible() {
    return plan != null && isValid();
  }

  public boolean requiresLegacyValidation() {
    return !fallbackReasons.isEmpty();
  }

  /**
   * Exposes the existing diagnostic contract after compatibility was decided.
   * @return compiled validation issues in the existing result form
   * @throws IllegalStateException when legacy validation is still required
   */
  public ValidationResult asValidationResult() {
    if (requiresLegacyValidation()) {
      throw new IllegalStateException("Legacy validation is still required");
    }
    return new ValidationResult(issues);
  }
}
