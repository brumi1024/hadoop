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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.conf.model.CSConfigModel;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.conf.model.ConfigDiagnostic;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.plan.ValidatedQueuePlan;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.rules.AbsoluteParentMinCoverageRule;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.rules.CapacityModeUniformityRule;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.rules.CapacityVectorUpdateRule;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.rules.ChildrenCapacitySumRule;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.rules.HierarchyTransitionRule;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.rules.MemoryAllocationRule;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.rules.NestedManagedParentRule;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.rules.PlacementRuleDuplicatesRule;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.rules.PlacementRulesParseRule;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.rules.PlanQueueSettingsValidationRule;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.rules.QueueNameRule;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.rules.VcoresAllocationRule;

/** Side-effect-free entry point for Capacity Scheduler configuration validation. */
public final class CSConfigValidationEngine {
  private static final Map<String, ValidationIssue.Severity> DIAGNOSTIC_SEVERITIES =
      Map.of(
          // Boolean parsing is deliberately lenient, unlike the other typed
          // parsers which throw in the legacy configuration getters.
          "invalid-boolean", ValidationIssue.Severity.WARNING,
          "invalid-float", ValidationIssue.Severity.ERROR,
          "invalid-integer", ValidationIssue.Severity.ERROR,
          "invalid-capacity", ValidationIssue.Severity.ERROR,
          "invalid-queue-state", ValidationIssue.Severity.ERROR,
          "deprecated-key", ValidationIssue.Severity.WARNING);
  private final List<ValidationRule> rules;

  public CSConfigValidationEngine() {
    this(List.of(new MemoryAllocationRule(), new VcoresAllocationRule(),
        new PlacementRuleDuplicatesRule(), new QueueNameRule(),
        new CapacityModeUniformityRule(), new ChildrenCapacitySumRule(),
        new NestedManagedParentRule(), new HierarchyTransitionRule(),
        new AbsoluteParentMinCoverageRule(), new PlanQueueSettingsValidationRule(),
        new PlacementRulesParseRule(), new CapacityVectorUpdateRule()));
  }

  CSConfigValidationEngine(List<ValidationRule> rules) {
    this.rules = List.copyOf(rules);
  }

  public ValidationResult validate(CSConfigModel proposed,
      ClusterFacts facts) {
    return compile(proposed, facts).asValidationResult();
  }

  /**
   * Compiles an immutable queue plan and evaluates all structured validation
   * rules against the same model and runtime-facts snapshot.
   *
   * @param proposed proposed immutable scheduler configuration model
   * @param facts immutable runtime facts captured for this validation
   * @return compiled plan and validation issues
   */
  public CompileResult compile(CSConfigModel proposed, ClusterFacts facts) {
    ValidatedQueuePlan plan = ValidatedQueuePlan.fromModel(proposed);
    return new CompileResult(plan,
        validatePlan(proposed, facts, plan).getIssues());
  }

  /**
   * Evaluates validation rules against an already compiled plan.
   *
   * @param proposed proposed immutable scheduler configuration model
   * @param facts immutable runtime facts captured for this validation
   * @param plan immutable plan compiled from the proposed model
   * @return structured validation result
   */
  public ValidationResult validatePlan(CSConfigModel proposed,
      ClusterFacts facts, ValidatedQueuePlan plan) {
    ValidationContext context = new ValidationContext(proposed, facts, plan);
    List<ValidationIssue> issues = new ArrayList<>();
    for (ConfigDiagnostic diagnostic : proposed.getDiagnostics()) {
      addDiagnostic(diagnostic, issues);
    }
    for (ConfigDiagnostic diagnostic : plan.getCompilationDiagnostics()) {
      addDiagnostic(diagnostic, issues);
    }
    runStage(ValidationRule.Stage.MODEL, context, issues);
    if (issues.stream().noneMatch(issue ->
        issue.getSeverity() == ValidationIssue.Severity.ERROR)) {
      runStage(ValidationRule.Stage.HIERARCHY, context, issues);
    }
    return new ValidationResult(issues);
  }

  private void addDiagnostic(ConfigDiagnostic diagnostic,
      List<ValidationIssue> issues) {
    ValidationIssue.Severity severity = DIAGNOSTIC_SEVERITIES.getOrDefault(
        diagnostic.getCode(), ValidationIssue.Severity.ERROR);
    issues.add(new ValidationIssue(diagnostic.getQueuePath(),
        diagnostic.getPropertyKey(), diagnostic.getCode(), severity,
        diagnostic.getMessage()));
  }

  private void runStage(ValidationRule.Stage stage, ValidationContext context,
      List<ValidationIssue> issues) {
    rules.stream().filter(rule -> rule.stage() == stage)
        .forEach(rule -> rule.run(context, issues::add));
  }
}
