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

import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.conf.model.CSConfigModel;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerQueueManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CSQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.rules.AbsoluteParentMinCoverageRule;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.rules.CapacityModeUniformityRule;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.rules.CapacityVectorUpdateRule;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.rules.ChildrenCapacitySumRule;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.rules.HierarchyTransitionRule;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.rules.MemoryAllocationRule;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.rules.NestedManagedParentRule;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.rules.PlacementRuleDuplicatesRule;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.rules.PlacementRulesParseRule;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.rules.QueueNameRule;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.rules.VcoresAllocationRule;

/** Capacity Scheduler validation and compiled-plan entry points. */
public final class CSConfigValidationEngine {
  private final List<ValidationRule> rules;

  public CSConfigValidationEngine() {
    this(List.of(new MemoryAllocationRule(), new VcoresAllocationRule(),
        new PlacementRuleDuplicatesRule(), new QueueNameRule(),
        new CapacityModeUniformityRule(), new ChildrenCapacitySumRule(),
        new NestedManagedParentRule(), new HierarchyTransitionRule(),
        new AbsoluteParentMinCoverageRule(),
        new PlacementRulesParseRule(), new CapacityVectorUpdateRule()));
  }

  CSConfigValidationEngine(List<ValidationRule> rules) {
    this.rules = List.copyOf(rules);
  }

  public ValidationResult validate(CSConfigModel proposed,
      ClusterFacts facts) {
    ValidationContext context = new ValidationContext(proposed, facts);
    List<ValidationIssue> issues = new ArrayList<>();
    proposed.getDiagnostics().stream().map(ValidationDiagnosticSupport::issue)
        .forEach(issues::add);
    runStage(ValidationRule.Stage.MODEL, context, issues);
    if (issues.stream().noneMatch(issue ->
        issue.getSeverity() == ValidationIssue.Severity.ERROR)) {
      buildHierarchy(context, issues);
    }
    if (issues.stream().noneMatch(issue ->
        issue.getSeverity() == ValidationIssue.Severity.ERROR)) {
      runStage(ValidationRule.Stage.HIERARCHY, context, issues);
    }
    return new ValidationResult(issues);
  }

  /**
   * Compiles and validates an immutable plan when every validation dependency
   * is represented by pure built-in logic.
   *
   * <p>An ineligible result contains deterministic fallback reasons and no
   * plan. An invalid result contains structured issues and no plan. Therefore
   * every plan returned by this method has already passed plan-only
   * validation against the captured facts.</p>
   *
   * @param proposed immutable candidate configuration model
   * @param facts relevant runtime facts captured for the same candidate
   * @return validated plan or an explicit fail-closed result
   */
  public CompileResult compile(CSConfigModel proposed, ClusterFacts facts) {
    CSConfigCompatibilityClassifier.Classification classification =
        new CSConfigCompatibilityClassifier().classify(proposed, facts);
    if (!classification.isEligible()) {
      return new CompileResult(null, List.of(), classification.reasons());
    }
    try {
      ValidatedQueuePlan candidate = QueuePlanCompiler.compile(proposed, facts);
      ValidationResult validation = new CompiledQueuePlanValidator()
          .validate(candidate);
      ValidatedQueuePlan validated = validation.isValid() ? candidate : null;
      return new CompileResult(validated, validation.getIssues(), List.of());
    } catch (RuntimeException unmodeled) {
      String message = unmodeled.getMessage() == null
          ? unmodeled.getClass().getSimpleName() : unmodeled.getMessage();
      LegacyFallbackReason reason = new LegacyFallbackReason(
          LegacyFallbackReason.Code.UNMODELED_VALIDATION_DEPENDENCY, null,
          null, unmodeled.getClass().getName(),
          "Compiled validation could not represent candidate: " + message);
      return new CompileResult(null, List.of(), List.of(reason));
    }
  }

  private void buildHierarchy(ValidationContext context,
      List<ValidationIssue> issues) {
    try {
      ValidationQueueBuildContext buildContext =
          new ValidationQueueBuildContext(context.getModel(), context.getFacts());
      CSQueue proposedRoot = CapacitySchedulerQueueManager
          .buildQueueTreeForValidation(buildContext,
          buildContext.getConfiguration());
      context.attachBuiltTree(proposedRoot, buildContext);
    } catch (Throwable failure) {
      issues.add(new ValidationIssue(null, null, "queue-tree-build",
          ValidationIssue.Severity.ERROR,
          failure.getMessage() == null ? failure.getClass().getSimpleName()
              : failure.getMessage()));
    }
  }

  private void runStage(ValidationRule.Stage stage, ValidationContext context,
      List<ValidationIssue> issues) {
    rules.stream().filter(rule -> rule.stage() == stage)
        .forEach(rule -> rule.run(context, issues::add));
  }
}
