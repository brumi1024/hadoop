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
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.QueueState;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CSQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueuePath;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueuePrefixes;
import org.apache.hadoop.yarn.util.resource.DefaultResourceCalculator;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestCSConfigValidationEngine {
  private static final QueuePath ROOT = new QueuePath("root");
  private static final QueuePath A = new QueuePath("root.a");
  private final CSConfigValidationEngine engine =
      new CSConfigValidationEngine();

  @Test
  public void testValidPercentageTree() {
    CapacitySchedulerConfiguration conf = conf();
    conf.setQueues(ROOT, new String[]{"a", "b"});
    conf.setCapacity(A, 50);
    conf.setCapacity(new QueuePath("root.b"), 50);

    ValidationResult result = engine.validate(conf.getModel(),
        ClusterFacts.empty());
    assertTrue(result.isValid(), result.getIssues().toString());
  }

  @Test
  public void testChildrenCapacitySumProducesStableRuleId() {
    CapacitySchedulerConfiguration conf = conf();
    conf.setQueues(ROOT, new String[]{"a", "b"});
    conf.setCapacity(A, 25);
    conf.setCapacity(new QueuePath("root.b"), 25);

    ValidationResult result = engine.validate(conf.getModel(),
        ClusterFacts.empty());

    assertFalse(result.isValid());
    assertTrue(result.getIssues().stream().anyMatch(issue ->
        "children-capacity-sum".equals(issue.getRuleId())));
  }

  @Test
  public void testAbsoluteParentCoverageProducesStableRuleIdAndMessage() {
    CapacitySchedulerConfiguration conf = conf();
    QueuePath a1 = new QueuePath("root.a.a1");
    QueuePath a2 = new QueuePath("root.a.a2");
    conf.setQueues(ROOT, new String[]{"a"});
    conf.setQueues(A, new String[]{"a1", "a2"});
    conf.setMinimumResourceRequirement("", A,
        Resource.newInstance(50, 5));
    conf.setMinimumResourceRequirement("", a1,
        Resource.newInstance(50, 5));
    conf.setMinimumResourceRequirement("", a2,
        Resource.newInstance(50, 5));

    ValidationResult result = engine.validate(conf.getModel(),
        ClusterFacts.empty());

    assertFalse(result.isValid());
    assertTrue(result.getIssues().stream().anyMatch(issue ->
        "absolute-parent-min-coverage".equals(issue.getRuleId())
            && ("Parent Queues capacity: <memory:50, vCores:5> is less "
                + "than to its children:<memory:100, vCores:10> for queue:a")
                .equals(issue.getMessage())));
  }

  @Test
  public void testNonportableQueueNameIsWarningOnly() {
    CapacitySchedulerConfiguration conf = conf();
    conf.setQueues(ROOT, new String[]{"wéird"});
    conf.setCapacity(new QueuePath("root.wéird"), 100);

    ValidationResult result = engine.validate(conf.getModel(),
        ClusterFacts.empty());

    assertTrue(result.isValid(), result.getIssues().toString());
    assertTrue(result.getIssues().stream().anyMatch(issue ->
        "queue-name".equals(issue.getRuleId())
            && issue.getSeverity() == ValidationIssue.Severity.WARNING));
  }

  @Test
  public void testEmbeddedDotInQueueListIsError() {
    CapacitySchedulerConfiguration conf = conf();
    conf.setQueues(ROOT, new String[]{"a.b"});
    conf.setCapacity(new QueuePath("root.a.b"), 100);

    ValidationResult result = engine.validate(conf.getModel(),
        ClusterFacts.empty());

    assertFalse(result.isValid());
    assertTrue(result.getIssues().stream().anyMatch(issue ->
        "queue-name".equals(issue.getRuleId())
            && issue.getSeverity() == ValidationIssue.Severity.ERROR
            && issue.getMessage().contains("embedded dot")));
  }

  @Test
  public void testDuplicatePlacementRulesAreRejected() {
    CapacitySchedulerConfiguration conf = conf();
    conf.setQueues(ROOT, new String[]{"a"});
    conf.setCapacity(A, 100);
    conf.set(CapacitySchedulerConfiguration.QUEUE_MAPPING,
        "u:alice:root.a,u:alice:root.a");

    ValidationResult result = engine.validate(conf.getModel(),
        ClusterFacts.empty());

    assertFalse(result.isValid());
    assertTrue(result.getIssues().stream().anyMatch(issue ->
        "placement-rule-duplicates".equals(issue.getRuleId())));
  }

  @Test
  public void testPlacementRuleTargetMustExist() {
    CapacitySchedulerConfiguration conf = conf();
    conf.setQueues(ROOT, new String[]{"a"});
    conf.setCapacity(A, 100);
    conf.set(CapacitySchedulerConfiguration.QUEUE_MAPPING,
        "u:alice:missing");

    ValidationResult result = engine.validate(conf.getModel(),
        ClusterFacts.empty());

    assertFalse(result.isValid());
    assertTrue(result.getIssues().stream().anyMatch(issue ->
        "placement-rules-parse".equals(issue.getRuleId())));
  }

  @Test
  public void testPlacementRuleTargetingExistingV1AutoCreatedLeafIsAllowed() {
    CapacitySchedulerConfiguration conf = conf();
    conf.setMappingRuleFormat(
        CapacitySchedulerConfiguration.MAPPING_RULE_FORMAT_LEGACY);
    conf.setQueues(ROOT, new String[] {"b"});
    conf.setCapacity(new QueuePath("root.b"), 100);
    conf.set(CapacitySchedulerConfiguration.QUEUE_MAPPING,
        "u:alice:a");

    ClusterFacts facts = ClusterFacts.builder()
        .withOldQueue(new QueuePath("root.a"), ClusterFacts.QueueKind.LEAF,
            QueueState.RUNNING, false, true)
        .build();

    ValidationResult result = engine.validate(conf.getModel(), facts);

    assertTrue(result.getIssues().stream().noneMatch(issue ->
        "placement-rules-parse".equals(issue.getRuleId())
            && issue.getSeverity() == ValidationIssue.Severity.ERROR),
        result.getIssues().toString());
  }

  @Test
  public void testManagedParentCannotContainManagedParent() {
    CapacitySchedulerConfiguration conf = conf();
    conf.setQueues(ROOT, new String[]{"a"});
    conf.setCapacity(A, 100);
    conf.setAutoCreateChildQueueEnabled(ROOT, true);
    conf.setAutoCreateChildQueueEnabled(A, true);

    ValidationResult result = engine.validate(conf.getModel(),
        ClusterFacts.empty());

    assertFalse(result.isValid());
    assertTrue(result.getIssues().stream().anyMatch(issue ->
        "nested-managed-parent".equals(issue.getRuleId())));
  }

  @Test
  public void testMalformedMemoryAllocationIsStructuredIssue() {
    CapacitySchedulerConfiguration conf = conf();
    conf.set(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB,
        "not-an-integer");

    ValidationResult result = engine.validate(conf.getModel(),
        ClusterFacts.empty());

    assertFalse(result.isValid());
    assertTrue(result.getIssues().stream().anyMatch(issue ->
        "memory-allocation".equals(issue.getRuleId())
            && YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB.equals(
                issue.getPropertyKey())));
  }

  @Test
  public void testMalformedVcoresAllocationIsStructuredIssue() {
    CapacitySchedulerConfiguration conf = conf();
    conf.set(YarnConfiguration.RM_SCHEDULER_MAXIMUM_ALLOCATION_VCORES,
        "not-an-integer");

    ValidationResult result = engine.validate(conf.getModel(),
        ClusterFacts.empty());

    assertFalse(result.isValid());
    assertTrue(result.getIssues().stream().anyMatch(issue ->
        "vcores-allocation".equals(issue.getRuleId())
            && YarnConfiguration.RM_SCHEDULER_MAXIMUM_ALLOCATION_VCORES.equals(
                issue.getPropertyKey())));
  }

  @Test
  public void testAllocationValuesMatchConfigurationIntegerSyntax() {
    CapacitySchedulerConfiguration conf = conf();
    conf.setQueues(ROOT, new String[]{"a"});
    conf.setCapacity(A, 100);
    conf.set(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB,
        " 0x400 ");
    conf.set(YarnConfiguration.RM_SCHEDULER_MAXIMUM_ALLOCATION_VCORES,
        " 4 ");

    ValidationResult result = engine.validate(conf.getModel(),
        ClusterFacts.empty());

    assertTrue(result.isValid(), result.getIssues().toString());
  }

  @Test
  public void testManagedParentUsesIsolatedQueueTreeBuild() {
    CapacitySchedulerConfiguration conf = conf();
    conf.setQueues(ROOT, new String[]{"a"});
    conf.setCapacity(A, 100);
    conf.setAutoCreateChildQueueEnabled(ROOT, true);

    ValidationResult result = engine.validate(conf.getModel(),
        ClusterFacts.empty());

    assertTrue(result.isValid(), result.getIssues().toString());
  }

  @Test
  public void testProposedAllocationBoundsAreValidated() {
    CapacitySchedulerConfiguration conf = conf();
    conf.setQueues(ROOT, new String[]{"a"});
    conf.setCapacity(A, 100);
    conf.setInt(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB, 4096);
    conf.setInt(YarnConfiguration.RM_SCHEDULER_MAXIMUM_ALLOCATION_MB, 1024);

    ValidationResult result = engine.validate(conf.getModel(),
        ClusterFacts.empty());

    assertFalse(result.isValid());
    assertTrue(result.getIssues().stream().anyMatch(issue ->
        "memory-allocation".equals(issue.getRuleId())));
  }

  @Test
  public void testLowercaseStoppedDoesNotAuthorizeDeletion() {
    CapacitySchedulerConfiguration conf = conf();
    conf.setQueues(ROOT, new String[]{"b"});
    conf.setCapacity(new QueuePath("root.b"), 100);
    conf.set("yarn.scheduler.capacity.root.a.state", "stopped");
    ClusterFacts facts = ClusterFacts.builder()
        .withOldQueue(A, ClusterFacts.QueueKind.LEAF,
            QueueState.RUNNING, false)
        .build();

    ValidationResult result = engine.validate(conf.getModel(), facts);

    assertFalse(result.isValid());
    assertTrue(result.getIssues().stream().anyMatch(issue ->
        "queue-deletion-requires-stopped".equals(issue.getRuleId())),
        result.getIssues().toString());
  }

  @Test
  public void testInvalidBooleanIsWarningAndHierarchyStillRuns() {
    CapacitySchedulerConfiguration conf = conf();
    conf.setQueues(ROOT, new String[]{"b"});
    conf.setCapacity(new QueuePath("root.b"), 100);
    conf.set(CapacitySchedulerConfiguration.PREFIX
        + "legacy-queue-mode.enabled", "not-a-boolean");
    ClusterFacts facts = ClusterFacts.builder()
        .withOldQueue(A, ClusterFacts.QueueKind.LEAF,
            QueueState.RUNNING, false)
        .build();

    ValidationResult result = engine.validate(conf.getModel(), facts);

    assertFalse(result.isValid());
    assertTrue(result.getIssues().stream().anyMatch(issue ->
        "invalid-boolean".equals(issue.getRuleId())
            && issue.getSeverity() == ValidationIssue.Severity.WARNING));
    assertTrue(result.getIssues().stream().anyMatch(issue ->
        "queue-deletion-requires-stopped".equals(issue.getRuleId())),
        result.getIssues().toString());
  }

  @Test
  public void testInvalidBooleanAloneIsWarningOnly() {
    CapacitySchedulerConfiguration conf = conf();
    conf.setQueues(ROOT, new String[] {"a"});
    conf.setCapacity(A, 100);
    conf.set(CapacitySchedulerConfiguration.PREFIX
        + "legacy-queue-mode.enabled", "not-a-boolean");

    ValidationResult result = engine.validate(conf.getModel(),
        ClusterFacts.empty());

    assertTrue(result.isValid(), result.getIssues().toString());
    assertEquals(1, result.getIssues().size(), result.getIssues().toString());
    assertEquals("invalid-boolean", result.getIssues().get(0).getRuleId());
    assertEquals(ValidationIssue.Severity.WARNING,
        result.getIssues().get(0).getSeverity());
  }

  @Test
  public void testNonBooleanModelDiagnosticsRemainErrors() {
    CapacitySchedulerConfiguration conf = conf();
    conf.setQueues(ROOT, new String[]{"a"});
    conf.setCapacity(A, "not-a-capacity");
    conf.set(QueuePrefixes.getQueuePrefix(A)
        + CapacitySchedulerConfiguration.STATE, "not-a-state");
    conf.set(QueuePrefixes.getQueuePrefix(A)
        + CapacitySchedulerConfiguration.MAXIMUM_AM_RESOURCE_SUFFIX,
        "not-a-float");
    conf.set(CapacitySchedulerConfiguration.PREFIX
        + CapacitySchedulerConfiguration.MAXIMUM_QUEUE_DEPTH,
        "not-an-integer");

    ValidationResult result = engine.validate(conf.getModel(),
        ClusterFacts.empty());

    for (String code : new String[]{"invalid-capacity", "invalid-queue-state",
        "invalid-float", "invalid-integer"}) {
      assertEquals(ValidationIssue.Severity.ERROR,
          result.getIssues().stream()
              .filter(issue -> code.equals(issue.getRuleId()))
              .findFirst().orElseThrow().getSeverity(), code);
    }
  }

  @Test
  public void testRemovedQueueCanBeStoppedInProposedConfiguration() {
    CapacitySchedulerConfiguration conf = conf();
    conf.setQueues(ROOT, new String[]{"b"});
    conf.setCapacity(new QueuePath("root.b"), 100);
    conf.set("yarn.scheduler.capacity.root.a.state", "STOPPED");
    ClusterFacts facts = ClusterFacts.builder()
        .withOldQueue(A, ClusterFacts.QueueKind.LEAF,
            QueueState.RUNNING, false)
        .build();

    ValidationResult result = engine.validate(conf.getModel(), facts);

    assertTrue(result.isValid(), result.getIssues().toString());
  }

  @Test
  public void testDuplicateLeafNameDeletionIsDeletionOnly() {
    CapacitySchedulerConfiguration conf = conf();
    QueuePath moved = new QueuePath("root.b.a");
    conf.setQueues(ROOT, new String[]{"b"});
    conf.setQueues(new QueuePath("root.b"), new String[]{"a"});
    conf.setCapacity(new QueuePath("root.b"), 100);
    conf.setCapacity(moved, 100);
    ClusterFacts facts = ClusterFacts.builder()
        .withOldQueue(A, ClusterFacts.QueueKind.LEAF,
            QueueState.RUNNING, false)
        .build();

    ValidationResult result = engine.validate(conf.getModel(), facts);

    assertTrue(result.getIssues().stream().anyMatch(issue ->
        "queue-deletion-requires-stopped".equals(issue.getRuleId())),
        result.getIssues().toString());
    assertTrue(result.getIssues().stream().noneMatch(issue ->
        "no-queue-move".equals(issue.getRuleId())));
  }

  @Test
  public void testStoppedDuplicateLeafNameDeletionIsValid() {
    CapacitySchedulerConfiguration conf = conf();
    QueuePath moved = new QueuePath("root.b.a");
    conf.setQueues(ROOT, new String[]{"b"});
    conf.setQueues(new QueuePath("root.b"), new String[]{"a"});
    conf.setCapacity(new QueuePath("root.b"), 100);
    conf.setCapacity(moved, 100);
    conf.set(QueuePrefixes.getQueuePrefix(A)
        + CapacitySchedulerConfiguration.STATE, "STOPPED");
    ClusterFacts facts = ClusterFacts.builder()
        .withOldQueue(A, ClusterFacts.QueueKind.LEAF,
            QueueState.RUNNING, false)
        .build();

    ValidationResult result = engine.validate(conf.getModel(), facts);

    assertTrue(result.isValid(), result.getIssues().toString());
  }

  @Test
  public void testAbsentAutoCreatedLeafDoesNotRequireDeletionStop() {
    CapacitySchedulerConfiguration conf = conf();
    conf.setQueues(ROOT, new String[]{"b"});
    conf.setCapacity(new QueuePath("root.b"), 100);
    ClusterFacts facts = ClusterFacts.builder()
        .withOldQueue(A, ClusterFacts.QueueKind.LEAF,
            QueueState.RUNNING, true, true)
        .build();

    ValidationResult result = engine.validate(conf.getModel(), facts);

    assertTrue(result.isValid(), result.getIssues().toString());
  }

  @Test
  public void testAbsentDynamicQueueDoesNotRequireDeletionStop() {
    CapacitySchedulerConfiguration conf = conf();
    conf.setQueues(ROOT, new String[]{"b"});
    conf.setCapacity(new QueuePath("root.b"), 100);
    ClusterFacts facts = ClusterFacts.builder()
        .withOldQueue(A, ClusterFacts.QueueKind.LEAF,
            QueueState.RUNNING, true)
        .build();

    ValidationResult result = engine.validate(conf.getModel(), facts);

    assertTrue(result.isValid(), result.getIssues().toString());
  }

  @Test
  public void testHierarchyRulesSeeBuiltTreeAndContext() {
    CapacitySchedulerConfiguration conf = conf();
    conf.setQueues(ROOT, new String[] {"a"});
    conf.setCapacity(A, 100);
    AtomicReference<CSQueue> root = new AtomicReference<>();
    AtomicReference<ValidationQueueBuildContext> buildContext =
        new AtomicReference<>();
    ValidationRule rule = new ValidationRule() {
      @Override
      public String id() {
        return "test-built-tree";
      }

      @Override
      public Stage stage() {
        return Stage.HIERARCHY;
      }

      @Override
      public void run(ValidationContext context,
          java.util.function.Consumer<ValidationIssue> issueSink) {
        root.set(context.getProposedRoot());
        buildContext.set(context.getBuildContext());
      }
    };

    ValidationResult result = new CSConfigValidationEngine(List.of(rule))
        .validate(conf.getModel(), ClusterFacts.empty());

    assertTrue(result.isValid(), result.getIssues().toString());
    assertNotNull(root.get());
    assertNotNull(buildContext.get());
  }

  @Test
  public void testCapacityUpdateIsSkippedForEmptyFacts() {
    CapacitySchedulerConfiguration conf = conf();
    conf.setQueues(ROOT, new String[] {"a"});
    conf.setCapacity(A, 100);

    ValidationResult result = engine.validate(conf.getModel(),
        ClusterFacts.empty());

    assertTrue(result.getIssues().isEmpty(), result.getIssues().toString());
  }

  @Test
  public void testEmptyQueueListComponentIsAnError() {
    CapacitySchedulerConfiguration conf = conf();
    conf.set(QueuePrefixes.getQueuePrefix(ROOT)
        + CapacitySchedulerConfiguration.QUEUES, "a,,b");

    ValidationResult result = engine.validate(conf.getModel(),
        ClusterFacts.empty());

    assertTrue(result.getIssues().stream().anyMatch(issue ->
        "queue-name".equals(issue.getRuleId())
            && issue.getSeverity() == ValidationIssue.Severity.ERROR
            && "Queue list contains an empty component".equals(
                issue.getMessage())), result.getIssues().toString());
  }

  @Test
  public void testCapacityUpdateReportsAbsoluteTreeWarnings() {
    CapacitySchedulerConfiguration conf = conf();
    conf.setQueues(ROOT, new String[] {"a", "b"});
    conf.setMinimumResourceRequirement("", A, Resource.newInstance(80, 8));
    conf.setMinimumResourceRequirement("", new QueuePath("root.b"),
        Resource.newInstance(80, 8));
    Resource cluster = Resource.newInstance(100, 10);
    ClusterFacts facts = ClusterFacts.builder()
        .withResources(cluster, Resource.newInstance(1, 1),
            Resource.newInstance(1000, 100), new DefaultResourceCalculator())
        .withResourcesByLabel(Map.of("", cluster))
        .build();

    ValidationResult result = engine.validate(conf.getModel(), facts);

    assertTrue(result.isValid(), result.getIssues().toString());
    assertTrue(result.getIssues().stream().anyMatch(issue ->
        "capacity-update-branch-downscaled".equals(issue.getRuleId())),
        result.getIssues().toString());
  }

  @Test
  public void testCapacityUpdateHealthyTreeHasNoIssues() {
    CapacitySchedulerConfiguration conf = conf();
    conf.setQueues(ROOT, new String[] {"a", "b"});
    conf.setCapacity(A, 50);
    conf.setCapacity(new QueuePath("root.b"), 50);
    Resource cluster = Resource.newInstance(100, 10);
    ClusterFacts facts = ClusterFacts.builder()
        .withResources(cluster, Resource.newInstance(1, 1),
            Resource.newInstance(1000, 100), new DefaultResourceCalculator())
        .withResourcesByLabel(Map.of("", cluster))
        .build();

    ValidationResult result = engine.validate(conf.getModel(), facts);

    assertTrue(result.isValid(), result.getIssues().toString());
    assertTrue(result.getIssues().isEmpty(), result.getIssues().toString());
  }

  @Test
  public void testCapacityUpdateReportsLabeledPartitionWarnings() {
    CapacitySchedulerConfiguration conf = conf();
    QueuePath b = new QueuePath("root.b");
    conf.setQueues(ROOT, new String[] {"a", "b"});
    conf.setCapacity(A, "[memory=80,vcores=8]");
    conf.setCapacity(b, "[memory=80,vcores=8]");
    conf.setAccessibleNodeLabels(ROOT, Set.of("GPU"));
    conf.setAccessibleNodeLabels(A, Set.of("GPU"));
    conf.setAccessibleNodeLabels(b, Set.of("GPU"));
    conf.setCapacityByLabel(ROOT, "GPU", "[memory=100,vcores=10]");
    conf.setCapacityByLabel(A, "GPU", "[memory=80,vcores=8]");
    conf.setCapacityByLabel(b, "GPU", "[memory=80,vcores=8]");
    Resource cluster = Resource.newInstance(100, 10);
    Resource gpu = Resource.newInstance(10, 2);
    ClusterFacts facts = ClusterFacts.builder()
        .withResources(cluster, Resource.newInstance(1, 1),
            Resource.newInstance(1000, 100), new DefaultResourceCalculator())
        .withNodeLabels(Set.of("GPU"))
        .withResourcesByLabel(Map.of("", cluster, "GPU", gpu))
        .build();

    ValidationResult result = engine.validate(conf.getModel(), facts);

    assertTrue(result.isValid(), result.getIssues().toString());
    assertTrue(result.getIssues().stream().anyMatch(issue ->
        "capacity-update-branch-downscaled".equals(issue.getRuleId())),
        result.getIssues().toString());
    assertTrue(result.getIssues().stream().noneMatch(issue ->
        "capacity-update-failure".equals(issue.getRuleId())),
        result.getIssues().toString());
  }

  @Test
  public void testCapacityUpdateLegacyModeParitySpotCheck() {
    CapacitySchedulerConfiguration legacy = conf();
    CapacitySchedulerConfiguration nonLegacy = conf();
    QueuePath b = new QueuePath("root.b");
    for (CapacitySchedulerConfiguration configuration :
        new CapacitySchedulerConfiguration[] {legacy, nonLegacy}) {
      configuration.setQueues(ROOT, new String[] {"a", "b"});
      configuration.setMinimumResourceRequirement("", A,
          Resource.newInstance(80, 8));
      configuration.setMinimumResourceRequirement("", b,
          Resource.newInstance(80, 8));
    }
    legacy.setLegacyQueueModeEnabled(true);
    nonLegacy.setLegacyQueueModeEnabled(false);
    Resource cluster = Resource.newInstance(100, 10);
    ClusterFacts facts = ClusterFacts.builder()
        .withResources(cluster, Resource.newInstance(1, 1),
            Resource.newInstance(1000, 100), new DefaultResourceCalculator())
        .withResourcesByLabel(Map.of("", cluster))
        .build();

    ValidationResult legacyResult = engine.validate(legacy.getModel(), facts);
    ValidationResult nonLegacyResult = engine.validate(nonLegacy.getModel(), facts);

    assertTrue(legacyResult.isValid(), legacyResult.getIssues().toString());
    assertTrue(nonLegacyResult.isValid(), nonLegacyResult.getIssues().toString());
    assertTrue(legacyResult.getIssues().stream().noneMatch(issue ->
        "capacity-update-failure".equals(issue.getRuleId())),
        legacyResult.getIssues().toString());
    assertTrue(nonLegacyResult.getIssues().stream().noneMatch(issue ->
        "capacity-update-failure".equals(issue.getRuleId())),
        nonLegacyResult.getIssues().toString());
    Set<String> legacyWarnings = legacyResult.getIssues().stream()
            .map(ValidationIssue::getRuleId)
            .collect(Collectors.toSet());
    Set<String> nonLegacyWarnings = nonLegacyResult.getIssues().stream()
            .map(ValidationIssue::getRuleId)
            .collect(Collectors.toSet());
    assertFalse(legacyWarnings.isEmpty(), legacyResult.getIssues().toString());
    assertEquals(legacyWarnings, nonLegacyWarnings);
  }

  private CapacitySchedulerConfiguration conf() {
    return new CapacitySchedulerConfiguration(new Configuration(false), false);
  }
}
