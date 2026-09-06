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
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.QueueState;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.LeafQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.ManagedParentQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.ParentQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.PlanQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueuePath;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueuePrefixes;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.ReservationQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.conf.model.CSConfigModel;
import org.apache.hadoop.yarn.util.resource.DefaultResourceCalculator;
import org.apache.hadoop.yarn.util.resource.DominantResourceCalculator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import org.mockito.MockedConstruction;
import org.mockito.Mockito;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Differential and side-effect checks for immutable plan validation. */
public class TestCompiledQueuePlanValidation {
  private static final QueuePath ROOT = new QueuePath("root");
  private static final QueuePath A = new QueuePath("root.a");
  private static final QueuePath B = new QueuePath("root.b");
  private final CSConfigValidationEngine engine =
      new CSConfigValidationEngine();

  @Test
  public void testPercentageAcceptanceMatchesLegacyWithoutQueueConstruction() {
    CapacitySchedulerConfiguration conf = percentageTree();

    try (MockedConstruction<LeafQueue> leaves =
             Mockito.mockConstruction(LeafQueue.class);
         MockedConstruction<ParentQueue> parents =
             Mockito.mockConstruction(ParentQueue.class);
         MockedConstruction<ManagedParentQueue> managed =
             Mockito.mockConstruction(ManagedParentQueue.class);
         MockedConstruction<PlanQueue> plans =
             Mockito.mockConstruction(PlanQueue.class);
         MockedConstruction<ReservationQueue> reservations =
             Mockito.mockConstruction(ReservationQueue.class)) {
      CompileResult compiled = engine.compile(conf.getModel(),
          ClusterFacts.empty());

      assertTrue(compiled.isCompiledActivationEligible(),
          compiled.getFallbackReasons() + " " + compiled.getIssues());
      assertNotNull(compiled.getPlan());
      assertTrue(leaves.constructed().isEmpty());
      assertTrue(parents.constructed().isEmpty());
      assertTrue(managed.constructed().isEmpty());
      assertTrue(plans.constructed().isEmpty());
      assertTrue(reservations.constructed().isEmpty());
    }
  }

  @Test
  public void testPercentageSumRejectionMatchesLegacy() {
    CapacitySchedulerConfiguration conf = conf();
    conf.setQueues(ROOT, new String[] {"a", "b"});
    conf.setCapacity(A, 25);
    conf.setCapacity(B, 25);

    assertParity(conf, ClusterFacts.empty());
  }

  @Test
  public void testWeightTreeMatchesLegacy() {
    CapacitySchedulerConfiguration conf = conf();
    conf.setQueues(ROOT, new String[] {"a"});
    conf.setNonLabeledQueueWeight(A, 1F);
    conf.setQueues(A, new String[] {"a1", "a2"});
    conf.setNonLabeledQueueWeight(new QueuePath("root.a.a1"), 1F);
    conf.setNonLabeledQueueWeight(new QueuePath("root.a.a2"), 2F);

    assertParity(conf, ClusterFacts.empty());
  }

  @Test
  public void testAbsoluteTreeMatchesLegacy() {
    CapacitySchedulerConfiguration conf = conf();
    QueuePath a1 = new QueuePath("root.a.a1");
    QueuePath a2 = new QueuePath("root.a.a2");
    conf.setQueues(ROOT, new String[] {"a"});
    conf.setMinimumResourceRequirement("", A,
        Resource.newInstance(100, 10));
    conf.setQueues(A, new String[] {"a1", "a2"});
    conf.setMinimumResourceRequirement("", a1,
        Resource.newInstance(50, 5));
    conf.setMinimumResourceRequirement("", a2,
        Resource.newInstance(50, 5));

    assertParity(conf, ClusterFacts.empty());
  }

  @Test
  public void testMixedVectorTreeMatchesLegacy() {
    CapacitySchedulerConfiguration conf = conf();
    conf.setLegacyQueueModeEnabled(false);
    conf.setQueues(ROOT, new String[] {"a", "b"});
    conf.setCapacity(A, "[memory=50%,vcores=1w]");
    conf.setCapacity(B, "[memory=50%,vcores=1w]");

    assertParity(conf, facts(Resource.newInstance(100, 10)));
  }

  @Test
  public void testInheritedAbsoluteMaximumMatchesLegacy() {
    for (boolean legacyMode : List.of(true, false)) {
      for (String label : List.of("", "blue")) {
        CapacitySchedulerConfiguration conf = conf();
        conf.setLegacyQueueModeEnabled(legacyMode);
        QueuePath middle = new QueuePath("root.a.middle");
        QueuePath leaf = new QueuePath("root.a.middle.leaf");
        conf.setQueues(ROOT, new String[] {"a"});
        conf.setQueues(A, new String[] {"middle"});
        conf.setQueues(middle, new String[] {"leaf"});
        if (!label.isEmpty()) {
          conf.setAccessibleNodeLabels(ROOT, Set.of(label));
        }
        for (QueuePath path : List.of(A, middle, leaf)) {
          conf.setMinimumResourceRequirement("", path,
              Resource.newInstance(1024, 1));
          if (!label.isEmpty()) {
            conf.setMinimumResourceRequirement(label, path,
                Resource.newInstance(1024, 1));
          }
        }
        conf.setMaximumResourceRequirement(label, A,
            Resource.newInstance(8192, 8));
        conf.setMaximumResourceRequirement(label, leaf,
            Resource.newInstance(16384, 16));
        ClusterFacts runtime = ClusterFacts.builder()
            .withNodeLabels(Set.of("blue"))
            .withResources(Resource.newInstance(32768, 32),
                Resource.newInstance(1, 1),
                Resource.newInstance(32768, 32),
                new DefaultResourceCalculator())
            .withResourcesByLabel(Map.of("",
                Resource.newInstance(32768, 32), "blue",
                Resource.newInstance(32768, 32)))
            .build();
        assertFalse(engine.validate(conf.getModel(), runtime).isValid());
        assertParity(conf, runtime);
        conf.setMaximumResourceRequirement(label, leaf,
            Resource.newInstance(8192, 8));
        assertParity(conf, runtime);
      }
    }
  }

  @Test
  public void testVcoresOnlyAbsoluteMaximumMatchesLegacy() {
    CapacitySchedulerConfiguration conf = conf();
    conf.setQueues(ROOT, new String[] {"a"});
    conf.setCapacity(A, "[memory=0,vcores=8]");
    conf.setMaximumResourceRequirement("", A, Resource.newInstance(0, 4));
    conf.setResourceComparator(DominantResourceCalculator.class);
    ClusterFacts runtime = ClusterFacts.builder()
        .withResources(Resource.newInstance(16384, 16),
            Resource.newInstance(1, 1), Resource.newInstance(32768, 32),
            new DominantResourceCalculator())
        .build();
    assertFalse(engine.validate(conf.getModel(), runtime).isValid());
    assertParity(conf, runtime);
  }

  @Test
  public void testManagedParentConstructorCapacityTypesMatchLegacy() {
    for (boolean legacyMode : List.of(true, false)) {
      for (String parentCapacity : List.of("100", "1w")) {
        for (String templateCapacity : List.of(
            "[memory=1024,vcores=1]", "[memory=0,vcores=1]")) {
          CapacitySchedulerConfiguration conf = conf();
          conf.setLegacyQueueModeEnabled(legacyMode);
          conf.setQueues(ROOT, new String[] {"a"});
          conf.setCapacity(A, parentCapacity);
          conf.setAutoCreateChildQueueEnabled(A, true);
          setTemplateCapacity(conf, "", templateCapacity);
          assertEquals(templateCapacity.startsWith("[memory=0,"),
              engine.validate(conf.getModel(), ClusterFacts.empty()).isValid());
          assertParity(conf, ClusterFacts.empty());
        }
      }
    }
  }

  @Test
  public void testManagedTemplateScalarGetterBoundsMatchLegacy() {
    for (String label : List.of("", "blue")) {
      for (String value : List.of("101", "10001w")) {
        CapacitySchedulerConfiguration conf = conf();
        conf.setQueues(ROOT, new String[] {"a"});
        conf.setCapacity(A, 100);
        conf.setAccessibleNodeLabels(ROOT, Set.of("blue"));
        conf.setAutoCreateChildQueueEnabled(A, true);
        setTemplateCapacity(conf, label, value);
        assertFalse(engine.validate(conf.getModel(), ClusterFacts.empty()).isValid());
        assertParity(conf, ClusterFacts.empty());
      }
    }
    CapacitySchedulerConfiguration conf = conf();
    conf.setQueues(ROOT, new String[] {"a"});
    conf.setCapacity(A, 100);
    conf.setAccessibleNodeLabels(ROOT, Set.of("blue"));
    conf.setAutoCreateChildQueueEnabled(A, true);
    QueuePath template =
        QueuePrefixes.getAutoCreatedQueueObjectTemplateConfPrefix(A);
    conf.set(QueuePrefixes.getNodeLabelPrefix(template, "blue")
        + CapacitySchedulerConfiguration.MAXIMUM_CAPACITY, "101");
    assertFalse(engine.validate(conf.getModel(), ClusterFacts.empty()).isValid());
    assertParity(conf, ClusterFacts.empty());
  }

  private static void setTemplateCapacity(CapacitySchedulerConfiguration conf,
      String label, String value) {
    QueuePath template =
        QueuePrefixes.getAutoCreatedQueueObjectTemplateConfPrefix(A);
    conf.set(QueuePrefixes.getNodeLabelPrefix(template, label)
        + CapacitySchedulerConfiguration.CAPACITY, value);
  }

  @Test
  public void testZeroMemoryResourceDependencyMatchesLegacy() {
    CapacitySchedulerConfiguration conf = percentageTree();
    conf.setLegacyQueueModeEnabled(false);
    conf.setCapacity(A, "[memory=100%,vcores=6w]");
    conf.setCapacity(B, "[memory=100,vcores=5]");

    assertParity(conf, facts(Resource.newInstance(100, 10)));
  }

  @Test
  public void testStaticFullPathMappingsInitializeWithAmbiguousShortNames()
      throws Exception {
    CapacitySchedulerConfiguration conf = percentageTree();
    conf.setQueues(A, new String[] {"shared"});
    conf.setQueues(B, new String[] {"shared"});
    conf.setCapacity(new QueuePath("root.a.shared"), 100);
    conf.setCapacity(new QueuePath("root.b.shared"), 100);
    conf.set(CapacitySchedulerConfiguration.QUEUE_MAPPING,
        "u:%user:root.a.shared,g:users:root.b.shared");
    conf.setClass(YarnConfiguration.RM_SCHEDULER, CapacityScheduler.class,
        ResourceScheduler.class);
    CapacitySchedulerConfiguration exact =
        new CapacitySchedulerConfiguration(conf, false);

    QueueMetrics.clearQueueMetrics();
    try (MockRM rm = new MockRM(conf)) {
      rm.start();
      CapacityScheduler scheduler =
          (CapacityScheduler) rm.getResourceScheduler();
      scheduler.reinitializeValidatedConfiguration(exact, rm.getRMContext());
      assertTrue(scheduler.getCapacitySchedulerQueueManager()
          .isAmbiguous("shared"));
      assertNotNull(scheduler.getCSMappingPlacementRule());
      CompileResult result = engine.compile(exact.getModel(),
          ClusterFacts.capture(scheduler));
      assertTrue(result.isCompiledActivationEligible(),
          result.getFallbackReasons() + " " + result.getIssues());
    } finally {
      QueueMetrics.clearQueueMetrics();
    }
  }

  @Test
  public void testNodeLabelInheritanceMatchesLegacy() {
    CapacitySchedulerConfiguration conf = percentageTree();
    conf.setAccessibleNodeLabels(ROOT, Set.of("blue"));
    conf.setAccessibleNodeLabels(A, Set.of("blue"));
    conf.setAccessibleNodeLabels(B, Set.of("blue"));
    conf.setCapacityByLabel(ROOT, "blue", 100F);
    conf.setCapacityByLabel(A, "blue", 50F);
    conf.setCapacityByLabel(B, "blue", 50F);
    Resource cluster = Resource.newInstance(100, 10);
    ClusterFacts facts = ClusterFacts.builder()
        .withResources(cluster, Resource.newInstance(1, 1),
            Resource.newInstance(1000, 100),
            new DefaultResourceCalculator())
        .withNodeLabels(Set.of("blue"))
        .withResourcesByLabel(Map.of("", cluster, "blue",
            Resource.newInstance(20, 2)))
        .build();

    assertParity(conf, facts);
  }

  @Test
  public void testQueueMaximumAllocationRejectionMatchesLegacy() {
    CapacitySchedulerConfiguration conf = percentageTree();
    conf.set(QueuePrefixes.getQueuePrefix(A)
        + CapacitySchedulerConfiguration.MAXIMUM_ALLOCATION_MB, "16384");
    ClusterFacts facts = ClusterFacts.builder()
        .withResources(Resource.newInstance(100, 10),
            Resource.newInstance(1, 1), Resource.newInstance(8192, 8),
            new DefaultResourceCalculator())
        .build();

    assertParity(conf, facts);
  }

  @Test
  public void testUserWeightRejectionMatchesLegacy() {
    CapacitySchedulerConfiguration conf = percentageTree();
    conf.set(QueuePrefixes.getQueuePrefix(A) + "user-settings.alice.weight",
        "2.0");

    assertParity(conf, ClusterFacts.empty());
  }

  @Test
  public void testDefaultLabelExpressionRejectionMatchesLegacy() {
    CapacitySchedulerConfiguration conf = percentageTree();
    conf.setAccessibleNodeLabels(A, Set.of("blue"));
    conf.setDefaultNodeLabelExpression(A, "ssd");

    assertParity(conf, ClusterFacts.empty());
  }

  @Test
  public void testDeletionTransitionMatchesLegacy() {
    CapacitySchedulerConfiguration conf = conf();
    conf.setQueues(ROOT, new String[] {"b"});
    conf.setCapacity(B, 100F);
    ClusterFacts facts = ClusterFacts.builder()
        .withOldQueue(A, ClusterFacts.QueueKind.LEAF,
            QueueState.RUNNING, false)
        .build();

    assertParity(conf, facts);
  }

  @Test
  public void testInvalidPlanNeverEscapesCompileResult() {
    CapacitySchedulerConfiguration conf = conf();
    conf.setQueues(ROOT, new String[] {"a", "b"});
    conf.setCapacity(A, 10F);
    conf.setCapacity(B, 10F);

    CompileResult result = engine.compile(conf.getModel(),
        ClusterFacts.empty());

    assertFalse(result.isValid());
    assertFalse(result.isCompiledActivationEligible());
    assertNull(result.getPlan());
    assertTrue(result.getFallbackReasons().isEmpty());
    assertFalse(result.getIssues().isEmpty());
  }

  @Test
  public void testCandidateAndFactsFingerprintsAreStableAndComplete() {
    CapacitySchedulerConfiguration first = percentageTree();
    CapacitySchedulerConfiguration second = conf();
    second.setCapacity(B, 50F);
    second.setQueues(ROOT, new String[] {"a", "b"});
    second.setCapacity(A, 50F);
    CompileResult firstResult = engine.compile(first.getModel(),
        ClusterFacts.empty());
    CompileResult secondResult = engine.compile(second.getModel(),
        ClusterFacts.empty());

    assertTrue(firstResult.isCompiledActivationEligible(),
        firstResult.getFallbackReasons().toString());
    assertTrue(secondResult.isCompiledActivationEligible(),
        secondResult.getFallbackReasons().toString());
    assertEquals(firstResult.getPlan().getIdentity().candidateFingerprint(),
        secondResult.getPlan().getIdentity().candidateFingerprint());

    ClusterFacts changedFacts = facts(Resource.newInstance(101, 10));
    CompileResult changed = engine.compile(first.getModel(), changedFacts);
    assertTrue(changed.isCompiledActivationEligible(),
        changed.getFallbackReasons().toString());
    assertFalse(firstResult.getPlan().getIdentity().factsFingerprint().equals(
        changed.getPlan().getIdentity().factsFingerprint()));
  }

  @Test
  public void testConfigurationNumericSyntaxMatchesLegacy() {
    CapacitySchedulerConfiguration conf = percentageTree();
    conf.set(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB, "010");
    conf.set(YarnConfiguration.RM_SCHEDULER_MAXIMUM_ALLOCATION_VCORES, "08");
    conf.set(QueuePrefixes.getQueuePrefix(A)
        + CapacitySchedulerConfiguration.MAXIMUM_LIFETIME_SUFFIX, "0x10");
    conf.set(QueuePrefixes.getQueuePrefix(A)
        + CapacitySchedulerConfiguration.DEFAULT_LIFETIME_SUFFIX, "010");
    conf.set(QueuePrefixes.getQueuePrefix(A)
        + CapacitySchedulerConfiguration.MAXIMUM_ALLOCATION_MB, "010");
    conf.set(QueuePrefixes.getQueuePrefix(A)
        + CapacitySchedulerConfiguration.MAXIMUM_ALLOCATION_VCORES, "08");

    assertParity(conf, ClusterFacts.empty());
  }

  @Test
  public void testInvalidBooleanUsesInheritedConfigurationDefault() {
    CapacitySchedulerConfiguration conf = percentageTree();
    conf.setBoolean(CapacitySchedulerConfiguration
        .INTRAQUEUE_PREEMPTION_ENABLED, true);
    conf.set(QueuePrefixes.getQueuePrefix(ROOT)
        + "intra-queue-preemption.disable_preemption", "true");
    conf.set(QueuePrefixes.getQueuePrefix(A)
        + "intra-queue-preemption.disable_preemption",
        "garbage");

    CompileResult result = engine.compile(conf.getModel(),
        ClusterFacts.empty());

    assertTrue(result.isCompiledActivationEligible(),
        result.getFallbackReasons() + " " + result.getIssues());
    assertTrue(result.getPlan().getQueue(A.getFullPath()).settings()
        .scheduling().intraQueuePreemptionDisabled());
  }

  @Test
  public void testStructuredMaximumAllocationZerosUnspecifiedResources() {
    CapacitySchedulerConfiguration conf = percentageTree();
    conf.set(QueuePrefixes.getQueuePrefix(A)
        + CapacitySchedulerConfiguration.MAXIMUM_ALLOCATION,
        "memory-mb=1024");

    CompileResult result = engine.compile(conf.getModel(),
        ClusterFacts.empty());

    assertTrue(result.isCompiledActivationEligible(),
        result.getFallbackReasons() + " " + result.getIssues());
    assertEquals(1024L, result.getPlan().getQueue(A.getFullPath()).settings()
        .maximumAllocation().value("memory-mb"));
    assertEquals(0L, result.getPlan().getQueue(A.getFullPath()).settings()
        .maximumAllocation().value("vcores"));
  }

  @Test
  public void testPercentageManagedParentRejectsAbsoluteLeafTemplate() {
    CapacitySchedulerConfiguration conf = conf();
    conf.setQueues(ROOT, new String[] {"a"});
    conf.setCapacity(A, 100F);
    conf.setAutoCreateChildQueueEnabled(A, true);
    conf.setAutoCreatedLeafQueueTemplateCapacityByLabel(A, "",
        Resource.newInstance(10, 1));

    assertParity(conf, ClusterFacts.empty());
  }

  @Test
  public void testAbsoluteManagedParentAcceptsAbsoluteLeafTemplate() {
    CapacitySchedulerConfiguration conf = conf();
    conf.setQueues(ROOT, new String[] {"a"});
    conf.setMinimumResourceRequirement("", A,
        Resource.newInstance(100, 10));
    conf.setAutoCreateChildQueueEnabled(A, true);
    conf.setAutoCreatedLeafQueueTemplateCapacityByLabel(A, "",
        Resource.newInstance(10, 1));

    assertParity(conf, facts(Resource.newInstance(100, 10)));
  }

  @Test
  public void testDominantComparisonSkipsZeroDimensionInfinity() {
    CompiledQueuePlanCapacityValidator validator =
        new CompiledQueuePlanCapacityValidator();
    Map<String, Long> left = Map.of("memory-mb", 50L, "vcores", 0L);
    Map<String, Long> right = Map.of("memory-mb", 40L, "vcores", 100L);
    ValidatedQueuePlan.ResourceValues cluster =
        new ValidatedQueuePlan.ResourceValues(
            Map.of("memory-mb", 100L, "vcores", 0L));

    assertTrue(validator.greater(left, right, cluster,
        ValidatedQueuePlan.CalculatorSemantics.DOMINANT));
  }

  @Test
  public void testQueueNumericSyntaxParityMatrix() {
    List<Executable> checks = new ArrayList<>();
    for (String suffix : List.of(
        CapacitySchedulerConfiguration.MAX_PARALLEL_APPLICATIONS,
        CapacitySchedulerConfiguration.DEFAULT_APPLICATION_PRIORITY,
        CapacitySchedulerConfiguration.MAXIMUM_LIFETIME_SUFFIX,
        CapacitySchedulerConfiguration.DEFAULT_LIFETIME_SUFFIX,
        CapacitySchedulerConfiguration.MAXIMUM_ALLOCATION_MB,
        CapacitySchedulerConfiguration.MAXIMUM_ALLOCATION_VCORES,
        CapacitySchedulerConfiguration.MAXIMUM_APPLICATIONS_SUFFIX,
        CapacitySchedulerConfiguration.MAXIMUM_AM_RESOURCE_SUFFIX,
        CapacitySchedulerConfiguration.USER_LIMIT,
        CapacitySchedulerConfiguration.USER_LIMIT_FACTOR, "priority")) {
      for (String value : List.of("", " ", "-1", "0", "+1", " 1 ",
          "0x10", "08", "2147483648", "NaN", "Infinity")) {
        String property = QueuePrefixes.getQueuePrefix(A) + suffix;
        checks.add(() -> {
          CapacitySchedulerConfiguration conf = percentageTree();
          conf.set(property, value);
          assertAll(property + "=" + value,
              () -> assertParity(conf, ClusterFacts.empty()));
        });
      }
    }
    assertAll(checks);
  }

  @Test
  public void testScalarAllocationSentinelsKeepInheritedValues() {
    for (String vcores : List.of("-1", "4294967295")) {
      CapacitySchedulerConfiguration conf = percentageTree();
      conf.set(QueuePrefixes.getQueuePrefix(A)
          + CapacitySchedulerConfiguration.MAXIMUM_ALLOCATION_MB, "-1");
      conf.set(QueuePrefixes.getQueuePrefix(A)
          + CapacitySchedulerConfiguration.MAXIMUM_ALLOCATION_VCORES, vcores);
      CompileResult result = engine.compile(conf.getModel(),
          ClusterFacts.empty());
      assertTrue(result.isCompiledActivationEligible(),
          result.getFallbackReasons() + " " + result.getIssues());
      assertEquals(result.getPlan().getRoot().settings().maximumAllocation(),
          result.getPlan().getQueue("root.a").settings().maximumAllocation());
      assertParity(conf, ClusterFacts.empty());
    }
  }

  @Test
  public void testClusterApplicationPrioritySyntaxMatchesLegacy() {
    for (String value : List.of("not-an-int", "0x10", " 1 ", "-1")) {
      CapacitySchedulerConfiguration conf = percentageTree();
      conf.set(YarnConfiguration.MAX_CLUSTER_LEVEL_APPLICATION_PRIORITY,
          value);
      assertParity(conf, ClusterFacts.empty());
    }
  }

  @Test
  public void testUnexpectedCompilerFailureSelectsLegacyFallback() {
    CSConfigModel model = Mockito.mock(CSConfigModel.class);
    Mockito.when(model.getRawProperties()).thenReturn(Map.of());
    Mockito.when(model.getNodes()).thenReturn(Map.of());
    Mockito.when(model.getMappingRuleFormat()).thenReturn(
        CapacitySchedulerConfiguration.MAPPING_RULE_FORMAT_LEGACY);
    Mockito.when(model.getDiagnostics()).thenReturn(List.of());
    Mockito.when(model.getRoot()).thenThrow(
        new IllegalStateException("unmodeled parser edge"));

    CompileResult result = engine.compile(model, ClusterFacts.empty());

    assertTrue(result.requiresLegacyValidation());
    assertNull(result.getPlan());
    assertTrue(result.getFallbackReasons().stream().anyMatch(reason ->
        reason.code() == LegacyFallbackReason.Code
            .UNMODELED_VALIDATION_DEPENDENCY));
  }

  private void assertParity(CapacitySchedulerConfiguration conf,
      ClusterFacts facts) {
    ValidationResult legacy = engine.validate(conf.getModel(), facts);
    CompileResult compiled = engine.compile(conf.getModel(), facts);

    assertTrue(compiled.getFallbackReasons().isEmpty(),
        compiled.getFallbackReasons().toString());
    assertEquals(legacy.isValid(), compiled.isValid(),
        "legacy=" + legacy.getIssues() + " compiled=" + compiled.getIssues());
    assertEquals(issueKeys(legacy.getIssues()), issueKeys(compiled.getIssues()));
    if (legacy.isValid()) {
      assertNotNull(compiled.getPlan());
      assertTrue(compiled.isCompiledActivationEligible());
    } else {
      assertNull(compiled.getPlan());
    }
  }

  private List<IssueKey> issueKeys(List<ValidationIssue> issues) {
    return issues.stream().map(issue -> new IssueKey(issue.getRuleId(),
        issue.getQueuePath() == null ? null
            : issue.getQueuePath().getFullPath(),
        issue.getPropertyKey(), issue.getSeverity(), issue.getMessage()))
        .toList();
  }

  private record IssueKey(String rule, String queuePath, String property,
      ValidationIssue.Severity severity, String message) {
  }

  private CapacitySchedulerConfiguration percentageTree() {
    CapacitySchedulerConfiguration conf = conf();
    conf.setQueues(ROOT, new String[] {"a", "b"});
    conf.setCapacity(A, 50F);
    conf.setCapacity(B, 50F);
    return conf;
  }

  private CapacitySchedulerConfiguration conf() {
    return new CapacitySchedulerConfiguration(new Configuration(false), false);
  }

  private ClusterFacts facts(Resource cluster) {
    return ClusterFacts.builder()
        .withResources(cluster, Resource.newInstance(1, 1),
            Resource.newInstance(1000, 100),
            new DefaultResourceCalculator())
        .withResourcesByLabel(Map.of("", cluster))
        .build();
  }
}
