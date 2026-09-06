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


package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.security.GroupMappingServiceProvider;
import org.apache.hadoop.security.Groups;
import org.apache.hadoop.security.ShellBasedUnixGroupsMapping;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.QueueState;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.server.resourcemanager.placement.ApplicationPlacementContext;
import org.apache.hadoop.yarn.server.resourcemanager.placement.CSMappingPlacementRule;
import org.apache.hadoop.yarn.server.resourcemanager.placement.PlacementManager;
import org.apache.hadoop.yarn.server.resourcemanager.placement.PlacementRule;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.CSConfigValidationEngine;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.ClusterFacts;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.CompileResult;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.LegacyFallbackReason;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.ValidatedQueuePlan;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerApp;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.policy.FifoOrderingPolicy;
import org.apache.hadoop.yarn.security.ConfiguredYarnAuthorizer;
import org.apache.hadoop.yarn.security.YarnAuthorizationProvider;
import org.apache.hadoop.yarn.util.Records;
import org.apache.hadoop.yarn.util.resource.DominantResourceCalculator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.junit.jupiter.api.function.Executable;
import org.mockito.MockedConstruction;

import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.mockStatic;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Live construction and activation through the validated-plan seam. */
public class TestValidatedQueuePlanMaterializer {
  private static final QueuePath ROOT = new QueuePath("root");
  private static final QueuePath A = new QueuePath("root.a");
  private static final QueuePath B = new QueuePath("root.b");
  private final CSConfigValidationEngine engine = new CSConfigValidationEngine();

  @Test
  public void testMaterializesEachNodeOnceWithoutPublishing() throws Exception {
    try (QueuePlanTestFixture fixture = new QueuePlanTestFixture(configuration())) {
      CapacityScheduler scheduler = fixture.getScheduler();
      fixture.getProvider().runUnderMutationLock(() ->
          scheduler.runWithStableQueueConfiguration(() -> {
            CapacitySchedulerConfiguration candidate = scheduler.getConfiguration();
            ValidatedQueuePlan plan = compile(scheduler, candidate);
            CSQueueStore old = store(scheduler);
            Set<String> constructed = new HashSet<>();
            AtomicInteger count = new AtomicInteger();
            ValidatedQueuePlanMaterializer materializer =
                new ValidatedQueuePlanMaterializer(
                    new CapacitySchedulerQueueManager.QueueHook() {
                      @Override
                      public CSQueue hook(CSQueue queue) {
                        assertTrue(constructed.add(queue.getQueuePath()));
                        count.incrementAndGet();
                        assertNotSame(old.get(queue.getQueuePath()), queue);
                        assertSame(old.get(queue.getQueuePath()).getMetrics(),
                            queue.getMetrics());
                        return queue;
                      }
                    });
            CSQueue root = materializer.materialize(plan,
                scheduler.getQueueContext(), old);
            assertEquals(3, count.get());
            assertEquals(Set.of("root", "root.a", "root.b"), constructed);
            assertNotSame(scheduler.getRootQueue(), root);
            assertSame(old.get("root.a"), scheduler.getQueue("root.a"));
            assertEquals(3, old.getQueues().size());
            return null;
          }));
    }
  }

  @Test
  public void testRejectsChangedCandidateAndFactsBeforeConstruction()
      throws Exception {
    try (QueuePlanTestFixture fixture = new QueuePlanTestFixture(configuration())) {
      CapacityScheduler scheduler = fixture.getScheduler();
      ValidatedQueuePlan plan = compile(scheduler, scheduler.getConfiguration());
      CapacitySchedulerConfiguration changed = fixture.candidate();
      changed.setCapacity(A, 40);
      changed.setCapacity(B, 60);
      fixture.getProvider().runUnderMutationLock(() -> {
        assertThrows(IOException.class, () ->
            scheduler.reinitializeCompiledConfiguration(changed,
                fixture.getRM().getRMContext(), plan));
        return null;
      });
      assertEquals(0.5f, scheduler.getQueue("root.a").getCapacity());
      scheduler.getCapacitySchedulerQueueManager().getQueueStateManager()
          .stopQueue("root.b");
      rejectsBeforeConstruction(() ->
          new ValidatedQueuePlanMaterializer().materialize(plan,
              scheduler.getQueueContext(), store(scheduler)));
      assertEquals(QueueState.STOPPED, scheduler.getQueue("root.b").getState());
    }
  }

  @Test
  public void testAddRemoveAndRetainLiveQueueIdentity() throws Exception {
    CapacitySchedulerConfiguration initial = configuration();
    initial.setState(B, QueueState.STOPPED);
    try (QueuePlanTestFixture fixture = new QueuePlanTestFixture(initial)) {
      CSQueue retained = fixture.getScheduler().getQueue("root.a");
      Object metrics = retained.getMetrics();
      CapacitySchedulerConfiguration candidate = fixture.candidate();
      candidate.setQueues(ROOT, new String[] {"a", "c"});
      candidate.unset(QueuePrefixes.getQueuePrefix(B)
          + CapacitySchedulerConfiguration.CAPACITY);
      candidate.unset(QueuePrefixes.getQueuePrefix(B)
          + CapacitySchedulerConfiguration.STATE);
      candidate.setCapacity(A, 40);
      candidate.setCapacity(new QueuePath("root.c"), 60);
      fixture.activate(candidate);
      assertSame(retained, fixture.getScheduler().getQueue("root.a"));
      assertSame(metrics, retained.getMetrics());
      assertNull(fixture.getScheduler().getQueue("root.b"));
      assertInstanceOf(LeafQueue.class, fixture.getScheduler().getQueue("root.c"));
      assertEquals(0.4f, retained.getCapacity());
    }
  }

  @Test
  public void testStoppedLeafParentAndLeafConversions() throws Exception {
    CapacitySchedulerConfiguration initial = configuration();
    initial.setState(B, QueueState.STOPPED);
    try (QueuePlanTestFixture fixture = new QueuePlanTestFixture(initial)) {
      CSQueue leaf = fixture.getScheduler().getQueue("root.b");
      QueuePath child = new QueuePath("root.b.child");
      CapacitySchedulerConfiguration parent = fixture.candidate();
      parent.setQueues(B, new String[] {"child"});
      parent.setCapacity(child, 100);
      fixture.activate(parent);
      CSQueue converted = fixture.getScheduler().getQueue("root.b");
      assertInstanceOf(ParentQueue.class, converted);
      assertNotSame(leaf, converted);
      assertEquals(QueueState.STOPPED, converted.getState());
      assertSame(converted, fixture.getScheduler().getQueue("root.b.child").getParent());

      CapacitySchedulerConfiguration back = fixture.candidate();
      back.unset(QueuePrefixes.getQueuePrefix(B)
          + CapacitySchedulerConfiguration.QUEUES);
      back.unset(QueuePrefixes.getQueuePrefix(child)
          + CapacitySchedulerConfiguration.CAPACITY);
      fixture.activate(back);
      assertInstanceOf(LeafQueue.class, fixture.getScheduler().getQueue("root.b"));
      assertNull(fixture.getScheduler().getQueue("root.b.child"));
    }
  }

  @Test
  public void testStoppedLeafToManagedParent() throws Exception {
    CapacitySchedulerConfiguration initial = configuration();
    initial.setState(B, QueueState.STOPPED);
    initial.setAccessibleNodeLabels(ROOT, Set.of("blue"));
    initial.setCapacityByLabel(ROOT, "blue", 100);
    initial.setCapacityByLabel(B, "blue", 100);
    try (QueuePlanTestFixture fixture = new QueuePlanTestFixture(initial)) {
      CapacitySchedulerConfiguration candidate = fixture.candidate();
      candidate.setAutoCreateChildQueueEnabled(B, true);
      fixture.getRM().getRMContext().getNodeLabelManager()
          .addToCluserNodeLabelsWithDefaultExclusivity(Set.of("blue"));
      candidate.setAccessibleNodeLabels(ROOT, Set.of("blue"));
      candidate.setCapacityByLabel(ROOT, "blue", 100);
      candidate.setCapacityByLabel(B, "blue", 100);
      candidate.setAutoCreatedLeafQueueTemplateCapacityByLabel(B, "blue", 25);
      fixture.activate(candidate);
      ManagedParentQueue managed = assertInstanceOf(ManagedParentQueue.class,
          fixture.getScheduler().getQueue("root.b"));
      assertEquals(0.25f, managed.getLeafQueueTemplate().getQueueCapacities()
          .getCapacity("blue"));
      assertEquals(QueueState.STOPPED,
          fixture.getScheduler().getQueue("root.b").getState());
    }
  }

  @Test
  public void testMaximumAllocationDecreaseKeepsLegacyActivation()
      throws Exception {
    try (QueuePlanTestFixture fixture = new QueuePlanTestFixture(configuration())) {
      CapacitySchedulerConfiguration candidate = fixture.candidate();
      candidate.set(QueuePrefixes.getQueuePrefix(A)
          + CapacitySchedulerConfiguration.MAXIMUM_ALLOCATION_MB, "4096");
      ClusterFacts facts = ClusterFacts.capture(fixture.getScheduler());
      assertTrue(engine.validate(candidate.getModel(), facts).isValid());
      CompileResult result = engine.compile(candidate.getModel(), facts);
      assertTrue(result.requiresLegacyValidation());
      assertEquals(LegacyFallbackReason.Code.LIVE_QUEUE_REINITIALIZATION,
          result.getFallbackReasons().get(0).code());
      assertEquals("root.a", result.getFallbackReasons().get(0).queuePath());
      assertThrows(IOException.class, () ->
          fixture.getScheduler().reinitializeValidatedConfiguration(candidate,
              fixture.getRM().getRMContext()));
    }
  }

  @Test
  public void testRetainedLabelSetChangesKeepLegacyActivation() throws Exception {
    try (QueuePlanTestFixture fixture = new QueuePlanTestFixture(configuration())) {
      fixture.getRM().getRMContext().getNodeLabelManager()
          .addToCluserNodeLabelsWithDefaultExclusivity(Set.of("blue"));
      CapacitySchedulerConfiguration candidate = fixture.candidate();
      candidate.setAccessibleNodeLabels(ROOT, Set.of("blue"));
      candidate.setCapacityByLabel(ROOT, "blue", 100);
      candidate.setCapacityByLabel(A, "blue", 100);
      ClusterFacts facts = ClusterFacts.capture(fixture.getScheduler());
      assertTrue(engine.validate(candidate.getModel(), facts).isValid());
      CompileResult compiled = engine.compile(candidate.getModel(), facts);
      assertTrue(compiled.requiresLegacyValidation(),
          "Retained queues cannot roll back newly configured label quotas");
      assertTrue(compiled.getFallbackReasons().stream().anyMatch(reason ->
          reason.code() == LegacyFallbackReason.Code.LIVE_QUEUE_REINITIALIZATION
              && "root".equals(reason.queuePath())));
    }
  }

  @Test
  public void testPreviousConfigurationMustBeRebuildableUnderCurrentFacts()
      throws Exception {
    CapacitySchedulerConfiguration initial = configuration();
    QueuePath child = new QueuePath("root.a.child");
    initial.setResourceComparator(DominantResourceCalculator.class);
    initial.setQueues(ROOT, new String[] {"a"});
    initial.unset(QueuePrefixes.getQueuePrefix(B) + CapacitySchedulerConfiguration.CAPACITY);
    initial.setQueues(A, new String[] {"child"});
    initial.setMinimumResourceRequirement("", A, Resource.newInstance(10240, 2));
    initial.setMinimumResourceRequirement("", child, Resource.newInstance(1024, 2));
    initial.setAccessibleNodeLabels(ROOT, Set.of("blue"));
    initial.setCapacityByLabel(ROOT, "blue", 100);
    initial.setMinimumResourceRequirement("blue", A, Resource.newInstance(10240, 1));
    initial.setMinimumResourceRequirement("blue", child, Resource.newInstance(1024, 2));
    try (QueuePlanTestFixture fixture = new QueuePlanTestFixture(initial)) {
      var labels = fixture.getRM().getRMContext().getNodeLabelManager();
      labels.addToCluserNodeLabelsWithDefaultExclusivity(Set.of("blue"));
      NodeId node = NodeId.newInstance("blue-node", 1);
      labels.replaceLabelsOnNode(java.util.Map.of(node, Set.of("blue")));
      labels.activateNode(node, Resource.newInstance(10240, 100));
      fixture.activate(fixture.candidate());
      labels.updateNodeResource(node, Resource.newInstance(102400, 2));
      ClusterFacts facts = ClusterFacts.capture(fixture.getScheduler());
      assertFalse(engine.validate(initial.getModel(), facts).isValid());
      CapacitySchedulerConfiguration candidate = fixture.candidate();
      candidate.setMinimumResourceRequirement("blue", A, Resource.newInstance(10240, 2));
      assertTrue(engine.validate(candidate.getModel(), facts).isValid());
      CompileResult result = engine.compile(candidate.getModel(), facts);
      assertTrue(result.requiresLegacyValidation());
      assertTrue(result.getFallbackReasons().stream().anyMatch(reason ->
          reason.code() == LegacyFallbackReason.Code.LIVE_QUEUE_REINITIALIZATION
              && reason.message().contains("cannot be rebuilt for rollback")));
    }
  }

  @Test
  public void testPreviousConfigurationIdentityIsCheckedBeforeConstruction()
      throws Exception {
    try (QueuePlanTestFixture fixture = new QueuePlanTestFixture(configuration())) {
      CapacityScheduler scheduler = fixture.getScheduler();
      ValidatedQueuePlan plan = compile(scheduler, fixture.candidate());
      // No queue state or capacity changed, only the retained rollback inputs.
      scheduler.getConfiguration().setInt(QueuePrefixes.getQueuePrefix(A)
          + CapacitySchedulerConfiguration.MAXIMUM_APPLICATIONS_SUFFIX, 7);
      rejectsBeforeConstruction(() -> new ValidatedQueuePlanMaterializer()
          .materialize(plan, scheduler.getQueueContext(), store(scheduler)));
    }
  }

  @Test
  public void testRetainedStoppedParentCannotActivateChild() throws Exception {
    CapacitySchedulerConfiguration initial = configuration();
    QueuePath child = new QueuePath("root.b.child");
    initial.setQueues(B, new String[] {"child"});
    initial.setCapacity(child, 100);
    initial.setState(B, QueueState.STOPPED);
    try (QueuePlanTestFixture fixture = new QueuePlanTestFixture(initial)) {
      CapacitySchedulerConfiguration candidate = fixture.candidate();
      candidate.unset(QueuePrefixes.getQueuePrefix(B)
          + CapacitySchedulerConfiguration.STATE);
      candidate.setState(child, QueueState.RUNNING);
      ClusterFacts facts = ClusterFacts.capture(fixture.getScheduler());
      assertTrue(engine.validate(candidate.getModel(), facts).isValid());
      CompileResult result = engine.compile(candidate.getModel(), facts);
      assertTrue(result.requiresLegacyValidation());
      assertEquals("root.b.child",
          result.getFallbackReasons().get(0).queuePath());
    }
  }

  @Test
  public void testExplicitParentAndChildRestart() throws Exception {
    CapacitySchedulerConfiguration initial = configuration();
    QueuePath child = new QueuePath("root.b.child");
    initial.setQueues(B, new String[] {"child"});
    initial.setCapacity(child, 100);
    initial.setState(B, QueueState.STOPPED);
    try (QueuePlanTestFixture fixture = new QueuePlanTestFixture(initial)) {
      CapacitySchedulerConfiguration candidate = fixture.candidate();
      candidate.setState(B, QueueState.RUNNING);
      candidate.setState(child, QueueState.RUNNING);
      fixture.activate(candidate);
      assertEquals(QueueState.RUNNING,
          fixture.getScheduler().getQueue("root.b.child").getState());
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void testRetainedManagedParentTemplateTypeKeepsLegacyPath(
      boolean initiallyAbsolute) throws Exception {
    CapacitySchedulerConfiguration initial = configuration();
    initial.setQueues(ROOT, new String[] {"a"});
    initial.unset(QueuePrefixes.getQueuePrefix(B) + CapacitySchedulerConfiguration.CAPACITY);
    initial.setCapacity(A, 100);
    initial.setAutoCreateChildQueueEnabled(A, true);
    initial.setAutoCreatedLeafQueueTemplateCapacityByLabel(A, "", 10F);
    if (initiallyAbsolute) {
      initial.setMinimumResourceRequirement("", A, Resource.newInstance(8192, 8));
      initial.setAutoCreatedLeafQueueTemplateCapacityByLabel(A, "",
          Resource.newInstance(1024, 1));
    }
    try (QueuePlanTestFixture fixture = new QueuePlanTestFixture(initial)) {
      CapacitySchedulerConfiguration candidate = fixture.candidate();
      if (initiallyAbsolute) {
        candidate.setCapacity(A, 100);
        candidate.setAutoCreatedLeafQueueTemplateCapacityByLabel(A, "", 10F);
      } else {
        candidate.setMinimumResourceRequirement("", A, Resource.newInstance(8192, 8));
        candidate.setAutoCreatedLeafQueueTemplateCapacityByLabel(A, "",
            Resource.newInstance(1024, 1));
      }
      ClusterFacts facts = ClusterFacts.capture(fixture.getScheduler());
      assertTrue(engine.validate(candidate.getModel(), facts).isValid());
      CompileResult result = engine.compile(candidate.getModel(), facts);
      assertTrue(result.requiresLegacyValidation());
      assertEquals("root.a", result.getFallbackReasons().get(0).queuePath());
      if (!initiallyAbsolute) {
        assertThrows(IOException.class, () ->
            fixture.getScheduler().reinitializeValidatedConfiguration(candidate,
                fixture.getRM().getRMContext()));
      }
    }
  }

  @Test
  public void testRetainedParentZeroCapacityFlagKeepsLegacyPath() throws Exception {
    CapacitySchedulerConfiguration initial = configuration();
    QueuePath child = new QueuePath("root.a.child");
    initial.setQueues(A, new String[] {"child"});
    initial.setCapacity(child, 100);
    initial.setAccessibleNodeLabels(ROOT, Set.of("blue"));
    initial.setCapacityByLabel(ROOT, "blue", 100);
    initial.setCapacityByLabel(A, "blue", 100);
    initial.setCapacityByLabel(B, "blue", 0);
    initial.setCapacityByLabel(child, "blue", 100);
    try (QueuePlanTestFixture fixture = new QueuePlanTestFixture(initial)) {
      CapacitySchedulerConfiguration candidate = fixture.candidate();
      candidate.setBoolean(QueuePrefixes.getQueuePrefix(A)
          + CapacitySchedulerConfiguration.ALLOW_ZERO_CAPACITY_SUM, true);
      candidate.setCapacityByLabel(child, "blue", 0);
      ClusterFacts facts = ClusterFacts.capture(fixture.getScheduler());
      assertTrue(engine.validate(candidate.getModel(), facts).isValid());
      CompileResult result = engine.compile(candidate.getModel(), facts);
      assertTrue(result.requiresLegacyValidation());
      assertEquals("root.a", result.getFallbackReasons().get(0).queuePath());
      assertThrows(IOException.class, () ->
          fixture.getScheduler().reinitializeValidatedConfiguration(candidate,
              fixture.getRM().getRMContext()));
    }
  }

  @Test
  public void testNodeResourceChangeInvalidatesPlan() throws Exception {
    try (QueuePlanTestFixture fixture = new QueuePlanTestFixture(configuration())) {
      CapacityScheduler scheduler = fixture.getScheduler();
      ValidatedQueuePlan plan = compile(scheduler, scheduler.getConfiguration());
      fixture.getRM().getRMContext().getNodeLabelManager().activateNode(
          NodeId.newInstance("node", 1), Resource.newInstance(1024, 1));
      assertFalse(engine.matchesInputs(plan,
          scheduler.getConfiguration().getModel(), ClusterFacts.capture(scheduler)));
      assertThrows(IOException.class, () ->
          new ValidatedQueuePlanMaterializer().materialize(plan,
              scheduler.getQueueContext(), store(scheduler)));
    }
  }

  @Test
  public void testCompiledActivationRequiresMutationLock() throws Exception {
    try (QueuePlanTestFixture fixture = new QueuePlanTestFixture(configuration())) {
      ValidatedQueuePlan plan = compile(fixture.getScheduler(),
          fixture.getScheduler().getConfiguration());
      assertThrows(IllegalStateException.class, () ->
          fixture.getScheduler().reinitializeCompiledConfiguration(
              fixture.candidate(), fixture.getRM().getRMContext(), plan));
    }
  }

  @Test
  public void testRetainedLeafAllocationIsPartOfPlanIdentity() throws Exception {
    CapacitySchedulerConfiguration initial = configuration();
    initial.set(QueuePrefixes.getQueuePrefix(A)
        + CapacitySchedulerConfiguration.MAXIMUM_ALLOCATION_MB, "4096");
    try (QueuePlanTestFixture fixture = new QueuePlanTestFixture(initial)) {
      CapacitySchedulerConfiguration candidate = fixture.candidate();
      candidate.unset(QueuePrefixes.getQueuePrefix(A)
          + CapacitySchedulerConfiguration.MAXIMUM_ALLOCATION_MB);
      ValidatedQueuePlan plan = compile(fixture.getScheduler(), candidate);
      fixture.activate(candidate);
      assertFalse(engine.matchesInputs(plan, candidate.getModel(),
          ClusterFacts.capture(fixture.getScheduler())));
      assertThrows(IOException.class, () ->
          new ValidatedQueuePlanMaterializer().materialize(plan,
              fixture.getScheduler().getQueueContext(), store(fixture.getScheduler())));
    }
  }

  private ValidatedQueuePlan compile(CapacityScheduler scheduler,
      CapacitySchedulerConfiguration candidate) {
    CompileResult result = engine.compile(candidate.getModel(),
        ClusterFacts.capture(scheduler));
    assertTrue(result.isCompiledActivationEligible(),
        result.getFallbackReasons() + " " + result.getIssues());
    return result.getPlan();
  }

  @ParameterizedTest
  @ValueSource(strings = {"percentage", "weight", "absolute", "mixed", "vector"})
  public void testCapacityModesMaterializeAcceptedPlans(String mode) throws Exception {
    try (QueuePlanTestFixture fixture = new QueuePlanTestFixture(configuration())) {
      CapacitySchedulerConfiguration candidate = fixture.candidate();
      switch (mode) {
      case "percentage":
        candidate.setCapacity(A, 40);
        candidate.setCapacity(B, 60);
        break;
      case "weight":
        candidate.setCapacity(A, "1w");
        candidate.setCapacity(B, "2w");
        break;
      case "absolute":
        candidate.setCapacity(A, "[memory=2048,vcores=2]");
        candidate.setCapacity(B, "[memory=4096,vcores=4]");
        break;
      case "mixed":
        candidate.setLegacyQueueModeEnabled(false);
        candidate.setCapacity(A, "50");
        candidate.setCapacity(B, "1w");
        break;
      case "vector":
        candidate.setLegacyQueueModeEnabled(false);
        candidate.setCapacity(A, "[memory=40%,vcores=1w]");
        candidate.setCapacity(B, "[memory=60%,vcores=2w]");
        break;
      default:
        throw new AssertionError(mode);
      }
      assertTrue(engine.validate(candidate.getModel(),
          ClusterFacts.capture(fixture.getScheduler())).isValid());
      fixture.activate(candidate);
      assertEquals(candidate.getModel().getRawProperties(),
          fixture.getScheduler().getConfiguration().getModel().getRawProperties());
      assertEquals(3, fixture.getScheduler().getCapacitySchedulerQueueManager()
          .getQueues().size());
    }
  }

  @Test
  public void testConstructionFailureDoesNotPublishQueues() throws Exception {
    try (QueuePlanTestFixture fixture = new QueuePlanTestFixture(configuration())) {
      CSQueueStore old = store(fixture.getScheduler());
      CSQueue originalRoot = fixture.getScheduler().getRootQueue();
      ValidatedQueuePlan plan = compile(fixture.getScheduler(), fixture.candidate());
      ValidatedQueuePlanMaterializer materializer =
          new ValidatedQueuePlanMaterializer(new CapacitySchedulerQueueManager.QueueHook() {
            @Override
            public CSQueue hook(CSQueue queue) {
              if (queue.getQueuePath().equals("root.b")) {
                throw new IllegalStateException("injected construction failure");
              }
              return queue;
            }
          });
      assertThrows(IllegalStateException.class, () -> materializer.materialize(plan,
          fixture.getScheduler().getQueueContext(), old));
      assertSame(originalRoot, fixture.getScheduler().getRootQueue());
      assertSame(old.get("root.a"), fixture.getScheduler().getQueue("root.a"));
      assertSame(old.get("root.b"), fixture.getScheduler().getQueue("root.b"));
    }
  }

  private void rejectsBeforeConstruction(Executable operation) {
    try (MockedConstruction<LeafQueue> leaves = mockConstruction(LeafQueue.class);
         MockedConstruction<ParentQueue> parents = mockConstruction(ParentQueue.class);
         MockedConstruction<ManagedParentQueue> managed =
             mockConstruction(ManagedParentQueue.class)) {
      assertThrows(IOException.class, operation);
      assertTrue(leaves.constructed().isEmpty());
      assertTrue(parents.constructed().isEmpty());
      assertTrue(managed.constructed().isEmpty());
    }
  }

  @Test
  public void testExistingCustomOrderingPolicyKeepsLegacyPath() throws Exception {
    try (QueuePlanTestFixture fixture = new QueuePlanTestFixture(configuration())) {
      LeafQueue queue = (LeafQueue) fixture.getScheduler().getQueue("root.a");
      queue.setOrderingPolicy(new UnreadableOrderingPolicy());
      CompileResult result = engine.compile(fixture.candidate().getModel(),
          ClusterFacts.capture(fixture.getScheduler()));
      assertTrue(result.requiresLegacyValidation());
      assertEquals(LegacyFallbackReason.Code.CUSTOM_APPLICATION_ORDERING_POLICY,
          result.getFallbackReasons().get(0).code());
    }
  }

  @Test
  public void testInstalledAuthorizerKeepsLegacyPathAfterConfigRemoval()
      throws Exception {
    CapacitySchedulerConfiguration initial = configuration();
    initial.set(YarnConfiguration.YARN_AUTHORIZATION_PROVIDER,
        CustomAuthorizer.class.getName());
    YarnAuthorizationProvider.destroy();
    try (QueuePlanTestFixture fixture = new QueuePlanTestFixture(initial)) {
      CapacitySchedulerConfiguration candidate = fixture.candidate();
      candidate.unset(YarnConfiguration.YARN_AUTHORIZATION_PROVIDER);
      fixture.getScheduler().reinitializeValidatedConfiguration(candidate,
          fixture.getRM().getRMContext());
      assertNull(fixture.getScheduler().getConfiguration().get(
          YarnConfiguration.YARN_AUTHORIZATION_PROVIDER));
      CompileResult result = engine.compile(candidate.getModel(),
          ClusterFacts.capture(fixture.getScheduler()));
      assertTrue(result.requiresLegacyValidation());
      assertTrue(result.getFallbackReasons().stream().anyMatch(reason ->
          reason.code() == LegacyFallbackReason.Code.CUSTOM_AUTHORIZATION_PROVIDER));
    } finally {
      YarnAuthorizationProvider.destroy();
    }
  }

  public static class CustomAuthorizer extends ConfiguredYarnAuthorizer {
  }

  public static class CustomCapacityScheduler extends CapacityScheduler {
  }

  @Test
  public void testInstalledCustomSchedulerKeepsLegacyPath() throws Exception {
    CapacitySchedulerConfiguration initial = configuration();
    initial.setClass(YarnConfiguration.RM_SCHEDULER,
        CustomCapacityScheduler.class, ResourceScheduler.class);
    try (QueuePlanTestFixture fixture = new QueuePlanTestFixture(initial)) {
      CapacitySchedulerConfiguration candidate = fixture.candidate();
      candidate.unset(YarnConfiguration.RM_SCHEDULER);
      fixture.getScheduler().reinitializeValidatedConfiguration(candidate,
          fixture.getRM().getRMContext());
      CompileResult result = engine.compile(candidate.getModel(),
          ClusterFacts.capture(fixture.getScheduler()));
      assertTrue(result.getFallbackReasons().stream().anyMatch(reason ->
          reason.code() == LegacyFallbackReason.Code.CUSTOM_SCHEDULER));
    }
  }

  @Test
  public void testInstalledPlacementExtensionsRejectBeforeConstruction()
      throws Exception {
    try (QueuePlanTestFixture fixture = new QueuePlanTestFixture(configuration())) {
      PlacementManager manager = fixture.getRM().getRMContext()
          .getQueuePlacementManager();
      List<PlacementRule> previous = manager.getPlacementRules();
      CapacitySchedulerConfiguration candidate = fixture.candidate();
      ValidatedQueuePlan plan = compile(fixture.getScheduler(), candidate);
      CSQueueStore queues = store(fixture.getScheduler());
      Configuration customGroups = new Configuration(false);
      customGroups.setClass(CommonConfigurationKeys.HADOOP_SECURITY_GROUP_MAPPING,
          CustomGroupMapping.class, GroupMappingServiceProvider.class);
      CSMappingPlacementRule cachedProvider = new CSMappingPlacementRule();
      cachedProvider.setGroups(new Groups(customGroups));
      CSMappingPlacementRule cachedSubclass = new CSMappingPlacementRule();
      cachedSubclass.setGroups(new Groups(new Configuration(false)) {
        @Override
        public Set<String> getGroupsSet(String user) {
          throw new AssertionError("Custom Groups must not execute");
        }
      });
      List<PlacementRule> extensions = List.of(new UnreadablePlacementRule(),
          new CSMappingPlacementRule() {
            @Override
            public Groups getGroups() {
              throw new AssertionError("Custom placement getter must not execute");
            }
          }, cachedProvider, cachedSubclass);
      try {
        CustomGroupMapping.rejectCacheAdd = true;
        for (PlacementRule extension : extensions) {
          manager.updateRules(List.of(extension));
          CompileResult result = engine.compile(candidate.getModel(),
              ClusterFacts.capture(fixture.getScheduler()));
          LegacyFallbackReason.Code expected =
              extension.getClass() == CSMappingPlacementRule.class
                  ? LegacyFallbackReason.Code.CUSTOM_GROUP_MAPPING
                  : LegacyFallbackReason.Code.CUSTOM_PLACEMENT_RULE;
          assertTrue(result.getFallbackReasons().stream().anyMatch(reason ->
              reason.code() == expected));
          rejectsBeforeConstruction(() -> new ValidatedQueuePlanMaterializer()
              .materialize(plan, fixture.getScheduler().getQueueContext(), queues));
        }
      } finally {
        CustomGroupMapping.rejectCacheAdd = false;
        manager.updateRules(previous);
      }
    }
  }

  private static final class UnreadablePlacementRule extends PlacementRule {
    @Override
    public String getName() {
      throw new AssertionError("Custom placement name must not execute");
    }

    @Override
    public boolean initialize(ResourceScheduler scheduler) {
      throw new AssertionError("Custom placement initialization must not execute");
    }

    @Override
    public ApplicationPlacementContext getPlacementForApp(
        ApplicationSubmissionContext context, String user) {
      throw new AssertionError("Custom placement evaluation must not execute");
    }
  }

  @Test
  public void testCachedCustomGroupProviderKeepsLegacyPath() throws Exception {
    Configuration groups = new Configuration(false);
    groups.setClass(CommonConfigurationKeys.HADOOP_SECURITY_GROUP_MAPPING,
        CustomGroupMapping.class, GroupMappingServiceProvider.class);
    Groups.getUserToGroupsMappingServiceWithLoadedConfiguration(groups);
    try (QueuePlanTestFixture fixture = new QueuePlanTestFixture(configuration())) {
      CustomGroupMapping.rejectCacheAdd = true;
      CapacitySchedulerConfiguration candidate = fixture.candidate();
      candidate.set(QueuePrefixes.getQueuePrefix(A)
          + "acl_submit_applications", "user group");
      CompileResult result = engine.compile(candidate.getModel(),
          ClusterFacts.capture(fixture.getScheduler()));
      assertTrue(result.requiresLegacyValidation());
      assertTrue(result.getFallbackReasons().stream().anyMatch(reason ->
          reason.code() == LegacyFallbackReason.Code.CUSTOM_GROUP_MAPPING));
    } finally {
      CustomGroupMapping.rejectCacheAdd = false;
      Groups.reset();
    }
  }

  @Test
  public void testCachedCustomRecordFactoryKeepsLegacyPath() throws Exception {
    try (QueuePlanTestFixture fixture = new QueuePlanTestFixture(configuration());
         org.mockito.MockedStatic<Records> records = mockStatic(Records.class)) {
      records.when(Records::getRecordFactoryClassName).thenReturn("custom.Factory");
      CompileResult result = engine.compile(fixture.candidate().getModel(),
          ClusterFacts.capture(fixture.getScheduler()));
      assertTrue(result.requiresLegacyValidation());
      assertTrue(result.getFallbackReasons().stream().anyMatch(reason ->
          reason.code() == LegacyFallbackReason.Code.CUSTOM_RECORD_FACTORY));
    }
  }

  public static class CustomGroupMapping extends ShellBasedUnixGroupsMapping {
    private static boolean rejectCacheAdd;

    @Override
    public List<String> getGroups(String user) throws IOException {
      if (rejectCacheAdd) {
        throw new AssertionError("Custom group lookup must not execute");
      }
      return super.getGroups(user);
    }

    @Override
    public void cacheGroupsAdd(java.util.List<String> groups) {
      if (rejectCacheAdd) {
        throw new AssertionError("Custom group provider must not execute during compilation");
      }
    }
  }

  @Test
  public void testChangedDefaultRecordFactoryKeepsLegacyPath() throws Exception {
    try (QueuePlanTestFixture fixture = new QueuePlanTestFixture(configuration());
         org.mockito.MockedStatic<RecordFactoryProvider> provider =
             mockStatic(RecordFactoryProvider.class)) {
      provider.when(RecordFactoryProvider::getDefaultRecordFactoryClassName)
          .thenReturn("custom.NewFactory");
      CompileResult result = engine.compile(fixture.candidate().getModel(),
          ClusterFacts.capture(fixture.getScheduler()));
      assertTrue(result.requiresLegacyValidation());
      assertTrue(result.getFallbackReasons().stream().anyMatch(reason ->
          reason.code() == LegacyFallbackReason.Code.CUSTOM_RECORD_FACTORY));
      provider.verify(() -> RecordFactoryProvider.getRecordFactory(null),
          org.mockito.Mockito.never());
    }
  }

  private static final class UnreadableOrderingPolicy
      extends FifoOrderingPolicy<FiCaSchedulerApp> {
    @Override
    public Collection<FiCaSchedulerApp> getSchedulableEntities() {
      throw new AssertionError("Old custom ordering policy must not execute during compilation");
    }
  }

  private static CSQueueStore store(CapacityScheduler scheduler) {
    CSQueueStore store = new CSQueueStore();
    scheduler.getCapacitySchedulerQueueManager().getQueues().values()
        .forEach(store::add);
    return store;
  }

  private static CapacitySchedulerConfiguration configuration() {
    CapacitySchedulerConfiguration conf = new CapacitySchedulerConfiguration(
        new Configuration(false), false);
    conf.setClass(YarnConfiguration.RM_SCHEDULER, CapacityScheduler.class,
        ResourceScheduler.class);
    conf.setQueues(ROOT, new String[] {"a", "b"});
    conf.setCapacity(A, 50);
    conf.setCapacity(B, 50);
    return conf;
  }
}
