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
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.Groups;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.QueueState;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.MockRMAppSubmissionData;
import org.apache.hadoop.yarn.server.resourcemanager.MockRMAppSubmitter;
import org.apache.hadoop.yarn.server.resourcemanager.placement.ApplicationPlacementContext;
import org.apache.hadoop.yarn.server.resourcemanager.placement.CSMappingPlacementRule;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerApp;
import org.apache.hadoop.yarn.util.Records;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;

/** Failure isolation through the same compiled activation interface as apply. */
public class TestCompiledQueueConfigurationTransaction {
  private static final QueuePath ROOT = new QueuePath("root");
  private static final QueuePath A = new QueuePath("root.a");
  private static final QueuePath B = new QueuePath("root.b");

  @Test
  public void testRollbackRestoresMaximumAllocationAndOriginalException()
      throws Exception {
    CapacitySchedulerConfiguration initial = QueuePlanTestFixture.configuration();
    initial.setQueueMaximumAllocation(A, "memory-mb=1024,vcores=1");
    try (QueuePlanTestFixture fixture = new QueuePlanTestFixture(initial)) {
      CSQueue oldQueue = fixture.getScheduler().getQueue("root.a");
      CapacitySchedulerConfiguration candidate = fixture.candidate();
      candidate.setQueueMaximumAllocation(A, "memory-mb=2048,vcores=2");
      IOException failure = new IOException("Commit preparation failed");
      IOException actual = assertThrows(IOException.class, () ->
          fixture.apply(candidate, () -> {
            assertEquals(Resource.newInstance(2048, 2),
                oldQueue.getMaximumAllocation());
            throw failure;
          }));
      assertSame(failure, actual);
      assertEquals(0, actual.getSuppressed().length);
      assertSame(oldQueue, fixture.getScheduler().getQueue("root.a"));
      assertEquals(Resource.newInstance(1024, 1), oldQueue.getMaximumAllocation());
      assertEquals(initial.getModel().getRawProperties(),
          fixture.getScheduler().getConfiguration().getModel().getRawProperties());
      assertFalse(fixture.getScheduler().getQueueContext().isConfigurationApplyInProgress());
    }
  }

  @Test
  public void testRollbackRestoresDeletedQueueIdentity() throws Exception {
    CapacitySchedulerConfiguration initial = QueuePlanTestFixture.configuration();
    initial.setState(B, QueueState.STOPPED);
    try (QueuePlanTestFixture fixture = new QueuePlanTestFixture(initial)) {
      CSQueue oldQueue = fixture.getScheduler().getQueue("root.b");
      Map<String, CSQueue> oldQueues =
          fixture.getScheduler().getCapacitySchedulerQueueManager().getQueues();
      CapacitySchedulerConfiguration candidate = fixture.candidate();
      candidate.setQueues(ROOT, new String[] {"a"});
      candidate.setCapacity(A, 100);
      candidate.unset(QueuePrefixes.getQueuePrefix(B) + CapacitySchedulerConfiguration.CAPACITY);
      candidate.unset(QueuePrefixes.getQueuePrefix(B) + CapacitySchedulerConfiguration.STATE);
      assertThrows(IOException.class, () -> fixture.apply(candidate, () -> {
        assertNull(fixture.getScheduler().getQueue("root.b"));
        throw new IOException("Do not commit deletion");
      }));
      assertSame(oldQueue, fixture.getScheduler().getQueue("root.b"));
      assertEquals(QueueState.STOPPED, oldQueue.getState());
      assertEquals(oldQueues,
          fixture.getScheduler().getCapacitySchedulerQueueManager().getQueues());
      assertTrue(fixture.getScheduler().getRootQueue().getChildQueues().contains(oldQueue));
      assertSame(fixture.getScheduler().getRootQueue(), oldQueue.getParent());
    }
  }

  @Test
  public void testRollbackRestoresConvertedQueueIdentity() throws Exception {
    CapacitySchedulerConfiguration initial = QueuePlanTestFixture.configuration();
    initial.setState(B, QueueState.STOPPED);
    try (QueuePlanTestFixture fixture = new QueuePlanTestFixture(initial)) {
      CSQueue oldQueue = fixture.getScheduler().getQueue("root.b");
      CapacitySchedulerConfiguration candidate = fixture.candidate();
      candidate.setQueues(B, new String[] {"child"});
      candidate.setCapacity(new QueuePath("root.b.child"), 100);
      assertThrows(IOException.class, () -> fixture.apply(candidate, () -> {
        assertInstanceOf(ParentQueue.class, fixture.getScheduler().getQueue("root.b"));
        throw new IOException("Do not commit conversion");
      }));
      assertSame(oldQueue, fixture.getScheduler().getQueue("root.b"));
      assertInstanceOf(LeafQueue.class, oldQueue);
      assertNull(fixture.getScheduler().getQueue("root.b.child"));
      assertEquals(QueueState.STOPPED, oldQueue.getState());
    }
  }

  @Test
  public void testRollbackRestoresActualStateNotConfigurationDefault()
      throws Exception {
    try (QueuePlanTestFixture fixture =
        new QueuePlanTestFixture(QueuePlanTestFixture.configuration())) {
      CSQueue oldA = fixture.getScheduler().getQueue("root.a");
      CSQueue oldB = fixture.getScheduler().getQueue("root.b");
      fixture.getScheduler().getCapacitySchedulerQueueManager()
          .getQueueStateManager().stopQueue("root.b");
      CapacitySchedulerConfiguration candidate = fixture.candidate();
      candidate.setState(A, QueueState.STOPPED);
      assertThrows(IOException.class, () -> fixture.apply(candidate, () -> {
        assertEquals(QueueState.STOPPED, oldA.getState());
        throw new IOException("Do not commit state transition");
      }));
      assertEquals(QueueState.RUNNING, oldA.getState());
      assertEquals(QueueState.STOPPED, oldB.getState());
    }
  }

  @Test
  public void testSuccessfulCommitClearsTransactionState() throws Exception {
    try (QueuePlanTestFixture fixture =
        new QueuePlanTestFixture(QueuePlanTestFixture.configuration())) {
      CapacitySchedulerConfiguration candidate = fixture.candidate();
      candidate.setCapacity(A, 40);
      candidate.setCapacity(B, 60);
      String result = fixture.apply(candidate, () -> {
        assertTrue(fixture.getScheduler().getQueueContext().isConfigurationApplyInProgress());
        return "committed";
      });
      assertEquals("committed", result);
      assertFalse(fixture.getScheduler().getQueueContext().isConfigurationApplyInProgress());
      assertEquals(0.4F, fixture.getScheduler().getQueue("root.a").getCapacity());
    }
  }

  @Test
  public void testPendingApplicationsActivateOnlyAfterCommit() throws Exception {
    try (QueuePlanTestFixture fixture =
        new QueuePlanTestFixture(QueuePlanTestFixture.configuration())) {
      fixture.getRM().registerNode("127.0.0.1:1234", 8192);
      AbstractLeafQueue leaf =
          (AbstractLeafQueue) fixture.getScheduler().getQueue("root.a");
      RMApp first = MockRMAppSubmitter.submit(fixture.getRM(),
          MockRMAppSubmissionData.Builder.createWithMemory(1024, fixture.getRM())
              .withQueue("a").withUser("user").build());
      RMApp second = MockRMAppSubmitter.submit(fixture.getRM(),
          MockRMAppSubmissionData.Builder.createWithMemory(1024, fixture.getRM())
              .withQueue("a").withUser("user").build());
      GenericTestUtils.waitFor(() -> leaf.getNumActiveApplications() == 1
          && leaf.getNumPendingApplications() == 1, 20, 10000);
      FiCaSchedulerApp oldAttempt = fixture.getScheduler().getApplicationAttempt(
          first.getCurrentAppAttempt().getAppAttemptId());
      Resource oldAMUsage = Resources.clone(leaf.getQueueResourceUsage().getAMUsed());
      CapacitySchedulerConfiguration candidate = fixture.candidate();
      candidate.setMaximumApplicationMasterResourcePerQueuePercent(A, 1F);
      assertThrows(IOException.class, () -> fixture.apply(candidate, () -> {
        assertEquals(1, leaf.getNumActiveApplications());
        assertEquals(1, leaf.getNumPendingApplications());
        throw new IOException("Do not promote pending applications");
      }));
      assertEquals(1, leaf.getNumActiveApplications());
      assertEquals(1, leaf.getNumPendingApplications());
      assertEquals(oldAMUsage, leaf.getQueueResourceUsage().getAMUsed());
      assertSame(oldAttempt, fixture.getScheduler().getApplicationAttempt(
          first.getCurrentAppAttempt().getAppAttemptId()));
      fixture.apply(candidate, () -> {
        assertEquals(1, leaf.getNumPendingApplications());
        return null;
      });
      assertEquals(2, leaf.getNumActiveApplications());
      assertEquals(0, leaf.getNumPendingApplications());
      assertSame(leaf, fixture.getScheduler().getQueue("root.a"));
      assertTrue(fixture.getScheduler().getApplicationAttempt(
          second.getCurrentAppAttempt().getAppAttemptId()) != null);
    }
  }

  @Test
  public void testPlacementCannotObserveRolledBackHierarchy() throws Exception {
    CapacitySchedulerConfiguration initial = QueuePlanTestFixture.configuration();
    initial.set(CapacitySchedulerConfiguration.QUEUE_MAPPING, "u:%user:root.a");
    ExecutorService executor = Executors.newSingleThreadExecutor();
    try (QueuePlanTestFixture fixture = new QueuePlanTestFixture(initial)) {
      CapacitySchedulerConfiguration candidate = fixture.candidate();
      candidate.set(CapacitySchedulerConfiguration.QUEUE_MAPPING, "u:%user:root.b");
      ApplicationSubmissionContext submission = Records.newRecord(
          ApplicationSubmissionContext.class);
      submission.setQueue("default");
      String user = UserGroupInformation.getCurrentUser().getShortUserName();
      java.util.concurrent.atomic.AtomicReference<Future<ApplicationPlacementContext>> placed =
          new java.util.concurrent.atomic.AtomicReference<>();
      assertThrows(IOException.class, () -> fixture.apply(candidate, () -> {
        CountDownLatch entered = new CountDownLatch(1);
        placed.set(executor.submit(() -> {
          entered.countDown();
          return fixture.getRM().getRMContext().getQueuePlacementManager()
              .placeApplication(submission, user);
        }));
        assertTrue(entered.await(5, TimeUnit.SECONDS));
        assertThrows(TimeoutException.class, () -> placed.get().get(100, TimeUnit.MILLISECONDS));
        throw new IOException("Do not publish candidate placement");
      }));
      assertEquals("root.a", placed.get().get(5, TimeUnit.SECONDS).getFullQueuePath());
    } finally {
      executor.shutdownNow();
    }
  }

  @Test
  public void testRollbackRestoresFixedLabelQuotas() throws Exception {
    CapacitySchedulerConfiguration initial = QueuePlanTestFixture.configuration();
    initial.setAccessibleNodeLabels(ROOT, Set.of("blue"));
    initial.setCapacityByLabel(ROOT, "blue", 100);
    initial.setCapacityByLabel(A, "blue", 50);
    initial.setCapacityByLabel(B, "blue", 50);
    try (QueuePlanTestFixture fixture = new QueuePlanTestFixture(initial)) {
      RMNodeLabelsManager labels = fixture.getRM().getRMContext().getNodeLabelManager();
      labels.addToCluserNodeLabelsWithDefaultExclusivity(Set.of("blue"));
      NodeId node = NodeId.newInstance("blue-node", 1234);
      labels.replaceLabelsOnNode(Map.of(node, Set.of("blue")));
      labels.activateNode(node, Resource.newInstance(8192, 8));
      fixture.activate(fixture.candidate());
      CSQueue queue = fixture.getScheduler().getQueue("root.a");
      Resource previousMin = Resources.clone(queue.getEffectiveCapacity("blue"));
      Resource previousMax = Resources.clone(queue.getEffectiveMaxCapacity("blue"));
      CapacitySchedulerConfiguration candidate = fixture.candidate();
      candidate.setCapacityByLabel(A, "blue", 25);
      candidate.setCapacityByLabel(B, "blue", 75);
      assertThrows(IOException.class, () -> fixture.apply(candidate, () -> {
        assertEquals(2048, queue.getEffectiveCapacity("blue").getMemorySize());
        throw new IOException("Do not commit label capacities");
      }));
      assertEquals(4096, previousMin.getMemorySize());
      assertEquals(previousMin, queue.getEffectiveCapacity("blue"));
      assertEquals(previousMax, queue.getEffectiveMaxCapacity("blue"));
    }
  }

  @Test
  public void testExistingPlacementReaderDrainsBeforeMutation() throws Exception {
    CapacitySchedulerConfiguration initial = QueuePlanTestFixture.configuration();
    initial.set(CapacitySchedulerConfiguration.QUEUE_MAPPING, "g:group:root.a");
    ExecutorService executor = Executors.newFixedThreadPool(2);
    CountDownLatch readerEntered = new CountDownLatch(1);
    CountDownLatch releaseReader = new CountDownLatch(1);
    try (QueuePlanTestFixture fixture = new QueuePlanTestFixture(initial)) {
      CSMappingPlacementRule rule = (CSMappingPlacementRule) fixture.getRM().getRMContext()
          .getQueuePlacementManager().getPlacementRules().get(0);
      Groups groups = spy(rule.getGroups());
      doAnswer(invocation -> {
        readerEntered.countDown();
        assertTrue(releaseReader.await(5, TimeUnit.SECONDS));
        return Set.of("group");
      }).when(groups).getGroupsSet(anyString());
      rule.setGroups(groups);
      ApplicationSubmissionContext submission = Records.newRecord(
          ApplicationSubmissionContext.class);
      submission.setQueue("default");
      Future<ApplicationPlacementContext> placed = executor.submit(() ->
          fixture.getRM().getRMContext().getQueuePlacementManager()
              .placeApplication(submission, "user"));
      assertTrue(readerEntered.await(5, TimeUnit.SECONDS));
      CapacitySchedulerConfiguration candidate = fixture.candidate();
      candidate.setCapacity(A, 40);
      candidate.setCapacity(B, 60);
      CountDownLatch mutationEntered = new CountDownLatch(1);
      Future<?> applied = executor.submit(() -> {
        mutationEntered.countDown();
        fixture.activate(candidate);
        return null;
      });
      assertTrue(mutationEntered.await(5, TimeUnit.SECONDS));
      assertThrows(TimeoutException.class, () -> applied.get(100, TimeUnit.MILLISECONDS));
      releaseReader.countDown();
      assertEquals("root.a", placed.get(5, TimeUnit.SECONDS).getFullQueuePath());
      applied.get(5, TimeUnit.SECONDS);
      assertEquals(0.4F, fixture.getScheduler().getQueue("root.a").getCapacity());
    } finally {
      releaseReader.countDown();
      executor.shutdownNow();
    }
  }
}
