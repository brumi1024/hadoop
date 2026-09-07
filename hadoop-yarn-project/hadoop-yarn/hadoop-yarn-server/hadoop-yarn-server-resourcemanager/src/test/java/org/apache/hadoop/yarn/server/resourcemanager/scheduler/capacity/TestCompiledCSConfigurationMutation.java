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

import java.lang.reflect.Field;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.conf.InMemoryConfigurationStore;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.conf.MutableCSConfigurationProvider;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.conf.YarnConfigurationStore;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.CSConfigValidationEngine;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.ClusterFacts;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.CompileResult;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.LegacyFallbackReason;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.ValidationResult;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerApp;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.policy.FifoOrderingPolicy;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.RMWebServices;
import org.apache.hadoop.yarn.webapp.dao.QueueConfigInfo;
import org.apache.hadoop.yarn.webapp.dao.SchedConfUpdateInfo;
import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** End-to-end pins for the actual mutable-provider compiled apply path. */
public class TestCompiledCSConfigurationMutation {
  private static final UserGroupInformation USER =
      UserGroupInformation.createUserForTesting("compiled-user", new String[0]);

  @Test
  public void testAtomicApplyMaterializesExactlyOnceWithoutValidationQueues()
      throws Exception {
    try (QueuePlanTestFixture fixture = fixture();
         MaterializationProbe probe = new MaterializationProbe();
         MockedStatic<CapacitySchedulerQueueManager> ignored = forbidValidationTree()) {
      CSQueue old = fixture.getScheduler().getQueue("root.a");
      InMemoryConfigurationStore store = spy((InMemoryConfigurationStore)
          replaceStore(fixture, null));
      replaceStore(fixture, store);
      long version = fixture.getProvider().getConfigVersion();

      ValidationResult result = fixture.getProvider().applyMutation(USER, capacities(40, 60));

      assertTrue(result.isValid(), result.getIssues().toString());
      assertEquals(1, probe.materializations());
      assertEquals(3, probe.queues());
      assertSame(old, fixture.getScheduler().getQueue("root.a"));
      assertEquals(0.4F, old.getCapacity());
      assertEquals(version + 1, fixture.getProvider().getConfigVersion());
      assertEquals("40", fixture.getProvider().getConfiguration()
          .get("yarn.scheduler.capacity.root.a.capacity"));
      verify(store).confirmMutation(any(), eq(true));
      verify(store, never()).getConfigVersion();
    }
  }

  @Test
  public void testInvalidCandidateNeverMaterializesOrChangesVersion() throws Exception {
    try (QueuePlanTestFixture fixture = fixture();
         MaterializationProbe probe = new MaterializationProbe();
         MockedStatic<CapacitySchedulerQueueManager> ignored = forbidValidationTree()) {
      long version = fixture.getProvider().getConfigVersion();
      ValidationResult result = fixture.getProvider().applyMutation(USER, capacities(40, 50));
      assertFalse(result.isValid());
      assertTrue(result.getIssues().stream().anyMatch(issue ->
          issue.getRuleId().equals("children-capacity-sum")));
      assertEquals(0, probe.materializations());
      assertEquals(version, fixture.getProvider().getConfigVersion());
      assertEquals(0.5F, fixture.getScheduler().getQueue("root.a").getCapacity());
    }
  }

  @Test
  public void testNativeStoreFailureRollsBackQueuesAndProviderSnapshot() throws Exception {
    try (QueuePlanTestFixture fixture = fixture();
         MaterializationProbe probe = new MaterializationProbe();
         MockedStatic<CapacitySchedulerQueueManager> ignored = forbidValidationTree()) {
      CSQueue old = fixture.getScheduler().getQueue("root.a");
      InMemoryConfigurationStore store = spy((InMemoryConfigurationStore)
          replaceStore(fixture, null));
      replaceStore(fixture, store);
      IllegalStateException failure = new IllegalStateException("confirmation rejected");
      doThrow(failure).when(store).confirmMutation(any(), eq(true));
      long version = fixture.getProvider().getConfigVersion();

      assertSame(failure, assertThrows(IllegalStateException.class, () ->
          fixture.getProvider().applyMutation(USER, capacities(40, 60))));

      assertEquals(1, probe.materializations());
      assertSame(old, fixture.getScheduler().getQueue("root.a"));
      assertEquals(0.5F, old.getCapacity());
      assertEquals(version, fixture.getProvider().getConfigVersion());
      assertEquals(version, store.getConfigVersion());
      assertEquals(50F, fixture.getProvider().getConfiguration()
          .getFloat("yarn.scheduler.capacity.root.a.capacity", -1));
      assertFalse(fixture.getScheduler().getQueueContext().isConfigurationApplyInProgress());
    }
  }

  @Test
  public void testCustomStoreUsesLegacyWithoutCompiledMaterialization() throws Exception {
    try (QueuePlanTestFixture fixture = fixture();
         MaterializationProbe probe = new MaterializationProbe()) {
      CountingStore store = new CountingStore();
      store.initialize(new Configuration(false), fixture.getProvider().getConfiguration(),
          fixture.getRM().getRMContext());
      replaceStore(fixture, store);
      assertTrue(fixture.getProvider().applyMutation(USER, capacities(40, 60)).isValid());
      assertEquals(0, probe.materializations());
      assertEquals(1, store.confirmations);
      assertEquals(2, fixture.getProvider().getConfigVersion());
    }
  }

  @Test
  public void testFailingCustomStoreRetainsLegacyConfirmationProtocol() throws Exception {
    try (QueuePlanTestFixture fixture = fixture();
         MaterializationProbe probe = new MaterializationProbe()) {
      CountingStore store = spy(new CountingStore());
      store.initialize(new Configuration(false), fixture.getProvider().getConfiguration(),
          fixture.getRM().getRMContext());
      replaceStore(fixture, store);
      IllegalStateException failure = new IllegalStateException("custom store rejected");
      doThrow(failure).when(store).confirmMutation(any(), eq(true));
      long version = fixture.getProvider().getConfigVersion();

      assertSame(failure, assertThrows(IllegalStateException.class, () ->
          fixture.getProvider().applyMutation(USER, capacities(40, 60))));

      verify(store).confirmMutation(any(), eq(false));
      assertEquals(0, probe.materializations());
      assertEquals(version, fixture.getProvider().getConfigVersion());
      assertEquals(version, store.getConfigVersion());
      assertEquals(50F, fixture.getProvider().getConfiguration()
          .getFloat("yarn.scheduler.capacity.root.a.capacity", -1));
    }
  }

  @Test
  public void testThrowingCustomPolicyIsExecutedOnlyByLegacyValidation() throws Exception {
    ThrowingOrderingPolicy.CONFIGURATIONS.set(0);
    try (QueuePlanTestFixture fixture = fixture();
         MaterializationProbe probe = new MaterializationProbe()) {
      SchedConfUpdateInfo update = new SchedConfUpdateInfo();
      update.getUpdateQueueInfo().add(new QueueConfigInfo("root.a",
          Map.of("ordering-policy", ThrowingOrderingPolicy.class.getName())));
      Configuration proposed = fixture.getProvider().applyChanges(
          fixture.getProvider().getConfiguration(), update);
      CompileResult compiled = new CSConfigValidationEngine().compile(
          new CapacitySchedulerConfiguration(proposed, false).getModel(),
          ClusterFacts.capture(fixture.getScheduler()));
      assertTrue(compiled.getFallbackReasons().stream().anyMatch(reason ->
          reason.code() == LegacyFallbackReason.Code.CUSTOM_APPLICATION_ORDERING_POLICY));
      assertThrows(IllegalStateException.class, compiled::asValidationResult);
      assertEquals(0, ThrowingOrderingPolicy.CONFIGURATIONS.get());

      ValidationResult result = fixture.getProvider().applyMutation(USER, update);

      assertFalse(result.isValid());
      assertTrue(result.getIssues().stream().anyMatch(issue ->
          issue.getMessage().contains("custom configure rejected")));
      assertEquals(1, ThrowingOrderingPolicy.CONFIGURATIONS.get());
      assertEquals(0, probe.materializations());
      assertEquals(1, fixture.getProvider().getConfigVersion());
    }
  }

  @Test
  public void testValidationEndpointsDoNotConstructQueues() throws Exception {
    try (QueuePlanTestFixture fixture = fixture();
         MaterializationProbe probe = new MaterializationProbe();
         MockedStatic<CapacitySchedulerQueueManager> ignored = forbidValidationTree()) {
      RMWebServices web = new RMWebServices(fixture.getRM(), fixture.getRM().getConfig());
      web.setResponse(mock(HttpServletResponse.class));
      HttpServletRequest request = mock(HttpServletRequest.class);
      when(request.getRemoteUser()).thenReturn(USER.getShortUserName());
      when(request.getUserPrincipal()).thenReturn(() -> USER.getShortUserName());
      long version = fixture.getProvider().getConfigVersion();
      assertEquals(200, web.validateAndGetSchedulerConfiguration(
          capacities(40, 60), request).getStatus());
      assertEquals(200, web.validateSchedulerConfigurationStructured(
          capacities(40, 60), request).getStatus());
      assertEquals(400, web.validateSchedulerConfigurationStructured(
          capacities(40, 50), request).getStatus());
      assertEquals(0, probe.materializations());
      assertEquals(version, fixture.getProvider().getConfigVersion());
      assertEquals(0.5F, fixture.getScheduler().getQueue("root.a").getCapacity());
    }
  }

  @Test
  public void testCompiledMutationsRemainSerializedThroughCommit() throws Exception {
    ExecutorService executor = Executors.newFixedThreadPool(2);
    CountDownLatch confirmationEntered = new CountDownLatch(1);
    CountDownLatch releaseConfirmation = new CountDownLatch(1);
    try (QueuePlanTestFixture fixture = fixture()) {
      InMemoryConfigurationStore store = spy((InMemoryConfigurationStore)
          replaceStore(fixture, null));
      replaceStore(fixture, store);
      AtomicInteger confirmations = new AtomicInteger();
      doAnswer(invocation -> {
        if (confirmations.incrementAndGet() == 1) {
          confirmationEntered.countDown();
          assertTrue(releaseConfirmation.await(5, TimeUnit.SECONDS));
        }
        return invocation.callRealMethod();
      }).when(store).confirmMutation(any(), eq(true));

      Future<ValidationResult> first = executor.submit(() ->
          fixture.getProvider().applyMutation(USER, capacities(40, 60)));
      assertTrue(confirmationEntered.await(5, TimeUnit.SECONDS));
      CountDownLatch secondEntered = new CountDownLatch(1);
      Future<ValidationResult> second = executor.submit(() -> {
        secondEntered.countDown();
        return fixture.getProvider().applyMutation(USER, capacities(30, 70));
      });
      assertTrue(secondEntered.await(5, TimeUnit.SECONDS));
      assertThrows(TimeoutException.class, () -> second.get(100, TimeUnit.MILLISECONDS));
      assertEquals(1, fixture.getProvider().getConfigVersion());
      releaseConfirmation.countDown();
      assertTrue(first.get(5, TimeUnit.SECONDS).isValid());
      assertTrue(second.get(5, TimeUnit.SECONDS).isValid());
      assertEquals(2, confirmations.get());
      assertEquals(3, fixture.getProvider().getConfigVersion());
      assertEquals(0.3F, fixture.getScheduler().getQueue("root.a").getCapacity());
    } finally {
      releaseConfirmation.countDown();
      executor.shutdownNow();
    }
  }

  private static QueuePlanTestFixture fixture() throws Exception {
    return new QueuePlanTestFixture(QueuePlanTestFixture.configuration());
  }

  private static SchedConfUpdateInfo capacities(int a, int b) {
    SchedConfUpdateInfo update = new SchedConfUpdateInfo();
    update.getUpdateQueueInfo().add(new QueueConfigInfo("root.a",
        Map.of("capacity", Integer.toString(a))));
    update.getUpdateQueueInfo().add(new QueueConfigInfo("root.b",
        Map.of("capacity", Integer.toString(b))));
    return update;
  }

  private static YarnConfigurationStore replaceStore(QueuePlanTestFixture fixture,
      YarnConfigurationStore replacement) throws Exception {
    Field field = MutableCSConfigurationProvider.class.getDeclaredField("confStore");
    field.setAccessible(true);
    YarnConfigurationStore previous = (YarnConfigurationStore) field.get(fixture.getProvider());
    if (replacement != null) {
      field.set(fixture.getProvider(), replacement);
    }
    return previous;
  }

  private static MockedStatic<CapacitySchedulerQueueManager> forbidValidationTree() {
    MockedStatic<CapacitySchedulerQueueManager> mock =
        mockStatic(CapacitySchedulerQueueManager.class, CALLS_REAL_METHODS);
    mock.when(() -> CapacitySchedulerQueueManager.buildQueueTreeForValidation(any(), any()))
        .thenThrow(new AssertionError("Validation must not construct a CSQueue hierarchy"));
    return mock;
  }

  private static final class CountingStore extends InMemoryConfigurationStore {
    private int confirmations;

    @Override
    public void confirmMutation(LogMutation mutation, boolean valid) {
      if (valid) {
        confirmations++;
      }
      super.confirmMutation(mutation, valid);
    }
  }

  public static class ThrowingOrderingPolicy extends FifoOrderingPolicy<FiCaSchedulerApp> {
    private static final AtomicInteger CONFIGURATIONS = new AtomicInteger();

    @Override
    public void configure(Map<String, String> configuration) {
      CONFIGURATIONS.incrementAndGet();
      throw new IllegalArgumentException("custom configure rejected");
    }
  }

  private static final class MaterializationProbe implements AutoCloseable {
    private final AtomicInteger materializations = new AtomicInteger();
    private final AtomicInteger queues = new AtomicInteger();
    private final MockedConstruction<ValidatedQueuePlanMaterializer> construction;

    private MaterializationProbe() {
      ValidatedQueuePlanMaterializer delegate = new ValidatedQueuePlanMaterializer(
          new CapacitySchedulerQueueManager.QueueHook() {
            @Override
            public CSQueue hook(CSQueue queue) {
              queues.incrementAndGet();
              return queue;
            }
          });
      construction = mockConstruction(ValidatedQueuePlanMaterializer.class, (mock, context) ->
          when(mock.materialize(any(), any(), any())).thenAnswer(invocation -> {
            materializations.incrementAndGet();
            return delegate.materialize(invocation.getArgument(0),
                invocation.getArgument(1), invocation.getArgument(2));
          }));
    }

    private int materializations() {
      return materializations.get();
    }

    private int queues() {
      return queues.get();
    }

    @Override
    public void close() {
      construction.close();
    }
  }
}
