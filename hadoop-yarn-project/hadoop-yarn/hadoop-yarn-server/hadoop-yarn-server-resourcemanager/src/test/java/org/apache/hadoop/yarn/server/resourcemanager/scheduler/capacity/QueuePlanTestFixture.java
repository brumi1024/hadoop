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
import java.util.concurrent.Callable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.conf.MutableCSConfigurationProvider;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.CSConfigValidationEngine;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.ClusterFacts;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.CompileResult;

import static org.junit.jupiter.api.Assertions.assertTrue;

/** A real scheduler with an exact, seeded mutable-provider configuration. */
final class QueuePlanTestFixture implements AutoCloseable {
  private final MockRM rm;
  private final CapacityScheduler scheduler;
  private final MutableCSConfigurationProvider provider;

  QueuePlanTestFixture(CapacitySchedulerConfiguration initial) throws Exception {
    QueueMetrics.clearQueueMetrics();
    CapacitySchedulerConfiguration seed =
        new CapacitySchedulerConfiguration(initial, false);
    rm = new MockRM(initial);
    rm.start();
    scheduler = (CapacityScheduler) rm.getResourceScheduler();
    provider = new MutableCSConfigurationProvider(rm.getRMContext()) {
      @Override
      protected Configuration getInitSchedulerConfig() {
        return new Configuration(seed);
      }
    };
    Configuration bootstrap = new Configuration(initial);
    bootstrap.set(YarnConfiguration.SCHEDULER_CONFIGURATION_STORE_CLASS,
        YarnConfiguration.MEMORY_CONFIGURATION_STORE);
    provider.init(bootstrap);
    Field field = CapacityScheduler.class.getDeclaredField("csConfProvider");
    field.setAccessible(true);
    field.set(scheduler, provider);
    scheduler.reinitializeValidatedConfiguration(seed, rm.getRMContext());
  }

  static CapacitySchedulerConfiguration configuration() {
    CapacitySchedulerConfiguration conf = new CapacitySchedulerConfiguration(
        new Configuration(false), false);
    conf.setClass(YarnConfiguration.RM_SCHEDULER, CapacityScheduler.class,
        ResourceScheduler.class);
    conf.setQueues(new QueuePath("root"), new String[] {"a", "b"});
    conf.setCapacity(new QueuePath("root.a"), 50);
    conf.setCapacity(new QueuePath("root.b"), 50);
    return conf;
  }

  CapacitySchedulerConfiguration candidate() {
    return new CapacitySchedulerConfiguration(scheduler.getConfiguration(), false);
  }

  MockRM getRM() {
    return rm;
  }

  CapacityScheduler getScheduler() {
    return scheduler;
  }

  MutableCSConfigurationProvider getProvider() {
    return provider;
  }

  <T> T apply(CapacitySchedulerConfiguration candidate, Callable<T> commit)
      throws Exception {
    return provider.runUnderMutationLock(() ->
        scheduler.runWithStableQueueConfiguration(() -> {
          CompileResult compiled = new CSConfigValidationEngine().compile(
              candidate.getModel(), ClusterFacts.capture(scheduler));
          assertTrue(compiled.isCompiledActivationEligible(),
              compiled.getFallbackReasons() + " " + compiled.getIssues());
          return scheduler.applyCompiledConfiguration(candidate,
              rm.getRMContext(), compiled.getPlan(), commit);
        }));
  }

  void activate(CapacitySchedulerConfiguration candidate) throws Exception {
    apply(candidate, () -> null);
  }

  @Override
  public void close() {
    rm.stop();
    QueueMetrics.clearQueueMetrics();
  }
}
