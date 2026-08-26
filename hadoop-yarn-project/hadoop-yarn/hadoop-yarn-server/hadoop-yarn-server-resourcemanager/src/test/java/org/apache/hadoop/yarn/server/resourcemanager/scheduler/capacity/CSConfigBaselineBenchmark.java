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
import java.util.Arrays;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.jupiter.api.Test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.ClusterMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.conf.MutableCSConfigurationProvider;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.conf.model.CSConfigModel;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.plan.ValidatedQueuePlan;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.CSConfigValidationEngine;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.ClusterFacts;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.CompileResult;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.ValidationResult;
import org.apache.hadoop.yarn.webapp.dao.SchedConfUpdateInfo;

/**
 * Stage timing harness for Capacity Scheduler configuration compilation,
 * validation, live materialization, activation, and atomic mutation apply.
 *
 * Deliberately NOT named Test* so that regular module test runs skip it.
 * Run it explicitly, from the repo root:
 *
 * <pre>
 * mvn test -Dtest=CSConfigBaselineBenchmark \
 *   -pl hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager \
 *   -DfailIfNoTests=false
 * </pre>
 * Run each configured size in a separate forked JVM when recording results so
 * heap state and JIT compilation from one size do not affect another size.
 *
 * Knobs (system properties, propagated by surefire from the mvn command line):
 * - {@code cs.bench.sizes}: comma list of requested queue counts, default
 *   100,1000,5000
 * - {@code cs.bench.iterations}: fixed timed-iteration count, default auto per
 *   size
 * - {@code cs.bench.warmups}: fixed warmup count, default auto per size
 *
 * Measured operations per size, against a live MockRM:
 * - load: new CapacityScheduler + setConf + setRMContext + init(conf), i.e. the
 *   serviceInit -> initScheduler -> initializeQueues path; stop() is untimed.
 * - compile: immutable model to configuration-only queue plan.
 * - validation-rules: structured rules against a precompiled plan.
 * - materialize: queue constructors only, using live adapters and existing
 *   queue state without publishing the resulting hierarchy.
 * - activation: a prevalidated plan through the complete live activation
 *   path, excluding compilation, validation, and mutation-store work.
 * - atomic-apply: the mutable provider's validate-log-activate-confirm path.
 *
 * Results are printed to stdout as BENCH lines (see surefire -output.txt).
 */
public class CSConfigBaselineBenchmark {

  private static final int MIN_TIMED_ITERATIONS = 5;
  private static final UserGroupInformation BENCHMARK_USER =
      UserGroupInformation.createRemoteUser("cs-plan-benchmark");

  private static final class SeededMutableProvider
      extends MutableCSConfigurationProvider {
    private final Configuration initial;

    SeededMutableProvider(RMContext rmContext, Configuration initial) {
      super(rmContext);
      this.initial = new Configuration(initial);
    }

    @Override
    protected Configuration getInitSchedulerConfig() {
      return new Configuration(initial);
    }
  }

  private interface TimedOp {
    /** Runs one iteration and returns the elapsed nanos of the timed part. */
    long runOnce(int iteration) throws Exception;
  }

  @Test
  public void runBaseline() throws Exception {
    String[] sizes = System.getProperty("cs.bench.sizes", "100,1000,5000").split(",");
    for (String size : sizes) {
      runForSize(Integer.parseInt(size.trim()));
    }
  }

  private void runForSize(int requestedQueues) throws Exception {
    QueueMetrics.clearQueueMetrics();
    CSConfigBenchmarkGenerator.GeneratedConfig gen =
        CSConfigBenchmarkGenerator.generate(requestedQueues);
    YarnConfiguration conf = new YarnConfiguration(gen.getConf());
    conf.setClass(YarnConfiguration.RM_SCHEDULER, CapacityScheduler.class,
        ResourceScheduler.class);

    int warmups = Integer.getInteger("cs.bench.warmups", defaultWarmups(requestedQueues));
    int iterations = Integer.getInteger("cs.bench.iterations",
        defaultIterations(requestedQueues));
    if (iterations < MIN_TIMED_ITERATIONS) {
      throw new IllegalArgumentException("cs.bench.iterations must be at least "
          + MIN_TIMED_ITERATIONS + " for median reporting");
    }

    System.out.println(String.format(Locale.ROOT,
        "BENCH-SETUP requested=%d queues=%d confProps=%d labels=%s "
            + "mutationLeaves=%s/%s warmups=%d iterations=%d",
        requestedQueues, gen.getQueueCount(), gen.getConf().size(),
        gen.getLabels(), gen.getMutationLeafA(), gen.getMutationLeafB(),
        warmups, iterations));

    long rmStart = System.nanoTime();
    MockRM rm = new MockRM(conf);
    rm.start();
    System.out.println(String.format(Locale.ROOT,
        "BENCH-SETUP requested=%d mockRmStartup_ms=%.1f", requestedQueues,
        (System.nanoTime() - rmStart) / 1e6));

    try {
      CapacityScheduler cs = (CapacityScheduler) rm.getResourceScheduler();
      RMContext rmContext = rm.getRMContext();
      Configuration mutatedConf =
          CSConfigBenchmarkGenerator.createMutatedCopy(conf, gen);
      Configuration originalConf = new Configuration(conf);
      CapacitySchedulerConfiguration originalCapacity =
          new CapacitySchedulerConfiguration(originalConf, false);
      CapacitySchedulerConfiguration mutatedCapacity =
          new CapacitySchedulerConfiguration(mutatedConf, false);
      CSConfigModel originalModel = originalCapacity.getModel();
      CSConfigModel mutatedModel = mutatedCapacity.getModel();
      ClusterFacts facts = ClusterFacts.capture(cs);
      CSConfigValidationEngine engine = new CSConfigValidationEngine();
      CompileResult originalCompiled = engine.compile(originalModel, facts);
      CompileResult mutatedCompiled = engine.compile(mutatedModel, facts);
      requireValid(originalCompiled.asValidationResult());
      requireValid(mutatedCompiled.asValidationResult());

      measureLoad(conf, rmContext, requestedQueues, gen.getQueueCount(),
          warmups, iterations);

      // (b) Configuration-only plan compilation from an immutable model.
      measure("compile", requestedQueues, gen.getQueueCount(), warmups,
          iterations, iteration -> {
            CSConfigModel target = iteration % 2 == 0
                ? mutatedModel : originalModel;
            long t0 = System.nanoTime();
            ValidatedQueuePlan plan = ValidatedQueuePlan.fromModel(target);
            long elapsed = System.nanoTime() - t0;
            if (plan.getRoot() == null) {
              throw new IllegalStateException("compiled plan had no root");
            }
            return elapsed;
          });

      // (c) Validation rules against plans compiled outside the timed region.
      measure("validation-rules", requestedQueues, gen.getQueueCount(),
          warmups, iterations, iteration -> {
            boolean useMutated = iteration % 2 == 0;
            long t0 = System.nanoTime();
            ValidationResult result = engine.validatePlan(
                useMutated ? mutatedModel : originalModel, facts,
                useMutated ? mutatedCompiled.getPlan()
                    : originalCompiled.getPlan());
            long elapsed = System.nanoTime() - t0;
            requireValid(result);
            return elapsed;
          });

      // Restore a known context before isolating live construction stages.
      cs.reinitialize(originalConf, rmContext);
      CSQueueStore existingQueues = queueStore(cs.getRootQueue());

      // (d) Queue construction without publishing or reinitializing live state.
      ValidatedQueuePlanMaterializer materializer =
          new ValidatedQueuePlanMaterializer();
      measure("materialize", requestedQueues, gen.getQueueCount(), warmups,
          iterations, iteration -> {
            long t0 = System.nanoTime();
            CSQueue root = materializer.materialize(
                originalCompiled.getPlan(), cs.getQueueContext(),
                existingQueues);
            long elapsed = System.nanoTime() - t0;
            if (root == null) {
              throw new IllegalStateException("materialized tree had no root");
            }
            return elapsed;
          });

      SeededMutableProvider provider = installMutableProvider(cs, rmContext,
          originalConf);

      // (e) Complete activation of a prevalidated plan.
      AtomicBoolean activateMutated = new AtomicBoolean(true);
      measure("activation", requestedQueues, gen.getQueueCount(), warmups,
          iterations, iteration -> {
            boolean useMutated = activateMutated.getAndSet(
                !activateMutated.get());
            return provider.runUnderMutationLock(() -> {
              long t0 = System.nanoTime();
              cs.reinitializeCompiledPrototype(
                  useMutated ? mutatedCapacity : originalCapacity,
                  rmContext,
                  useMutated ? mutatedCompiled : originalCompiled);
              return System.nanoTime() - t0;
            });
          });

      provider.runUnderMutationLock(() -> {
        cs.reinitializeCompiledPrototype(originalCapacity, rmContext,
            originalCompiled);
        return null;
      });

      // (f) Exact mutable-provider validate-log-activate-confirm operation.
      SchedConfUpdateInfo mutate = changedProperties(originalConf,
          mutatedConf);
      SchedConfUpdateInfo restore = changedProperties(mutatedConf,
          originalConf);
      AtomicBoolean applyMutated = new AtomicBoolean(true);
      measure("atomic-apply", requestedQueues, gen.getQueueCount(), warmups,
          iterations, iteration -> {
            boolean useMutated = applyMutated.getAndSet(!applyMutated.get());
            long t0 = System.nanoTime();
            ValidationResult result = provider.applyMutation(BENCHMARK_USER,
                useMutated ? mutate : restore);
            long elapsed = System.nanoTime() - t0;
            requireValid(result);
            return elapsed;
          });
    } finally {
      rm.stop();
      QueueMetrics.clearQueueMetrics();
      ClusterMetrics.destroy();
      DefaultMetricsSystem.shutdown();
    }
  }

  private void measure(String op, int requested, int actualQueues, int warmups,
      int iterations, TimedOp timedOp) throws Exception {
    System.gc();
    for (int i = 0; i < warmups; i++) {
      timedOp.runOnce(i);
    }
    long[] nanos = new long[iterations];
    for (int i = 0; i < iterations; i++) {
      nanos[i] = timedOp.runOnce(i);
    }
    report(op, requested, actualQueues, warmups, nanos);
  }

  private void measureLoad(Configuration conf, RMContext rmContext,
      int requestedQueues, int actualQueues, int warmups, int iterations)
      throws Exception {
    // The provider's loadConfiguration mutates the passed configuration by
    // adding a resource, so each iteration gets an untimed independent copy.
    measure("load", requestedQueues, actualQueues, warmups, iterations,
        iteration -> {
          Configuration loadConf = new Configuration(conf);
          long t0 = System.nanoTime();
          CapacityScheduler fresh = new CapacityScheduler();
          fresh.setConf(loadConf);
          fresh.setRMContext(rmContext);
          fresh.init(loadConf);
          long elapsed = System.nanoTime() - t0;
          fresh.stop();
          return elapsed;
        });
  }

  private SeededMutableProvider installMutableProvider(CapacityScheduler cs,
      RMContext rmContext, Configuration initial) throws Exception {
    YarnConfiguration bootstrap = new YarnConfiguration(initial);
    bootstrap.set(YarnConfiguration.SCHEDULER_CONFIGURATION_STORE_CLASS,
        YarnConfiguration.MEMORY_CONFIGURATION_STORE);
    SeededMutableProvider provider = new SeededMutableProvider(rmContext,
        initial);
    provider.init(bootstrap);
    Field providerField = CapacityScheduler.class.getDeclaredField(
        "csConfProvider");
    providerField.setAccessible(true);
    providerField.set(cs, provider);
    return provider;
  }

  private SchedConfUpdateInfo changedProperties(Configuration source,
      Configuration target) {
    SchedConfUpdateInfo update = new SchedConfUpdateInfo();
    for (Map.Entry<String, String> entry : target) {
      String key = entry.getKey();
      if (key.startsWith(CapacitySchedulerConfiguration.PREFIX)
          && !Objects.equals(source.getRaw(key), target.getRaw(key))) {
        update.getGlobalParams().put(key, target.getRaw(key));
      }
    }
    if (update.getGlobalParams().isEmpty()) {
      throw new IllegalStateException("benchmark mutation changed no values");
    }
    return update;
  }

  private CSQueueStore queueStore(CSQueue root) {
    CSQueueStore store = new CSQueueStore();
    addQueueTree(root, store);
    return store;
  }

  private void addQueueTree(CSQueue queue, CSQueueStore store) {
    store.add(queue);
    if (queue.getChildQueues() == null) {
      return;
    }
    for (CSQueue child : queue.getChildQueues()) {
      addQueueTree(child, store);
    }
  }

  private void requireValid(ValidationResult result) {
    if (!result.isValid()) {
      throw new IllegalStateException("generated config was invalid: "
          + result.getIssues());
    }
  }

  private void report(String op, int requested, int actualQueues, int warmups,
      long[] nanos) {
    long[] sorted = nanos.clone();
    Arrays.sort(sorted);
    int n = sorted.length;
    double median = sorted[rankIndex(50, n)] / 1e6;
    double p90 = sorted[rankIndex(90, n)] / 1e6;
    StringBuilder all = new StringBuilder();
    for (int i = 0; i < nanos.length; i++) {
      if (i > 0) {
        all.append(',');
      }
      all.append(String.format(Locale.ROOT, "%.1f", nanos[i] / 1e6));
    }
    System.out.println(String.format(Locale.ROOT,
        "BENCH op=%s requested=%d queues=%d iters=%d warmups=%d "
            + "median_ms=%.1f p90_ms=%.1f min_ms=%.1f max_ms=%.1f all_ms=[%s]",
        op, requested, actualQueues, n, warmups, median, p90,
        sorted[0] / 1e6, sorted[n - 1] / 1e6, all));
  }

  /** Nearest-rank percentile index for a sorted array of length n. */
  private static int rankIndex(int percentile, int n) {
    int rank = (int) Math.ceil(percentile / 100.0 * n);
    return Math.max(0, Math.min(n - 1, rank - 1));
  }

  private static int defaultIterations(int requestedQueues) {
    if (requestedQueues <= 100) {
      return 15;
    }
    if (requestedQueues <= 1000) {
      return 10;
    }
    return 5;
  }

  private static int defaultWarmups(int requestedQueues) {
    if (requestedQueues <= 1000) {
      return 2;
    }
    return 1;
  }
}
