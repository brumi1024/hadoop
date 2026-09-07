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
import java.lang.management.ManagementFactory;
import java.util.Arrays;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;

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
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.CSConfigValidationEngine;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.ClusterFacts;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.CompileResult;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.QueuePlanBenchmarkSupport;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.ValidationResult;
import org.apache.hadoop.yarn.webapp.dao.SchedConfUpdateInfo;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.when;

/**
 * Phase timing harness for Capacity Scheduler configuration mutations.
 *
 * Deliberately NOT named Test* so that regular module test runs skip it.
 * Run it explicitly, from the repo root:
 *
 * <pre>
 * JAVA_HOME=&lt;jdk-17-home&gt; PATH="$JAVA_HOME/bin:$PATH" ./mvnw test \
 *   -Dtest=CSConfigBaselineBenchmark -Dcs.bench.sizes=1000 \
 *   -Dcs.bench.iterations=5 -Dcs.bench.warmups=2 \
 *   -Dcs.bench.revision=&lt;commit&gt; -Dcs.bench.fork=1 \
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
 * - {@code cs.bench.revision}: exact source revision under test
 * - {@code cs.bench.fork}: caller-assigned independent JVM fork identifier
 *
 * Measured foundation operations per size, against a live MockRM:
 * - load: new CapacityScheduler + setConf + setRMContext + init(conf), i.e. the
 *   serviceInit -> initScheduler -> initializeQueues path; stop() is untimed.
 * - model-build: immutable configuration model construction from an already
 *   loaded candidate snapshot.
 * - legacy-validation: complete validation, including its isolated CSQueue
 *   hierarchy construction.
 * - reinitialize: scheduler.reinitialize(changedConf, rmContext) on the live
 *   scheduler, alternating between a mutated and the original config.
 * - live-materialization: live CSQueue constructors with existing queue state,
 *   without publishing the resulting hierarchy.
 * - activation: a prevalidated candidate through the complete live activation
 *   path, excluding validation and mutation-store work.
 * - atomic-apply: the mutable provider's validate-log-activate-confirm path.
 *
 * Compiled implementations add plan-compile and validation-rules operations
 * to this harness. Keeping the foundation stages explicit avoids treating the
 * legacy validation hierarchy build as pure rule evaluation.
 *
 * Results are printed to stdout as BENCH lines (see surefire -output.txt).
 */
public class CSConfigBaselineBenchmark {

  private static final int MIN_TIMED_ITERATIONS = 5;
  private static final String BENCHMARK_REVISION =
      System.getProperty("cs.bench.revision", "unspecified");
  private static final String BENCHMARK_FORK =
      System.getProperty("cs.bench.fork", "unspecified");
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
    System.out.println(String.format(Locale.ROOT,
        "BENCH-ENV revision=%s fork=%s java=%s vm=%s os=%s osVersion=%s "
            + "arch=%s jvmArgs=%s",
        BENCHMARK_REVISION, BENCHMARK_FORK,
        System.getProperty("java.version"), System.getProperty("java.vm.name"),
        System.getProperty("os.name"), System.getProperty("os.version"),
        System.getProperty("os.arch"),
        ManagementFactory.getRuntimeMXBean().getInputArguments()));
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
        "BENCH-SETUP revision=%s fork=%s requested=%d queues=%d confProps=%d "
            + "labels=%s mutationLeaves=%s/%s warmups=%d iterations=%d",
        BENCHMARK_REVISION, BENCHMARK_FORK, requestedQueues,
        gen.getQueueCount(), gen.getConf().size(), gen.getLabels(),
        gen.getMutationLeafA(), gen.getMutationLeafB(), warmups, iterations));

    long rmStart = System.nanoTime();
    MockRM rm = new MockRM(conf);
    rm.start();
    System.out.println(String.format(Locale.ROOT,
        "BENCH-SETUP revision=%s fork=%s requested=%d mockRmStartup_ms=%.1f",
        BENCHMARK_REVISION, BENCHMARK_FORK, requestedQueues,
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
      CSConfigValidationEngine validationEngine =
          new CSConfigValidationEngine();

      measureLoad(conf, rmContext, requestedQueues, gen.getQueueCount(),
          warmups, iterations);

      // Immutable model construction, the input seam for future plan compile.
      measure("model-build", requestedQueues, gen.getQueueCount(), warmups,
          iterations,
          iteration -> {
            Configuration target = new Configuration(
                iteration % 2 == 0 ? mutatedConf : originalConf);
            CapacitySchedulerConfiguration targetCapacity =
                new CapacitySchedulerConfiguration(target, false);
            long t0 = System.nanoTime();
            CSConfigModel model = targetCapacity.getModel();
            long elapsed = System.nanoTime() - t0;
            if (model.getRoot() == null) {
              throw new IllegalStateException("configuration model had no root");
            }
            return elapsed;
          });

      // Full foundation validation. This deliberately includes construction of
      // an isolated CSQueue hierarchy and is not labelled pure rule execution.
      measure("legacy-validation", requestedQueues, gen.getQueueCount(),
          warmups, iterations, iteration -> {
            long t0 = System.nanoTime();
            ValidationResult result = validationEngine.validate(
                iteration % 2 == 0 ? mutatedModel : originalModel, facts);
            long elapsed = System.nanoTime() - t0;
            requireValid(result);
            return elapsed;
          });

      // Existing public refresh path, retained to anchor earlier measurements.
      measure("reinitialize", requestedQueues, gen.getQueueCount(), warmups,
          iterations, iteration -> {
            Configuration target = new Configuration(
                (iteration % 2 == 0) ? mutatedConf : originalConf);
            long t0 = System.nanoTime();
            cs.reinitialize(target, rmContext);
            return System.nanoTime() - t0;
          });

      // Restore a known live configuration before isolating construction and
      // activation. The live materialization result is never published.
      cs.reinitialize(originalConf, rmContext);
      measureLiveMaterialization(cs, originalCapacity, requestedQueues,
          gen.getQueueCount(), warmups, iterations);

      SeededMutableProvider provider = installMutableProvider(cs, rmContext,
          originalConf);

      AtomicBoolean activateMutated = new AtomicBoolean(true);
      measure("activation", requestedQueues, gen.getQueueCount(), warmups,
          iterations, iteration -> {
            boolean useMutated = toggle(activateMutated);
            return provider.runUnderMutationLock(() -> {
              long t0 = System.nanoTime();
              cs.reinitializePreValidated(
                  useMutated ? mutatedCapacity : originalCapacity,
                  rmContext,
                  useMutated ? mutatedModel : originalModel, facts);
              return System.nanoTime() - t0;
            });
          });

      provider.runUnderMutationLock(() -> {
        cs.reinitializePreValidated(originalCapacity, rmContext, originalModel,
            facts);
        return null;
      });

      SchedConfUpdateInfo mutate = changedProperties(originalConf,
          mutatedConf);
      SchedConfUpdateInfo restore = changedProperties(mutatedConf,
          originalConf);
      provider.runUnderMutationLock(() ->
          cs.runWithStableQueueConfiguration(() -> {
            requireEligible(validationEngine.compile(mutatedModel,
                ClusterFacts.capture(cs)));
            return null;
          }));
      AtomicBoolean applyMutated = new AtomicBoolean(true);
      measure("atomic-apply", requestedQueues, gen.getQueueCount(), warmups,
          iterations, iteration -> {
            boolean useMutated = toggle(applyMutated);
            long t0 = System.nanoTime();
            ValidationResult result = provider.applyMutation(BENCHMARK_USER,
                useMutated ? mutate : restore);
            long elapsed = System.nanoTime() - t0;
            requireValid(result);
            return elapsed;
          });
      // Preserve the foundation phase order through complete atomic apply.
      measureCompiledPlanPhases(requestedQueues, gen, warmups, iterations,
          originalModel, mutatedModel, ClusterFacts.capture(cs));
      measureCompiledActivationPhases(provider, requestedQueues,
          gen.getQueueCount(), warmups, iterations, mutate, restore);
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

  private void measureCompiledPlanPhases(int requestedQueues,
      CSConfigBenchmarkGenerator.GeneratedConfig generated, int warmups,
      int iterations, CSConfigModel originalModel, CSConfigModel mutatedModel,
      ClusterFacts facts) throws Exception {
    CSConfigValidationEngine validationEngine = new CSConfigValidationEngine();
    CompileResult originalCompiled = validationEngine.compile(originalModel,
        facts);
    CompileResult mutatedCompiled = validationEngine.compile(mutatedModel,
        facts);
    requireEligible(originalCompiled);
    requireEligible(mutatedCompiled);
    QueuePlanBenchmarkSupport.Session originalSession =
        QueuePlanBenchmarkSupport.compile(originalModel, facts);
    QueuePlanBenchmarkSupport.Session mutatedSession =
        QueuePlanBenchmarkSupport.compile(mutatedModel, facts);

    measure("plan-compile", requestedQueues, generated.getQueueCount(),
        warmups, iterations, iteration -> {
          long t0 = System.nanoTime();
          QueuePlanBenchmarkSupport.Session session =
              QueuePlanBenchmarkSupport.compile(
                  iteration % 2 == 0 ? mutatedModel : originalModel, facts);
          long elapsed = System.nanoTime() - t0;
          if (session == null) {
            throw new IllegalStateException("plan compilation returned null");
          }
          return elapsed;
        });

    measure("validation-rules", requestedQueues, generated.getQueueCount(),
        warmups, iterations, iteration -> {
          long t0 = System.nanoTime();
          ValidationResult result = QueuePlanBenchmarkSupport.validate(
              iteration % 2 == 0 ? mutatedSession : originalSession);
          long elapsed = System.nanoTime() - t0;
          requireValid(result);
          return elapsed;
        });
  }

  private void measureCompiledActivationPhases(SeededMutableProvider provider,
      int requestedQueues, int actualQueues, int warmups, int iterations,
      SchedConfUpdateInfo mutate, SchedConfUpdateInfo restore) throws Exception {
    requireValid(provider.applyMutation(BENCHMARK_USER, restore));
    // Keep instrumentation out of the headline atomic-apply measurement.
    // Forward to the real constructors without replacing the queue manager or
    // its context. Each sample is one complete, committed provider mutation.
    ValidatedQueuePlanMaterializer delegate = new ValidatedQueuePlanMaterializer();
    long[] materialization = new long[iterations];
    long[] activation = new long[iterations];
    long[] timing = new long[3];
    try (MockedConstruction<ValidatedQueuePlanMaterializer> ignored =
        mockConstruction(ValidatedQueuePlanMaterializer.class, (mock, context) ->
            when(mock.materialize(any(), any(), any())).thenAnswer(invocation -> {
              timing[2]++;
              long start = System.nanoTime();
              CSQueue root = delegate.materialize(invocation.getArgument(0),
                  invocation.getArgument(1), invocation.getArgument(2));
              timing[1] = System.nanoTime();
              timing[0] = timing[1] - start;
              return root;
            }))) {
      System.gc();
      for (int i = -warmups; i < iterations; i++) {
        Arrays.fill(timing, 0);
        ValidationResult result = provider.applyMutation(BENCHMARK_USER,
            i % 2 == 0 ? mutate : restore);
        long end = System.nanoTime();
        requireValid(result);
        if (timing[2] != 1) {
          throw new IllegalStateException("Expected exactly one compiled materialization, got "
              + timing[2]);
        }
        if (i >= 0) {
          materialization[i] = timing[0];
          activation[i] = end - timing[1];
        }
      }
    }
    report("compiled-materialization", requestedQueues, actualQueues, warmups,
        materialization);
    report("compiled-activation-after-materialization", requestedQueues,
        actualQueues, warmups, activation);
  }

  private void measureLiveMaterialization(CapacityScheduler cs,
      CapacitySchedulerConfiguration configuration, int requestedQueues,
      int actualQueues, int warmups, int iterations) throws Exception {
    CSQueueStore existingQueues = queueStore(cs.getRootQueue());
    CapacitySchedulerQueueManager.QueueHook queueHook =
        new CapacitySchedulerQueueManager.QueueHook();
    measure("live-materialization", requestedQueues, actualQueues, warmups,
        iterations, iteration -> {
          CSQueueStore materializedQueues = new CSQueueStore();
          long t0 = System.nanoTime();
          CSQueue root = CapacitySchedulerQueueManager.parseQueue(
              cs.getQueueContext(), configuration, null,
              CapacitySchedulerConfiguration.ROOT, materializedQueues,
              existingQueues, queueHook);
          long elapsed = System.nanoTime() - t0;
          if (root == null || materializedQueues.getQueues().isEmpty()) {
            throw new IllegalStateException(
                "live materialization produced no queues");
          }
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

  private boolean toggle(AtomicBoolean nextMutated) {
    boolean useMutated = nextMutated.get();
    nextMutated.set(!useMutated);
    return useMutated;
  }

  private void requireValid(ValidationResult result) {
    if (!result.isValid()) {
      throw new IllegalStateException("generated config was invalid: "
          + result.getIssues());
    }
  }

  private void requireEligible(CompileResult result) {
    if (!result.isCompiledActivationEligible()) {
      throw new IllegalStateException("generated config was not eligible: "
          + result.getFallbackReasons() + " issues=" + result.getIssues());
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
        "BENCH revision=%s fork=%s op=%s requested=%d queues=%d "
            + "iters=%d warmups=%d "
            + "median_ms=%.1f p90_ms=%.1f min_ms=%.1f max_ms=%.1f all_ms=[%s]",
        BENCHMARK_REVISION, BENCHMARK_FORK, op, requested, actualQueues, n,
        warmups, median, p90,
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
