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

import java.util.Arrays;
import java.util.Locale;

import org.junit.jupiter.api.Test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.ClusterMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.CSConfigValidationEngine;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.ClusterFacts;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.ValidationResult;

/**
 * Baseline timing harness for the CapacityScheduler config load, reinitialize
 * and validate paths (wayfinder cs-config-reload ticket 0001).
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
 * - reinitialize: scheduler.reinitialize(changedConf, rmContext) on the live
 *   scheduler, alternating between a mutated and the original config.
 * - validate: immutable model parsing plus CSConfigValidationEngine, the
 *   POST /scheduler-conf/validate core.
 *
 * Results are printed to stdout as BENCH lines (see surefire -output.txt).
 */
public class CSConfigBaselineBenchmark {

  private static final int MIN_TIMED_ITERATIONS = 5;

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

      // (a) Full config load + queue hierarchy build.
      // The provider's loadConfiguration mutates the passed conf (addResource),
      // so every iteration gets a fresh untimed copy to stay independent.
      measure("load", requestedQueues, gen.getQueueCount(), warmups, iterations,
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

      // (b) reinitialize with a changed config on the live scheduler.
      measure("reinitialize", requestedQueues, gen.getQueueCount(), warmups,
          iterations, iteration -> {
            Configuration target = new Configuration(
                (iteration % 2 == 0) ? mutatedConf : originalConf);
            long t0 = System.nanoTime();
            cs.reinitialize(target, rmContext);
            return System.nanoTime() - t0;
          });

      // (c) POST /scheduler-conf/validate equivalent.
      measure("validate", requestedQueues, gen.getQueueCount(), warmups,
          iterations, iteration -> {
            Configuration target = new Configuration(
                (iteration % 2 == 0) ? mutatedConf : originalConf);
            long t0 = System.nanoTime();
            CapacitySchedulerConfiguration targetCapacity =
                new CapacitySchedulerConfiguration(target, false);
            ValidationResult result = new CSConfigValidationEngine().validate(
                targetCapacity.getModel(), ClusterFacts.capture(cs));
            long elapsed = System.nanoTime() - t0;
            if (!result.isValid()) {
              throw new IllegalStateException("generated config was invalid");
            }
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
