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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.conf;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.ValidationResult;
import org.apache.hadoop.yarn.webapp.dao.QueueConfigInfo;
import org.apache.hadoop.yarn.webapp.dao.SchedConfUpdateInfo;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Concurrency pin for the mutable scheduler configuration pipeline. */
public class TestMutableCSConfigurationProviderConcurrency {

  private static final Logger LOG = LoggerFactory.getLogger(
      TestMutableCSConfigurationProviderConcurrency.class);

  private static final String SEED_PROPERTY =
      "hadoop.cs.config.concurrency.seed";
  private static final String OPERATIONS_PROPERTY =
      "hadoop.cs.config.concurrency.operations";
  private static final long DEFAULT_SEED = 0x5EED5EEDL;
  private static final int DEFAULT_OPERATIONS = 200;
  private static final int MUTATOR_COUNT = 3;
  private static final int MIN_TEAR_FREE_CHECKS = 10;
  private static final String MARKER_PROPERTY_A = "concurrency-marker-a";
  private static final String MARKER_PROPERTY_B = "concurrency-marker-b";
  private static final String MARKER_KEY_A = CapacitySchedulerConfiguration.PREFIX
      + "root.a." + MARKER_PROPERTY_A;
  private static final String MARKER_KEY_B = CapacitySchedulerConfiguration.PREFIX
      + "root.a." + MARKER_PROPERTY_B;
  private static final String ROOT_A = "root.a";
  private static final UserGroupInformation TEST_USER = UserGroupInformation
      .createUserForTesting("concurrency-test", new String[] {});

  private MockRM rm;

  @AfterEach
  public void tearDown() {
    if (rm != null) {
      rm.stop();
      rm = null;
    }
  }

  @Test
  public void testConcurrentMutationRefreshFormatAndReads() throws Exception {
    long seed = Long.getLong(SEED_PROPERTY, DEFAULT_SEED);
    int operations = Integer.getInteger(OPERATIONS_PROPERTY,
        DEFAULT_OPERATIONS);
    assertTrue(operations > 0, "operation count must be positive");
    LOG.info("Running mutable configuration concurrency test with seed {} "
        + "and {} operations per actor", seed, operations);

    YarnConfiguration conf = new YarnConfiguration();
    conf.setClass(YarnConfiguration.RM_SCHEDULER, CapacityScheduler.class,
        ResourceScheduler.class);
    conf.set(YarnConfiguration.SCHEDULER_CONFIGURATION_STORE_CLASS,
        YarnConfiguration.MEMORY_CONFIGURATION_STORE);
    rm = new MockRM(conf);
    rm.start();

    CapacityScheduler scheduler = (CapacityScheduler) rm.getResourceScheduler();
    MutableCSConfigurationProvider provider = (MutableCSConfigurationProvider)
        scheduler.getMutableConfProvider();

    ConcurrentMap<Long, String> versionMarkers = new ConcurrentHashMap<>();
    versionMarkers.put(provider.getConfigVersion(), marker(provider));
    Set<String> recordedMarkerPairs = ConcurrentHashMap.newKeySet();
    recordedMarkerPairs.add(marker(provider));
    AtomicLong totalValidMutations = new AtomicLong();
    ConcurrentLinkedQueue<Long> preFormatVersions =
        new ConcurrentLinkedQueue<>();
    AtomicReference<Throwable> escaped = new AtomicReference<>();
    AtomicInteger tearFreeChecks = new AtomicInteger();
    CountDownLatch start = new CountDownLatch(1);
    ExecutorService executor = Executors.newFixedThreadPool(
        MUTATOR_COUNT + 4);
    List<Runnable> actors = new ArrayList<>();

    for (int i = 0; i < MUTATOR_COUNT; i++) {
      final int actor = i;
      actors.add(() -> runActor(start, escaped, () -> {
        List<MutationPlan> deck = mutationDeck(seed, actor, operations);
        LOG.info("Mutation actor {} uses {} seeded operations", actor,
            deck.size());
        for (MutationPlan plan : deck) {
          if (escaped.get() != null) {
            break;
          }
          applyTiming(plan.timing);
          recordedMarkerPairs.add(plan.payloadPair());
          ValidationResult result = provider.applyMutation(
              TEST_USER, markerUpdate(plan));
          if (!result.isValid()) {
            throw new AssertionError("unexpected invalid mutation: "
                + result.getIssues());
          }
          totalValidMutations.incrementAndGet();
          recordStableVersionMarker(provider, versionMarkers);
        }
      }));
    }

    actors.add(() -> runActor(start, escaped, () -> {
      List<Integer> deck = timingDeck(seed, MUTATOR_COUNT, operations);
      for (int timing : deck) {
        if (escaped.get() != null) {
          break;
        }
        applyTiming(timing);
        provider.runUnderMutationLock(() -> {
          long versionBefore = provider.getConfigVersion();
          provider.formatConfigurationInStore(conf);
          preFormatVersions.add(versionBefore);
          versionMarkers.clear();
          String initialMarker = marker(provider);
          versionMarkers.put(provider.getConfigVersion(), initialMarker);
          recordedMarkerPairs.add(initialMarker);
          return null;
        });
      }
    }));

    actors.add(() -> runActor(start, escaped, () -> {
      for (int timing : timingDeck(seed, MUTATOR_COUNT + 1, operations)) {
        if (escaped.get() != null) {
          break;
        }
        applyTiming(timing);
        rm.getAdminService().refreshQueues();
      }
    }));

    for (int i = 0; i < 2; i++) {
      final int reader = i;
      actors.add(() -> runActor(start, escaped, () -> {
        for (int timing : timingDeck(seed, MUTATOR_COUNT + 2 + reader,
            operations)) {
          if (escaped.get() != null) {
            break;
          }
          applyTiming(timing);
          assertTearFreePair(provider, versionMarkers, recordedMarkerPairs,
              tearFreeChecks);
        }
      }));
    }

    for (Runnable actor : actors) {
      executor.submit(actor);
    }
    start.countDown();
    executor.shutdown();
    assertTrue(executor.awaitTermination(10, TimeUnit.MINUTES),
        "concurrency actors did not finish");
    Throwable failure = escaped.get();
    if (failure != null) {
      throw new AssertionError("concurrency actor escaped a throwable", failure);
    }

    long finalVersion = provider.getConfigVersion();
    long accountedMutations = preFormatVersions.stream()
        .mapToLong(version -> version - 1L).sum() + finalVersion - 1L;
    assertEquals(totalValidMutations.get(), accountedMutations,
        "every valid mutation must advance exactly one configuration epoch");
    assertTrue(tearFreeChecks.get() >= Math.min(MIN_TEAR_FREE_CHECKS,
            Math.max(1, operations)),
        "tear-free readers must execute a non-vacuous number of checks");
    assertTearFreePair(provider, versionMarkers, recordedMarkerPairs,
        tearFreeChecks);
  }

  private static void runActor(CountDownLatch start,
      AtomicReference<Throwable> escaped, ThrowingRunnable actor) {
    try {
      start.await();
      actor.run();
    } catch (Throwable failure) {
      escaped.compareAndSet(null, failure);
    }
  }

  private static List<MutationPlan> mutationDeck(long seed, int actor,
      int operations) {
    Random random = new Random(seed ^ (0x9E3779B97F4A7C15L * (actor + 1)));
    List<MutationPlan> deck = new ArrayList<>();
    for (int op = 0; op < operations; op++) {
      String property = random.nextBoolean() ? MARKER_PROPERTY_A
          : MARKER_PROPERTY_B;
      deck.add(new MutationPlan(property,
          actor + "-" + op + "-" + random.nextInt(), random.nextInt(32)));
    }
    Collections.shuffle(deck, random);
    return deck;
  }

  private static List<Integer> timingDeck(long seed, int actor,
      int operations) {
    Random random = new Random(seed ^ (0xD1B54A32D192ED03L * (actor + 1)));
    List<Integer> deck = new ArrayList<>();
    for (int op = 0; op < operations; op++) {
      deck.add(random.nextInt(32));
    }
    Collections.shuffle(deck, random);
    return deck;
  }

  private static void applyTiming(int timing) {
    if ((timing & 3) == 0) {
      Thread.yield();
    }
  }

  private static SchedConfUpdateInfo markerUpdate(MutationPlan plan) {
    SchedConfUpdateInfo update = new SchedConfUpdateInfo();
    Map<String, String> values = new HashMap<>();
    values.put(MARKER_PROPERTY_A, plan.markerA());
    values.put(MARKER_PROPERTY_B, plan.markerB());
    update.getUpdateQueueInfo().add(new QueueConfigInfo(ROOT_A, values));
    return update;
  }

  private static String marker(MutableCSConfigurationProvider provider) {
    return marker(provider.getConfiguration());
  }

  private static String marker(Configuration configuration) {
    return Objects.toString(configuration.get(MARKER_KEY_A), "") + "|"
        + Objects.toString(configuration.get(MARKER_KEY_B), "");
  }

  private static void recordStableVersionMarker(
      MutableCSConfigurationProvider provider,
      Map<Long, String> versionMarkers) throws Exception {
    while (true) {
      long versionBefore = provider.getConfigVersion();
      String observed = marker(provider.getConfiguration());
      long versionAfter = provider.getConfigVersion();
      if (versionBefore == versionAfter) {
        String previous = versionMarkers.putIfAbsent(versionBefore, observed);
        if (previous != null) {
          assertEquals(previous, observed,
              "stable snapshots for one version must have one marker");
        }
        return;
      }
    }
  }

  private static void assertTearFreePair(
      MutableCSConfigurationProvider provider,
      Map<Long, String> versionMarkers,
      Set<String> recordedMarkerPairs,
      AtomicInteger tearFreeChecks) throws Exception {
    long versionBefore = provider.getConfigVersion();
    String observed = marker(provider.getConfiguration());
    long versionAfter = provider.getConfigVersion();
    assertTrue(recordedMarkerPairs.contains(observed),
        "marker pair must come from a recorded mutation payload");
    if (versionBefore == versionAfter) {
      String expected = versionMarkers.get(versionBefore);
      if (expected != null) {
        assertEquals(expected, observed,
            "stable version and configuration marker must agree");
        tearFreeChecks.incrementAndGet();
      }
    }
  }

  private static final class MutationPlan {
    private final String property;
    private final String value;
    private final String companionValue;
    private final int timing;

    private MutationPlan(String property, String value, int timing) {
      this.property = property;
      this.value = value;
      this.companionValue = value + "-pair";
      this.timing = timing;
    }

    private String markerA() {
      return property.endsWith("-a") ? value : companionValue;
    }

    private String markerB() {
      return property.endsWith("-b") ? value : companionValue;
    }

    private String payloadPair() {
      return markerA() + "|" + markerB();
    }
  }

  @FunctionalInterface
  private interface ThrowingRunnable {
    void run() throws Exception;
  }
}
