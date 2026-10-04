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

package org.apache.hadoop.yarn.server.resourcemanager.webapp;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import javax.ws.rs.core.Response;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.test.TimedOutTestsListener;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.CapacityReservationSystem;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.ReservationSystem;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import static org.apache.hadoop.yarn.server.resourcemanager.webapp.SchedulerConfMutationTestSupport.PREFIX;
import static org.apache.hadoop.yarn.server.resourcemanager.webapp.SchedulerConfMutationTestSupport.etag;
import static org.apache.hadoop.yarn.server.resourcemanager.webapp.SchedulerConfMutationTestSupport.request;
import static org.apache.hadoop.yarn.server.resourcemanager.webapp.SchedulerConfMutationTestSupport.updateQueue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Review finding B1: scheduler configuration mutations against a persistent
 * configuration store, with the reservation system enabled, run
 * concurrently with the plan follower (its monitor, then the scheduler
 * write lock) and with plan synchronization (the reservation system lock,
 * the follower monitor, then the scheduler write lock). Holding the
 * scheduler write lock while refreshing the reservation system closes a
 * lock cycle with either of them. The test also asserts that no store file
 * is written and the reservation system is not refreshed while the current
 * thread holds the scheduler write lock.
 */
public class TestSchedulerConfMutationReservationLocking {

  private static final int MUTATIONS_PER_THREAD = 15;
  private static final long DEADLINE_SECONDS = 120;

  /** Checks store file writes; see {@link LockCheckingFileSystem}. */
  private static volatile CapacityScheduler checkedScheduler;
  private static volatile String checkedDir;
  private static final AtomicInteger STORE_WRITES = new AtomicInteger();
  private static final Queue<String> VIOLATIONS =
      new ConcurrentLinkedQueue<>();

  @TempDir
  private File testDir;

  private MockRM rm;
  private boolean deadlocked;

  @AfterEach
  public void tearDown() {
    checkedScheduler = null;
    checkedDir = null;
    // A deadlocked ResourceManager cannot be stopped
    if (rm != null && !deadlocked) {
      rm.stop();
    }
  }

  @Test
  @Timeout(value = 300, unit = TimeUnit.SECONDS)
  public void testMutationsWithPlanFollowerAndPlanSynchronization()
      throws Exception {
    STORE_WRITES.set(0);
    VIOLATIONS.clear();
    File storeDir = new File(testDir, "store");
    Map<String, String> schedulerConf = new LinkedHashMap<>();
    schedulerConf.put(PREFIX + "root.queues", "default,a,dedicated");
    schedulerConf.put(PREFIX + "root.default.capacity", "10");
    schedulerConf.put(PREFIX + "root.a.capacity", "10");
    schedulerConf.put(PREFIX + "root.dedicated.capacity", "80");
    schedulerConf.put(PREFIX + "root.dedicated.reservable", "true");
    YarnConfiguration conf = SchedulerConfMutationTestSupport
        .createConfiguration(storeDir, schedulerConf);
    conf.setBoolean(YarnConfiguration.RM_RESERVATION_SYSTEM_ENABLE, true);
    conf.setLong(
        YarnConfiguration.RM_RESERVATION_SYSTEM_PLAN_FOLLOWER_TIME_STEP, 5L);
    conf.setClass("fs.file.impl", LockCheckingFileSystem.class,
        org.apache.hadoop.fs.FileSystem.class);
    checkedDir = new Path(storeDir.toURI()).toUri().getPath();

    AtomicInteger reinitializations = new AtomicInteger();
    rm = new MockRM(conf) {
      @Override
      protected ReservationSystem createReservationSystem() {
        return new CapacityReservationSystem() {
          @Override
          public void reinitialize(Configuration config, RMContext context)
              throws YarnException {
            checkLock("reservation system refresh");
            reinitializations.incrementAndGet();
            super.reinitialize(config, context);
          }
        };
      }
    };
    rm.start();
    rm.registerNode("h1:1234", 64 * 1024, 64);
    CapacityScheduler cs = (CapacityScheduler) rm.getResourceScheduler();
    checkedScheduler = cs;
    ReservationSystem reservations = rm.getRMContext().getReservationSystem();
    assertEquals(1, reservations.getAllPlans().size());
    String plan = reservations.getAllPlans().keySet().iterator().next();
    RMWebServices ws = SchedulerConfMutationTestSupport.webServices(rm);
    int writesBefore = STORE_WRITES.get();
    int reinitializationsBefore = reinitializations.get();

    AtomicBoolean done = new AtomicBoolean();
    List<Throwable> failures = new ArrayList<>();
    CountDownLatch mutators = new CountDownLatch(2);
    List<Thread> threads = new ArrayList<>();
    for (int t = 0; t < 2; t++) {
      final String queue = t == 0 ? "root.a" : "root.default";
      threads.add(new Thread(() -> {
        try {
          for (int i = 0; i < MUTATIONS_PER_THREAD; i++) {
            // Accepted: a new maximum capacity; rejected by validation:
            // children capacities that no longer sum to 100
            boolean valid = i % 3 != 2;
            Response response = ws.updateSchedulerConfiguration(valid
                ? updateQueue(queue, "maximum-capacity",
                    String.valueOf(90 + i % 10))
                : updateQueue(queue, "capacity", "60"), request());
            assertEquals(valid ? 200 : 400, response.getStatus(),
                String.valueOf(response.getEntity()));
          }
        } catch (Throwable e) {
          synchronized (failures) {
            failures.add(e);
          }
        } finally {
          mutators.countDown();
        }
      }, "scheduler-conf-mutation-" + t));
    }
    threads.add(new Thread(() -> {
      try {
        while (!done.get()) {
          reservations.synchronizePlan(plan, true);
          Thread.sleep(1);
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }, "plan-synchronization"));
    threads.add(new Thread(() -> {
      try {
        while (!done.get()) {
          assertNotNull(etag(ws));
          Thread.sleep(1);
        }
      } catch (Throwable e) {
        synchronized (failures) {
          failures.add(e);
        }
      }
    }, "scheduler-conf-reader"));
    for (Thread thread : threads) {
      thread.setDaemon(true);
      thread.start();
    }

    if (!mutators.await(DEADLINE_SECONDS, TimeUnit.SECONDS)) {
      deadlocked = true;
      fail("Mutations did not finish in " + DEADLINE_SECONDS + " seconds\n"
          + TimedOutTestsListener.buildThreadDiagnosticString());
    }
    done.set(true);
    for (Thread thread : threads) {
      thread.join(TimeUnit.SECONDS.toMillis(DEADLINE_SECONDS));
      if (thread.isAlive()) {
        deadlocked = true;
        fail(thread.getName() + " did not finish\n"
            + TimedOutTestsListener.buildThreadDiagnosticString());
      }
    }

    assertTrue(failures.isEmpty(), String.valueOf(failures));
    assertTrue(VIOLATIONS.isEmpty(), String.valueOf(VIOLATIONS));
    // Every accepted mutation is logged and confirmed in the store and
    // refreshes the reservation system
    int accepted = 2 * (MUTATIONS_PER_THREAD - MUTATIONS_PER_THREAD / 3);
    assertTrue(STORE_WRITES.get() - writesBefore >= accepted,
        "store writes " + (STORE_WRITES.get() - writesBefore));
    assertEquals(accepted,
        reinitializations.get() - reinitializationsBefore);
    // The plan follower created the default reservation queue
    assertNotNull(cs.getQueue("dedicated" + "-default"));
  }

  private static void checkLock(String operation) {
    CapacityScheduler cs = checkedScheduler;
    if (cs != null && cs.isWriteLockHeldByCurrentThread()) {
      VIOLATIONS.add(operation + " under the scheduler write lock in "
          + Thread.currentThread().getName());
    }
  }

  /**
   * The local file system, recording writes into the configuration store
   * directory made while the scheduler write lock is held.
   */
  public static class LockCheckingFileSystem extends LocalFileSystem {
    private static void checkWrite(Path path, String operation) {
      String dir = checkedDir;
      if (dir != null && path.toUri().getPath().startsWith(dir)) {
        STORE_WRITES.incrementAndGet();
        checkLock(operation + " " + path);
      }
    }

    @Override
    public FSDataOutputStream create(Path f, FsPermission permission,
        boolean overwrite, int bufferSize, short replication, long blockSize,
        Progressable progress) throws IOException {
      checkWrite(f, "create");
      return super.create(f, permission, overwrite, bufferSize, replication,
          blockSize, progress);
    }

    @Override
    public boolean rename(Path src, Path dst) throws IOException {
      checkWrite(dst, "rename");
      return super.rename(src, dst);
    }

    @Override
    public boolean delete(Path f, boolean recursive) throws IOException {
      checkWrite(f, "delete");
      return super.delete(f, recursive);
    }
  }
}
