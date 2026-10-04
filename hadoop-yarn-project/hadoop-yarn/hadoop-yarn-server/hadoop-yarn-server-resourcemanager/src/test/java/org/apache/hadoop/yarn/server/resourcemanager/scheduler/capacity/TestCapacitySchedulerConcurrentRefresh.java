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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.yarn.LocalConfigurationProvider;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Concurrent queue refreshes (admin refreshQueues, the auto refresh policy,
 * a scheduler-conf PUT, format, HA transitionToActive) must activate the
 * configurations in the order they loaded them. The scheduler loads and
 * validates the configuration before taking its write lock; without the
 * refresh lock a refresh that loaded first could activate its configuration
 * last and leave a stale configuration active.
 */
public class TestCapacitySchedulerConcurrentRefresh {

  private static final String ULF =
      CapacitySchedulerConfiguration.PREFIX + "root.default.user-limit-factor";

  private MockRM rm;

  @AfterEach
  public void tearDown() {
    BlockingProvider.reset();
    if (rm != null) {
      rm.stop();
    }
  }

  @Test
  @Timeout(60)
  public void testRefreshesActivateInLoadOrder() throws Exception {
    BlockingProvider.serve("1");
    YarnConfiguration conf = new YarnConfiguration();
    conf.setClass(YarnConfiguration.RM_SCHEDULER, CapacityScheduler.class,
        ResourceScheduler.class);
    conf.set(YarnConfiguration.RM_CONFIGURATION_PROVIDER_CLASS,
        BlockingProvider.class.getName());
    rm = new MockRM(conf);
    rm.start();
    CapacityScheduler cs = (CapacityScheduler) rm.getResourceScheduler();
    assertEquals("1", cs.getConfiguration().get(ULF));

    // The first refresh loads "2" and stalls inside the provider load.
    BlockingProvider.serve("2");
    AtomicReference<Throwable> failure = new AtomicReference<>();
    Thread first = refresh("first", failure);
    BlockingProvider.blockOnce(first);
    first.start();
    assertTrue(BlockingProvider.held.await(30, TimeUnit.SECONDS));

    // The second refresh starts after "3" is published, so it must win.
    BlockingProvider.serve("3");
    Thread second = refresh("second", failure);
    second.start();
    GenericTestUtils.waitFor(() -> !second.isAlive()
        || cs.hasQueuedRefresh(second), 10, 30000);

    BlockingProvider.release.countDown();
    first.join(30000);
    second.join(30000);
    assertNull(failure.get());
    assertEquals("3", cs.getConfiguration().get(ULF),
        "the configuration loaded last must be the active one");
    assertEquals(3f, ((LeafQueue) cs.getQueue("default")).getUserLimitFactor(),
        1e-6);
  }

  private Thread refresh(String name, AtomicReference<Throwable> failure) {
    return new Thread(() -> {
      try {
        rm.getAdminService().refreshQueues();
      } catch (Throwable t) {
        failure.compareAndSet(null, t);
      }
    }, name);
  }

  /**
   * Serves {@code capacity-scheduler.xml} from memory and can stall one
   * thread right after it has read the served content.
   */
  public static final class BlockingProvider
      extends LocalConfigurationProvider {
    private static volatile byte[] schedulerXml;
    private static volatile Thread blocked;
    private static volatile CountDownLatch held = new CountDownLatch(1);
    private static volatile CountDownLatch release = new CountDownLatch(1);

    static void serve(String userLimitFactor) throws IOException {
      Configuration conf = new Configuration(false);
      conf.set(CapacitySchedulerConfiguration.PREFIX + "root.queues",
          "default");
      conf.set(CapacitySchedulerConfiguration.PREFIX
          + "root.default.capacity", "100");
      conf.set(ULF, userLimitFactor);
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      conf.writeXml(out);
      schedulerXml = out.toByteArray();
    }

    static void blockOnce(Thread thread) {
      blocked = thread;
    }

    static void reset() {
      release.countDown();
      blocked = null;
      held = new CountDownLatch(1);
      release = new CountDownLatch(1);
    }

    @Override
    public InputStream getConfigurationInputStream(Configuration bootstrapConf,
        String name) throws IOException, YarnException {
      if (!YarnConfiguration.CS_CONFIGURATION_FILE.equals(name)) {
        return super.getConfigurationInputStream(bootstrapConf, name);
      }
      byte[] served = schedulerXml;
      if (Thread.currentThread() == blocked) {
        blocked = null;
        held.countDown();
        try {
          release.await();
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new IOException(e);
        }
      }
      return new ByteArrayInputStream(served);
    }
  }
}
