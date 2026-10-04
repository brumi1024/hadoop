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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * A queue refresh validates the new configuration before taking the
 * scheduler write lock, so a configuration the validation rejects is never
 * activated: the scheduler keeps its configuration and queue context
 * configuration objects, where trunk swapped them in and rolled back (or,
 * for an invalid scheduler allocation, kept the rejected configuration).
 */
public class TestCapacitySchedulerValidateBeforeActivation {

  private static final String P = CapacitySchedulerConfiguration.PREFIX + "root";

  @Test
  public void testInvalidAllocationIsNotActivated() throws Exception {
    try (MockRM rm = start()) {
      CapacityScheduler cs = (CapacityScheduler) rm.getResourceScheduler();
      CapacitySchedulerConfiguration active = cs.getConfiguration();
      CapacitySchedulerConfiguration queueConf =
          cs.getQueueContext().getConfiguration();
      Configuration proposed = new Configuration(cs.getConf());
      proposed.setInt(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB, 0);

      YarnRuntimeException e = assertThrows(YarnRuntimeException.class,
          () -> cs.reinitialize(proposed, rm.getRMContext()));
      assertEquals("Invalid resource scheduler memory allocation"
          + " configuration, yarn.scheduler.minimum-allocation-mb=0,"
          + " yarn.scheduler.maximum-allocation-mb=8192, min and max should be"
          + " greater than 0, max should be no smaller than min.",
          e.getMessage());
      assertSame(active, cs.getConfiguration());
      assertSame(queueConf, cs.getQueueContext().getConfiguration());
    }
  }

  @Test
  public void testInvalidQueueConfigurationIsNotActivated() throws Exception {
    try (MockRM rm = start()) {
      CapacityScheduler cs = (CapacityScheduler) rm.getResourceScheduler();
      CapacitySchedulerConfiguration active = cs.getConfiguration();
      CapacitySchedulerConfiguration queueConf =
          cs.getQueueContext().getConfiguration();
      CSQueue a = cs.getQueue("a");
      Configuration proposed = new Configuration(cs.getConf());
      proposed.set(P + ".a.capacity", "60");

      IOException e = assertThrows(IOException.class,
          () -> cs.reinitialize(proposed, rm.getRMContext()));
      assertEquals("Failed to re-init queues : Illegal capacity sum of 1.1"
          + " for children of queue root for label=. It should be either 0 or"
          + " 1.0", e.getMessage());
      assertSame(active, cs.getConfiguration());
      assertSame(queueConf, cs.getQueueContext().getConfiguration());
      assertSame(a, cs.getQueue("a"));
      assertEquals(0.5f, a.getCapacity(), 1e-6);
    }
  }

  private static MockRM start() throws Exception {
    Configuration conf = new YarnConfiguration();
    conf.setClass(YarnConfiguration.RM_SCHEDULER, CapacityScheduler.class,
        ResourceScheduler.class);
    conf.set(P + ".queues", "a,b");
    conf.set(P + ".a.capacity", "50");
    conf.set(P + ".b.capacity", "50");
    MockRM rm = new MockRM(conf);
    rm.start();
    rm.registerNode("h1:1234", 100 * 1024, 100);
    return rm;
  }
}
