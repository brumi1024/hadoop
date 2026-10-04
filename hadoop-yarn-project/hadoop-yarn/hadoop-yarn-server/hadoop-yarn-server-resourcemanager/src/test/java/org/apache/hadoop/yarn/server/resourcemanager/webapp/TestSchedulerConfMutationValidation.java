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
import java.util.LinkedHashMap;
import java.util.Map;

import javax.ws.rs.core.Response;

import org.apache.hadoop.test.GenericTestUtils.LogCapturer;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.MutableConfigurationProvider;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
import org.apache.hadoop.yarn.webapp.dao.SchedConfUpdateInfo;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.yarn.server.resourcemanager.webapp.SchedulerConfMutationTestSupport.PREFIX;
import static org.apache.hadoop.yarn.server.resourcemanager.webapp.SchedulerConfMutationTestSupport.etag;
import static org.apache.hadoop.yarn.server.resourcemanager.webapp.SchedulerConfMutationTestSupport.pendingMutationFiles;
import static org.apache.hadoop.yarn.server.resourcemanager.webapp.SchedulerConfMutationTestSupport.request;
import static org.apache.hadoop.yarn.server.resourcemanager.webapp.SchedulerConfMutationTestSupport.updateGlobal;
import static org.apache.hadoop.yarn.server.resourcemanager.webapp.SchedulerConfMutationTestSupport.updateQueue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@code PUT /scheduler-conf} validates the configuration a mutation would
 * activate before the mutation is logged to the configuration store: a
 * mutation the queue refresh would reject fails with the response the
 * refresh failure produces, and leaves the store, the provider, the
 * scheduler and the ETag unchanged.
 */
public class TestSchedulerConfMutationValidation {

  @TempDir
  private File testDir;

  private File storeDir;
  private MockRM rm;
  private RMWebServices ws;
  private CapacityScheduler cs;
  private MutableConfigurationProvider provider;

  @BeforeEach
  public void setUp() throws Exception {
    storeDir = new File(testDir, "store");
    Map<String, String> schedulerConf = new LinkedHashMap<>();
    schedulerConf.put(PREFIX + "root.queues", "a,b");
    schedulerConf.put(PREFIX + "root.a.capacity", "50");
    schedulerConf.put(PREFIX + "root.b.capacity", "50");
    YarnConfiguration conf = SchedulerConfMutationTestSupport
        .createConfiguration(storeDir, schedulerConf);
    rm = new MockRM(conf);
    rm.start();
    ws = SchedulerConfMutationTestSupport.webServices(rm);
    cs = (CapacityScheduler) rm.getResourceScheduler();
    provider = cs.getMutableConfProvider();
  }

  @AfterEach
  public void tearDown() {
    storeDir.setWritable(true);
    if (rm != null) {
      rm.stop();
    }
  }

  /**
   * On trunk this mutation was logged to the store first, and the refresh
   * then failed outside its rollback, leaving the scheduler and the provider
   * on the rejected configuration and the store mutation unconfirmed.
   */
  @Test
  public void testAllocationViolationRejectedBeforeStoreWrite()
      throws Exception {
    String key = YarnConfiguration.RM_SCHEDULER_MAXIMUM_ALLOCATION_MB;
    String etag = etag(ws);
    long version = provider.getConfigVersion();
    CapacitySchedulerConfiguration active = cs.getConfiguration();

    SchedConfUpdateInfo update = updateGlobal(key, "512");
    YarnRuntimeException e = assertThrows(YarnRuntimeException.class,
        () -> ws.updateSchedulerConfiguration(update, request()));
    assertTrue(e.getMessage().startsWith("Invalid resource scheduler memory"
        + " allocation configuration, yarn.scheduler.minimum-allocation-mb="
        + "1024, yarn.scheduler.maximum-allocation-mb=512"), e.getMessage());

    assertUnchanged(etag, version);
    assertNull(provider.getConfiguration().get(key));
    assertSame(active, cs.getConfiguration());

    assertOk(ws.updateSchedulerConfiguration(
        updateQueue("root.a", "maximum-capacity", "80"), request()));
    assertEquals(0.8f, cs.getQueue("a").getMaximumCapacity(), 1e-6);
  }

  @Test
  public void testQueueErrorRejectedBeforeStoreWrite() throws Exception {
    String etag = etag(ws);
    long version = provider.getConfigVersion();
    CapacitySchedulerConfiguration active = cs.getConfiguration();

    Response response = ws.updateSchedulerConfiguration(
        updateQueue("root.a", "capacity", "60"), request());
    assertEquals(Response.Status.BAD_REQUEST.getStatusCode(),
        response.getStatus());
    assertEquals("Failed to re-init queues : Illegal capacity sum of 1.1 for"
        + " children of queue root for label=. It should be either 0 or 1.0",
        response.getEntity());

    assertUnchanged(etag, version);
    assertEquals("50", provider.getConfiguration().get(
        PREFIX + "root.a.capacity"));
    assertSame(active, cs.getConfiguration());
    assertEquals(0.5f, cs.getQueue("a").getCapacity(), 1e-6);
  }

  @Test
  public void testWarningIsLoggedAndApplied() throws Exception {
    String key = PREFIX
        + "per-node-heartbeat.maximum-offswitch-assignments";
    LogCapturer logs = LogCapturer.captureLogs(
        LoggerFactory.getLogger(CapacityScheduler.class));
    try {
      assertOk(ws.updateSchedulerConfiguration(updateGlobal(key, "0"),
          request()));
      assertTrue(logs.getOutput().contains("Scheduler configuration warning:"
          + " WARNING offswitch-assignments-below-one"), logs.getOutput());
    } finally {
      logs.stopCapturing();
    }
    assertEquals("0", provider.getConfiguration().get(key));
  }

  @Test
  public void testIfMatchIsCheckedBeforeValidation() throws Exception {
    String etag = etag(ws);
    long version = provider.getConfigVersion();
    SchedConfUpdateInfo invalid = updateQueue("root.a", "capacity", "60");

    Response stale = ws.updateSchedulerConfiguration(invalid,
        request("\"stale\""));
    assertEquals(Response.Status.PRECONDITION_FAILED.getStatusCode(),
        stale.getStatus());

    Response rejected = ws.updateSchedulerConfiguration(invalid,
        request(etag));
    assertEquals(Response.Status.BAD_REQUEST.getStatusCode(),
        rejected.getStatus());
    assertUnchanged(etag, version);

    assertOk(ws.updateSchedulerConfiguration(
        updateQueue("root.a", "maximum-capacity", "80"), request(etag)));
    assertNotEquals(etag, etag(ws));
  }

  /**
   * A failed store write leaves nothing behind that blocks later reads or
   * writes.
   */
  @Test
  public void testFailedStoreWriteDoesNotBlockLaterRequests()
      throws Exception {
    String etag = etag(ws);
    assertTrue(storeDir.setWritable(false));
    Response failed = ws.updateSchedulerConfiguration(
        updateQueue("root.a", "maximum-capacity", "70"), request());
    assertEquals(Response.Status.BAD_REQUEST.getStatusCode(),
        failed.getStatus());
    assertEquals(etag, etag(ws));
    assertTrue(storeDir.setWritable(true));

    Response read = ws.getSchedulerConfiguration(request());
    assertEquals(Response.Status.OK.getStatusCode(), read.getStatus());
    assertEquals(Response.Status.OK.getStatusCode(),
        ws.getSchedulerConfigurationVersion(request()).getStatus());
    long version = provider.getConfigVersion();
    assertOk(ws.updateSchedulerConfiguration(
        updateQueue("root.a", "maximum-capacity", "70"), request()));
    assertEquals(version + 1, provider.getConfigVersion());
    assertEquals(0.7f, cs.getQueue("a").getMaximumCapacity(), 1e-6);
  }

  private void assertUnchanged(String etag, long version) throws Exception {
    assertEquals(etag, etag(ws));
    assertEquals(version, provider.getConfigVersion());
    assertEquals(0, pendingMutationFiles(storeDir));
  }

  private static void assertOk(Response response) {
    assertEquals(Response.Status.OK.getStatusCode(), response.getStatus(),
        String.valueOf(response.getEntity()));
  }
}
