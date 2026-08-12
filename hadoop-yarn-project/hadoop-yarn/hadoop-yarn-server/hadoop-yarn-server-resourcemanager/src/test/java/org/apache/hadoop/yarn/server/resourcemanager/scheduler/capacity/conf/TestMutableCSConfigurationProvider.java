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

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.AdminService;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueuePath;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.MutableConfigurationProvider;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.conf.model.CSConfigModel;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.ClusterFacts;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.ValidationResult;
import org.apache.hadoop.yarn.webapp.dao.QueueConfigInfo;
import org.apache.hadoop.yarn.webapp.dao.SchedConfUpdateInfo;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests {@link MutableCSConfigurationProvider}.
 */
public class TestMutableCSConfigurationProvider {

  private MutableCSConfigurationProvider confProvider;
  private RMContext rmContext;
  private SchedConfUpdateInfo goodUpdate;
  private SchedConfUpdateInfo badUpdate;
  private CapacityScheduler cs;
  private AdminService adminService;

  private static final UserGroupInformation TEST_USER = UserGroupInformation
      .createUserForTesting("testUser", new String[] {});

  @BeforeEach
  public void setUp() {
    cs = mock(CapacityScheduler.class);
    rmContext = mock(RMContext.class);
    when(rmContext.getScheduler()).thenReturn(cs);
    when(cs.getConfiguration()).thenReturn(
        new CapacitySchedulerConfiguration());
    adminService = mock(AdminService.class);
    when(rmContext.getRMAdminService()).thenReturn(adminService);
    confProvider = new MutableCSConfigurationProvider(rmContext);
    goodUpdate = new SchedConfUpdateInfo();
    Map<String, String> goodUpdateMap = new HashMap<>();
    goodUpdateMap.put("goodKey", "goodVal");
    QueueConfigInfo goodUpdateInfo = new
        QueueConfigInfo("root.a", goodUpdateMap);
    goodUpdate.getUpdateQueueInfo().add(goodUpdateInfo);

    badUpdate = new SchedConfUpdateInfo();
    badUpdate.getGlobalParams().put(
        YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB, "0");
  }

  @Test
  public void testInMemoryBackedProvider() throws Exception {
    Configuration conf = new Configuration();
    conf.set(YarnConfiguration.SCHEDULER_CONFIGURATION_STORE_CLASS,
        YarnConfiguration.MEMORY_CONFIGURATION_STORE);
    confProvider.init(conf);
    assertNull(confProvider.loadConfiguration(conf)
        .get("yarn.scheduler.capacity.root.a.goodKey"));

    ValidationResult result = confProvider.applyMutation(TEST_USER,
        goodUpdate);
    assertTrue(result.isValid(), result.getIssues().toString());
    assertEquals("goodVal", confProvider.loadConfiguration(conf)
        .get("yarn.scheduler.capacity.root.a.goodKey"));

    assertNull(confProvider.loadConfiguration(conf).get(
        "yarn.scheduler.capacity.root.a.badKey"));
    assertFalse(confProvider.applyMutation(TEST_USER, badUpdate).isValid());
    assertNull(confProvider.loadConfiguration(conf).get(
        "yarn.scheduler.capacity.root.a.badKey"));

    confProvider.formatConfigurationInStore(conf);
    assertNull(confProvider.loadConfiguration(conf)
        .get("yarn.scheduler.capacity.root.a.goodKey"));
  }

  @Test
  public void testMutationUsesPreValidatedActivationOnce() throws Exception {
    Configuration conf = new Configuration();
    conf.set(YarnConfiguration.SCHEDULER_CONFIGURATION_STORE_CLASS,
        YarnConfiguration.MEMORY_CONFIGURATION_STORE);
    confProvider.init(conf);

    ValidationResult result = confProvider.applyMutation(TEST_USER,
        goodUpdate);

    assertTrue(result.isValid(), result.getIssues().toString());
    verify(cs).reinitializePreValidated(any(CapacitySchedulerConfiguration.class),
        eq(rmContext), any(CSConfigModel.class), any(ClusterFacts.class));
    verify(cs, never()).reinitializeValidatedConfiguration(
        any(CapacitySchedulerConfiguration.class), eq(rmContext));
  }

  @Test
  public void testPreValidatedActivationRequiresMutationLock() {
    CapacityScheduler scheduler = spy(new CapacityScheduler());
    MutableConfigurationProvider provider = mock(MutableConfigurationProvider.class);
    when(scheduler.getMutableConfProvider()).thenReturn(provider);
    when(provider.isMutationLockHeld()).thenReturn(false);
    CapacitySchedulerConfiguration proposed =
        new CapacitySchedulerConfiguration(new Configuration(false), false);

    assertThrows(IllegalStateException.class, () ->
        scheduler.reinitializePreValidated(proposed, rmContext,
            proposed.getModel(), ClusterFacts.empty()));
  }

  @Test
  public void testRemoveQueueConfig() throws Exception {
    Configuration conf = new Configuration();
    conf.set(YarnConfiguration.SCHEDULER_CONFIGURATION_STORE_CLASS,
        YarnConfiguration.MEMORY_CONFIGURATION_STORE);
    confProvider.init(conf);

    SchedConfUpdateInfo updateInfo = new SchedConfUpdateInfo();
    Map<String, String> updateMap = new HashMap<>();
    updateMap.put("testkey1", "testval1");
    updateMap.put("testkey2", "testval2");
    QueueConfigInfo queueConfigInfo = new
        QueueConfigInfo("root.a", updateMap);
    updateInfo.getUpdateQueueInfo().add(queueConfigInfo);

    confProvider.applyMutation(TEST_USER, updateInfo);
    assertEquals("testval1", confProvider.loadConfiguration(conf)
        .get("yarn.scheduler.capacity.root.a.testkey1"));
    assertEquals("testval2", confProvider.loadConfiguration(conf)
        .get("yarn.scheduler.capacity.root.a.testkey2"));

    // Unset testkey1.
    updateInfo = new SchedConfUpdateInfo();
    updateMap.put("testkey1", "");
    queueConfigInfo = new QueueConfigInfo("root.a", updateMap);
    updateInfo.getUpdateQueueInfo().add(queueConfigInfo);

    confProvider.applyMutation(TEST_USER, updateInfo);
    assertNull(confProvider.loadConfiguration(conf)
        .get("yarn.scheduler.capacity.root.a.testkey1"),
        "Failed to remove config");
    assertEquals("testval2", confProvider.loadConfiguration(conf)
        .get("yarn.scheduler.capacity.root.a.testkey2"));
  }

  @Test
  public void testMultipleUpdatesNotLost() throws Exception {
    Configuration conf = new Configuration();
    conf.set(YarnConfiguration.SCHEDULER_CONFIGURATION_STORE_CLASS,
        YarnConfiguration.MEMORY_CONFIGURATION_STORE);
    confProvider.init(conf);

    SchedConfUpdateInfo updateInfo1 = new SchedConfUpdateInfo();
    Map<String, String> updateMap1 = new HashMap<>();
    updateMap1.put("key1", "val1");
    QueueConfigInfo queueConfigInfo1 = new
        QueueConfigInfo("root.a", updateMap1);
    updateInfo1.getUpdateQueueInfo().add(queueConfigInfo1);
    confProvider.applyMutation(TEST_USER, updateInfo1);

    SchedConfUpdateInfo updateInfo2 = new SchedConfUpdateInfo();
    Map<String, String> updateMap2 = new HashMap<>();
    updateMap2.put("key2", "val2");
    QueueConfigInfo queueConfigInfo2 = new
        QueueConfigInfo("root.a", updateMap2);
    updateInfo2.getUpdateQueueInfo().add(queueConfigInfo2);
    confProvider.applyMutation(TEST_USER, updateInfo2);

    assertEquals("val1", confProvider.loadConfiguration(conf)
        .get("yarn.scheduler.capacity.root.a.key1"));
    assertEquals("val2", confProvider.loadConfiguration(conf)
        .get("yarn.scheduler.capacity.root.a.key2"));
  }

  @Test
  public void testHDFSBackedProvider() throws Exception {
    File testSchedulerConfigurationDir = new File(
        TestMutableCSConfigurationProvider.class.getResource("").getPath()
            + TestMutableCSConfigurationProvider.class.getSimpleName());
    FileUtils.deleteDirectory(testSchedulerConfigurationDir);
    testSchedulerConfigurationDir.mkdirs();

    Configuration conf = new Configuration(false);
    conf.set(YarnConfiguration.SCHEDULER_CONFIGURATION_STORE_CLASS,
        YarnConfiguration.FS_CONFIGURATION_STORE);
    conf.set(YarnConfiguration.SCHEDULER_CONFIGURATION_FS_PATH,
        testSchedulerConfigurationDir.getAbsolutePath());
    conf.set("yarn.scheduler.capacity.root.queues", "a");
    conf.set("yarn.scheduler.capacity.root.a.capacity", "100");
    writeConf(conf, testSchedulerConfigurationDir.getAbsolutePath());

    confProvider.init(conf);
    assertNull(confProvider.loadConfiguration(conf)
        .get("yarn.scheduler.capacity.root.a.goodKey"));

    ValidationResult result = confProvider.applyMutation(TEST_USER,
        goodUpdate);
    assertTrue(result.isValid(), result.getIssues().toString());
    assertEquals("goodVal", confProvider.loadConfiguration(conf)
        .get("yarn.scheduler.capacity.root.a.goodKey"));

    assertNull(confProvider.loadConfiguration(conf).get(
        "yarn.scheduler.capacity.root.a.badKey"));
    assertFalse(confProvider.applyMutation(TEST_USER, badUpdate).isValid());
    assertNull(confProvider.loadConfiguration(conf).get(
        "yarn.scheduler.capacity.root.a.badKey"));

    confProvider.formatConfigurationInStore(conf);
    assertNull(confProvider.loadConfiguration(conf)
        .get("yarn.scheduler.capacity.root.a.goodKey"));

  }

  @Test
  public void testAddRemoveQueueWithSpacesInConfig() throws Exception {
    CapacitySchedulerConfiguration csConf =
        new CapacitySchedulerConfiguration();
    QueuePath root = new QueuePath(CapacitySchedulerConfiguration.ROOT);
    QueuePath a = root.createNewLeaf("a");
    QueuePath b = root.createNewLeaf("b");
    QueuePath c = root.createNewLeaf("c");

    csConf.setQueues(root, new String[] {" a   , b, c" });

    csConf.setCapacity(a, 0);
    csConf.setCapacity(b, 50);
    csConf.setCapacity(c, 50);

    confProvider = new MutableCSConfigurationProvider(rmContext) {
      @Override
      protected Configuration getInitSchedulerConfig() {
        return csConf;
      }
    };

    Configuration conf = new Configuration();
    conf.set(YarnConfiguration.SCHEDULER_CONFIGURATION_STORE_CLASS,
        YarnConfiguration.MEMORY_CONFIGURATION_STORE);
    confProvider.init(conf);

    SchedConfUpdateInfo update = new SchedConfUpdateInfo();
    update.getRemoveQueueInfo().add("root.a");

    confProvider.applyMutation(UserGroupInformation.getCurrentUser(), update);
  }

  private void writeConf(Configuration conf, String storePath)
      throws IOException {
    FileSystem fileSystem = FileSystem.get(new Configuration(conf));
    String schedulerConfigurationFile = YarnConfiguration.CS_CONFIGURATION_FILE
        + "." + System.currentTimeMillis();
    try (FSDataOutputStream outputStream = fileSystem.create(
        new Path(storePath, schedulerConfigurationFile))) {
      conf.writeXml(outputStream);
    }
  }
}
