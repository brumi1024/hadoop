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
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.MutableConfigurationProvider;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueuePath;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.ClusterFacts;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.ValidationResult;
import org.apache.hadoop.yarn.server.records.Version;
import org.apache.hadoop.yarn.webapp.dao.QueueConfigInfo;
import org.apache.hadoop.yarn.webapp.dao.SchedConfUpdateInfo;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
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
    when(cs.captureClusterFacts()).thenReturn(ClusterFacts.empty());
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
        goodUpdate).getValidationResult();
    assertTrue(result.isValid(), result.getIssues().toString());
    assertEquals("goodVal", confProvider.loadConfiguration(conf)
        .get("yarn.scheduler.capacity.root.a.goodKey"));

    assertNull(confProvider.loadConfiguration(conf).get(
        "yarn.scheduler.capacity.root.a.badKey"));
    assertFalse(confProvider.applyMutation(TEST_USER, badUpdate)
        .getValidationResult().isValid());
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
        goodUpdate).getValidationResult();

    assertTrue(result.isValid(), result.getIssues().toString());
    verify(cs).reinitializePreValidated(
        any(CapacitySchedulerConfiguration.class), eq(rmContext),
        any(ClusterFacts.class));
  }

  @Test
  public void testVersionedMutationRejectsStaleVersionBeforeActivation()
      throws Exception {
    Configuration conf = new Configuration();
    conf.set(YarnConfiguration.SCHEDULER_CONFIGURATION_STORE_CLASS,
        YarnConfiguration.MEMORY_CONFIGURATION_STORE);
    confProvider.init(conf);
    long currentVersion = confProvider.getConfigVersion();

    MutableConfigurationProvider.VersionMismatchException failure =
        assertThrows(MutableConfigurationProvider.VersionMismatchException.class,
            () -> confProvider.applyMutation(TEST_USER, goodUpdate,
                currentVersion - 1));

    assertEquals(currentVersion - 1, failure.getExpectedVersion());
    assertEquals(currentVersion, failure.getActualVersion());
    verify(cs, never()).reinitializePreValidated(
        any(CapacitySchedulerConfiguration.class), eq(rmContext),
        any(ClusterFacts.class));
    assertNull(confProvider.getConfiguration()
        .get("yarn.scheduler.capacity.root.a.goodKey"));
  }

  @Test
  public void testVersionedMutationReturnsItsCommittedVersion()
      throws Exception {
    Configuration conf = new Configuration();
    conf.set(YarnConfiguration.SCHEDULER_CONFIGURATION_STORE_CLASS,
        YarnConfiguration.MEMORY_CONFIGURATION_STORE);
    confProvider.init(conf);
    long currentVersion = confProvider.getConfigVersion();

    MutableConfigurationProvider.MutationResult result =
        confProvider.applyMutation(TEST_USER, goodUpdate, currentVersion);

    assertTrue(result.getValidationResult().isValid());
    assertEquals(currentVersion + 1, result.getConfigVersion());
  }

  @Test
  public void testMutationReconcilesAfterAmbiguousStoreFailure()
      throws Exception {
    Configuration conf = new Configuration();
    conf.setClass(YarnConfiguration.SCHEDULER_CONFIGURATION_STORE_CLASS,
        ApplyThenFailStore.class, YarnConfigurationStore.class);
    confProvider.init(conf);

    RuntimeException failure = assertThrows(RuntimeException.class,
        () -> confProvider.applyMutation(TEST_USER, goodUpdate));

    assertEquals(ApplyThenFailStore.FAILURE_MESSAGE, failure.getMessage());
    verify(cs, times(2)).reinitializePreValidated(
        any(CapacitySchedulerConfiguration.class), eq(rmContext),
        any(ClusterFacts.class));
    assertEquals("goodVal", confProvider.getConfiguration()
        .get("yarn.scheduler.capacity.root.a.goodKey"));
    assertEquals("goodVal", confProvider.getConfStore().retrieve()
        .get("yarn.scheduler.capacity.root.a.goodKey"));
  }

  @Test
  public void testMutationAbortsAndReconcilesAfterAmbiguousLogFailure()
      throws Exception {
    Configuration conf = new Configuration();
    conf.setClass(YarnConfiguration.SCHEDULER_CONFIGURATION_STORE_CLASS,
        LogThenFailStore.class, YarnConfigurationStore.class);
    confProvider.init(conf);

    RuntimeException failure = assertThrows(RuntimeException.class,
        () -> confProvider.applyMutation(TEST_USER, goodUpdate));

    assertEquals(LogThenFailStore.FAILURE_MESSAGE, failure.getMessage());
    LogThenFailStore store = (LogThenFailStore) confProvider.getConfStore();
    assertTrue(store.isAborted());
    assertNull(confProvider.getConfiguration()
        .get("yarn.scheduler.capacity.root.a.goodKey"));
    assertNull(store.retrieve()
        .get("yarn.scheduler.capacity.root.a.goodKey"));
    verify(cs).reinitializePreValidated(
        any(CapacitySchedulerConfiguration.class), eq(rmContext),
        any(ClusterFacts.class));
  }

  @Test
  public void testMutationAbortsStoreAndReconcilesAfterActivationFailure()
      throws Exception {
    Configuration conf = new Configuration();
    conf.setClass(YarnConfiguration.SCHEDULER_CONFIGURATION_STORE_CLASS,
        RecordingStore.class, YarnConfigurationStore.class);
    confProvider.init(conf);
    doThrow(new IOException("injected activation failure"))
        .doNothing()
        .when(cs).reinitializePreValidated(
            any(CapacitySchedulerConfiguration.class), eq(rmContext),
            any(ClusterFacts.class));

    IOException failure = assertThrows(IOException.class,
        () -> confProvider.applyMutation(TEST_USER, goodUpdate));

    assertEquals("injected activation failure", failure.getMessage());
    RecordingStore store = (RecordingStore) confProvider.getConfStore();
    assertEquals(Collections.singletonList(false), store.getConfirmations());
    assertNull(confProvider.getConfiguration()
        .get("yarn.scheduler.capacity.root.a.goodKey"));
    assertNull(store.retrieve()
        .get("yarn.scheduler.capacity.root.a.goodKey"));
    verify(cs, times(2)).reinitializePreValidated(
        any(CapacitySchedulerConfiguration.class), eq(rmContext),
        any(ClusterFacts.class));
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
        goodUpdate).getValidationResult();
    assertTrue(result.isValid(), result.getIssues().toString());
    assertEquals("goodVal", confProvider.loadConfiguration(conf)
        .get("yarn.scheduler.capacity.root.a.goodKey"));

    assertNull(confProvider.loadConfiguration(conf).get(
        "yarn.scheduler.capacity.root.a.badKey"));
    assertFalse(confProvider.applyMutation(TEST_USER, badUpdate)
        .getValidationResult().isValid());
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

  /**
   * Store that makes a successful durable update look like a failed confirm.
   */
  public static final class ApplyThenFailStore extends YarnConfigurationStore {
    private static final String FAILURE_MESSAGE =
        "injected failure after durable update";

    private final InMemoryConfigurationStore delegate =
        new InMemoryConfigurationStore();

    @Override
    public void initialize(Configuration conf, Configuration schedConf,
        RMContext context) {
      delegate.initialize(conf, schedConf, context);
    }

    @Override
    public void close() throws IOException {
      delegate.close();
    }

    @Override
    public void logMutation(LogMutation logMutation) {
      delegate.logMutation(logMutation);
    }

    @Override
    public void confirmMutation(LogMutation pendingMutation, boolean isValid) {
      delegate.confirmMutation(pendingMutation, isValid);
      if (isValid) {
        throw new RuntimeException(FAILURE_MESSAGE);
      }
    }

    @Override
    public Configuration retrieve() {
      return delegate.retrieve();
    }

    @Override
    public void format() {
      delegate.format();
    }

    @Override
    public long getConfigVersion() {
      return delegate.getConfigVersion();
    }

    @Override
    public List<LogMutation> getConfirmedConfHistory(long fromId) {
      return delegate.getConfirmedConfHistory(fromId);
    }

    @Override
    protected Version getConfStoreVersion() throws Exception {
      return delegate.getConfStoreVersion();
    }

    @Override
    protected LinkedList<LogMutation> getLogs() throws Exception {
      return delegate.getLogs();
    }

    @Override
    protected void storeVersion() throws Exception {
      delegate.storeVersion();
    }

    @Override
    protected Version getCurrentVersion() {
      return delegate.getCurrentVersion();
    }
  }

  /** Store that persists a pending log entry before reporting failure. */
  public static final class LogThenFailStore
      extends InMemoryConfigurationStore {
    private static final String FAILURE_MESSAGE =
        "injected failure after logging mutation";
    private boolean aborted;

    @Override
    public void logMutation(LogMutation logMutation) {
      super.logMutation(logMutation);
      throw new RuntimeException(FAILURE_MESSAGE);
    }

    @Override
    public void confirmMutation(LogMutation pendingMutation, boolean isValid) {
      aborted = !isValid;
      super.confirmMutation(pendingMutation, isValid);
    }

    boolean isAborted() {
      return aborted;
    }
  }

  /** Store that records commit and abort decisions. */
  public static final class RecordingStore extends InMemoryConfigurationStore {
    private final List<Boolean> confirmations = new ArrayList<>();

    @Override
    public void confirmMutation(LogMutation pendingMutation, boolean isValid) {
      confirmations.add(isValid);
      super.confirmMutation(pendingMutation, isValid);
    }

    List<Boolean> getConfirmations() {
      return confirmations;
    }
  }
}
