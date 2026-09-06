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

import java.util.Set;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceInformation;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.conf.model.LegacyCapacityDerivations;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.CSConfigValidationEngine;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.ClusterFacts;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.CompileResult;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.apache.hadoop.yarn.util.resource.ResourceUtils;
import org.apache.hadoop.yarn.util.resource.DominantResourceCalculator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.junit.jupiter.params.provider.CsvSource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertFalse;

/** Preserve the REST metadata contract at the live resource adapter boundary. */
public class TestQueueAbsoluteResourceMetadata {
  private static final QueuePath ROOT = new QueuePath("root");
  private static final QueuePath A = new QueuePath("root.a");
  private static final QueuePath B = new QueuePath("root.b");

  @Test
  public void testMissingAbsoluteValuesRetainEmptyMetadata() throws Exception {
    try (QueuePlanTestFixture fixture = new QueuePlanTestFixture(
        QueuePlanTestFixture.configuration())) {
      assertEmptyResources((AbstractCSQueue) fixture.getScheduler().getQueue("root.a"));
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"0", "[memory=0,vcores=0]"})
  public void testExplicitZeroValuesRetainEmptyMetadata(String capacity) throws Exception {
    CapacitySchedulerConfiguration conf = QueuePlanTestFixture.configuration();
    conf.setCapacity(A, capacity);
    conf.setCapacity(B, capacity.startsWith("[") ? "[memory=1024,vcores=1]" : "100");
    conf.set(QueuePrefixes.getQueuePrefix(A)
        + CapacitySchedulerConfiguration.MAXIMUM_CAPACITY, capacity);
    try (QueuePlanTestFixture fixture = new QueuePlanTestFixture(conf)) {
      assertEmptyResources((AbstractCSQueue) fixture.getScheduler().getQueue("root.a"));
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"[memory=0,vcores=2]", "[memory=1024,vcores=2]"})
  public void testNonzeroVectorsRemainUnchanged(String capacity) throws Exception {
    CapacitySchedulerConfiguration conf = QueuePlanTestFixture.configuration();
    conf.setQueues(new QueuePath("root"), new String[]{"a"});
    conf.setCapacity(A, capacity);
    conf.setMaximumResourceRequirement("", A, Resource.newInstance(4096, 4));
    try (QueuePlanTestFixture fixture = new QueuePlanTestFixture(conf)) {
      AbstractCSQueue queue = (AbstractCSQueue) fixture.getScheduler().getQueue("root.a");
      Resource expected = LegacyCapacityDerivations.absoluteResource(
          conf.getModel().getNode(A).getCapacity(""), Set.of());
      Resource actual = queue.getMinimumAbsoluteResource("");
      for (ResourceInformation entry : expected.getResources()) {
        ResourceInformation result = actual.getResourceInformation(entry.getName());
        assertEquals(entry.getValue(), result.getValue());
        assertEquals(entry.getMinimumAllocation(), result.getMinimumAllocation());
        assertEquals(entry.getMaximumAllocation(), result.getMaximumAllocation());
      }
    }
  }

  @Test
  public void testRootConfiguredResourcesRemainIndependentOfCluster() throws Exception {
    CapacitySchedulerConfiguration conf = QueuePlanTestFixture.configuration();
    conf.setQueues(ROOT, new String[]{"a"});
    conf.unset(QueuePrefixes.getQueuePrefix(B) + "capacity");
    conf.set(QueuePrefixes.getQueuePrefix(ROOT) + "capacity", "[memory=32768,vcores=32]");
    conf.set(QueuePrefixes.getQueuePrefix(ROOT) + "maximum-capacity",
        "[memory=65536,vcores=64]");
    conf.setCapacity(A, "[memory=1024,vcores=1]");
    try (QueuePlanTestFixture fixture = new QueuePlanTestFixture(conf)) {
      AbstractCSQueue root = (AbstractCSQueue) fixture.getScheduler().getQueue("root");
      assertEquals(Resource.newInstance(32768, 32), root.getMinimumAbsoluteResource(""));
      assertEquals(Resource.newInstance(65536, 64), root.getMaximumAbsoluteResource(""));
      assertEquals(1.0f, root.getCapacity());
      fixture.getRM().registerNode("127.0.0.1:1234", 8192, 8);
      assertEquals(Resource.newInstance(32768, 32),
          root.getQueueResourceQuotas().getConfiguredMinResource(""));
      assertEquals(Resource.newInstance(8192, 8),
          root.getQueueResourceQuotas().getEffectiveMinResource(""));
      CapacitySchedulerConfiguration candidate = fixture.candidate();
      candidate.set(QueuePrefixes.getQueuePrefix(ROOT) + "capacity", "100");
      candidate.unset(QueuePrefixes.getQueuePrefix(ROOT)
          + CapacitySchedulerConfiguration.MAXIMUM_CAPACITY);
      CompileResult result = new CSConfigValidationEngine().compile(candidate.getModel(),
          ClusterFacts.capture(fixture.getScheduler()));
      assertTrue(result.getFallbackReasons().stream()
          .anyMatch(reason -> "root".equals(reason.queuePath())
              && (QueuePrefixes.getQueuePrefix(ROOT) + "capacity").equals(reason.propertyKey())),
          result.getFallbackReasons().toString());
    }
  }

  @Test
  public void testRootZeroMemoryVectorRetainsLegacyAbsence() throws Exception {
    CapacitySchedulerConfiguration conf = QueuePlanTestFixture.configuration();
    conf.set(QueuePrefixes.getQueuePrefix(ROOT) + "capacity", "[memory=0,vcores=2]");
    try (QueuePlanTestFixture fixture = new QueuePlanTestFixture(conf)) {
      assertEmptyResources((AbstractCSQueue) fixture.getScheduler().getQueue("root"));
    }
  }

  @ParameterizedTest
  @CsvSource(delimiter = '|', value = {
      "[memory=1024,vcores=2w]|1024|0",
      "[memory=1024bad,vcores=2]|0|0",
      "[memory=16777217,vcores=2]|16777217|2",
      "[memory=2Gi,vcores=2]|2048|2"})
  public void testRawRootQuotaRetainsLegacyParsing(String raw, long memory, int cores) {
    CapacitySchedulerConfiguration conf = QueuePlanTestFixture.configuration();
    conf.set(QueuePrefixes.getQueuePrefix(ROOT) + "capacity", raw);
    assertEquals(Resource.newInstance(memory, cores),
        LegacyCapacityDerivations.rootAbsoluteResource(
            conf.getModel().getNode(ROOT).getCapacity(""), Set.of("memory", "vcores")));
    assertEquals(QueueCapacityVector.of(100,
        QueueCapacityVector.ResourceUnitCapacityType.PERCENTAGE).toString(),
        conf.getModel().getNode(ROOT).getCapacity("").getVector().toString());
  }

  @ParameterizedTest
  @ValueSource(strings = {"", "GPU"})
  public void testRootQuotaBelowChildrenIsRejectedByLegacyValidation(String label) {
    CapacitySchedulerConfiguration conf = QueuePlanTestFixture.configuration();
    conf.setQueues(ROOT, new String[]{"a"});
    conf.unset(QueuePrefixes.getQueuePrefix(B) + "capacity");
    conf.set(QueuePrefixes.getNodeLabelPrefix(ROOT, label) + "capacity",
        "[memory=1024,vcores=1]");
    conf.setCapacity(A, "[memory=2048,vcores=2]");
    if (!label.isEmpty()) {
      conf.setAccessibleNodeLabels(ROOT, Set.of(label));
      conf.setAccessibleNodeLabels(A, Set.of(label));
      conf.setCapacityByLabel(A, label, "[memory=2048,vcores=2]");
    }
    CSConfigValidationEngine engine = new CSConfigValidationEngine();
    ClusterFacts facts = ClusterFacts.builder().withNodeLabels(
        label.isEmpty() ? Set.of() : Set.of(label)).build();
    assertTrue(engine.compile(conf.getModel(), facts).requiresLegacyValidation());
    var legacy = engine.validate(conf.getModel(), facts);
    assertFalse(legacy.isValid());
    assertTrue(legacy.getIssues().stream().anyMatch(issue ->
        issue.getMessage().contains("is less than")), legacy.getIssues().toString());
  }

  @Test
  public void testCustomOnlyNonzeroResourceIsNotNormalizedToEmpty() throws Exception {
    Map<String, ResourceInformation> original = new HashMap<>();
    ResourceUtils.getResourceTypes().forEach((name, info) ->
        original.put(name, ResourceInformation.newInstance(info)));
    CapacitySchedulerConfiguration conf = QueuePlanTestFixture.configuration();
    conf.set(YarnConfiguration.RESOURCE_TYPES, "custom");
    conf.setResourceComparator(DominantResourceCalculator.class);
    conf.setQueues(ROOT, new String[]{"a"});
    conf.setCapacity(A, "[memory=0,vcores=0,custom=2]");
    conf.set(QueuePrefixes.getQueuePrefix(A) + "maximum-capacity",
        "[memory=4096,vcores=4,custom=4]");
    try (QueuePlanTestFixture fixture = new QueuePlanTestFixture(conf)) {
      AbstractCSQueue queue = (AbstractCSQueue) fixture.getScheduler().getQueue("root.a");
      Resource actual = queue.getMinimumAbsoluteResource("");
      assertEquals(2, actual.getResourceValue("custom"));
      Resource expected = LegacyCapacityDerivations.absoluteResource(
          conf.getModel().getNode(A).getCapacity(""), Set.of("memory", "vcores"));
      assertEquals(expected.getResourceInformation("memory-mb").getMaximumAllocation(),
          actual.getResourceInformation("memory-mb").getMaximumAllocation());
    } finally {
      ResourceUtils.initializeResourcesFromResourceInformationMap(original);
    }
  }

  private void assertEmptyResources(AbstractCSQueue queue) {
    Resource minimum = queue.getMinimumAbsoluteResource("");
    Resource maximum = queue.getMaximumAbsoluteResource("");
    assertNotSame(minimum, maximum);
    assertEmptyMetadata(minimum);
    assertEmptyMetadata(maximum);

    for (ResourceInformation entry : minimum.getResources()) {
      entry.setValue(1);
      entry.setMinimumAllocation(7);
      entry.setMaximumAllocation(11);
    }
    assertEmptyMetadata(maximum);
    assertEmptyMetadata(queue.getMinimumAbsoluteResource(""));
    assertEmptyMetadata(queue.getMaximumAbsoluteResource(""));
    for (ResourceInformation entry : Resources.none().getResources()) {
      assertEquals(0, entry.getValue());
    }
  }

  private void assertEmptyMetadata(Resource resource) {
    assertNotSame(Resources.none(), resource);
    for (ResourceInformation entry : resource.getResources()) {
      ResourceInformation empty = Resources.none().getResourceInformation(entry.getName());
      assertNotSame(empty, entry);
      assertEquals(0, entry.getValue());
      assertEquals(empty.getMinimumAllocation(), entry.getMinimumAllocation());
      assertEquals(empty.getMaximumAllocation(), entry.getMaximumAllocation());
    }
  }
}
