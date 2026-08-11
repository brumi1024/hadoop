/*
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
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.AbstractParentQueue.QueueCapacityType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueCapacityVector.ResourceUnitCapacityType;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Pins legacy queue-mode classification inputs before consumer migration. */
public class TestLegacyQueueModeClassificationParity {
  private static final QueuePath ROOT = new QueuePath("root");
  private static final QueuePath A = new QueuePath("root.a");

  @Test
  public void testUniformPercentageVector() {
    CapacitySchedulerConfiguration conf = configuration("50");

    assertEquals(Set.of(ResourceUnitCapacityType.PERCENTAGE),
        conf.getModel().getNode(A).getCapacity("")
            .getVector().getDefinedCapacityTypes());
  }

  @Test
  public void testUniformWeightVector() {
    CapacitySchedulerConfiguration conf = configuration("2w");

    assertEquals(Set.of(ResourceUnitCapacityType.WEIGHT),
        conf.getModel().getNode(A).getCapacity("")
            .getVector().getDefinedCapacityTypes());
  }

  @Test
  public void testUniformAbsoluteVector() {
    CapacitySchedulerConfiguration conf = configuration(
        "[memory=1024,vcores=1]");

    assertEquals(Set.of(ResourceUnitCapacityType.ABSOLUTE),
        conf.getModel().getNode(A).getCapacity("")
            .getVector().getDefinedCapacityTypes());
  }

  @Test
  public void testMixedVectorRetainsBothTypes() {
    CapacitySchedulerConfiguration conf = configuration(
        "[memory=50%,vcores=2w]");

    assertEquals(Set.of(ResourceUnitCapacityType.PERCENTAGE,
            ResourceUnitCapacityType.WEIGHT),
        conf.getModel().getNode(A).getCapacity("")
            .getVector().getDefinedCapacityTypes());
  }

  @Test
  public void testLabeledCapacityUsesItsOwnVector() {
    CapacitySchedulerConfiguration conf = configuration("100");
    conf.setCapacityVector(A, "gpu", "[memory=1024,vcores=1]");

    assertTrue(conf.getModel().getNode(A).getCapacity("gpu")
        .getVector().getDefinedCapacityTypes()
        .contains(ResourceUnitCapacityType.ABSOLUTE));
  }

  @Test
  public void testTemplateEffectiveConfigPreservesRawWeight() {
    CapacitySchedulerConfiguration conf = configuration("100");
    conf.set("yarn.scheduler.capacity.root.a.auto-queue-creation-v2."
        + "leaf-template.capacity", "2w");

    assertEquals("2w", conf.getModel()
        .effectiveConfigFor(new QueuePath("root.a.dynamic"), true)
        .getCapacity("").getRawValue());
  }

  @Test
  public void testMockRmUniformPercentageWeightAndAbsoluteTrees() throws Exception {
    assertEquals(QueueCapacityType.PERCENT,
        classify(rootChildren(50, 50), ROOT));

    CapacitySchedulerConfiguration weights = baseConfiguration(false);
    weights.setNonLabeledQueueWeight(A, 1);
    weights.setNonLabeledQueueWeight(new QueuePath("root.b"), 1);
    assertEquals(QueueCapacityType.WEIGHT, classify(weights, ROOT));

    CapacitySchedulerConfiguration absolute = baseConfiguration(false);
    absolute.setCapacity(A, "[memory=1024,vcores=1]");
    absolute.setCapacity(new QueuePath("root.b"), "[memory=1024,vcores=1]");
    absolute.setCapacityByLabel(A, "gpu", "[memory=512,vcores=1]");
    absolute.setCapacityByLabel(new QueuePath("root.b"), "gpu",
        "[memory=512,vcores=1]");
    assertEquals(QueueCapacityType.ABSOLUTE_RESOURCE,
        classify(absolute, ROOT));
  }

  @Test
  public void testMixedPercentageAndWeightPinsExactMessage() {
    CapacitySchedulerConfiguration conf = baseConfiguration(false);
    conf.setCapacity(A, 50);
    conf.setNonLabeledQueueWeight(new QueuePath("root.b"), 1);

    IOException error = assertThrows(IOException.class,
        () -> classify(conf, ROOT));
    assertEquals("Parent queue 'root' have children queue used mixed of  weight "
        + "mode, percentage and absolute mode, it is not allowed, please "
        + "double check, details:{Queue=root.a, label= uses percentage mode}. "
        + "{Queue=root.b, label= uses weight mode}. "
        + "{Queue=root.b, label= uses percentage mode}. ", error.getMessage());
  }

  @Test
  public void testMixedAbsoluteAndPercentageDependsOnDeclarationOrder() throws Exception {
    CapacitySchedulerConfiguration absoluteFirst = baseConfiguration(false);
    absoluteFirst.setQueues(ROOT, new String[] {"a", "b"});
    absoluteFirst.setCapacity(A, "[memory=1024,vcores=1]");
    absoluteFirst.setCapacity(new QueuePath("root.b"), 50);
    assertThrows(IOException.class, () -> classify(absoluteFirst, ROOT));

    CapacitySchedulerConfiguration percentageFirst = baseConfiguration(false);
    percentageFirst.setQueues(ROOT, new String[] {"b", "a"});
    percentageFirst.setCapacity(new QueuePath("root.b"), 50);
    percentageFirst.setCapacity(A, "[memory=1024,vcores=1]");
    assertEquals(QueueCapacityType.ABSOLUTE_RESOURCE,
        classify(percentageFirst, ROOT));
  }

  @Test
  public void testAbsoluteParentAndPercentageChildrenMessage() throws Exception {
    CapacitySchedulerConfiguration valid = rootChildren(50, 50);
    try (MockRM rm = start(valid)) {
      CapacitySchedulerConfiguration proposed = baseConfiguration(true);
      QueuePath parent = A;
      proposed.setQueues(ROOT, new String[] {"a"});
      proposed.setQueues(parent, new String[] {"a1", "a2"});
      proposed.setCapacity(parent, "[memory=2048,vcores=1]");
      proposed.setCapacity(parent.createNewLeaf("a1"), 50);
      proposed.setCapacity(parent.createNewLeaf("a2"), 50);

      IOException error = assertThrows(IOException.class,
          () -> ((CapacityScheduler) rm.getResourceScheduler())
              .reinitialize(proposed, rm.getRMContext()));
      assertTrue(error.getMessage().contains(
          "When absolute minResource is used, we must make sure both parent "
              + "and child all use absolute minResource"));
    }
  }

  @Test
  public void testZeroSumPercentageUnderWeightParent() throws Exception {
    CapacitySchedulerConfiguration conf = baseConfiguration(false);
    QueuePath parent = A;
    conf.setQueues(ROOT, new String[] {"a"});
    conf.setQueues(parent, new String[] {"a1", "a2"});
    conf.setNonLabeledQueueWeight(parent, 1);
    conf.setCapacity(parent.createNewLeaf("a1"), 0);
    conf.setCapacity(parent.createNewLeaf("a2"), 0);
    conf.setAllowZeroCapacitySum(parent, true);
    assertEquals(QueueCapacityType.PERCENT,
        classify(conf, parent));
  }

  @Test
  public void testPerLabelMixingIsRejected() {
    CapacitySchedulerConfiguration conf = baseConfiguration(false);
    conf.setCapacityByLabel(A, "gpu", 50);
    conf.setLabeledQueueWeight(new QueuePath("root.b"), "gpu", 1);
    IOException error = assertThrows(IOException.class,
        () -> classify(conf, ROOT));
    assertTrue(error.getMessage().contains("label=gpu"));
  }

  @Test
  public void testAqcV1LeafVector() throws Exception {
    CapacitySchedulerConfiguration conf = baseConfiguration(false);
    conf.setQueues(ROOT, new String[] {"a"});
    conf.setCapacity(A, "1w");
    conf.setAutoCreateChildQueueEnabled(A, true);
    conf.setAutoCreatedLeafQueueConfigCapacity(A, 50);
    conf.setAutoCreatedLeafQueueConfigMaxCapacity(A, 100);
    try (MockRM rm = start(conf)) {
      CapacityScheduler cs = (CapacityScheduler) rm.getResourceScheduler();
      cs.getCapacitySchedulerQueueManager().createQueue(
          new QueuePath("root.a.dynamic"));
      assertEquals(QueueCapacityType.PERCENT,
          ((AbstractParentQueue) cs.getQueue(A.getFullPath()))
              .getCapacityConfigurationTypeForQueues(
                  ((AbstractParentQueue) cs.getQueue(A.getFullPath()))
                      .getChildQueues()));
    }
  }

  @Test
  public void testV2DynamicLeafUnderWeightParent() throws Exception {
    CapacitySchedulerConfiguration v2 = baseConfiguration(false);
    v2.setQueues(ROOT, new String[] {"a"});
    v2.setCapacity(A, "1w");
    v2.setAutoQueueCreationV2Enabled(A, true);
    v2.set(CapacitySchedulerConfiguration.PREFIX + A.getFullPath()
        + ".auto-queue-creation-v2.leaf-template.capacity", "50%");
    try (MockRM rm = start(v2)) {
      CapacityScheduler cs = (CapacityScheduler) rm.getResourceScheduler();
      AbstractParentQueue parent = (AbstractParentQueue) cs.getQueue(
          A.getFullPath());
      CSQueue dynamic = cs.getCapacitySchedulerQueueManager().createQueue(
          new QueuePath("root.a.dynamic"));

      assertTrue(dynamic.isDynamicQueue(),
          "createQueue must return a runtime dynamic queue");
      assertEquals(1.0f, dynamic.getQueueCapacities().getWeight(""));
      assertEquals(QueueCapacityType.WEIGHT,
          parent.getCapacityConfigurationTypeForQueues(
              parent.getChildQueues()));
    }
  }

  @Test
  public void testV2PercentageTemplateIsIgnoredByV1ManagedParent()
      throws Exception {
    CapacitySchedulerConfiguration conf = baseConfiguration(true);
    conf.setQueues(ROOT, new String[] {"a"});
    conf.setCapacity(A, 100);
    conf.setAutoCreateChildQueueEnabled(A, true);
    conf.set(CapacitySchedulerConfiguration.PREFIX + A.getFullPath()
        + ".auto-queue-creation-v2.leaf-template.capacity", "50%");
    try (MockRM rm = start(conf)) {
      CapacityScheduler cs = (CapacityScheduler) rm.getResourceScheduler();
      AbstractParentQueue parent = (AbstractParentQueue) cs.getQueue(
          A.getFullPath());
      CSQueue dynamic = cs.getCapacitySchedulerQueueManager().createQueue(
          new QueuePath("root.a.dynamic"));

      assertEquals(0.0f, dynamic.getQueueCapacities().getCapacity(""));
      assertEquals(QueueCapacityType.PERCENT,
          parent.getCapacityConfigurationTypeForQueues(
              parent.getChildQueues()));
    }
  }

  @Test
  public void testEdgeRawValuesAndRootExemption() throws Exception {
    CapacitySchedulerConfiguration zeroWeight = baseConfiguration(false);
    zeroWeight.setNonLabeledQueueWeight(A, 0);
    zeroWeight.setNonLabeledQueueWeight(new QueuePath("root.b"), 0);
    assertEquals(QueueCapacityType.WEIGHT, classify(zeroWeight, ROOT));

    CapacitySchedulerConfiguration zeroCapacity = baseConfiguration(false);
    zeroCapacity.setCapacity(A, 0);
    zeroCapacity.setCapacity(new QueuePath("root.b"), 0);
    assertEquals(QueueCapacityType.PERCENT, classify(zeroCapacity, ROOT));

    CapacitySchedulerConfiguration unset = baseConfiguration(false);
    assertEquals(QueueCapacityType.PERCENT, classify(unset, ROOT));

    CapacitySchedulerConfiguration rootOnly = new CapacitySchedulerConfiguration(
        new Configuration(false), false);
    rootOnly.setClass(YarnConfiguration.RM_SCHEDULER, CapacityScheduler.class,
        ResourceScheduler.class);
    try (MockRM rm = start(rootOnly)) {
      CapacityScheduler cs = (CapacityScheduler) rm.getResourceScheduler();
      assertEquals(QueueCapacityType.WEIGHT,
          ((AbstractParentQueue) cs.getRootQueue())
              .getCapacityConfigurationTypeForQueues(Set.of()));
    }
  }

  /** This is the known round-1 absolute-plus-weight probe flip. */
  @Test
  public void testMixedAbsoluteAndWeightStaticQueuePin() {
    CapacitySchedulerConfiguration conf = baseConfiguration(false);
    conf.setCapacity(A, "[memory=2048,vcores=2w]");
    conf.setNonLabeledQueueWeight(new QueuePath("root.b"), 1);
    IOException error = assertThrows(IOException.class,
        () -> classify(conf, ROOT));
    assertTrue(error.getMessage().contains("mixed of  weight mode"));
  }

  private CapacitySchedulerConfiguration rootChildren(float a, float b) {
    CapacitySchedulerConfiguration conf = baseConfiguration(false);
    conf.setCapacity(A, a);
    conf.setCapacity(new QueuePath("root.b"), b);
    return conf;
  }

  private CapacitySchedulerConfiguration baseConfiguration(boolean legacy) {
    CapacitySchedulerConfiguration conf = new CapacitySchedulerConfiguration(
        new Configuration(false), false);
    conf.setClass(YarnConfiguration.RM_SCHEDULER, CapacityScheduler.class,
        ResourceScheduler.class);
    conf.setLegacyQueueModeEnabled(legacy);
    conf.setQueues(ROOT, new String[] {"a", "b"});
    return conf;
  }

  private QueueCapacityType classify(CapacitySchedulerConfiguration conf,
      QueuePath parentPath) throws Exception {
    try (MockRM rm = start(conf)) {
      CapacityScheduler cs = (CapacityScheduler) rm.getResourceScheduler();
      AbstractParentQueue parent = (AbstractParentQueue) cs.getQueue(
          parentPath.getFullPath());
      return parent.getCapacityConfigurationTypeForQueues(parent.getChildQueues());
    }
  }

  private MockRM start(CapacitySchedulerConfiguration conf) {
    MockRM rm = new MockRM(conf);
    rm.start();
    return rm;
  }

  private CapacitySchedulerConfiguration configuration(String capacity) {
    CapacitySchedulerConfiguration conf =
        new CapacitySchedulerConfiguration(new Configuration(false), false);
    conf.setQueues(ROOT, new String[]{"a"});
    conf.setCapacity(A, capacity);
    return conf;
  }
}
