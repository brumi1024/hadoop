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
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.nodelabels.CommonNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Pins compatibility behaviours that the Capacity Scheduler configuration
 * redesign must preserve unless a later change deliberately deprecates them.
 * The covered behaviours correspond to quirks 3, 4, 13, and 20 in the design
 * inventory, plus the legacy queue mode default.
 */
public class TestCapacitySchedulerConfigurationPinnedBehaviours {

  private static final QueuePath ROOT = new QueuePath("root");
  private static final QueuePath CHILD = new QueuePath("root.child");

  private CapacitySchedulerConfiguration createConfiguration() {
    return new CapacitySchedulerConfiguration(new Configuration(false), false);
  }

  @Test
  public void testNoLabelPrefixAliasing() {
    CapacitySchedulerConfiguration conf = createConfiguration();

    conf.setCapacity(CHILD, 25f);
    assertEquals(25f, conf.getLabeledQueueCapacity(
        CHILD, CommonNodeLabelsManager.NO_LABEL));

    conf.setCapacityByLabel(CHILD, CommonNodeLabelsManager.NO_LABEL, 35f);
    assertEquals(35f, conf.getNonLabeledQueueCapacity(CHILD));
  }

  @Test
  public void testRootAccessibleLabelsOverride() {
    CapacitySchedulerConfiguration conf = createConfiguration();
    conf.setAccessibleNodeLabels(ROOT, Set.of("red"));

    assertEquals(Set.of(RMNodeLabelsManager.ANY),
        conf.getAccessibleNodeLabels(ROOT));
  }

  @Test
  public void testStarCollapsesLabelSet() {
    CapacitySchedulerConfiguration conf = createConfiguration();
    conf.setAccessibleNodeLabels(CHILD,
        Set.of("red", RMNodeLabelsManager.ANY));

    assertEquals(Set.of(RMNodeLabelsManager.ANY),
        conf.getAccessibleNodeLabels(CHILD));
  }

  @Test
  public void testUnsetAccessibleLabelsInheritsOnNonRoot() {
    assertNull(createConfiguration().getAccessibleNodeLabels(CHILD));
  }

  @Test
  public void testLegacyQueueModeDefaultsTrue() {
    assertTrue(createConfiguration().isLegacyQueueMode());
  }

  @Test
  public void testSetCapacityOnRootThrows() {
    CapacitySchedulerConfiguration conf = createConfiguration();

    assertThrows(IllegalArgumentException.class,
        () -> conf.setCapacity(ROOT, 50f));
  }

  @Test
  public void testRootCapacityPinnedAt100DespiteConfiguredValue() {
    CapacitySchedulerConfiguration conf = createConfiguration();
    conf.set(QueuePrefixes.getQueuePrefix(ROOT)
        + CapacitySchedulerConfiguration.CAPACITY, "42");

    assertEquals(100f, conf.getNonLabeledQueueCapacity(ROOT));
  }

  @Test
  public void testRootConfiguredNodeLabelsRemainRootLocal() {
    CapacitySchedulerConfiguration conf = createConfiguration();
    conf.setQueues(ROOT, new String[] {"child"});
    conf.setCapacityByLabel(CHILD, "blue", 100f);

    assertEquals(Set.of(RMNodeLabelsManager.NO_LABEL),
        conf.getConfiguredNodeLabels(ROOT));
  }

  @ParameterizedTest
  @ValueSource(strings = {
      "6w",
      "[memory=1024,vcores=1]",
      "[memory=50%,vcores=2w]"
  })
  public void testPercentageGettersReturnPlaceholdersForNonPercentageFormats(
      String configuredCapacity) {
    CapacitySchedulerConfiguration conf = createConfiguration();
    conf.set(QueuePrefixes.getQueuePrefix(CHILD)
        + CapacitySchedulerConfiguration.CAPACITY, configuredCapacity);
    conf.set(QueuePrefixes.getQueuePrefix(ROOT)
        + CapacitySchedulerConfiguration.CAPACITY, configuredCapacity);

    assertEquals(0f, conf.getNonLabeledQueueCapacity(CHILD));
    assertEquals(100f, conf.getNonLabeledQueueMaximumCapacity(CHILD));
    assertEquals(100f, conf.getNonLabeledQueueCapacity(ROOT));
    assertEquals(100f, conf.getNonLabeledQueueMaximumCapacity(ROOT));
  }

  @Test
  public void testUnknownMappingRuleFormatThrows() {
    CapacitySchedulerConfiguration conf = createConfiguration();
    conf.setMappingRuleFormat("bogus");

    assertThrows(IllegalArgumentException.class, conf::getMappingRules);
  }

  @Test
  public void testUnsetMappingRuleFormatParsesLegacy() throws IOException {
    CapacitySchedulerConfiguration conf = createConfiguration();
    conf.set(CapacitySchedulerConfiguration.QUEUE_MAPPING,
        "u:alice:root.child");

    assertEquals(1, conf.getMappingRules().size());
  }

  @ParameterizedTest
  @ValueSource(strings = {"junk", ""})
  public void testInvalidLegacyModeBooleanReturnsDefault(String value) {
    CapacitySchedulerConfiguration conf = createConfiguration();
    conf.set(CapacitySchedulerConfiguration.PREFIX
        + "legacy-queue-mode.enabled", value);

    assertTrue(conf.isLegacyQueueMode());
  }

  @ParameterizedTest
  @ValueSource(strings = {"junk", ""})
  public void testInvalidAutoCreateBooleanReturnsDefault(String value) {
    CapacitySchedulerConfiguration conf = createConfiguration();
    conf.setAutoCreateChildQueueEnabled(CHILD, false);
    conf.set(QueuePrefixes.getQueuePrefix(CHILD)
        + CapacitySchedulerConfiguration.AUTO_CREATE_CHILD_QUEUE_ENABLED,
        value);

    assertEquals(false, conf.isAutoCreateChildQueueEnabled(CHILD));
  }

  @ParameterizedTest
  @CsvSource({
      "'[memory=2048,vcores=50%]', false",
      "'[memory=2048,vcores=2w]', true",
      "'[foo]', true",
      "'[memory=2048,vcores=2]', true",
      "'memory=2048', false",
      "'[memory=2048,vcores=2w', false"
  })
  public void testAbsoluteResourceClassificationUsesLegacyRawRegex(
      String configuredCapacity, boolean expected) {
    CapacitySchedulerConfiguration conf = createConfiguration();
    conf.set(QueuePrefixes.getQueuePrefix(CHILD)
        + CapacitySchedulerConfiguration.CAPACITY, configuredCapacity);

    assertEquals(expected, conf.checkConfigTypeIsAbsoluteResource("", CHILD));
  }

  @Test
  public void testLabeledAbsoluteResourceClassificationUsesLegacyRawRegex() {
    CapacitySchedulerConfiguration conf = createConfiguration();
    conf.setCapacityByLabel(CHILD, "gpu", "[memory=2048,vcores=2w]");

    assertTrue(conf.checkConfigTypeIsAbsoluteResource("gpu", CHILD));
  }
}
