/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration.AUTO_CREATE_CHILD_QUEUE_AUTO_REMOVAL_ENABLE;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.conf.model.LegacyCapacityDerivations.weight;

public class TestAutoCreatedQueueTemplate {
  private static final QueuePath TEST_QUEUE_ABC = new QueuePath("root.a.b.c");
  private static final QueuePath TEST_QUEUE_AB = new QueuePath("root.a.b");
  private static final QueuePath TEST_QUEUE_A = new QueuePath("root.a");
  private static final QueuePath TEST_QUEUE_B = new QueuePath("root.b");
  private static final QueuePath TEST_QUEUE_WILDCARD = new QueuePath("*");
  private static final QueuePath TEST_QUEUE_ROOT_WILDCARD = new QueuePath("root.*");
  private static final QueuePath TEST_QUEUE_TWO_LEVEL_WILDCARDS = new QueuePath("root.*.*");
  private static final QueuePath TEST_QUEUE_A_WILDCARD = new QueuePath("root.a.*");
  private static final QueuePath ROOT = new QueuePath("root");

  private CapacitySchedulerConfiguration conf;

  @BeforeEach
  public void setUp() throws Exception {
    conf = new CapacitySchedulerConfiguration();
    conf.setQueues(ROOT, new String[]{"a"});
    conf.setQueues(TEST_QUEUE_A, new String[]{"b"});
    conf.setQueues(TEST_QUEUE_B, new String[]{"c"});

  }

  @Test
  public void testNonWildCardTemplate() {
    conf.set(getTemplateKey(TEST_QUEUE_AB, "capacity"), "6w");
    assertEquals(6f, getWeight(TEST_QUEUE_ABC, false),
        10e-6, "weight is not set");

  }

  @Test
  public void testOneLevelWildcardTemplate() {
    conf.set(getTemplateKey(TEST_QUEUE_A_WILDCARD, "capacity"), "6w");
    assertEquals(6f, getWeight(TEST_QUEUE_ABC, false), 10e-6,
        "weight is not set");

  }

  @Test
  public void testTwoLevelWildcardTemplate() {
    conf.set(getTemplateKey(TEST_QUEUE_ROOT_WILDCARD, "capacity"), "6w");
    conf.set(getTemplateKey(TEST_QUEUE_TWO_LEVEL_WILDCARDS, "capacity"), "5w");

    assertEquals(6f, getWeight(TEST_QUEUE_AB, false), 10e-6,
        "weight is not set");
    assertEquals(5f, getWeight(TEST_QUEUE_ABC, false), 10e-6,
        "weight is not set");
  }

  @Test
  public void testIgnoredWhenRootWildcarded() {
    conf.set(getTemplateKey(TEST_QUEUE_WILDCARD, "capacity"), "6w");
    assertEquals(-1f, getWeight(TEST_QUEUE_A, false), 10e-6,
        "weight is set");
  }

  @Test
  public void testIgnoredWhenNoParent() {
    conf.set(getTemplateKey(ROOT, "capacity"), "6w");
    assertEquals(-1f, getWeight(ROOT, false), 10e-6, "weight is set");
  }

  @Test
  public void testWildcardAfterRoot() {
    conf.set(getTemplateKey(TEST_QUEUE_ROOT_WILDCARD, "acl_submit_applications"), "user");
    AutoCreatedQueueTemplate template =
        new AutoCreatedQueueTemplate(conf.getModel(), TEST_QUEUE_A);

    assertEquals("user",
        template.getTemplateProperties().get("acl_submit_applications"),
        "acl_submit_applications is set");
  }

  @Test
  public void testTemplatePrecedence() {
    conf.set(getTemplateKey(TEST_QUEUE_AB, "capacity"), "6w");
    conf.set(getTemplateKey(TEST_QUEUE_A_WILDCARD, "capacity"), "4w");
    conf.set(getTemplateKey(TEST_QUEUE_TWO_LEVEL_WILDCARDS, "capacity"), "2w");

    assertEquals(6f, getWeight(TEST_QUEUE_ABC, false), 10e-6,
        "explicit template does not have the highest precedence");

    CapacitySchedulerConfiguration newConf =
        new CapacitySchedulerConfiguration();
    newConf.set(getTemplateKey(TEST_QUEUE_A_WILDCARD, "capacity"), "4w");
    assertEquals(4f, getWeight(newConf, TEST_QUEUE_ABC, false),
        10e-6, "precedence is invalid");
  }

  @Test
  public void testRootTemplate() {
    conf.set(getTemplateKey(ROOT, "capacity"), "2w");

    assertEquals(2f, getWeight(TEST_QUEUE_A, false), 10e-6,
        "root property is not set");
  }

  @Test
  public void testQueueSpecificTemplates() {
    conf.set(getTemplateKey(ROOT, "capacity"), "2w");
    conf.set(getLeafTemplateKey(ROOT,
        "default-node-label-expression"), "test");
    conf.set(getLeafTemplateKey(ROOT, "capacity"), "10w");
    conf.setBoolean(getParentTemplateKey(
            ROOT, AUTO_CREATE_CHILD_QUEUE_AUTO_REMOVAL_ENABLE), false);

    assertNull(conf.getModel().effectiveConfigFor(TEST_QUEUE_A, false)
        .getDefaultNodeLabelExpression(),
        "default-node-label-expression is set for parent");
    assertEquals("test", conf.getModel().effectiveConfigFor(TEST_QUEUE_B, true)
        .getDefaultNodeLabelExpression(),
        "default-node-label-expression is not set for leaf");
    assertFalse(Boolean.parseBoolean(conf.getModel()
            .effectiveConfigFor(TEST_QUEUE_A, false)
            .getRawProperty(AUTO_CREATE_CHILD_QUEUE_AUTO_REMOVAL_ENABLE)),
        "auto queue removal is not disabled for parent");
    assertEquals(10f, getWeight(TEST_QUEUE_B, true),
        10e-6, "weight should not be overridden when set by " +
        "queue type specific template");
    assertEquals(2f, getWeight(TEST_QUEUE_A, false), 10e-6,
        "weight should be set by common template");

  }

  @Test
  public void testWildcardTemplateWithLimitedAutoCreatedQueueDepth() {
    conf.set(getTemplateKey(TEST_QUEUE_ROOT_WILDCARD, "capacity"), "6w");
    conf.set(getTemplateKey(TEST_QUEUE_A_WILDCARD, "capacity"), "5w");
    conf.setMaximumAutoCreatedQueueDepth(TEST_QUEUE_A, 1);
    conf.setMaximumAutoCreatedQueueDepth(TEST_QUEUE_AB, 1);

    assertEquals(6f, getWeight(TEST_QUEUE_AB, false), 10e-6,
        "weight is not set");
    assertEquals(5f, getWeight(TEST_QUEUE_ABC, false), 10e-6,
        "weight is not set");
  }

  @Test
  public void testIgnoredTemplateWithLimitedAutoCreatedQueueDepth() {
    conf.set(getTemplateKey(TEST_QUEUE_TWO_LEVEL_WILDCARDS, "capacity"), "5w");
    conf.setMaximumAutoCreatedQueueDepth(TEST_QUEUE_AB, 1);

    assertEquals(-1f, getWeight(TEST_QUEUE_ABC, false), 10e-6,
        "weight is set incorrectly");
  }

  @Test
  public void testIgnoredTemplateWhenQueuePathIsInvalid() {
    QueuePath invalidPath = new QueuePath("a");
    conf.set(getTemplateKey(invalidPath, "capacity"), "6w");
    AutoCreatedQueueTemplate template =
        new AutoCreatedQueueTemplate(conf.getModel(), invalidPath);

    assertEquals(-1f, getWeight(TEST_QUEUE_AB, false),
        10e-6, "weight is set using invalid queue path");
    assertEquals(0, template.getTemplateProperties().size());
  }

  @Test
  public void testExplicitChildPropertyBeatsAllTemplates() {
    conf.set(getTemplateKey(TEST_QUEUE_AB, "capacity"), "6w");
    conf.setCapacity(TEST_QUEUE_ABC, "9w");

    assertEquals(9f, getWeight(TEST_QUEUE_ABC, false), 10e-6);
  }

  private float getWeight(QueuePath queuePath, boolean isLeaf) {
    return getWeight(conf, queuePath, isLeaf);
  }

  private float getWeight(CapacitySchedulerConfiguration configuration,
      QueuePath queuePath, boolean isLeaf) {
    return weight(configuration.getModel().effectiveConfigFor(queuePath,
        isLeaf).getCapacity(""));
  }

  private String getTemplateKey(QueuePath queuePath, String entryKey) {
    return QueuePrefixes.getQueuePrefix(queuePath)
        + AutoCreatedQueueTemplate.AUTO_QUEUE_TEMPLATE_PREFIX + entryKey;
  }

  private String getParentTemplateKey(QueuePath queuePath, String entryKey) {
    return QueuePrefixes.getQueuePrefix(queuePath)
        + AutoCreatedQueueTemplate.AUTO_QUEUE_PARENT_TEMPLATE_PREFIX + entryKey;
  }

  private String getLeafTemplateKey(QueuePath queuePath, String entryKey) {
    return QueuePrefixes.getQueuePrefix(queuePath)
        + AutoCreatedQueueTemplate.AUTO_QUEUE_LEAF_TEMPLATE_PREFIX + entryKey;
  }
}
