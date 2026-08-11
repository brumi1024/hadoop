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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.conf.model;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.QueueState;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.AutoCreatedQueueTemplate;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueCapacityVector;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueCapacityVector.ResourceUnitCapacityType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueuePath;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueuePrefixes;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestCSConfigModelBuilder {

  private static final QueuePath ROOT = new QueuePath("root");
  private static final QueuePath A = new QueuePath("root.a");
  private static final QueuePath A1 = new QueuePath("root.a.a1");

  @Test
  public void testBuildsTreeIndexAndTypedQueueValues() {
    CapacitySchedulerConfiguration conf = createConfiguration();
    conf.setQueues(ROOT, new String[]{"a", "b"});
    conf.setQueues(A, new String[]{"a1"});
    conf.setCapacity(A, "[memory=1024,vcores=2]");
    conf.setCapacity(A1, 100f);
    conf.setState(A1, QueueState.STOPPED);
    conf.setAccessibleNodeLabels(A, Set.of("blue"));

    CSConfigModel model = CSConfigModelBuilder.build(conf);
    QueueConfigNode node = model.getNode(A);

    assertEquals(ROOT, model.getRoot().getQueuePath());
    assertSame(node, model.getRoot().getChildren().get("a"));
    assertSame(model.getNode(A1), node.getChildren().get("a1"));
    assertEquals("[memory=1024,vcores=2]",
        node.getCapacity("").getRawValue());
    assertTrue(node.getCapacity("").getVector()
        .isResourceOfType("memory-mb", ResourceUnitCapacityType.ABSOLUTE));
    assertEquals(QueueState.STOPPED, model.getNode(A1).getState());
    assertEquals(Set.of("blue"), node.getAccessibleNodeLabels());
    assertTrue(model.getDiagnostics().isEmpty());
  }

  @Test
  public void testMalformedValuesBecomeDiagnosticsInsteadOfParseFailures() {
    CapacitySchedulerConfiguration conf = createConfiguration();
    conf.setQueues(ROOT, new String[]{"a"});
    conf.set("yarn.scheduler.capacity.root.a.state", "paused");
    conf.set("yarn.scheduler.capacity.root.a.capacity", "not-a-capacity");

    CSConfigModel model = CSConfigModelBuilder.build(conf);

    assertNull(model.getNode(A).getState());
    assertFalse(model.getDiagnostics().isEmpty());
    assertTrue(model.getDiagnostics().stream()
        .anyMatch(diagnostic -> "invalid-queue-state".equals(
            diagnostic.getCode())));
    assertTrue(model.getDiagnostics().stream()
        .anyMatch(diagnostic -> "invalid-capacity".equals(
            diagnostic.getCode())));
  }

  @Test
  public void testDeprecatedGlobalKeyIsWarningDiagnostic() {
    CapacitySchedulerConfiguration conf = createConfiguration();
    conf.setQueues(ROOT, new String[] {"a"});
    conf.setCapacity(A, 100);
    conf.set("topology.script.file.name", "/etc/hadoop/topology.sh");

    CSConfigModel model = conf.getModel();

    ConfigDiagnostic diagnostic = model.getDiagnostics().stream()
        .filter(item -> "deprecated-key".equals(item.getCode()))
        .findFirst().orElseThrow();
    assertEquals("topology.script.file.name", diagnostic.getPropertyKey());
  }

  @Test
  public void testV2DynamicLeafUsesV2FamilyWhenV1TemplateIsStray() {
    CapacitySchedulerConfiguration conf = createConfiguration();
    conf.setQueues(ROOT, new String[] {"a"});
    conf.setAutoQueueCreationV2Enabled(A, true);
    conf.set(v1LeafTemplateKey(A, CapacitySchedulerConfiguration.CAPACITY), "50%");
    conf.set(v2LeafTemplateKey(A, CapacitySchedulerConfiguration.CAPACITY), "2w");

    QueueConfigNode node = conf.getModel().effectiveConfigFor(
        new QueuePath("root.a.dynamic"), true, true);

    assertEquals("2w", node.getCapacity("").getRawValue());
  }

  @Test
  public void testV1DynamicLeafUsesV1FamilyWhenBothAreConfigured() {
    CapacitySchedulerConfiguration conf = createConfiguration();
    conf.setQueues(ROOT, new String[] {"a"});
    conf.setAutoCreateChildQueueEnabled(A, true);
    conf.set(v1LeafTemplateKey(A, CapacitySchedulerConfiguration.CAPACITY), "50%");
    conf.set(v2LeafTemplateKey(A, CapacitySchedulerConfiguration.CAPACITY), "2w");

    QueueConfigNode node = conf.getModel().effectiveConfigFor(
        new QueuePath("root.a.dynamic"), true, true);

    assertEquals("50%", node.getCapacity("").getRawValue());
  }

  @Test
  public void testStaticLeafDoesNotInheritV1LeafTemplate() {
    CapacitySchedulerConfiguration conf = createConfiguration();
    conf.setQueues(ROOT, new String[] {"a"});
    conf.setQueues(A, new String[] {"a1"});
    conf.set(v1LeafTemplateKey(A, CapacitySchedulerConfiguration.CAPACITY), "50%");
    conf.set(v2LeafTemplateKey(A, CapacitySchedulerConfiguration.CAPACITY), "2w");

    QueueConfigNode node = conf.getModel().effectiveConfigFor(A1, true, false);

    assertEquals("2w", node.getCapacity("").getRawValue());
  }

  private static String v1LeafTemplateKey(QueuePath parent, String suffix) {
    QueuePath template = QueuePrefixes
        .getAutoCreatedQueueObjectTemplateConfPrefix(parent);
    return QueuePrefixes.getQueuePrefix(template) + suffix;
  }

  private static String v2LeafTemplateKey(QueuePath parent, String suffix) {
    return QueuePrefixes.getQueuePrefix(parent)
        + AutoCreatedQueueTemplate.AUTO_QUEUE_LEAF_TEMPLATE_PREFIX + suffix;
  }

  @Test
  public void testAdapterRebuildsModelAfterMutation() {
    CapacitySchedulerConfiguration conf = createConfiguration();
    conf.setQueues(ROOT, new String[]{"a"});
    conf.setCapacity(A, 100f);

    CSConfigModel before = conf.getModel();
    assertSame(before, conf.getModel());

    conf.setCapacity(A, 75f);
    CSConfigModel after = conf.getModel();
    assertNotSame(before, after);
    assertEquals("75.0", after.getNode(A).getCapacity("").getRawValue());
  }

  @Test
  public void testCopiesShareModelUntilOneCopyMutates() {
    CapacitySchedulerConfiguration original = createConfiguration();
    original.setQueues(ROOT, new String[]{"a"});
    original.setCapacity(A, 100f);
    CSConfigModel shared = original.getModel();

    CapacitySchedulerConfiguration copy =
        new CapacitySchedulerConfiguration(original, false);
    assertSame(shared, copy.getModel());

    copy.setCapacity(A, 50f);
    assertNotSame(shared, copy.getModel());
    assertSame(shared, original.getModel());
  }

  @Test
  public void testEffectiveConfigUsesTemplatePrecedenceAndExplicitValues() {
    CapacitySchedulerConfiguration conf = createConfiguration();
    conf.setQueues(ROOT, new String[]{"a"});
    conf.setQueues(A, new String[]{"a1"});
    conf.set("yarn.scheduler.capacity.root.*.auto-queue-creation-v2."
        + "template.user-limit-factor", "2");
    conf.set("yarn.scheduler.capacity.root.a.auto-queue-creation-v2."
        + "template.user-limit-factor", "3");
    conf.set("yarn.scheduler.capacity.root.a.auto-queue-creation-v2."
        + "leaf-template.capacity", "6w");
    conf.set("yarn.scheduler.capacity.root.a.a1.user-limit-factor", "4");

    CSConfigModel model = conf.getModel();
    QueueConfigNode hypothetical = model.effectiveConfigFor(
        new QueuePath("root.a.dynamic"), true);
    QueueConfigNode configured = model.effectiveConfigFor(A1, true);

    assertNotSame(model.getNode(A1), configured);
    assertEquals("3", hypothetical.getRawProperty("user-limit-factor"));
    assertEquals("6w", hypothetical.getCapacity("").getRawValue());
    assertEquals(ConfigProvenance.TEMPLATE,
        hypothetical.getProvenance("user-limit-factor"));
    assertEquals("4", configured.getRawProperty("user-limit-factor"));
    assertEquals(ConfigProvenance.USER,
        configured.getProvenance("user-limit-factor"));
  }

  @Test
  public void testTemplatelessStaticNodeIsSharedWithItsChildren() {
    CapacitySchedulerConfiguration conf = createConfiguration();
    conf.setQueues(ROOT, new String[] {"a"});
    conf.setQueues(A, new String[] {"a1"});

    CSConfigModel model = conf.getModel();
    QueueConfigNode configured = model.getNode(A);

    assertFalse(configured.getChildren().isEmpty());
    assertSame(configured, model.effectiveConfigFor(A, false, false));
  }

  @Test
  public void testEffectiveConfigResolvesLegacyLeafTemplateResources() {
    CapacitySchedulerConfiguration conf = createConfiguration();
    conf.setQueues(ROOT, new String[]{"a"});
    conf.setAutoCreateChildQueueEnabled(A, true);
    conf.setAutoCreatedLeafQueueTemplateCapacityByLabel(A, "",
        org.apache.hadoop.yarn.api.records.Resource.newInstance(25600, 5));
    conf.setAutoCreatedLeafQueueTemplateMaxCapacity(A, "",
        org.apache.hadoop.yarn.api.records.Resource.newInstance(153600, 20));

    QueueConfigNode effective = conf.getModel().effectiveConfigFor(
        new QueuePath("root.a.dynamic"), true, true);

    assertEquals(5, effective.getCapacity("").getVector()
        .getResource("vcores").getResourceValue());
    assertEquals(20, effective.getMaximumCapacity("").getVector()
        .getResource("vcores").getResourceValue());
  }

  @Test
  public void testAddResourceAndClearInvalidateTheModel() throws Exception {
    CapacitySchedulerConfiguration conf = createConfiguration();
    CSConfigModel empty = conf.getModel();
    CapacitySchedulerConfiguration resource = createConfiguration();
    resource.setQueues(ROOT, new String[]{"a"});
    resource.setCapacity(A, 100f);
    ByteArrayOutputStream xml = new ByteArrayOutputStream();
    resource.writeXml(xml);

    conf.addResource(new ByteArrayInputStream(xml.toByteArray()));
    CSConfigModel loaded = conf.getModel();
    assertNotSame(empty, loaded);
    assertEquals("100.0", loaded.getNode(A).getCapacity("").getRawValue());

    conf.clear();
    assertNotSame(loaded, conf.getModel());
    assertNull(conf.getModel().getNode(A));
  }

  @Test
  public void testReadFieldsInvalidatesTheModel() throws Exception {
    CapacitySchedulerConfiguration source = createConfiguration();
    source.setQueues(ROOT, new String[]{"a"});
    source.setCapacity(A, 100f);
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    source.write(new DataOutputStream(bytes));

    CapacitySchedulerConfiguration target = createConfiguration();
    CSConfigModel before = target.getModel();
    target.readFields(new DataInputStream(
        new ByteArrayInputStream(bytes.toByteArray())));

    assertNotSame(before, target.getModel());
    assertEquals("100.0",
        target.getModel().getNode(A).getCapacity("").getRawValue());
  }

  @Test
  public void testPublishedCapacityVectorCannotMutateModel() {
    CapacitySchedulerConfiguration conf = createConfiguration();
    conf.setQueues(ROOT, new String[]{"a"});
    conf.setCapacity(A, "[memory=1024,vcores=2]");
    QueueConfigNode.CapacityValue value =
        conf.getModel().getNode(A).getCapacity("");

    QueueCapacityVector exposed = value.getVector();
    exposed.setResource("memory", 4096,
        ResourceUnitCapacityType.ABSOLUTE);

    assertEquals(1024, value.getVector().getMemory());
  }

  /**
   * Auto queue creation templates must never contribute ACLs to a statically
   * configured queue. A static queue that omits its own {@code acl_*} has to
   * fall back to the NONE default so that authorization stays with its
   * ancestors, because {@code ConfiguredYarnAuthorizer} only ever widens
   * access as it walks up the hierarchy.
   */
  @Test
  public void testTemplateAclsDoNotLeakIntoStaticQueues() {
    CapacitySchedulerConfiguration conf = createConfiguration();
    conf.setQueues(ROOT, new String[]{"a"});
    conf.setQueues(A, new String[]{"a1"});
    // The parent restricts submission, but carries a template that opens it up
    // for the queues it auto-creates.
    conf.set("yarn.scheduler.capacity.root.a.acl_submit_applications", "alice");
    conf.set("yarn.scheduler.capacity.root.a.auto-queue-creation-v2."
        + "template.acl_submit_applications", "*");
    conf.set("yarn.scheduler.capacity.root.a.auto-queue-creation-v2."
        + "template.user-limit-factor", "3");

    CSConfigModel model = conf.getModel();

    QueueConfigNode staticChild = model.effectiveConfigFor(A1, true, false);
    assertNull(staticChild.getRawProperty("acl_submit_applications"),
        "a static queue must not inherit acl_submit_applications from an "
            + "auto-queue-creation template");
    assertNull(staticChild.getRawProperty("acl_administer_queue"),
        "a static queue must not inherit acl_administer_queue from an "
            + "auto-queue-creation template");
    // Non-ACL template propagation is unchanged.
    assertEquals("3", staticChild.getRawProperty("user-limit-factor"));

    // A dynamic queue under the same parent still gets the template ACL,
    // which is the whole point of the template.
    QueueConfigNode dynamicChild = model.effectiveConfigFor(
        new QueuePath("root.a.dynamic"), true, true);
    assertEquals("*", dynamicChild.getRawProperty("acl_submit_applications"));
    assertEquals("3", dynamicChild.getRawProperty("user-limit-factor"));
  }

  /**
   * When the only template properties an ancestor declares are ACLs, a static
   * queue has nothing left to merge, so it must resolve to its own configured
   * node rather than an equivalent copy.
   */
  @Test
  public void testAclOnlyTemplateLeavesStaticNodeIdentityIntact() {
    CapacitySchedulerConfiguration conf = createConfiguration();
    conf.setQueues(ROOT, new String[]{"a"});
    conf.setQueues(A, new String[]{"a1"});
    conf.set("yarn.scheduler.capacity.root.a.auto-queue-creation-v2."
        + "template.acl_submit_applications", "*");

    CSConfigModel model = conf.getModel();

    assertSame(model.getNode(A1), model.effectiveConfigFor(A1, true, false));
  }

  private CapacitySchedulerConfiguration createConfiguration() {
    return new CapacitySchedulerConfiguration(new Configuration(false), false);
  }
}
