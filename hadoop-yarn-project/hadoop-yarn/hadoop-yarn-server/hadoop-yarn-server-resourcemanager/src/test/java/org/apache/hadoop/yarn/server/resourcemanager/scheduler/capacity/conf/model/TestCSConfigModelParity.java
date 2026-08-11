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

import java.util.Map;
import java.util.Set;

import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.AutoCreatedQueueTemplate;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CSConfigBenchmarkGenerator;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueuePath;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueuePrefixes;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestCSConfigModelParity {

  @ParameterizedTest
  @ValueSource(ints = {100, 1000})
  public void testGeneratedConfigurationMatchesCompatibilityGetters(
      int requestedQueues) {
    CSConfigBenchmarkGenerator.GeneratedConfig generated =
        CSConfigBenchmarkGenerator.generate(requestedQueues);
    assertGetterParity(generated.getConf(), generated.getQueueCount() + 1);
  }

  @Test
  public void testHandWrittenEdgeConfigurationMatchesCompatibilityGetters() {
    QueuePath root = new QueuePath("root");
    QueuePath a = new QueuePath("root.a");
    QueuePath b = new QueuePath("root.b");
    QueuePath c = new QueuePath("root.c");
    CapacitySchedulerConfiguration conf = new CapacitySchedulerConfiguration();
    conf.setQueues(root, new String[] {"a", "b", "c"});
    conf.setMaximumCapacity(a, -1);
    conf.setCapacity(b, "5w");
    conf.setAccessibleNodeLabels(a, Set.of("red"));
    conf.setCapacityByLabel(a, "red", 25);
    conf.setAutoCreateChildQueueEnabled(a, true);
    conf.setAutoCreatedLeafQueueConfigCapacity(a, 50);
    conf.setAutoQueueCreationV2Enabled(b, true);
    conf.set(QueuePrefixes.getQueuePrefix(b)
        + AutoCreatedQueueTemplate.AUTO_QUEUE_LEAF_TEMPLATE_PREFIX
        + CapacitySchedulerConfiguration.CAPACITY, "5w");
    conf.set(QueuePrefixes.getQueuePrefix(c)
        + CapacitySchedulerConfiguration.STATE, "stopped");

    assertGetterParity(conf, 4);
  }

  private void assertGetterParity(CapacitySchedulerConfiguration conf,
      int expectedNodeCount) {
    CSConfigModel model = conf.getModel();

    assertEquals(expectedNodeCount, model.getNodes().size());
    for (Map.Entry<QueuePath, QueueConfigNode> entry
        : model.getNodes().entrySet()) {
      QueuePath path = entry.getKey();
      QueueConfigNode node = entry.getValue();
      assertEquals(conf.getConfiguredState(path), node.getState(),
          path.getFullPath());
      assertEquals(conf.getAccessibleNodeLabels(path),
          node.getAccessibleNodeLabels(), path.getFullPath());
      assertEquals(conf.getNonLabeledQueueCapacity(path),
          LegacyCapacityDerivations.capacity(path, node.getCapacity(""), 0f),
          path.getFullPath());
      assertEquals(conf.getNonLabeledQueueMaximumCapacity(path),
          LegacyCapacityDerivations.maximumCapacity(path,
              node.getMaximumCapacity("")), path.getFullPath());
      assertEquals(conf.getNonLabeledQueueWeight(path),
          LegacyCapacityDerivations.weight(node.getCapacity("")),
          path.getFullPath());
    }
  }
}
