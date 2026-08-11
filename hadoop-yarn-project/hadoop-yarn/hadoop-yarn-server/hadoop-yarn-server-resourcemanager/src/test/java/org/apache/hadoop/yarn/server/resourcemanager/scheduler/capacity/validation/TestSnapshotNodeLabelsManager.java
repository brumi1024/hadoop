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
package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation;

import java.util.Map;
import java.util.Set;

import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestSnapshotNodeLabelsManager {

  @Test
  public void testSnapshotResourcesAndUnknownLabels() {
    Resource cluster = Resource.newInstance(100, 10);
    Resource gpu = Resource.newInstance(40, 4);
    SnapshotNodeLabelsManager manager = new SnapshotNodeLabelsManager(
        Map.of("GPU", gpu), Set.of("GPU"));

    CapacitySchedulerConfiguration configuration =
        new CapacitySchedulerConfiguration();
    manager.init(configuration);

    assertEquals(Set.of("GPU"), manager.getClusterNodeLabelNames());
    assertEquals(gpu, manager.getResourceByLabel("GPU", cluster));
    assertEquals(cluster, manager.getResourceByLabel(
        RMNodeLabelsManager.NO_LABEL, cluster));
    assertTrue(manager.getResourceByLabel("missing", cluster).equals(
        Resource.newInstance(0, 0)));
  }

  @Test
  public void testValidationContextUsesFactsManager() {
    ClusterFacts facts = ClusterFacts.builder()
        .withResourcesByLabel(Map.of("GPU", Resource.newInstance(4, 1)))
        .withNodeLabels(Set.of("GPU"))
        .build();
    ValidationQueueBuildContext context = new ValidationQueueBuildContext(
        new CapacitySchedulerConfiguration().getModel(), facts);

    assertEquals(Set.of("GPU"), context.getLabelManager()
        .getClusterNodeLabelNames());
    assertEquals(4, context.getLabelManager().getResourceByLabel("GPU",
        Resource.newInstance(10, 2)).getMemorySize());
  }
}
