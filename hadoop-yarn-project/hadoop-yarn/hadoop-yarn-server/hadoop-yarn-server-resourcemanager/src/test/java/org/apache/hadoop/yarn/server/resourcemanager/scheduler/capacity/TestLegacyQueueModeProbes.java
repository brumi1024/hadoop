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

import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.conf.model.CSConfigModel;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestLegacyQueueModeProbes {

  @Test
  public void staticQueuesUseCanonicalPercentageAndWeightVectors() {
    CapacitySchedulerConfiguration configuration =
        new CapacitySchedulerConfiguration();
    QueuePath root = new QueuePath("root");
    QueuePath percentagePath = root.createNewLeaf("percentage");
    QueuePath weightPath = root.createNewLeaf("weight");
    configuration.setQueues(root, new String[] {"percentage", "weight"});
    configuration.setCapacity(percentagePath, 50f);
    configuration.setCapacity(weightPath, "2w");
    CSConfigModel model = configuration.getModel();

    CSQueue percentage = mock(CSQueue.class);
    when(percentage.getQueuePathObject()).thenReturn(percentagePath);
    when(percentage.getQueueCapacities()).thenReturn(mock(QueueCapacities.class));
    CSQueue weight = mock(CSQueue.class);
    when(weight.getQueuePathObject()).thenReturn(weightPath);
    when(weight.getQueueCapacities()).thenReturn(mock(QueueCapacities.class));

    assertTrue(LegacyQueueModeProbes.isPercentageConfigured(
        percentage, "", model));
    assertTrue(LegacyQueueModeProbes.isWeightConfigured(weight, "", model));
  }

  @Test
  public void runtimeQueuesKeepQueueCapacitiesProbes() {
    CSQueue queue = mock(CSQueue.class);
    QueueCapacities capacities = mock(QueueCapacities.class);
    when(queue.isDynamicQueue()).thenReturn(true);
    when(queue.getQueueCapacities()).thenReturn(capacities);
    when(capacities.getCapacity("blue")).thenReturn(25f);
    when(capacities.getWeight("blue")).thenReturn(3f);

    assertTrue(LegacyQueueModeProbes.isPercentageConfigured(
        queue, "blue", null));
    assertTrue(LegacyQueueModeProbes.isWeightConfigured(queue, "blue", null));
  }
}
