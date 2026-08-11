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

import java.util.Set;

import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.TestCapacitySchedulerAutoCreatedQueueBase;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestClusterFacts extends TestCapacitySchedulerAutoCreatedQueueBase {

  @Test
  public void testEmptyFactsHaveNoLiveCollections() {
    ClusterFacts facts = ClusterFacts.empty();

    assertTrue(facts.getNodeLabels().isEmpty());
    assertTrue(facts.getResourcesByLabel().isEmpty());
  }

  @Test
  public void testCaptureIncludesLabelsAndResources() {
    ClusterFacts facts = ClusterFacts.capture(cs);
    Set<String> labels = mockRM.getRMContext().getNodeLabelManager()
        .getClusterNodeLabelNames();
    assertEquals(labels, facts.getNodeLabels());
    assertTrue(facts.getResourcesByLabel().containsKey(
        RMNodeLabelsManager.NO_LABEL));
    for (String label : labels) {
      assertEquals(mockRM.getRMContext().getNodeLabelManager()
              .getResourceByLabel(label, cs.getClusterResource()),
          facts.getResourcesByLabel().get(label));
    }
  }
}
