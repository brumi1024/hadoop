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

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueuePath;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueuePrefixes;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueUpdateWarning;
import org.apache.hadoop.yarn.util.resource.DefaultResourceCalculator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestPlanCapacityEvaluator {
  private static final QueuePath ROOT = new QueuePath("root");
  private static final QueuePath A = new QueuePath("root.a");
  private static final QueuePath B = new QueuePath("root.b");

  @Test
  public void testPercentageCapacityParity() {
    CapacitySchedulerConfiguration conf = conf();
    conf.setQueues(ROOT, new String[] {"a", "b"});
    conf.setCapacity(A, 50);
    conf.setCapacity(B, 50);

    assertParity(conf, facts(Resource.newInstance(100, 10)));
  }

  @Test
  public void testWeightCapacityParity() {
    CapacitySchedulerConfiguration conf = conf();
    conf.setQueues(ROOT, new String[] {"a", "b"});
    conf.setNonLabeledQueueWeight(A, 1);
    conf.setNonLabeledQueueWeight(B, 3);

    assertParity(conf, facts(Resource.newInstance(101, 11)));
  }

  @Test
  public void testAbsoluteDownscaleParity() {
    CapacitySchedulerConfiguration conf = conf();
    conf.setQueues(ROOT, new String[] {"a", "b"});
    conf.setMinimumResourceRequirement("", A, Resource.newInstance(80, 8));
    conf.setMinimumResourceRequirement("", B, Resource.newInstance(80, 8));

    assertParity(conf, facts(Resource.newInstance(100, 10)));
  }

  @Test
  public void testMixedVectorParity() {
    CapacitySchedulerConfiguration conf = conf();
    conf.setLegacyQueueModeEnabled(false);
    conf.setQueues(ROOT, new String[] {"a", "b"});
    conf.setCapacity(A, "[memory=60%,vcores=1w]");
    conf.setCapacity(B, "[memory=40%,vcores=3w]");
    conf.set(QueuePrefixes.getQueuePrefix(A)
        + CapacitySchedulerConfiguration.MAXIMUM_CAPACITY,
        "[memory=80%,vcores=100%]");
    conf.set(QueuePrefixes.getQueuePrefix(B)
        + CapacitySchedulerConfiguration.MAXIMUM_CAPACITY,
        "[memory=100%,vcores=100%]");

    assertParity(conf, facts(Resource.newInstance(101, 11)));
  }

  @Test
  public void testLabeledPartitionParity() {
    CapacitySchedulerConfiguration conf = conf();
    conf.setQueues(ROOT, new String[] {"a", "b"});
    conf.setMinimumResourceRequirement("", A, Resource.newInstance(50, 5));
    conf.setMinimumResourceRequirement("", B, Resource.newInstance(50, 5));
    conf.setAccessibleNodeLabels(ROOT, Set.of("GPU"));
    conf.setAccessibleNodeLabels(A, Set.of("GPU"));
    conf.setAccessibleNodeLabels(B, Set.of("GPU"));
    conf.setCapacityByLabel(ROOT, "GPU", "[memory=100,vcores=10]");
    conf.setCapacityByLabel(A, "GPU", "[memory=8,vcores=2]");
    conf.setCapacityByLabel(B, "GPU", "[memory=8,vcores=2]");

    Resource cluster = Resource.newInstance(100, 10);
    ClusterFacts facts = ClusterFacts.builder()
        .withResources(cluster, Resource.newInstance(1, 1),
            Resource.newInstance(1000, 100),
            new DefaultResourceCalculator())
        .withNodeLabels(Set.of("GPU"))
        .withResourcesByLabel(Map.of("", cluster, "GPU",
            Resource.newInstance(10, 2)))
        .build();

    assertParity(conf, facts);
  }

  private void assertParity(CapacitySchedulerConfiguration conf,
      ClusterFacts facts) {
    CompileResult legacy = new CSConfigValidationEngine().compile(
        conf.getModel(), facts);
    assertTrue(legacy.isValid(), legacy.getIssues().toString());

    List<String> expected = legacy.getIssues().stream()
        .filter(issue -> issue.getRuleId().startsWith("capacity-update-"))
        .map(issue -> issue.getRuleId() + "|" + issue.getMessage())
        .collect(Collectors.toList());
    List<String> actual = new PlanCapacityEvaluator()
        .evaluate(legacy.getPlan(), facts).stream()
        .map(this::warningKey)
        .collect(Collectors.toList());

    assertEquals(expected, actual);
  }

  private String warningKey(QueueUpdateWarning warning) {
    return "capacity-update-" + warning.getWarningType().name()
        .toLowerCase(Locale.ROOT).replace('_', '-') + "|" + warning;
  }

  private ClusterFacts facts(Resource cluster) {
    return ClusterFacts.builder()
        .withResources(cluster, Resource.newInstance(1, 1),
            Resource.newInstance(1000, 100),
            new DefaultResourceCalculator())
        .withResourcesByLabel(Map.of("", cluster))
        .build();
  }

  private CapacitySchedulerConfiguration conf() {
    return new CapacitySchedulerConfiguration(new Configuration(false), false);
  }
}
