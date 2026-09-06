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

import java.util.List;

import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueCapacityCalculationKernel.CalculatedResourceInput;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueCapacityCalculationKernel.CalculatedResourceResult;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueCapacityCalculationKernel.CapacityKind;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueCapacityCalculationKernel.WarningKind;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Pins the pure scalar and acceptance decisions shared with live queues. */
public class TestQueueCapacityCalculationKernel {

  @Test
  public void testPercentageFormulaPreservesLegacyOperationOrder() {
    double minimum = QueueCapacityCalculationKernel.percentageMinimum(
        853867661134L, 827678397871L, 808062014492.6799D,
        94.3845118885871D);
    double maximum = QueueCapacityCalculationKernel.percentageMaximum(
        675670004778L, 635000074629L, 83.1512083738716D);

    assertEquals(762685388136L, (long) Math.floor(minimum));
    assertEquals(528010235229L, (long) Math.floor(maximum));
  }

  @Test
  public void testAbsoluteNormalizationPreservesLongAndFloatSemantics() {
    long configured = 33554435L;
    long effective = 33554433L;
    QueueCapacityCalculationKernel.AbsoluteNormalization result =
        QueueCapacityCalculationKernel.normalizeAbsolute(configured,
            effective, "", "");
    float legacyNumerator = effective;
    double expectedRatio = legacyNumerator / configured;

    assertTrue(result.defined());
    assertTrue(result.downscaled());
    assertEquals(expectedRatio, result.ratio());
  }

  @Test
  public void testRoundingUsesFloorUntilTheLastCapacityKind() {
    assertEquals(10D, QueueCapacityCalculationKernel.round(10.9D,
        CapacityKind.PERCENTAGE));
    assertEquals(11D, QueueCapacityCalculationKernel.round(10.5D,
        CapacityKind.WEIGHT));
  }

  @Test
  public void testCalculatedResourceWarningsRetainLegacyOrder() {
    CalculatedResourceResult result = QueueCapacityCalculationKernel
        .validateCalculatedResources(new CalculatedResourceInput(
            "memory-mb", 100D, 100L, 200D, 50L, 10D, false));

    assertEquals(10D, result.minimumResource());
    assertEquals(50D, result.maximumResource());
    assertEquals(List.of(WarningKind.QUEUE_MAX_RESOURCE_EXCEEDS_PARENT,
        WarningKind.QUEUE_EXCEEDS_MAX_RESOURCE,
        WarningKind.QUEUE_OVERUTILIZED), result.warnings().stream()
        .map(QueueCapacityCalculationKernel.DecisionWarning::kind).toList());
  }

  @Test
  public void testManagedParentExhaustionProducesZeroResourceWarning() {
    CalculatedResourceResult result = QueueCapacityCalculationKernel
        .validateCalculatedResources(new CalculatedResourceInput(
            "memory-mb", 100D, 100L, 0D, 1000L, 10D, true));

    assertEquals(0D, result.minimumResource());
    assertEquals(List.of(WarningKind.QUEUE_ZERO_RESOURCE),
        result.warnings().stream()
            .map(QueueCapacityCalculationKernel.DecisionWarning::kind)
            .toList());
    assertFalse(result.warnings().get(0).info().isEmpty());
  }
}
