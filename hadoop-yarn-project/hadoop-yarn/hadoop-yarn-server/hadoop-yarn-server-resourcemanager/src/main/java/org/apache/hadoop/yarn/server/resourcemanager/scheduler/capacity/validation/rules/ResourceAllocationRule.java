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
package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.rules;

import java.util.function.Consumer;
import java.util.function.ToLongFunction;

import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.ValidationContext;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.ValidationIssue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.ValidationRule;

/** Shared validation implementation for scheduler-wide resource bounds. */
public final class ResourceAllocationRule implements ValidationRule {
  private final String ruleId;
  private final String minimumKey;
  private final String maximumKey;
  private final int defaultMinimum;
  private final int defaultMaximum;
  private final ToLongFunction<Resource> resourceValue;
  private final String resourceNoun;

  private ResourceAllocationRule(String ruleId, String minimumKey,
      String maximumKey,
      int defaultMinimum, int defaultMaximum,
      ToLongFunction<Resource> resourceValue, String resourceNoun) {
    this.ruleId = ruleId;
    this.minimumKey = minimumKey;
    this.maximumKey = maximumKey;
    this.defaultMinimum = defaultMinimum;
    this.defaultMaximum = defaultMaximum;
    this.resourceValue = resourceValue;
    this.resourceNoun = resourceNoun;
  }

  public static ResourceAllocationRule memory() {
    return new ResourceAllocationRule("memory-allocation",
        YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB,
        YarnConfiguration.RM_SCHEDULER_MAXIMUM_ALLOCATION_MB,
        YarnConfiguration.DEFAULT_RM_SCHEDULER_MINIMUM_ALLOCATION_MB,
        YarnConfiguration.DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_MB,
        Resource::getMemorySize, "memory");
  }

  public static ResourceAllocationRule vcores() {
    return new ResourceAllocationRule("vcores-allocation",
        YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_VCORES,
        YarnConfiguration.RM_SCHEDULER_MAXIMUM_ALLOCATION_VCORES,
        YarnConfiguration.DEFAULT_RM_SCHEDULER_MINIMUM_ALLOCATION_VCORES,
        YarnConfiguration.DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_VCORES,
        Resource::getVirtualCores, "vcores");
  }

  @Override
  public String id() {
    return ruleId;
  }

  @Override
  public Stage stage() {
    return Stage.MODEL;
  }

  @Override
  public void run(ValidationContext context,
      Consumer<ValidationIssue> sink) {
    Integer min = configuredInt(context, minimumKey,
        positiveOrDefault(resourceValue.applyAsLong(
            context.getFacts().getMinimumAllocation()), defaultMinimum), sink);
    Integer max = configuredInt(context, maximumKey,
        positiveOrDefault(resourceValue.applyAsLong(
            context.getFacts().getMaximumAllocation()), defaultMaximum), sink);
    if (min == null || max == null) {
      return;
    }
    if (min <= 0 || min > max) {
      sink.accept(new ValidationIssue(null, null, id(),
          ValidationIssue.Severity.ERROR, errorMessage(resourceNoun)));
    }
  }

  private Integer configuredInt(ValidationContext context, String key,
      int fallback, Consumer<ValidationIssue> sink) {
    String value = context.getModel().getRawProperties().get(key);
    if (value == null) {
      return fallback;
    }
    try {
      return AllocationRuleSupport.parseConfigurationInt(value);
    } catch (NumberFormatException e) {
      sink.accept(new ValidationIssue(null, key, id(),
          ValidationIssue.Severity.ERROR,
          "Invalid integer value '" + value + "' for " + key));
      return null;
    }
  }

  private int positiveOrDefault(long value, int defaultValue) {
    return value > 0 ? (int) value : defaultValue;
  }

  private static String errorMessage(String resourceNoun) {
    return "Invalid resource scheduler " + resourceNoun
        + " allocation configuration, min and max should be greater than 0, "
        + "max should be no smaller than min.";
  }
}
