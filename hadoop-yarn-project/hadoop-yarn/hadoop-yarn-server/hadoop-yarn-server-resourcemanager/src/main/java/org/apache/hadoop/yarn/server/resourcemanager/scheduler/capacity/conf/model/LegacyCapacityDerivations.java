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

import java.util.Set;
import java.util.regex.Matcher;

import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceInformation;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueCapacityVector;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueCapacityVector.ResourceUnitCapacityType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueuePath;
import org.apache.hadoop.yarn.util.resource.ResourceUtils;
import org.apache.hadoop.yarn.util.UnitsConversionUtil;

/** Derives the legacy scalar view from a canonical capacity value. */
public final class LegacyCapacityDerivations {
  private LegacyCapacityDerivations() {
  }

  /**
   * Checks a legacy scalar getter without constructing queue state.
   * @param capacity scalar percentage
   * @param path queue or template path used by the getter
   * @param label null for the non-labeled getter, otherwise the exact label
   */
  public static void validateCapacity(float capacity, QueuePath path,
      String label) {
    if (capacity < 0F || capacity > 100F) {
      if (label == null) {
        throw new IllegalArgumentException(
            "Illegal capacity of " + capacity + " for queue " + path.getFullPath());
      }
      throw new IllegalArgumentException(
          "Illegal capacity of " + capacity + " for node-label=" + label
              + " in queue=" + path
              + ", valid capacity should in range of [0, 100].");
    }
  }

  /**
   * Checks the legacy scalar weight bounds and absent-weight sentinel.
   * @param weight scalar weight
   * @param path queue or template path used by the getter
   * @param label exact label used in the diagnostic
   */
  public static void validateWeight(float weight, QueuePath path, String label) {
    if ((weight < -1e-6 && Math.abs(weight + 1) > 1e-6) || weight > 10000) {
      throw new IllegalArgumentException(
          "Illegal weight=" + weight + " for queue=" + path.getFullPath() + "label="
              + label + ". Acceptable values: [0, 10000], -1 is same as not set");
    }
  }

  public static float capacity(QueuePath path,
      QueueConfigNode.CapacityValue value, float missingValue) {
    if (path.isRoot()) {
      return 100f;
    }
    if (value == null || value.getRawValue() == null) {
      return missingValue;
    }
    String raw = value.getRawValue().trim();
    if (raw.startsWith("[") || raw.endsWith("w")) {
      return missingValue;
    }
    Set<ResourceUnitCapacityType> types =
        value.getVector().getDefinedCapacityTypes();
    if (types.size() != 1
        || !types.contains(ResourceUnitCapacityType.PERCENTAGE)) {
      return missingValue;
    }
    return Float.parseFloat(raw.replace("%", ""));
  }

  public static float maximumCapacity(QueuePath path,
      QueueConfigNode.CapacityValue value) {
    float maximum = capacity(path, value, 100f);
    return maximum == -1f ? 100f : maximum;
  }

  public static float weight(QueueConfigNode.CapacityValue value) {
    if (value == null || value.getRawValue() == null
        || !value.getRawValue().trim().endsWith("w")) {
      return -1f;
    }
    String raw = value.getRawValue().trim();
    return Float.parseFloat(raw.substring(0, raw.length() - 1));
  }

  public static Resource absoluteResource(QueueConfigNode.CapacityValue value,
      Set<String> resourceTypes) {
    Resource resource = Resource.newInstance(0, 0);
    if (value == null || !value.getVector().getDefinedCapacityTypes()
        .equals(Set.of(ResourceUnitCapacityType.ABSOLUTE))) {
      return resource;
    }
    for (QueueCapacityVector.QueueCapacityVectorEntry entry
        : value.getVector()) {
      String name = entry.getResourceName();
      long amount = (long) entry.getResourceValue();
      if (ResourceInformation.MEMORY_URI.equals(name)) {
        resource.setMemorySize(amount);
      } else if (ResourceInformation.VCORES_URI.equals(name)) {
        resource.setVirtualCores((int) amount);
      } else if (resourceTypes.contains(name)
          || ResourceUtils.getResourceTypes().containsKey(name)) {
        resource.setResourceInformation(name,
            ResourceInformation.newInstance(name, amount));
      }
    }
    return resource;
  }

  /**
   * Preserves the legacy raw root quota contract, independently of its fixed
   * percentage scheduling vector. This is a live compatibility adapter only.
   */
  public static Resource rootAbsoluteResource(QueueConfigNode.CapacityValue value,
      Set<String> resourceTypes) {
    Resource resource = Resource.newInstance(0, 0);
    if (value == null || value.getRawValue() == null) {
      return resource;
    }
    Matcher matcher = CapacitySchedulerConfiguration.RESOURCE_PATTERN.matcher(
        value.getRawValue());
    if (matcher.find()) {
      String group = matcher.group(0);
      for (String pair : group.substring(1, group.length() - 1).trim().split(",")) {
        String[] parts = pair.split("=");
        if (parts.length > 1) {
          updateRootResource(resource, resourceTypes, parts[0].trim(), parts[1]);
        }
      }
    }
    // The legacy root resource getter treats a zero-memory vector as absent.
    return resource.getMemorySize() == 0 ? Resource.newInstance(0, 0) : resource;
  }

  private static void updateRootResource(Resource resource, Set<String> resourceTypes,
      String name, String raw) {
    if (!resourceTypes.contains(name) && !ResourceUtils.getResourceTypes().containsKey(name)) {
      return;
    }
    String units = CapacitySchedulerConfiguration.getUnits(raw);
    if (!UnitsConversionUtil.KNOWN_UNITS.contains(units)) {
      return;
    }
    long amount = Long.parseLong(raw.substring(0, raw.length() - units.length()));
    if (!units.isEmpty()) {
      amount = UnitsConversionUtil.convert(units, "Mi", amount);
    }
    if (resourceTypes.contains(name) && "memory".equals(name)) {
      resource.setMemorySize(amount);
    } else if (resourceTypes.contains(name) && "vcores".equals(name)) {
      resource.setVirtualCores((int) amount);
    } else {
      resource.setResourceInformation(name, ResourceInformation.newInstance(name, units, amount));
    }
  }
}
