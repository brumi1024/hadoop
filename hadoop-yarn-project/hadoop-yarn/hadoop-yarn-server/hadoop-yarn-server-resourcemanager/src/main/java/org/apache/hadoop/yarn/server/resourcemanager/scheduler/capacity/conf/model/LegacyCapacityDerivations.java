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

import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceInformation;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueCapacityVector;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueCapacityVector.ResourceUnitCapacityType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueuePath;
import org.apache.hadoop.yarn.util.resource.ResourceUtils;

/** Derives the legacy scalar view from a canonical capacity value. */
public final class LegacyCapacityDerivations {
  private LegacyCapacityDerivations() {
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
}
