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

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.yarn.api.records.ResourceInformation;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.ValidatedQueuePlan.CapacityEntry;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.ValidatedQueuePlan.CapacitySetting;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.ValidatedQueuePlan.CapacityType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.ValidatedQueuePlan.CapacityVector;
import org.apache.hadoop.yarn.util.UnitsConversionUtil;

/**
 * Pure parser for the built-in memory/vcores capacity syntax represented by
 * compiled validation.
 *
 * <p>This deliberately does not consult {@code ResourceUtils}. Unsupported
 * resource names and future suffixes are compatibility fallbacks rather than
 * guessed extensions.</p>
 */
final class BuiltInCapacityParser {
  private static final Pattern UNIFORM = Pattern.compile("^([0-9.]+)(.*)");
  private static final Pattern VECTOR = Pattern.compile(
      "^\\[([\\w\\.,\\-_%\\ /]+=[\\w\\.,\\-_%\\ /]+)+\\]$");
  private static final Pattern FLOAT_DIGITS = Pattern.compile("[0-9.]");

  private BuiltInCapacityParser() {
  }

  static CapacitySetting parse(String raw, boolean root) {
    if (root) {
      return new CapacitySetting(raw, uniform(100F,
          CapacityType.PERCENTAGE));
    }
    if (raw == null) {
      return empty(raw);
    }
    String normalized = raw.replace(" ", "");
    Matcher vector = VECTOR.matcher(normalized);
    if (vector.find()) {
      return new CapacitySetting(raw, parseVector(vector.group()));
    }
    Matcher uniform = UNIFORM.matcher(normalized);
    if (uniform.find()) {
      String suffix = uniform.group(2);
      CapacityType type = switch (suffix) {
      case "" -> CapacityType.PERCENTAGE;
      case "w" -> CapacityType.WEIGHT;
      default -> null;
      };
      if (type != null) {
        return new CapacitySetting(raw,
            uniform(Float.parseFloat(uniform.group(1)), type));
      }
    }
    return empty(raw);
  }

  private static CapacityVector parseVector(String raw) {
    Map<String, CapacityEntry> entries = builtInZeros();
    String content = raw.substring(1, raw.length() - 1);
    for (String pair : content.trim().split(",")) {
      String[] parts = pair.split("=");
      if (parts.length > 1) {
        set(entries, parts[0], parts[1]);
      }
    }
    return new CapacityVector(entries);
  }

  private static void set(Map<String, CapacityEntry> entries,
      String configuredResourceName, String configuredValue) {
    String resourceName = "memory".equals(configuredResourceName)
        ? ResourceInformation.MEMORY_URI : configuredResourceName;
    if (!ResourceInformation.MEMORY_URI.equals(resourceName)
        && !ResourceInformation.VCORES_URI.equals(resourceName)) {
      throw new UnsupportedCapacitySyntaxException(
          "Resource " + configuredResourceName
              + " is outside the captured built-in schema");
    }
    String suffix = FLOAT_DIGITS.matcher(configuredValue).replaceAll("");
    float parsed = Float.parseFloat(configuredValue.substring(0,
        configuredValue.length() - suffix.length()));
    float converted = parsed;
    CapacityType type = CapacityType.ABSOLUTE;
    if (!suffix.isEmpty() && UnitsConversionUtil.KNOWN_UNITS.contains(suffix)) {
      converted = UnitsConversionUtil.convert(suffix, "Mi", (long) parsed);
    } else {
      type = switch (suffix) {
      case "" -> CapacityType.ABSOLUTE;
      case "%" -> CapacityType.PERCENTAGE;
      case "w" -> CapacityType.WEIGHT;
      default -> throw new UnsupportedCapacitySyntaxException(
          "Capacity suffix " + suffix + " is not represented");
      };
    }
    entries.put(resourceName, new CapacityEntry(converted, type));
  }

  private static CapacityVector uniform(float value, CapacityType type) {
    Map<String, CapacityEntry> entries = new LinkedHashMap<>();
    entries.put(ResourceInformation.MEMORY_URI,
        new CapacityEntry(value, type));
    entries.put(ResourceInformation.VCORES_URI,
        new CapacityEntry(value, type));
    return new CapacityVector(entries);
  }

  private static Map<String, CapacityEntry> builtInZeros() {
    Map<String, CapacityEntry> entries = new LinkedHashMap<>();
    entries.put(ResourceInformation.MEMORY_URI,
        new CapacityEntry(0D, CapacityType.ABSOLUTE));
    entries.put(ResourceInformation.VCORES_URI,
        new CapacityEntry(0D, CapacityType.ABSOLUTE));
    return entries;
  }

  private static CapacitySetting empty(String raw) {
    return new CapacitySetting(raw, new CapacityVector(Map.of()));
  }

  static final class UnsupportedCapacitySyntaxException
      extends IllegalArgumentException {
    UnsupportedCapacitySyntaxException(String message) {
      super(message);
    }
  }
}
