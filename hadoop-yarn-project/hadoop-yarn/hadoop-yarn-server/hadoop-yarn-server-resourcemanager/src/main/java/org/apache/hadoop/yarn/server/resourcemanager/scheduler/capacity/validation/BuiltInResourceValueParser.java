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

/** Pure parser for the canonical built-in queue maximum-allocation syntax. */
final class BuiltInResourceValueParser {
  private static final Pattern ENTRY = Pattern.compile(
      "^(memory-mb|vcores)=([0-9]+)$");

  private BuiltInResourceValueParser() {
  }

  /**
   * Parses the represented subset of ResourceUtils.createResourceFromString.
   * Missing built-in resources retain the live structured syntax value of 0.
   */
  static Map<String, Long> parseQueueMaximum(String raw) {
    Map<String, Long> values = new LinkedHashMap<>();
    values.put(ResourceInformation.MEMORY_URI, 0L);
    values.put(ResourceInformation.VCORES_URI, 0L);
    for (String configured : raw.trim().split(",")) {
      String entry = configured.trim();
      Matcher matcher = ENTRY.matcher(entry);
      if (!matcher.matches()) {
        throw new IllegalArgumentException(
            "Unsupported built-in resource value '" + entry + "'");
      }
      String resource = matcher.group(1);
      long amount = Long.parseLong(matcher.group(2));
      values.put(resource, amount);
    }
    return values;
  }
}
