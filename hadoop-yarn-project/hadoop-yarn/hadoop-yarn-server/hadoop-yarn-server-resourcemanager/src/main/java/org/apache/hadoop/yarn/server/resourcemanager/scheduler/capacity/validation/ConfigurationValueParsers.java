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

/** Pure mirrors of Hadoop Configuration's primitive parsing contract. */
final class ConfigurationValueParsers {
  private ConfigurationValueParsers() {
  }

  static int parseInt(String raw) {
    String value = raw.trim();
    String hex = hexDigits(value);
    return hex == null ? Integer.parseInt(value)
        : Integer.parseInt(hex, 16);
  }

  static long parseLong(String raw) {
    String value = raw.trim();
    String hex = hexDigits(value);
    return hex == null ? Long.parseLong(value) : Long.parseLong(hex, 16);
  }

  static boolean parseBoolean(String raw, boolean fallback) {
    if (raw == null) {
      return fallback;
    }
    String value = raw.trim();
    if ("true".equalsIgnoreCase(value)) {
      return true;
    }
    if ("false".equalsIgnoreCase(value)) {
      return false;
    }
    return fallback;
  }

  private static String hexDigits(String value) {
    boolean negative = value.startsWith("-");
    String unsigned = negative ? value.substring(1) : value;
    if (!unsigned.startsWith("0x") && !unsigned.startsWith("0X")) {
      return null;
    }
    return negative ? "-" + unsigned.substring(2) : unsigned.substring(2);
  }
}
