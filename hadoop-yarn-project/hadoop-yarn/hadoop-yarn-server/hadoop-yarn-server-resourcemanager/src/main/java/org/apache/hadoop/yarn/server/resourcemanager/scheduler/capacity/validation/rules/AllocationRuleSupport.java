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
package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.rules;

/** Shared parsing behavior for scheduler-wide allocation rules. */
final class AllocationRuleSupport {
  private AllocationRuleSupport() {
  }

  static int parseConfigurationInt(String value) {
    String trimmed = value.trim();
    boolean negative = trimmed.startsWith("-");
    String unsigned = negative ? trimmed.substring(1) : trimmed;
    if (unsigned.startsWith("0x") || unsigned.startsWith("0X")) {
      String digits = unsigned.substring(2);
      return Integer.parseInt(negative ? "-" + digits : digits, 16);
    }
    return Integer.parseInt(trimmed);
  }
}
