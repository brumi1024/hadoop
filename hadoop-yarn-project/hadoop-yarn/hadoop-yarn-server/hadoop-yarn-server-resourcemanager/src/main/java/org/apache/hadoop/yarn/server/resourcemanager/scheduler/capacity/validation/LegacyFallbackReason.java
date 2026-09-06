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

import java.util.Objects;

/** One deterministic reason that selects legacy compatibility validation. */
public record LegacyFallbackReason(Code code, String queuePath,
    String propertyKey, String descriptor, String message) {

  /** Unsupported validation dependency categories. */
  public enum Code {
    CUSTOM_APPLICATION_ORDERING_POLICY,
    CUSTOM_PARENT_ORDERING_POLICY,
    CUSTOM_QUEUE_MANAGEMENT_POLICY,
    CUSTOM_MULTI_NODE_POLICY,
    RESERVATION_EXTENSION,
    DYNAMIC_QUEUE_STATE,
    UNSUPPORTED_TEMPLATE_PROPERTY,
    CUSTOM_PLACEMENT_RULE,
    EXTERNAL_PLACEMENT_RULE_SOURCE,
    CUSTOM_RESOURCE_CALCULATOR,
    CUSTOM_RESOURCE_SCHEMA,
    SCHEDULING_MONITOR_POLICY,
    CUSTOM_GROUP_MAPPING,
    CUSTOM_AUTHORIZATION_PROVIDER,
    WORKFLOW_PRIORITY_MAPPING,
    CONFIGURATION_VARIABLE_SUBSTITUTION,
    UNSUPPORTED_RESOURCE_VALUE,
    PRIORITY_ACL,
    UNKNOWN_CAPACITY_PROPERTY,
    UNMODELED_VALIDATION_DEPENDENCY
  }

  public LegacyFallbackReason {
    Objects.requireNonNull(code);
    Objects.requireNonNull(message);
  }
}
