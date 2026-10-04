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

/**
 * Validation types for YARN Capacity Scheduler configuration
 */

export type CapacityType = 'percentage' | 'weight' | 'absolute';

export interface ParsedCapacity {
  type: CapacityType;
  value: number;
  resources?: Record<string, number>;
  rawValue: string;
}

/**
 * A server validation issue as displayed by the UI. Issues are attached to fields by their
 * exact (queuePath, propertyKey) pair; a null queuePath marks a global issue.
 */
export interface ValidationIssue {
  queuePath: string | null;
  propertyKey: string | null;
  ruleId: string;
  severity: 'error' | 'warning';
  message: string;
}
