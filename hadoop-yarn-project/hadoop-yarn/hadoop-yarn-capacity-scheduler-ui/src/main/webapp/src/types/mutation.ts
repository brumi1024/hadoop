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

export type MutationError = {
  RemoteException: {
    exception: string;
    message: string;
    javaClassName: string;
  };
};

export type SchedulerValidationSeverity = 'ERROR' | 'WARNING';

/**
 * One issue from POST /scheduler-conf/validate/v2. A null queuePath means the issue is not
 * about a single queue; propertyKey is always the fully qualified configuration key.
 */
export type SchedulerValidationIssue = {
  queuePath: string | null;
  propertyKey: string | null;
  ruleId: string;
  severity: SchedulerValidationSeverity;
  message: string;
};

/**
 * Where a resolved queue value comes from, as reported by the explain section of validate/v2.
 */
export type ValueSource =
  | 'QUEUE'
  | 'TEMPLATE_V2'
  | 'TEMPLATE_V1'
  | 'PARENT'
  | 'GLOBAL'
  | 'DEFAULT'
  | 'DERIVED';

export type ExplainedProperty = {
  key: string;
  value: string | null;
  source: ValueSource;
  sourceDetail: string | null;
};

export type QueueExplain = {
  queuePath: string;
  properties: ExplainedProperty[];
};

/**
 * Normalized validate/v2 result. Single-element collections that the REST layer renders as
 * objects are already turned into arrays.
 */
export type ValidationResponse = {
  valid: boolean;
  issues: SchedulerValidationIssue[];
  explain: QueueExplain[];
};

/**
 * The explain query parameter of validate/v2: queues touched by the proposal and their
 * descendants, or an explicit list of queue paths.
 */
export type ExplainRequest = 'affected' | string[];
