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
 * Server validation issues and resolved-value sources.
 *
 * The UI has no validation rules of its own. Issues come from POST
 * /scheduler-conf/validate/v2 and are attached to fields only by their exact
 * (queuePath, propertyKey) pair; an issue with a null queuePath is global.
 */

import type {
  ExplainedProperty,
  QueueExplain,
  SchedulerValidationIssue,
  StagedChange,
  ValidationIssue,
} from '~/types';
import { SPECIAL_VALUES } from '~/types/constants/special-values';
import { buildGlobalPropertyKey, buildPropertyKey } from '~/utils/propertyUtils';

export function toValidationIssues(issues: SchedulerValidationIssue[]): ValidationIssue[] {
  return issues.map((issue) => ({
    queuePath: issue.queuePath,
    propertyKey: issue.propertyKey,
    ruleId: issue.ruleId,
    severity: issue.severity === 'WARNING' ? 'warning' : 'error',
    message: issue.message,
  }));
}

/**
 * The fully qualified key a staged change writes, or null for a queue removal.
 */
export function stagedChangePropertyKey(change: StagedChange): string | null {
  if (change.property === SPECIAL_VALUES.QUEUE_MARKER) {
    return null;
  }
  if (change.queuePath === SPECIAL_VALUES.GLOBAL_QUEUE_PATH) {
    return buildGlobalPropertyKey(change.property);
  }
  return buildPropertyKey(change.queuePath, change.property);
}

/**
 * Issues for one queue property, matched by queue path and full property key.
 */
export function getPropertyIssues(
  issues: ValidationIssue[],
  queuePath: string,
  propertyName: string,
): ValidationIssue[] {
  const key = buildPropertyKey(queuePath, propertyName);
  return issues.filter((issue) => issue.queuePath === queuePath && issue.propertyKey === key);
}

/**
 * Issues for one global property. Global issues carry a null queue path.
 */
export function getGlobalPropertyIssues(
  issues: ValidationIssue[],
  propertyName: string,
): ValidationIssue[] {
  const key = buildGlobalPropertyKey(propertyName);
  return issues.filter((issue) => issue.queuePath === null && issue.propertyKey === key);
}

/**
 * Every issue reported for a queue, with or without a property key.
 */
export function getQueueIssues(issues: ValidationIssue[], queuePath: string): ValidationIssue[] {
  return issues.filter((issue) => issue.queuePath === queuePath);
}

export function getGlobalIssues(issues: ValidationIssue[]): ValidationIssue[] {
  return issues.filter((issue) => issue.queuePath === null);
}

/**
 * Issues for the key a staged change writes. A queue removal shows the queue's issues that
 * have no property key.
 */
export function getStagedChangeIssues(
  issues: ValidationIssue[],
  change: StagedChange,
): ValidationIssue[] {
  const key = stagedChangePropertyKey(change);
  const queuePath = change.queuePath === SPECIAL_VALUES.GLOBAL_QUEUE_PATH ? null : change.queuePath;
  return issues.filter((issue) => issue.queuePath === queuePath && issue.propertyKey === key);
}

/**
 * The short property name of an issue on a queue, when its key belongs to that queue.
 */
export function getIssuePropertyName(issue: ValidationIssue): string | null {
  if (!issue.queuePath || !issue.propertyKey) {
    return null;
  }
  const prefix = buildPropertyKey(issue.queuePath, '');
  return issue.propertyKey.startsWith(prefix) ? issue.propertyKey.slice(prefix.length) : null;
}

export function formatIssue(issue: ValidationIssue): string {
  const target = [issue.queuePath, issue.propertyKey ? `(${issue.propertyKey})` : null]
    .filter(Boolean)
    .join(' ');
  return target ? `${target}: ${issue.message}` : issue.message;
}

export type ExplainIndex = Record<string, Record<string, ExplainedProperty>>;

export function indexExplain(explain: QueueExplain[]): ExplainIndex {
  const index: ExplainIndex = {};
  for (const queue of explain) {
    const properties: Record<string, ExplainedProperty> = {};
    for (const property of queue.properties) {
      properties[property.key] = property;
    }
    index[queue.queuePath] = properties;
  }
  return index;
}

/**
 * The explained source of a queue property, or null when the server did not explain it.
 */
export function getExplainedProperty(
  index: ExplainIndex,
  queuePath: string,
  propertyName: string,
): ExplainedProperty | null {
  return index[queuePath]?.[buildPropertyKey(queuePath, propertyName)] ?? null;
}

/**
 * Describes where a resolved value comes from, using the explain section of validate/v2.
 * Returns null when the queue sets the value itself.
 */
export function describeValueSource(explained: ExplainedProperty): string | null {
  const detail = explained.sourceDetail;
  switch (explained.source) {
    case 'QUEUE':
      return null;
    case 'PARENT':
      return detail ? `inherited from ${detail}` : 'inherited from the parent queue';
    case 'GLOBAL':
      return detail ? `from global setting ${detail}` : 'from a global setting';
    case 'TEMPLATE_V2':
      return detail ? `from flexible auto-creation template ${detail}` : 'from a template';
    case 'TEMPLATE_V1':
      return detail ? `from legacy auto-creation template ${detail}` : 'from a template';
    case 'DEFAULT':
      return 'scheduler default';
    case 'DERIVED':
      return detail ? `derived from ${detail}` : 'derived';
  }
}

export function hasBlockingIssues(issues: ValidationIssue[]): boolean {
  return issues.some((issue) => issue.severity === 'error');
}

export function splitIssues(issues: ValidationIssue[]): {
  errors: ValidationIssue[];
  warnings: ValidationIssue[];
} {
  const errors: ValidationIssue[] = [];
  const warnings: ValidationIssue[] = [];

  issues.forEach((issue) => {
    if (issue.severity === 'error') {
      errors.push(issue);
    } else {
      warnings.push(issue);
    }
  });

  return { errors, warnings };
}
