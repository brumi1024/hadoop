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

import { buildPropertyKey } from '~/utils/propertyUtils';
import { parseCapacityValue, getCapacityType } from '~/utils/capacityUtils';
import { AUTO_CREATION_PROPS } from '~/types/constants/auto-creation';
import type { StagedChange, ValidationIssue, SchedulerInfo } from '~/types';
import { isTemplateQueuePath } from '~/utils/templateUtils';

export interface ValidationContext {
  queuePath: string;
  fieldName: string;
  fieldValue: unknown;
  config: Map<string, string>;
  originalConfig?: Map<string, string>;
  schedulerData?: SchedulerInfo | null;
  stagedChanges: StagedChange[];
  legacyModeEnabled: boolean;
}

export interface ValidationRule {
  id: string;
  description: string;
  level: 'error' | 'warning';
  triggers: string[];
  evaluate: (context: ValidationContext) => ValidationIssue[];
}

export const QUEUE_VALIDATION_RULES: ValidationRule[] = [
  {
    id: 'MAX_CAPACITY_CONSTRAINT',
    description: 'Ensures maximum capacity is not less than capacity and uses consistent units.',
    level: 'error',
    triggers: ['capacity', 'maximum-capacity'],
    evaluate: (context) => evaluateMaxCapacityRelationship(context),
  },
  {
    id: 'WEIGHT_MODE_TRANSITION_FLEXIBLE_AQC',
    description:
      'Ensures flexible auto-queue creation is disabled when transitioning from weight mode (legacy mode only).',
    level: 'error',
    triggers: ['capacity'],
    evaluate: (context) => evaluateWeightModeTransitionFlexibleAQC(context),
  },
];

export function runFieldValidation(context: ValidationContext): ValidationIssue[] {
  const applicableRules = QUEUE_VALIDATION_RULES.filter((rule) =>
    rule.triggers.includes(context.fieldName),
  );
  return applicableRules.flatMap((rule) => rule.evaluate(context));
}

// --- Rule evaluators -------------------------------------------------------

function evaluateMaxCapacityRelationship(context: ValidationContext): ValidationIssue[] {
  if (isTemplateQueuePath(context.queuePath)) {
    return [];
  }
  if (!context.legacyModeEnabled) {
    return [];
  }

  const queuePath = context.queuePath;
  const capacityKey = buildPropertyKey(queuePath, 'capacity');
  const maxCapacityKey = buildPropertyKey(queuePath, 'maximum-capacity');

  const capacityValue =
    context.fieldName === 'capacity'
      ? (context.fieldValue as string)
      : context.config.get(capacityKey) || '';
  const maxCapacityValue =
    context.fieldName === 'maximum-capacity'
      ? (context.fieldValue as string)
      : context.config.get(maxCapacityKey) || '';

  if (!maxCapacityValue || maxCapacityValue.trim() === '' || maxCapacityValue === '-1') {
    return [];
  }

  const parsedCapacity = parseCapacityValue(capacityValue);
  const parsedMaxCapacity = parseCapacityValue(maxCapacityValue);

  if (!parsedCapacity || !parsedMaxCapacity) {
    return [];
  }

  if (parsedCapacity.type === 'absolute') {
    if (parsedMaxCapacity.type !== 'absolute') {
      return [
        {
          queuePath,
          field: 'maximum-capacity',
          message:
            'Maximum capacity must use an absolute resource vector when capacity is absolute',
          severity: 'error',
          rule: 'max-capacity-format-match',
        },
      ];
    }

    const capacityResources = parsedCapacity.resources ?? {};
    const maxCapacityResources = parsedMaxCapacity.resources ?? {};
    const resourceIssues: ValidationIssue[] = [];

    Object.entries(capacityResources).forEach(([resource, value]) => {
      const maxValue = maxCapacityResources[resource];
      if (maxValue === undefined || maxValue < value) {
        resourceIssues.push({
          queuePath,
          field: 'maximum-capacity',
          message: `Maximum capacity ${resource} allocation (${maxValue ?? 'unset'}) must be greater than or equal to capacity allocation (${value})`,
          severity: 'error',
          rule: 'max-capacity-minimum',
        });
      }
    });

    return resourceIssues;
  }

  if (parsedMaxCapacity.type !== 'percentage') {
    return [
      {
        queuePath,
        field: 'maximum-capacity',
        message:
          'Maximum capacity must be expressed as a percentage when capacity uses percentage or weight',
        severity: 'error',
        rule: 'max-capacity-format-match',
      },
    ];
  }

  if (parsedCapacity.type === 'percentage' && parsedMaxCapacity.value < parsedCapacity.value) {
    return [
      {
        queuePath,
        field: 'maximum-capacity',
        message: 'Maximum capacity must be greater than or equal to capacity',
        severity: 'error',
        rule: 'max-capacity-minimum',
      },
    ];
  }

  return [];
}

function evaluateWeightModeTransitionFlexibleAQC(context: ValidationContext): ValidationIssue[] {
  if (isTemplateQueuePath(context.queuePath)) {
    return [];
  }
  if (!context.legacyModeEnabled) {
    return [];
  }

  // Only check when the capacity field is being changed
  if (context.fieldName !== 'capacity') {
    return [];
  }

  // Get the old capacity value from config
  const oldValue = (context.originalConfig ?? context.config).get(
    buildPropertyKey(context.queuePath, 'capacity'),
  );
  const oldType = getCapacityType(oldValue);

  // Get the new capacity value from the field being edited
  const newValue = context.fieldValue as string;
  const newType = getCapacityType(newValue);

  // Check if we're transitioning from weight mode to percentage or absolute mode
  if (oldType === 'weight' && (newType === 'percentage' || newType === 'absolute')) {
    // Check if flexible auto-queue creation is enabled for this queue
    const flexibleAQCKey = buildPropertyKey(
      context.queuePath,
      AUTO_CREATION_PROPS.FLEXIBLE_ENABLED,
    );
    const flexibleAQCValue = context.config.get(flexibleAQCKey);

    if (flexibleAQCValue === 'true') {
      return [
        {
          queuePath: context.queuePath,
          field: 'capacity',
          message: `Cannot change from weight mode to ${newType} mode while flexible auto-queue creation is enabled. Please disable "${AUTO_CREATION_PROPS.FLEXIBLE_ENABLED}" first (legacy mode requirement)`,
          severity: 'error',
          rule: 'weight-mode-transition-flexible-aqc',
        },
      ];
    }
  }

  return [];
}
