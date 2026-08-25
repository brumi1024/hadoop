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

import { describe, expect, it } from 'vitest';
import { runFieldValidation, type ValidationContext } from '~/config/validation-rules';
import { AUTO_CREATION_PROPS } from '~/types/constants/auto-creation';

function createValidationInput(
  fieldValue: string,
  configEntries: Array<[string, string]>,
  legacyModeEnabled = true,
  originalConfigEntries = configEntries,
): ValidationContext {
  return {
    queuePath: 'root.production',
    fieldName: 'capacity',
    fieldValue,
    config: new Map(configEntries),
    originalConfig: new Map(originalConfigEntries),
    stagedChanges: [],
    legacyModeEnabled,
  };
}

describe('queue-scoped validation rules', () => {
  it('keeps maximum capacity greater than or equal to percentage capacity', () => {
    const validationInput = createValidationInput('60', [
      ['yarn.scheduler.capacity.root.production.capacity', '50'],
      ['yarn.scheduler.capacity.root.production.maximum-capacity', '40'],
    ]);

    expect(runFieldValidation(validationInput)).toContainEqual(
      expect.objectContaining({
        field: 'maximum-capacity',
        rule: 'max-capacity-minimum',
        severity: 'error',
      }),
    );
  });

  it('requires an absolute maximum for an absolute capacity', () => {
    const validationInput = createValidationInput('[memory=4096,vcores=4]', [
      ['yarn.scheduler.capacity.root.production.capacity', '[memory=2048,vcores=2]'],
      ['yarn.scheduler.capacity.root.production.maximum-capacity', '100'],
    ]);

    expect(runFieldValidation(validationInput)).toContainEqual(
      expect.objectContaining({
        field: 'maximum-capacity',
        rule: 'max-capacity-format-match',
        severity: 'error',
      }),
    );
  });

  it('blocks a transition from weight mode while flexible AQC is enabled', () => {
    const validationInput = createValidationInput(
      '60',
      [
        ['yarn.scheduler.capacity.root.production.capacity', '60'],
        [`yarn.scheduler.capacity.root.production.${AUTO_CREATION_PROPS.FLEXIBLE_ENABLED}`, 'true'],
      ],
      true,
      [
        ['yarn.scheduler.capacity.root.production.capacity', '2w'],
        [`yarn.scheduler.capacity.root.production.${AUTO_CREATION_PROPS.FLEXIBLE_ENABLED}`, 'true'],
      ],
    );

    expect(runFieldValidation(validationInput)).toContainEqual(
      expect.objectContaining({
        field: 'capacity',
        rule: 'weight-mode-transition-flexible-aqc',
        severity: 'error',
      }),
    );
  });

  it('allows the weight transition when flexible AQC is disabled', () => {
    const validationInput = createValidationInput(
      '60',
      [
        ['yarn.scheduler.capacity.root.production.capacity', '60'],
        [
          `yarn.scheduler.capacity.root.production.${AUTO_CREATION_PROPS.FLEXIBLE_ENABLED}`,
          'false',
        ],
      ],
      true,
      [
        ['yarn.scheduler.capacity.root.production.capacity', '2w'],
        [
          `yarn.scheduler.capacity.root.production.${AUTO_CREATION_PROPS.FLEXIBLE_ENABLED}`,
          'false',
        ],
      ],
    );

    const issues = runFieldValidation(validationInput);

    expect(
      issues.filter((issue) => issue.rule === 'weight-mode-transition-flexible-aqc'),
    ).toHaveLength(0);
  });
});
