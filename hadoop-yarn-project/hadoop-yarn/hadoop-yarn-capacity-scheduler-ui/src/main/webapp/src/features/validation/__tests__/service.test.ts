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

import { describe, it, expect } from 'vitest';
import {
  describeValueSource,
  getExplainedProperty,
  getGlobalIssues,
  getGlobalPropertyIssues,
  getIssuePropertyName,
  getPropertyIssues,
  getQueueIssues,
  getStagedChangeIssues,
  hasBlockingIssues,
  indexExplain,
  splitIssues,
  toValidationIssues,
} from '~/features/validation/service';
import type { StagedChange, ValidationIssue } from '~/types';

const issue = (overrides: Partial<ValidationIssue>): ValidationIssue => ({
  queuePath: 'root.a',
  propertyKey: 'yarn.scheduler.capacity.root.a.capacity',
  ruleId: 'rule',
  severity: 'error',
  message: 'message',
  ...overrides,
});

const change = (overrides: Partial<StagedChange>): StagedChange => ({
  id: '1',
  type: 'update',
  queuePath: 'root.a',
  property: 'capacity',
  newValue: '10',
  timestamp: 0,
  ...overrides,
});

describe('server issue mapping', () => {
  it('converts severities and keeps paths and keys as sent', () => {
    expect(
      toValidationIssues([
        {
          queuePath: null,
          propertyKey: null,
          ruleId: 'invalid-mutation',
          severity: 'ERROR',
          message: 'Queue root.x not found',
        },
        {
          queuePath: 'root.a',
          propertyKey: 'yarn.scheduler.capacity.root.a.capacity',
          ruleId: 'w',
          severity: 'WARNING',
          message: 'Warning',
        },
      ]),
    ).toEqual([
      {
        queuePath: null,
        propertyKey: null,
        ruleId: 'invalid-mutation',
        severity: 'error',
        message: 'Queue root.x not found',
      },
      {
        queuePath: 'root.a',
        propertyKey: 'yarn.scheduler.capacity.root.a.capacity',
        ruleId: 'w',
        severity: 'warning',
        message: 'Warning',
      },
    ]);
  });

  it('matches queue properties by queue path and full key only', () => {
    const own = issue({});
    const sameKeyOtherQueue = issue({ queuePath: 'root.b' });
    const label = issue({
      propertyKey: 'yarn.scheduler.capacity.root.a.accessible-node-labels.gpu.capacity',
    });
    const issues = [own, sameKeyOtherQueue, label, issue({ propertyKey: null })];

    expect(getPropertyIssues(issues, 'root.a', 'capacity')).toEqual([own]);
    expect(getPropertyIssues(issues, 'root.a', 'accessible-node-labels.gpu.capacity')).toEqual([
      label,
    ]);
    expect(getQueueIssues(issues, 'root.a')).toHaveLength(3);
  });

  it('keeps null-path issues global and matches global fields by key', () => {
    const globalKey = issue({
      queuePath: null,
      propertyKey: 'yarn.scheduler.capacity.maximum-applications',
    });
    const globalOnly = issue({ queuePath: null, propertyKey: null });
    const issues = [globalKey, globalOnly, issue({})];

    expect(getGlobalIssues(issues)).toEqual([globalKey, globalOnly]);
    expect(getGlobalPropertyIssues(issues, 'maximum-applications')).toEqual([globalKey]);
    expect(getGlobalPropertyIssues(issues, 'yarn.scheduler.capacity.maximum-applications')).toEqual(
      [globalKey],
    );
  });

  it('attaches issues to the staged change that writes their key', () => {
    const own = issue({});
    const removal = issue({ propertyKey: null });
    const globalKey = issue({
      queuePath: null,
      propertyKey: 'yarn.scheduler.capacity.maximum-applications',
    });
    const issues = [own, removal, globalKey];

    expect(getStagedChangeIssues(issues, change({}))).toEqual([own]);
    expect(
      getStagedChangeIssues(issues, change({ type: 'remove', property: '__queue__' })),
    ).toEqual([removal]);
    expect(
      getStagedChangeIssues(
        issues,
        change({ queuePath: 'global', property: 'maximum-applications' }),
      ),
    ).toEqual([globalKey]);
  });

  it('derives a property name only from a key under the issue queue', () => {
    expect(getIssuePropertyName(issue({}))).toBe('capacity');
    expect(
      getIssuePropertyName(issue({ propertyKey: 'yarn.scheduler.capacity.root.b.capacity' })),
    ).toBeNull();
    expect(getIssuePropertyName(issue({ propertyKey: null }))).toBeNull();
    expect(getIssuePropertyName(issue({ queuePath: null }))).toBeNull();
  });
});

describe('explained value sources', () => {
  const explained = {
    key: 'yarn.scheduler.capacity.root.a.user-limit-factor',
    value: '2',
    source: 'PARENT' as const,
    sourceDetail: 'root',
  };

  it('indexes explain by queue path and full key', () => {
    const index = indexExplain([{ queuePath: 'root.a', properties: [explained] }]);

    expect(getExplainedProperty(index, 'root.a', 'user-limit-factor')).toEqual(explained);
    expect(getExplainedProperty(index, 'root.b', 'user-limit-factor')).toBeNull();
  });

  it('describes each source', () => {
    expect(describeValueSource({ ...explained, source: 'QUEUE' })).toBeNull();
    expect(describeValueSource(explained)).toBe('inherited from root');
    expect(describeValueSource({ ...explained, source: 'DEFAULT' })).toBe('scheduler default');
    expect(describeValueSource({ ...explained, source: 'GLOBAL', sourceDetail: 'yarn.x' })).toBe(
      'from global setting yarn.x',
    );
  });
});

describe('issue severity helpers', () => {
  it('detects blocking issues', () => {
    expect(hasBlockingIssues([issue({ severity: 'warning' })])).toBe(false);
    expect(hasBlockingIssues([issue({ severity: 'warning' }), issue({})])).toBe(true);
    expect(hasBlockingIssues([])).toBe(false);
  });

  it('splits errors and warnings', () => {
    const error = issue({});
    const warning = issue({ severity: 'warning' });

    expect(splitIssues([warning, error])).toEqual({ errors: [error], warnings: [warning] });
  });
});
