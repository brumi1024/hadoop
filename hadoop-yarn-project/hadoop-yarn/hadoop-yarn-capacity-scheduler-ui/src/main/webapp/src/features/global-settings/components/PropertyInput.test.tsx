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

import { describe, it, expect, beforeEach, vi } from 'vitest';
import { render, screen } from '~/testing/setup/setup';
import { PropertyInput } from './PropertyInput';
import { useSchedulerStore } from '~/stores/schedulerStore';
import type { PropertyDescriptor } from '~/types/property-descriptor';
import type { ValidationIssue } from '~/types';

const property: PropertyDescriptor = {
  name: 'yarn.scheduler.capacity.maximum-applications',
  displayName: 'Maximum Applications',
  description: 'Maximum applications in the cluster',
  type: 'number',
  category: 'application-limits',
  defaultValue: '10000',
  required: false,
};

const issue = (overrides: Partial<ValidationIssue>): ValidationIssue => ({
  queuePath: null,
  propertyKey: 'yarn.scheduler.capacity.maximum-applications',
  ruleId: 'invalid-number',
  severity: 'error',
  message: 'Must be a non-negative integer',
  ...overrides,
});

describe('PropertyInput server issues', () => {
  beforeEach(() => {
    useSchedulerStore.setState({ serverIssues: [] });
  });

  it('renders errors and warnings reported for the global key', () => {
    useSchedulerStore.setState({
      serverIssues: [
        issue({}),
        issue({ severity: 'warning', ruleId: 'large-limit', message: 'Unusually large limit' }),
      ],
    });

    render(<PropertyInput property={property} value="-5" isStaged={true} onChange={vi.fn()} />);

    expect(screen.getByText('Must be a non-negative integer')).toBeInTheDocument();
    expect(screen.getByText('Unusually large limit')).toBeInTheDocument();
  });

  it('ignores queue issues and other keys', () => {
    useSchedulerStore.setState({
      serverIssues: [
        issue({
          queuePath: 'root.a',
          propertyKey: 'yarn.scheduler.capacity.root.a.maximum-applications',
          message: 'Queue issue',
        }),
        issue({ propertyKey: 'yarn.scheduler.capacity.node-locality-delay', message: 'Other' }),
      ],
    });

    render(<PropertyInput property={property} value="5" isStaged={false} onChange={vi.fn()} />);

    expect(screen.queryByText('Queue issue')).not.toBeInTheDocument();
    expect(screen.queryByText('Other')).not.toBeInTheDocument();
  });
});
