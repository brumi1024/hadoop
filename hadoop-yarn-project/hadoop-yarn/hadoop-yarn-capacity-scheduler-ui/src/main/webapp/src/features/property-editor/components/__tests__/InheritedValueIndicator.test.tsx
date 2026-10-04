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
import { render, screen } from '~/testing/setup/setup';
import { InheritedValueIndicator } from '../PropertyFieldHelpers';
import type { ExplainedProperty } from '~/types';

const explained = (overrides: Partial<ExplainedProperty>): ExplainedProperty => ({
  key: 'yarn.scheduler.capacity.root.a.user-limit-factor',
  value: '2',
  source: 'PARENT',
  sourceDetail: 'root',
  ...overrides,
});

describe('InheritedValueIndicator', () => {
  it('renders nothing without an explained value', () => {
    const { container } = render(<InheritedValueIndicator explained={null} />);
    expect(container).toBeEmptyDOMElement();
  });

  it('renders nothing when the queue sets the value itself', () => {
    const { container } = render(
      <InheritedValueIndicator explained={explained({ source: 'QUEUE', sourceDetail: null })} />,
    );
    expect(container).toBeEmptyDOMElement();
  });

  it('shows the parent queue a value is inherited from', () => {
    render(<InheritedValueIndicator explained={explained({})} />);
    expect(screen.getByText('2')).toBeInTheDocument();
    expect(screen.getByText(/inherited from root/i)).toBeInTheDocument();
  });

  it('shows the global key a value comes from', () => {
    render(
      <InheritedValueIndicator
        explained={explained({
          source: 'GLOBAL',
          sourceDetail: 'yarn.scheduler.capacity.user-limit-factor',
        })}
      />,
    );
    expect(
      screen.getByText(/from global setting yarn\.scheduler\.capacity\.user-limit-factor/i),
    ).toBeInTheDocument();
  });

  it('labels template, default and derived sources', () => {
    const { rerender } = render(
      <InheritedValueIndicator
        explained={explained({
          source: 'TEMPLATE_V2',
          sourceDetail: 'yarn.scheduler.capacity.root.auto-queue-creation-v2.template.capacity',
        })}
      />,
    );
    expect(screen.getByText(/flexible auto-creation template/i)).toBeInTheDocument();

    rerender(<InheritedValueIndicator explained={explained({ source: 'DEFAULT' })} />);
    expect(screen.getByText(/scheduler default/i)).toBeInTheDocument();

    rerender(
      <InheritedValueIndicator
        explained={explained({ source: 'DERIVED', sourceDetail: 'capacity' })}
      />,
    );
    expect(screen.getByText(/derived from capacity/i)).toBeInTheDocument();
  });
});
