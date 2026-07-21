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

import { describe, it, expect, vi } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import { YarnAssistLauncher } from '~/features/assist/YarnAssist';

describe('YarnAssistLauncher', () => {
  it('renders the Open AI Assist button', () => {
    render(<YarnAssistLauncher open={false} onToggle={vi.fn()} />);
    expect(screen.getByRole('button', { name: 'Open AI Assist' })).toBeInTheDocument();
  });

  it('calls onToggle when clicked while closed', () => {
    const onToggle = vi.fn();
    render(<YarnAssistLauncher open={false} onToggle={onToggle} />);
    fireEvent.click(screen.getByRole('button', { name: 'Open AI Assist' }));
    expect(onToggle).toHaveBeenCalledOnce();
  });

  it('calls onToggle when clicked while open', () => {
    const onToggle = vi.fn();
    render(<YarnAssistLauncher open onToggle={onToggle} />);
    fireEvent.click(screen.getByRole('button', { name: 'Close AI Assist' }));
    expect(onToggle).toHaveBeenCalledOnce();
  });

  it('reflects open state in its accessible attributes', () => {
    const { rerender } = render(<YarnAssistLauncher open={false} onToggle={vi.fn()} />);
    expect(screen.getByRole('button', { name: 'Open AI Assist' })).toHaveAttribute(
      'aria-expanded',
      'false',
    );
    rerender(<YarnAssistLauncher open onToggle={vi.fn()} />);
    expect(screen.getByRole('button', { name: 'Close AI Assist' })).toHaveAttribute(
      'aria-expanded',
      'true',
    );
  });
});
