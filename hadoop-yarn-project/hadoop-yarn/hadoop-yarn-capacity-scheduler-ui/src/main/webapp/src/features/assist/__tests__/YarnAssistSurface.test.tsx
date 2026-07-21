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

import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen } from '@testing-library/react';
import { YarnAssistSurface } from '~/features/assist/YarnAssist';

// Mock useIsMobile to control desktop vs mobile presentation
vi.mock('~/hooks/use-mobile', () => ({
  useIsMobile: vi.fn(() => false),
}));

// Mock AssistPanel to avoid complex chat hook setup
vi.mock('~/features/assist/components/AssistPanel', () => ({
  AssistPanel: ({ containerRole }: { containerRole?: string }) => (
    <div data-testid="assist-panel" data-container-role={containerRole}>
      AssistPanel
    </div>
  ),
}));

import { useIsMobile } from '~/hooks/use-mobile';

describe('YarnAssistSurface - desktop (md+ breakpoint)', () => {
  beforeEach(() => {
    vi.mocked(useIsMobile).mockReturnValue(false);
  });

  it('renders nothing when open is false', () => {
    const { container } = render(<YarnAssistSurface open={false} onClose={vi.fn()} />);
    expect(container.firstChild).toBeNull();
  });

  it('renders a semantic aside element when open', () => {
    render(<YarnAssistSurface open={true} onClose={vi.fn()} />);
    const aside = screen.getByRole('complementary', { name: 'YARN AI Assist' });
    expect(aside).toBeInTheDocument();
  });

  it('does not render an overlay element on desktop', () => {
    render(<YarnAssistSurface open={true} onClose={vi.fn()} />);
    // No fixed inset overlay - only the aside
    const aside = screen.getByRole('complementary', { name: 'YARN AI Assist' });
    expect(aside.tagName).toBe('ASIDE');
    expect(document.querySelector('[class*="fixed inset-0"]')).toBeNull();
  });

  it('passes complementary containerRole to AssistPanel', () => {
    render(<YarnAssistSurface open={true} onClose={vi.fn()} />);
    const panel = screen.getByTestId('assist-panel');
    expect(panel).toHaveAttribute('data-container-role', 'complementary');
  });

  it('has the accessible label YARN AI Assist', () => {
    render(<YarnAssistSurface open={true} onClose={vi.fn()} />);
    expect(screen.getByRole('complementary', { name: 'YARN AI Assist' })).toBeInTheDocument();
  });
});

describe('YarnAssistSurface - mobile (below md breakpoint)', () => {
  beforeEach(() => {
    vi.mocked(useIsMobile).mockReturnValue(true);
  });

  it('renders nothing when open is false', () => {
    const { container } = render(<YarnAssistSurface open={false} onClose={vi.fn()} />);
    expect(container.firstChild).toBeNull();
  });

  it('renders the drawer presentation when open', () => {
    render(<YarnAssistSurface open={true} onClose={vi.fn()} />);
    // Vaul drawer renders its content; AssistPanel should be present
    expect(screen.getByTestId('assist-panel')).toBeInTheDocument();
  });

  it('does not render a static aside element on mobile', () => {
    render(<YarnAssistSurface open={true} onClose={vi.fn()} />);
    expect(screen.queryByRole('complementary', { name: 'YARN AI Assist' })).toBeNull();
  });

  it('passes region containerRole to AssistPanel in drawer', () => {
    render(<YarnAssistSurface open={true} onClose={vi.fn()} />);
    const panel = screen.getByTestId('assist-panel');
    expect(panel).toHaveAttribute('data-container-role', 'region');
  });
});
