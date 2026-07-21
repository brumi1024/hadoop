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
import userEvent from '@testing-library/user-event';
import { AssistEmptyState } from '~/features/assist/components/AssistEmptyState';
import { getAssistPresentation } from '~/features/assist/presentation/assistPresentation';
import type { YarnPageContext } from '~/features/assist/types';

function ctx(overrides: Partial<YarnPageContext> = {}): YarnPageContext {
  return {
    page_kind: 'queues',
    selected_queue_path: null,
    selected_node_label: null,
    search_context: null,
    is_read_only: false,
    staged_change_count: 0,
    staged_change_summary: null,
    ...overrides,
  };
}

describe('AssistEmptyState', () => {
  // -------------------------------------------------------------------------
  // Identity, hero, lede
  // -------------------------------------------------------------------------

  it('renders the YARN Assist identity label', () => {
    const context = ctx();
    const presentation = getAssistPresentation(context);
    render(
      <AssistEmptyState
        presentation={presentation}
        context={context}
        disabled={false}
        onSend={vi.fn()}
      />,
    );
    expect(screen.getByText('YARN Assist')).toBeInTheDocument();
  });

  it('renders the leading hero text', () => {
    const context = ctx();
    const presentation = getAssistPresentation(context);
    render(
      <AssistEmptyState
        presentation={presentation}
        context={context}
        disabled={false}
        onSend={vi.fn()}
      />,
    );
    expect(screen.getByText('Capacity Scheduler ready.')).toBeInTheDocument();
  });

  it('renders the accent hero text', () => {
    const context = ctx();
    const presentation = getAssistPresentation(context);
    render(
      <AssistEmptyState
        presentation={presentation}
        context={context}
        disabled={false}
        onSend={vi.fn()}
      />,
    );
    expect(screen.getByText('Explore the queue hierarchy.')).toBeInTheDocument();
  });

  it('renders the lede text', () => {
    const context = ctx();
    const presentation = getAssistPresentation(context);
    render(
      <AssistEmptyState
        presentation={presentation}
        context={context}
        disabled={false}
        onSend={vi.fn()}
      />,
    );
    expect(
      screen.getByText(
        'Understand capacity, pressure, and configuration across the scheduler before making changes.',
      ),
    ).toBeInTheDocument();
  });

  // -------------------------------------------------------------------------
  // Scope chips
  // -------------------------------------------------------------------------

  it('renders Page chip for known page kind', () => {
    const context = ctx({ page_kind: 'queues' });
    const presentation = getAssistPresentation(context);
    render(
      <AssistEmptyState
        presentation={presentation}
        context={context}
        disabled={false}
        onSend={vi.fn()}
      />,
    );
    expect(screen.getByText('Page')).toBeInTheDocument();
    expect(screen.getByText('Queues')).toBeInTheDocument();
  });

  it('renders Queue chip when a queue is selected', () => {
    const context = ctx({ selected_queue_path: 'root.analytics' });
    const presentation = getAssistPresentation(context);
    render(
      <AssistEmptyState
        presentation={presentation}
        context={context}
        disabled={false}
        onSend={vi.fn()}
      />,
    );
    expect(screen.getByText('Queue')).toBeInTheDocument();
    expect(screen.getByText('root.analytics')).toBeInTheDocument();
  });

  it('renders Read-only Mode chip when is_read_only', () => {
    const context = ctx({ is_read_only: true });
    const presentation = getAssistPresentation(context);
    render(
      <AssistEmptyState
        presentation={presentation}
        context={context}
        disabled={false}
        onSend={vi.fn()}
      />,
    );
    expect(screen.getByText('Mode')).toBeInTheDocument();
    expect(screen.getByText('Read-only')).toBeInTheDocument();
  });

  it('renders Staged chip when there are staged changes', () => {
    const context = ctx({ staged_change_count: 3 });
    const presentation = getAssistPresentation(context);
    render(
      <AssistEmptyState
        presentation={presentation}
        context={context}
        disabled={false}
        onSend={vi.fn()}
      />,
    );
    expect(screen.getByText('Staged')).toBeInTheDocument();
    expect(screen.getByText('3')).toBeInTheDocument();
  });

  it('renders Partition chip on queues page with node label filter', () => {
    const context = ctx({ selected_node_label: 'gpu' });
    const presentation = getAssistPresentation(context);
    render(
      <AssistEmptyState
        presentation={presentation}
        context={context}
        disabled={false}
        onSend={vi.fn()}
      />,
    );
    expect(screen.getByText('Partition')).toBeInTheDocument();
    expect(screen.getByText('gpu')).toBeInTheDocument();
  });

  // -------------------------------------------------------------------------
  // Prompt rendering
  // -------------------------------------------------------------------------

  it('renders the recommended workflow card', () => {
    const context = ctx();
    const presentation = getAssistPresentation(context);
    render(
      <AssistEmptyState
        presentation={presentation}
        context={context}
        disabled={false}
        onSend={vi.fn()}
      />,
    );
    expect(screen.getByTestId('recommended-prompt-card')).toBeInTheDocument();
    expect(screen.getByText('Explain the queue hierarchy')).toBeInTheDocument();
    expect(screen.getByText('Recommended')).toBeInTheDocument();
  });

  it('renders three compact workflow rows', () => {
    const context = ctx();
    const presentation = getAssistPresentation(context);
    render(
      <AssistEmptyState
        presentation={presentation}
        context={context}
        disabled={false}
        onSend={vi.fn()}
      />,
    );
    const rows = screen.getAllByTestId('compact-prompt-row');
    expect(rows).toHaveLength(3);
  });

  // -------------------------------------------------------------------------
  // Send behavior
  // -------------------------------------------------------------------------

  it('clicking the recommended card calls onSend with the exact prompt', () => {
    const onSend = vi.fn();
    const context = ctx();
    const presentation = getAssistPresentation(context);
    render(
      <AssistEmptyState
        presentation={presentation}
        context={context}
        disabled={false}
        onSend={onSend}
      />,
    );
    const card = screen.getByTestId('recommended-prompt-card');
    fireEvent.click(card);
    expect(onSend).toHaveBeenCalledOnce();
    expect(onSend).toHaveBeenCalledWith(presentation.prompts[0]?.prompt);
  });

  it('clicking a compact row calls onSend with its exact prompt', () => {
    const onSend = vi.fn();
    const context = ctx();
    const presentation = getAssistPresentation(context);
    render(
      <AssistEmptyState
        presentation={presentation}
        context={context}
        disabled={false}
        onSend={onSend}
      />,
    );
    const rows = screen.getAllByTestId('compact-prompt-row');
    fireEvent.click(rows[0]!);
    expect(onSend).toHaveBeenCalledOnce();
    expect(onSend).toHaveBeenCalledWith(presentation.prompts[1]?.prompt);
  });

  it('Enter key on the recommended card calls onSend', async () => {
    const onSend = vi.fn();
    const user = userEvent.setup();
    const context = ctx();
    const presentation = getAssistPresentation(context);
    render(
      <AssistEmptyState
        presentation={presentation}
        context={context}
        disabled={false}
        onSend={onSend}
      />,
    );
    const card = screen.getByTestId('recommended-prompt-card');
    card.focus();
    await user.keyboard('{Enter}');
    expect(onSend).toHaveBeenCalledOnce();
  });

  it('Space key on a compact row calls onSend', async () => {
    const onSend = vi.fn();
    const user = userEvent.setup();
    const context = ctx();
    const presentation = getAssistPresentation(context);
    render(
      <AssistEmptyState
        presentation={presentation}
        context={context}
        disabled={false}
        onSend={onSend}
      />,
    );
    const rows = screen.getAllByTestId('compact-prompt-row');
    rows[1]!.focus();
    await user.keyboard(' ');
    expect(onSend).toHaveBeenCalledOnce();
  });

  // -------------------------------------------------------------------------
  // Disabled state
  // -------------------------------------------------------------------------

  it('does not call onSend when disabled and card is clicked', () => {
    const onSend = vi.fn();
    const context = ctx();
    const presentation = getAssistPresentation(context);
    render(
      <AssistEmptyState
        presentation={presentation}
        context={context}
        disabled={true}
        onSend={onSend}
      />,
    );
    const card = screen.getByTestId('recommended-prompt-card');
    fireEvent.click(card);
    expect(onSend).not.toHaveBeenCalled();
  });

  it('does not call onSend when disabled and a compact row is clicked', () => {
    const onSend = vi.fn();
    const context = ctx();
    const presentation = getAssistPresentation(context);
    render(
      <AssistEmptyState
        presentation={presentation}
        context={context}
        disabled={true}
        onSend={onSend}
      />,
    );
    const rows = screen.getAllByTestId('compact-prompt-row');
    fireEvent.click(rows[0]!);
    expect(onSend).not.toHaveBeenCalled();
  });

  // -------------------------------------------------------------------------
  // Contextual prompt substitution
  // -------------------------------------------------------------------------

  it('substitutes queue path into the recommended card prompt', () => {
    const onSend = vi.fn();
    const context = ctx({ selected_queue_path: 'root.etl' });
    const presentation = getAssistPresentation(context);
    render(
      <AssistEmptyState
        presentation={presentation}
        context={context}
        disabled={false}
        onSend={onSend}
      />,
    );
    const card = screen.getByTestId('recommended-prompt-card');
    fireEvent.click(card);
    expect(onSend).toHaveBeenCalledWith(expect.stringContaining('root.etl'));
    expect(onSend).not.toHaveBeenCalledWith(expect.stringContaining('<queue path>'));
  });
});
