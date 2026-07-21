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
import { render, screen, waitFor } from '@testing-library/react';
import { StrictMode } from 'react';
import type { UIMessage } from 'ai';

// Mock the chat hook to control messages
vi.mock('@ai-sdk/react', () => ({
  useChat: vi.fn(() => mockChatResult()),
}));

// Mock the API client
vi.mock('~/features/assist/api/client', () => ({
  assistApi: {
    listConversations: vi.fn().mockResolvedValue({ conversations: [] }),
    createConversation: vi.fn().mockResolvedValue({ conversation_id: 'test-conv-1' }),
    deleteConversation: vi.fn().mockResolvedValue(undefined),
  },
  CHAT_STREAM_URL: '/yarn-assist/v1/chat',
}));

// Mock DefaultChatTransport
vi.mock('ai', () => ({
  DefaultChatTransport: vi.fn().mockImplementation(() => ({})),
}));

// Mock useYarnContext
vi.mock('~/features/assist/hooks/useYarnContext', () => ({
  useYarnContext: vi.fn(() => ({
    page_kind: 'queues',
    selected_queue_path: null,
    selected_node_label: null,
    search_context: null,
    is_read_only: false,
    staged_change_count: 0,
    staged_change_summary: null,
  })),
}));

// Mock MessageScroller components to track props
vi.mock('~/components/ui/message-scroller', () => ({
  MessageScrollerProvider: ({ children, ...props }: Record<string, unknown>) => (
    <div data-testid="message-scroller-provider" data-auto-scroll={String(props.autoScroll)}>
      {children as React.ReactNode}
    </div>
  ),
  MessageScroller: ({ children }: { children: React.ReactNode }) => (
    <div data-testid="message-scroller">{children}</div>
  ),
  MessageScrollerViewport: ({ children }: { children: React.ReactNode }) => (
    <div data-testid="message-scroller-viewport">{children}</div>
  ),
  MessageScrollerContent: ({ children }: { children: React.ReactNode }) => (
    <div data-testid="message-scroller-content">{children}</div>
  ),
  MessageScrollerItem: ({
    children,
    messageId,
    scrollAnchor,
  }: {
    children: React.ReactNode;
    messageId: string;
    scrollAnchor: boolean;
  }) => (
    <div
      data-testid="message-scroller-item"
      data-message-id={messageId}
      data-scroll-anchor={String(scrollAnchor)}
    >
      {children}
    </div>
  ),
  MessageScrollerButton: () => (
    <button data-testid="message-scroller-button">Jump to latest</button>
  ),
}));

import { useChat } from '@ai-sdk/react';
import { assistApi } from '~/features/assist/api/client';
import { AssistPanel } from '~/features/assist/components/AssistPanel';

function makeMessage(id: string, role: 'user' | 'assistant', text: string): UIMessage {
  return {
    id,
    role,
    parts: [{ type: 'text', text }],
  };
}

function mockChatResult(
  overrides: {
    messages?: UIMessage[];
    status?: 'ready' | 'streaming' | 'submitted' | 'error';
    error?: Error;
  } = {},
) {
  return {
    id: 'test-conv',
    messages: overrides.messages ?? ([] as UIMessage[]),
    sendMessage: vi.fn(),
    status: overrides.status ?? ('ready' as const),
    error: overrides.error,
    stop: vi.fn(),
    setMessages: vi.fn(),
    regenerate: vi.fn(),
    resumeStream: vi.fn(),
    addToolResult: vi.fn(),
    addToolOutput: vi.fn(),
    addToolApprovalResponse: vi.fn(),
    clearError: vi.fn(),
    input: '',
    handleInputChange: vi.fn(),
    handleSubmit: vi.fn(),
  } as unknown as ReturnType<typeof useChat>;
}

function mockSettledAssistInitialization() {
  vi.mocked(assistApi.listConversations).mockResolvedValue({ conversations: [] });
  vi.mocked(assistApi.createConversation).mockResolvedValue({
    conversation_id: 'test-conv-1',
  });
}

beforeEach(() => {
  vi.clearAllMocks();
  vi.mocked(assistApi.listConversations).mockImplementation(
    () => new Promise<{ conversations: [] }>(() => {}),
  );
  vi.mocked(assistApi.createConversation).mockImplementation(
    () => new Promise<{ conversation_id: string }>(() => {}),
  );
});

describe('AssistPanel - initialization', () => {
  beforeEach(() => {
    vi.mocked(useChat).mockReturnValue(mockChatResult());
    mockSettledAssistInitialization();
  });

  it('coalesces conversation initialization in Strict Mode', async () => {
    render(
      <StrictMode>
        <AssistPanel onClose={vi.fn()} />
      </StrictMode>,
    );

    await waitFor(() => {
      expect(assistApi.createConversation).toHaveBeenCalledOnce();
      expect(assistApi.listConversations).toHaveBeenCalledOnce();
    });
    expect(vi.mocked(assistApi.createConversation).mock.invocationCallOrder[0]).toBeLessThan(
      vi.mocked(assistApi.listConversations).mock.invocationCallOrder[0],
    );
  });
});

describe('AssistPanel - scroll behavior', () => {
  beforeEach(() => {
    vi.mocked(useChat).mockReturnValue(mockChatResult());
  });

  it('with no messages, does not show MessageScrollerProvider once settled', async () => {
    mockSettledAssistInitialization();
    render(<AssistPanel onClose={vi.fn()} />);
    // Wait for async conversation creation to settle
    await waitFor(() => {
      expect(screen.queryByTestId('message-scroller-provider')).toBeNull();
    });
  });

  it('with messages, uses MessageScrollerProvider instead of forced scrollTop', () => {
    vi.mocked(useChat).mockReturnValue(
      mockChatResult({ messages: [makeMessage('m1', 'user', 'Hi')] }),
    );
    render(<AssistPanel onClose={vi.fn()} />);
    expect(screen.getByTestId('message-scroller-provider')).toBeInTheDocument();
  });

  it('does not render MessageScroller in empty state once settled', async () => {
    mockSettledAssistInitialization();
    render(<AssistPanel onClose={vi.fn()} />);
    await waitFor(() => {
      expect(screen.queryByTestId('message-scroller')).toBeNull();
    });
  });

  it('renders MessageScrollerProvider with autoScroll when messages are present', () => {
    vi.mocked(useChat).mockReturnValue(
      mockChatResult({ messages: [makeMessage('msg-1', 'user', 'Hello')] }),
    );
    render(<AssistPanel onClose={vi.fn()} />);
    const provider = screen.getByTestId('message-scroller-provider');
    expect(provider).toHaveAttribute('data-auto-scroll', 'true');
  });
});

describe('AssistPanel - message anchoring', () => {
  it('marks user messages as scroll anchors', () => {
    vi.mocked(useChat).mockReturnValue(
      mockChatResult({ messages: [makeMessage('user-msg-1', 'user', 'Hello')] }),
    );
    render(<AssistPanel onClose={vi.fn()} />);
    const items = screen.getAllByTestId('message-scroller-item');
    const userItem = items.find((el) => el.getAttribute('data-message-id') === 'user-msg-1');
    expect(userItem).toHaveAttribute('data-scroll-anchor', 'true');
  });

  it('does not mark assistant messages as scroll anchors', () => {
    vi.mocked(useChat).mockReturnValue(
      mockChatResult({ messages: [makeMessage('asst-msg-1', 'assistant', 'Reply')] }),
    );
    render(<AssistPanel onClose={vi.fn()} />);
    const items = screen.getAllByTestId('message-scroller-item');
    const asstItem = items.find((el) => el.getAttribute('data-message-id') === 'asst-msg-1');
    expect(asstItem).toHaveAttribute('data-scroll-anchor', 'false');
  });

  it('uses message.id as the messageId for scroll items', () => {
    vi.mocked(useChat).mockReturnValue(
      mockChatResult({
        messages: [
          makeMessage('stable-id-abc', 'user', 'Hello'),
          makeMessage('stable-id-def', 'assistant', 'World'),
        ],
      }),
    );
    render(<AssistPanel onClose={vi.fn()} />);
    const items = screen.getAllByTestId('message-scroller-item');
    const ids = items.map((el) => el.getAttribute('data-message-id'));
    expect(ids).toContain('stable-id-abc');
    expect(ids).toContain('stable-id-def');
  });

  it('does not mark the thinking indicator as a scroll anchor', () => {
    vi.mocked(useChat).mockReturnValue(
      mockChatResult({
        messages: [makeMessage('user-msg-1', 'user', 'Hello')],
        status: 'streaming',
      }),
    );
    render(<AssistPanel onClose={vi.fn()} />);
    const items = screen.getAllByTestId('message-scroller-item');
    const thinkingItem = items.find((el) =>
      el.getAttribute('data-message-id')?.includes('-thinking'),
    );
    if (thinkingItem) {
      expect(thinkingItem).toHaveAttribute('data-scroll-anchor', 'false');
    }
  });

  it('does not mark error rows as scroll anchors', () => {
    vi.mocked(useChat).mockReturnValue(
      mockChatResult({
        messages: [makeMessage('user-msg-1', 'user', 'Hello')],
        error: new Error('Something went wrong'),
      }),
    );
    render(<AssistPanel onClose={vi.fn()} />);
    const items = screen.getAllByTestId('message-scroller-item');
    const errorItem = items.find((el) => el.getAttribute('data-message-id')?.includes('-error'));
    if (errorItem) {
      expect(errorItem).toHaveAttribute('data-scroll-anchor', 'false');
    }
  });
});

describe('AssistPanel - composer placement', () => {
  it('renders the composer outside the message scroller viewport', () => {
    vi.mocked(useChat).mockReturnValue(
      mockChatResult({ messages: [makeMessage('msg-1', 'user', 'Hello')] }),
    );
    render(<AssistPanel onClose={vi.fn()} />);
    const viewport = screen.getByTestId('message-scroller-viewport');
    const composer = screen.getByLabelText('Message input');
    // Composer must not be a descendant of the viewport
    expect(viewport.contains(composer)).toBe(false);
  });

  it('renders the composer when there are no messages', () => {
    vi.mocked(useChat).mockReturnValue(mockChatResult());
    render(<AssistPanel onClose={vi.fn()} />);
    expect(screen.getByLabelText('Message input')).toBeInTheDocument();
  });
});

describe('AssistPanel - semantic role', () => {
  beforeEach(() => {
    vi.mocked(useChat).mockReturnValue(mockChatResult());
  });

  it('uses complementary role by default', () => {
    render(<AssistPanel onClose={vi.fn()} />);
    expect(screen.getByRole('complementary', { name: 'YARN AI Assist' })).toBeInTheDocument();
  });

  it('uses region role when containerRole is region', () => {
    render(<AssistPanel onClose={vi.fn()} containerRole="region" />);
    expect(screen.getByRole('region', { name: 'YARN AI Assist' })).toBeInTheDocument();
  });
});

describe('AssistPanel - existing behavior preserved', () => {
  beforeEach(() => {
    vi.mocked(useChat).mockReturnValue(mockChatResult());
  });

  it('renders the close button in the header', () => {
    render(<AssistPanel onClose={vi.fn()} />);
    expect(screen.getByRole('button', { name: 'Close YARN Assist' })).toBeInTheDocument();
  });

  it('calls onClose when close button is clicked', () => {
    const onClose = vi.fn();
    render(<AssistPanel onClose={onClose} />);
    screen.getByRole('button', { name: 'Close YARN Assist' }).click();
    expect(onClose).toHaveBeenCalledOnce();
  });

  it('renders new conversation button', () => {
    render(<AssistPanel onClose={vi.fn()} />);
    expect(screen.getByRole('button', { name: 'New conversation' })).toBeInTheDocument();
  });

  it('renders the history toggle button', () => {
    render(<AssistPanel onClose={vi.fn()} />);
    expect(screen.getByRole('button', { name: 'Open conversation history' })).toBeInTheDocument();
  });
});
