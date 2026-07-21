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

import { useCallback, useEffect, useRef, useState } from 'react';
import { useChat } from '@ai-sdk/react';
import { DefaultChatTransport } from 'ai';
import { AlertCircle, Loader2, X } from 'lucide-react';
import { Button } from '~/components/ui/button';
import { ScrollArea } from '~/components/ui/scroll-area';
import {
  MessageScrollerProvider,
  MessageScroller,
  MessageScrollerViewport,
  MessageScrollerContent,
  MessageScrollerItem,
  MessageScrollerButton,
} from '~/components/ui/message-scroller';
import { assistApi, CHAT_STREAM_URL } from '~/features/assist/api/client';
import { AssistComposer } from '~/features/assist/components/AssistComposer';
import { AssistEmptyState } from '~/features/assist/components/AssistEmptyState';
import { AssistHeader } from '~/features/assist/components/AssistHeader';
import { ChatMessage } from '~/features/assist/components/ChatMessage';
import { useYarnContext } from '~/features/assist/hooks/useYarnContext';
import { getAssistPresentation } from '~/features/assist/presentation/assistPresentation';
import type { ConversationSummary } from '~/features/assist/types';

interface AssistPanelProps {
  onClose: () => void;
  /** ARIA role for the panel container. Defaults to 'complementary' for desktop aside. */
  containerRole?: 'complementary' | 'region';
}

export function AssistPanel({ onClose, containerRole = 'complementary' }: AssistPanelProps) {
  const yarnContext = useYarnContext();
  const yarnContextRef = useRef(yarnContext);
  yarnContextRef.current = yarnContext;

  const [conversationId, setConversationId] = useState<string | null>(null);
  const [conversations, setConversations] = useState<ConversationSummary[]>([]);
  const [showHistory, setShowHistory] = useState(false);
  const [isCreatingConversation, setIsCreatingConversation] = useState(false);
  const [inputValue, setInputValue] = useState('');
  const conversationListRequestRef = useRef<Promise<void> | null>(null);
  const conversationCreationRequestRef = useRef<Promise<void> | null>(null);
  const conversationInitializationRequestRef = useRef<Promise<void> | null>(null);
  const hasInitializedConversationsRef = useRef(false);

  const transport = useRef(new DefaultChatTransport({ api: CHAT_STREAM_URL })).current;

  const { messages, sendMessage, status, error, stop, setMessages } = useChat({
    id: conversationId ?? undefined,
    transport,
  });

  const isStreaming = status === 'streaming' || status === 'submitted';
  const hasMessages = messages.length > 0;

  const presentation = getAssistPresentation(yarnContext);

  // Load conversation list
  const loadConversations = useCallback(async () => {
    if (conversationListRequestRef.current) {
      await conversationListRequestRef.current;
      return;
    }

    const request = (async () => {
      try {
        const data = await assistApi.listConversations();
        setConversations(data.conversations);
      } catch {
        // Assist may be unavailable (static mode starts without conversations)
      }
    })();
    conversationListRequestRef.current = request;

    try {
      await request;
    } finally {
      if (conversationListRequestRef.current === request) {
        conversationListRequestRef.current = null;
      }
    }
  }, []);

  // Create new conversation
  const createConversation = useCallback(async () => {
    if (conversationCreationRequestRef.current) {
      await conversationCreationRequestRef.current;
      return;
    }

    const request = (async () => {
      setIsCreatingConversation(true);
      try {
        const result = await assistApi.createConversation(yarnContextRef.current);
        setConversationId(result.conversation_id);
        setMessages([]);
        setShowHistory(false);
      } catch {
        // Assist unavailable - use a client-side-only conversation without an ID
      } finally {
        setIsCreatingConversation(false);
      }
    })();
    conversationCreationRequestRef.current = request;

    try {
      await request;
    } finally {
      if (conversationCreationRequestRef.current === request) {
        conversationCreationRequestRef.current = null;
      }
    }
  }, [setMessages]);

  useEffect(() => {
    if (hasInitializedConversationsRef.current || conversationInitializationRequestRef.current) {
      return;
    }

    const request = (async () => {
      if (!conversationId) {
        await createConversation();
      }
      await loadConversations();
    })();
    conversationInitializationRequestRef.current = request;

    const finishInitialization = () => {
      hasInitializedConversationsRef.current = true;
      if (conversationInitializationRequestRef.current === request) {
        conversationInitializationRequestRef.current = null;
      }
    };
    void request.then(finishInitialization, finishInitialization);
  }, [conversationId, createConversation, loadConversations]);

  // Escape closes the panel, but only when no modal dialog is in the foreground
  useEffect(() => {
    function handleKeyDown(e: KeyboardEvent) {
      if (e.key !== 'Escape') return;
      // Don't steal Escape from an open Radix dialog/alertdialog/drawer
      if (document.querySelector('[role="dialog"][data-state="open"]')) return;
      onClose();
    }
    window.addEventListener('keydown', handleKeyDown);
    return () => window.removeEventListener('keydown', handleKeyDown);
  }, [onClose]);

  function handleSend() {
    const text = inputValue.trim();
    if (!text || isStreaming) return;
    setInputValue('');
    sendMessage({ text });
  }

  function handleWorkflowSend(prompt: string) {
    if (isStreaming) return;
    sendMessage({ text: prompt });
  }

  async function handleSwitchConversation(conv: ConversationSummary) {
    setConversationId(conv.conversation_id);
    setMessages([]);
    setShowHistory(false);
  }

  async function handleDeleteConversation(convId: string) {
    try {
      await assistApi.deleteConversation(convId);
      setConversations((prev) => prev.filter((c) => c.conversation_id !== convId));
      if (convId === conversationId) {
        await handleNewConversation();
      }
    } catch {
      // ignore
    }
  }

  async function handleNewConversation() {
    await createConversation();
    await loadConversations();
  }

  // Stable ID for the thinking indicator row (scoped to conversation)
  const thinkingId = `${conversationId ?? 'local'}-thinking`;
  // Stable ID for the error row
  const errorId = `${conversationId ?? 'local'}-error`;

  return (
    <div
      className="flex flex-col h-full bg-background"
      role={containerRole}
      aria-label="YARN AI Assist"
    >
      {/* Header */}
      <AssistHeader
        historyOpen={showHistory}
        isStreaming={isStreaming}
        onToggleHistory={() => setShowHistory((v) => !v)}
        onNewConversation={() => void handleNewConversation()}
        onClose={onClose}
      />

      {/* Conversation history panel */}
      {showHistory && (
        <div className="border-b shrink-0">
          <div className="px-3 py-2 flex items-center justify-between">
            <span className="text-xs font-medium text-muted-foreground uppercase tracking-wide">
              Conversations
            </span>
          </div>
          <ScrollArea className="max-h-40">
            {conversations.length === 0 ? (
              <p className="px-3 pb-2 text-xs text-muted-foreground">No conversations yet</p>
            ) : (
              conversations.map((conv) => (
                <div
                  key={conv.conversation_id}
                  className="flex items-center gap-1 px-3 py-1 hover:bg-muted/50 group"
                >
                  <button
                    className="flex-1 text-left text-xs truncate"
                    onClick={() => void handleSwitchConversation(conv)}
                  >
                    {conv.title}
                  </button>
                  <Button
                    variant="ghost"
                    size="icon"
                    className="h-4 w-4 opacity-0 group-hover:opacity-100"
                    onClick={() => void handleDeleteConversation(conv.conversation_id)}
                    aria-label="Delete conversation"
                  >
                    <X className="h-2.5 w-2.5" />
                  </Button>
                </div>
              ))
            )}
          </ScrollArea>
        </div>
      )}

      {/* Transcript region */}
      {!hasMessages && !isCreatingConversation ? (
        // Empty state - single height-constrained region, no nested scroller
        <div className="flex-1 overflow-y-auto">
          <AssistEmptyState
            presentation={presentation}
            context={yarnContext}
            disabled={isStreaming}
            onSend={handleWorkflowSend}
          />
        </div>
      ) : (
        // Non-empty transcript - MessageScroller owns scrolling
        <MessageScrollerProvider
          autoScroll
          defaultScrollPosition="last-anchor"
          scrollPreviousItemPeek={48}
          key={conversationId ?? 'local'}
        >
          <MessageScroller className="flex-1 min-h-0">
            <MessageScrollerViewport>
              <MessageScrollerContent className="px-3 py-3 gap-4">
                {messages.map((msg) => (
                  <MessageScrollerItem
                    key={msg.id}
                    messageId={msg.id}
                    scrollAnchor={msg.role === 'user'}
                  >
                    <ChatMessage message={msg} />
                  </MessageScrollerItem>
                ))}

                {/* Thinking indicator - non-anchor */}
                {isStreaming && messages[messages.length - 1]?.role === 'user' && (
                  <MessageScrollerItem messageId={thinkingId} scrollAnchor={false}>
                    <div className="flex items-center gap-2 py-1">
                      <span className="inline-block w-1.5 h-1.5 rounded-full bg-primary shrink-0" />
                      <span className="font-mono text-[11px] text-muted-foreground tracking-widest uppercase">
                        YARN Assist
                      </span>
                      <Loader2 className="h-3 w-3 animate-spin text-muted-foreground ml-0.5" />
                      <span className="text-xs text-muted-foreground">Thinking...</span>
                    </div>
                  </MessageScrollerItem>
                )}

                {/* Error - non-anchor */}
                {error && (
                  <MessageScrollerItem messageId={errorId} scrollAnchor={false}>
                    <div className="flex items-start gap-2 rounded-md border border-destructive/30 bg-destructive/10 p-2.5 text-xs text-destructive">
                      <AlertCircle className="h-3.5 w-3.5 mt-0.5 shrink-0" />
                      <span>{error.message}</span>
                    </div>
                  </MessageScrollerItem>
                )}
              </MessageScrollerContent>
            </MessageScrollerViewport>
            <MessageScrollerButton />
          </MessageScroller>
        </MessageScrollerProvider>
      )}

      {/* Composer - always outside the transcript viewport */}
      <AssistComposer
        value={inputValue}
        isStreaming={isStreaming}
        hasMessages={hasMessages}
        placeholder={presentation.placeholder}
        onChange={setInputValue}
        onSend={handleSend}
        onStop={stop}
      />
    </div>
  );
}
