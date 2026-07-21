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

import type {
  AssistCapabilities,
  ConversationSummary,
  YarnPageContext,
} from '~/features/assist/types';

const BASE = '/yarn-assist/api/v1';

async function request<T>(path: string, init?: RequestInit): Promise<T> {
  const res = await fetch(`${BASE}${path}`, {
    ...init,
    headers: {
      'Content-Type': 'application/json',
      ...(init?.headers ?? {}),
    },
  });
  if (!res.ok) {
    const detail = await res.text().catch(() => 'unknown error');
    throw new Error(`YARN Assist API error ${String(res.status)}: ${detail}`);
  }
  return res.json() as Promise<T>;
}

export const assistApi = {
  getCapabilities(): Promise<AssistCapabilities> {
    return request<AssistCapabilities>('/capabilities');
  },

  listConversations(): Promise<{ conversations: ConversationSummary[] }> {
    return request<{ conversations: ConversationSummary[] }>('/conversations');
  },

  createConversation(page_context: YarnPageContext): Promise<{ conversation_id: string }> {
    return request<{ conversation_id: string }>('/conversations', {
      method: 'POST',
      body: JSON.stringify({ page_context }),
    });
  },

  renameConversation(conversationId: string, title: string): Promise<void> {
    return request<void>(`/conversations/${conversationId}`, {
      method: 'PATCH',
      body: JSON.stringify({ title }),
    });
  },

  deleteConversation(conversationId: string): Promise<void> {
    return request<void>(`/conversations/${conversationId}`, { method: 'DELETE' });
  },
};

/** Chat stream endpoint URL - passed directly to the AI SDK useChat hook */
export const CHAT_STREAM_URL = `${BASE}/chat/stream`;
