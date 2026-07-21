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

/**
 * Vite dev-only plugin for YARN Assist mock API.
 * Intercepts /yarn-assist/api/v1/* and returns realistic SSE streams.
 * Never included in production bundles.
 */
import type { IncomingMessage, ServerResponse } from 'node:http';
import type { Plugin } from 'vite';

import {
  CAPABILITIES,
  CONVERSATIONS,
  HEALTH,
  type MockToolCall,
  selectResponse,
  selectToolCalls,
} from './assistMockData';

// ---------------------------------------------------------------------------
// In-memory state
// ---------------------------------------------------------------------------

const renamedTitles = new Map<string, string>();
const createdConversations = new Map<
  string,
  (typeof CONVERSATIONS.conversations)[number]
>();

function convSummary(id: string) {
  const created = createdConversations.get(id);
  const preset = CONVERSATIONS.conversations.find((c) => c.conversation_id === id);
  const base = created ?? preset;
  if (!base) return null;
  return { ...base, title: renamedTitles.get(id) ?? base.title };
}

// ---------------------------------------------------------------------------
// Body reader
// ---------------------------------------------------------------------------

function readBody(req: IncomingMessage): Promise<string> {
  return new Promise((resolve) => {
    let data = '';
    req.on('data', (chunk: Buffer) => {
      data += chunk.toString();
    });
    req.on('end', () => resolve(data));
  });
}

// ---------------------------------------------------------------------------
// Latest user message text extraction (AI SDK v6 format)
// ---------------------------------------------------------------------------

function latestUserText(body: string): string {
  try {
    const parsed = JSON.parse(body) as {
      messages?: { role: string; parts?: { type: string; text?: string }[] }[];
    };
    let text = '';
    for (const msg of parsed.messages ?? []) {
      if (msg.role === 'user') {
        text =
          msg.parts?.reduce(
            (acc, p) => (p.type === 'text' ? acc + (p.text ?? '') : acc),
            '',
          ) ?? '';
      }
    }
    return text;
  } catch {
    return '';
  }
}

// ---------------------------------------------------------------------------
// SSE streaming
// ---------------------------------------------------------------------------

function setupSSEResponse(res: ServerResponse): void {
  res.setHeader('Content-Type', 'text/event-stream');
  res.setHeader('x-vercel-ai-ui-message-stream', 'v1');
  res.setHeader('Cache-Control', 'no-cache');
  res.setHeader('Connection', 'keep-alive');
  res.statusCode = 200;
}

function writeEvent(res: ServerResponse, data: unknown): void {
  res.write(`data: ${JSON.stringify(data)}\n\n`);
}

function streamMockResponse(res: ServerResponse, userMessage: string): void {
  setupSSEResponse(res);

  const toolCalls = selectToolCalls(userMessage);
  const responseText = selectResponse(userMessage);
  const tokens = responseText.match(/\S+\s*/g) ?? [responseText];
  const textId = `text-${Date.now()}`;
  const streamDelayMs = 20;

  writeEvent(res, { type: 'start' });
  writeEvent(res, { type: 'start-step' });

  let toolIdx = 0;
  let toolPhase: 'input-start' | 'waiting' | 'output' | 'done' = 'input-start';
  let toolWaitTicks = 0;
  let textStarted = false;
  let textIdx = 0;

  const interval = setInterval(() => {
    // Phase 1: stream tool calls
    if (toolIdx < toolCalls.length) {
      const tc = toolCalls[toolIdx] as MockToolCall;
      const id = `mock-call-${String(toolIdx + 1)}`;
      const latency = tc.latencyTicks ?? 6;

      if (toolPhase === 'input-start') {
        writeEvent(res, { type: 'tool-input-start', toolCallId: id, toolName: tc.name });
        writeEvent(res, {
          type: 'tool-input-delta',
          toolCallId: id,
          inputTextDelta: JSON.stringify(tc.args),
        });
        toolPhase = 'waiting';
        toolWaitTicks = 0;
        return;
      }

      if (toolPhase === 'waiting') {
        toolWaitTicks++;
        if (toolWaitTicks < latency) return;
        writeEvent(res, {
          type: 'tool-input-available',
          toolCallId: id,
          toolName: tc.name,
          input: tc.args,
        });
        toolPhase = 'output';
        return;
      }

      if (toolPhase === 'output') {
        writeEvent(res, {
          type: 'tool-output-available',
          toolCallId: id,
          output: tc.result,
        });
        toolIdx++;
        toolPhase = 'input-start';
        return;
      }
    }

    // Phase 2: stream text response
    if (!textStarted) {
      writeEvent(res, { type: 'text-start', id: textId });
      textStarted = true;
      return;
    }

    if (textIdx >= tokens.length) {
      writeEvent(res, { type: 'text-end', id: textId });
      writeEvent(res, { type: 'finish-step' });
      writeEvent(res, { type: 'finish', finishReason: 'stop' });
      res.write('data: [DONE]\n\n');
      clearInterval(interval);
      res.end();
      return;
    }

    writeEvent(res, { type: 'text-delta', id: textId, delta: tokens[textIdx] });
    textIdx++;
  }, streamDelayMs);

  res.on('close', () => clearInterval(interval));
}

function streamError(res: ServerResponse, code: string, message: string): void {
  setupSSEResponse(res);
  writeEvent(res, { type: 'start' });
  writeEvent(res, { type: 'start-step' });
  writeEvent(res, { type: 'error', error: { code, message } });
  writeEvent(res, { type: 'finish', finishReason: 'error' });
  res.write('data: [DONE]\n\n');
  res.end();
}

// ---------------------------------------------------------------------------
// Plugin
// ---------------------------------------------------------------------------

export function yarnAssistMockPlugin(): Plugin {
  return {
    name: 'yarn-assist-mock',
    configureServer(server) {
      server.middlewares.use(
        async (req: IncomingMessage, res: ServerResponse, next: () => void) => {
          const url = req.url ?? '';
          if (!url.startsWith('/yarn-assist/api/v1/')) {
            next();
            return;
          }

          const apiPath = url.slice('/yarn-assist/api/v1'.length).split('?')[0];

          // GET /health
          if (apiPath === '/health' && req.method === 'GET') {
            res.setHeader('Content-Type', 'application/json');
            res.statusCode = 200;
            res.end(JSON.stringify(HEALTH));
            return;
          }

          // GET /capabilities
          if (apiPath === '/capabilities' && req.method === 'GET') {
            res.setHeader('Content-Type', 'application/json');
            res.statusCode = 200;
            res.end(JSON.stringify(CAPABILITIES));
            return;
          }

          // GET /conversations
          if (apiPath === '/conversations' && req.method === 'GET') {
            const all = [
              ...createdConversations.values(),
              ...CONVERSATIONS.conversations,
            ].map((c) => convSummary(c.conversation_id) ?? c);
            res.setHeader('Content-Type', 'application/json');
            res.statusCode = 200;
            res.end(JSON.stringify({ conversations: all }));
            return;
          }

          // POST /conversations
          if (apiPath === '/conversations' && req.method === 'POST') {
            const id = `mock-${String(Date.now())}`;
            const now = new Date().toISOString();
            createdConversations.set(id, {
              conversation_id: id,
              created_at: now,
              updated_at: now,
              title: 'New conversation',
              message_count: 0,
            });
            res.setHeader('Content-Type', 'application/json');
            res.statusCode = 201;
            res.end(JSON.stringify({ conversation_id: id }));
            return;
          }

          // PATCH /conversations/:id
          if (apiPath.startsWith('/conversations/') && req.method === 'PATCH') {
            const id = apiPath.replace('/conversations/', '');
            const body = JSON.parse(await readBody(req)) as { title?: string };
            if (body.title) renamedTitles.set(id, body.title.trim());
            res.setHeader('Content-Type', 'application/json');
            res.statusCode = 200;
            res.end(JSON.stringify({ ok: true }));
            return;
          }

          // GET /conversations/:id
          if (apiPath.startsWith('/conversations/') && req.method === 'GET') {
            const id = apiPath.replace('/conversations/', '');
            const summary = convSummary(id);
            res.setHeader('Content-Type', 'application/json');
            res.statusCode = summary ? 200 : 404;
            res.end(
              JSON.stringify(
                summary ? { ...summary, messages: [] } : { detail: 'Conversation not found.' },
              ),
            );
            return;
          }

          // DELETE /conversations/:id
          if (apiPath.startsWith('/conversations/') && req.method === 'DELETE') {
            const id = apiPath.replace('/conversations/', '');
            createdConversations.delete(id);
            renamedTitles.delete(id);
            res.statusCode = 204;
            res.end();
            return;
          }

          // POST /chat/stream
          if (apiPath === '/chat/stream' && req.method === 'POST') {
            const body = await readBody(req);
            const userText = latestUserText(body);

            // Simulate stream error scenario
            if (userText.toLowerCase().includes('stream error')) {
              streamError(res, 'stream_failure', 'Simulated stream failure for demo purposes.');
              return;
            }

            streamMockResponse(res, userText);
            return;
          }

          next();
        },
      );

      console.log('\n  \x1b[36m✓ YARN Assist mock API enabled\x1b[0m — /yarn-assist/api/v1/*\n');
    },
  };
}
