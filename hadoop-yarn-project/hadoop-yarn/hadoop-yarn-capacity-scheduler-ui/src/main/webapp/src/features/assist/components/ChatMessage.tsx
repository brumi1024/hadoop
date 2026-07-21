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

import type { UIMessage } from 'ai';
import ReactMarkdown from 'react-markdown';
import remarkGfm from 'remark-gfm';
import { cn } from '~/utils/cn';
import { ToolCallCard } from '~/features/assist/components/ToolCallCard';
import { ProposalCard } from '~/features/assist/components/ProposalCard';
import type { SchedulerChangeProposal } from '~/features/assist/types';

interface ChatMessageProps {
  message: UIMessage;
}

function tryParseProposal(text: string): SchedulerChangeProposal | null {
  const trimmed = text.trim();
  if (!trimmed.startsWith('{')) return null;
  try {
    const parsed = JSON.parse(trimmed) as Record<string, unknown>;
    if (parsed.proposal_id && parsed.changes && Array.isArray(parsed.changes)) {
      return parsed as unknown as SchedulerChangeProposal;
    }
  } catch {
    // not a proposal
  }
  return null;
}

export function ChatMessage({ message }: ChatMessageProps) {
  const isUser = message.role === 'user';

  if (isUser) {
    return (
      <div className="flex flex-col items-end gap-1">
        <span className="font-mono text-[10px] text-muted-foreground tracking-widest uppercase">
          You
        </span>
        <div
          className={cn(
            'max-w-[82%] rounded-lg border border-border bg-muted/60 px-3 py-2',
            'text-sm text-foreground leading-relaxed',
          )}
        >
          {message.parts?.map((part, partIndex) => {
            const partKey = `${message.id}-part-${String(partIndex)}`;
            if (part.type === 'text') {
              const textPart = part as { type: 'text'; text: string };
              return <span key={partKey}>{textPart.text}</span>;
            }
            return null;
          })}
        </div>
      </div>
    );
  }

  // Assistant message - open content style
  return (
    <div className="flex flex-col gap-1.5 min-w-0">
      <div className="flex items-center gap-1.5">
        <span className="inline-block w-1.5 h-1.5 rounded-full bg-primary shrink-0" />
        <span className="font-mono text-[10px] text-muted-foreground tracking-widest uppercase">
          YARN Assist
        </span>
      </div>
      <div className="min-w-0">
        {message.parts?.map((part, partIndex) => {
          const partKey = `${message.id}-part-${String(partIndex)}`;

          if (part.type === 'text') {
            const textPart = part as { type: 'text'; text: string };
            const proposal = tryParseProposal(textPart.text);
            if (proposal) {
              return <ProposalCard key={partKey} proposal={proposal} />;
            }
            return (
              <div key={partKey} className="assist-markdown text-sm text-foreground">
                <ReactMarkdown remarkPlugins={[remarkGfm]}>{textPart.text}</ReactMarkdown>
              </div>
            );
          }

          // Handle dynamic tool parts (state-based in AI SDK v4)
          if (part.type === 'dynamic-tool' || (part.type as string).startsWith('tool-')) {
            const toolPart = part as {
              type: string;
              toolName?: string;
              toolCallId?: string;
              state?: string;
              input?: unknown;
              output?: unknown;
              errorText?: string;
            };

            const name = toolPart.toolName ?? part.type;
            const state = toolPart.state ?? 'input-streaming';
            const toolId = toolPart.toolCallId ?? partKey;
            let cardState:
              | 'input-streaming'
              | 'input-available'
              | 'output-available'
              | 'output-error' = 'input-streaming';
            if (state === 'input-streaming') cardState = 'input-streaming';
            else if (state === 'input-available' || state === 'approval-requested')
              cardState = 'input-available';
            else if (state === 'output-available') cardState = 'output-available';
            else if (state === 'output-error') cardState = 'output-error';

            return (
              <ToolCallCard
                key={toolId}
                name={name}
                state={cardState}
                input={toolPart.input}
                output={toolPart.output}
              />
            );
          }

          return null;
        })}
      </div>
    </div>
  );
}
