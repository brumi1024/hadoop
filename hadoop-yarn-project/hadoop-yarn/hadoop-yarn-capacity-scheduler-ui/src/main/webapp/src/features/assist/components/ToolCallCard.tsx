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

import { useState } from 'react';
import { ChevronDown, ChevronRight, Loader2, Terminal, XCircle } from 'lucide-react';
import { cn } from '~/utils/cn';

interface ToolCallCardProps {
  name: string;
  state: 'input-streaming' | 'input-available' | 'output-available' | 'output-error';
  input?: unknown;
  output?: unknown;
}

const MAX_PREVIEW_CHARS = 200;

function truncate(text: string): string {
  if (text.length <= MAX_PREVIEW_CHARS) return text;
  return text.slice(0, MAX_PREVIEW_CHARS) + '…';
}

export function ToolCallCard({ name, state, input, output }: ToolCallCardProps) {
  const [expanded, setExpanded] = useState(false);

  const isStreaming = state === 'input-streaming';
  const isError = state === 'output-error';
  const isDone = state === 'output-available';

  const outputStr = output !== undefined ? JSON.stringify(output, null, 2) : '';
  const inputStr = input !== undefined ? JSON.stringify(input, null, 2) : '';

  return (
    <div
      className={cn(
        'rounded-md border text-xs font-mono my-1',
        isError ? 'border-destructive/50 bg-destructive/5' : 'border-border bg-muted/30',
      )}
    >
      <button
        className="flex items-center gap-1.5 w-full px-2 py-1.5 text-left"
        onClick={() => setExpanded((v) => !v)}
        aria-expanded={expanded}
        disabled={isStreaming}
      >
        {isStreaming ? (
          <Loader2 className="h-3 w-3 animate-spin text-muted-foreground" />
        ) : isError ? (
          <XCircle className="h-3 w-3 text-destructive" />
        ) : (
          <Terminal className="h-3 w-3 text-muted-foreground" />
        )}
        <span className="text-muted-foreground">tool:</span>
        <span className="font-semibold">{name}</span>
        {!isStreaming && (
          <span className="ml-auto">
            {expanded ? <ChevronDown className="h-3 w-3" /> : <ChevronRight className="h-3 w-3" />}
          </span>
        )}
      </button>

      {expanded && !isStreaming && (
        <div className="border-t border-border px-2 py-1.5 space-y-1">
          {inputStr && (
            <div>
              <span className="text-muted-foreground text-[10px] uppercase tracking-wide">
                Input
              </span>
              <pre className="mt-0.5 whitespace-pre-wrap break-all text-[10px] max-h-32 overflow-auto">
                {truncate(inputStr)}
              </pre>
            </div>
          )}
          {isDone && outputStr && (
            <div>
              <span className="text-muted-foreground text-[10px] uppercase tracking-wide">
                Output
              </span>
              <pre className="mt-0.5 whitespace-pre-wrap break-all text-[10px] max-h-48 overflow-auto">
                {truncate(outputStr)}
              </pre>
            </div>
          )}
          {isError && <p className="text-destructive text-[10px]">Tool call failed</p>}
        </div>
      )}
    </div>
  );
}
