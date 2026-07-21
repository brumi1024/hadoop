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

import { useEffect, useRef } from 'react';
import { ArrowUp, Square } from 'lucide-react';
import { Button } from '~/components/ui/button';
import { cn } from '~/utils/cn';

interface AssistComposerProps {
  value: string;
  isStreaming: boolean;
  hasMessages: boolean;
  placeholder: string;
  onChange: (value: string) => void;
  onSend: () => void;
  onStop: () => void;
}

export function AssistComposer({
  value,
  isStreaming,
  hasMessages,
  placeholder,
  onChange,
  onSend,
  onStop,
}: AssistComposerProps) {
  const textareaRef = useRef<HTMLTextAreaElement>(null);

  // Auto-grow the textarea
  useEffect(() => {
    const el = textareaRef.current;
    if (!el) return;
    el.style.height = 'auto';
    el.style.height = `${Math.min(el.scrollHeight, 200)}px`;
  }, [value]);

  function handleKeyDown(e: React.KeyboardEvent<HTMLTextAreaElement>) {
    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault();
      if (!isStreaming && value.trim()) {
        onSend();
      }
    }
  }

  const currentPlaceholder = hasMessages ? 'Follow up on this conversation...' : placeholder;

  const canSend = !isStreaming && Boolean(value.trim());

  return (
    <div className="border-t shrink-0 p-2">
      <div
        className={cn(
          'flex flex-col rounded-lg border bg-background',
          'shadow-sm transition-shadow duration-150',
          'focus-within:ring-2 focus-within:ring-ring focus-within:ring-offset-1',
        )}
      >
        <textarea
          ref={textareaRef}
          className={cn(
            'w-full resize-none bg-transparent text-sm px-3 pt-2.5 pb-1',
            'focus-visible:outline-none placeholder:text-muted-foreground',
            'min-h-[2.25rem] max-h-[200px] overflow-y-auto',
            'disabled:opacity-50',
          )}
          rows={1}
          placeholder={currentPlaceholder}
          value={value}
          onChange={(e) => onChange(e.target.value)}
          onKeyDown={handleKeyDown}
          disabled={false}
          aria-label="Message input"
        />
        <div className="flex items-center justify-end px-2 pb-2 pt-1">
          {isStreaming ? (
            <Button
              type="button"
              size="icon"
              variant="destructive"
              className="h-8 w-8 rounded-full"
              onClick={onStop}
              aria-label="Stop generating"
            >
              <Square className="h-3.5 w-3.5" />
            </Button>
          ) : (
            <Button
              type="button"
              size="icon"
              className="h-8 w-8 rounded-full"
              onClick={onSend}
              disabled={!canSend}
              aria-label="Send message"
            >
              <ArrowUp className="h-3.5 w-3.5" />
            </Button>
          )}
        </div>
      </div>
    </div>
  );
}
