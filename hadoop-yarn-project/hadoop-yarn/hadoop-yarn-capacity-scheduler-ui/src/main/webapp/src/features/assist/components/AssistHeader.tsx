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

import { Clock, Plus, X } from 'lucide-react';
import { Button } from '~/components/ui/button';

interface AssistHeaderProps {
  historyOpen: boolean;
  isStreaming: boolean;
  onToggleHistory: () => void;
  onNewConversation: () => void;
  onClose: () => void;
}

export function AssistHeader({
  historyOpen,
  isStreaming,
  onToggleHistory,
  onNewConversation,
  onClose,
}: AssistHeaderProps) {
  return (
    <div className="flex items-center gap-1 px-2 border-b shrink-0 h-11">
      <Button
        variant="ghost"
        size="icon"
        className="h-7 w-7 text-muted-foreground hover:text-foreground"
        onClick={onToggleHistory}
        aria-label={historyOpen ? 'Close conversation history' : 'Open conversation history'}
        aria-pressed={historyOpen}
      >
        <Clock className="h-3.5 w-3.5" />
      </Button>

      <div className="flex-1 flex items-center justify-center gap-1.5 min-w-0">
        <span className="inline-block w-1.5 h-1.5 rounded-full bg-primary shrink-0" />
        <span className="font-mono text-[11px] text-muted-foreground tracking-widest uppercase truncate">
          YARN Assist
        </span>
      </div>

      <Button
        variant="ghost"
        size="icon"
        className="h-7 w-7 text-muted-foreground hover:text-foreground"
        onClick={onNewConversation}
        disabled={isStreaming}
        aria-label="New conversation"
      >
        <Plus className="h-3.5 w-3.5" />
      </Button>

      <Button
        variant="ghost"
        size="icon"
        className="h-7 w-7 text-muted-foreground hover:text-foreground"
        onClick={onClose}
        aria-label="Close YARN Assist"
      >
        <X className="h-3.5 w-3.5" />
      </Button>
    </div>
  );
}
