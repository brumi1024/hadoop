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

import {
  Activity,
  BookOpen,
  GitCompare,
  ListTree,
  Route,
  Settings,
  ShieldCheck,
  Sparkles,
  Tag,
  TriangleAlert,
} from 'lucide-react';
import type { LucideIcon } from 'lucide-react';
import { cn } from '~/utils/cn';
import type {
  AssistPrompt,
  AssistPromptIcon,
} from '~/features/assist/presentation/assistPresentation';

const ICON_MAP: Record<AssistPromptIcon, LucideIcon> = {
  activity: Activity,
  'book-open': BookOpen,
  'git-compare': GitCompare,
  'list-tree': ListTree,
  route: Route,
  settings: Settings,
  'shield-check': ShieldCheck,
  sparkles: Sparkles,
  tag: Tag,
  'triangle-alert': TriangleAlert,
};

interface AssistPromptListProps {
  prompts: readonly AssistPrompt[];
  disabled: boolean;
  onSend: (prompt: string) => void;
}

function RecommendedCard({
  prompt,
  disabled,
  onSend,
}: {
  prompt: AssistPrompt;
  disabled: boolean;
  onSend: (p: string) => void;
}) {
  const Icon = ICON_MAP[prompt.icon];

  function handleActivate() {
    if (disabled) return;
    onSend(prompt.prompt);
  }

  function handleKeyDown(e: React.KeyboardEvent) {
    if (e.key === 'Enter' || e.key === ' ') {
      e.preventDefault();
      handleActivate();
    }
  }

  return (
    <button
      type="button"
      className={cn(
        'w-full text-left rounded-lg border border-primary/30 bg-primary/5 px-3 py-2.5',
        'transition-all duration-150',
        'focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-offset-1',
        'motion-reduce:transition-none',
        !disabled && 'hover:border-primary/60 hover:bg-primary/10 hover:shadow-sm cursor-pointer',
        disabled && 'opacity-50 cursor-not-allowed',
      )}
      onClick={handleActivate}
      onKeyDown={handleKeyDown}
      disabled={disabled}
      data-testid="recommended-prompt-card"
    >
      <div className="flex items-start gap-3">
        <div className="flex items-center justify-center w-8 h-8 rounded-md border border-primary/30 bg-primary/10 text-primary shrink-0 mt-0.5">
          <Icon className="h-3.5 w-3.5" />
        </div>
        <div className="flex-1 min-w-0">
          <p className="font-mono text-[10px] text-primary/70 tracking-widest uppercase mb-0.5">
            Recommended
          </p>
          <p className="text-sm font-semibold text-foreground leading-tight">{prompt.title}</p>
          <p className="text-xs text-muted-foreground mt-0.5 leading-relaxed">
            {prompt.description}
          </p>
          <p className="font-mono text-[10px] text-primary/70 tracking-widest uppercase mt-1.5">
            {prompt.cue} &rarr;
          </p>
        </div>
      </div>
    </button>
  );
}

function CompactRow({
  prompt,
  disabled,
  onSend,
}: {
  prompt: AssistPrompt;
  disabled: boolean;
  onSend: (p: string) => void;
}) {
  const Icon = ICON_MAP[prompt.icon];

  function handleActivate() {
    if (disabled) return;
    onSend(prompt.prompt);
  }

  function handleKeyDown(e: React.KeyboardEvent) {
    if (e.key === 'Enter' || e.key === ' ') {
      e.preventDefault();
      handleActivate();
    }
  }

  return (
    <button
      type="button"
      className={cn(
        'group w-full text-left rounded-md border border-border px-3 py-2',
        'flex items-center gap-2.5 min-h-[2.5rem]',
        'transition-all duration-150',
        'focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-offset-1',
        'motion-reduce:transition-none',
        !disabled && 'hover:border-foreground/30 cursor-pointer',
        disabled && 'opacity-50 cursor-not-allowed',
      )}
      onClick={handleActivate}
      onKeyDown={handleKeyDown}
      disabled={disabled}
      data-testid="compact-prompt-row"
    >
      <Icon className="h-3.5 w-3.5 text-muted-foreground shrink-0" />
      <span className="flex-1 text-sm font-medium text-foreground truncate">{prompt.title}</span>
      <span
        className={cn(
          'font-mono text-[10px] text-muted-foreground shrink-0 tracking-widest uppercase',
          'opacity-0 transition-opacity duration-150',
          'group-hover:opacity-100 group-focus-within:opacity-100',
          'motion-reduce:opacity-100',
        )}
      >
        {prompt.cue} &rarr;
      </span>
    </button>
  );
}

export function AssistPromptList({ prompts, disabled, onSend }: AssistPromptListProps) {
  if (prompts.length === 0) return null;

  const [recommended, ...rest] = prompts;

  return (
    <div className="flex flex-col gap-2">
      {recommended && <RecommendedCard prompt={recommended} disabled={disabled} onSend={onSend} />}
      {rest.map((prompt) => (
        <CompactRow key={prompt.id} prompt={prompt} disabled={disabled} onSend={onSend} />
      ))}
    </div>
  );
}
