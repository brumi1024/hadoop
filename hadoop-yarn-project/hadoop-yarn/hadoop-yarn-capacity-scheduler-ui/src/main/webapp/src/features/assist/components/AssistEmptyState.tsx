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

import { cn } from '~/utils/cn';
import { Badge } from '~/components/ui/badge';
import { AssistPromptList } from '~/features/assist/components/AssistPromptList';
import type { AssistPresentation } from '~/features/assist/presentation/assistPresentation';
import type { YarnPageContext } from '~/features/assist/types';

interface AssistEmptyStateProps {
  presentation: AssistPresentation;
  context: YarnPageContext;
  disabled: boolean;
  onSend: (prompt: string) => void;
}

const PAGE_LABELS: Record<NonNullable<YarnPageContext['page_kind']>, string> = {
  queues: 'Queues',
  'node-labels': 'Node Labels',
  'global-settings': 'Global Settings',
  'placement-rules': 'Placement Rules',
};

function ScopeChips({ context }: { context: YarnPageContext }) {
  const chips: {
    label: string;
    value: string;
    variant?: 'destructive' | 'secondary' | 'outline';
  }[] = [];

  if (context.page_kind) {
    chips.push({ label: 'Page', value: PAGE_LABELS[context.page_kind], variant: 'secondary' });
  }
  if (context.selected_queue_path) {
    chips.push({ label: 'Queue', value: context.selected_queue_path, variant: 'secondary' });
  }
  if (context.selected_node_label) {
    const label = context.page_kind === 'queues' ? 'Partition' : 'Label';
    chips.push({ label, value: context.selected_node_label, variant: 'secondary' });
  }
  if (context.staged_change_count > 0) {
    chips.push({
      label: 'Staged',
      value: String(context.staged_change_count),
      variant: 'secondary',
    });
  }
  if (context.is_read_only) {
    chips.push({ label: 'Mode', value: 'Read-only', variant: 'destructive' });
  }

  if (chips.length === 0) return null;

  return (
    <div
      className="flex flex-wrap gap-1.5 rounded-lg border border-border bg-muted/30 px-3 py-2"
      role="status"
      aria-label="Current context"
    >
      {chips.map((chip) => (
        <span key={`${chip.label}-${chip.value}`} className="flex items-center gap-1">
          <span className="text-[10px] font-mono text-muted-foreground uppercase tracking-wider">
            {chip.label}
          </span>
          <Badge variant={chip.variant ?? 'outline'} className="text-xs font-mono px-1.5 py-0">
            {chip.value}
          </Badge>
        </span>
      ))}
    </div>
  );
}

export function AssistEmptyState({
  presentation,
  context,
  disabled,
  onSend,
}: AssistEmptyStateProps) {
  return (
    <div className="flex items-center justify-center min-h-full py-6 px-4">
      <div className="flex flex-col gap-4 w-full max-w-sm">
        {/* Identity */}
        <div
          className={cn(
            'flex items-center gap-1.5',
            'motion-safe:animate-in motion-safe:fade-in motion-safe:slide-in-from-bottom-1',
            'motion-safe:[animation-duration:300ms] motion-safe:[animation-delay:50ms] motion-safe:[animation-fill-mode:both]',
          )}
        >
          <span className="inline-block w-1.5 h-1.5 rounded-full bg-primary shrink-0" />
          <span className="font-mono text-[11px] text-muted-foreground tracking-widest uppercase">
            {presentation.identity}
          </span>
        </div>

        {/* Hero */}
        <div
          className={cn(
            'motion-safe:animate-in motion-safe:fade-in motion-safe:slide-in-from-bottom-1',
            'motion-safe:[animation-duration:300ms] motion-safe:[animation-delay:100ms] motion-safe:[animation-fill-mode:both]',
          )}
        >
          <h2 className="text-xl font-bold leading-tight tracking-tight text-foreground">
            {presentation.hero.leading}{' '}
            <span className="text-primary">{presentation.hero.accent}</span>
          </h2>
          <p className="text-sm text-muted-foreground mt-1.5 leading-relaxed">
            {presentation.hero.lede}
          </p>
        </div>

        {/* Scope chips */}
        <div
          className={cn(
            'motion-safe:animate-in motion-safe:fade-in motion-safe:slide-in-from-bottom-1',
            'motion-safe:[animation-duration:300ms] motion-safe:[animation-delay:150ms] motion-safe:[animation-fill-mode:both]',
          )}
        >
          <ScopeChips context={context} />
        </div>

        {/* Workflows */}
        <div
          className={cn(
            'motion-safe:animate-in motion-safe:fade-in motion-safe:slide-in-from-bottom-1',
            'motion-safe:[animation-duration:300ms] motion-safe:[animation-delay:200ms] motion-safe:[animation-fill-mode:both]',
          )}
        >
          <AssistPromptList prompts={presentation.prompts} disabled={disabled} onSend={onSend} />
        </div>
      </div>
    </div>
  );
}
