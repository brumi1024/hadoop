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
import { AlertTriangle, CheckCircle, ChevronRight, GitMerge, XCircle } from 'lucide-react';
import { Button } from '~/components/ui/button';
import { Badge } from '~/components/ui/badge';
import { Separator } from '~/components/ui/separator';
import { useSchedulerStore } from '~/stores/schedulerStore';
import { stageProposal } from '~/features/assist/proposals/stageProposal';
import type { ProposalChange, SchedulerChangeProposal } from '~/features/assist/types';

interface ProposalCardProps {
  proposal: SchedulerChangeProposal;
  onStaged?: (staged: number, rejected: number) => void;
}

function ChangeRow({ change }: { change: ProposalChange }) {
  const badge = (text: string, variant: 'default' | 'secondary' | 'destructive') => (
    <Badge variant={variant} className="text-xs font-mono">
      {text}
    </Badge>
  );

  if (change.kind === 'queue_property') {
    return (
      <div className="flex items-start gap-2 text-sm py-1">
        <ChevronRight className="h-3.5 w-3.5 mt-0.5 text-muted-foreground shrink-0" />
        <div className="flex-1 min-w-0">
          <span className="font-mono text-xs text-muted-foreground">{change.queue_path}</span>
          <span className="mx-1 text-muted-foreground">-</span>
          <span className="font-mono text-xs">{change.property}</span>
          <div className="flex items-center gap-1 mt-0.5">
            {badge(change.old_value, 'secondary')}
            <ChevronRight className="h-3 w-3 text-muted-foreground" />
            {badge(change.new_value, 'default')}
          </div>
          {change.reason && <p className="text-xs text-muted-foreground mt-0.5">{change.reason}</p>}
        </div>
      </div>
    );
  }

  if (change.kind === 'global_property') {
    return (
      <div className="flex items-start gap-2 text-sm py-1">
        <ChevronRight className="h-3.5 w-3.5 mt-0.5 text-muted-foreground shrink-0" />
        <div className="flex-1 min-w-0">
          <span className="font-mono text-xs">Global: {change.property}</span>
          <div className="flex items-center gap-1 mt-0.5">
            {badge(String(change.old_value), 'secondary')}
            <ChevronRight className="h-3 w-3 text-muted-foreground" />
            {badge(String(change.new_value), 'default')}
          </div>
        </div>
      </div>
    );
  }

  if (change.kind === 'queue_addition') {
    return (
      <div className="flex items-start gap-2 text-sm py-1">
        <ChevronRight className="h-3.5 w-3.5 mt-0.5 text-green-500 shrink-0" />
        <div className="flex-1 min-w-0">
          <span className="text-green-600 dark:text-green-400 font-medium">Add queue</span>
          <span className="mx-1 font-mono text-xs">
            {change.parent_path}/{change.queue_name}
          </span>
          {change.reason && <p className="text-xs text-muted-foreground mt-0.5">{change.reason}</p>}
        </div>
      </div>
    );
  }

  if (change.kind === 'queue_removal') {
    return (
      <div className="flex items-start gap-2 text-sm py-1">
        <ChevronRight className="h-3.5 w-3.5 mt-0.5 text-destructive shrink-0" />
        <div className="flex-1 min-w-0">
          <span className="text-destructive font-medium">Remove queue</span>
          <span className="mx-1 font-mono text-xs">{change.queue_path}</span>
          {change.reason && <p className="text-xs text-muted-foreground mt-0.5">{change.reason}</p>}
        </div>
      </div>
    );
  }

  if (change.kind === 'label_queue_property') {
    return (
      <div className="flex items-start gap-2 text-sm py-1">
        <ChevronRight className="h-3.5 w-3.5 mt-0.5 text-muted-foreground shrink-0" />
        <div className="flex-1 min-w-0">
          <span className="font-mono text-xs text-muted-foreground">{change.queue_path}</span>
          <span className="mx-1 text-xs text-muted-foreground">[{change.label}]</span>
          <span className="font-mono text-xs">{change.property}</span>
          <div className="flex items-center gap-1 mt-0.5">
            {badge(change.old_value, 'secondary')}
            <ChevronRight className="h-3 w-3 text-muted-foreground" />
            {badge(change.new_value, 'default')}
          </div>
        </div>
      </div>
    );
  }

  return null;
}

export function ProposalCard({ proposal, onStaged }: ProposalCardProps) {
  const isReadOnly = useSchedulerStore((s) => s.isReadOnly);
  const [stageResult, setStageResult] = useState<{ staged: number; rejected: number } | null>(null);
  const [hasStaged, setHasStaged] = useState(false);

  function handleStage() {
    const result = stageProposal(proposal);
    setStageResult({ staged: result.staged, rejected: result.rejected.length });
    setHasStaged(true);
    onStaged?.(result.staged, result.rejected.length);
  }

  return (
    <div className="rounded-lg border bg-card text-card-foreground shadow-sm p-3 space-y-2 mt-2">
      <div className="flex items-start gap-2">
        <GitMerge className="h-4 w-4 mt-0.5 text-primary shrink-0" />
        <div className="flex-1 min-w-0">
          <p className="text-sm font-semibold">{proposal.summary}</p>
          <p className="text-xs text-muted-foreground mt-0.5">{proposal.rationale}</p>
        </div>
      </div>

      {proposal.warnings.length > 0 && (
        <div className="rounded-md bg-amber-50 dark:bg-amber-950/30 border border-amber-200 dark:border-amber-800 p-2 space-y-1">
          {proposal.warnings.map((w) => (
            <div
              key={w}
              className="flex items-start gap-1.5 text-xs text-amber-700 dark:text-amber-400"
            >
              <AlertTriangle className="h-3.5 w-3.5 mt-0.5 shrink-0" />
              <span>{w}</span>
            </div>
          ))}
        </div>
      )}

      <Separator />

      <div className="space-y-0.5">
        <p className="text-xs font-medium text-muted-foreground uppercase tracking-wide mb-1">
          {proposal.changes.length} proposed {proposal.changes.length === 1 ? 'change' : 'changes'}
        </p>
        {proposal.changes.map((c, changeIdx) => (
          <ChangeRow key={`${proposal.proposal_id}-change-${String(changeIdx)}`} change={c} />
        ))}
      </div>

      {stageResult !== null ? (
        <div className="flex items-center gap-2 pt-1">
          {stageResult.staged > 0 && (
            <div className="flex items-center gap-1 text-xs text-green-600 dark:text-green-400">
              <CheckCircle className="h-3.5 w-3.5" />
              {stageResult.staged} change{stageResult.staged !== 1 ? 's' : ''} staged
            </div>
          )}
          {stageResult.rejected > 0 && (
            <div className="flex items-center gap-1 text-xs text-destructive">
              <XCircle className="h-3.5 w-3.5" />
              {stageResult.rejected} rejected
            </div>
          )}
        </div>
      ) : (
        <Button
          size="sm"
          variant="default"
          className="w-full"
          disabled={isReadOnly || hasStaged}
          onClick={handleStage}
        >
          {isReadOnly ? 'Read-only mode' : 'Stage changes'}
        </Button>
      )}
    </div>
  );
}
