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

import { useSchedulerStore } from '~/stores/schedulerStore';
import { buildPropertyKey } from '~/utils/propertyUtils';
import type {
  GlobalPropertyChange,
  LabelQueuePropertyChange,
  ProposalChange,
  QueueAdditionChange,
  QueuePropertyChange,
  QueueRemovalChange,
  SchedulerChangeProposal,
} from '~/features/assist/types';

export interface StageResult {
  staged: number;
  rejected: StagingRejection[];
}

export interface StagingRejection {
  change: ProposalChange;
  reason: string;
}

/**
 * Stage all supported changes from a proposal into the existing Zustand store.
 *
 * Contract:
 * - Reads configData from the store to verify old_value matches current state.
 * - Uses only public store actions (stageQueueChange, stageGlobalChange, etc.).
 * - Never calls applyChanges.
 * - In read-only mode, rejects all changes.
 * - Preserves all pre-existing staged changes.
 * - Returns a summary of what was staged and what was rejected.
 */
export function stageProposal(proposal: SchedulerChangeProposal): StageResult {
  const store = useSchedulerStore.getState();
  const { isReadOnly, configData } = store;

  if (isReadOnly) {
    return {
      staged: 0,
      rejected: proposal.changes.map((c) => ({
        change: c,
        reason: 'Scheduler is in read-only mode.',
      })),
    };
  }

  let staged = 0;
  const rejected: StagingRejection[] = [];

  for (const change of proposal.changes) {
    const result = stageOneChange(change, store, configData);
    if (result === null) {
      staged++;
    } else {
      rejected.push({ change, reason: result });
    }
  }

  return { staged, rejected };
}

function stageOneChange(
  change: ProposalChange,
  store: ReturnType<typeof useSchedulerStore.getState>,
  configData: Map<string, string>,
): string | null {
  try {
    switch (change.kind) {
      case 'queue_property':
        return stageQueueProperty(change, store, configData);
      case 'global_property':
        return stageGlobalProperty(change, store);
      case 'queue_addition':
        return stageQueueAddition(change, store);
      case 'queue_removal':
        return stageQueueRemoval(change, store);
      case 'label_queue_property':
        return stageLabelQueueProperty(change, store, configData);
      default:
        return `Unknown change kind: ${String((change as { kind: string }).kind)}`;
    }
  } catch (err) {
    return err instanceof Error ? err.message : 'Unexpected error during staging.';
  }
}

function stageQueueProperty(
  change: QueuePropertyChange,
  store: ReturnType<typeof useSchedulerStore.getState>,
  configData: Map<string, string>,
): string | null {
  if (!change.queue_path || !change.property || change.new_value === undefined) {
    return 'Missing required fields: queue_path, property, or new_value.';
  }
  const currentValue = configData.get(buildPropertyKey(change.queue_path, change.property));
  if (
    change.old_value !== undefined &&
    currentValue !== undefined &&
    currentValue !== change.old_value
  ) {
    return `Stale proposal: current value is "${currentValue}", proposal expected "${change.old_value}".`;
  }
  store.stageQueueChange(change.queue_path, change.property, change.new_value);
  return null;
}

function stageGlobalProperty(
  change: GlobalPropertyChange,
  store: ReturnType<typeof useSchedulerStore.getState>,
): string | null {
  if (!change.property || change.new_value === undefined) {
    return 'Missing required fields: property or new_value.';
  }
  store.stageGlobalChange(change.property, change.new_value);
  return null;
}

function stageQueueAddition(
  change: QueueAdditionChange,
  store: ReturnType<typeof useSchedulerStore.getState>,
): string | null {
  if (!change.parent_path || !change.queue_name) {
    return 'Missing required fields: parent_path or queue_name.';
  }
  store.stageQueueAddition(change.parent_path, change.queue_name, change.config ?? {});
  return null;
}

function stageQueueRemoval(
  change: QueueRemovalChange,
  store: ReturnType<typeof useSchedulerStore.getState>,
): string | null {
  if (!change.queue_path) {
    return 'Missing required field: queue_path.';
  }
  store.stageQueueRemoval(change.queue_path);
  return null;
}

function stageLabelQueueProperty(
  change: LabelQueuePropertyChange,
  store: ReturnType<typeof useSchedulerStore.getState>,
  _configData: Map<string, string>,
): string | null {
  if (!change.queue_path || !change.label || !change.property || change.new_value === undefined) {
    return 'Missing required fields for label_queue_property change.';
  }
  store.stageLabelQueueChange(change.queue_path, change.label, change.property, change.new_value);
  return null;
}
