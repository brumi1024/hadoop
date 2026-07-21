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

import { useLocation } from 'react-router';
import { useShallow } from 'zustand/react/shallow';
import { useSchedulerStore } from '~/stores/schedulerStore';
import type { YarnPageContext } from '~/features/assist/types';

type PageKind = YarnPageContext['page_kind'];

function derivePageKind(pathname: string): PageKind {
  if (pathname === '/') return 'queues';
  if (pathname === '/node-labels') return 'node-labels';
  if (pathname === '/global-settings') return 'global-settings';
  if (pathname === '/placement-rules') return 'placement-rules';
  return null;
}

const MAX_STAGED_SUMMARY = 3;

export function useYarnContext(): YarnPageContext {
  const location = useLocation();
  const {
    isReadOnly,
    stagedChanges,
    selectedNodeLabel,
    selectedQueuePath,
    selectedNodeLabelFilter,
  } = useSchedulerStore(
    useShallow((s) => ({
      isReadOnly: s.isReadOnly,
      stagedChanges: s.stagedChanges,
      selectedNodeLabel: s.selectedNodeLabel ?? null,
      selectedQueuePath: s.selectedQueuePath ?? null,
      selectedNodeLabelFilter: s.selectedNodeLabelFilter,
    })),
  );

  // searchContext via a separate selector; cast through unknown to avoid index-signature error
  const searchContext = useSchedulerStore(
    (s) =>
      ((s as unknown as Record<string, unknown>).searchContext as string | null | undefined) ??
      null,
  );

  const pageKind = derivePageKind(location.pathname);

  // Build a bounded summary of staged changes (StagedChange has type: 'add'|'update'|'remove')
  let stagedChangeSummary: string | null = null;
  if (stagedChanges.length > 0) {
    const previews = stagedChanges.slice(0, MAX_STAGED_SUMMARY).map((c) => {
      if (c.type === 'add') return `+${c.queuePath}`;
      if (c.type === 'remove') return `-${c.queuePath}`;
      // 'update' - show property change
      return `${c.queuePath}:${c.property}`;
    });
    const remainder = stagedChanges.length - previews.length;
    stagedChangeSummary =
      previews.join(', ') + (remainder > 0 ? ` +${String(remainder)} more` : '');
  }

  // On the queues page use the active partition filter as the node-label context so
  // the empty state scope readout reflects which partition is being viewed.
  const effectiveNodeLabel =
    pageKind === 'node-labels'
      ? selectedNodeLabel
      : pageKind === 'queues' && selectedNodeLabelFilter !== ''
        ? selectedNodeLabelFilter
        : null;

  return {
    page_kind: pageKind,
    selected_queue_path: pageKind === 'queues' ? selectedQueuePath : null,
    selected_node_label: effectiveNodeLabel,
    search_context: searchContext,
    is_read_only: isReadOnly,
    staged_change_count: stagedChanges.length,
    staged_change_summary: stagedChangeSummary,
  };
}
