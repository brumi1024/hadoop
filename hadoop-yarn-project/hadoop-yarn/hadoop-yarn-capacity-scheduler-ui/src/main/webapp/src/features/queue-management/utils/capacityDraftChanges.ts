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
 * Extracts the property changes the capacity editor drafts make, so they can be staged.
 */

import type { CapacityRowDraft } from '~/stores/slices/capacityEditorSlice';
import {
  convertVectorDraftToString,
  DEFAULT_PARTITION_VALUE,
  getPropertyNameForLabel,
} from './capacityEditor';

export interface DraftCacheEntry {
  drafts: Record<string, CapacityRowDraft>;
  draftOrder: string[];
}

export interface ExtractChangesParams {
  draftCache: Record<string, DraftCacheEntry>;
  currentDrafts: Record<string, CapacityRowDraft>;
  currentDraftOrder: string[];
  selectedNodeLabel: string | null;
  getQueuePropertyValue: (queuePath: string, property: string) => { value: string };
}

/**
 * Extract changes from capacity editor drafts across all cached labels.
 * Compares current values to existing store values and returns a map of changes.
 */
export function extractChangesFromDrafts({
  draftCache,
  currentDrafts,
  currentDraftOrder,
  selectedNodeLabel,
  getQueuePropertyValue,
}: ExtractChangesParams): Map<string, Record<string, string>> {
  const normalizeValue = (value: string) => value.trim();
  const changesByQueue = new Map<string, Record<string, string>>();

  // Build a complete cache including the current drafts
  const currentCacheKey = selectedNodeLabel ?? DEFAULT_PARTITION_VALUE;
  const completeDraftCache: Record<string, DraftCacheEntry> = {
    ...draftCache,
    [currentCacheKey]: {
      drafts: { ...currentDrafts },
      draftOrder: [...currentDraftOrder],
    },
  };

  // Process all cached labels (including the currently selected one)
  Object.entries(completeDraftCache).forEach(([cacheKey, cachedData]) => {
    const label = cacheKey === DEFAULT_PARTITION_VALUE ? null : cacheKey;
    const capacityProperty = getPropertyNameForLabel(label, 'capacity');
    const maxCapacityProperty = getPropertyNameForLabel(label, 'maximum-capacity');

    cachedData.draftOrder.forEach((queuePath) => {
      const draft = cachedData.drafts[queuePath];
      if (!draft) {
        return;
      }

      const capacityString =
        draft.mode === 'vector'
          ? convertVectorDraftToString(draft.vectorCapacity)
          : draft.capacityValue;
      const maxCapacityString =
        draft.mode === 'vector'
          ? convertVectorDraftToString(draft.vectorMaxCapacity)
          : draft.maxCapacityValue;

      const currentCapacity = normalizeValue(capacityString);
      const currentMaxCapacity = normalizeValue(maxCapacityString);

      const existingCapacity = normalizeValue(
        getQueuePropertyValue(queuePath, capacityProperty).value,
      );
      const existingMaxCapacity = normalizeValue(
        getQueuePropertyValue(queuePath, maxCapacityProperty).value,
      );

      const existingChanges = changesByQueue.get(queuePath) ?? {};

      if (currentCapacity !== existingCapacity) {
        existingChanges[capacityProperty] = currentCapacity;
      }

      if (currentMaxCapacity !== existingMaxCapacity) {
        existingChanges[maxCapacityProperty] = currentMaxCapacity;
      }

      if (Object.keys(existingChanges).length > 0) {
        changesByQueue.set(queuePath, existingChanges);
      }
    });
  });

  return changesByQueue;
}
