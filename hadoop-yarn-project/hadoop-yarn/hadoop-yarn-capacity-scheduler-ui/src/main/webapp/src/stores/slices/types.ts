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
 * Shared types for store slices
 */

import type { YarnApiClient } from '~/lib/api/YarnApiClient';
import type {
  SchedulerInfo,
  NodeLabel,
  NodeInfo,
  NodeToLabelMapping,
  StagedChange,
  QueueInfo,
  QueueCapacitiesByPartition,
  ValidationIssue,
} from '~/types';
import type { ExplainIndex } from '~/features/validation/service';
import type { PlacementRulesSlice } from './placementRulesSlice';
import type { CapacityEditorSlice } from './capacityEditorSlice';

export interface BaseStoreSlice {
  apiClient: YarnApiClient;
}

export interface SchedulerDataSlice {
  schedulerData: SchedulerInfo | null;
  configData: Map<string, string>;
  configVersion: number;
  configEtag: string | null;
  isLoading: boolean;
  error: string | null;
  errorContext: 'load' | 'mutation' | 'nodeLabels' | null;
  isReadOnly: boolean;

  loadInitialData: () => Promise<void>;
  refreshSchedulerData: () => Promise<void>;
}

export interface NodeLabelsSlice {
  nodeLabels: NodeLabel[];
  nodes: NodeInfo[];
  nodeToLabels: NodeToLabelMapping[];
  selectedNodeLabel: string | null;

  selectNodeLabel: (label: string | null) => void;
  addNodeLabel: (name: string, exclusivity: boolean) => Promise<void>;
  removeNodeLabel: (name: string) => Promise<void>;
  assignNodeToLabel: (nodeId: string, labelName: string | null) => Promise<void>;
}

export interface StagedChangesSlice {
  stagedChanges: StagedChange[];
  applyError: string | null;
  /** Issues of the last validated proposal, from validate/v2. */
  serverIssues: ValidationIssue[];
  /** Warnings of the last applied proposal, kept until dismissed or the next edit. */
  appliedWarnings: ValidationIssue[];
  /** Set when the validation request itself failed, so no result is available. */
  validationError: string | null;
  isValidatingProposal: boolean;
  /** Identity of the current proposal and of the one serverIssues belong to. */
  proposalKey: string | null;
  validatedProposalKey: string | null;
  /** Set by a 412 on apply; cleared by compareFreshBaseline. */
  proposalStale: boolean;
  /** Resolved value sources per queue path and full property key, from validate/v2 explain. */
  explain: ExplainIndex;
  explainProposalKey: string | null;
  validateProposal: () => Promise<boolean>;
  /**
   * Loads the explain entry of one queue for the current proposal, unless the pending
   * validation will provide it. Requests each queue at most once per proposal.
   */
  loadExplain: (queuePath: string) => Promise<void>;
  compareFreshBaseline: () => Promise<void>;
  dismissAppliedWarnings: () => void;
  stageQueueChange: (queuePath: string, property: string, value: string) => void;
  stageGlobalChange: (
    property: string,
    value: string | Record<string, unknown> | unknown[],
  ) => void;
  stageQueueAddition: (
    parentPath: string,
    queueName: string,
    config: Record<string, string>,
  ) => void;
  stageQueueRemoval: (queuePath: string) => void;
  stageLabelQueueChange: (
    queuePath: string,
    label: string,
    property: string,
    value: string,
  ) => void;
  applyChanges: () => Promise<void>;
  revertChange: (changeId: string) => void;
  revertQueueDeletion: (queuePath: string) => void;
  clearAllChanges: () => void;
  clearQueueChanges: (queuePath: string) => void;
  hasUnsavedChanges: () => boolean;
  getChangesForQueue: (queuePath: string) => StagedChange[];
  hasPendingDeletion: (queuePath: string) => boolean;
  getStagedChangeById: (changeId: string) => StagedChange | undefined;
  getLabelChangesForQueue: (queuePath: string, label: string) => StagedChange[];
}

export interface QueueSelectionSlice {
  selectedQueuePath: string | null;
  comparisonQueues: string[];
  isPropertyPanelOpen: boolean;
  propertyPanelInitialTab: 'overview' | 'info' | 'settings';
  shouldOpenTemplateConfig: boolean;
  isComparisonModeActive: boolean;

  selectQueue: (queuePath: string | null) => void;
  toggleComparisonQueue: (queuePath: string) => void;
  setPropertyPanelOpen: (isOpen: boolean) => void;
  setPropertyPanelInitialTab: (tab: 'overview' | 'info' | 'settings') => void;
  requestTemplateConfigOpen: () => void;
  clearTemplateConfigRequest: () => void;
  clearComparisonQueues: () => void;
  canCompareQueues: () => boolean;
  getComparisonData: () => Map<string, Record<string, string>>;
  toggleComparisonMode: () => void;
  setComparisonMode: (active: boolean) => void;
}

export interface QueueDataSlice {
  getQueuePropertyValue: (
    queuePath: string,
    property: string,
  ) => { value: string; isStaged: boolean };
  getGlobalPropertyValue: (property: string) => { value: string; isStaged: boolean };
  hasQueueProperty: (queuePath: string, property: string) => boolean;
  getQueueByPath: (queuePath: string) => QueueInfo | null;
  getChildQueues: (parentPath: string) => QueueInfo[];
  getQueuePartitionCapacities: (
    queuePath: string,
    partitionName: string,
  ) => QueueCapacitiesByPartition | null;
}

export interface SearchSlice {
  // State
  searchQuery: string;
  searchContext: 'queues' | 'nodes' | 'settings' | null;
  isSearchFocused: boolean;
  selectedNodeLabelFilter: string; // '' for DEFAULT partition

  // Actions
  setSearchQuery: (query: string) => void;
  setSearchContext: (context: 'queues' | 'nodes' | 'settings' | null) => void;
  clearSearch: () => void;
  setSearchFocused: (focused: boolean) => void;
  selectNodeLabelFilter: (label: string) => void;

  // Computed
  getFilteredQueues: () => SchedulerInfo | null;
  getFilteredNodes: () => NodeInfo[];
  getFilteredSettings: () => import('~/types').PropertyDescriptor[];
  getSearchResults: () => { count: number; hasResults: boolean };
  getQueueAccessibility: (queuePath: string, label: string) => boolean;
  getQueueLabelCapacity: (
    queuePath: string,
    label: string,
  ) => {
    capacity: string;
    maxCapacity: string;
    absoluteCapacity: string;
    isLabelSpecific: boolean;
    label: string;
    hasAccess: boolean;
    canUseLabel: boolean;
  } | null;
}

export type SchedulerStore = BaseStoreSlice &
  SchedulerDataSlice &
  NodeLabelsSlice &
  StagedChangesSlice &
  QueueSelectionSlice &
  QueueDataSlice &
  PlacementRulesSlice &
  SearchSlice &
  CapacityEditorSlice;
