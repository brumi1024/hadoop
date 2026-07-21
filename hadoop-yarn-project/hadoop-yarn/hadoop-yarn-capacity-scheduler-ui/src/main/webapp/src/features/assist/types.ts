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

/** Page context sent with each new conversation. */
export interface YarnPageContext {
  page_kind: 'queues' | 'node-labels' | 'global-settings' | 'placement-rules' | null;
  selected_queue_path: string | null;
  selected_node_label: string | null;
  search_context: string | null;
  is_read_only: boolean;
  staged_change_count: number;
  staged_change_summary: string | null;
}

/** Conversation summary returned by the list endpoint. */
export interface ConversationSummary {
  conversation_id: string;
  created_at: string;
  updated_at: string;
  title: string;
  message_count: number;
}

/** Capabilities returned by the capabilities endpoint. */
export interface AssistCapabilities {
  chat_enabled: boolean;
  mcp_read_enabled: boolean;
  staging_proposals_enabled: boolean;
  write_enabled: boolean;
  model_configured: boolean;
}

// ---------------------------------------------------------------------------
// Scheduler change proposal (must stay in sync with Python ProposalChange)
// ---------------------------------------------------------------------------

export type ProposalChangeKind =
  | 'queue_property'
  | 'global_property'
  | 'queue_addition'
  | 'queue_removal'
  | 'label_queue_property';

export interface QueuePropertyChange {
  kind: 'queue_property';
  queue_path: string;
  property: string;
  new_value: string;
  old_value: string;
  reason: string;
}

export interface GlobalPropertyChange {
  kind: 'global_property';
  property: string;
  new_value: string | Record<string, unknown> | unknown[];
  old_value: string | Record<string, unknown> | unknown[];
  reason: string;
}

export interface QueueAdditionChange {
  kind: 'queue_addition';
  parent_path: string;
  queue_name: string;
  config: Record<string, string>;
  reason: string;
}

export interface QueueRemovalChange {
  kind: 'queue_removal';
  queue_path: string;
  reason: string;
}

export interface LabelQueuePropertyChange {
  kind: 'label_queue_property';
  queue_path: string;
  label: string;
  property: string;
  new_value: string;
  old_value: string;
  reason: string;
}

export type ProposalChange =
  | QueuePropertyChange
  | GlobalPropertyChange
  | QueueAdditionChange
  | QueueRemovalChange
  | LabelQueuePropertyChange;

export interface SchedulerChangeProposal {
  proposal_id: string;
  summary: string;
  rationale: string;
  changes: ProposalChange[];
  warnings: string[];
}
