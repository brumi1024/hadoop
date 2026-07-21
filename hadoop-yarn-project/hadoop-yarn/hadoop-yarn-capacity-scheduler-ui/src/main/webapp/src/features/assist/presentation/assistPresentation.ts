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

import type { YarnPageContext } from '~/features/assist/types';

// ---------------------------------------------------------------------------
// Presentation types (frontend-only, no server contract)
// ---------------------------------------------------------------------------

export type AssistPromptIcon =
  | 'activity'
  | 'book-open'
  | 'git-compare'
  | 'list-tree'
  | 'route'
  | 'settings'
  | 'shield-check'
  | 'sparkles'
  | 'tag'
  | 'triangle-alert';

export interface AssistPrompt {
  id: string;
  title: string;
  description: string;
  cue: string;
  icon: AssistPromptIcon;
  prompt: string;
  recommended: boolean;
}

export interface AssistPresentation {
  identity: string;
  hero: {
    leading: string;
    accent: string;
    lede: string;
  };
  placeholder: string;
  prompts: readonly AssistPrompt[];
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

const READ_ONLY_SUFFIX =
  'You can investigate and review proposals, but staging is unavailable in read-only mode.';

function appendReadOnly(lede: string, isReadOnly: boolean): string {
  if (!isReadOnly) return lede;
  return `${lede} ${READ_ONLY_SUFFIX}`;
}

// ---------------------------------------------------------------------------
// Queues - no selection
// ---------------------------------------------------------------------------

function queuesNoSelection(isReadOnly: boolean): AssistPresentation {
  return {
    identity: 'YARN Assist',
    hero: {
      leading: 'Capacity Scheduler ready.',
      accent: 'Explore the queue hierarchy.',
      lede: appendReadOnly(
        'Understand capacity, pressure, and configuration across the scheduler before making changes.',
        isReadOnly,
      ),
    },
    placeholder: 'Ask about queues, capacity, or scheduler health...',
    prompts: [
      {
        id: 'queues-explain-hierarchy',
        title: 'Explain the queue hierarchy',
        description: 'Summarize structure, capacities, usage, and inheritance.',
        cue: 'Explain',
        icon: 'list-tree',
        prompt:
          "Explain the current Capacity Scheduler queue hierarchy, including each queue's configured capacity, maximum capacity, current usage, and important parent-child relationships. Call out anything unusual.",
        recommended: true,
      },
      {
        id: 'queues-find-pressure',
        title: 'Find capacity pressure',
        description: 'Locate saturated queues, pending demand, and unused sibling headroom.',
        cue: 'Investigate',
        icon: 'activity',
        prompt:
          'Find saturated or under-served queues in the current Capacity Scheduler. Compare configured capacity, current usage, pending demand, and available sibling headroom, then rank the issues that need attention.',
        recommended: false,
      },
      {
        id: 'queues-review-config',
        title: 'Review queue configuration',
        description: 'Surface risky limits, inconsistent capacity, and surprising inheritance.',
        cue: 'Review',
        icon: 'shield-check',
        prompt:
          'Review the current queue configuration for risky limits, inconsistent capacities, surprising inheritance, or settings that are likely to cause starvation or unused resources. Explain each finding.',
        recommended: false,
      },
      {
        id: 'queues-propose-rebalance',
        title: 'Propose a safe rebalance',
        description: 'Draft a conservative capacity change for review and staging.',
        cue: 'Draft proposal',
        icon: 'git-compare',
        prompt:
          'Propose a conservative Capacity Scheduler rebalance based on current queue usage and demand. Explain the tradeoffs and return a staged-change proposal, but do not apply anything.',
        recommended: false,
      },
    ],
  };
}

// ---------------------------------------------------------------------------
// Queues - with selected queue
// ---------------------------------------------------------------------------

function queuesWithSelection(queuePath: string, isReadOnly: boolean): AssistPresentation {
  return {
    identity: 'YARN Assist',
    hero: {
      leading: `${queuePath} is in context.`,
      accent: 'Inspect its capacity and pressure.',
      lede: appendReadOnly(
        'Compare this queue with its parent and siblings, then investigate configuration or workload pressure.',
        isReadOnly,
      ),
    },
    placeholder: `Ask about ${queuePath}...`,
    prompts: [
      {
        id: 'queue-explain',
        title: 'Explain this queue',
        description:
          "Read this queue's capacity, usage, limits, applications, and inherited settings.",
        cue: 'Explain',
        icon: 'book-open',
        prompt: `Explain the selected queue ${queuePath}: its place in the hierarchy, configured and effective capacities, current usage, applications, limits, and inherited settings. Call out anything unusual.`,
        recommended: true,
      },
      {
        id: 'queue-diagnose-pressure',
        title: 'Diagnose queue pressure',
        description:
          'Determine whether this queue is saturated, starved, or leaving capacity idle.',
        cue: 'Diagnose',
        icon: 'triangle-alert',
        prompt: `Investigate whether the selected queue ${queuePath} is saturated, starved, or leaving capacity unused. Compare demand, usage, limits, parent capacity, and sibling headroom, then identify the likely cause.`,
        recommended: false,
      },
      {
        id: 'queue-compare-siblings',
        title: 'Compare with siblings',
        description: 'Compare capacity, demand, and application load across sibling queues.',
        cue: 'Compare',
        icon: 'git-compare',
        prompt: `Compare the selected queue ${queuePath} with its sibling queues. Focus on configured capacity, maximum capacity, usage, pending demand, application load, and whether the current split is fair.`,
        recommended: false,
      },
      {
        id: 'queue-propose-adjustment',
        title: 'Propose a safe adjustment',
        description: 'Draft a conservative change that preserves parent and sibling invariants.',
        cue: 'Draft proposal',
        icon: 'settings',
        prompt: `Propose a conservative configuration adjustment for the selected queue ${queuePath} based on current evidence. Preserve parent capacity invariants, explain sibling impact, and return a staged-change proposal without applying it.`,
        recommended: false,
      },
    ],
  };
}

// ---------------------------------------------------------------------------
// Node labels - no selection
// ---------------------------------------------------------------------------

function nodeLabelsNoSelection(isReadOnly: boolean): AssistPresentation {
  return {
    identity: 'YARN Assist',
    hero: {
      leading: 'Node labels ready.',
      accent: 'Audit partition access and capacity.',
      lede: appendReadOnly(
        'Understand which labels exist, where they are assigned, and how queues can consume their resources.',
        isReadOnly,
      ),
    },
    placeholder: 'Ask about node labels or partitions...',
    prompts: [
      {
        id: 'labels-audit',
        title: 'Audit node labels',
        description: 'Summarize label assignments, queue access, capacity, and unused resources.',
        cue: 'Audit',
        icon: 'tag',
        prompt:
          'Audit the current YARN node labels and partitions. Summarize label assignments, accessible queues, configured capacities, and any labels or resources that appear unused or unreachable.',
        recommended: true,
      },
      {
        id: 'labels-explain-allocation',
        title: 'Explain label allocation',
        description: 'Trace how labeled resources become available to scheduler queues.',
        cue: 'Explain',
        icon: 'book-open',
        prompt:
          'Explain how node-label resources are allocated across Capacity Scheduler queues, including accessible labels, default label expressions, and label-specific capacity settings.',
        recommended: false,
      },
      {
        id: 'labels-find-gaps',
        title: 'Find configuration gaps',
        description: 'Find unreachable labels, missing access, and inconsistent capacity.',
        cue: 'Inspect',
        icon: 'triangle-alert',
        prompt:
          'Find node-label configuration gaps such as labeled nodes with no usable queue capacity, queues that cannot reach intended labels, inconsistent label capacities, or surprising defaults.',
        recommended: false,
      },
      {
        id: 'labels-propose-fixes',
        title: 'Propose label fixes',
        description: 'Draft safe label and queue configuration changes for review.',
        cue: 'Draft proposal',
        icon: 'settings',
        prompt:
          'Propose safe node-label and label-specific queue configuration fixes for the issues visible in this cluster. Explain the impact and return a staged-change proposal without applying it.',
        recommended: false,
      },
    ],
  };
}

// ---------------------------------------------------------------------------
// Node labels - with selected label
// ---------------------------------------------------------------------------

function nodeLabelsWithSelection(label: string, isReadOnly: boolean): AssistPresentation {
  return {
    identity: 'YARN Assist',
    hero: {
      leading: `${label} is in context.`,
      accent: 'Inspect its queue access and resources.',
      lede: appendReadOnly(
        'Trace where this label is assigned and whether scheduler capacity can use it effectively.',
        isReadOnly,
      ),
    },
    placeholder: `Ask about the ${label} label...`,
    prompts: [
      {
        id: 'label-explain',
        title: 'Explain this label',
        description: "Read this label's nodes, resources, queue access, and capacity settings.",
        cue: 'Explain',
        icon: 'tag',
        prompt: `Explain the selected node label ${label}: which nodes carry it, how much resource it represents, which queues can access it, and the relevant label-specific capacity settings.`,
        recommended: true,
      },
      {
        id: 'label-find-stranded',
        title: 'Find stranded resources',
        description: 'Check whether access or capacity settings leave labeled resources idle.',
        cue: 'Investigate',
        icon: 'activity',
        prompt: `Check whether resources under the selected node label ${label} are stranded or underused because of queue access, capacity, default expression, or node-assignment configuration.`,
        recommended: false,
      },
      {
        id: 'label-review-access',
        title: 'Review queue access',
        description: 'Identify missing, overly broad, or inconsistent queue access.',
        cue: 'Review',
        icon: 'shield-check',
        prompt: `Review which queues can access the selected node label ${label}. Identify overly broad access, missing access, inconsistent capacities, or settings that do not match current usage.`,
        recommended: false,
      },
      {
        id: 'label-propose-adjustment',
        title: 'Propose a label adjustment',
        description: 'Draft a conservative label-specific change for review and staging.',
        cue: 'Draft proposal',
        icon: 'settings',
        prompt: `Propose a conservative configuration adjustment for the selected node label ${label}. Explain affected queues and resources, then return a staged-change proposal without applying it.`,
        recommended: false,
      },
    ],
  };
}

// ---------------------------------------------------------------------------
// Global settings
// ---------------------------------------------------------------------------

function globalSettings(isReadOnly: boolean): AssistPresentation {
  return {
    identity: 'YARN Assist',
    hero: {
      leading: 'Global settings in context.',
      accent: 'Review scheduler-wide behavior.',
      lede: appendReadOnly(
        'Understand defaults and policies that affect every queue before changing individual configuration.',
        isReadOnly,
      ),
    },
    placeholder: 'Ask about global scheduler settings...',
    prompts: [
      {
        id: 'global-review',
        title: 'Review global settings',
        description: 'Summarize important values and flag risky or surprising configuration.',
        cue: 'Review',
        icon: 'settings',
        prompt:
          'Review the current global Capacity Scheduler settings. Explain the most operationally important values and flag risky, inconsistent, deprecated, or surprising configuration.',
        recommended: true,
      },
      {
        id: 'global-explain-behavior',
        title: 'Explain scheduler behavior',
        description: 'Trace how global policy affects queues, mappings, and preemption.',
        cue: 'Explain',
        icon: 'book-open',
        prompt:
          'Explain how the current global settings affect resource calculation, queue mappings, preemption, application limits, scheduling behavior, and configuration refreshes.',
        recommended: false,
      },
      {
        id: 'global-spot-risky',
        title: 'Spot risky defaults',
        description: 'Find unsafe implicit behavior and settings that deserve explicit values.',
        cue: 'Inspect',
        icon: 'triangle-alert',
        prompt:
          'Identify global Capacity Scheduler settings that rely on defaults or values that may be unsafe for this cluster. Rank findings by operational impact and explain the evidence.',
        recommended: false,
      },
      {
        id: 'global-propose-hardening',
        title: 'Propose hardening changes',
        description: 'Draft conservative scheduler-wide improvements with compatibility notes.',
        cue: 'Draft proposal',
        icon: 'shield-check',
        prompt:
          'Propose conservative scheduler-wide configuration changes that improve safety, fairness, or predictability. Explain compatibility risks and return a staged-change proposal without applying it.',
        recommended: false,
      },
    ],
  };
}

// ---------------------------------------------------------------------------
// Placement rules
// ---------------------------------------------------------------------------

function placementRules(isReadOnly: boolean): AssistPresentation {
  return {
    identity: 'YARN Assist',
    hero: {
      leading: 'Placement rules in context.',
      accent: 'Trace where workloads land.',
      lede: appendReadOnly(
        'Review rule ordering and fallbacks so users and applications reach the intended queues.',
        isReadOnly,
      ),
    },
    placeholder: 'Ask about placement rules...',
    prompts: [
      {
        id: 'placement-audit-ordering',
        title: 'Audit rule ordering',
        description: 'Read every rule in order and verify its match, target, and fallback.',
        cue: 'Audit',
        icon: 'route',
        prompt:
          'Audit the current Capacity Scheduler placement rules in evaluation order. Explain what each rule matches, where it places applications, and whether its fallback behavior is safe.',
        recommended: true,
      },
      {
        id: 'placement-explain-flow',
        title: 'Explain placement flow',
        description: 'Trace users and applications from the first rule to final fallback.',
        cue: 'Trace',
        icon: 'list-tree',
        prompt:
          'Explain the current application placement flow from the first rule to the final fallback. Include user, group, application-name, and default-queue behavior where configured.',
        recommended: false,
      },
      {
        id: 'placement-find-shadowed',
        title: 'Find shadowed rules',
        description: 'Identify unreachable, conflicting, overly broad, or unsafe rules.',
        cue: 'Inspect',
        icon: 'triangle-alert',
        prompt:
          'Find unreachable, shadowed, overly broad, conflicting, or unsafe placement rules. Show which earlier rule prevents each problematic rule from behaving as intended.',
        recommended: false,
      },
      {
        id: 'placement-propose-safer',
        title: 'Propose safer placement',
        description: 'Draft safer ordering or matching while preserving intended routing.',
        cue: 'Draft proposal',
        icon: 'shield-check',
        prompt:
          'Propose a safer placement-rule ordering or configuration for the issues you find. Preserve intended routing, explain migration risks, and return a staged-change proposal without applying it.',
        recommended: false,
      },
    ],
  };
}

// ---------------------------------------------------------------------------
// Unknown route fallback
// ---------------------------------------------------------------------------

function unknownRoute(isReadOnly: boolean): AssistPresentation {
  return {
    identity: 'YARN Assist',
    hero: {
      leading: 'YARN Assist ready.',
      accent: 'Start with scheduler context.',
      lede: appendReadOnly(
        'Ask about queues, resources, applications, or configuration.',
        isReadOnly,
      ),
    },
    placeholder: 'Ask about this YARN scheduler...',
    prompts: [
      {
        id: 'unknown-summarize',
        title: 'Summarize the scheduler',
        description: 'Get a concise view of queues, resources, workload, and current risks.',
        cue: 'Summarize',
        icon: 'sparkles',
        prompt:
          'Summarize the current Capacity Scheduler, including queue structure, resource usage, active workload, and anything that needs attention.',
        recommended: true,
      },
      {
        id: 'unknown-check-health',
        title: 'Check cluster health',
        description: 'Review metrics, node state, scheduler usage, and pending demand.',
        cue: 'Check health',
        icon: 'activity',
        prompt:
          'Check current YARN cluster health using cluster metrics, node state, scheduler usage, and pending applications. Highlight anomalies and likely operational impact.',
        recommended: false,
      },
      {
        id: 'unknown-list-apps',
        title: 'List active applications',
        description: 'See active workload by state and queue, with notable outliers.',
        cue: 'List applications',
        icon: 'list-tree',
        prompt:
          'List and summarize the currently active YARN applications, grouped by state and queue. Highlight applications that are pending, unusually large, or failing.',
        recommended: false,
      },
      {
        id: 'unknown-review-config',
        title: 'Review scheduler configuration',
        description: 'Surface risky, inconsistent, deprecated, or surprising settings.',
        cue: 'Review',
        icon: 'shield-check',
        prompt:
          'Review the current Capacity Scheduler configuration for risky, inconsistent, deprecated, or surprising values. Explain the highest-impact findings first.',
        recommended: false,
      },
    ],
  };
}

// ---------------------------------------------------------------------------
// Public selector
// ---------------------------------------------------------------------------

export function getAssistPresentation(context: YarnPageContext): AssistPresentation {
  const { page_kind, selected_queue_path, selected_node_label, is_read_only } = context;

  switch (page_kind) {
    case 'queues':
      return selected_queue_path
        ? queuesWithSelection(selected_queue_path, is_read_only)
        : queuesNoSelection(is_read_only);

    case 'node-labels':
      return selected_node_label
        ? nodeLabelsWithSelection(selected_node_label, is_read_only)
        : nodeLabelsNoSelection(is_read_only);

    case 'global-settings':
      return globalSettings(is_read_only);

    case 'placement-rules':
      return placementRules(is_read_only);

    default:
      return unknownRoute(is_read_only);
  }
}
