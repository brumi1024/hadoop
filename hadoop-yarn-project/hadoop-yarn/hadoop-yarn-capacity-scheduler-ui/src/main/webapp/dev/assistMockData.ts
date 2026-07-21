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
 * Static mock data for the YARN Assist development server.
 * Never included in production bundles.
 */

// ---------------------------------------------------------------------------
// Capabilities and health
// ---------------------------------------------------------------------------

export const CAPABILITIES = {
  chat_enabled: true,
  mcp_read_enabled: true,
  staging_proposals_enabled: true,
  write_enabled: false,
  model_configured: true,
};

export const HEALTH = {
  status: 'ok',
  model_configured: true,
  service: 'yarn-assist',
  version: '0.0.1',
};

// ---------------------------------------------------------------------------
// Conversations
// ---------------------------------------------------------------------------

export const CONVERSATIONS = {
  conversations: [
    {
      conversation_id: 'conv-queue-explain',
      created_at: new Date(Date.now() - 3_600_000).toISOString(),
      updated_at: new Date(Date.now() - 1_800_000).toISOString(),
      title: 'Explain queue hierarchy',
      message_count: 4,
    },
    {
      conversation_id: 'conv-saturated-queue',
      created_at: new Date(Date.now() - 7_200_000).toISOString(),
      updated_at: new Date(Date.now() - 3_600_000).toISOString(),
      title: 'Analytics queue saturation',
      message_count: 6,
    },
    {
      conversation_id: 'conv-cluster-health',
      created_at: new Date(Date.now() - 86_400_000).toISOString(),
      updated_at: new Date(Date.now() - 43_200_000).toISOString(),
      title: 'Cluster health summary',
      message_count: 3,
    },
  ],
};

// ---------------------------------------------------------------------------
// Response text per scenario
// ---------------------------------------------------------------------------

export const RESPONSE_QUEUE_EXPLAIN = `The cluster has a three-level Capacity Scheduler queue hierarchy:

**root** (100%)
- **root.default** (20%) - General-purpose queue for unassigned workloads
- **root.analytics** (40%) - Reserved for Spark and reporting jobs
- **root.etl** (25%) - Extract-transform-load pipelines
- **root.ml** (15%) - Machine learning training with GPU label support

Each leaf queue has a maximum capacity of 80% to prevent monopolization. The \`root.analytics\` queue currently has 22% absolute usage while showing 68% utilization of its reserved allocation.`;

export const RESPONSE_SATURATED_QUEUE = `The \`root.analytics\` queue is consistently running above 80% of its allocated capacity. Here is what I found:

**Current state (from get_scheduler)**
- Allocated capacity: 40% of cluster (26,214 MB)
- Current usage: 22,528 MB (85.9% of allocation)
- 8 running containers, 3 pending

**Active applications (from list_applications)**
- SparkWordCount (application_1703001234567_0042): consuming 8,192 MB across 8 containers
- Two additional smaller jobs are competing for the remaining headroom

**Sibling queue comparison**
- root.etl is at 15% of its 25% allocation - significant headroom available

**Recommendation**
I can propose temporarily increasing root.analytics capacity to 50% and reducing root.etl to 15%. This would give analytics 6,553 MB of additional headroom while the ETL queue has more than enough for its current workload.

Would you like me to prepare that change proposal?`;

export const RESPONSE_CLUSTER_HEALTH = `**Cluster health summary (from get_cluster_metrics)**

| Metric | Value |
|--------|-------|
| Active nodes | 4 / 4 |
| Total memory | 64 GB |
| Available memory | 20 GB (31%) |
| Total vCores | 32 |
| Available vCores | 10 (31%) |
| Running applications | 11 |
| Pending applications | 3 |

The cluster is healthy. No unhealthy or lost nodes detected. Memory utilization is 68% which is within normal operating range. The 3 pending applications are in the root.etl queue waiting for the ETL-Pipeline-Daily job to register its AM container.`;

export const RESPONSE_LIST_APPS = `Here are the currently active applications retrieved from the ResourceManager:

**Running (2)**
- \`application_1703001234567_0042\` — SparkWordCount (spark-user @ root.analytics) — 68.5% — 8,192 MB
- \`application_1703001234567_0041\` — MapReduceHistoryServer (mapred @ root.default) — 100% — 2,048 MB

**Accepted (1)**
- \`application_1703001234567_0040\` — ETL-Pipeline-Daily (etl-service @ root.etl) — waiting for AM registration

**Recently finished**
- \`application_1703001234567_0039\` — DataQualityCheck — SUCCEEDED
- \`application_1703001234567_0038\` — ModelTrainingV2 — FAILED (AM OOM exit code -104)
- \`application_1703001234567_0037\` — BatchExport — KILLED by user`;

export const RESPONSE_TOOL_ERROR = `I attempted to retrieve the scheduler configuration but the ResourceManager returned a timeout error. This can happen when the RM is under load or temporarily unreachable.

I was able to retrieve the scheduler state and cluster metrics before the failure. Based on the scheduler state, the queue hierarchy appears intact.

If the timeout persists, check the ResourceManager logs for GC or leader election activity.`;

// ---------------------------------------------------------------------------
// Tool call sequences per scenario
// ---------------------------------------------------------------------------

export interface MockToolCall {
  name: string;
  args: Record<string, unknown>;
  result: unknown;
  latencyTicks?: number;
}

export function selectToolCalls(message: string): MockToolCall[] {
  const msg = message.toLowerCase();

  if (msg.includes('saturated') || msg.includes('full') || (msg.includes('analytics') && msg.includes('capacity') && !msg.includes('propose') && !msg.includes('increase'))) {
    return [
      {
        name: 'get_scheduler',
        args: {},
        result: { scheduler: { schedulerInfo: { type: 'capacityScheduler', usedCapacity: 68.5 } } },
      },
      {
        name: 'list_applications',
        args: { queue: 'root.analytics', state: 'RUNNING', limit: 20 },
        result: { apps: { app: [{ id: 'application_1703001234567_0042', name: 'SparkWordCount', allocatedMB: 8192 }] } },
        latencyTicks: 8,
      },
    ];
  }

  if (msg.includes('health') || msg.includes('metric') || msg.includes('cluster status')) {
    return [
      {
        name: 'get_cluster_metrics',
        args: {},
        result: { clusterMetrics: { totalMB: 65536, availableMB: 20480, activeNodes: 4, appsRunning: 11 } },
      },
      {
        name: 'list_nodes',
        args: { state: 'RUNNING' },
        result: { nodes: { node: [{ id: 'node1:8041', state: 'RUNNING' }, { id: 'node2:8041', state: 'RUNNING' }] } },
        latencyTicks: 6,
      },
    ];
  }

  if (msg.includes('application') || msg.includes('app') || msg.includes('job') || msg.includes('running')) {
    return [
      {
        name: 'list_applications',
        args: { limit: 20 },
        result: { apps: { app: [{ id: 'application_1703001234567_0042', state: 'RUNNING' }] } },
      },
    ];
  }

  if (msg.includes('propose') || msg.includes('increase') || (msg.includes('change') && !msg.includes('explain'))) {
    return [
      {
        name: 'get_scheduler',
        args: {},
        result: { scheduler: { schedulerInfo: { type: 'capacityScheduler' } } },
      },
      {
        name: 'propose_scheduler_changes',
        args: {
          summary: 'Increase analytics queue capacity from 40% to 50%',
          rationale: 'The analytics queue is consistently at 85% of allocation while root.etl has significant headroom.',
          changes: [
            {
              kind: 'queue_property',
              queue_path: 'root.analytics',
              property: 'capacity',
              new_value: '50',
              old_value: '40',
              reason: 'Queue is saturated; sibling has headroom',
            },
            {
              kind: 'queue_property',
              queue_path: 'root.etl',
              property: 'capacity',
              new_value: '15',
              old_value: '25',
              reason: 'ETL queue has ample headroom; reducing to compensate',
            },
          ],
          warnings: ['Ensure ETL workloads can complete within 15% during peak hours before applying.'],
        },
        result: {
          proposal_id: 'mock-proposal-001',
          summary: 'Increase analytics queue capacity from 40% to 50%',
          rationale: 'The analytics queue is consistently at 85% of allocation while root.etl has significant headroom.',
          changes: [
            {
              kind: 'queue_property',
              queue_path: 'root.analytics',
              property: 'capacity',
              new_value: '50',
              old_value: '40',
              reason: 'Queue is saturated; sibling has headroom',
            },
            {
              kind: 'queue_property',
              queue_path: 'root.etl',
              property: 'capacity',
              new_value: '15',
              old_value: '25',
              reason: 'ETL queue has ample headroom; reducing to compensate',
            },
          ],
          warnings: ['Ensure ETL workloads can complete within 15% during peak hours before applying.'],
        },
        latencyTicks: 12,
      },
    ];
  }

  if (msg.includes('error') || msg.includes('timeout') || msg.includes('fail')) {
    return [
      {
        name: 'get_scheduler_configuration',
        args: {},
        result: { error: 'timeout', message: 'ResourceManager request timed out', retryable: true },
        latencyTicks: 15,
      },
    ];
  }

  // Default: explain queue hierarchy
  return [
    {
      name: 'get_scheduler',
      args: {},
      result: { scheduler: { schedulerInfo: { type: 'capacityScheduler', usedCapacity: 62.5 } } },
    },
  ];
}

export function selectResponse(message: string): string {
  const msg = message.toLowerCase();

  // Proposal/change check comes before saturated-queue to avoid keyword collision
  if (msg.includes('propose') || msg.includes('increase') || (msg.includes('change') && !msg.includes('explain'))) {
    return JSON.stringify({
      proposal_id: 'mock-proposal-001',
      summary: 'Increase analytics queue capacity from 40% to 50%',
      rationale:
        'The analytics queue is consistently at 85% of allocation while root.etl has significant headroom.',
      changes: [
        {
          kind: 'queue_property',
          queue_path: 'root.analytics',
          property: 'capacity',
          new_value: '50',
          old_value: '40',
          reason: 'Queue is saturated; sibling has headroom',
        },
        {
          kind: 'queue_property',
          queue_path: 'root.etl',
          property: 'capacity',
          new_value: '15',
          old_value: '25',
          reason: 'ETL queue has ample headroom; reducing to compensate',
        },
      ],
      warnings: ['Ensure ETL workloads can complete within 15% during peak hours before applying.'],
    });
  }
  if (msg.includes('saturated') || msg.includes('full') || (msg.includes('analytics') && msg.includes('capacity'))) {
    return RESPONSE_SATURATED_QUEUE;
  }
  if (msg.includes('health') || msg.includes('metric') || msg.includes('cluster status')) {
    return RESPONSE_CLUSTER_HEALTH;
  }
  if (msg.includes('application') || msg.includes('app') || msg.includes('job') || msg.includes('running')) {
    return RESPONSE_LIST_APPS;
  }
  if (msg.includes('capacity')) {
    return 'I have prepared a change proposal above. Review the proposed values and click **Stage changes** to add them to the staged-changes panel for further validation before applying.';
  }
  if (msg.includes('error') || msg.includes('timeout') || msg.includes('fail')) {
    return RESPONSE_TOOL_ERROR;
  }
  return RESPONSE_QUEUE_EXPLAIN;
}
