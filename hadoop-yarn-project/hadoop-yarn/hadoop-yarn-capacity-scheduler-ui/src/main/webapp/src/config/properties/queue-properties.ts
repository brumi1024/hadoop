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

import { SPECIAL_VALUES } from '~/types';
import type {
  PropertyDescriptor,
  PropertyCategory,
  PropertyType,
  PropertyCondition,
  PropertyEvaluationContext,
} from '~/types';
import { getCapacityType } from '~/utils/capacityUtils';

const LEGACY_QUEUE_MODE_PROPERTY = 'yarn.scheduler.capacity.legacy-queue-mode.enabled';

type CapacityCategory = 'percentage' | 'weight' | 'absolute' | null;

const getQueueCapacityType = (context: PropertyEvaluationContext): CapacityCategory => {
  const rawCapacity = context.getValue?.('capacity');

  if (rawCapacity && rawCapacity.trim()) {
    return getCapacityType(rawCapacity);
  }

  if (context.queueInfo) {
    const capacityNumber = context.queueInfo.capacity;
    if (typeof capacityNumber === 'number' && !Number.isNaN(capacityNumber)) {
      return getCapacityType(String(capacityNumber));
    }
  }

  return null;
};

const isLegacyQueueModeEnabled: PropertyCondition = ({ getGlobalValue }) => {
  const rawValue = getGlobalValue?.(LEGACY_QUEUE_MODE_PROPERTY);
  if (rawValue == null || rawValue === '') {
    return true;
  }
  return rawValue === 'true';
};

const shouldShowLegacyAutoCreation: PropertyCondition = (context) => {
  // Never show legacy AQC for root queue
  if (context.queuePath === SPECIAL_VALUES.ROOT_QUEUE_NAME) {
    return false;
  }
  return isLegacyQueueModeEnabled(context) && getQueueCapacityType(context) !== 'weight';
};

const shouldShowFlexibleAutoCreation: PropertyCondition = (context) => {
  if (!isLegacyQueueModeEnabled(context)) {
    return true;
  }
  // In legacy mode, always show for root (enableWhen handles the children capacity check)
  if (context.queuePath === SPECIAL_VALUES.ROOT_QUEUE_NAME) {
    return true;
  }
  // For non-root queues in legacy mode, only show if using weight capacity
  return getQueueCapacityType(context) === 'weight';
};

const canEnableFlexibleAutoCreationForRoot: PropertyCondition = (context) => {
  // In non-legacy mode, always enabled
  if (!isLegacyQueueModeEnabled(context)) {
    return true;
  }

  // For non-root queues in legacy mode, always enabled (if visible)
  if (context.queuePath !== SPECIAL_VALUES.ROOT_QUEUE_NAME) {
    return true;
  }

  // For root queue in legacy mode, check if all children use weight mode
  const children = context.queueInfo?.queues?.queue;
  if (!children || children.length === 0) {
    // No children yet, allow enabling
    return true;
  }

  // Check if all direct children use weight-based capacity
  return children.every((child) => {
    const capacity = context.getQueueValue?.(child.queuePath, 'capacity');
    if (!capacity || !capacity.trim()) {
      // If child has no capacity set, be permissive
      return true;
    }
    return getCapacityType(capacity) === 'weight';
  });
};

// Specify only the short config name (without the yarn.scheduler.capacity.<queue-path> prefix)
export const queuePropertyDefinitions: PropertyDescriptor[] = [
  {
    name: 'capacity',
    displayName: 'Capacity',
    description:
      'Queue capacity allocation. Supports percentage (50), weight (2w), or absolute ([memory=1024,vcores=2]) formats.',
    type: 'string' as PropertyType,
    category: 'capacity' as PropertyCategory,
    defaultValue: '',
    required: true,
    templateSupport: true,
  },
  {
    name: 'maximum-capacity',
    displayName: 'Maximum Capacity',
    description:
      'Maximum capacity the queue can expand to. Must be >= capacity. Use -1 for unlimited (100%).',
    type: 'string' as PropertyType,
    category: 'capacity' as PropertyCategory,
    defaultValue: '',
    required: false,
    templateSupport: true,
  },
  {
    name: 'state',
    displayName: 'Queue State',
    description: 'Operational state of the queue.',
    type: 'enum' as PropertyType,
    category: 'capacity' as PropertyCategory,
    defaultValue: 'RUNNING',
    required: false,
    enumValues: [
      {
        value: 'RUNNING',
        label: 'Running',
        description: 'Queue accepts and schedules new applications.',
      },
      {
        value: 'STOPPED',
        label: 'Stopped',
        description: 'Queue rejects new applications but continues existing workloads.',
      },
    ],
    enableWhen: [({ queuePath }) => queuePath !== SPECIAL_VALUES.ROOT_QUEUE_NAME],
  },

  {
    name: 'minimum-user-limit-percent',
    displayName: 'Minimum User Limit Percent',
    description:
      'Minimum percentage of queue resources allocated to a user when there is demand. Default is 100 (no user limits). Overrides the global default if set.',
    type: 'number' as PropertyType,
    category: 'application-limits' as PropertyCategory,
    defaultValue: '',
    required: false,
    templateSupport: true,
    inputRange: { min: 0, max: 100 },
  },
  {
    name: 'user-limit-factor',
    displayName: 'User Limit Factor',
    description:
      "Controls the max amount of resources a single user can consume as a multiple of the queue's capacity. Default is 1 (limited to queue capacity). Set to -1 for unlimited. Overrides the global default if set. Note: auto-queue-creation-v2 with weights automatically sets this to -1 on the created queues.",
    type: 'number' as PropertyType,
    category: 'application-limits' as PropertyCategory,
    defaultValue: '',
    required: false,
    templateSupport: true,
  },

  {
    name: 'maximum-applications',
    displayName: 'Maximum Applications',
    description: 'Maximum concurrent applications (running + pending) in this queue.',
    type: 'number' as PropertyType,
    category: 'application-limits' as PropertyCategory,
    defaultValue: '',
    required: false,
    templateSupport: true,
  },
  {
    name: 'maximum-am-resource-percent',
    displayName: 'Maximum AM Resource Percent',
    description:
      'Maximum percentage of queue resources for Application Masters (expressen in values between 0.0-1.0). Default is 0.1 (10%).',
    type: 'number' as PropertyType,
    category: 'application-limits' as PropertyCategory,
    defaultValue: '',
    required: false,
    templateSupport: true,
    inputRange: { min: 0.0, max: 1.0 },
    displayFormat: {
      suffix: ' (0.0-1.0)',
      decimals: 2,
    },
  },
  {
    name: 'max-parallel-apps',
    displayName: 'Max Parallel Apps',
    description: 'Maximum simultaneously running applications (not pending submissions).',
    type: 'number' as PropertyType,
    category: 'application-limits' as PropertyCategory,
    defaultValue: '',
    required: false,
  },

  {
    name: 'ordering-policy',
    displayName: 'Ordering Policy',
    description: 'Application ordering policy within the queue.',
    type: 'enum' as PropertyType,
    category: 'scheduling' as PropertyCategory,
    defaultValue: '',
    required: false,
    templateSupport: true,
    enumValues: [
      {
        value: 'fifo',
        label: 'FIFO',
        description: 'First-in First-out scheduling; simple ordering for predictable workloads.',
      },
      {
        value: 'fair',
        label: 'Fair',
        description: 'Balances resource allocation across applications for fairness.',
      },
      {
        value: 'fifo-with-partitions',
        label: 'FIFO with Partitions',
        description: 'FIFO scheduling with support for node label partitions.',
      },
      {
        value: 'fifo-for-pending-apps',
        label: 'FIFO for Pending Apps',
        description: 'FIFO ordering specifically for pending applications.',
      },
    ],
  },
  {
    name: 'ordering-policy.fair.enable-size-based-weight',
    displayName: 'Fair Policy Size-Based Weight',
    description:
      'Enable size-based weighting in fair scheduling (only applies when ordering-policy=fair).',
    type: 'boolean' as PropertyType,
    category: 'scheduling' as PropertyCategory,
    defaultValue: '',
    required: false,
    templateSupport: true,
    enableWhen: [({ getValue }) => getValue('ordering-policy') === 'fair'],
  },
  {
    name: 'default-application-priority',
    displayName: 'Default Application Priority',
    description: 'Default priority for applications submitted to this queue.',
    type: 'number' as PropertyType,
    category: 'scheduling' as PropertyCategory,
    defaultValue: '',
    required: false,
    templateSupport: true,
  },

  {
    name: 'acl_submit_applications',
    displayName: 'Submit Applications ACL',
    description:
      'Controls who can submit applications to this queue. Format: "user1,user2 group1,group2", "*" for all, " " (space) for none.',
    type: 'string' as PropertyType,
    category: 'security' as PropertyCategory,
    defaultValue: '',
    required: false,
    templateSupport: true,
  },
  {
    name: 'acl_administer_queue',
    displayName: 'Administer Queue ACL',
    description:
      'Controls who can administer applications on this queue (kill, view, modify). Format: "user1,user2 group1,group2", "*" for all, " " (space) for none.',
    type: 'string' as PropertyType,
    category: 'security' as PropertyCategory,
    defaultValue: '',
    required: false,
    templateSupport: true,
  },

  {
    name: 'maximum-allocation-mb',
    displayName: 'Maximum Allocation MB',
    description: 'Per-queue maximum memory allocation override (MB).',
    type: 'number' as PropertyType,
    category: 'resource' as PropertyCategory,
    defaultValue: '',
    required: false,
    templateSupport: true,
  },
  {
    name: 'maximum-allocation-vcores',
    displayName: 'Maximum Allocation VCores',
    description: 'Per-queue maximum vcore allocation override.',
    type: 'number' as PropertyType,
    category: 'resource' as PropertyCategory,
    defaultValue: '',
    required: false,
    templateSupport: true,
  },

  {
    name: 'maximum-application-lifetime',
    displayName: 'Maximum Application Lifetime',
    description: 'Hard limit on application lifetime in seconds. Use -1 to disable.',
    type: 'number' as PropertyType,
    category: 'application-limits' as PropertyCategory,
    defaultValue: '',
    required: false,
    templateSupport: true,
  },
  {
    name: 'default-application-lifetime',
    displayName: 'Default Application Lifetime',
    description:
      'Default application lifetime in seconds. Cannot exceed maximum-application-lifetime.',
    type: 'number' as PropertyType,
    category: 'application-limits' as PropertyCategory,
    defaultValue: '',
    required: false,
    templateSupport: true,
  },

  {
    name: 'disable_preemption',
    displayName: 'Disable Preemption',
    description: 'Disable preemption for this queue.',
    type: 'boolean' as PropertyType,
    category: 'preemption' as PropertyCategory,
    defaultValue: '',
    required: false,
    templateSupport: true,
  },
  {
    name: 'intra-queue-preemption.disable_preemption',
    displayName: 'Disable Intra-Queue Preemption',
    description: 'Disable preemption within this queue.',
    type: 'boolean' as PropertyType,
    category: 'preemption' as PropertyCategory,
    defaultValue: '',
    required: false,
    templateSupport: true,
  },
  {
    name: 'priority',
    displayName: 'Queue Priority',
    description: 'Priority of this queue relative to other sibling queues. Default is 0.',
    type: 'number' as PropertyType,
    category: 'preemption' as PropertyCategory,
    defaultValue: '0',
    required: false,
    templateSupport: true,
  },

  {
    name: 'auto-create-child-queue.enabled',
    displayName: 'Legacy Queue Auto-Creation',
    description: 'Enable leaf queue auto-creation (legacy mode).',
    type: 'boolean' as PropertyType,
    category: 'dynamic-queues' as PropertyCategory,
    defaultValue: '',
    required: false,
    showWhen: [shouldShowLegacyAutoCreation],
    enableWhen: [
      ({ queueInfo }) => {
        // Disable if queue has children
        return !queueInfo?.queues?.queue || queueInfo.queues.queue.length === 0;
      },
    ],
  },
  {
    name: 'auto-queue-creation-v2.enabled',
    displayName: 'Flexible Queue Auto-Creation',
    description:
      'Enable flexible queue auto-creation (parent and leaf queues). In legacy queue mode, root queue requires all child queues to use weight-based capacity.',
    type: 'boolean' as PropertyType,
    category: 'dynamic-queues' as PropertyCategory,
    defaultValue: '',
    required: false,
    showWhen: [shouldShowFlexibleAutoCreation],
    enableWhen: [canEnableFlexibleAutoCreationForRoot],
  },
  {
    name: 'auto-queue-creation-v2.max-queues',
    displayName: 'Max Auto-Created Queues',
    description: 'Maximum dynamic queues under this parent.',
    type: 'number' as PropertyType,
    category: 'dynamic-queues' as PropertyCategory,
    defaultValue: '',
    required: false,
    showWhen: [shouldShowFlexibleAutoCreation],
    enableWhen: [({ getValue }) => getValue('auto-queue-creation-v2.enabled') === 'true'],
  },

  // Node Label Access Control Properties (queue-specific configuration)
  {
    name: 'accessible-node-labels',
    displayName: 'Accessible Node Labels',
    description:
      'Comma-separated list of node labels this queue can access. Use "*" for all labels, empty for default partition only.',
    type: 'string' as PropertyType,
    category: 'node-labels' as PropertyCategory,
    defaultValue: '',
    required: false,
    templateSupport: true,
  },
  {
    name: 'default-node-label-expression',
    displayName: 'Default Node Label Expression',
    description:
      'Default node label expression for applications submitted to this queue. Empty for default partition.',
    type: 'string' as PropertyType,
    category: 'node-labels' as PropertyCategory,
    defaultValue: '',
    required: false,
  },
];
