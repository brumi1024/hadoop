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
 * Staged changes slice - handles all change management operations.
 *
 * Validation is done by the server only. Every change to the staged proposal or to the
 * loaded baseline schedules a debounced POST /scheduler-conf/validate/v2; responses for a
 * proposal that has changed in the meantime are discarded. Apply validates the proposal
 * once more, then writes it with If-Match set to the ETag of the last GET, and every later
 * write of the same apply with the ETag the previous write returned.
 */

import type { StateCreator } from 'zustand';
import { nanoid } from 'nanoid';
import { SPECIAL_VALUES } from '~/types';
import type { StagedChange, ValidationIssue } from '~/types';
import {
  buildGlobalPropertyKey,
  buildNodeLabelPropertyKey,
  buildPropertyKey,
} from '~/utils/propertyUtils';
import {
  applyQueueStates,
  buildSubmissionPlan,
  type SubmissionPlan,
} from '~/features/staged-changes/utils/queueStateManager';
import { getQueueNameValidationError } from '~/types';
import {
  createStoreError,
  ERROR_CODES,
  extractErrorMessage,
  isNetworkError,
  isPreconditionFailed,
  YarnApiError,
} from '~/lib/errors';
import { assertWritable } from '~/lib/errors/readOnlyGuard';
import type { StagedChangesSlice, SchedulerStore } from './types';
import { indexExplain, toValidationIssues } from '~/features/validation/service';

export const VALIDATION_DEBOUNCE_MS = 400;

export const STALE_BASELINE_MESSAGE =
  'The scheduler configuration changed after it was loaded. Nothing was applied. ' +
  'Reload it and compare your staged changes before applying again.';

const UNKNOWN_OUTCOME_MESSAGE =
  'The outcome of the configuration update is unknown. It was not retried. ' +
  'Reload the configuration to see whether it was applied.';

const CONCURRENT_CHANGE_MESSAGE =
  'The scheduler configuration was changed by someone else while the staged changes were ' +
  'being applied, so they were not applied. ' +
  'Reload it and compare your staged changes before applying again.';

/** A write whose response was lost, for example to a network failure or a timeout. */
const isUnknownOutcome = (error: unknown) => !(error instanceof YarnApiError);

const describeWriteError = (error: unknown) => {
  if (isPreconditionFailed(error)) {
    return 'the scheduler configuration was changed by someone else in the meantime';
  }
  if (isUnknownOutcome(error)) {
    return `its outcome is unknown (${extractErrorMessage(error)})`;
  }
  return extractErrorMessage(error);
};

const stoppedQueuesNote = (queues: Set<string>) =>
  queues.size > 0
    ? ` These queues were stopped for the apply and may still be STOPPED: ${Array.from(queues).join(', ')}.`
    : '';

type MutationErrorState = Pick<
  SchedulerStore,
  'applyError' | 'error' | 'errorContext' | 'appliedWarnings'
>;
const clearMutationError = (state: MutationErrorState) => {
  state.appliedWarnings = [];
  if (state.applyError) {
    state.applyError = null;
  }
  if (state.errorContext === 'mutation') {
    state.error = null;
    state.errorContext = null;
  }
};

const submissionPlan = (state: SchedulerStore): SubmissionPlan =>
  buildSubmissionPlan(state.stagedChanges, (path) => state.getChildQueues(path));

/**
 * Identity of what the server validates: the baseline it applies to and the proposal as
 * it will be submitted.
 */
const proposalIdentity = (state: SchedulerStore, plan: SubmissionPlan): string =>
  JSON.stringify([state.configEtag ?? state.configVersion, plan.validationRequest]);

/**
 * A response that says the proposal is invalid always has a visible reason.
 */
const issuesWithReason = (valid: boolean, issues: ValidationIssue[]): ValidationIssue[] => {
  if (valid || issues.some((issue) => issue.severity === 'error')) {
    return issues;
  }
  return [
    ...issues,
    {
      queuePath: null,
      propertyKey: null,
      ruleId: 'invalid-proposal',
      severity: 'error',
      message: 'The server rejected the proposed configuration without details.',
    },
  ];
};

export const createStagedChangesSlice: StateCreator<
  SchedulerStore,
  [['zustand/immer', never]],
  [],
  StagedChangesSlice
> = (set, get, api) => {
  let validationTimer: ReturnType<typeof setTimeout> | null = null;
  // Queues whose explain was requested for explainRequestedFor. The server leaves out queues
  // that are not in the proposed tree (e.g. one staged for removal), so a missing entry in
  // the response does not mean the request has to be repeated.
  let explainRequestedFor: string | null = null;
  const explainRequested = new Set<string>();

  const cancelScheduledValidation = () => {
    if (validationTimer) {
      clearTimeout(validationTimer);
      validationTimer = null;
    }
  };

  const scheduleValidation = () => {
    cancelScheduledValidation();
    const state = get();
    const identity = proposalIdentity(state, submissionPlan(state));
    if (identity === state.proposalKey) {
      return;
    }
    if (state.stagedChanges.length === 0) {
      set((next) => {
        next.proposalKey = identity;
        next.serverIssues = [];
        next.validationError = null;
        next.isValidatingProposal = false;
      });
      return;
    }
    set((next) => {
      next.proposalKey = identity;
      next.isValidatingProposal = true;
    });
    validationTimer = setTimeout(() => {
      validationTimer = null;
      void get().validateProposal();
    }, VALIDATION_DEBOUNCE_MS);
  };

  // Validation follows the store, not whichever panel is open.
  api?.subscribe?.((state, previous) => {
    if (
      state.stagedChanges !== previous.stagedChanges ||
      state.configEtag !== previous.configEtag ||
      state.schedulerData !== previous.schedulerData
    ) {
      scheduleValidation();
    }
  });

  return {
    stagedChanges: [],
    applyError: null,
    serverIssues: [],
    appliedWarnings: [],
    validationError: null,
    isValidatingProposal: false,
    proposalKey: null,
    validatedProposalKey: null,
    proposalStale: false,
    explain: {},
    explainProposalKey: null,

    validateProposal: async () => {
      cancelScheduledValidation();
      const state = get();
      const plan = submissionPlan(state);
      const identity = proposalIdentity(state, plan);
      const isCurrent = () => proposalIdentity(get(), submissionPlan(get())) === identity;

      if (state.stagedChanges.length === 0) {
        set((next) => {
          next.proposalKey = identity;
          next.validatedProposalKey = identity;
          next.serverIssues = [];
          next.validationError = null;
          next.isValidatingProposal = false;
        });
        return true;
      }

      set((next) => {
        next.proposalKey = identity;
        next.isValidatingProposal = true;
      });

      try {
        const response = await state.apiClient.validateSchedulerConf(plan.validationRequest, {
          explain: 'affected',
        });
        if (!isCurrent()) {
          return false;
        }
        const issues = issuesWithReason(response.valid, toValidationIssues(response.issues));
        const explained = indexExplain(response.explain);
        set((next) => {
          next.serverIssues = issues;
          next.validationError = null;
          next.validatedProposalKey = identity;
          next.isValidatingProposal = false;
          next.explain =
            next.explainProposalKey === identity ? { ...next.explain, ...explained } : explained;
          next.explainProposalKey = identity;
        });
        return response.valid;
      } catch (error) {
        if (isCurrent()) {
          set((next) => {
            next.serverIssues = [];
            next.validationError = `Validation failed: ${extractErrorMessage(error)}`;
            next.validatedProposalKey = null;
            next.isValidatingProposal = false;
          });
        }
        return false;
      }
    },

    loadExplain: async (queuePath) => {
      const state = get();
      const plan = submissionPlan(state);
      const identity = proposalIdentity(state, plan);
      // The scheduled validation explains the affected queues; ask for this queue only if
      // that answer leaves it out.
      if (state.isValidatingProposal && state.proposalKey === identity) {
        return;
      }
      if (state.explainProposalKey === identity && state.explain[queuePath]) {
        return;
      }
      if (explainRequestedFor !== identity) {
        explainRequestedFor = identity;
        explainRequested.clear();
      }
      if (explainRequested.has(queuePath)) {
        return;
      }
      explainRequested.add(queuePath);
      try {
        const response = await state.apiClient.validateSchedulerConf(plan.validationRequest, {
          explain: [queuePath],
        });
        if (proposalIdentity(get(), submissionPlan(get())) !== identity) {
          return;
        }
        const explained = indexExplain(response.explain);
        if (get().explainProposalKey === identity && Object.keys(explained).length === 0) {
          return;
        }
        set((next) => {
          next.explain =
            next.explainProposalKey === identity ? { ...next.explain, ...explained } : explained;
          next.explainProposalKey = identity;
        });
      } catch (error) {
        // Source labels are informational; validation errors are reported by validateProposal.
        console.warn('Failed to load resolved value sources:', extractErrorMessage(error));
        if (explainRequestedFor === identity) {
          explainRequested.delete(queuePath);
        }
      }
    },

    compareFreshBaseline: async () => {
      // An explicit user action: keep the edits and show what changed underneath them.
      await get().loadInitialData();
      set((state) => {
        state.proposalStale = false;
        state.applyError = null;
        state.stagedChanges.forEach((change) => {
          if (change.type !== 'update') return;
          const key =
            change.queuePath === SPECIAL_VALUES.GLOBAL_QUEUE_PATH
              ? buildGlobalPropertyKey(change.property)
              : buildPropertyKey(change.queuePath, change.property);
          const current = state.configData.get(key);
          if (current !== change.oldValue) {
            change.baselineDrift = { stagedAgainst: change.oldValue, current };
            change.oldValue = current;
          }
        });
      });
      await get().validateProposal();
    },

    dismissAppliedWarnings: () => {
      set((state) => {
        state.appliedWarnings = [];
      });
    },

    stageQueueChange: (queuePath, property, value) => {
      if (!queuePath || !queuePath.startsWith(SPECIAL_VALUES.ROOT_QUEUE_NAME)) {
        throw createStoreError(
          ERROR_CODES.INVALID_QUEUE_PATH,
          `Invalid queue path: ${queuePath}. Queue paths must start with '${SPECIAL_VALUES.ROOT_QUEUE_NAME}'`,
        );
      }

      if (!property || property.trim() === '') {
        throw createStoreError(ERROR_CODES.INVALID_PROPERTY_NAME, 'Property name cannot be empty');
      }

      let mutated = false;
      set((state) => {
        const propertyKey = buildPropertyKey(queuePath, property);
        const originalValue = state.configData.get(propertyKey);

        const existingIndex = state.stagedChanges.findIndex(
          (c) => c.queuePath === queuePath && c.property === property,
        );

        // If the new value matches the original value, remove the staged change
        if (value === originalValue && existingIndex >= 0) {
          state.stagedChanges.splice(existingIndex, 1);
          mutated = true;
        } else if (existingIndex >= 0) {
          // Update existing staged change
          state.stagedChanges[existingIndex].newValue = value;
          mutated = true;
        } else if (value !== originalValue) {
          // Only create a new staged change if the value differs from the original
          const change: StagedChange = {
            id: nanoid(),
            type: 'update',
            queuePath,
            property,
            oldValue: originalValue,
            newValue: value,
            timestamp: Date.now(),
          };
          state.stagedChanges.push(change);
          mutated = true;
        }

        if (mutated) {
          clearMutationError(state);
        }
      });
    },

    stageGlobalChange: (property, value) => {
      let mutated = false;
      set((state) => {
        // For JSON properties like placement rules, stringify the value if it's an object
        let stringValue: string;
        if (property === SPECIAL_VALUES.MAPPING_RULE_JSON_PROPERTY && typeof value === 'object') {
          stringValue = JSON.stringify(value);
        } else {
          stringValue = String(value);
        }

        const propertyKey = buildGlobalPropertyKey(property);
        const originalValue = state.configData.get(propertyKey);

        const existingIndex = state.stagedChanges.findIndex(
          (c) => c.queuePath === SPECIAL_VALUES.GLOBAL_QUEUE_PATH && c.property === property,
        );

        // If the new value matches the original value, remove the staged change
        if (stringValue === originalValue && existingIndex >= 0) {
          state.stagedChanges.splice(existingIndex, 1);
          mutated = true;
        } else if (existingIndex >= 0) {
          // Update existing staged change
          state.stagedChanges[existingIndex].newValue = stringValue;
          mutated = true;
        } else if (stringValue !== originalValue) {
          // Only create a new staged change if the value differs from the original
          const change: StagedChange = {
            id: nanoid(),
            type: 'update',
            queuePath: SPECIAL_VALUES.GLOBAL_QUEUE_PATH,
            property,
            oldValue: originalValue,
            newValue: stringValue,
            timestamp: Date.now(),
          };
          state.stagedChanges.push(change);
          mutated = true;
        }

        if (mutated) {
          clearMutationError(state);
        }
      });
    },

    stageQueueAddition: (parentPath, queueName, config) => {
      const queueNameError = getQueueNameValidationError(queueName);
      if (queueNameError) {
        throw createStoreError(
          ERROR_CODES.INVALID_QUEUE_NAME,
          `Invalid queue name: "${queueName}". ${queueNameError}`,
        );
      }

      const newQueuePath =
        parentPath === SPECIAL_VALUES.ROOT_QUEUE_NAME
          ? `${SPECIAL_VALUES.ROOT_QUEUE_NAME}.${queueName}`
          : `${parentPath}.${queueName}`;

      set((state) => {
        let mutated = false;
        // Check if queue already exists
        const queue = get().getQueueByPath(newQueuePath);
        if (queue) {
          throw createStoreError(
            ERROR_CODES.QUEUE_ALREADY_EXISTS,
            `Queue "${newQueuePath}" already exists`,
          );
        }

        // Remove any existing changes for the same queue
        const beforeLength = state.stagedChanges.length;
        state.stagedChanges = state.stagedChanges.filter((c) => c.queuePath !== newQueuePath);
        if (state.stagedChanges.length !== beforeLength) {
          mutated = true;
        }

        // Create one staged change per property
        Object.entries(config).forEach(([property, value]) => {
          const change: StagedChange = {
            id: nanoid(),
            type: 'add',
            queuePath: newQueuePath,
            property,
            oldValue: undefined,
            newValue: value,
            timestamp: Date.now(),
          };
          state.stagedChanges.push(change);
          mutated = true;
        });

        if (mutated) {
          clearMutationError(state);
        }
      });
    },

    stageQueueRemoval: (queuePath) => {
      set((state) => {
        state.stagedChanges = state.stagedChanges.filter((c) => c.queuePath !== queuePath);

        const change: StagedChange = {
          id: nanoid(),
          type: 'remove',
          queuePath,
          property: SPECIAL_VALUES.QUEUE_MARKER,
          oldValue: 'exists',
          newValue: undefined,
          timestamp: Date.now(),
        };

        state.stagedChanges.push(change);
        clearMutationError(state);
      });
    },

    stageLabelQueueChange: (queuePath, label, property, value) => {
      if (!queuePath || !queuePath.startsWith(SPECIAL_VALUES.ROOT_QUEUE_NAME)) {
        throw createStoreError(ERROR_CODES.INVALID_QUEUE_PATH, `Invalid queue path: ${queuePath}`);
      }

      if (!label || label.trim() === '') {
        throw createStoreError(ERROR_CODES.INVALID_PROPERTY_NAME, 'Label name cannot be empty');
      }

      if (!property || property.trim() === '') {
        throw createStoreError(ERROR_CODES.INVALID_PROPERTY_NAME, 'Property name cannot be empty');
      }

      const fullPropertyName = `accessible-node-labels.${label}.${property}`;

      let mutated = false;
      set((state) => {
        const propertyKey = buildNodeLabelPropertyKey(queuePath, label, property);
        const originalValue = state.configData.get(propertyKey);

        const existingIndex = state.stagedChanges.findIndex(
          (c) => c.queuePath === queuePath && c.property === fullPropertyName,
        );

        // If the new value matches the original value, remove the staged change
        if (value === originalValue && existingIndex >= 0) {
          state.stagedChanges.splice(existingIndex, 1);
          mutated = true;
        } else if (existingIndex >= 0) {
          // Update existing staged change
          state.stagedChanges[existingIndex].newValue = value;
          state.stagedChanges[existingIndex].label = label;
          mutated = true;
        } else if (value !== originalValue) {
          // Only create a new staged change if the value differs from the original
          const change: StagedChange = {
            id: nanoid(),
            type: 'update',
            queuePath,
            property: fullPropertyName,
            oldValue: originalValue,
            newValue: value,
            timestamp: Date.now(),
            label,
          };
          state.stagedChanges.push(change);
          mutated = true;
        }

        if (mutated) {
          clearMutationError(state);
        }
      });
    },

    applyChanges: async () => {
      const changes = get().stagedChanges;
      if (changes.length === 0) return;

      // Block applying changes in read-only mode
      assertWritable(get().isReadOnly, 'apply changes');

      const fail = (message: string, error?: unknown): never => {
        set((state) => {
          state.error = message;
          state.errorContext = 'mutation';
          state.applyError = message;
          state.isLoading = false;
        });
        throw createStoreError(
          isNetworkError(error) ? ERROR_CODES.NETWORK_ERROR : ERROR_CODES.APPLY_CHANGES_FAILED,
          message,
          error,
        );
      };

      if (get().proposalStale) {
        fail(STALE_BASELINE_MESSAGE);
      }

      set((state) => {
        state.isLoading = true;
        clearMutationError(state);
      });

      // Validate the exact submission before any write, including the queue stops below.
      const plan = submissionPlan(get());
      const identity = proposalIdentity(get(), plan);
      const valid = await get().validateProposal();
      if (!valid || proposalIdentity(get(), submissionPlan(get())) !== identity) {
        const errors = get().serverIssues.filter((issue) => issue.severity === 'error');
        fail(
          get().validationError ??
            (errors.length > 0
              ? `Validation failed: ${errors.map((issue) => issue.message).join('; ')}`
              : 'The staged changes changed during validation. Apply again.'),
        );
      }
      const warnings = get().serverIssues.filter((issue) => issue.severity === 'warning');

      const apiClient = get().apiClient;
      // Every write is conditional on the ETag of the configuration the previous write
      // committed, starting from the loaded baseline, so a change someone else makes between
      // two writes of this apply fails the next write instead of being overwritten.
      let etag: string | null = get().configEtag;
      const writeStates = async (queues: Iterable<string>, state: 'STOPPED' | 'RUNNING') => {
        const result = await applyQueueStates(queues, state, apiClient, { ifMatch: etag });
        if (result) {
          etag = result.etag;
        }
      };

      const removalQueues = new Set(plan.removalQueuesToStop);
      const stopped = new Set([
        ...plan.parentQueuesToStop,
        ...plan.removalQueuesToStop,
        ...plan.autoCreationQueuesToStop,
      ]);
      const restartedAfterCommit = new Set(
        [...plan.parentQueuesToStop, ...plan.autoCreationQueuesToStop].filter(
          (queue) => !removalQueues.has(queue),
        ),
      );

      // Nothing more is written after a write that found someone else's change or whose
      // outcome is unknown: the user reloads and reconciles instead.
      const failUncommitted = (error: unknown, possiblyStopped: Set<string>): never => {
        const note = stoppedQueuesNote(possiblyStopped);
        if (isPreconditionFailed(error)) {
          set((state) => {
            state.proposalStale = true;
          });
          fail(note ? `${CONCURRENT_CHANGE_MESSAGE}${note}` : STALE_BASELINE_MESSAGE, error);
        }
        if (isUnknownOutcome(error)) {
          fail(`${UNKNOWN_OUTCOME_MESSAGE} (${extractErrorMessage(error)})${note}`, error);
        }
        return fail(extractErrorMessage(error), error);
      };

      if (stopped.size > 0) {
        try {
          await writeStates(stopped, 'STOPPED');
        } catch (error) {
          failUncommitted(error, isUnknownOutcome(error) ? stopped : new Set());
        }
      }

      try {
        etag = (await apiClient.updateSchedulerConf(plan.request, { ifMatch: etag })).etag;
      } catch (error) {
        if (isPreconditionFailed(error) || isUnknownOutcome(error)) {
          failUncommitted(error, stopped);
        }
        // The server rejected the mutation, so the configuration is still the one the stop
        // committed: restart the stopped queues, conditional on it.
        let restartError = '';
        try {
          await writeStates(stopped, 'RUNNING');
        } catch (restart) {
          restartError = ` Restarting the stopped queues also failed: ${describeWriteError(restart)}.${stoppedQueuesNote(stopped)}`;
        }
        fail(`${extractErrorMessage(error)}${restartError}`, error);
      }

      // The mutation is committed; failing to restart or start queues must not keep it staged.
      let followUpError: string | null = null;
      try {
        await writeStates(restartedAfterCommit, 'RUNNING');
      } catch (error) {
        followUpError = `The configuration was applied, but restarting the queues stopped for it failed: ${describeWriteError(error)}.${stoppedQueuesNote(restartedAfterCommit)}`;
      }
      if (!followUpError) {
        try {
          await writeStates(plan.childQueuesToStart, 'RUNNING');
        } catch (error) {
          followUpError = `The configuration was applied, but starting the new queues failed: ${describeWriteError(error)}`;
        }
      }

      set((state) => {
        // Edits made while the write was in flight belong to the next proposal.
        state.stagedChanges = state.stagedChanges.filter(
          (change) =>
            !changes.some(
              (submitted) => submitted.id === change.id && submitted.newValue === change.newValue,
            ),
        );
      });

      try {
        await get().loadInitialData();
      } catch (error) {
        fail(extractErrorMessage(error), error);
      }

      set((state) => {
        state.isLoading = false;
        clearMutationError(state);
        state.appliedWarnings = warnings;
        if (followUpError) {
          state.applyError = followUpError;
        }
      });
    },

    revertChange: (changeId) => {
      set((state) => {
        const beforeLength = state.stagedChanges.length;
        state.stagedChanges = state.stagedChanges.filter((c) => c.id !== changeId);
        if (state.stagedChanges.length !== beforeLength) {
          clearMutationError(state);
        }
      });
    },

    clearAllChanges: () => {
      set((state) => {
        if (state.stagedChanges.length > 0) {
          state.stagedChanges = [];
          clearMutationError(state);
        }
      });
    },

    clearQueueChanges: (queuePath) => {
      set((state) => {
        const beforeLength = state.stagedChanges.length;
        state.stagedChanges = state.stagedChanges.filter((c) => c.queuePath !== queuePath);
        if (state.stagedChanges.length !== beforeLength) {
          clearMutationError(state);
        }
      });
    },

    hasUnsavedChanges: () => {
      return get().stagedChanges.length > 0;
    },

    getChangesForQueue: (queuePath) => {
      return get().stagedChanges.filter((c) => c.queuePath === queuePath);
    },

    hasPendingDeletion: (queuePath) => {
      return get().stagedChanges.some((c) => c.queuePath === queuePath && c.type === 'remove');
    },

    revertQueueDeletion: (queuePath) => {
      const removalChange = get().stagedChanges.find(
        (c) => c.queuePath === queuePath && c.type === 'remove',
      );
      if (removalChange) {
        get().revertChange(removalChange.id);
      }
    },

    getStagedChangeById: (changeId) => {
      return get().stagedChanges.find((c) => c.id === changeId);
    },

    getLabelChangesForQueue: (queuePath, label) => {
      return get().stagedChanges.filter((c) => c.queuePath === queuePath && c.label === label);
    },
  };
};
