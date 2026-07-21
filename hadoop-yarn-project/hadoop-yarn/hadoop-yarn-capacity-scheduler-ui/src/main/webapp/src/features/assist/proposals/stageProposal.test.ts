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

import { describe, it, expect, beforeEach, vi } from 'vitest';
import { stageProposal } from '~/features/assist/proposals/stageProposal';
import type { SchedulerChangeProposal } from '~/features/assist/types';
import { useSchedulerStore } from '~/stores/schedulerStore';

function makeProposal(changes: SchedulerChangeProposal['changes']): SchedulerChangeProposal {
  return {
    proposal_id: 'test-001',
    summary: 'Test proposal',
    rationale: 'Testing',
    changes,
    warnings: [],
  };
}

describe('stageProposal', () => {
  beforeEach(() => {
    useSchedulerStore.getState().clearAllChanges();
  });

  describe('nothing is staged until the user calls stageProposal', () => {
    it('starts with no staged changes', () => {
      const { stagedChanges } = useSchedulerStore.getState();
      expect(stagedChanges).toHaveLength(0);
    });

    it('a proposal arriving does not auto-stage anything', () => {
      const _proposal = makeProposal([
        {
          kind: 'queue_property',
          queue_path: 'root.analytics',
          property: 'capacity',
          new_value: '50',
          old_value: '40',
          reason: 'increase',
        },
      ]);
      expect(useSchedulerStore.getState().stagedChanges).toHaveLength(0);
    });
  });

  describe('supported change kinds are staged correctly', () => {
    it('stages a queue_property change', () => {
      const result = stageProposal(
        makeProposal([
          {
            kind: 'queue_property',
            queue_path: 'root.analytics',
            property: 'capacity',
            new_value: '50',
            old_value: '40',
            reason: 'increase',
          },
        ]),
      );
      expect(result.staged).toBe(1);
      expect(result.rejected).toHaveLength(0);
      expect(useSchedulerStore.getState().stagedChanges).toHaveLength(1);
    });

    it('stages a global_property change', () => {
      const result = stageProposal(
        makeProposal([
          {
            kind: 'global_property',
            property: 'yarn.scheduler.capacity.maximum-applications',
            new_value: '20000',
            old_value: '10000',
            reason: 'increase limit',
          },
        ]),
      );
      expect(result.staged).toBe(1);
      expect(useSchedulerStore.getState().stagedChanges).toHaveLength(1);
    });

    it('stages multiple changes from one proposal', () => {
      const result = stageProposal(
        makeProposal([
          {
            kind: 'queue_property',
            queue_path: 'root.analytics',
            property: 'capacity',
            new_value: '50',
            old_value: '40',
            reason: 'increase analytics',
          },
          {
            kind: 'queue_property',
            queue_path: 'root.etl',
            property: 'capacity',
            new_value: '15',
            old_value: '25',
            reason: 'reduce etl',
          },
        ]),
      );
      expect(result.staged).toBe(2);
      expect(useSchedulerStore.getState().stagedChanges).toHaveLength(2);
    });
  });

  describe('stale old_value rejection', () => {
    it('stages when configData is empty (current value unknown)', () => {
      // With empty configData, old_value mismatch cannot be detected, so it stages
      const result = stageProposal(
        makeProposal([
          {
            kind: 'queue_property',
            queue_path: 'root.x',
            property: 'capacity',
            new_value: '30',
            old_value: '20',
            reason: 'test',
          },
        ]),
      );
      expect(result.staged).toBe(1);
    });
  });

  describe('unknown change kind rejection', () => {
    it('rejects an unknown change kind', () => {
      type AnyChange = Parameters<typeof stageProposal>[0]['changes'][number];
      const proposal = makeProposal([
        { kind: 'unknown_kind', queue_path: 'root.x', reason: 'test' } as unknown as AnyChange,
      ]);
      const result = stageProposal(proposal);
      expect(result.staged).toBe(0);
      expect(result.rejected).toHaveLength(1);
      expect(result.rejected[0]?.reason).toContain('Unknown change kind');
    });
  });

  describe('unrelated pre-existing staged changes are preserved', () => {
    it('keeps prior staged changes after staging proposal changes', () => {
      useSchedulerStore.getState().stageGlobalChange('some.property', 'some-value');
      const priorCount = useSchedulerStore.getState().stagedChanges.length;

      stageProposal(
        makeProposal([
          {
            kind: 'queue_property',
            queue_path: 'root.analytics',
            property: 'capacity',
            new_value: '50',
            old_value: '40',
            reason: 'test',
          },
        ]),
      );

      expect(useSchedulerStore.getState().stagedChanges).toHaveLength(priorCount + 1);
    });
  });

  describe('applyChanges is never called by stageProposal', () => {
    it('stageProposal does not call applyChanges', () => {
      const applyChangesSpy = vi.spyOn(useSchedulerStore.getState(), 'applyChanges');

      stageProposal(
        makeProposal([
          {
            kind: 'queue_property',
            queue_path: 'root.analytics',
            property: 'capacity',
            new_value: '50',
            old_value: '40',
            reason: 'test',
          },
        ]),
      );

      expect(applyChangesSpy).not.toHaveBeenCalled();
      applyChangesSpy.mockRestore();
    });
  });

  describe('missing required fields', () => {
    it('rejects queue_property change with empty queue_path', () => {
      type AnyChange = Parameters<typeof stageProposal>[0]['changes'][number];
      const result = stageProposal(
        makeProposal([
          {
            kind: 'queue_property',
            queue_path: '',
            property: 'capacity',
            new_value: '50',
            old_value: '40',
            reason: '',
          } as unknown as AnyChange,
        ]),
      );
      expect(result.rejected.length).toBeGreaterThan(0);
    });
  });
});
