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

import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { createSchedulerStore } from '~/stores/schedulerStore';
import { YarnApiClient } from '~/lib/api/YarnApiClient';
import { YarnApiError } from '~/lib/errors';
import { STALE_BASELINE_MESSAGE, VALIDATION_DEBOUNCE_MS } from '~/stores/slices/stagedChangesSlice';
import type {
  QueueInfo,
  SchedConfUpdateInfo,
  SchedulerInfo,
  SchedulerValidationIssue,
  ValidationResponse,
} from '~/types';

const result = (
  valid = true,
  issues: SchedulerValidationIssue[] = [],
  explain: ValidationResponse['explain'] = [],
): ValidationResponse => ({ valid, issues, explain });

const leaf = (queuePath: string, state: 'RUNNING' | 'STOPPED' = 'RUNNING'): QueueInfo => ({
  queueName: queuePath.split('.').pop() ?? queuePath,
  queuePath,
  queueType: 'leaf',
  state,
  capacity: 50,
  usedCapacity: 0,
  maxCapacity: 100,
  absoluteCapacity: 50,
  absoluteMaxCapacity: 100,
  absoluteUsedCapacity: 0,
  numApplications: 0,
  numActiveApplications: 0,
  numPendingApplications: 0,
});

const schedulerData = {
  type: 'capacityScheduler',
  queueName: 'root',
  capacity: 100,
  usedCapacity: 0,
  maxCapacity: 100,
  queues: { queue: [leaf('root.a'), leaf('root.b')] },
} as unknown as SchedulerInfo;

function fixture() {
  const client = new YarnApiClient('/ws/v1/cluster', { detectSecurityMode: false });
  const store = createSchedulerStore(client);
  store.setState({
    schedulerData,
    configData: new Map([
      ['yarn.scheduler.capacity.root.queues', 'a,b'],
      ['yarn.scheduler.capacity.root.a.maximum-applications', '10'],
    ]),
    configEtag: '"etag-1"',
  });
  // Reloading the baseline is out of scope here; it only refreshes the ETag.
  const loadInitialData = vi.fn(async () => {
    store.setState({ configEtag: '"etag-2"' });
  });
  store.setState({ loadInitialData });
  const validate = vi.spyOn(client, 'validateSchedulerConf').mockResolvedValue(result());
  // A server that honors If-Match and returns the ETag of every configuration it commits.
  // Writes are numbered from 1; a rejected write commits nothing, a lost response commits.
  const server = {
    etag: '"etag-1"',
    writes: 0,
    rejections: new Map<number, Error>(),
    lostResponses: new Set<number>(),
    afterCommit: (_write: number) => {},
  };
  const update = vi
    .spyOn(client, 'updateSchedulerConf')
    .mockImplementation(async (_body, options) => {
      const write = ++server.writes;
      if (options?.ifMatch && options.ifMatch !== server.etag) {
        throw new YarnApiError('ETag mismatch', 412, 'precondition-failed');
      }
      const rejection = server.rejections.get(write);
      if (rejection) {
        throw rejection;
      }
      server.etag = `"etag-${write + 1}"`;
      const etag = server.etag;
      server.afterCommit(write);
      if (server.lostResponses.has(write)) {
        throw new TypeError('Failed to fetch');
      }
      return { etag };
    });
  return { store, client, validate, update, loadInitialData, server };
}

const stateWrite = (queues: string[], state: 'STOPPED' | 'RUNNING'): SchedConfUpdateInfo => ({
  'update-queue': queues.map((queue) => ({
    'queue-name': queue,
    params: { entry: [{ key: 'state', value: state }] },
  })),
});

describe('server validation of staged proposals', () => {
  beforeEach(() => {
    vi.useFakeTimers();
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  it('validates staged edits after a debounce, independent of any panel', async () => {
    const { store, validate } = fixture();

    store.getState().stageQueueChange('root.a', 'maximum-applications', '20');
    store.getState().stageQueueChange('root.a', 'maximum-applications', '25');
    expect(store.getState().isValidatingProposal).toBe(true);

    await vi.advanceTimersByTimeAsync(VALIDATION_DEBOUNCE_MS - 1);
    expect(validate).not.toHaveBeenCalled();

    await vi.advanceTimersByTimeAsync(1);
    expect(validate).toHaveBeenCalledOnce();
    expect(validate).toHaveBeenCalledWith(
      {
        'update-queue': [
          {
            'queue-name': 'root.a',
            params: { entry: [{ key: 'maximum-applications', value: '25' }] },
          },
        ],
      },
      { explain: 'affected' },
    );
    expect(store.getState().isValidatingProposal).toBe(false);
    expect(store.getState().validatedProposalKey).toBe(store.getState().proposalKey);
  });

  it('maps issues by queue path and full key, keeping null paths global', async () => {
    const { store, validate } = fixture();
    validate.mockResolvedValue(
      result(false, [
        {
          queuePath: 'root.a',
          propertyKey: 'yarn.scheduler.capacity.root.a.maximum-applications',
          ruleId: 'invalid-number',
          severity: 'ERROR',
          message: 'Not a number',
        },
        {
          queuePath: null,
          propertyKey: 'yarn.scheduler.capacity.maximum-applications',
          ruleId: 'global-limit',
          severity: 'WARNING',
          message: 'Global warning',
        },
      ]),
    );

    store.getState().stageQueueChange('root.a', 'maximum-applications', 'x');
    await expect(store.getState().validateProposal()).resolves.toBe(false);

    expect(store.getState().serverIssues).toEqual([
      {
        queuePath: 'root.a',
        propertyKey: 'yarn.scheduler.capacity.root.a.maximum-applications',
        ruleId: 'invalid-number',
        severity: 'error',
        message: 'Not a number',
      },
      {
        queuePath: null,
        propertyKey: 'yarn.scheduler.capacity.maximum-applications',
        ruleId: 'global-limit',
        severity: 'warning',
        message: 'Global warning',
      },
    ]);
  });

  it('gives an invalid result without errors a visible reason', async () => {
    const { store, validate } = fixture();
    validate.mockResolvedValue(result(false));

    store.getState().stageQueueChange('root.a', 'maximum-applications', '20');
    await store.getState().validateProposal();

    expect(store.getState().serverIssues).toEqual([
      expect.objectContaining({ queuePath: null, severity: 'error' }),
    ]);
  });

  it('discards a validation response that arrives after a staged edit', async () => {
    const { store, validate } = fixture();
    let resolveFirst: (value: ValidationResponse) => void = () => {};
    validate.mockImplementationOnce(
      () => new Promise<ValidationResponse>((resolve) => (resolveFirst = resolve)),
    );

    store.getState().stageQueueChange('root.a', 'maximum-applications', '20');
    const first = store.getState().validateProposal();
    store.getState().stageQueueChange('root.a', 'maximum-applications', '30');
    resolveFirst(
      result(false, [
        {
          queuePath: 'root.a',
          propertyKey: null,
          ruleId: 'old',
          severity: 'ERROR',
          message: 'Old',
        },
      ]),
    );

    await expect(first).resolves.toBe(false);
    expect(store.getState().serverIssues).toEqual([]);
    expect(store.getState().validatedProposalKey).toBeNull();
    expect(store.getState().isValidatingProposal).toBe(true);
  });

  it('records a failed validation request as an error, not as a result', async () => {
    const { store, validate } = fixture();
    validate.mockRejectedValue(
      new YarnApiError('Unrecognized field', 400, 'remote-exception', 'BadRequestException'),
    );

    store.getState().stageQueueChange('root.a', 'maximum-applications', '20');
    await expect(store.getState().validateProposal()).resolves.toBe(false);

    expect(store.getState().validationError).toBe('Validation failed: Unrecognized field');
    expect(store.getState().serverIssues).toEqual([]);
    expect(store.getState().proposalStale).toBe(false);
  });

  it('loads explain for one queue and keeps it for the current proposal', async () => {
    const { store, validate } = fixture();
    const explained = {
      key: 'yarn.scheduler.capacity.root.a.user-limit-factor',
      value: '1',
      source: 'DEFAULT' as const,
      sourceDetail: null,
    };
    validate.mockResolvedValue(
      result(true, [], [{ queuePath: 'root.a', properties: [explained] }]),
    );

    await store.getState().loadExplain('root.a');

    expect(validate).toHaveBeenCalledWith({}, { explain: ['root.a'] });
    expect(store.getState().explain['root.a']).toEqual({ [explained.key]: explained });

    await store.getState().loadExplain('root.a');
    expect(validate).toHaveBeenCalledOnce();
  });

  it('asks once per proposal for a queue the server leaves out', async () => {
    const { store, validate } = fixture();

    await store.getState().loadExplain('root.b');
    const explain = store.getState().explain;
    await store.getState().loadExplain('root.b');

    expect(validate).toHaveBeenCalledOnce();
    expect(store.getState().explain).toBe(explain);

    store.getState().stageQueueChange('root.a', 'maximum-applications', '20');
    await store.getState().loadExplain('root.b');
    expect(validate).toHaveBeenCalledOnce();

    await vi.advanceTimersByTimeAsync(VALIDATION_DEBOUNCE_MS);
    await store.getState().loadExplain('root.b');
    await store.getState().loadExplain('root.b');
    expect(validate.mock.calls.map(([, options]) => options)).toEqual([
      { explain: ['root.b'] },
      { explain: 'affected' },
      { explain: ['root.b'] },
    ]);
  });

  it('uses the explain of the validated proposal for an affected queue', async () => {
    const { store, validate } = fixture();
    validate.mockResolvedValue(result(true, [], [{ queuePath: 'root.a', properties: [] }]));

    store.getState().stageQueueChange('root.a', 'maximum-applications', '20');
    await store.getState().loadExplain('root.a');
    await vi.advanceTimersByTimeAsync(VALIDATION_DEBOUNCE_MS);
    await store.getState().loadExplain('root.a');

    expect(validate).toHaveBeenCalledOnce();
    expect(validate).toHaveBeenCalledWith(expect.anything(), { explain: 'affected' });
  });
});

describe('applying staged proposals', () => {
  it('validates before any write and does not write an invalid proposal', async () => {
    const { store, validate, update } = fixture();
    validate.mockResolvedValue(
      result(false, [
        { queuePath: 'root.b', propertyKey: null, ruleId: 'x', severity: 'ERROR', message: 'Bad' },
      ]),
    );
    store.getState().stageQueueRemoval('root.b');

    await expect(store.getState().applyChanges()).rejects.toThrow('Validation failed: Bad');

    expect(update).not.toHaveBeenCalled();
    expect(store.getState().stagedChanges).toHaveLength(1);
  });

  it('validates in legacy queue mode before stopping queues for a removal', async () => {
    const { store, validate, update } = fixture();
    store.setState({
      configData: new Map([['yarn.scheduler.capacity.legacy-queue-mode.enabled', 'true']]),
    });
    store.getState().stageQueueRemoval('root.b');

    await store.getState().applyChanges();

    expect(validate.mock.invocationCallOrder[0]).toBeLessThan(update.mock.invocationCallOrder[0]);
    expect(validate.mock.calls[0][0]).toEqual({
      'remove-queue': 'root.b',
      'update-queue': stateWrite(['root.b'], 'STOPPED')['update-queue'],
    });
    expect(update.mock.calls.map(([body]) => body)).toEqual([
      stateWrite(['root.b'], 'STOPPED'),
      { 'remove-queue': 'root.b' },
    ]);
  });

  it('adds a child under a running leaf by stopping the leaf and starting both after', async () => {
    const { store, validate, update } = fixture();
    store.getState().stageQueueAddition('root.a', 'child', {
      capacity: '100',
      'maximum-capacity': '100',
      state: 'RUNNING',
    });

    await store.getState().applyChanges();

    // The server sees the leaf stopped and the child created stopped, as the apply submits it.
    expect(validate.mock.calls.at(-1)?.[0]).toEqual({
      'add-queue': [
        {
          'queue-name': 'root.a.child',
          params: {
            entry: [
              { key: 'capacity', value: '100' },
              { key: 'maximum-capacity', value: '100' },
              { key: 'state', value: 'STOPPED' },
            ],
          },
        },
      ],
      'update-queue': stateWrite(['root.a'], 'STOPPED')['update-queue'],
    });
    expect(update.mock.calls.map(([body]) => body)).toEqual([
      stateWrite(['root.a'], 'STOPPED'),
      expect.objectContaining({ 'add-queue': expect.any(Array) }),
      stateWrite(['root.a'], 'RUNNING'),
      stateWrite(['root.a.child'], 'RUNNING'),
    ]);
    expect(store.getState().stagedChanges).toEqual([]);
  });

  it('sends If-Match on every write with the ETag the previous write returned', async () => {
    const { store, update } = fixture();
    store.getState().stageQueueAddition('root.a', 'child', {
      capacity: '100',
      'maximum-capacity': '100',
      state: 'RUNNING',
    });

    await store.getState().applyChanges();

    expect(update.mock.calls.map(([, options]) => options)).toEqual([
      { ifMatch: '"etag-1"' },
      { ifMatch: '"etag-2"' },
      { ifMatch: '"etag-3"' },
      { ifMatch: '"etag-4"' },
    ]);
  });

  it('stops without overwriting a change made between the stop and the mutation', async () => {
    const { store, update, server } = fixture();
    server.afterCommit = (write) => {
      if (write === 1) {
        // Another administrator changes the configuration after the stop commits.
        server.etag = '"etag-other-admin"';
      }
    };
    store.getState().stageQueueAddition('root.a', 'child', {
      capacity: '100',
      'maximum-capacity': '100',
      state: 'RUNNING',
    });
    store.getState().stageQueueChange('root.a', 'maximum-applications', '25');
    const staged = store.getState().stagedChanges;

    await expect(store.getState().applyChanges()).rejects.toThrow(
      /changed by someone else .* may still be STOPPED: root\.a\./,
    );

    // The mutation was refused, and nothing restarts the queue under the other change.
    expect(update).toHaveBeenCalledTimes(2);
    expect(server.etag).toBe('"etag-other-admin"');
    expect(store.getState().proposalStale).toBe(true);
    expect(store.getState().stagedChanges).toEqual(staged);
  });

  it('marks a 412 as stale, keeps the edits and never retries or restarts queues', async () => {
    const { store, update } = fixture();
    update.mockRejectedValueOnce(new YarnApiError('ETag mismatch', 412, 'precondition-failed'));
    store.getState().stageQueueRemoval('root.b');

    await expect(store.getState().applyChanges()).rejects.toThrow(STALE_BASELINE_MESSAGE);

    expect(update).toHaveBeenCalledOnce();
    expect(store.getState().proposalStale).toBe(true);
    expect(store.getState().applyError).toBe(STALE_BASELINE_MESSAGE);
    expect(store.getState().stagedChanges).toHaveLength(1);

    // A stale proposal cannot be applied until the user reloads and compares.
    await expect(store.getState().applyChanges()).rejects.toThrow(STALE_BASELINE_MESSAGE);
    expect(update).toHaveBeenCalledOnce();
  });

  it('reloads and compares, recording what changed under the staged edit', async () => {
    const { store, loadInitialData, validate } = fixture();
    store.getState().stageQueueChange('root.a', 'maximum-applications', '25');
    store.setState({ proposalStale: true });
    loadInitialData.mockImplementation(async () => {
      store.setState({
        configEtag: '"etag-2"',
        configData: new Map([['yarn.scheduler.capacity.root.a.maximum-applications', '15']]),
      });
    });

    await store.getState().compareFreshBaseline();

    const [change] = store.getState().stagedChanges;
    expect(store.getState().proposalStale).toBe(false);
    expect(change.oldValue).toBe('15');
    expect(change.baselineDrift).toEqual({ stagedAgainst: '10', current: '15' });
    expect(change.newValue).toBe('25');
    expect(validate).toHaveBeenCalled();
  });

  it('does not retry or compensate a write whose outcome is unknown', async () => {
    const { store, update, server } = fixture();
    server.lostResponses.add(2);
    store.getState().stageQueueRemoval('root.b');

    await expect(store.getState().applyChanges()).rejects.toThrow(/outcome .* is unknown/);

    // Stop, then the mutation; no retry and no restart of a queue that may be gone.
    expect(update).toHaveBeenCalledTimes(2);
    expect(store.getState().proposalStale).toBe(false);
    expect(store.getState().stagedChanges).toHaveLength(1);
  });

  it('does not restart a parent stopped for a mutation whose outcome is unknown', async () => {
    const { store, update, server } = fixture();
    server.lostResponses.add(2);
    store.getState().stageQueueAddition('root.a', 'child', {
      capacity: '100',
      'maximum-capacity': '100',
      state: 'RUNNING',
    });
    const staged = store.getState().stagedChanges;

    await expect(store.getState().applyChanges()).rejects.toThrow(
      /outcome .* is unknown.* may still be STOPPED: root\.a\./,
    );

    expect(update).toHaveBeenCalledTimes(2);
    expect(store.getState().stagedChanges).toEqual(staged);
  });

  it('restarts stopped queues, conditionally, when the server rejects the mutation', async () => {
    const { store, update, server } = fixture();
    server.rejections.set(2, new YarnApiError('Failed to re-init queues : bad', 400, 'http'));
    store.getState().stageQueueRemoval('root.b');

    await expect(store.getState().applyChanges()).rejects.toThrow('Failed to re-init queues : bad');

    expect(update.mock.calls.map(([body]) => body)).toEqual([
      stateWrite(['root.b'], 'STOPPED'),
      { 'remove-queue': 'root.b' },
      stateWrite(['root.b'], 'RUNNING'),
    ]);
    // The rejected mutation committed nothing, so the restart expects the stop's ETag.
    expect(update.mock.calls[2][1]).toEqual({ ifMatch: '"etag-2"' });
  });

  it('reports queues left stopped when restarting them after a rejection fails', async () => {
    const { store, server } = fixture();
    server.rejections.set(2, new YarnApiError('Failed to re-init queues : bad', 400, 'http'));
    server.rejections.set(3, new YarnApiError('Restart refused', 400, 'http'));
    store.getState().stageQueueRemoval('root.b');

    await expect(store.getState().applyChanges()).rejects.toThrow(
      'Failed to re-init queues : bad Restarting the stopped queues also failed: Restart refused. ' +
        'These queues were stopped for the apply and may still be STOPPED: root.b.',
    );
  });

  it('keeps a committed mutation applied and reports a failed restart', async () => {
    const { store, update, server } = fixture();
    server.rejections.set(3, new YarnApiError('Restart refused', 400, 'http'));
    store.getState().stageQueueAddition('root.a', 'child', {
      capacity: '100',
      'maximum-capacity': '100',
      state: 'RUNNING',
    });

    await store.getState().applyChanges();

    // The new queue is not started under a parent that is still stopped.
    expect(update).toHaveBeenCalledTimes(3);
    expect(store.getState().stagedChanges).toEqual([]);
    expect(store.getState().applyError).toBe(
      'The configuration was applied, but restarting the queues stopped for it failed: ' +
        'Restart refused. These queues were stopped for the apply and may still be STOPPED: root.a.',
    );
  });

  it('does not restart queues over a change made after the mutation committed', async () => {
    const { store, update, server } = fixture();
    server.afterCommit = (write) => {
      if (write === 2) {
        server.etag = '"etag-other-admin"';
      }
    };
    store.getState().stageQueueAddition('root.a', 'child', {
      capacity: '100',
      'maximum-capacity': '100',
      state: 'RUNNING',
    });

    await store.getState().applyChanges();

    expect(update).toHaveBeenCalledTimes(3);
    expect(store.getState().stagedChanges).toEqual([]);
    expect(store.getState().applyError).toMatch(
      /^The configuration was applied, but restarting .* changed by someone else .* root\.a\.$/,
    );
  });

  it('keeps apply warnings visible after reloading the committed baseline', async () => {
    const { store, validate } = fixture();
    validate.mockResolvedValue(
      result(true, [
        {
          queuePath: 'root.a',
          propertyKey: null,
          ruleId: 'w',
          severity: 'WARNING',
          message: 'Warn',
        },
      ]),
    );
    store.getState().stageQueueChange('root.a', 'maximum-applications', '25');

    await store.getState().applyChanges();

    expect(store.getState().stagedChanges).toEqual([]);
    expect(store.getState().appliedWarnings).toEqual([
      expect.objectContaining({ message: 'Warn', severity: 'warning' }),
    ]);

    store.getState().stageQueueChange('root.a', 'maximum-applications', '30');
    expect(store.getState().appliedWarnings).toEqual([]);
  });
});
