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

import { describe, it, expect, vi } from 'vitest';
import { createStagedChangesSlice } from './stagedChangesSlice';
import type { StagedChange } from '~/types';

describe('stagedChangesSlice - auto-creation enablement', () => {
  it('stops and restarts queues when enabling legacy auto-creation', async () => {
    const queuePath = 'root.default';
    const propertyKey = 'yarn.scheduler.capacity.root.default.auto-create-child-queue.enabled';

    const updateSchedulerConf = vi.fn().mockResolvedValue({ etag: null });
    const validateSchedulerConf = vi
      .fn()
      .mockResolvedValue({ valid: true, issues: [], explain: [] });

    const apiClient = {
      updateSchedulerConf,
      validateSchedulerConf,
    } as any;

    const state: any = {
      apiClient,
      loadInitialData: vi.fn(async () => {
        state.configData = new Map([[propertyKey, 'true']]);
      }),
      getChildQueues: () => [],
      schedulerData: {
        type: 'capacityScheduler',
        capacity: 100,
        usedCapacity: 0,
        maxCapacity: 100,
        queueName: 'root',
        queues: { queue: [] },
      },
      configData: new Map(),
      configVersion: 1,
      configEtag: '"etag-1"',
      isLoading: false,
      error: null,
      errorContext: null,
      applyError: null,
    };

    const set = (fn: (draft: any) => void) => {
      fn(state);
    };
    const get = () => state;

    Object.assign(state, createStagedChangesSlice(set as any, get as any, {} as any));

    state.configData = new Map([[propertyKey, 'false']]);
    state.stagedChanges = [
      {
        id: '1',
        type: 'update',
        queuePath,
        property: 'auto-create-child-queue.enabled',
        oldValue: 'false',
        newValue: 'true',
        timestamp: Date.now(),
      },
    ] satisfies StagedChange[];

    await state.applyChanges();

    expect(validateSchedulerConf).toHaveBeenCalledOnce();

    expect(updateSchedulerConf).toHaveBeenCalledTimes(3);

    expect(updateSchedulerConf.mock.calls[0][0]).toEqual({
      'update-queue': [
        {
          'queue-name': queuePath,
          params: {
            entry: [{ key: 'state', value: 'STOPPED' }],
          },
        },
      ],
    });

    const mutation = updateSchedulerConf.mock.calls[1][0];
    expect(mutation['update-queue']).toEqual([
      {
        'queue-name': queuePath,
        params: {
          entry: expect.arrayContaining([
            { key: 'auto-create-child-queue.enabled', value: 'true' },
          ]),
        },
      },
    ]);
    expect(mutation['global-updates']).toBeUndefined();

    expect(updateSchedulerConf.mock.calls[2][0]).toEqual({
      'update-queue': [
        {
          'queue-name': queuePath,
          params: {
            entry: [{ key: 'state', value: 'RUNNING' }],
          },
        },
      ],
    });

    expect(state.loadInitialData).toHaveBeenCalledTimes(1);
    expect(state.stagedChanges).toEqual([]);
    expect(state.configData.get(propertyKey)).toBe('true');
  });
});
