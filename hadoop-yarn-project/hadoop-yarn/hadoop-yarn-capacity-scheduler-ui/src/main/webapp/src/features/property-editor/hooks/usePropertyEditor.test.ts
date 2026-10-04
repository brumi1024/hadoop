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
import { act, renderHook } from '@testing-library/react';
import { usePropertyEditor } from './usePropertyEditor';
import { useSchedulerStore } from '~/stores/schedulerStore';
import type { QueueInfo, SchedulerInfo, ValidationResponse } from '~/types';

const leaf = (queuePath: string): QueueInfo =>
  ({
    queueName: queuePath.split('.').pop(),
    queuePath,
    queueType: 'leaf',
    state: 'RUNNING',
    capacity: 50,
  }) as QueueInfo;

const schedulerData = {
  type: 'capacityScheduler',
  queueName: 'root',
  capacity: 100,
  queues: { queue: [leaf('root.a'), leaf('root.b')] },
} as unknown as SchedulerInfo;

const SERVER_LATENCY_MS = 50;

// The server explains no queue: the static mock, or a queue missing from the proposed tree.
function mockValidation() {
  return vi
    .spyOn(useSchedulerStore.getState().apiClient, 'validateSchedulerConf')
    .mockImplementation(
      () =>
        new Promise<ValidationResponse>((resolve) =>
          setTimeout(() => resolve({ valid: true, issues: [], explain: [] }), SERVER_LATENCY_MS),
        ),
    );
}

describe('usePropertyEditor explain requests', () => {
  beforeEach(() => {
    vi.useFakeTimers();
    useSchedulerStore.setState({
      schedulerData,
      configData: new Map([['yarn.scheduler.capacity.root.queues', 'a,b']]),
      configEtag: '"etag-1"',
      stagedChanges: [],
      explain: {},
      explainProposalKey: null,
    });
  });

  afterEach(() => {
    vi.restoreAllMocks();
    vi.useRealTimers();
  });

  it('asks once for a queue the server does not explain', async () => {
    const validate = mockValidation();

    renderHook(() => usePropertyEditor({ queuePath: 'root.a' }));
    await act(() => vi.advanceTimersByTimeAsync(2000));

    expect(validate).toHaveBeenCalledOnce();
    expect(validate).toHaveBeenCalledWith({}, { explain: ['root.a'] });
  });

  it('settles after a change is staged while the panel stays open', async () => {
    const validate = mockValidation();
    renderHook(() => usePropertyEditor({ queuePath: 'root.b' }));
    await act(() => vi.advanceTimersByTimeAsync(2000));
    validate.mockClear();

    act(() => useSchedulerStore.getState().stageQueueRemoval('root.b'));
    await act(() => vi.advanceTimersByTimeAsync(2000));

    // One validation of the proposal, then one request for the queue it did not explain.
    expect(validate).toHaveBeenCalledTimes(2);
    expect(validate.mock.calls[0][1]).toEqual({ explain: 'affected' });
    expect(validate.mock.calls[1][1]).toEqual({ explain: ['root.b'] });

    await act(() => vi.advanceTimersByTimeAsync(2000));
    expect(validate).toHaveBeenCalledTimes(2);
  });
});
