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

import { describe, it, expect } from 'vitest';
import type { MutationError, ValidationResponse } from '~/types/mutation';

describe('MutationError interface', () => {
  it('should accept standard YARN exception format', () => {
    const error: MutationError = {
      RemoteException: {
        exception: 'YarnException',
        message: 'Invalid queue configuration',
        javaClassName: 'org.apache.hadoop.yarn.exceptions.YarnException',
      },
    };

    expect(error.RemoteException.exception).toBe('YarnException');
    expect(error.RemoteException.javaClassName).toBe(
      'org.apache.hadoop.yarn.exceptions.YarnException',
    );
  });

  it('should accept AccessControlException', () => {
    const accessError: MutationError = {
      RemoteException: {
        exception: 'AccessControlException',
        message: 'User does not have permission to modify scheduler configuration',
        javaClassName: 'org.apache.hadoop.security.AccessControlException',
      },
    };

    expect(accessError.RemoteException.exception).toBe('AccessControlException');
    expect(accessError.RemoteException.message).toContain('permission');
  });
});

describe('ValidationResponse interface', () => {
  it('should accept a valid result without issues', () => {
    const validResponse: ValidationResponse = { valid: true, issues: [], explain: [] };

    expect(validResponse.valid).toBe(true);
    expect(validResponse.issues).toHaveLength(0);
  });

  it('should accept global and queue issues with explained values', () => {
    const response: ValidationResponse = {
      valid: false,
      issues: [
        {
          queuePath: null,
          propertyKey: null,
          ruleId: 'invalid-mutation',
          severity: 'ERROR',
          message: 'Queue root.missing not found',
        },
        {
          queuePath: 'root.a',
          propertyKey: 'yarn.scheduler.capacity.root.a.capacity',
          ruleId: 'invalid-capacity',
          severity: 'WARNING',
          message: 'Capacity is low',
        },
      ],
      explain: [
        {
          queuePath: 'root.a',
          properties: [
            {
              key: 'yarn.scheduler.capacity.root.a.user-limit-factor',
              value: '1',
              source: 'DEFAULT',
              sourceDetail: null,
            },
          ],
        },
      ],
    };

    expect(response.issues[0].queuePath).toBeNull();
    expect(response.explain[0].properties[0].source).toBe('DEFAULT');
  });
});
