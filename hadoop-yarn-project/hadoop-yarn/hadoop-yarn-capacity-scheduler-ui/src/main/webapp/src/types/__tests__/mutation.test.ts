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
  it('should accept successful validation response', () => {
    const validResponse: ValidationResponse = {
      validationResult: {
        valid: true,
        configVersion: 12345,
        issues: { issue: [] },
      },
    };

    expect(validResponse.validationResult.valid).toBe(true);
    expect(validResponse.validationResult.issues.issue).toHaveLength(0);
    expect(validResponse.validationResult.configVersion).toBe(12345);
  });

  it('should accept validation failure response', () => {
    const invalidResponse: ValidationResponse = {
      validationResult: {
        valid: false,
        configVersion: 12346,
        issues: {
          issue: [
            {
              queuePath: 'root.production',
              propertyKey: 'capacity',
              ruleId: 'capacity-sum',
              severity: 'ERROR',
              message: 'Queue capacities do not sum to 100%',
            },
          ],
        },
      },
    };

    expect(invalidResponse.validationResult.valid).toBe(false);
    expect(invalidResponse.validationResult.issues.issue).toHaveLength(1);
    expect(invalidResponse.validationResult.issues.issue[0].message).toContain('sum to 100%');
  });

  it('should accept validation warnings', () => {
    const warningResponse: ValidationResponse = {
      validationResult: {
        valid: true,
        configVersion: 12347,
        issues: {
          issue: [
            {
              queuePath: 'root.wéird',
              ruleId: 'queue-name',
              severity: 'WARNING',
              message: 'Queue name contains non-portable characters',
            },
          ],
        },
      },
    };

    expect(warningResponse.validationResult.valid).toBe(true);
    expect(warningResponse.validationResult.issues.issue[0].severity).toBe('WARNING');
  });
});
