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
 * Typed errors for YARN REST responses that are not a successful result.
 */

export type YarnApiErrorKind =
  // 412: the configuration changed since the ETag sent in If-Match was read.
  | 'precondition-failed'
  // A Jersey RemoteException body, for example a 400 for a body the server cannot interpret.
  | 'remote-exception'
  // Any other non-success status, with the plain-text or JSON message the server returned.
  | 'http'
  // A success status whose body does not have the documented shape.
  | 'invalid-response';

export class YarnApiError extends Error {
  constructor(
    message: string,
    public readonly status: number,
    public readonly kind: YarnApiErrorKind,
    public readonly exception?: string,
  ) {
    super(message);
    this.name = 'YarnApiError';
  }
}

export function isPreconditionFailed(error: unknown): boolean {
  return error instanceof YarnApiError && error.kind === 'precondition-failed';
}
