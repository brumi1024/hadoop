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

export type StagedChangeType = 'add' | 'update' | 'remove';

export type StagedChange = {
  id: string;
  type: StagedChangeType;
  queuePath: string | 'global';
  property: string;
  oldValue?: string;
  newValue?: string;
  timestamp: number;
  label?: string; // Used for node label changes
  // Set when a reload showed that the server value changed after this edit was staged
  baselineDrift?: { stagedAgainst?: string; current?: string };
};
