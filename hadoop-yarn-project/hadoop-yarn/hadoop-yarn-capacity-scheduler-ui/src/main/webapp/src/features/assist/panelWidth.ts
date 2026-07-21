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

export const PANEL_WIDTH_KEY = 'yarn-assist-panel-width';
export const DEFAULT_PANEL_WIDTH = 440;
export const MIN_PANEL_WIDTH = 360;
export const MAX_PANEL_WIDTH = 640;

export function clampWidth(value: number): number {
  return Math.max(MIN_PANEL_WIDTH, Math.min(MAX_PANEL_WIDTH, value));
}

export function loadPersistedWidth(): number {
  try {
    const stored = localStorage.getItem(PANEL_WIDTH_KEY);
    if (stored === null) return DEFAULT_PANEL_WIDTH;
    const parsed = JSON.parse(stored) as unknown;
    if (typeof parsed === 'number' && Number.isFinite(parsed)) {
      return clampWidth(parsed);
    }
  } catch {
    // ignore malformed stored values
  }
  return DEFAULT_PANEL_WIDTH;
}
