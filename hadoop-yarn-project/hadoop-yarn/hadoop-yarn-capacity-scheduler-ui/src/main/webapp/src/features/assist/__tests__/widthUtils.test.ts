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

import { describe, it, expect, beforeEach } from 'vitest';
import { clampWidth, loadPersistedWidth, PANEL_WIDTH_KEY } from '~/features/assist/panelWidth';

describe('clampWidth', () => {
  it('returns value within bounds unchanged', () => {
    expect(clampWidth(440)).toBe(440);
    expect(clampWidth(360)).toBe(360);
    expect(clampWidth(640)).toBe(640);
  });

  it('clamps below minimum to 360', () => {
    expect(clampWidth(100)).toBe(360);
    expect(clampWidth(0)).toBe(360);
    expect(clampWidth(-100)).toBe(360);
  });

  it('clamps above maximum to 640', () => {
    expect(clampWidth(900)).toBe(640);
    expect(clampWidth(10000)).toBe(640);
  });
});

describe('loadPersistedWidth', () => {
  beforeEach(() => {
    localStorage.clear();
  });

  it('returns default 440 when no stored value', () => {
    expect(loadPersistedWidth()).toBe(440);
  });

  it('loads and clamps a valid stored width', () => {
    localStorage.setItem(PANEL_WIDTH_KEY, JSON.stringify(500));
    expect(loadPersistedWidth()).toBe(500);
  });

  it('clamps a stored width that is too small', () => {
    localStorage.setItem(PANEL_WIDTH_KEY, JSON.stringify(100));
    expect(loadPersistedWidth()).toBe(360);
  });

  it('clamps a stored width that is too large', () => {
    localStorage.setItem(PANEL_WIDTH_KEY, JSON.stringify(9999));
    expect(loadPersistedWidth()).toBe(640);
  });

  it('returns default for a non-numeric stored value', () => {
    localStorage.setItem(PANEL_WIDTH_KEY, '"not-a-number"');
    expect(loadPersistedWidth()).toBe(440);
  });

  it('returns default for non-finite stored value', () => {
    localStorage.setItem(PANEL_WIDTH_KEY, JSON.stringify(Infinity));
    expect(loadPersistedWidth()).toBe(440);
  });

  it('returns default for NaN stored value', () => {
    localStorage.setItem(PANEL_WIDTH_KEY, JSON.stringify(NaN));
    expect(loadPersistedWidth()).toBe(440);
  });

  it('returns default for malformed JSON', () => {
    localStorage.setItem(PANEL_WIDTH_KEY, 'not-json{{{');
    expect(loadPersistedWidth()).toBe(440);
  });
});
