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
import type {
  PropertyDescriptor,
  PropertyType,
  PropertyCategory,
} from '~/types/property-descriptor';

describe('PropertyDescriptor interface', () => {
  it('should accept basic property descriptor', () => {
    const descriptor: PropertyDescriptor = {
      name: 'capacity',
      displayName: 'Queue Capacity',
      description: 'The percentage of cluster resources allocated to this queue',
      type: 'number',
      category: 'resource',
      defaultValue: '0',
      required: true,
    };

    expect(descriptor.name).toBe('capacity');
    expect(descriptor.displayName).toBe('Queue Capacity');
    expect(descriptor.type).toBe('number');
    expect(descriptor.category).toBe('resource');
    expect(descriptor.required).toBe(true);
  });

  it('should handle property with an input range', () => {
    const descriptorWithRange: PropertyDescriptor = {
      name: 'maximum-capacity',
      displayName: 'Maximum Capacity',
      description: 'The maximum percentage of cluster resources this queue can use',
      type: 'number',
      category: 'resource',
      defaultValue: '100',
      required: false,
      inputRange: { min: 0, max: 100 },
    };

    expect(descriptorWithRange.inputRange).toEqual({ min: 0, max: 100 });
  });

  it('should handle boolean property descriptor', () => {
    const booleanDescriptor: PropertyDescriptor = {
      name: 'enable-preemption',
      displayName: 'Enable Preemption',
      description: 'Whether to enable preemption for this queue',
      type: 'boolean',
      category: 'scheduling',
      defaultValue: 'false',
      required: false,
    };

    expect(booleanDescriptor.type).toBe('boolean');
    expect(booleanDescriptor.defaultValue).toBe('false');
  });

  it('should handle enum property descriptor', () => {
    const enumDescriptor: PropertyDescriptor = {
      name: 'state',
      displayName: 'Queue State',
      description: 'The operational state of the queue',
      type: 'enum',
      category: 'core',
      defaultValue: 'RUNNING',
      required: true,
      enumValues: [
        { value: 'RUNNING', label: 'Running' },
        { value: 'STOPPED', label: 'Stopped' },
      ],
    };

    expect(enumDescriptor.type).toBe('enum');
    expect(enumDescriptor.enumValues?.some((option) => option.value === 'RUNNING')).toBe(true);
    expect(enumDescriptor.enumValues?.some((option) => option.value === 'STOPPED')).toBe(true);
  });

  it('should handle property dependent on other properties', () => {
    const dependentProperty: PropertyDescriptor = {
      name: 'user-limit-factor',
      displayName: 'User Limit Factor',
      description: 'Multiplier for user resource limits',
      type: 'number',
      category: 'application-limits',
      defaultValue: '1',
      required: false,
      enableWhen: [({ getValue }) => getValue('minimum-user-limit-percent') !== '100'],
    };

    expect(Array.isArray(dependentProperty.enableWhen)).toBe(true);
    const condition = dependentProperty.enableWhen?.[0];
    expect(condition).toBeInstanceOf(Function);
    if (condition) {
      const positive = condition({
        scope: 'queue',
        property: dependentProperty,
        propertyValue: '1',
        values: { 'minimum-user-limit-percent': '50' },
        globalValues: {},
        queuePath: 'root.a',
        queueInfo: null,
        schedulerInfo: null,
        stagedChanges: [],
        configData: new Map(),
        getValue: (name: string) => (name === 'minimum-user-limit-percent' ? '50' : undefined),
        getGlobalValue: () => undefined,
        getQueueValue: () => undefined,
        getConfigValue: () => undefined,
      });
      expect(positive).toBe(true);

      const negative = condition({
        scope: 'queue',
        property: dependentProperty,
        propertyValue: '1',
        values: { 'minimum-user-limit-percent': '100' },
        globalValues: {},
        queuePath: 'root.a',
        queueInfo: null,
        schedulerInfo: null,
        stagedChanges: [],
        configData: new Map(),
        getValue: (name: string) => (name === 'minimum-user-limit-percent' ? '100' : undefined),
        getGlobalValue: () => undefined,
        getQueueValue: () => undefined,
        getConfigValue: () => undefined,
      });
      expect(negative).toBe(false);
    }
  });

  it('should handle property with display formatting', () => {
    const formattedProperty: PropertyDescriptor = {
      name: 'maximum-am-resource-percent',
      displayName: 'Maximum AM Resource Percent',
      description: 'Maximum percentage of resources for Application Masters',
      type: 'number',
      category: 'resource',
      defaultValue: '0.1',
      required: false,
      displayFormat: {
        suffix: '%',
        multiplier: 100,
        decimals: 1,
      },
    };

    expect(formattedProperty.displayFormat?.suffix).toBe('%');
    expect(formattedProperty.displayFormat?.multiplier).toBe(100);
    expect(formattedProperty.displayFormat?.decimals).toBe(1);
  });

  it('should handle deprecated property', () => {
    const deprecatedProperty: PropertyDescriptor = {
      name: 'maximum-applications',
      displayName: 'Maximum Applications',
      description: 'Maximum number of applications in the queue',
      type: 'number',
      category: 'application-limits',
      defaultValue: '10000',
      required: false,
      deprecated: true,
      deprecationMessage: 'Use max-parallel-apps instead',
    };

    expect(deprecatedProperty.deprecated).toBe(true);
    expect(deprecatedProperty.deprecationMessage).toBe('Use max-parallel-apps instead');
  });

  it('should mark template-supported properties when configured', () => {
    const templateProperty: PropertyDescriptor = {
      name: 'capacity',
      displayName: 'Capacity',
      description: 'Queue capacity setting',
      type: 'number',
      category: 'core',
      defaultValue: '0',
      required: true,
      templateSupport: true,
    };

    expect(templateProperty.templateSupport).toBe(true);
  });
});

describe('PropertyType', () => {
  it('should only accept valid property types', () => {
    const stringType: PropertyType = 'string';
    const numberType: PropertyType = 'number';
    const booleanType: PropertyType = 'boolean';
    const enumType: PropertyType = 'enum';
    const listType: PropertyType = 'list';

    expect(stringType).toBe('string');
    expect(numberType).toBe('number');
    expect(booleanType).toBe('boolean');
    expect(enumType).toBe('enum');
    expect(listType).toBe('list');
  });
});

describe('PropertyCategory', () => {
  it('should only accept valid property categories', () => {
    const coreCategory: PropertyCategory = 'core';
    const resourceCategory: PropertyCategory = 'resource';
    const schedulingCategory: PropertyCategory = 'scheduling';
    const applicationLimitsCategory: PropertyCategory = 'application-limits';
    const securityCategory: PropertyCategory = 'security';
    const capacityCategory: PropertyCategory = 'capacity';

    expect(coreCategory).toBe('core');
    expect(resourceCategory).toBe('resource');
    expect(schedulingCategory).toBe('scheduling');
    expect(applicationLimitsCategory).toBe('application-limits');
    expect(securityCategory).toBe('security');
    expect(capacityCategory).toBe('capacity');
  });
});
