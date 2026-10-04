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

import { useForm, useWatch } from 'react-hook-form';
import { zodResolver } from '@hookform/resolvers/zod';
import { z } from 'zod';
import { useEffect, useMemo, useRef } from 'react';
import { useSchedulerStore } from '~/stores/schedulerStore';
import type { PropertyDescriptor } from '~/types/property-descriptor';
import { queuePropertyDefinitions } from '~/config/properties/queue-properties';
import { toast } from 'sonner';
import {
  getExplainedProperty,
  getIssuePropertyName,
  getPropertyIssues,
  getQueueIssues,
} from '~/features/validation/service';
import type { ExplainedProperty, ValidationIssue } from '~/types';
import { CONFIG_PREFIXES } from '~/types';

// Field values are plain strings; the server validates them after they are staged.
function createFormSchema(
  properties: Array<
    PropertyDescriptor & {
      formFieldName?: string;
      originalName?: string;
    }
  >,
) {
  const schemaFields: Record<string, z.ZodType> = {};

  properties.forEach((property) => {
    // Use escaped field name for React Hook Form to prevent dot notation conflicts
    const fieldName = property.formFieldName || property.name;
    schemaFields[fieldName] = property.required ? z.string() : z.string().optional();
  });

  return z.object(schemaFields);
}

interface UsePropertyEditorOptions {
  queuePath: string;
  properties?: PropertyDescriptor[];
}

export function usePropertyEditor({
  queuePath,
  properties = queuePropertyDefinitions,
}: UsePropertyEditorOptions) {
  const { getQueuePropertyValue, stageQueueChange, clearQueueChanges, configData } =
    useSchedulerStore();
  const serverIssues = useSchedulerStore((state) => state.serverIssues);
  const explain = useSchedulerStore((state) => state.explain);
  const proposalKey = useSchedulerStore((state) => state.proposalKey);
  const isValidatingProposal = useSchedulerStore((state) => state.isValidatingProposal);
  const loadExplain = useSchedulerStore((state) => state.loadExplain);

  const stagedChanges = useSchedulerStore((state) => state.stagedChanges);
  const cleanResetRef = useRef(false);
  const previousQueuePathRef = useRef<string | null>(null);

  useEffect(() => {
    if (previousQueuePathRef.current !== queuePath) {
      cleanResetRef.current = true;
      previousQueuePathRef.current = queuePath;
    }
  }, [queuePath]);

  // Escape dot notation in property names to prevent React Hook Form from treating them as nested paths
  // Note: This must be memoized because it's used as a dependency in useEffect below.
  // Without memoization, it creates a new array on every render, causing infinite loops.
  const allProperties = useMemo(
    () =>
      properties.map((property) => ({
        ...property,
        formFieldName: property.formFieldName ?? property.name.replace(/\./g, '__DOT__'),
        originalName: property.originalName ?? property.name,
      })),
    [properties],
  );

  const formSchema = useMemo(() => createFormSchema(allProperties), [allProperties]);

  // Source labels for this queue come from the explain section of validate/v2. Runs when
  // the proposal or its validation state changes, never on the explain index it fills: the
  // server omits queues that are not in the proposed tree, so the entry may never appear.
  useEffect(() => {
    if (queuePath) {
      void loadExplain(queuePath);
    }
  }, [queuePath, proposalKey, isValidatingProposal, loadExplain]);

  const form = useForm({
    resolver: zodResolver(formSchema),
    defaultValues: {},
    mode: 'onBlur', // Changed from 'onChange' for better performance
    criteriaMode: 'all', // Show all validation errors
  });

  const { control, handleSubmit, reset } = form;

  const watchedValues = useWatch({ control });

  const normalizeFieldName = (field: string): string => {
    const property = allProperties.find(
      (p) => p.formFieldName === field || p.originalName === field || p.name === field,
    );

    if (property) {
      return property.originalName || property.name;
    }

    return field.replace(/__DOT__/g, '.');
  };

  const getFieldIssues = (field: string): ValidationIssue[] =>
    getPropertyIssues(serverIssues, queuePath, normalizeFieldName(field));

  const getFieldErrors = (field: string): string[] => {
    return getFieldIssues(field)
      .filter((issue) => issue.severity === 'error')
      .map((issue) => issue.message);
  };

  const getFieldWarnings = (field: string): string[] => {
    return getFieldIssues(field)
      .filter((issue) => issue.severity === 'warning')
      .map((issue) => issue.message);
  };

  useEffect(() => {
    const initialValues: Record<string, string> = {};

    allProperties.forEach((property) => {
      const { value } = getQueuePropertyValue(queuePath, property.originalName || property.name);

      // Use escaped field name for React Hook Form
      const fieldName = property.formFieldName || property.name;
      initialValues[fieldName] = value;
    });

    const shouldForceClean = cleanResetRef.current;
    reset(initialValues, {
      keepDirty: !shouldForceClean,
      keepDirtyValues: !shouldForceClean,
    });
    if (shouldForceClean) {
      cleanResetRef.current = false;
    }
  }, [queuePath, allProperties, getQueuePropertyValue, reset, stagedChanges]);

  const getStagedStatus = (propertyName: string): 'new' | 'modified' | 'deleted' | undefined => {
    const { isStaged } = getQueuePropertyValue(queuePath, propertyName);
    return isStaged ? 'modified' : undefined;
  };

  const getExplained = (propertyName: string): ExplainedProperty | null =>
    getExplainedProperty(explain, queuePath, propertyName);

  const stageChange = (propertyName: string, value: string) => {
    stageQueueChange(queuePath, propertyName, value);
  };

  const collectTemplateMatches = <T>(
    fromConfig: (key: string) => T | null | undefined,
    fromChange: (change: (typeof stagedChanges)[number]) => T | null | undefined,
  ): T[] => {
    const matches = new Set<T>();

    for (const key of configData.keys()) {
      const match = fromConfig(key);
      if (match != null) {
        matches.add(match);
      }
    }

    stagedChanges.forEach((change) => {
      const match = fromChange(change);
      if (match != null) {
        matches.add(match);
      }
    });

    return Array.from(matches);
  };

  const collectTemplateProperties = (templateQueuePath: string): string[] => {
    const prefix = `${CONFIG_PREFIXES.BASE}.${templateQueuePath}.`;

    return collectTemplateMatches(
      (key) => {
        if (!key.startsWith(prefix)) {
          return null;
        }
        const propertyName = key.slice(prefix.length);
        return propertyName || null;
      },
      (change) => {
        if (change.queuePath === templateQueuePath && change.property) {
          return change.property;
        }
        return null;
      },
    );
  };

  const collectTemplateQueuePaths = (suffix: string): string[] => {
    const configPrefix = `${CONFIG_PREFIXES.BASE}.`;
    const basePrefix = `${queuePath}.`;
    const suffixToken = `.${suffix}`;

    const matches = collectTemplateMatches(
      (key) => {
        if (!key.startsWith(configPrefix)) {
          return null;
        }
        const remainder = key.slice(configPrefix.length);
        if (!remainder.startsWith(basePrefix)) {
          return null;
        }
        const index = remainder.indexOf(suffixToken);
        if (index === -1) {
          return null;
        }
        return remainder.slice(0, index + suffix.length);
      },
      (change) => {
        const changePath = change.queuePath;
        if (!changePath) {
          return null;
        }
        if (
          changePath === `${queuePath}.${suffix}` ||
          (changePath.startsWith(basePrefix) && changePath.includes(suffixToken))
        ) {
          return changePath;
        }
        return null;
      },
    );

    const defaultPath = `${queuePath}.${suffix}`;
    if (!matches.includes(defaultPath)) {
      matches.push(defaultPath);
    }

    return Array.from(new Set(matches));
  };

  const stageTemplatePropertyRemovals = (templateQueuePaths: string[]): number => {
    let removals = 0;

    templateQueuePaths.forEach((templatePath) => {
      const properties = collectTemplateProperties(templatePath);
      properties.forEach((property) => {
        stageQueueChange(templatePath, property, '');
        removals += 1;
      });
    });

    return removals;
  };

  const onSubmit = async (data: Record<string, string>) => {
    try {
      const fieldNameMapping: Record<string, string> = {};
      const changedData: Record<string, string> = {};

      allProperties.forEach((property) => {
        const escapedName = property.formFieldName || property.name;
        const originalName = property.originalName || property.name;
        fieldNameMapping[escapedName] = originalName;
      });

      Object.entries(form.formState.dirtyFields).forEach(([escapedFieldName, isDirty]) => {
        if (isDirty && typeof data[escapedFieldName] === 'string') {
          const originalName = fieldNameMapping[escapedFieldName] || escapedFieldName;
          changedData[originalName] = data[escapedFieldName];
        }
      });

      const pendingEntries = Object.entries(changedData);

      let stagedCount = 0;
      let flexibleTemplatesDisabled = false;

      pendingEntries.forEach(([propertyName, value]) => {
        stageChange(propertyName, value);
        stagedCount += 1;

        if (propertyName === 'auto-queue-creation-v2.enabled' && value !== 'true') {
          flexibleTemplatesDisabled = true;
        }
      });

      if (flexibleTemplatesDisabled) {
        const flexiblePaths = new Set<string>([
          ...collectTemplateQueuePaths('auto-queue-creation-v2.template'),
          ...collectTemplateQueuePaths('auto-queue-creation-v2.parent-template'),
          ...collectTemplateQueuePaths('auto-queue-creation-v2.leaf-template'),
        ]);
        if (flexiblePaths.size > 0) {
          stagedCount += stageTemplatePropertyRemovals(Array.from(flexiblePaths));
        }
      }

      const result = {
        success: true,
        message: `${stagedCount} change${stagedCount !== 1 ? 's' : ''} staged successfully!`,
      };

      if (stagedCount > 0) {
        const latestValues = form.getValues();
        reset(latestValues, {
          keepDirty: false,
          keepDirtyValues: false,
        });
        cleanResetRef.current = true;
      }

      toast.success(result.message);

      return result;
    } catch (error) {
      const errorMessage = error instanceof Error ? error.message : 'Failed to stage changes';
      toast.error(errorMessage);
      throw error;
    }
  };

  const handleReset = () => {
    // Clear only the changes for the current queue
    clearQueueChanges(queuePath);

    // Reset form to original values
    const currentValues: Record<string, string> = {};
    allProperties.forEach((property) => {
      const { value } = getQueuePropertyValue(queuePath, property.originalName || property.name);
      const fieldName = property.formFieldName || property.name;
      currentValues[fieldName] = value;
    });
    cleanResetRef.current = true;
    reset(currentValues);
  };

  const hasChangesCheck = !Array.isArray(stagedChanges)
    ? false
    : stagedChanges.filter((c) => c.queuePath === queuePath).length > 0;
  const hasChanges = hasChangesCheck;

  const propertiesByCategoryTemp: Record<string, PropertyDescriptor[]> = {};

  allProperties.forEach((property) => {
    if (!propertiesByCategoryTemp[property.category]) {
      propertiesByCategoryTemp[property.category] = [];
    }
    propertiesByCategoryTemp[property.category].push(property);
  });

  const propertiesByCategory = propertiesByCategoryTemp;

  // Server errors for this queue, keyed by property name, so categories with errors expand.
  const errors: Record<string, { type: string; message: string }> = {};
  getQueueIssues(serverIssues, queuePath)
    .filter((issue) => issue.severity === 'error')
    .forEach((issue) => {
      const name = getIssuePropertyName(issue);
      if (!name) return;
      const existing = errors[name];
      errors[name] = {
        type: 'server',
        message: existing ? `${existing.message}. ${issue.message}` : issue.message,
      };
    });

  // Server issues never block staging: they describe the staged proposal, and staging is
  // how the user fixes them.
  const isFormValid = form.formState.isValid;

  return {
    form,
    control,
    handleSubmit: handleSubmit(onSubmit),
    handleReset,
    stageChange,
    errors,
    isValid: isFormValid,

    hasChanges,
    watchedValues,
    propertiesByCategory,

    getStagedStatus,
    getExplained,

    properties: allProperties,
    formState: form.formState,

    getFieldErrors,
    getFieldWarnings,
  };
}
