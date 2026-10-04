<!--
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->

# Extending Scheduler Properties

This guide explains how to make new Capacity Scheduler properties editable in the UI.
Validation is done by the ResourceManager; see adding-validation-rules.md.
Follow the relevant section depending on whether you are working with global scheduler settings or queue-level configuration.

## Key modules

- `src/config/properties/global-properties.ts`: property descriptors that drive the Global Settings page.
- `src/config/properties/queue-properties.ts`: queue-level descriptors used by the property editor, queue dialogs, and staged changes.
- `src/features/validation/service.ts`: maps server validation issues and explained value sources to fields.
- `src/config/__tests__/propertyDefinitions.test.ts`: regression tests that assert descriptor consistency.

## Adding a global property

1. **Define the descriptor** in `src/config/properties/global-properties.ts`.
   - Use the fully qualified key (for example, `yarn.scheduler.capacity.maximum-applications`).
   - Populate `displayName`, `description`, `type`, `category`, `defaultValue`, and `required`.
   - Use `inputRange` for HTML number-input affordances.
   - The ResourceManager validates global property semantics when the mutation is submitted.
   - Supply `enumValues` when `type` is `enum`. Each option should provide `{ value, label, description? }`. Use `enumDisplay` to control the visual presentation: `choiceCard` for prominent cards or `toggle` for compact buttons (see below for guidance).

- Use `displayFormat` to add user-friendly suffixes to numeric inputs.
- Add conditional logic with `showWhen` / `enableWhen` when the property should only appear or be interactive under specific scheduler states. Each condition receives the merged configuration context (global + queue values, staged changes, scheduler metadata).

**Available property categories** (defined in `src/types/property-descriptor.ts`):

- `'resource'` - Resource allocation settings
- `'scheduling'` - Scheduling policies and behavior
- `'security'` - ACLs and permissions
- `'core'` - Core scheduler settings
- `'application-limits'` - Application count and resource limits
- `'placement'` - Placement rules and policies
- `'container-allocation'` - Container sizing and allocation
- `'async-scheduling'` - Asynchronous scheduling configuration
- `'capacity'` - Capacity values and modes
- `'dynamic-queues'` - Auto-created queue settings
- `'node-labels'` - Node label and partition configuration
- `'preemption'` - Preemption policies

```ts
{
  name: 'yarn.scheduler.capacity.sample-property',
  displayName: 'Sample Property',
  description: 'What this property controls.',
  type: 'number',
  category: 'core',
  defaultValue: '0',
  required: false,
  inputRange: { min: 0, max: 10 },
},
```

### Choosing the right `enumDisplay` variant

For `enum` type properties, you can control the visual presentation using the `enumDisplay` field:

- **`choiceCard`** - Large bordered radio cards with labels, descriptions, and "Selected" badge. Best for 2-3 options where descriptions are important and visual prominence is desired.

```ts
{
  name: 'yarn.scheduler.capacity.resource-calculator',
  type: 'enum',
  enumValues: [
    {
      value: 'org.apache.hadoop.yarn.util.resource.DefaultResourceCalculator',
      label: 'Default (Memory Only)',
      description: 'Memory-based calculator suitable for clusters without CPU enforcement.',
    },
    {
      value: 'org.apache.hadoop.yarn.util.resource.DominantResourceCalculator',
      label: 'Dominant Resource',
      description: 'Considers the dominant resource usage across memory and CPU.',
    },
  ],
  enumDisplay: 'choiceCard',  // Renders as large cards in a grid
}
```

- **`toggle`** (default) - Compact toggle group (pill buttons) in horizontal layout. Space-efficient for 2-4 options with self-explanatory labels. This is the default when `enumDisplay` is omitted.

```ts
{
  name: 'yarn.scheduler.capacity.queue-state',
  type: 'enum',
  enumValues: [
    { value: 'RUNNING', label: 'Running' },
    { value: 'STOPPED', label: 'Stopped' },
    { value: 'DRAINING', label: 'Draining' },
  ],
  // enumDisplay defaults to 'toggle' - no need to specify
}
```

### Using `displayFormat` for numeric inputs

The `displayFormat` object adds visual hints and formatting to numeric input fields:

```ts
export type DisplayFormat = {
  suffix?: string; // Text appended inside input (e.g., "(0.0-1.0)")
  prefix?: string; // Text prepended to input
  multiplier?: number; // Value multiplier for display
  decimals?: number; // Number of decimal places (affects step)
};
```

**Example:**

```ts
{
  name: 'maximum-am-resource-percent',
  type: 'number',
  displayFormat: {
    suffix: ' (0.0-1.0)',  // Shows range hint inside the input
    decimals: 2,           // Allows 0.01 step increments
  },
}
```

The `suffix` renders as muted gray text positioned inside the right side of the input field, providing an inline hint about the expected range or format. The `decimals` value controls the input's `step` attribute (0.01 for 2 decimals, 0.001 for 3 decimals, etc.).

### Value sources

Queue property fields show where their resolved value comes from (the queue itself, a parent queue, a global setting, a template, the scheduler default or a derived value).
The label comes from the explain section of `POST /scheduler-conf/validate/v2`, keyed by queue path and full property key, so a new queue property needs no inheritance code in the UI.
If the server does not explain a key, the field shows no source label.

2. **Adjust the UI if needed.** The global settings form renders inputs based on `PropertyDescriptor.type`. For bespoke widgets, extend `src/features/global-settings/components/PropertyInput.tsx`.
3. **Update tests.** Extend `src/config/__tests__/propertyDefinitions.test.ts` if you need coverage for descriptor metadata.
4. **Verify** by running the app or unit tests (`npm run test`) and confirming the new field renders and its mutation is accepted by ResourceManager validation.

### Global property validation

Global settings are staged directly.
The ResourceManager validates their semantics through `POST /scheduler-conf/validate/v2` when the user applies the staged mutation.
Do not add a second client-side implementation of a global scheduler rule.

## Adding a queue-level property

1. **Add a descriptor** in `src/config/properties/queue-properties.ts`.
   - Queue descriptors use the short key (`capacity`, `maximum-capacity`, etc.).

- Populate the same core fields as global descriptors. Add `showWhen` when the field should be hidden until prerequisites are met and `enableWhen` to keep the field visible but read-only. Both take arrays of predicates that receive the current queue/global context.
- For enum properties, keep the `{ value, label, description? }` shape and select an `enumDisplay` variant if the default toggle group is not ideal.
- Set `required: true` if the field must be provided when adding new queues.

```ts
{
  name: 'example-threshold',
  displayName: 'Example Threshold',
  description: 'Upper bound applied per queue.',
  type: 'number',
  category: 'application-limits',
  defaultValue: '',
  required: false,
  inputRange: { min: 0 },
},
```

2. **Wire dependent UI.** Components such as `PropertyFormField` and `PropertyEditorTab` already read descriptors; only extend them if you need new interaction patterns.
3. **Tests.** Update `propertyDefinitions.test.ts` or add targeted tests under `src/features/property-editor` / `src/stores` when the new field affects staged-change flows or reducers.

## Working with validation

The UI has no validation rules.
Staged changes are validated by `POST /scheduler-conf/validate/v2`, and the returned issues are attached to fields by queue path and full property key.
Add new checks to the ResourceManager validation engine; `inputRange` on a descriptor is only an HTML input affordance.

### Property condition utilities

The `src/utils/propertyConditions.ts` module provides two utility functions for evaluating `showWhen` and `enableWhen` conditions outside of the form rendering pipeline. These are useful when you need to programmatically check property visibility or enabled state:

#### `shouldShowProperty(property, options)`

Evaluates all `showWhen` conditions for a property. Returns `true` if the property should be visible.

```ts
import { shouldShowProperty } from '~/utils/propertyConditions';

const visible = shouldShowProperty(property, {
  scope: 'global',
  property,
  propertyValue: currentValue,
  values: formValues,
  globalValues,
  stagedChanges,
  configData,
  schedulerInfo,
  // ... other context
});
```

#### `isPropertyEnabled(property, options)`

Evaluates all `enableWhen` conditions for a property. Returns `true` if the property should be interactive (not disabled).

```ts
import { isPropertyEnabled } from '~/utils/propertyConditions';

const enabled = isPropertyEnabled(property, {
  scope: 'queue',
  queuePath: 'root.production',
  property,
  propertyValue: currentValue,
  // ... other context
});
```

**Error handling:** Both functions catch and log errors from condition evaluation, defaulting to `true` (show/enable) on failure to ensure the UI remains functional even with misconfigured conditions.

**When to use these utilities:**

- In component logic that needs to conditionally render UI based on property visibility
- When computing available properties for autocomplete or validation
- For debugging or testing property condition behavior
- Use inline predicates in property definitions when the condition is simple and self-contained

## Sanity checklist

- [ ] Descriptor added to the appropriate file with accurate metadata.
- [ ] UI renders the expected input type (extend components only if necessary).
- [ ] Any new check added to the ResourceManager validation engine, not to the UI.
- [ ] Tests updated or added, and `npm run test` completes successfully.
