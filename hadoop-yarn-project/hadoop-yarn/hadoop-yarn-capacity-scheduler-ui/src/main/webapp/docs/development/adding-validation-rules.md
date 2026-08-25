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

# Adding Validation Rules

The ResourceManager is the source of truth for Capacity Scheduler configuration semantics.
The UI should validate only what provides safe, immediate editing feedback without duplicating scheduler logic.

## Choose the owner first

Use a queue property descriptor rule when one input has a syntax, pattern, integer, or range requirement that should be shown before submission.
Use src/config/validation-rules.ts when a check relates fields on the same queue and immediate feedback materially helps the editor.
Use a pure UI gate when an action is unavailable by design, such as deleting the root queue.
Add every sibling, parent-child, queue-tree, global, runtime-dependent, or scheduler-wide rule to the ResourceManager validation engine.

Do not add a client copy of a server structural rule.
The UI always posts the complete proposed mutation to POST /scheduler-conf/validate/v2 before applying it.

## Queue property syntax

Add validationRules to the descriptor in src/config/properties/queue-properties.ts.
The property editor converts descriptor rules into its Zod schema and displays failures inline.

```typescript
{
  name: 'maximum-applications',
  displayName: 'Maximum Applications',
  type: 'number',
  validationRules: [
    {
      type: 'custom',
      message: 'Must be 0 or a positive integer',
      validator: (value) => /^\d+$/.test(value),
    },
  ],
}
```

Global property descriptors do not run client-side semantic validation.
Use inputRange only when a number input benefits from HTML min or max affordances.
The server validates the submitted value.

## Queue-scoped cross-field checks

Add the rule to QUEUE_VALIDATION_RULES in src/config/validation-rules.ts.
Return a stable lowercase rule identifier in each ValidationIssue.
An error blocks staging, while a warning can be staged.

```typescript
{
  id: 'MY_QUEUE_SCOPED_RULE',
  description: 'Explains the local relationship.',
  level: 'error',
  triggers: ['first-property', 'second-property'],
  evaluate: (context) => evaluateMyQueueScopedRule(context),
}
```

The evaluator may read the edited queue's effective values from context.config.
It must not traverse parents, children, siblings, or live scheduler state.

```typescript
function evaluateMyQueueScopedRule(context: ValidationContext): ValidationIssue[] {
  const relatedValue = context.config.get(buildPropertyKey(context.queuePath, 'second-property'));

  if (isInvalid(context.fieldValue, relatedValue)) {
    return [
      {
        queuePath: context.queuePath,
        field: context.fieldName,
        message: 'Explain how to correct the value.',
        severity: 'error',
        rule: 'my-queue-scoped-rule',
      },
    ];
  }

  return [];
}
```

## Server validation contract

The API client accepts both the 200 valid response and the structured 400 invalid response from /scheduler-conf/validate/v2.
The response contains valid, configVersion, and an issue list with queuePath, propertyKey, ruleId, severity, and message.
Warnings are legal in a valid response.
The staged-changes store aborts before PUT /scheduler-conf when valid is false and threads configVersion into a valid mutation.

## Tests

Add descriptor tests under <code>src/config/**tests**/propertyDefinitions.test.ts</code>.
Add queue-scoped evaluator tests under <code>src/config/**tests**/validation-rules.test.ts</code>.
Add server-owned validation tests to the ResourceManager validation engine and REST endpoint tests.
Run npm run test:run, npm run typecheck, and npm run lint.
