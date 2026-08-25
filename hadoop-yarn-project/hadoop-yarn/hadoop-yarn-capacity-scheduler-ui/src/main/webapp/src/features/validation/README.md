<!---
  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License. See accompanying LICENSE file.
-->

# Validation Feature

The ResourceManager is authoritative for Capacity Scheduler configuration semantics.
The UI keeps only immediate field syntax checks, queue-scoped cross-field checks, and UX gates that help users edit safely.

## Validation layers

1. Property descriptor rules provide immediate format and range feedback for queue properties.
2. src/config/validation-rules.ts contains the small set of queue-scoped checks that need two values from the same queue.
3. POST /scheduler-conf/validate/v2 validates the complete proposed configuration before the UI sends the mutation.

The UI must not reproduce sibling, parent-child, queue-tree, or global scheduler rules.
Those rules belong in the server validation engine so every client receives the same result.

## Local service

service.ts exposes the local validation entry points:

- validateField() validates one edited property.
- validateQueue() validates the edited properties for one queue.
- validateStagedChanges() refreshes queue-scoped issues attached to staged capacity changes.
- hasBlockingIssues() and splitIssues() support UI gating and rendering.

utils/dedupeIssues.ts removes duplicate local issues.

## Apply-time validation

The staged-changes store posts the complete mutation to /scheduler-conf/validate/v2.
A valid response contains the configuration version that must be included in the subsequent mutation.
Warnings do not block the mutation.
Errors abort the mutation and are rendered in the apply error banner.

## Adding validation

Use property descriptor rules for immediate syntax feedback on queue-scoped inputs.
Use validation-rules.ts only for a queue-scoped relationship that cannot be expressed by one descriptor, such as maximum capacity relative to capacity.
Add structural or scheduler-wide validation to the ResourceManager validation engine instead of the UI.

See docs/development/adding-validation-rules.md for examples and the ownership boundary.

## Tests

Local service tests live in <code>src/features/validation/**tests**/service.test.ts</code>.
Rule tests live in <code>src/config/**tests**/validation-rules.test.ts</code>.
The API and staged-change tests cover the structured server response and version handoff.
