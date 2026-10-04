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

The ResourceManager is the only validator of Capacity Scheduler configuration.
The UI has no validation rules of its own; it sends the staged proposal to the server and displays the issues it gets back.

## Server validation

The staged-changes store posts the proposal to POST /scheduler-conf/validate/v2 with explain=affected.
Every change to the staged proposal, and every reload of the baseline, schedules a validation after a short debounce (VALIDATION_DEBOUNCE_MS in stores/slices/stagedChangesSlice.ts).
The trigger is a store subscription, so validation does not depend on which panel is open.
Each request is tagged with the proposal identity (the baseline ETag plus the request body); a response for an older identity is discarded.

The validated request is the proposal as apply submits it: queues that apply stops first (parents of new queues, removed queues, queues gaining auto-creation) appear as STOPPED, and new queues appear STOPPED until they are started after the mutation.
Lifecycle prerequisites that apply satisfies itself are therefore never reported.

A 200 response is always a validation result.
Any other status, including a 400 RemoteException, is a failed request: it is shown as "Validation unavailable" and never as an accepted proposal.

## Issue mapping

service.ts holds the single mapper from server issues to fields.
Issues are matched only by their exact (queuePath, propertyKey) pair:

- getPropertyIssues() for a queue property, with the key built by buildPropertyKey().
- getGlobalPropertyIssues() for a global property; global issues have a null queuePath.
- getStagedChangeIssues() for the key a staged change writes.
- getQueueIssues() and getGlobalIssues() for badges and summary lists.

There are no heuristics: an issue whose key matches no field is still listed in the staged-changes panel and counted in the queue badges.

## Value sources

The explain section of validate/v2 reports, per queue and full key, the resolved value and where it comes from (QUEUE, PARENT, GLOBAL, TEMPLATE_V1, TEMPLATE_V2, DEFAULT, DERIVED).
indexExplain() and getExplainedProperty() read it, and describeValueSource() turns it into the label shown under a property field.
The property editor requests explain for the selected queue with loadExplain().

## Adding validation

Add every check to the ResourceManager validation engine.
See docs/development/adding-validation-rules.md.

## Tests

Mapper tests live in <code>src/features/validation/**tests**/service.test.ts</code>.
Debounce, stale-response, apply and 412 tests live in <code>src/stores/slices/**tests**/serverProposal.test.ts</code>.
