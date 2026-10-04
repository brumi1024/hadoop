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
The UI contains no validation rules.

## Where a rule goes

Add every syntax, range, cross-field, queue-tree, global or runtime-dependent check to the ResourceManager validation engine.
The rule then reaches every client through POST /scheduler-conf/validate/v2.
A UI gate is acceptable only when an action is unavailable by design, such as deleting the root queue, or when a value would make the request itself malformed, such as a queue name containing a list delimiter.

Property descriptors may declare inputRange so a number input gets HTML min and max affordances.
That is an input hint, not validation: the server still decides.

## Server validation contract

The UI posts the staged proposal to /scheduler-conf/validate/v2 and reads the 200 response:

```json
{"validationResult": {
  "valid": false,
  "issues": {"issue": [{"queuePath": "root.a", "propertyKey": "yarn.scheduler.capacity.root.a.capacity",
                        "ruleId": "invalid-capacity", "severity": "ERROR", "message": "..."}]},
  "explain": {"queue": [{"queuePath": "root.a",
                         "property": [{"key": "...", "value": "...", "source": "PARENT", "sourceDetail": "root"}]}]}}}
```

The client accepts a one-element list rendered as a bare object and a missing list.
A null queuePath marks a global issue, and propertyKey is always the fully qualified key.
ERROR issues block apply; WARNING issues do not.
Use a stable kebab-case ruleId; the UI does not interpret it.

## Tests

Add rule tests to the ResourceManager validation engine and REST endpoint tests.
UI tests mock the API client; see <code>src/stores/slices/**tests**/serverProposal.test.ts</code>.
Run npm run test:run, npm run typecheck, and npm run lint.
