<!--
   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to You under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
-->

Hadoop: YARN Capacity Scheduler UI
===================================

Overview
--------

The YARN Capacity Scheduler UI is a modern web interface for managing the
YARN Capacity Scheduler configuration. It provides visual queue management,
node label administration, placement rule editing, and global scheduler
settings — all from the browser.

Key features include:

* **Queue Management** — View and edit the queue hierarchy with an interactive
  tree visualization.
* **Staged Changes** — Stage configuration changes, review them as a batch,
  and apply them to the cluster in one operation.
* **Validation** — Real-time validation of configuration changes with
  error and warning severity levels.
* **Node Labels** — Create, delete, and assign node labels to cluster nodes.
* **Placement Rules** — Configure application placement policies.
* **Global Settings** — Manage cluster-wide scheduler parameters.
* **Read-Only Mode** — Optionally restrict the UI to view-only access.

Prerequisites
-------------

The Capacity Scheduler UI must be built by passing `-Pyarn-ui` to Maven.
Refer to `BUILDING.txt` for more details.

Configurations
--------------

*In `yarn-site.xml`*

| Configuration Property | Description |
|:---- |:---- |
| `yarn.webapp.scheduler-ui.enable` | *(Required)* Enables the Capacity Scheduler UI on the ResourceManager. Defaults to `false`. |
| `yarn.webapp.scheduler-ui.war-file-path` | *(Optional)* WAR file path for the Capacity Scheduler UI web application. By default this is empty and YARN will look up the required WAR file from the classpath. |
| `yarn.webapp.scheduler-ui.read-only.enable` | *(Optional)* When set to `true`, the UI operates in read-only mode: users can view configuration and stage changes for review, but cannot apply mutations to the cluster. Defaults to `false`. |

If you run YARN daemons locally for testing, you need the following
configurations added to `yarn-site.xml` to enable cross-origin (CORS) support.

| Configuration Property | Value | Description |
|:---- |:---- |:---- |
| `yarn.resourcemanager.webapp.cross-origin.enabled` | true | Enable CORS support for Resource Manager |

Also ensure that CORS related configurations are enabled in `core-site.xml`.
Refer to [HTTP Authentication](../../hadoop-project-dist/hadoop-common/HttpAuthentication.html)
for details.

Use it
------

Open your browser and go to `rm-address:8088/scheduler-ui`.

Notes
-----

* The Capacity Scheduler UI is served by the ResourceManager as a separate
  web application context at the `/scheduler-ui` path.
* The UI communicates with the ResourceManager via its REST API endpoints
  under `/ws/v1/cluster/`.
* In read-only mode, mutation endpoints (updating scheduler configuration,
  adding/removing node labels, replacing node-to-label mappings) are blocked
  on the client side.
* This UI framework is verified under security environment as well.


Validation and concurrent changes
---------------------------------

The UI has no validation rules of its own.
Staged changes are validated by the ResourceManager through `POST /ws/v1/cluster/scheduler-conf/validate/v2`.
Validation runs shortly after every staged edit, and after the configuration is reloaded, whether or not the staged changes panel is open.
A result that arrives after the staged changes were edited again is discarded.
The proposal is validated as Apply will submit it, including the queues Apply stops first, such as the parent of a new child queue or a queue being removed.
Validation runs the same way whether or not legacy queue mode is enabled.

Issues are shown where they belong: on the property field and staged change with the same queue path and configuration key, on the queue in the tree, and in the staged changes panel.
Issues without a queue path, and issues for a key that is not being edited, are listed in the staged changes panel.
Errors block Apply; warnings do not, and the warnings of the last applied proposal stay visible until the next edit.
If the validation request itself fails, for example because the caller is not an administrator, the panel reports that validation is unavailable.

Queue property fields show where a value that the queue does not set comes from: a parent queue, a global setting, an auto-creation template, the scheduler default or a derived value.
This information comes from the `explain` section of the validation response.

Apply validates the proposal once more and then sends `PUT /ws/v1/cluster/scheduler-conf` with an `If-Match` header carrying the `ETag` of the last configuration read.
If the configuration was changed by someone else in the meantime, the ResourceManager answers `412 Precondition Failed` and nothing is applied.
The staged changes are kept, and the panel offers to reload the configuration and compare: values that changed under a staged edit are marked so they can be reviewed before applying again.
A write whose outcome is unknown, for example after a network error, is never retried automatically; reload the configuration to see whether it was applied.

The `ETag` is only readable when the UI is served by the ResourceManager itself.
Cross-origin setups do not expose the header, and the Router does not return it, so writes made through them are not guarded by `If-Match`.
