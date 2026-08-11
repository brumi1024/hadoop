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
package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.util.resource.Resources;

/** Node-label manager backed by the immutable ClusterFacts snapshot. */
final class SnapshotNodeLabelsManager extends RMNodeLabelsManager {
  private final Map<String, Resource> resourcesByLabel;
  private final Set<String> nodeLabels;

  SnapshotNodeLabelsManager(Map<String, Resource> resourcesByLabel,
      Set<String> nodeLabels) {
    this.resourcesByLabel = new LinkedHashMap<>();
    resourcesByLabel.forEach((label, resource) -> this.resourcesByLabel.put(
        label, Resources.clone(resource)));
    this.nodeLabels = Collections.unmodifiableSet(new LinkedHashSet<>(
        nodeLabels));
  }

  @Override
  public Resource getResourceByLabel(String label, Resource clusterResource) {
    String normalized = normalizeLabel(label);
    Resource resource = resourcesByLabel.get(normalized);
    if (resource != null) {
      return Resources.clone(resource);
    }
    if (RMNodeLabelsManager.NO_LABEL.equals(normalized)) {
      return Resources.clone(clusterResource);
    }
    return Resources.none();
  }

  @Override
  public Set<String> getClusterNodeLabelNames() {
    return nodeLabels;
  }
}
