/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.conf.model.QueueConfigNode;
import java.io.IOException;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;

/**
 * This class determines accessible node labels, configured node labels and the default node
 * label expression based on the {@link CapacitySchedulerConfiguration} object and other queue
 * properties.
 */
public class QueueNodeLabelsSettings {
  private Set<String> accessibleLabels;
  private Set<String> configuredNodeLabels;
  private String defaultLabelExpression;

  public QueueNodeLabelsSettings(QueueConfigNode queueNode, CSQueue parent,
      Set<String> configuredNodeLabels) throws IOException {
    QueuePath queuePath = queueNode.getQueuePath();
    ResolvedNodeLabels resolved = resolve(queuePath.getFullPath(),
        queuePath.isRoot(), queueNode.getAccessibleNodeLabels(),
        queueNode.getDefaultNodeLabelExpression(), configuredNodeLabels,
        parent == null ? null : parent.getAccessibleNodeLabels(),
        parent == null ? null : parent.getDefaultNodeLabelExpression());
    this.accessibleLabels = resolved.accessibleLabels();
    this.defaultLabelExpression = resolved.defaultLabelExpression();
    this.configuredNodeLabels = resolved.configuredNodeLabels();
  }

  /** Immutable result of node-label inheritance and validation. */
  public record ResolvedNodeLabels(Set<String> accessibleLabels,
      Set<String> configuredNodeLabels, String defaultLabelExpression) {
    public ResolvedNodeLabels {
      accessibleLabels = immutableSet(accessibleLabels);
      configuredNodeLabels = immutableSet(configuredNodeLabels);
    }
  }

  /**
   * Resolves label settings without consulting a queue or label manager.
   * @throws IOException when child labels exceed the parent's label access
   */
  public static ResolvedNodeLabels resolve(String queuePath, boolean root,
      Set<String> declaredAccessibleLabels,
      String declaredDefaultLabelExpression,
      Set<String> configuredNodeLabels, Set<String> parentAccessibleLabels,
      String parentDefaultLabelExpression) throws IOException {
    Set<String> accessible = declaredAccessibleLabels == null
        ? parentAccessibleLabels : declaredAccessibleLabels;
    Set<String> accessibleCopy = accessible == null
        ? Collections.emptySet() : new LinkedHashSet<>(accessible);
    String defaultExpression = declaredDefaultLabelExpression;
    if (!root && defaultExpression == null && parentAccessibleLabels != null
        && accessibleCopy.containsAll(parentAccessibleLabels)) {
      defaultExpression = parentDefaultLabelExpression;
    }
    if (!root && parentAccessibleLabels != null
        && !parentAccessibleLabels.contains(RMNodeLabelsManager.ANY)) {
      if (accessibleCopy.contains(RMNodeLabelsManager.ANY)) {
        throw new IOException("Parent's accessible queue is not ANY(*), "
            + "but child's accessible queue is " + RMNodeLabelsManager.ANY);
      }
      Set<String> difference = new LinkedHashSet<>(accessibleCopy);
      difference.removeAll(parentAccessibleLabels);
      if (!difference.isEmpty()) {
        throw new IOException(String.format(
            "Some labels of child queue is not a subset of parent queue, "
                + "these labels=[%s]", StringUtils.join(difference, ",")));
      }
    }
    return new ResolvedNodeLabels(accessibleCopy, configuredNodeLabels,
        defaultExpression);
  }

  private static Set<String> immutableSet(Set<String> values) {
    return Collections.unmodifiableSet(new LinkedHashSet<>(
        values == null ? Collections.emptySet() : values));
  }

  public boolean isAccessibleToPartition(String nodePartition) {
    // if queue's label is *, it can access any node
    if (accessibleLabels != null && accessibleLabels.contains(RMNodeLabelsManager.ANY)) {
      return true;
    }
    // any queue can access to a node without label
    if (nodePartition == null || nodePartition.equals(RMNodeLabelsManager.NO_LABEL)) {
      return true;
    }
    // a queue can access to a node only if it contains any label of the node
    if (accessibleLabels != null && accessibleLabels.contains(nodePartition)) {
      return true;
    }
    // The partition cannot be accessed
    return false;
  }

  public Set<String> getAccessibleNodeLabels() {
    return accessibleLabels;
  }

  public Set<String> getConfiguredNodeLabels() {
    return configuredNodeLabels;
  }

  public String getDefaultLabelExpression() {
    return defaultLabelExpression;
  }
}
