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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.conf.model;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.ConfigurationProperties;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueuePath;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueuePrefixes;

import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.AutoCreatedQueueTemplate.AUTO_QUEUE_LEAF_TEMPLATE_PREFIX;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.AutoCreatedQueueTemplate.AUTO_QUEUE_PARENT_TEMPLATE_PREFIX;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.AutoCreatedQueueTemplate.AUTO_QUEUE_TEMPLATE_PREFIX;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration.AUTO_CREATED_LEAF_QUEUE_TEMPLATE_PREFIX;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration.MAXIMUM_AM_RESOURCE_SUFFIX;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration.USER_LIMIT_FACTOR;

/** Immutable result of one total Capacity Scheduler configuration parse. */
public final class CSConfigModel {
  private static final String ACL_PROPERTY_PREFIX = "acl_";

  private final QueueConfigNode root;
  private final Map<QueuePath, QueueConfigNode> nodes;
  private final boolean legacyQueueMode;
  private final String mappingRuleFormat;
  private final int maximumAutoCreatedQueueDepth;
  private final String resourceTypes;
  private final ConfigurationProperties rawSnapshot;
  private final Map<String, String> rawProperties;
  private final Map<String, Set<String>> configuredNodeLabelsByQueue;
  private final CSConfigModelBuilder.NodeDefaults nodeDefaults;
  private final List<ConfigDiagnostic> diagnostics;

  @SuppressWarnings("checkstyle:ParameterNumber")
  CSConfigModel(QueueConfigNode root, Map<QueuePath, QueueConfigNode> nodes,
      boolean legacyQueueMode, String mappingRuleFormat,
      int maximumAutoCreatedQueueDepth, String resourceTypes,
      ConfigurationProperties rawSnapshot, Map<String, String> rawProperties,
      Map<String, Set<String>> configuredNodeLabelsByQueue,
      CSConfigModelBuilder.NodeDefaults nodeDefaults,
      List<ConfigDiagnostic> diagnostics) {
    this.root = root;
    this.nodes = Collections.unmodifiableMap(new LinkedHashMap<>(nodes));
    this.legacyQueueMode = legacyQueueMode;
    this.mappingRuleFormat = mappingRuleFormat;
    this.maximumAutoCreatedQueueDepth = maximumAutoCreatedQueueDepth;
    this.resourceTypes = resourceTypes;
    this.rawSnapshot = rawSnapshot;
    this.rawProperties = Collections.unmodifiableMap(
        new LinkedHashMap<>(rawProperties));
    Map<String, Set<String>> labels = new LinkedHashMap<>();
    configuredNodeLabelsByQueue.forEach((path, values) ->
        labels.put(path, Set.copyOf(values)));
    this.configuredNodeLabelsByQueue = Collections.unmodifiableMap(labels);
    this.nodeDefaults = nodeDefaults;
    this.diagnostics = List.copyOf(diagnostics);
  }

  public QueueConfigNode getRoot() {
    return root;
  }

  public QueueConfigNode getNode(QueuePath queuePath) {
    return nodes.get(queuePath);
  }

  public Map<QueuePath, QueueConfigNode> getNodes() {
    return nodes;
  }

  public boolean isLegacyQueueMode() {
    return legacyQueueMode;
  }

  public String getMappingRuleFormat() {
    return mappingRuleFormat;
  }

  public int getMaximumAutoCreatedQueueDepth() {
    return maximumAutoCreatedQueueDepth;
  }

  public String getResourceTypes() {
    return resourceTypes;
  }

  public ConfigurationProperties getRawSnapshot() {
    return rawSnapshot;
  }

  public Map<String, String> getRawProperties() {
    return rawProperties;
  }

  public Map<String, Set<String>> getConfiguredNodeLabelsByQueue() {
    return configuredNodeLabelsByQueue;
  }

  public Set<String> getConfiguredNodeLabels(QueuePath queuePath) {
    Set<String> labels = new LinkedHashSet<>();
    labels.addAll(configuredNodeLabelsByQueue.getOrDefault(
        queuePath.getFullPath(), Collections.emptySet()));
    labels.add("");
    return Collections.unmodifiableSet(labels);
  }

  public Set<String> getConfiguredNodeLabelsForAllQueues() {
    Set<String> labels = new LinkedHashSet<>();
    configuredNodeLabelsByQueue.values().forEach(labels::addAll);
    labels.add("");
    return Collections.unmodifiableSet(labels);
  }

  public List<ConfigDiagnostic> getDiagnostics() {
    return diagnostics;
  }

  public Map<String, String> getCommonTemplateProperties(QueuePath owner) {
    return resolvedTemplateProperties(owner, AUTO_QUEUE_TEMPLATE_PREFIX);
  }

  public Map<String, String> getLeafTemplateProperties(QueuePath owner) {
    return resolvedTemplateProperties(owner, AUTO_QUEUE_LEAF_TEMPLATE_PREFIX);
  }

  public Map<String, String> getParentTemplateProperties(QueuePath owner) {
    return resolvedTemplateProperties(owner, AUTO_QUEUE_PARENT_TEMPLATE_PREFIX);
  }

  /**
   * Resolves a configured queue from the immutable model.
   * Hypothetical queue template resolution is added by the S2c consumer slice.
   * @param queuePath queue to resolve
   * @return configured queue node, or null when the queue is not configured
   */
  public QueueConfigNode effectiveConfigFor(QueuePath queuePath) {
    QueueConfigNode configured = getNode(queuePath);
    boolean isLeaf = configured == null || configured.getChildren().isEmpty();
    return effectiveConfigFor(queuePath, isLeaf);
  }

  public QueueConfigNode effectiveConfigFor(QueuePath queuePath,
      boolean isLeaf) {
    return effectiveConfigFor(queuePath, isLeaf, false);
  }

  public QueueConfigNode effectiveConfigFor(QueuePath queuePath,
      boolean isLeaf, boolean dynamic) {
    if (queuePath.isRoot()) {
      return root;
    }
    Map<String, String> effective = new LinkedHashMap<>();
    Map<String, ConfigProvenance> provenance = new LinkedHashMap<>();
    Map<String, String> commonTemplates = new LinkedHashMap<>();
    Map<String, String> typeTemplates = new LinkedHashMap<>();
    Map<String, String> legacyLeafTemplates = new LinkedHashMap<>();
    QueueConfigNode configured = getNode(queuePath);
    QueuePath parentPath = queuePath.getParentObject();
    QueueConfigNode parent = getNode(parentPath);
    boolean v1ManagedAutoLeaf = isLeaf && dynamic && parent != null
        && parent.isAutoCreateChildQueueEnabled();

    for (QueuePath templateOwner : parentPath.getWildcardedQueuePaths(
        maximumDepthFor(parentPath))) {
      collectTemplate(templateOwner, AUTO_QUEUE_TEMPLATE_PREFIX,
          commonTemplates);
      collectTemplate(templateOwner,
          isLeaf ? AUTO_QUEUE_LEAF_TEMPLATE_PREFIX
              : AUTO_QUEUE_PARENT_TEMPLATE_PREFIX,
          typeTemplates);
    }
    if (isLeaf) {
      if (v1ManagedAutoLeaf) {
        collectTemplate(parentPath,
            AUTO_CREATED_LEAF_QUEUE_TEMPLATE_PREFIX + ".",
            legacyLeafTemplates);
        // The v1 setter reads only the v1 leaf template for capacity vectors.
        // Keep v2 leaf properties for the other dynamic settings, but do not
        // let their capacity keys bypass that compatibility boundary.
        typeTemplates.entrySet().removeIf(entry -> isCapacityProperty(
            entry.getKey()));
        commonTemplates.entrySet().removeIf(entry -> isCapacityProperty(
            entry.getKey()));
        legacyLeafTemplates.forEach(typeTemplates::put);
      } else {
        collectTemplate(parentPath,
            AUTO_CREATED_LEAF_QUEUE_TEMPLATE_PREFIX + ".", typeTemplates);
      }
    }
    if (!dynamic) {
      // Auto queue creation templates describe the queues a parent creates,
      // not the ones an administrator declared. Their acl_* entries must not
      // reach a statically configured queue: getAcls falls back to NONE only
      // when the key is absent, and ConfiguredYarnAuthorizer widens access as
      // it walks up the hierarchy, so an inherited ACL can never be narrowed
      // by an ancestor again.
      commonTemplates.entrySet().removeIf(
          entry -> isAclProperty(entry.getKey()));
      typeTemplates.entrySet().removeIf(
          entry -> isAclProperty(entry.getKey()));
    }
    if (!dynamic && configured != null && commonTemplates.isEmpty()
        && typeTemplates.isEmpty()) {
      return configured;
    }
    commonTemplates.forEach((key, value) -> {
      effective.putIfAbsent(key, value);
      provenance.putIfAbsent(key, ConfigProvenance.TEMPLATE);
    });
    typeTemplates.forEach((key, value) -> {
      effective.put(key, value);
      provenance.put(key, ConfigProvenance.TEMPLATE);
    });
    if (isLeaf && dynamic) {
      effective.putIfAbsent(USER_LIMIT_FACTOR, "-1");
      effective.putIfAbsent(MAXIMUM_AM_RESOURCE_SUFFIX, "1");
      provenance.putIfAbsent(USER_LIMIT_FACTOR, ConfigProvenance.DEFAULT);
      provenance.putIfAbsent(MAXIMUM_AM_RESOURCE_SUFFIX,
          ConfigProvenance.DEFAULT);
    }
    if (configured != null) {
      configured.getRawProperties().forEach((key, value) -> {
        effective.put(key, value);
        provenance.put(key, ConfigProvenance.USER);
      });
    } else {
      rawSnapshot.getPropertiesWithPrefix(QueuePrefixes.getQueuePrefix(queuePath))
          .forEach((key, value) -> {
            effective.put(key, value);
            provenance.put(key, ConfigProvenance.USER);
          });
    }
    return CSConfigModelBuilder.buildEffectiveNode(queuePath,
        parent, effective, provenance, nodeDefaults);
  }

  private static boolean isCapacityProperty(String key) {
    return key.equals(CapacitySchedulerConfiguration.CAPACITY)
        || key.endsWith("." + CapacitySchedulerConfiguration.CAPACITY)
        || key.equals(CapacitySchedulerConfiguration.MAXIMUM_CAPACITY)
        || key.endsWith("." + CapacitySchedulerConfiguration.MAXIMUM_CAPACITY);
  }

  /**
   * Every Capacity Scheduler ACL key is built as {@code "acl_"} followed by a
   * lower-cased enum name, covering {@code QueueACL},
   * {@code ReservationACL} and {@code AccessType}. Matching the last path
   * segment therefore catches all of them, including any label-scoped form.
   *
   * @param key queue-relative property key
   * @return true when the key names an ACL
   */
  private static boolean isAclProperty(String key) {
    return key.startsWith(ACL_PROPERTY_PREFIX, key.lastIndexOf('.') + 1);
  }

  private void collectTemplate(QueuePath owner, String templatePrefix,
      Map<String, String> destination) {
    String prefix = QueuePrefixes.getQueuePrefix(owner) + templatePrefix;
    rawSnapshot.getPropertiesWithPrefix(prefix).forEach(destination::putIfAbsent);
  }

  private Map<String, String> resolvedTemplateProperties(QueuePath owner,
      String templatePrefix) {
    if (owner.isInvalid()) {
      return Collections.emptyMap();
    }
    Map<String, String> resolved = new LinkedHashMap<>();
    for (QueuePath candidate : owner.getWildcardedQueuePaths(
        maximumDepthFor(owner))) {
      collectTemplate(candidate, templatePrefix, resolved);
    }
    return Collections.unmodifiableMap(resolved);
  }

  private int maximumDepthFor(QueuePath owner) {
    String configured = rawProperties.get(QueuePrefixes.getQueuePrefix(owner)
        + CapacitySchedulerConfiguration.MAXIMUM_QUEUE_DEPTH);
    return configured == null ? maximumAutoCreatedQueueDepth
        : Integer.parseInt(configured);
  }
}
