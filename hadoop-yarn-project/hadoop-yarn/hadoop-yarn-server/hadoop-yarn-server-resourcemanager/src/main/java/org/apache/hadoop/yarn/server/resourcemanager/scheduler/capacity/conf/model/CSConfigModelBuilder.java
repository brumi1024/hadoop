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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.QueueState;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.ConfigurationProperties;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueCapacityVector;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueCapacityVector.ResourceUnitCapacityType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueuePath;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueuePrefixes;

/** Builds a {@link CSConfigModel} in one pass over a raw property snapshot. */
public final class CSConfigModelBuilder {
  private static final String ROOT = CapacitySchedulerConfiguration.ROOT;
  private static final QueuePath ROOT_PATH = new QueuePath(ROOT);

  private CSConfigModelBuilder() {
  }

  static final class NodeDefaults {
    private final float userLimit;
    private final float userLimitFactor;
    private final float maximumApplicationMasterShare;
    private final int globalMaximumApplications;

    private NodeDefaults(float userLimit, float userLimitFactor,
        float maximumApplicationMasterShare, int globalMaximumApplications) {
      this.userLimit = userLimit;
      this.userLimitFactor = userLimitFactor;
      this.maximumApplicationMasterShare = maximumApplicationMasterShare;
      this.globalMaximumApplications = globalMaximumApplications;
    }

    float getUserLimit() {
      return userLimit;
    }

    float getUserLimitFactor() {
      return userLimitFactor;
    }

    float getMaximumApplicationMasterShare() {
      return maximumApplicationMasterShare;
    }

    int getGlobalMaximumApplications() {
      return globalMaximumApplications;
    }
  }

  public static CSConfigModel build(Configuration configuration) {
    Map<String, String> properties = new LinkedHashMap<>();
    for (Map.Entry<String, String> entry : configuration) {
      properties.put(entry.getKey(), entry.getValue());
    }
    return build(properties);
  }

  public static CSConfigModel build(Map<String, String> inputProperties) {
    Map<String, String> properties = Collections.unmodifiableMap(
        new LinkedHashMap<>(inputProperties));
    List<ConfigDiagnostic> diagnostics = new ArrayList<>();
    for (String key : properties.keySet()) {
      if (Configuration.isDeprecated(key)) {
        diagnostics.add(new ConfigDiagnostic(null, key, "deprecated-key",
            "Configuration key '" + key + "' is deprecated"));
      }
    }
    ConfigurationProperties snapshot = new ConfigurationProperties(properties);
    RawIndex rawIndex = indexSchedulerProperties(properties);
    NodeDefaults defaults = nodeDefaults(properties, diagnostics);
    Map<QueuePath, QueueConfigNode> nodes = new LinkedHashMap<>();
    QueueConfigNode root = buildNode(ROOT_PATH, null,
        rawIndex.declaredChildren, snapshot, diagnostics, nodes, defaults);

    boolean legacyMode = parseBoolean(properties,
        CapacitySchedulerConfiguration.PREFIX + "legacy-queue-mode.enabled",
        CapacitySchedulerConfiguration.DEFAULT_LEGACY_QUEUE_MODE,
        diagnostics);
    String mappingFormat = properties.getOrDefault(
        CapacitySchedulerConfiguration.MAPPING_RULE_FORMAT,
        CapacitySchedulerConfiguration.MAPPING_RULE_FORMAT_DEFAULT);
    int maximumDepth = parseInt(properties,
        CapacitySchedulerConfiguration.PREFIX
            + CapacitySchedulerConfiguration.MAXIMUM_QUEUE_DEPTH,
        CapacitySchedulerConfiguration.DEFAULT_MAXIMUM_QUEUE_DEPTH,
        diagnostics);
    String resourceTypes = properties.getOrDefault(
        YarnConfiguration.RESOURCE_TYPES,
        CapacitySchedulerConfiguration.DEFAULT_RESOURCE_TYPES);
    return new CSConfigModel(root, nodes, legacyMode, mappingFormat,
        maximumDepth, resourceTypes, snapshot, properties,
        rawIndex.configuredLabelsByQueue, defaults, diagnostics);
  }

  private static final class RawIndex {
    private final Map<QueuePath, List<String>> declaredChildren;
    private final Map<String, Set<String>> configuredLabelsByQueue;

    private RawIndex(Map<QueuePath, List<String>> declaredChildren,
        Map<String, Set<String>> configuredLabelsByQueue) {
      this.declaredChildren = declaredChildren;
      this.configuredLabelsByQueue = configuredLabelsByQueue;
    }
  }

  /**
   * Extracts all facts that require looking at fully qualified scheduler keys.
   * Queue-local parsing below uses trie prefix queries and never scans the
   * complete property map again.
   */
  private static RawIndex indexSchedulerProperties(
      Map<String, String> properties) {
    Map<QueuePath, List<String>> declaredChildren = new LinkedHashMap<>();
    Map<String, Set<String>> configuredLabelsByQueue = new LinkedHashMap<>();
    String schedulerPrefix = CapacitySchedulerConfiguration.PREFIX;
    String queuesSuffix = "." + CapacitySchedulerConfiguration.QUEUES;
    String labelMarker = "."
        + CapacitySchedulerConfiguration.ACCESSIBLE_NODE_LABELS + ".";
    for (Map.Entry<String, String> entry : properties.entrySet()) {
      String key = entry.getKey();
      if (!key.startsWith(schedulerPrefix)) {
        continue;
      }
      if (key.endsWith(queuesSuffix)) {
        String path = key.substring(schedulerPrefix.length(),
            key.length() - queuesSuffix.length());
        if (path.equals(ROOT) || path.startsWith(ROOT + ".")) {
          declaredChildren.put(new QueuePath(path),
              splitCommaSeparated(entry.getValue()));
        }
      }
      int marker = key.indexOf(labelMarker, schedulerPrefix.length());
      if (marker < 0) {
        continue;
      }
      int labelStart = marker + labelMarker.length();
      int labelEnd = key.indexOf('.', labelStart);
      if (labelEnd < 0) {
        continue;
      }
      String queuePath = key.substring(schedulerPrefix.length(), marker);
      configuredLabelsByQueue.computeIfAbsent(queuePath,
          ignored -> new LinkedHashSet<>())
          .add(RMNodeLabelsManager.NO_LABEL);
      configuredLabelsByQueue.get(queuePath)
          .add(key.substring(labelStart, labelEnd));
    }
    declaredChildren.putIfAbsent(ROOT_PATH, Collections.emptyList());
    return new RawIndex(declaredChildren, configuredLabelsByQueue);
  }

  static QueueConfigNode buildEffectiveNode(QueuePath path,
      QueueConfigNode parent, Map<String, String> raw,
      Map<String, ConfigProvenance> provenance, NodeDefaults defaults) {
    return createNode(path, parent, raw, provenance, defaults,
        new ArrayList<>());
  }

  private static QueueConfigNode buildNode(QueuePath path,
      QueueConfigNode parent, Map<QueuePath, List<String>> declaredChildren,
      ConfigurationProperties snapshot, List<ConfigDiagnostic> diagnostics,
      Map<QueuePath, QueueConfigNode> nodes, NodeDefaults defaults) {
    List<String> childNames = declaredChildren.getOrDefault(path,
        Collections.emptyList());
    Map<String, String> raw = directProperties(path, childNames, snapshot);
    Map<String, ConfigProvenance> provenance = new LinkedHashMap<>();
    raw.keySet().forEach(key -> provenance.put(key, ConfigProvenance.USER));

    QueueConfigNode node = createNode(path, parent, raw, provenance, defaults,
        diagnostics);
    nodes.put(path, node);
    for (String childName : childNames) {
      QueueConfigNode child = buildNode(path.createNewLeaf(childName), node,
          declaredChildren, snapshot, diagnostics, nodes, defaults);
      node.addChild(child);
    }
    return node;
  }

  private static QueueConfigNode createNode(QueuePath path,
      QueueConfigNode parent, Map<String, String> raw,
      Map<String, ConfigProvenance> provenance, NodeDefaults defaults,
      List<ConfigDiagnostic> diagnostics) {
    Set<String> configuredLabels = configuredLabels(raw);
    return new QueueConfigNode(path, parent, raw, provenance,
        parseCapacities(path, raw, configuredLabels,
            CapacitySchedulerConfiguration.CAPACITY, diagnostics),
        parseCapacities(path, raw, configuredLabels,
            CapacitySchedulerConfiguration.MAXIMUM_CAPACITY, diagnostics),
        accessibleLabels(path, raw), configuredLabels,
        parseState(path, raw, diagnostics), raw.getOrDefault(
            CapacitySchedulerConfiguration.ORDERING_POLICY,
            CapacitySchedulerConfiguration.DEFAULT_APP_ORDERING_POLICY),
        propertiesWithPrefix(raw,
            CapacitySchedulerConfiguration.ORDERING_POLICY + "."),
        resolvedMaximumApplications(path, raw, defaults, diagnostics),
        parseFloat(path, raw,
            CapacitySchedulerConfiguration.MAXIMUM_AM_RESOURCE_SUFFIX,
            defaults.getMaximumApplicationMasterShare(), diagnostics),
        parseFloat(path, raw, CapacitySchedulerConfiguration.USER_LIMIT,
            defaults.getUserLimit(), diagnostics),
        parseFloat(path, raw, CapacitySchedulerConfiguration.USER_LIMIT_FACTOR,
            defaults.getUserLimitFactor(), diagnostics),
        trimmed(raw.get(
            CapacitySchedulerConfiguration.DEFAULT_NODE_LABEL_EXPRESSION)),
        parseBoolean(raw,
            CapacitySchedulerConfiguration.AUTO_CREATE_CHILD_QUEUE_ENABLED,
            false, path, diagnostics),
        parseBoolean(raw,
            CapacitySchedulerConfiguration.AUTO_QUEUE_CREATION_V2_ENABLED,
            false, path, diagnostics));
  }

  private static Map<String, String> directProperties(QueuePath path,
      List<String> childNames, ConfigurationProperties snapshot) {
    String prefix = QueuePrefixes.getQueuePrefix(path);
    Map<String, String> result = new LinkedHashMap<>(
        snapshot.getPropertiesWithPrefix(prefix));
    result.entrySet().removeIf(entry -> childNames.stream()
        .anyMatch(child -> entry.getKey().startsWith(child + ".")));
    return result;
  }

  private static Set<String> configuredLabels(Map<String, String> raw) {
    String prefix = CapacitySchedulerConfiguration.ACCESSIBLE_NODE_LABELS + ".";
    Set<String> labels = new LinkedHashSet<>();
    for (String suffix : raw.keySet()) {
      if (suffix.startsWith(prefix)) {
        String remainder = suffix.substring(prefix.length());
        int delimiter = remainder.indexOf('.');
        if (delimiter > 0) {
          labels.add(remainder.substring(0, delimiter));
        }
      }
    }
    return labels;
  }

  private static Map<String, QueueConfigNode.CapacityValue> parseCapacities(
      QueuePath path, Map<String, String> raw, Set<String> configuredLabels,
      String capacitySuffix, List<ConfigDiagnostic> diagnostics) {
    Map<String, QueueConfigNode.CapacityValue> result = new LinkedHashMap<>();
    addCapacity(path, "", capacitySuffix, raw.get(capacitySuffix), result,
        diagnostics);
    for (String label : configuredLabels) {
      String suffix = CapacitySchedulerConfiguration.ACCESSIBLE_NODE_LABELS
          + "." + label + "." + capacitySuffix;
      addCapacity(path, label, suffix, raw.get(suffix), result, diagnostics);
    }
    return result;
  }

  private static void addCapacity(QueuePath path, String label,
      String propertySuffix, String rawValue,
      Map<String, QueueConfigNode.CapacityValue> result,
      List<ConfigDiagnostic> diagnostics) {
    QueueCapacityVector vector = CapacitySchedulerConfiguration
        .getQueueCapacityConfigParser().parse(rawValue, path);
    if (vector.isEmpty() && rawValue != null) {
      try {
        vector = QueueCapacityVector.of(Double.parseDouble(rawValue),
            ResourceUnitCapacityType.PERCENTAGE);
      } catch (NumberFormatException ignored) {
        // The diagnostic below reports the invalid value.
      }
    }
    result.put(label, new QueueConfigNode.CapacityValue(rawValue, vector));
    boolean legacyMaximumSentinel = propertySuffix.endsWith(
        CapacitySchedulerConfiguration.MAXIMUM_CAPACITY)
        && "-1".equals(rawValue);
    if (rawValue != null && !path.isRoot() && vector.isEmpty()
        && !legacyMaximumSentinel) {
      diagnostics.add(new ConfigDiagnostic(path,
          QueuePrefixes.getQueuePrefix(path) + propertySuffix,
          "invalid-capacity", "Invalid capacity value '" + rawValue + "'"));
    }
  }

  private static Set<String> accessibleLabels(QueuePath path,
      Map<String, String> raw) {
    if (path.isRoot()) {
      return Set.of(RMNodeLabelsManager.ANY);
    }
    String configured = raw.get(
        CapacitySchedulerConfiguration.ACCESSIBLE_NODE_LABELS);
    if (configured == null) {
      return null;
    }
    Set<String> labels = new HashSet<>(splitCommaSeparated(configured));
    if (labels.contains(RMNodeLabelsManager.ANY)) {
      return Set.of(RMNodeLabelsManager.ANY);
    }
    return Collections.unmodifiableSet(labels);
  }

  private static QueueState parseState(QueuePath path,
      Map<String, String> raw, List<ConfigDiagnostic> diagnostics) {
    String configured = raw.get(CapacitySchedulerConfiguration.STATE);
    if (configured == null) {
      return null;
    }
    try {
      return QueueState.valueOf(configured.toUpperCase(Locale.ROOT));
    } catch (IllegalArgumentException e) {
      diagnostics.add(new ConfigDiagnostic(path,
          QueuePrefixes.getQueuePrefix(path)
              + CapacitySchedulerConfiguration.STATE,
          "invalid-queue-state", "Invalid queue state '" + configured + "'"));
      return null;
    }
  }

  private static Map<String, String> propertiesWithPrefix(
      Map<String, String> properties, String prefix) {
    Map<String, String> result = new LinkedHashMap<>();
    properties.forEach((key, value) -> {
      if (key.startsWith(prefix)) {
        result.put(key.substring(prefix.length()), value);
      }
    });
    return result;
  }

  private static List<String> splitCommaSeparated(String value) {
    if (value == null || value.trim().isEmpty()) {
      return Collections.emptyList();
    }
    List<String> values = new ArrayList<>();
    for (String item : value.split(",")) {
      if (!item.trim().isEmpty()) {
        values.add(item.trim());
      }
    }
    return values;
  }

  private static String trimmed(String value) {
    return value == null ? null : value.trim();
  }

  private static boolean parseBoolean(Map<String, String> properties,
      String key, boolean defaultValue,
      List<ConfigDiagnostic> diagnostics) {
    String value = properties.get(key);
    if (value == null || value.isEmpty()) {
      return defaultValue;
    }
    if ("true".equalsIgnoreCase(value) || "false".equalsIgnoreCase(value)) {
      return Boolean.parseBoolean(value);
    }
    diagnostics.add(new ConfigDiagnostic(null, key, "invalid-boolean",
        "Invalid boolean value '" + value + "'"));
    return defaultValue;
  }

  private static boolean parseBoolean(Map<String, String> raw, String suffix,
      boolean defaultValue, QueuePath path,
      List<ConfigDiagnostic> diagnostics) {
    String value = raw.get(suffix);
    if (value == null || value.isEmpty()) {
      return defaultValue;
    }
    if ("true".equalsIgnoreCase(value) || "false".equalsIgnoreCase(value)) {
      return Boolean.parseBoolean(value);
    }
    diagnostics.add(new ConfigDiagnostic(path,
        QueuePrefixes.getQueuePrefix(path) + suffix, "invalid-boolean",
        "Invalid boolean value '" + value + "'"));
    return defaultValue;
  }

  private static float parseFloat(QueuePath path, Map<String, String> raw,
      String suffix, float defaultValue,
      List<ConfigDiagnostic> diagnostics) {
    String value = raw.get(suffix);
    if (value == null) {
      return defaultValue;
    }
    try {
      return Float.parseFloat(value);
    } catch (NumberFormatException e) {
      diagnostics.add(new ConfigDiagnostic(path,
          QueuePrefixes.getQueuePrefix(path) + suffix, "invalid-float",
          "Invalid floating-point value '" + value + "'"));
      return defaultValue;
    }
  }

  private static int resolvedMaximumApplications(QueuePath path,
      Map<String, String> raw, NodeDefaults defaults,
      List<ConfigDiagnostic> diagnostics) {
    int fallback = defaults.getGlobalMaximumApplications() > 0
        ? defaults.getGlobalMaximumApplications()
        : (int) CapacitySchedulerConfiguration.UNDEFINED;
    String value = raw.get(
        CapacitySchedulerConfiguration.MAXIMUM_APPLICATIONS_SUFFIX);
    if (value == null) {
      return fallback;
    }
    try {
      int configured = Integer.parseInt(value);
      return configured < 0 ? fallback : configured;
    } catch (NumberFormatException e) {
      diagnostics.add(new ConfigDiagnostic(path,
          QueuePrefixes.getQueuePrefix(path)
              + CapacitySchedulerConfiguration.MAXIMUM_APPLICATIONS_SUFFIX,
          "invalid-integer", "Invalid integer value '" + value + "'"));
      return fallback;
    }
  }

  private static NodeDefaults nodeDefaults(Map<String, String> properties,
      List<ConfigDiagnostic> diagnostics) {
    float userLimit = parseGlobalFloat(properties,
        CapacitySchedulerConfiguration.PREFIX
            + CapacitySchedulerConfiguration.USER_LIMIT,
        CapacitySchedulerConfiguration.DEFAULT_USER_LIMIT, diagnostics);
    float userLimitFactor = parseGlobalFloat(properties,
        CapacitySchedulerConfiguration.PREFIX
            + CapacitySchedulerConfiguration.USER_LIMIT_FACTOR,
        CapacitySchedulerConfiguration.DEFAULT_USER_LIMIT_FACTOR, diagnostics);
    float amShare = parseGlobalFloat(properties,
        CapacitySchedulerConfiguration.MAXIMUM_APPLICATION_MASTERS_RESOURCE_PERCENT,
        CapacitySchedulerConfiguration
            .DEFAULT_MAXIMUM_APPLICATIONMASTERS_RESOURCE_PERCENT,
        diagnostics);
    int globalMaximumApplications = parseInt(properties,
        CapacitySchedulerConfiguration.QUEUE_GLOBAL_MAX_APPLICATION,
        (int) CapacitySchedulerConfiguration.UNDEFINED, diagnostics);
    return new NodeDefaults(userLimit, userLimitFactor, amShare,
        globalMaximumApplications);
  }

  private static float parseGlobalFloat(Map<String, String> properties,
      String key, float defaultValue, List<ConfigDiagnostic> diagnostics) {
    String value = properties.get(key);
    if (value == null) {
      return defaultValue;
    }
    try {
      return Float.parseFloat(value);
    } catch (NumberFormatException e) {
      diagnostics.add(new ConfigDiagnostic(null, key, "invalid-float",
          "Invalid floating-point value '" + value + "'"));
      return defaultValue;
    }
  }

  private static int parseInt(Map<String, String> properties, String key,
      int defaultValue, List<ConfigDiagnostic> diagnostics) {
    String value = properties.get(key);
    if (value == null) {
      return defaultValue;
    }
    try {
      return Integer.parseInt(value);
    } catch (NumberFormatException e) {
      diagnostics.add(new ConfigDiagnostic(null, key, "invalid-integer",
          "Invalid integer value '" + value + "'"));
      return defaultValue;
    }
  }
}
