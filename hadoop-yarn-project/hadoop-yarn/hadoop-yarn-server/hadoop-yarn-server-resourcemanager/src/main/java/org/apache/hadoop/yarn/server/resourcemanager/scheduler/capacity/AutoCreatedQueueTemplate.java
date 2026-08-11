/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.conf.model.CSConfigModel;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration.AUTO_QUEUE_CREATION_V2_PREFIX;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueuePrefixes.getQueuePrefix;

/**
 * A handler for storing and setting auto created queue template settings.
 */
public class AutoCreatedQueueTemplate {
  public static final String AUTO_QUEUE_TEMPLATE_PREFIX =
      AUTO_QUEUE_CREATION_V2_PREFIX + "template.";
  public static final String AUTO_QUEUE_LEAF_TEMPLATE_PREFIX =
      AUTO_QUEUE_CREATION_V2_PREFIX + "leaf-template.";
  public static final String AUTO_QUEUE_PARENT_TEMPLATE_PREFIX =
      AUTO_QUEUE_CREATION_V2_PREFIX + "parent-template.";

  public static final String WILDCARD_QUEUE = "*";

  private final Map<String, String> templateProperties;
  private final Map<String, String> leafOnlyProperties;
  private final Map<String, String> parentOnlyProperties;

  public AutoCreatedQueueTemplate(
      CapacitySchedulerConfiguration configuration, QueuePath queuePath) {
    this(configuration.getModel(), queuePath);
  }

  public AutoCreatedQueueTemplate(CSConfigModel model, QueuePath queuePath) {
    this(model.getCommonTemplateProperties(queuePath),
        model.getLeafTemplateProperties(queuePath),
        model.getParentTemplateProperties(queuePath));
  }

  public AutoCreatedQueueTemplate(Map<String, String> templateProperties,
      Map<String, String> leafOnlyProperties,
      Map<String, String> parentOnlyProperties) {
    this.templateProperties = immutableCopy(templateProperties);
    this.leafOnlyProperties = immutableCopy(leafOnlyProperties);
    this.parentOnlyProperties = immutableCopy(parentOnlyProperties);
  }

  @VisibleForTesting
  public static String getAutoQueueTemplatePrefix(QueuePath queuePath) {
    return getQueuePrefix(queuePath) + AUTO_QUEUE_TEMPLATE_PREFIX;
  }

  /**
   * Get the common template properties specified for a parent queue.
   * @return template property names and values
   */
  public Map<String, String> getTemplateProperties() {
    return templateProperties;
  }

  /**
   * Get the leaf specific template properties specified for a parent queue.
   * @return template property names and values
   */
  public Map<String, String> getLeafOnlyProperties() {
    return leafOnlyProperties;
  }

  /**
   * Get the parent specific template properties specified for a parent queue.
   * @return template property names and values
   */
  public Map<String, String> getParentOnlyProperties() {
    return parentOnlyProperties;
  }

  /**
   * Sets common and parent-specific template properties for a child queue.
   * @param conf configuration to set
   * @param childQueuePath child queue path used for property prefixes
   */
  public void setTemplateEntriesForChild(CapacitySchedulerConfiguration conf,
      QueuePath childQueuePath) {
    setTemplateEntriesForChild(conf, childQueuePath, false);
  }

  /**
   * Sets common and queue-type-specific template properties for a child queue.
   * @param conf configuration to set
   * @param childQueuePath child queue path used for property prefixes
   * @param isLeaf whether leaf-specific rather than parent-specific properties
   *     should be applied
   */
  public void setTemplateEntriesForChild(CapacitySchedulerConfiguration conf,
      QueuePath childQueuePath, boolean isLeaf) {
    if (childQueuePath.isRoot()) {
      return;
    }

    Set<String> alreadySetProps = conf.getConfigurationProperties()
        .getPropertiesWithPrefix(getQueuePrefix(childQueuePath)).keySet();
    Map<String, String> typeSpecificProperties = isLeaf
        ? leafOnlyProperties : parentOnlyProperties;

    typeSpecificProperties.forEach((key, value) -> {
      if (!alreadySetProps.contains(key)) {
        conf.set(getQueuePrefix(childQueuePath) + key, value);
      }
    });
    templateProperties.forEach((key, value) -> {
      if (!alreadySetProps.contains(key)
          && !typeSpecificProperties.containsKey(key)) {
        conf.set(getQueuePrefix(childQueuePath) + key, value);
      }
    });
  }

  private static Map<String, String> immutableCopy(
      Map<String, String> properties) {
    return Collections.unmodifiableMap(new LinkedHashMap<>(properties));
  }
}
