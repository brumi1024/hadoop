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
package org.apache.hadoop.yarn.server.resourcemanager.webapp.dao;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;

import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.ValidationIssue;

/** A structured Capacity Scheduler configuration validation issue. */
@XmlAccessorType(XmlAccessType.FIELD)
public class ValidationIssueInfo {
  private String queuePath;
  private String propertyKey;
  private String ruleId;
  private String severity;
  private String message;

  public ValidationIssueInfo() {
  }

  ValidationIssueInfo(ValidationIssue issue) {
    queuePath = issue.getQueuePath() == null
        ? null : issue.getQueuePath().getFullPath();
    propertyKey = issue.getPropertyKey();
    ruleId = issue.getRuleId();
    severity = issue.getSeverity().name();
    message = issue.getMessage();
  }

  public String getQueuePath() {
    return queuePath;
  }

  public String getPropertyKey() {
    return propertyKey;
  }

  public String getRuleId() {
    return ruleId;
  }

  public String getSeverity() {
    return severity;
  }

  public String getMessage() {
    return message;
  }
}
