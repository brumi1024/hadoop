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

import java.util.Objects;

import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueuePath;

/** One stable, structured Capacity Scheduler validation finding. */
public final class ValidationIssue {
  public enum Severity { ERROR, WARNING }

  private final QueuePath queuePath;
  private final String propertyKey;
  private final String ruleId;
  private final Severity severity;
  private final String message;

  public ValidationIssue(QueuePath queuePath, String propertyKey,
      String ruleId, Severity severity, String message) {
    this.queuePath = queuePath;
    this.propertyKey = propertyKey;
    this.ruleId = Objects.requireNonNull(ruleId);
    this.severity = Objects.requireNonNull(severity);
    this.message = Objects.requireNonNull(message);
  }

  public QueuePath getQueuePath() {
    return queuePath;
  }
  public String getPropertyKey() {
    return propertyKey;
  }
  public String getRuleId() {
    return ruleId;
  }
  public Severity getSeverity() {
    return severity;
  }
  public String getMessage() {
    return message;
  }

  @Override
  public String toString() {
    return severity + " " + ruleId + ": " + message;
  }
}
