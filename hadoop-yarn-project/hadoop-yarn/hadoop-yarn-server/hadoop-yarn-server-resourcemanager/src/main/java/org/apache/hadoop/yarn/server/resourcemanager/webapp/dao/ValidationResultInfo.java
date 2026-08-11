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

import java.util.ArrayList;
import java.util.List;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlElementWrapper;
import javax.xml.bind.annotation.XmlRootElement;

import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.ValidationResult;

/** Structured result returned by the v2 scheduler validation endpoint. */
@XmlRootElement(name = "validationResult")
@XmlAccessorType(XmlAccessType.FIELD)
public class ValidationResultInfo {
  private boolean valid;
  private long configVersion;
  @XmlElementWrapper(name = "issues")
  @XmlElement(name = "issue")
  private List<ValidationIssueInfo> issues = new ArrayList<>();

  public ValidationResultInfo() {
  }

  public static ValidationResultInfo from(ValidationResult result,
      long configVersion) {
    ValidationResultInfo info = new ValidationResultInfo();
    info.valid = result.isValid();
    info.configVersion = configVersion;
    result.getIssues().forEach(issue ->
        info.issues.add(new ValidationIssueInfo(issue)));
    return info;
  }

  public boolean isValid() {
    return valid;
  }

  public long getConfigVersion() {
    return configVersion;
  }

  public List<ValidationIssueInfo> getIssues() {
    return issues;
  }
}
