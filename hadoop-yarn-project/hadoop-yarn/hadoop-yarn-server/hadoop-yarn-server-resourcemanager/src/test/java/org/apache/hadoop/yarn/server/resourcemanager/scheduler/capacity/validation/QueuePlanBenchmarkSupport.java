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

import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.conf.model.CSConfigModel;

/** Test-only opaque access to independently timed compilation phases. */
public final class QueuePlanBenchmarkSupport {
  private QueuePlanBenchmarkSupport() {
  }

  /** Holds an unexposed provisional plan between benchmark phases. */
  public static final class Session {
    private final ValidatedQueuePlan provisionalPlan;

    private Session(ValidatedQueuePlan provisionalPlan) {
      this.provisionalPlan = provisionalPlan;
    }
  }

  public static Session compile(CSConfigModel model, ClusterFacts facts) {
    CSConfigCompatibilityClassifier.Classification classification =
        new CSConfigCompatibilityClassifier().classify(model, facts);
    if (!classification.isEligible()) {
      throw new IllegalStateException("benchmark profile selected legacy "
          + "validation: " + classification.reasons());
    }
    return new Session(QueuePlanCompiler.compile(model, facts));
  }

  public static ValidationResult validate(Session session) {
    return new CompiledQueuePlanValidator().validate(
        session.provisionalPlan);
  }
}
