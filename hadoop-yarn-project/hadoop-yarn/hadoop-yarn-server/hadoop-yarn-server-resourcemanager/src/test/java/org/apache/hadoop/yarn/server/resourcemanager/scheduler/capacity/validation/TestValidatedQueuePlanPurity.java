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

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.GenericArrayType;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.RecordComponent;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.lang.reflect.WildcardType;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.protocolrecords.ResourceTypes;
import org.apache.hadoop.yarn.api.records.ResourceInformation;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueuePath;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.conf.model.CSConfigModel;
import org.apache.hadoop.yarn.util.resource.ResourceUtils;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Structural and runtime purity invariants for validated queue plans. */
public class TestValidatedQueuePlanPurity {
  private static final QueuePath ROOT = new QueuePath("root");
  private static final QueuePath A = new QueuePath("root.a");
  private static final Set<Class<?>> ALLOWED_EXTERNAL_VALUE_TYPES = Set.of(
      Object.class, Record.class, Enum.class, String.class, Boolean.class,
      Byte.class, Short.class, Integer.class, Long.class, Float.class,
      Double.class, Character.class, Map.class, Set.class,
      java.util.List.class, org.apache.hadoop.yarn.api.records.QueueState.class);
  private final CSConfigValidationEngine engine =
      new CSConfigValidationEngine();

  @Test
  public void testPlanTypeGraphContainsOnlyClosedValueTypes() {
    scanType(ValidatedQueuePlan.class, new HashSet<>());
  }

  @Test
  public void testPlanCollectionsAreTransitivelyImmutable() {
    ValidatedQueuePlan plan = compile(percentageTree(), ClusterFacts.empty());

    assertThrows(UnsupportedOperationException.class,
        () -> plan.getQueues().clear());
    assertThrows(UnsupportedOperationException.class,
        () -> plan.getRoot().childPaths().add("root.b"));
    assertThrows(UnsupportedOperationException.class,
        () -> plan.getRoot().capacities().clear());
    assertThrows(UnsupportedOperationException.class,
        () -> plan.getFacts().resourceNames().add("vendor/gpu"));
    assertThrows(UnsupportedOperationException.class,
        () -> plan.getFacts().resourcesByLabel().clear());
    assertThrows(UnsupportedOperationException.class,
        () -> plan.getRoot().settings().userWeights().clear());
  }

  @Test
  public void testStaticResourceRegistryCannotChangeCapturedCompilation() {
    CapacitySchedulerConfiguration conf = percentageTree();
    CSConfigModel model = conf.getModel();
    ClusterFacts facts = ClusterFacts.empty();
    CompileResult before = engine.compile(model, facts);
    Map<String, ResourceInformation> original = copyResourceTypes(
        ResourceUtils.getResourceTypes());
    Map<String, ResourceInformation> changed = copyResourceTypes(original);
    changed.put("vendor/gpu", ResourceInformation.newInstance("vendor/gpu",
        "", 0L, ResourceTypes.COUNTABLE, 0L, 100L));

    try {
      ResourceUtils.initializeResourcesFromResourceInformationMap(changed);
      CompileResult after = engine.compile(model, facts);

      assertTrue(after.isCompiledActivationEligible(),
          after.getFallbackReasons() + " " + after.getIssues());
      assertEquals(before.getIssues(), after.getIssues());
      assertEquals(before.getPlan().getIdentity(),
          after.getPlan().getIdentity());
      assertEquals(before.getPlan().getQueues(), after.getPlan().getQueues());
    } finally {
      ResourceUtils.initializeResourcesFromResourceInformationMap(original);
    }
  }

  @Test
  public void testModelFallbackFactsResolveInheritedStates() {
    CapacitySchedulerConfiguration conf = percentageTree();
    CapacityScheduler scheduler = Mockito.mock(CapacityScheduler.class);

    ClusterFacts facts = ClusterFacts.capture(scheduler, conf.getModel());
    CompileResult result = engine.compile(conf.getModel(), facts);

    assertTrue(result.isCompiledActivationEligible(),
        result.getFallbackReasons() + " " + result.getIssues());
    assertNotNull(result.getPlan());
    result.getPlan().getFacts().oldHierarchy().values().forEach(old ->
        assertNotNull(old.state()));
  }

  private ValidatedQueuePlan compile(CapacitySchedulerConfiguration conf,
      ClusterFacts facts) {
    CompileResult result = engine.compile(conf.getModel(), facts);
    assertTrue(result.isCompiledActivationEligible(),
        result.getFallbackReasons() + " " + result.getIssues());
    return result.getPlan();
  }

  private void scanType(Type type, Set<Type> visited) {
    if (type == null || !visited.add(type)) {
      return;
    }
    if (type instanceof ParameterizedType parameterized) {
      scanType(parameterized.getRawType(), visited);
      scanType(parameterized.getOwnerType(), visited);
      for (Type argument : parameterized.getActualTypeArguments()) {
        scanType(argument, visited);
      }
      return;
    }
    if (type instanceof GenericArrayType array) {
      scanType(array.getGenericComponentType(), visited);
      return;
    }
    if (type instanceof WildcardType wildcard) {
      for (Type bound : wildcard.getUpperBounds()) {
        scanType(bound, visited);
      }
      for (Type bound : wildcard.getLowerBounds()) {
        scanType(bound, visited);
      }
      return;
    }
    if (type instanceof TypeVariable<?> variable) {
      for (Type bound : variable.getBounds()) {
        scanType(bound, visited);
      }
      return;
    }
    if (!(type instanceof Class<?> current)) {
      return;
    }
    if (current.isArray()) {
      scanType(current.getComponentType(), visited);
      return;
    }
    assertTrue(isPlanOwned(current) || current.isPrimitive()
            || ALLOWED_EXTERNAL_VALUE_TYPES.contains(current),
        () -> "Non-value plan type: " + current.getName());
    if (!isPlanOwned(current)) {
      return;
    }
    scanType(current.getGenericSuperclass(), visited);
    for (Type implemented : current.getGenericInterfaces()) {
      scanType(implemented, visited);
    }
    for (TypeVariable<?> variable : current.getTypeParameters()) {
      scanType(variable, visited);
    }
    for (Field field : current.getDeclaredFields()) {
      scanType(field.getGenericType(), visited);
    }
    for (RecordComponent component : current.getRecordComponents() == null
        ? new RecordComponent[0] : current.getRecordComponents()) {
      scanType(component.getGenericType(), visited);
    }
    for (Constructor<?> constructor : current.getDeclaredConstructors()) {
      for (TypeVariable<?> variable : constructor.getTypeParameters()) {
        scanType(variable, visited);
      }
      for (Type parameter : constructor.getGenericParameterTypes()) {
        scanType(parameter, visited);
      }
      for (Type exception : constructor.getGenericExceptionTypes()) {
        scanType(exception, visited);
      }
    }
    for (Method method : current.getDeclaredMethods()) {
      for (TypeVariable<?> variable : method.getTypeParameters()) {
        scanType(variable, visited);
      }
      scanType(method.getGenericReturnType(), visited);
      for (Type parameter : method.getGenericParameterTypes()) {
        scanType(parameter, visited);
      }
      for (Type exception : method.getGenericExceptionTypes()) {
        scanType(exception, visited);
      }
    }
    for (Class<?> nested : current.getDeclaredClasses()) {
      scanType(nested, visited);
    }
  }

  private boolean isPlanOwned(Class<?> candidate) {
    return candidate == ValidatedQueuePlan.class
        || candidate.getEnclosingClass() == ValidatedQueuePlan.class;
  }

  private Map<String, ResourceInformation> copyResourceTypes(
      Map<String, ResourceInformation> source) {
    Map<String, ResourceInformation> result = new LinkedHashMap<>();
    source.forEach((name, information) -> result.put(name,
        ResourceInformation.newInstance(information)));
    return result;
  }

  private CapacitySchedulerConfiguration percentageTree() {
    CapacitySchedulerConfiguration conf = new CapacitySchedulerConfiguration(
        new Configuration(false), false);
    conf.setQueues(ROOT, new String[] {"a"});
    conf.setCapacity(A, 100F);
    return conf;
  }
}
