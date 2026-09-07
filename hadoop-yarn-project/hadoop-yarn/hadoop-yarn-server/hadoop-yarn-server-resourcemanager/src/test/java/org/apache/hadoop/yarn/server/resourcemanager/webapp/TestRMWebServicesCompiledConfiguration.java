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


package org.apache.hadoop.yarn.server.resourcemanager.webapp;

import java.util.Map;

import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.TestCompiledCSConfigurationMutation.ThrowingOrderingPolicy;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.conf.MutableCSConfigurationProvider;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.CSConfigValidationEngine;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.ClusterFacts;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.CompileResult;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.jsonprovider.ExcludeRootJSONProvider;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.jsonprovider.IncludeRootJSONProvider;
import org.apache.hadoop.yarn.webapp.dao.QueueConfigInfo;
import org.apache.hadoop.yarn.webapp.dao.SchedConfUpdateInfo;
import org.codehaus.jettison.json.JSONObject;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** HTTP contracts for the eligible, built-in compiled mutation profile. */
public class TestRMWebServicesCompiledConfiguration
    extends AbstractRMWebServicesConfigurationTest {
  @Override
  protected void configureSchedulerQueues(CapacitySchedulerConfiguration config) {
    config.setQueues(ROOT, new String[]{"a", "b"});
    config.setCapacity(ROOT_A, 50);
    config.setCapacity(ROOT_B, 50);
  }

  @Test
  public void testValidationJsonAndXmlLeavesLiveConfigurationUnchanged() throws Exception {
    establishEligibleConfiguration();
    long version = provider().getConfigVersion();
    SchedConfUpdateInfo update = capacities(40, 60);
    try (Response legacy = endpoint(RMWSConsts.SCHEDULER_CONF_VALIDATE)
        .request(MediaType.APPLICATION_JSON)
        .post(Entity.entity(update, MediaType.APPLICATION_JSON))) {
      assertEquals(200, legacy.getStatus());
    }
    try (Response json = endpoint(RMWSConsts.SCHEDULER_CONF_VALIDATE_V2)
        .request(MediaType.APPLICATION_JSON)
        .post(Entity.entity(update, MediaType.APPLICATION_JSON))) {
      assertEquals(200, json.getStatus());
      JSONObject result = validation(json);
      assertTrue(result.getBoolean("valid"));
      assertEquals(version, result.getLong("configVersion"));
    }
    try (Response xml = endpoint(RMWSConsts.SCHEDULER_CONF_VALIDATE_V2)
        .request(MediaType.APPLICATION_XML)
        .post(Entity.entity(update, MediaType.APPLICATION_XML))) {
      assertEquals(200, xml.getStatus());
      assertTrue(xml.readEntity(String.class).contains("<valid>true</valid>"));
    }
    assertEquals(version, provider().getConfigVersion());
    assertEquals(0.5F, scheduler().getQueue("root.a").getCapacity());
  }

  @Test
  public void testPutPublishesVersionAndConfiguration() throws Exception {
    establishEligibleConfiguration();
    long version = provider().getConfigVersion();
    try (Response put = endpoint(RMWSConsts.SCHEDULER_CONF)
        .request(MediaType.APPLICATION_JSON)
        .put(Entity.entity(capacities(40, 60), MediaType.APPLICATION_JSON))) {
      assertEquals(200, put.getStatus());
      JSONObject result = validation(put);
      assertTrue(result.getBoolean("valid"));
      assertEquals(version + 1, result.getLong("configVersion"));
    }
    try (Response get = endpoint(RMWSConsts.SCHEDULER_CONF)
        .request(MediaType.APPLICATION_JSON).get()) {
      assertEquals(200, get.getStatus());
      String configuration = get.readEntity(String.class);
      assertTrue(configuration.contains("root.a.capacity"), configuration);
    }
    try (Response get = endpoint(RMWSConsts.SCHEDULER_CONF_VERSION)
        .request(MediaType.APPLICATION_JSON).get()) {
      assertEquals(200, get.getStatus());
      assertTrue(get.readEntity(String.class).contains(
          Long.toString(version + 1)));
    }
    assertEquals(version + 1, provider().getConfigVersion());
    assertEquals(0.4F, scheduler().getQueue("root.a").getCapacity());
  }

  @Test
  public void testRejectedValidationAndPutPreserveVersionAndQueue() throws Exception {
    establishEligibleConfiguration();
    long version = provider().getConfigVersion();
    SchedConfUpdateInfo update = capacities(40, 50);
    try (Response post = endpoint(RMWSConsts.SCHEDULER_CONF_VALIDATE_V2)
        .request(MediaType.APPLICATION_JSON)
        .post(Entity.entity(update, MediaType.APPLICATION_JSON));
         Response put = endpoint(RMWSConsts.SCHEDULER_CONF)
            .request(MediaType.APPLICATION_JSON)
            .put(Entity.entity(update, MediaType.APPLICATION_JSON))) {
      for (Response response : new Response[]{post, put}) {
        assertEquals(400, response.getStatus());
        JSONObject result = validation(response);
        assertFalse(result.getBoolean("valid"));
        assertEquals(version, result.getLong("configVersion"));
        assertTrue(result.getJSONObject("issues").getJSONArray("issue")
            .toString().contains("children-capacity-sum"));
      }
    }
    assertEquals(version, provider().getConfigVersion());
    assertEquals(0.5F, scheduler().getQueue("root.a").getCapacity());
  }

  @Test
  public void testCustomPolicyFallsBackAndPreservesRejectionContract() throws Exception {
    establishEligibleConfiguration();
    long version = provider().getConfigVersion();
    SchedConfUpdateInfo update = new SchedConfUpdateInfo();
    update.getUpdateQueueInfo().add(new QueueConfigInfo("root.a",
        Map.of("ordering-policy", ThrowingOrderingPolicy.class.getName())));
    try (Response post = endpoint(RMWSConsts.SCHEDULER_CONF_VALIDATE_V2)
        .request(MediaType.APPLICATION_JSON)
        .post(Entity.entity(update, MediaType.APPLICATION_JSON));
         Response put = endpoint(RMWSConsts.SCHEDULER_CONF)
            .request(MediaType.APPLICATION_JSON)
            .put(Entity.entity(update, MediaType.APPLICATION_JSON))) {
      for (Response response : new Response[]{post, put}) {
        assertEquals(400, response.getStatus());
        JSONObject result = validation(response);
        assertFalse(result.getBoolean("valid"));
        assertEquals(version, result.getLong("configVersion"));
        assertTrue(result.toString().contains("custom configure rejected"));
      }
    }
    assertEquals(version, provider().getConfigVersion());
    assertEquals(0.5F, scheduler().getQueue("root.a").getCapacity());
  }

  private void establishEligibleConfiguration() throws Exception {
    // Initial scheduler defaults have unmodeled substitutions. The first
    // mutation remains legacy and activates this fixture's narrow store model.
    // Verify the real transition, without injecting a seeded provider.
    assertTrue(compileCurrent().requiresLegacyValidation());
    try (Response put = endpoint(RMWSConsts.SCHEDULER_CONF)
        .request(MediaType.APPLICATION_JSON)
        .put(Entity.entity(new SchedConfUpdateInfo(), MediaType.APPLICATION_JSON))) {
      assertEquals(200, put.getStatus());
    }
    CompileResult result = compileCurrent();
    assertTrue(result.isCompiledActivationEligible(),
        () -> result.getFallbackReasons() + " " + result.getIssues());
  }

  private CompileResult compileCurrent() throws Exception {
    return provider().runUnderMutationLock(() ->
        scheduler().runWithStableQueueConfiguration(() ->
            new CSConfigValidationEngine().compile(
                new CapacitySchedulerConfiguration(provider().getConfiguration(),
                    false).getModel(), ClusterFacts.capture(scheduler()))));
  }

  private CapacityScheduler scheduler() {
    return (CapacityScheduler) rm.getResourceScheduler();
  }

  private MutableCSConfigurationProvider provider() {
    return (MutableCSConfigurationProvider) scheduler().getCsConfProvider();
  }

  private WebTarget endpoint(String path) {
    return target().register(new IncludeRootJSONProvider())
        .register(new ExcludeRootJSONProvider())
        .path("ws").path("v1").path("cluster").path(path)
        .queryParam("user.name", userName);
  }

  private JSONObject validation(Response response) throws Exception {
    return new JSONObject(response.readEntity(String.class))
        .getJSONObject("validationResult");
  }

  private static SchedConfUpdateInfo capacities(int a, int b) {
    SchedConfUpdateInfo update = new SchedConfUpdateInfo();
    update.getUpdateQueueInfo().add(new QueueConfigInfo("root.a",
        Map.of("capacity", Integer.toString(a))));
    update.getUpdateQueueInfo().add(new QueueConfigInfo("root.b",
        Map.of("capacity", Integer.toString(b))));
    return update;
  }
}
