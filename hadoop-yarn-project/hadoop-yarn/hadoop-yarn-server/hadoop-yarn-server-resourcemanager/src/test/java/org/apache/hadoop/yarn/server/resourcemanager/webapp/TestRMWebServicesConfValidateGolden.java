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

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.jsonprovider.ExcludeRootJSONProvider;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.jsonprovider.IncludeRootJSONProvider;
import org.apache.hadoop.yarn.webapp.dao.QueueConfigInfo;
import org.apache.hadoop.yarn.webapp.dao.SchedConfUpdateInfo;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Captures the frozen response bodies of the legacy validation endpoint. */
public class TestRMWebServicesConfValidateGolden
    extends AbstractRMWebServicesConfigurationTest {
  private static final String LEGACY_INVALID_CAPACITY_RESPONSE =
      "CapacityScheduler configuration validation failed:java.io.IOException: "
          + "root.a: Invalid capacity value 'not-a-capacity'";

  @Test
  public void testValidBodyGolden() throws Exception {
    Response response = validate(new SchedConfUpdateInfo());
    assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
    assertEquals(readGolden("conf-validate-valid.json"),
        response.readEntity(String.class));
  }

  @Test
  public void testInvalidBodyGolden() throws Exception {
    SchedConfUpdateInfo mutation = new SchedConfUpdateInfo();
    Map<String, String> properties = new HashMap<>();
    properties.put(CapacitySchedulerConfiguration.CAPACITY, "not-a-capacity");
    mutation.getUpdateQueueInfo().add(
        new QueueConfigInfo("root.a", properties));
    Response response = validate(mutation);
    assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), response.getStatus());
    String responseBody = response.readEntity(String.class);
    assertTrue(responseBody.contains(LEGACY_INVALID_CAPACITY_RESPONSE),
        responseBody);
    assertEquals(readGolden("conf-validate-invalid.txt"), responseBody);
  }

  private String readGolden(String name) throws IOException {
    try (InputStream stream = getClass().getResourceAsStream(
        "/webapp/" + name)) {
      assertNotNull(stream, name);
      String golden = new String(stream.readAllBytes(), StandardCharsets.UTF_8)
          .trim();
      return golden;
    }
  }

  private Response validate(SchedConfUpdateInfo mutation) throws Exception {
    WebTarget endpoint = target()
        .register(new IncludeRootJSONProvider())
        .register(new ExcludeRootJSONProvider())
        .path("ws").path("v1").path("cluster")
        .path(RMWSConsts.SCHEDULER_CONF_VALIDATE)
        .queryParam("user.name",
            UserGroupInformation.getCurrentUser().getShortUserName());
    return endpoint.request(MediaType.APPLICATION_JSON)
        .post(Entity.entity(mutation, MediaType.APPLICATION_JSON), Response.class);
  }
}
