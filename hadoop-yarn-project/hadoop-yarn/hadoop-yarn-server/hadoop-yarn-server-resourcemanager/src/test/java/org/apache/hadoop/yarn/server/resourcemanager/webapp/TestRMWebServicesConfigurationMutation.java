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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.http.JettyUtils;
import org.apache.hadoop.util.Sets;
import org.apache.hadoop.yarn.api.records.QueueState;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueuePath;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueuePrefixes;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.TestCapacitySchedulerConfigValidator;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.NodeLabelInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.NodeLabelsInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.jsonprovider.ExcludeRootJSONProvider;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.jsonprovider.IncludeRootJSONProvider;
import org.apache.hadoop.yarn.server.security.ApplicationACLsManager;
import org.apache.hadoop.yarn.webapp.dao.QueueConfigInfo;
import org.apache.hadoop.yarn.webapp.dao.SchedConfUpdateInfo;

import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import java.util.HashMap;
import java.util.Map;

import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration.ACCESSIBLE_NODE_LABELS;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration.CAPACITY;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration.MAXIMUM_CAPACITY;
import static org.apache.hadoop.yarn.server.resourcemanager.webapp.TestWebServiceUtil.toJson;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration.ORDERING_POLICY;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Test scheduler configuration mutation via REST API.
 */
public class TestRMWebServicesConfigurationMutation
    extends AbstractRMWebServicesConfigurationTest {
  private static final Logger LOG = LoggerFactory
      .getLogger(TestRMWebServicesConfigurationMutation.class);
  private static final String LABEL_1 = "label1";

  public TestRMWebServicesConfigurationMutation() {
  }

  private CapacitySchedulerConfiguration getSchedulerConf()
      throws JSONException {
    WebTarget r = targetWithJsonObject();
    Response response =
        r.path("ws").path("v1").path("cluster")
            .queryParam("user.name", userName).path("scheduler-conf")
            .request(MediaType.APPLICATION_JSON)
            .get(Response.class);
    assertEquals(Status.OK.getStatusCode(), response.getStatus());
    JSONObject json = response.readEntity(JSONObject.class);
    JSONArray items = (JSONArray) json.get("property");
    CapacitySchedulerConfiguration parsedConf =
        new CapacitySchedulerConfiguration();
    for (int i = 0; i < items.length(); i++) {
      JSONObject obj = (JSONObject) items.get(i);
      parsedConf.set(obj.get("name").toString(),
          obj.get("value").toString());
    }
    return parsedConf;
  }

  @Test
  public void testGetSchedulerConf() throws Exception {
    CapacitySchedulerConfiguration orgConf = getSchedulerConf();
    assertNotNull(orgConf);
    assertEquals(4, orgConf.getQueues(ROOT).size());
  }

  @Test
  public void testFormatSchedulerConf() throws Exception {
    CapacitySchedulerConfiguration newConf = getSchedulerConf();
    assertNotNull(newConf);
    assertEquals(4, newConf.getQueues(ROOT).size());

    SchedConfUpdateInfo updateInfo = new SchedConfUpdateInfo();
    Map<String, String> nearEmptyCapacity = new HashMap<>();
    nearEmptyCapacity.put(CapacitySchedulerConfiguration.CAPACITY, "1E-4");
    QueueConfigInfo d = new QueueConfigInfo("root.formattest",
        nearEmptyCapacity);
    updateInfo.getAddQueueInfo().add(d);

    Map<String, String> stoppedParam = new HashMap<>();
    stoppedParam.put(CapacitySchedulerConfiguration.STATE,
        QueueState.STOPPED.toString());
    QueueConfigInfo stoppedInfo = new QueueConfigInfo("root.formattest",
        stoppedParam);
    updateInfo.getUpdateQueueInfo().add(stoppedInfo);

    // Add a queue root.formattest to the existing three queues
    WebTarget r = target()
        .register(new IncludeRootJSONProvider())
        .register(new ExcludeRootJSONProvider());
    Response response = r.path("ws").path("v1").path("cluster")
        .path("scheduler-conf").queryParam("user.name", userName)
        .request(MediaType.APPLICATION_JSON)
        .put(Entity.entity(updateInfo, MediaType.APPLICATION_JSON), Response.class);
    newConf = getSchedulerConf();
    assertNotNull(newConf);
    assertEquals(5, newConf.getQueues(ROOT).size());

    // Format the scheduler config and validate root.formattest is not present
    response = r.path("ws").path("v1").path("cluster")
        .queryParam("user.name", userName)
        .path(RMWSConsts.FORMAT_SCHEDULER_CONF)
        .request(MediaType.APPLICATION_JSON).get(Response.class);
    assertEquals(Status.OK.getStatusCode(), response.getStatus());
    newConf = getSchedulerConf();
    assertEquals(4, newConf.getQueues(ROOT).size());
  }

  private long getConfigVersion() throws Exception {
    WebTarget r = targetWithJsonObject();
    Response response = r.path("ws").path("v1").path("cluster")
        .queryParam("user.name", userName)
        .path(RMWSConsts.SCHEDULER_CONF_VERSION)
        .request(MediaType.APPLICATION_JSON).get(Response.class);
    assertEquals(Status.OK.getStatusCode(), response.getStatus());

    JSONObject json = response.readEntity(JSONObject.class).
        getJSONObject("configversion");
    return Long.parseLong(json.get("versionID").toString());
  }

  @Test
  public void testSchedulerConfigVersion() throws Exception {
    assertEquals(1, getConfigVersion());
    testAddNestedQueue();
    assertEquals(2, getConfigVersion());
  }

  @Test
  public void testAddNestedQueue() throws Exception {
    CapacitySchedulerConfiguration orgConf = getSchedulerConf();
    assertNotNull(orgConf);
    assertEquals(4, orgConf.getQueues(ROOT).size());

    WebTarget r = target()
        .register(new IncludeRootJSONProvider())
        .register(new ExcludeRootJSONProvider());

    Response response;

    // Add parent queue root.d with two children d1 and d2.
    SchedConfUpdateInfo updateInfo = new SchedConfUpdateInfo();
    Map<String, String> d1Capacity = new HashMap<>();
    d1Capacity.put(CapacitySchedulerConfiguration.CAPACITY, "25");
    d1Capacity.put(CapacitySchedulerConfiguration.MAXIMUM_CAPACITY, "25");
    Map<String, String> nearEmptyCapacity = new HashMap<>();
    nearEmptyCapacity.put(CapacitySchedulerConfiguration.CAPACITY, "1E-4");
    nearEmptyCapacity.put(CapacitySchedulerConfiguration.MAXIMUM_CAPACITY,
        "1E-4");
    Map<String, String> d2Capacity = new HashMap<>();
    d2Capacity.put(CapacitySchedulerConfiguration.CAPACITY, "75");
    d2Capacity.put(CapacitySchedulerConfiguration.MAXIMUM_CAPACITY, "75");
    QueueConfigInfo d1 = new QueueConfigInfo("root.d.d1", d1Capacity);
    QueueConfigInfo d2 = new QueueConfigInfo("root.d.d2", d2Capacity);
    QueueConfigInfo d = new QueueConfigInfo("root.d", nearEmptyCapacity);
    updateInfo.getAddQueueInfo().add(d1);
    updateInfo.getAddQueueInfo().add(d2);
    updateInfo.getAddQueueInfo().add(d);
    response =
        r.path("ws").path("v1").path("cluster")
            .path("scheduler-conf").queryParam("user.name", userName)
            .request(MediaType.APPLICATION_JSON)
            .put(Entity.entity(updateInfo, MediaType.APPLICATION_JSON), Response.class);

    assertEquals(Status.OK.getStatusCode(), response.getStatus());
    CapacitySchedulerConfiguration newCSConf =
        ((CapacityScheduler) rm.getResourceScheduler()).getConfiguration();
    assertEquals(5, newCSConf.getQueues(ROOT).size());
    assertEquals(2, newCSConf.getQueues(ROOT_D).size());
    assertEquals(25.0f, newCSConf.getNonLabeledQueueCapacity(new QueuePath("root.d.d1")),
        0.01f);
    assertEquals(75.0f, newCSConf.getNonLabeledQueueCapacity(new QueuePath("root.d.d2")),
        0.01f);

    CapacitySchedulerConfiguration newConf = getSchedulerConf();
    assertNotNull(newConf);
    assertEquals(5, newConf.getQueues(ROOT).size());
  }

  @Test
  public void testAddWithUpdate() throws Exception {
    WebTarget r = target()
        .register(new IncludeRootJSONProvider())
        .register(new ExcludeRootJSONProvider());

    Response response;

    // Add root.d with capacity 25, reducing root.b capacity from 75 to 50.
    SchedConfUpdateInfo updateInfo = new SchedConfUpdateInfo();
    Map<String, String> dCapacity = new HashMap<>();
    dCapacity.put(CapacitySchedulerConfiguration.CAPACITY, "25");
    Map<String, String> bCapacity = new HashMap<>();
    bCapacity.put(CapacitySchedulerConfiguration.CAPACITY, "50");
    QueueConfigInfo d = new QueueConfigInfo("root.d", dCapacity);
    QueueConfigInfo b = new QueueConfigInfo("root.b", bCapacity);
    updateInfo.getAddQueueInfo().add(d);
    updateInfo.getUpdateQueueInfo().add(b);
    response =
        r.path("ws").path("v1").path("cluster")
            .path("scheduler-conf").queryParam("user.name", userName)
            .request(MediaType.APPLICATION_JSON)
            .put(Entity.entity(updateInfo, MediaType.APPLICATION_JSON), Response.class);

    assertEquals(Status.OK.getStatusCode(), response.getStatus());
    CapacitySchedulerConfiguration newCSConf =
        ((CapacityScheduler) rm.getResourceScheduler()).getConfiguration();
    assertEquals(5, newCSConf.getQueues(ROOT).size());
    assertEquals(25.0f, newCSConf.getNonLabeledQueueCapacity(new QueuePath("root.d")), 0.01f);
    assertEquals(50.0f, newCSConf.getNonLabeledQueueCapacity(new QueuePath("root.b")), 0.01f);
  }

  @Test
  public void testUnsetParentQueueOrderingPolicy() throws Exception {
    WebTarget r = target()
        .register(new IncludeRootJSONProvider())
        .register(new ExcludeRootJSONProvider());
    Response response;

    // Update ordering policy of Leaf Queue root.b to fair
    SchedConfUpdateInfo updateInfo1 = new SchedConfUpdateInfo();
    Map<String, String> updateParam = new HashMap<>();
    updateParam.put(CapacitySchedulerConfiguration.ORDERING_POLICY,
        "fair");
    QueueConfigInfo aUpdateInfo = new QueueConfigInfo("root.b", updateParam);
    updateInfo1.getUpdateQueueInfo().add(aUpdateInfo);
    response = r.path("ws").path("v1").path("cluster")
        .path("scheduler-conf").queryParam("user.name", userName)
        .request(MediaType.APPLICATION_JSON)
        .put(Entity.entity(updateInfo1, MediaType.APPLICATION_JSON), Response.class);
    assertEquals(Status.OK.getStatusCode(), response.getStatus());
    CapacitySchedulerConfiguration newCSConf =
        ((CapacityScheduler) rm.getResourceScheduler()).getConfiguration();
    String bOrderingPolicy = CapacitySchedulerConfiguration.PREFIX
        + "root.b" + CapacitySchedulerConfiguration.DOT + ORDERING_POLICY;
    assertEquals("fair", newCSConf.get(bOrderingPolicy));

    stopQueue(ROOT_B);

    // Add root.b.b1 which makes root.b a Parent Queue
    SchedConfUpdateInfo updateInfo2 = new SchedConfUpdateInfo();
    Map<String, String> capacity = new HashMap<>();
    capacity.put(CapacitySchedulerConfiguration.CAPACITY, "100");
    QueueConfigInfo b1 = new QueueConfigInfo("root.b.b1", capacity);
    updateInfo2.getAddQueueInfo().add(b1);
    response = r.path("ws").path("v1").path("cluster")
        .path("scheduler-conf").queryParam("user.name", userName)
        .request(MediaType.APPLICATION_JSON)
        .put(Entity.entity(updateInfo2, MediaType.APPLICATION_JSON), Response.class);

    // Validate unset ordering policy of root.b after converted to
    // Parent Queue
    assertEquals(Status.OK.getStatusCode(), response.getStatus());
    newCSConf = ((CapacityScheduler) rm.getResourceScheduler())
        .getConfiguration();
    bOrderingPolicy = CapacitySchedulerConfiguration.PREFIX
        + "root.b" + CapacitySchedulerConfiguration.DOT + ORDERING_POLICY;
    assertNull(newCSConf.get(bOrderingPolicy),
        "Failed to unset Parent Queue OrderingPolicy");
  }

  @Test
  public void testUnsetLeafQueueOrderingPolicy() throws Exception {
    WebTarget r = target()
        .register(new IncludeRootJSONProvider())
        .register(new ExcludeRootJSONProvider());
    Response response;

    // Update ordering policy of Parent Queue root.c to priority-utilization
    SchedConfUpdateInfo updateInfo1 = new SchedConfUpdateInfo();
    Map<String, String> updateParam = new HashMap<>();
    updateParam.put(CapacitySchedulerConfiguration.ORDERING_POLICY,
        "priority-utilization");
    QueueConfigInfo aUpdateInfo = new QueueConfigInfo("root.c", updateParam);
    updateInfo1.getUpdateQueueInfo().add(aUpdateInfo);
    response = r.path("ws").path("v1").path("cluster")
        .path("scheduler-conf").queryParam("user.name", userName)
        .request(MediaType.APPLICATION_JSON)
        .put(Entity.entity(updateInfo1, MediaType.APPLICATION_JSON), Response.class);
    assertEquals(Status.OK.getStatusCode(), response.getStatus());
    CapacitySchedulerConfiguration newCSConf =
        ((CapacityScheduler) rm.getResourceScheduler()).getConfiguration();
    String cOrderingPolicy = CapacitySchedulerConfiguration.PREFIX
        + "root.c" + CapacitySchedulerConfiguration.DOT + ORDERING_POLICY;
    assertEquals("priority-utilization", newCSConf.get(cOrderingPolicy));

    stopQueue(ROOT_C_C1);

    // Remove root.c.c1 which makes root.c a Leaf Queue
    SchedConfUpdateInfo updateInfo2 = new SchedConfUpdateInfo();
    updateInfo2.getRemoveQueueInfo().add("root.c.c1");
    response = r.path("ws").path("v1").path("cluster")
        .path("scheduler-conf").queryParam("user.name", userName)
        .request(MediaType.APPLICATION_JSON)
        .put(Entity.entity(updateInfo2, MediaType.APPLICATION_JSON), Response.class);
    assertEquals(Status.OK.getStatusCode(), response.getStatus());

    // Validate unset ordering policy of root.c after converted to
    // Leaf Queue
    newCSConf = ((CapacityScheduler) rm.getResourceScheduler())
        .getConfiguration();
    cOrderingPolicy = CapacitySchedulerConfiguration.PREFIX
        + "root.c" + CapacitySchedulerConfiguration.DOT + ORDERING_POLICY;
    assertNull(newCSConf.get(cOrderingPolicy),
        "Failed to unset Leaf Queue OrderingPolicy");
  }

  @Test
  public void testRemoveQueue() throws Exception {
    WebTarget r = target()
        .register(new IncludeRootJSONProvider())
        .register(new ExcludeRootJSONProvider());
    Response response;

    stopQueue(ROOT_A_A2);
    // Remove root.a.a2
    SchedConfUpdateInfo updateInfo = new SchedConfUpdateInfo();
    updateInfo.getRemoveQueueInfo().add("root.a.a2");
    response =
        r.path("ws").path("v1").path("cluster")
        .path("scheduler-conf").queryParam("user.name", userName)
        .request(MediaType.APPLICATION_JSON)
        .put(Entity.entity(updateInfo, MediaType.APPLICATION_JSON),
        Response.class);

    assertEquals(Status.OK.getStatusCode(), response.getStatus());
    CapacitySchedulerConfiguration newCSConf =
        ((CapacityScheduler) rm.getResourceScheduler()).getConfiguration();
    assertEquals(1, newCSConf.getQueues(ROOT_A).size(),
        "Failed to remove the queue");
    assertEquals("a1", newCSConf.getQueues(ROOT_A).get(0),
        "Failed to remove the right queue");
  }

  @Test
  public void testStopWithRemoveQueue() throws Exception {
    WebTarget r = target()
        .register(new IncludeRootJSONProvider())
        .register(new ExcludeRootJSONProvider());

    Response response;

    // Set state of queues to STOPPED.
    SchedConfUpdateInfo updateInfo = new SchedConfUpdateInfo();
    Map<String, String> stoppedParam = new HashMap<>();
    stoppedParam.put(CapacitySchedulerConfiguration.STATE,
        QueueState.STOPPED.toString());
    QueueConfigInfo stoppedInfo = new QueueConfigInfo("root.a.a2",
        stoppedParam);
    updateInfo.getUpdateQueueInfo().add(stoppedInfo);

    updateInfo.getRemoveQueueInfo().add("root.a.a2");
    response = r.path("ws").path("v1").path("cluster")
        .path("scheduler-conf").queryParam("user.name", userName)
        .request(MediaType.APPLICATION_JSON)
        .put(Entity.entity(updateInfo, MediaType.APPLICATION_JSON),
        Response.class);

    assertEquals(Status.OK.getStatusCode(), response.getStatus());
    CapacitySchedulerConfiguration newCSConf =
        ((CapacityScheduler) rm.getResourceScheduler()).getConfiguration();
    assertEquals(1, newCSConf.getQueues(ROOT_A).size());
    assertEquals("a1", newCSConf.getQueues(ROOT_A).get(0));
  }

  @Test
  public void testRemoveQueueWhichHasQueueMapping() throws Exception {
    WebTarget r = target()
        .register(new IncludeRootJSONProvider())
        .register(new ExcludeRootJSONProvider());

    Response response;
    CapacityScheduler cs = (CapacityScheduler) rm.getResourceScheduler();
    long versionBefore = getConfigVersion();

    // Validate Queue 'mappedqueue' exists before deletion
    assertNotNull(cs.getQueue("mappedqueue"),
        "Failed to setup CapacityScheduler Configuration");

    // Set state of queue 'mappedqueue' to STOPPED.
    SchedConfUpdateInfo updateInfo = new SchedConfUpdateInfo();
    Map<String, String> stoppedParam = new HashMap<>();
    stoppedParam.put(CapacitySchedulerConfiguration.STATE, QueueState.STOPPED.toString());
    QueueConfigInfo stoppedInfo = new QueueConfigInfo("root.mappedqueue", stoppedParam);
    updateInfo.getUpdateQueueInfo().add(stoppedInfo);

    // Remove queue 'mappedqueue' using update scheduler-conf
    updateInfo.getRemoveQueueInfo().add("root.mappedqueue");
    response = r.path("ws").path("v1").path("cluster").path("scheduler-conf")
        .queryParam("user.name", userName).request(MediaType.APPLICATION_JSON)
        .put(Entity.entity(updateInfo, MediaType.APPLICATION_JSON), Response.class);
    String responseText = response.readEntity(String.class);

    // Queue 'mappedqueue' deletion will fail as there is queue mapping present
    assertEquals(Status.BAD_REQUEST.getStatusCode(), response.getStatus());
    assertEquals(versionBefore, getConfigVersion(),
        "validation rejection must not create a store mutation");
    assertTrue(responseText.contains(
        "Path root 'mappedqueue' does not exist. Path 'mappedqueue' is invalid"));

    // Validate queue 'mappedqueue' exists after above failure
    CapacitySchedulerConfiguration newCSConf =
        ((CapacityScheduler) rm.getResourceScheduler()).getConfiguration();
    assertEquals(4, newCSConf.getQueues(ROOT).size());
    assertNotNull(cs.getQueue("mappedqueue"),
        "CapacityScheduler Configuration is corrupt");
  }

  @Test
  public void testStopWithConvertLeafToParentQueue() throws Exception {
    WebTarget r = target()
        .register(new IncludeRootJSONProvider())
        .register(new ExcludeRootJSONProvider());
    Response response;

    // Set state of queues to STOPPED.
    SchedConfUpdateInfo updateInfo = new SchedConfUpdateInfo();
    Map<String, String> stoppedParam = new HashMap<>();
    stoppedParam.put(CapacitySchedulerConfiguration.STATE,
        QueueState.STOPPED.toString());
    QueueConfigInfo stoppedInfo = new QueueConfigInfo("root.b",
        stoppedParam);
    updateInfo.getUpdateQueueInfo().add(stoppedInfo);

    Map<String, String> b1Capacity = new HashMap<>();
    b1Capacity.put(CapacitySchedulerConfiguration.CAPACITY, "100");
    QueueConfigInfo b1 = new QueueConfigInfo("root.b.b1", b1Capacity);
    updateInfo.getAddQueueInfo().add(b1);

    response = r.path("ws").path("v1").path("cluster")
        .path("scheduler-conf").queryParam("user.name", userName)
        .request(MediaType.APPLICATION_JSON)
        .put(Entity.entity(updateInfo, MediaType.APPLICATION_JSON), Response.class);

    assertEquals(Status.OK.getStatusCode(), response.getStatus());
    CapacitySchedulerConfiguration newCSConf =
        ((CapacityScheduler) rm.getResourceScheduler()).getConfiguration();
    assertEquals(1, newCSConf.getQueues(ROOT_B).size());
    assertEquals("b1", newCSConf.getQueues(ROOT_B).get(0));
  }

  @Test
  public void testRemoveParentQueue() throws Exception {
    WebTarget r = target()
        .register(new IncludeRootJSONProvider())
        .register(new ExcludeRootJSONProvider());

    Response response;

    stopQueue(ROOT_C, ROOT_C_C1);
    // Remove root.c (parent queue)
    SchedConfUpdateInfo updateInfo = new SchedConfUpdateInfo();
    updateInfo.getRemoveQueueInfo().add("root.c");
    response =
        r.path("ws").path("v1").path("cluster")
            .path("scheduler-conf").queryParam("user.name", userName)
            .request(MediaType.APPLICATION_JSON)
            .put(Entity.entity(updateInfo, MediaType.APPLICATION_JSON), Response.class);

    assertEquals(Status.OK.getStatusCode(), response.getStatus());
    CapacitySchedulerConfiguration newCSConf =
        ((CapacityScheduler) rm.getResourceScheduler()).getConfiguration();
    assertEquals(3, newCSConf.getQueues(ROOT).size());
    assertEquals(0, newCSConf.getQueues(ROOT_C).size());
  }

  @Test
  public void testRemoveParentQueueWithCapacity() throws Exception {
    WebTarget r = target()
        .register(new IncludeRootJSONProvider())
        .register(new ExcludeRootJSONProvider());

    Response response;

    stopQueue(ROOT_A, ROOT_A_A1, ROOT_A_A2);
    // Remove root.a (parent queue) with capacity 25
    SchedConfUpdateInfo updateInfo = new SchedConfUpdateInfo();
    updateInfo.getRemoveQueueInfo().add("root.a");

    // Set root.b capacity to 100
    Map<String, String> bCapacity = new HashMap<>();
    bCapacity.put(CapacitySchedulerConfiguration.CAPACITY, "100");
    QueueConfigInfo b = new QueueConfigInfo("root.b", bCapacity);
    updateInfo.getUpdateQueueInfo().add(b);
    response =
        r.path("ws").path("v1").path("cluster")
            .path("scheduler-conf").queryParam("user.name", userName)
            .request(MediaType.APPLICATION_JSON)
            .put(Entity.entity(updateInfo, MediaType.APPLICATION_JSON), Response.class);

    assertEquals(Status.OK.getStatusCode(), response.getStatus());
    CapacitySchedulerConfiguration newCSConf =
        ((CapacityScheduler) rm.getResourceScheduler()).getConfiguration();
    assertEquals(3, newCSConf.getQueues(ROOT).size());
    assertEquals(100.0f, newCSConf.getNonLabeledQueueCapacity(new QueuePath("root.b")),
        0.01f);
  }

  @Test
  public void testRemoveMultipleQueues() throws Exception {
    WebTarget r = target()
        .register(new IncludeRootJSONProvider())
        .register(new ExcludeRootJSONProvider());

    Response response;

    stopQueue(ROOT_B, ROOT_C, ROOT_C_C1);
    // Remove root.b and root.c
    SchedConfUpdateInfo updateInfo = new SchedConfUpdateInfo();
    updateInfo.getRemoveQueueInfo().add("root.b");
    updateInfo.getRemoveQueueInfo().add("root.c");
    Map<String, String> aCapacity = new HashMap<>();
    aCapacity.put(CapacitySchedulerConfiguration.CAPACITY, "100");
    aCapacity.put(CapacitySchedulerConfiguration.MAXIMUM_CAPACITY, "100");
    QueueConfigInfo configInfo = new QueueConfigInfo("root.a", aCapacity);
    updateInfo.getUpdateQueueInfo().add(configInfo);
    response =
        r.path("ws").path("v1").path("cluster")
            .path("scheduler-conf").queryParam("user.name", userName)
            .request(MediaType.APPLICATION_JSON)
            .put(Entity.entity(updateInfo, MediaType.APPLICATION_JSON), Response.class);

    assertEquals(Status.OK.getStatusCode(), response.getStatus());
    CapacitySchedulerConfiguration newCSConf =
        ((CapacityScheduler) rm.getResourceScheduler()).getConfiguration();
    assertEquals(2, newCSConf.getQueues(ROOT).size());
  }

  private void stopQueue(QueuePath... queuePaths) throws Exception {
    WebTarget r = target()
        .register(new IncludeRootJSONProvider())
        .register(new ExcludeRootJSONProvider());

    Response response;

    // Set state of queues to STOPPED.
    SchedConfUpdateInfo updateInfo = new SchedConfUpdateInfo();
    Map<String, String> stoppedParam = new HashMap<>();
    stoppedParam.put(CapacitySchedulerConfiguration.STATE,
        QueueState.STOPPED.toString());
    for (QueuePath queue : queuePaths) {
      QueueConfigInfo stoppedInfo = new QueueConfigInfo(queue.getFullPath(), stoppedParam);
      updateInfo.getUpdateQueueInfo().add(stoppedInfo);
    }
    response =
        r.path("ws").path("v1").path("cluster")
            .path("scheduler-conf").queryParam("user.name", userName)
            .request(MediaType.APPLICATION_JSON)
            .put(Entity.entity(updateInfo, MediaType.APPLICATION_JSON), Response.class);
    assertEquals(Status.OK.getStatusCode(), response.getStatus());
    CapacitySchedulerConfiguration newCSConf =
        ((CapacityScheduler) rm.getResourceScheduler()).getConfiguration();
    for (QueuePath queue : queuePaths) {
      assertEquals(QueueState.STOPPED, newCSConf.getState(queue));
    }
  }

  @Test
  public void testUpdateQueue() throws Exception {
    WebTarget r = target()
        .register(new IncludeRootJSONProvider())
        .register(new ExcludeRootJSONProvider());

    Response response;

    // Update config value.
    SchedConfUpdateInfo updateInfo = new SchedConfUpdateInfo();
    Map<String, String> updateParam = new HashMap<>();
    updateParam.put(CapacitySchedulerConfiguration.MAXIMUM_AM_RESOURCE_SUFFIX,
        "0.2");
    QueueConfigInfo aUpdateInfo = new QueueConfigInfo("root.a", updateParam);
    updateInfo.getUpdateQueueInfo().add(aUpdateInfo);
    CapacityScheduler cs = (CapacityScheduler) rm.getResourceScheduler();

    assertEquals(CapacitySchedulerConfiguration
            .DEFAULT_MAXIMUM_APPLICATIONMASTERS_RESOURCE_PERCENT,
        cs.getConfiguration()
            .getMaximumApplicationMasterResourcePerQueuePercent(ROOT_A),
        0.001f);
    response =
        r.path("ws").path("v1").path("cluster")
            .path("scheduler-conf").queryParam("user.name", userName)
            .request(MediaType.APPLICATION_JSON)
            .put(Entity.entity(updateInfo, MediaType.APPLICATION_JSON), Response.class);
    LOG.debug("Response headers: {}.", response.getHeaders());
    assertEquals(Status.OK.getStatusCode(), response.getStatus());
    CapacitySchedulerConfiguration newCSConf = cs.getConfiguration();
    assertEquals(0.2f, newCSConf
        .getMaximumApplicationMasterResourcePerQueuePercent(ROOT_A), 0.001f);

    // Remove config. Config value should be reverted to default.
    updateParam.put(CapacitySchedulerConfiguration.MAXIMUM_AM_RESOURCE_SUFFIX,
        null);
    aUpdateInfo = new QueueConfigInfo("root.a", updateParam);
    updateInfo.getUpdateQueueInfo().clear();
    updateInfo.getUpdateQueueInfo().add(aUpdateInfo);
    response =
        r.path("ws").path("v1").path("cluster")
            .path("scheduler-conf").queryParam("user.name", userName)
            .request(MediaType.APPLICATION_JSON)
            .put(Entity.entity(updateInfo, MediaType.APPLICATION_JSON),
            Response.class);
    assertEquals(Status.OK.getStatusCode(), response.getStatus());
    newCSConf = cs.getConfiguration();
    assertEquals(CapacitySchedulerConfiguration
        .DEFAULT_MAXIMUM_APPLICATIONMASTERS_RESOURCE_PERCENT, newCSConf
            .getMaximumApplicationMasterResourcePerQueuePercent(ROOT_A),
        0.001f);
  }

  @Test
  public void testUpdateQueueCapacity() throws Exception {
    WebTarget r = target()
        .register(new IncludeRootJSONProvider())
        .register(new ExcludeRootJSONProvider());

    Response response;

    // Update root.a and root.b capacity to 50.
    SchedConfUpdateInfo updateInfo = new SchedConfUpdateInfo();
    Map<String, String> updateParam = new HashMap<>();
    updateParam.put(CapacitySchedulerConfiguration.CAPACITY, "50");
    QueueConfigInfo aUpdateInfo = new QueueConfigInfo("root.a", updateParam);
    QueueConfigInfo bUpdateInfo = new QueueConfigInfo("root.b", updateParam);
    updateInfo.getUpdateQueueInfo().add(aUpdateInfo);
    updateInfo.getUpdateQueueInfo().add(bUpdateInfo);

    response =
        r.path("ws").path("v1").path("cluster")
            .path("scheduler-conf").queryParam("user.name", userName)
            .request(MediaType.APPLICATION_JSON)
            .put(Entity.entity(updateInfo, MediaType.APPLICATION_JSON), Response.class);
    assertEquals(Status.OK.getStatusCode(), response.getStatus());
    CapacitySchedulerConfiguration newCSConf =
        ((CapacityScheduler) rm.getResourceScheduler()).getConfiguration();
    assertEquals(50.0f, newCSConf.getNonLabeledQueueCapacity(new QueuePath("root.a")), 0.01f);
    assertEquals(50.0f, newCSConf.getNonLabeledQueueCapacity(new QueuePath("root.b")), 0.01f);
  }

  @Test
  public void testGlobalConfChange() throws Exception {
    WebTarget r = target()
        .register(new IncludeRootJSONProvider())
        .register(new ExcludeRootJSONProvider());

    Response response;

    // Set maximum-applications to 30000.
    SchedConfUpdateInfo updateInfo = new SchedConfUpdateInfo();
    updateInfo.getGlobalParams().put(CapacitySchedulerConfiguration.PREFIX +
        "maximum-applications", "30000");

    response = r.path("ws").path("v1").path("cluster")
        .path("scheduler-conf").queryParam("user.name", userName)
        .request(MediaType.APPLICATION_JSON)
        .put(Entity.entity(updateInfo, MediaType.APPLICATION_JSON), Response.class);
    assertEquals(Status.OK.getStatusCode(), response.getStatus());
    CapacitySchedulerConfiguration newCSConf =
        ((CapacityScheduler) rm.getResourceScheduler()).getConfiguration();
    assertEquals(30000, newCSConf.getMaximumSystemApplications());

    updateInfo.getGlobalParams().put(CapacitySchedulerConfiguration.PREFIX +
        "maximum-applications", null);
    // Unset maximum-applications. Should be set to default.
    response =
        r.path("ws").path("v1").path("cluster")
            .path("scheduler-conf").queryParam("user.name", userName)
            .request(MediaType.APPLICATION_JSON)
            .put(Entity.entity(updateInfo, MediaType.APPLICATION_JSON),
            Response.class);
    assertEquals(Status.OK.getStatusCode(), response.getStatus());
    newCSConf =
        ((CapacityScheduler) rm.getResourceScheduler()).getConfiguration();
    assertEquals(CapacitySchedulerConfiguration
        .DEFAULT_MAXIMUM_SYSTEM_APPLICATIIONS,
        newCSConf.getMaximumSystemApplications());
  }

  @Test
  public void testNodeLabelRemovalResidualConfigsAreCleared() throws Exception {
    WebTarget r = target()
        .register(new IncludeRootJSONProvider())
        .register(new ExcludeRootJSONProvider());
    Response response;

    // 1. Create Node Label: label1
    NodeLabelsInfo nodeLabelsInfo = new NodeLabelsInfo();
    nodeLabelsInfo.getNodeLabelsInfo().add(new NodeLabelInfo(LABEL_1));
    WebTarget addNodeLabelsResource = r.path("ws").path("v1").path("cluster")
        .path("add-node-labels");
    WebTarget getNodeLabelsResource = r.path("ws").path("v1").path("cluster")
        .path("get-node-labels");
    WebTarget removeNodeLabelsResource = r.path("ws").path("v1").path("cluster")
        .path("remove-node-labels");
    WebTarget schedulerConfResource = r.path("ws").path("v1").path("cluster")
        .path(RMWSConsts.SCHEDULER_CONF);
    response = addNodeLabelsResource.queryParam("user.name", userName)
        .request(MediaType.APPLICATION_JSON)
        .post(Entity.entity(logAndReturnJson(addNodeLabelsResource,
        toJson(nodeLabelsInfo, NodeLabelsInfo.class)), MediaType.APPLICATION_JSON), Response.class);

    // 2. Verify new Node Label
    response = getNodeLabelsResource.queryParam("user.name", userName)
        .request(MediaType.APPLICATION_JSON).get(Response.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE + ";" + JettyUtils.UTF_8,
        response.getMediaType().toString());
    nodeLabelsInfo = response.readEntity(NodeLabelsInfo.class);
    assertEquals(1, nodeLabelsInfo.getNodeLabels().size());
    for (NodeLabelInfo nl : nodeLabelsInfo.getNodeLabelsInfo()) {
      assertEquals(LABEL_1, nl.getName());
      assertTrue(nl.getExclusivity());
    }

    // 3. Assign 'label1' to root.a
    SchedConfUpdateInfo updateInfo = new SchedConfUpdateInfo();
    Map<String, String> updateForRoot = new HashMap<>();
    updateForRoot.put(CapacitySchedulerConfiguration.ACCESSIBLE_NODE_LABELS, "*");
    QueueConfigInfo rootUpdateInfo = new QueueConfigInfo(ROOT.getFullPath(), updateForRoot);

    Map<String, String> updateForRootA = new HashMap<>();
    updateForRootA.put(CapacitySchedulerConfiguration.ACCESSIBLE_NODE_LABELS, LABEL_1);
    QueueConfigInfo rootAUpdateInfo = new QueueConfigInfo(ROOT_A.getFullPath(), updateForRootA);

    updateInfo.getUpdateQueueInfo().add(rootUpdateInfo);
    updateInfo.getUpdateQueueInfo().add(rootAUpdateInfo);

    response = schedulerConfResource
        .queryParam("user.name", userName)
        .request(MediaType.APPLICATION_JSON)
        .put(Entity.entity(logAndReturnJson(schedulerConfResource, toJson(updateInfo,
        SchedConfUpdateInfo.class)), MediaType.APPLICATION_JSON), Response.class);
    assertEquals(Status.OK.getStatusCode(), response.getStatus());

    CapacityScheduler cs = (CapacityScheduler) rm.getResourceScheduler();

    assertEquals(Sets.newHashSet("*"),
        cs.getConfiguration().getAccessibleNodeLabels(ROOT));
    assertEquals(Sets.newHashSet(LABEL_1),
        cs.getConfiguration().getAccessibleNodeLabels(ROOT_A));

    // 4. Set partition capacities to queues as below
    updateInfo = new SchedConfUpdateInfo();
    updateForRoot = new HashMap<>();
    updateForRoot.put(getAccessibleNodeLabelsCapacityPropertyName(LABEL_1), "100");
    updateForRoot.put(getAccessibleNodeLabelsMaxCapacityPropertyName(LABEL_1), "100");
    rootUpdateInfo = new QueueConfigInfo(ROOT.getFullPath(), updateForRoot);

    updateForRootA = new HashMap<>();
    updateForRootA.put(getAccessibleNodeLabelsCapacityPropertyName(LABEL_1), "100");
    updateForRootA.put(getAccessibleNodeLabelsMaxCapacityPropertyName(LABEL_1), "100");
    rootAUpdateInfo = new QueueConfigInfo(ROOT_A.getFullPath(), updateForRootA);

    // Avoid the following exception by adding some capacities to root.a.a1 and root.a.a2 to label1
    // Illegal capacity sum of 0.0 for children of queue a for label=label1.
    // It is set to 0, but parent percent != 0, and doesn't allow children capacity to set to 0
    Map<String, String> updateForRootA_A1 = new HashMap<>();
    updateForRootA_A1.put(getAccessibleNodeLabelsCapacityPropertyName(LABEL_1), "20");
    updateForRootA_A1.put(getAccessibleNodeLabelsMaxCapacityPropertyName(LABEL_1), "20");
    QueueConfigInfo rootA_A1UpdateInfo = new QueueConfigInfo(ROOT_A_A1.getFullPath(),
        updateForRootA_A1);

    Map<String, String> updateForRootA_A2 = new HashMap<>();
    updateForRootA_A2.put(getAccessibleNodeLabelsCapacityPropertyName(LABEL_1), "80");
    updateForRootA_A2.put(getAccessibleNodeLabelsMaxCapacityPropertyName(LABEL_1), "80");
    QueueConfigInfo rootA_A2UpdateInfo = new QueueConfigInfo(ROOT_A_A2.getFullPath(),
        updateForRootA_A2);


    updateInfo.getUpdateQueueInfo().add(rootUpdateInfo);
    updateInfo.getUpdateQueueInfo().add(rootAUpdateInfo);
    updateInfo.getUpdateQueueInfo().add(rootA_A1UpdateInfo);
    updateInfo.getUpdateQueueInfo().add(rootA_A2UpdateInfo);

    response = schedulerConfResource
        .queryParam("user.name", userName)
        .request(MediaType.APPLICATION_JSON)
        .put(Entity.entity(logAndReturnJson(schedulerConfResource, toJson(updateInfo,
        SchedConfUpdateInfo.class)), MediaType.APPLICATION_JSON),
        Response.class);
    assertEquals(Status.OK.getStatusCode(), response.getStatus());

    assertEquals(100.0, cs.getConfiguration().getLabeledQueueCapacity(ROOT, LABEL_1), 0.001f);
    assertEquals(100.0, cs.getConfiguration().getLabeledQueueMaximumCapacity(ROOT, LABEL_1),
        0.001f);
    assertEquals(100.0, cs.getConfiguration().getLabeledQueueCapacity(ROOT_A, LABEL_1), 0.001f);
    assertEquals(100.0, cs.getConfiguration().getLabeledQueueMaximumCapacity(ROOT_A, LABEL_1),
        0.001f);
    assertEquals(20.0, cs.getConfiguration().getLabeledQueueCapacity(ROOT_A_A1, LABEL_1), 0.001f);
    assertEquals(20.0, cs.getConfiguration().getLabeledQueueMaximumCapacity(ROOT_A_A1, LABEL_1),
        0.001f);
    assertEquals(80.0, cs.getConfiguration().getLabeledQueueCapacity(ROOT_A_A2, LABEL_1), 0.001f);
    assertEquals(80.0, cs.getConfiguration().getLabeledQueueMaximumCapacity(ROOT_A_A2, LABEL_1),
        0.001f);

    //5. De-assign node label: "label1" + Remove residual properties
    updateInfo = new SchedConfUpdateInfo();
    updateForRoot = new HashMap<>();
    updateForRoot.put(CapacitySchedulerConfiguration.ACCESSIBLE_NODE_LABELS, "*");
    updateForRoot.put(getAccessibleNodeLabelsCapacityPropertyName(LABEL_1), "");
    updateForRoot.put(getAccessibleNodeLabelsMaxCapacityPropertyName(LABEL_1), "");
    rootUpdateInfo = new QueueConfigInfo(ROOT.getFullPath(), updateForRoot);

    updateForRootA = new HashMap<>();
    updateForRootA.put(CapacitySchedulerConfiguration.ACCESSIBLE_NODE_LABELS, "");
    updateForRootA.put(getAccessibleNodeLabelsCapacityPropertyName(LABEL_1), "");
    updateForRootA.put(getAccessibleNodeLabelsMaxCapacityPropertyName(LABEL_1), "");
    rootAUpdateInfo = new QueueConfigInfo(ROOT_A.getFullPath(), updateForRootA);

    updateForRootA_A1 = new HashMap<>();
    updateForRootA_A1.put(CapacitySchedulerConfiguration.ACCESSIBLE_NODE_LABELS, "");
    updateForRootA_A1.put(getAccessibleNodeLabelsCapacityPropertyName(LABEL_1), "");
    updateForRootA_A1.put(getAccessibleNodeLabelsMaxCapacityPropertyName(LABEL_1), "");
    rootA_A1UpdateInfo = new QueueConfigInfo(ROOT_A_A1.getFullPath(), updateForRootA_A1);

    updateForRootA_A2 = new HashMap<>();
    updateForRootA_A2.put(CapacitySchedulerConfiguration.ACCESSIBLE_NODE_LABELS, "");
    updateForRootA_A2.put(getAccessibleNodeLabelsCapacityPropertyName(LABEL_1), "");
    updateForRootA_A2.put(getAccessibleNodeLabelsMaxCapacityPropertyName(LABEL_1), "");
    rootA_A2UpdateInfo = new QueueConfigInfo(ROOT_A_A2.getFullPath(), updateForRootA_A2);

    updateInfo.getUpdateQueueInfo().add(rootUpdateInfo);
    updateInfo.getUpdateQueueInfo().add(rootAUpdateInfo);
    updateInfo.getUpdateQueueInfo().add(rootA_A1UpdateInfo);
    updateInfo.getUpdateQueueInfo().add(rootA_A2UpdateInfo);

    response = schedulerConfResource
        .queryParam("user.name", userName)
        .request(MediaType.APPLICATION_JSON)
        .put(Entity.entity(logAndReturnJson(schedulerConfResource, toJson(updateInfo,
        SchedConfUpdateInfo.class)), MediaType.APPLICATION_JSON), Response.class);
    assertEquals(Status.OK.getStatusCode(), response.getStatus());
    assertEquals(Sets.newHashSet("*"),
        cs.getConfiguration().getAccessibleNodeLabels(ROOT));
    assertNull(cs.getConfiguration().getAccessibleNodeLabels(ROOT_A));

    //6. Remove node label 'label1'
    response =
        removeNodeLabelsResource
            .queryParam("user.name", userName)
            .queryParam("labels", LABEL_1)
            .request(MediaType.APPLICATION_JSON)
            .post(null, Response.class);

    // Verify
    response =
        getNodeLabelsResource.queryParam("user.name", userName)
            .request(MediaType.APPLICATION_JSON).get(Response.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE + ";" + JettyUtils.UTF_8,
        response.getMediaType().toString());
    nodeLabelsInfo = response.readEntity(NodeLabelsInfo.class);
    assertEquals(0, nodeLabelsInfo.getNodeLabels().size());

    //6. Check residual configs
    assertNull(getConfValueForQueueAndLabelAndType(cs, ROOT, LABEL_1, CAPACITY));
    assertNull(getConfValueForQueueAndLabelAndType(cs, ROOT, LABEL_1, MAXIMUM_CAPACITY));
    assertNull(getConfValueForQueueAndLabelAndType(cs, ROOT_A, LABEL_1, CAPACITY));
    assertNull(getConfValueForQueueAndLabelAndType(cs, ROOT_A, LABEL_1, MAXIMUM_CAPACITY));
    assertNull(getConfValueForQueueAndLabelAndType(cs, ROOT_A_A1, LABEL_1, CAPACITY));
    assertNull(getConfValueForQueueAndLabelAndType(cs, ROOT_A_A1, LABEL_1, MAXIMUM_CAPACITY));
    assertNull(getConfValueForQueueAndLabelAndType(cs, ROOT_A_A2, LABEL_1, CAPACITY));
    assertNull(getConfValueForQueueAndLabelAndType(cs, ROOT_A_A2, LABEL_1, MAXIMUM_CAPACITY));
  }

  private String getConfValueForQueueAndLabelAndType(CapacityScheduler cs,
      QueuePath queuePath, String label, String type) {
    return cs.getConfiguration().get(
            QueuePrefixes.getNodeLabelPrefix(
            queuePath, label) + type);
  }

  private Object logAndReturnJson(WebTarget ws, String json) {
    LOG.info("Sending to web resource: {}, json: {}", ws, json);
    return json;
  }

  private String getAccessibleNodeLabelsCapacityPropertyName(String label) {
    return String.format("%s.%s.%s", ACCESSIBLE_NODE_LABELS, label, CAPACITY);
  }

  private String getAccessibleNodeLabelsMaxCapacityPropertyName(String label) {
    return String.format("%s.%s.%s", ACCESSIBLE_NODE_LABELS, label, MAXIMUM_CAPACITY);
  }

  @Test
  public void testValidateWithClusterMaxAllocation() throws Exception {
    WebTarget r = target()
        .register(new IncludeRootJSONProvider())
        .register(new ExcludeRootJSONProvider());
    int clusterMax = YarnConfiguration.
        DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_MB * 2;
    conf.setInt(YarnConfiguration.RM_SCHEDULER_MAXIMUM_ALLOCATION_MB,
        clusterMax);

    SchedConfUpdateInfo updateInfo = new SchedConfUpdateInfo();
    Map<String, String> updateParam = new HashMap<>();
    updateParam.put(CapacitySchedulerConfiguration.MAXIMUM_APPLICATIONS_SUFFIX,
        "100");
    QueueConfigInfo aUpdateInfo = new QueueConfigInfo("root.a", updateParam);
    updateInfo.getUpdateQueueInfo().add(aUpdateInfo);

    Response response =
        r.path("ws").path("v1").path("cluster")
        .path(RMWSConsts.SCHEDULER_CONF_VALIDATE)
        .queryParam("user.name", userName)
        .request(MediaType.APPLICATION_JSON)
        .post(Entity.entity(updateInfo, MediaType.APPLICATION_JSON),
        Response.class);
    assertEquals(Status.OK.getStatusCode(), response.getStatus());
  }

  @Test
  public void testStructuredValidationResponseJsonAndXml() throws Exception {
    WebTarget endpoint = target()
        .register(new IncludeRootJSONProvider())
        .register(new ExcludeRootJSONProvider())
        .path("ws").path("v1").path("cluster")
        .path(RMWSConsts.SCHEDULER_CONF_VALIDATE_V2)
        .queryParam("user.name", userName);
    SchedConfUpdateInfo updateInfo = new SchedConfUpdateInfo();

    Response jsonResponse = endpoint.request(MediaType.APPLICATION_JSON)
        .post(Entity.entity(updateInfo, MediaType.APPLICATION_JSON));
    assertEquals(Status.OK.getStatusCode(), jsonResponse.getStatus());
    JSONObject json = new JSONObject(jsonResponse.readEntity(String.class))
        .getJSONObject("validationResult");
    assertTrue(json.getBoolean("valid"));
    assertTrue(json.has("configVersion"));
    assertTrue(json.has("issues"));

    Response xmlResponse = endpoint.request(MediaType.APPLICATION_XML)
        .post(Entity.entity(updateInfo, MediaType.APPLICATION_XML));
    assertEquals(Status.OK.getStatusCode(), xmlResponse.getStatus());
    String xml = xmlResponse.readEntity(String.class);
    assertTrue(xml.contains("<validationResult>"));
    assertTrue(xml.contains("<valid>true</valid>"));
    assertTrue(xml.contains("<configVersion>"));
  }

  @Test
  public void testStructuredValidationWarningsRemainValid() throws Exception {
    WebTarget endpoint = validationV2Endpoint();
    SchedConfUpdateInfo updateInfo = new SchedConfUpdateInfo();
    updateInfo.getGlobalParams().put("yarn.resourcemanager.zk-acl",
        "world:anyone:rwcda");

    Response response = endpoint.request(MediaType.APPLICATION_JSON)
        .post(Entity.entity(updateInfo, MediaType.APPLICATION_JSON),
            Response.class);
    assertEquals(Status.OK.getStatusCode(), response.getStatus());
    JSONObject validation = new JSONObject(response.readEntity(String.class))
        .getJSONObject("validationResult");
    assertTrue(validation.getBoolean("valid"));
    JSONArray issues = validation.getJSONObject("issues")
        .getJSONArray("issue");
    assertTrue(issues.toString().contains("deprecated-key"),
        issues.toString());
    assertTrue(issues.toString().contains("WARNING"), issues.toString());
  }

  @Test
  public void testPutReturnsStructuredValidationAndNewVersion()
      throws Exception {
    long version = getConfigVersion();
    SchedConfUpdateInfo updateInfo = new SchedConfUpdateInfo();
    Map<String, String> properties = new HashMap<>();
    properties.put(CAPACITY, "1");
    updateInfo.getAddQueueInfo().add(new QueueConfigInfo("root.e", properties));
    Map<String, String> updateProperties = new HashMap<>();
    updateProperties.put(CAPACITY, "74");
    updateInfo.getUpdateQueueInfo().add(
        new QueueConfigInfo("root.b", updateProperties));
    Response put = mutationEndpoint().request(MediaType.APPLICATION_JSON)
        .put(Entity.entity(updateInfo, MediaType.APPLICATION_JSON),
            Response.class);
    assertEquals(Status.OK.getStatusCode(), put.getStatus());
    JSONObject result = new JSONObject(put.readEntity(String.class))
        .getJSONObject("validationResult");
    assertTrue(result.getBoolean("valid"));
    assertEquals(version + 1, result.getLong("configVersion"));
    assertTrue(result.has("issues"));
    assertEquals(version + 1, getConfigVersion());
  }

  @Test
  public void testPutReturnsStructuredIssuesWithoutMutation()
      throws Exception {
    long version = getConfigVersion();
    SchedConfUpdateInfo updateInfo = new SchedConfUpdateInfo();
    updateInfo.getGlobalParams().put(
        YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB,
        "not-an-integer");

    Response put = mutationEndpoint().request(MediaType.APPLICATION_JSON)
        .put(Entity.entity(updateInfo, MediaType.APPLICATION_JSON),
            Response.class);
    assertEquals(Status.BAD_REQUEST.getStatusCode(), put.getStatus());
    JSONObject result = new JSONObject(put.readEntity(String.class))
        .getJSONObject("validationResult");
    assertFalse(result.getBoolean("valid"));
    assertEquals(version, result.getLong("configVersion"));
    assertTrue(result.getJSONObject("issues").getJSONArray("issue")
        .toString().contains("memory-allocation"));
    assertEquals(version, getConfigVersion());
  }

  @Test
  public void testStructuredValidationXmlOmitsNullableIssueFields()
      throws Exception {
    WebTarget endpoint = validationV2Endpoint();
    SchedConfUpdateInfo updateInfo = new SchedConfUpdateInfo();
    updateInfo.getGlobalParams().put(
        YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB,
        "not-an-integer");

    Response response = endpoint.request(MediaType.APPLICATION_XML)
        .post(Entity.entity(updateInfo, MediaType.APPLICATION_XML),
            Response.class);
    assertEquals(Status.BAD_REQUEST.getStatusCode(), response.getStatus());
    String xml = response.readEntity(String.class);
    assertTrue(xml.contains("<ruleId>memory-allocation</ruleId>"), xml);
    assertFalse(xml.contains("<queuePath>"), xml);
    assertTrue(xml.contains("<propertyKey>"
        + YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB
        + "</propertyKey>"), xml);
  }

  @Test
  public void testQueueNameValidationRestContract() throws Exception {
    assertEquals(Status.BAD_REQUEST.getStatusCode(),
        validateQueueName("root.a..b").getStatus());

    Response warning = validateQueueName("root.wéird");
    assertEquals(Status.OK.getStatusCode(), warning.getStatus());
    JSONObject warningResult = new JSONObject(
        warning.readEntity(String.class)).getJSONObject("validationResult");
    assertTrue(warningResult.getBoolean("valid"));
    assertTrue(warningResult.getJSONObject("issues")
        .getJSONArray("issue").toString().contains("queue-name"));

    assertEquals(Status.BAD_REQUEST.getStatusCode(),
        validateQueueName("root..b").getStatus());

    Response emptyComponent = validateQueueList("a,,b");
    assertEquals(Status.BAD_REQUEST.getStatusCode(),
        emptyComponent.getStatus());
    assertTrue(emptyComponent.readEntity(String.class)
        .contains("Queue list contains an empty component"));
  }

  @Test
  public void testStructuredValidationRejectsFileBasedScheduler()
      throws Exception {
    ResourceManager nonMutableRm = mock(ResourceManager.class);
    when(nonMutableRm.getResourceScheduler()).thenReturn(
        new CapacityScheduler());
    RMContext context = TestCapacitySchedulerConfigValidator.prepareRMContext();
    when(nonMutableRm.getRMContext()).thenReturn(context);
    ApplicationACLsManager aclsManager = mock(ApplicationACLsManager.class);
    when(aclsManager.areACLsEnabled()).thenReturn(false);
    when(nonMutableRm.getApplicationACLsManager()).thenReturn(aclsManager);
    RMWebServices webService = new RMWebServices(nonMutableRm,
        new Configuration(), mock(HttpServletResponse.class));
    HttpServletRequest servletRequest = mock(HttpServletRequest.class);
    when(servletRequest.getUserPrincipal()).thenReturn(() -> userName);

    Response response = webService.validateSchedulerConfigurationStructured(
        new SchedConfUpdateInfo(), servletRequest);
    assertEquals(Status.BAD_REQUEST.getStatusCode(), response.getStatus());
    assertEquals("Configuration change validation only supported by "
        + "MutableConfScheduler.", response.getEntity());
  }

  private Response validateQueueName(String queueName) {
    return validateQueueList("a,b,c,mappedqueue,"
        + queueName.substring("root.".length()));
  }

  private Response validateQueueList(String queueList) {
    SchedConfUpdateInfo updateInfo = new SchedConfUpdateInfo();
    updateInfo.getGlobalParams().put(
        QueuePrefixes.getQueuePrefix(ROOT)
            + CapacitySchedulerConfiguration.QUEUES,
        queueList);
    return validationV2Endpoint().request(MediaType.APPLICATION_JSON)
        .post(Entity.entity(updateInfo, MediaType.APPLICATION_JSON),
            Response.class);
  }

  private WebTarget validationV2Endpoint() {
    return target().register(new IncludeRootJSONProvider())
        .register(new ExcludeRootJSONProvider())
        .path("ws").path("v1").path("cluster")
        .path(RMWSConsts.SCHEDULER_CONF_VALIDATE_V2)
        .queryParam("user.name", userName);
  }

  private WebTarget mutationEndpoint() {
    return target().register(new IncludeRootJSONProvider())
        .register(new ExcludeRootJSONProvider())
        .path("ws").path("v1").path("cluster")
        .path(RMWSConsts.SCHEDULER_CONF)
        .queryParam("user.name", userName);
  }

}
