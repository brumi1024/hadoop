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

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.security.Principal;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.webapp.dao.QueueConfigInfo;
import org.apache.hadoop.yarn.webapp.dao.SchedConfUpdateInfo;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Calls the scheduler-conf endpoints of {@link RMWebServices} directly,
 * without a web server, against a ResourceManager whose Capacity Scheduler
 * configuration lives in a file system store.
 */
final class SchedulerConfMutationTestSupport {

  static final String PREFIX = "yarn.scheduler.capacity.";

  private SchedulerConfMutationTestSupport() {
  }

  /**
   * A configuration for a ResourceManager with the Capacity Scheduler and
   * a file system configuration store in {@code storeDir}, seeded with
   * {@code schedulerConf}.
   */
  static YarnConfiguration createConfiguration(File storeDir,
      Map<String, String> schedulerConf) throws IOException {
    if (!storeDir.isDirectory() && !storeDir.mkdirs()) {
      throw new IOException("Cannot create " + storeDir);
    }
    Configuration seed = new Configuration(false);
    for (Map.Entry<String, String> e : schedulerConf.entrySet()) {
      seed.set(e.getKey(), e.getValue());
    }
    // The store reads the latest capacity-scheduler.xml.<timestamp> file
    File seedFile = new File(storeDir, YarnConfiguration.CS_CONFIGURATION_FILE
        + "." + (System.currentTimeMillis() - 60000L));
    try (OutputStream out = new FileOutputStream(seedFile)) {
      seed.writeXml(out);
    }
    YarnConfiguration conf = new YarnConfiguration();
    conf.setClass(YarnConfiguration.RM_SCHEDULER, CapacityScheduler.class,
        ResourceScheduler.class);
    conf.set(YarnConfiguration.SCHEDULER_CONFIGURATION_STORE_CLASS,
        YarnConfiguration.FS_CONFIGURATION_STORE);
    conf.set(YarnConfiguration.SCHEDULER_CONFIGURATION_FS_PATH,
        storeDir.toURI().toString());
    return conf;
  }

  static RMWebServices webServices(MockRM rm) {
    return new RMWebServices(rm, rm.getConfig(),
        mock(HttpServletResponse.class));
  }

  /** A request of an authenticated user, with optional If-Match values. */
  static HttpServletRequest request(String... ifMatch) {
    HttpServletRequest hsr = mock(HttpServletRequest.class);
    Principal principal = () -> "admin";
    when(hsr.getUserPrincipal()).thenReturn(principal);
    when(hsr.getRemoteUser()).thenReturn("admin");
    List<String> values = Arrays.asList(ifMatch);
    when(hsr.getHeaders(HttpHeaders.IF_MATCH)).thenAnswer(
        invocation -> Collections.enumeration(values));
    return hsr;
  }

  static SchedConfUpdateInfo updateQueue(String queuePath, String key,
      String value) {
    SchedConfUpdateInfo update = new SchedConfUpdateInfo();
    update.getUpdateQueueInfo().add(new QueueConfigInfo(queuePath,
        Collections.singletonMap(key, value)));
    return update;
  }

  static SchedConfUpdateInfo updateGlobal(String key, String value) {
    SchedConfUpdateInfo update = new SchedConfUpdateInfo();
    update.getGlobalParams().put(key, value);
    return update;
  }

  /** The ETag of {@code GET /scheduler-conf}. */
  static String etag(RMWebServices ws) throws Exception {
    Response response = ws.getSchedulerConfiguration(request());
    Object tag = response.getMetadata().getFirst(HttpHeaders.ETAG);
    return tag == null ? null : tag.toString();
  }

  /** Files of the store directory still holding an unconfirmed mutation. */
  static int pendingMutationFiles(File storeDir) {
    String[] names = storeDir.list((dir, name) -> name.endsWith(".tmp"));
    return names == null ? 0 : names.length;
  }
}
