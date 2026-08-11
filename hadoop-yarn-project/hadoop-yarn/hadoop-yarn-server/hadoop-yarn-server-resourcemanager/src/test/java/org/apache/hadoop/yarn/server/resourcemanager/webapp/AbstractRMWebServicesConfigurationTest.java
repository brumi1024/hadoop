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

import org.glassfish.jersey.internal.inject.AbstractBinder;
import org.glassfish.jersey.server.ResourceConfig;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueuePath;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.jsonprovider.JsonProviderFeature;
import org.apache.hadoop.yarn.webapp.GenericExceptionHandler;
import org.apache.hadoop.yarn.webapp.JerseyTestBase;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.Application;

import java.io.FileOutputStream;
import java.io.IOException;
import java.security.Principal;

import static org.apache.hadoop.yarn.server.resourcemanager.webapp.TestWebServiceUtil.getCapacitySchedulerConfigFileInTarget;
import static org.apache.hadoop.yarn.server.resourcemanager.webapp.TestWebServiceUtil.backupSchedulerConfigFileInTarget;
import static org.apache.hadoop.yarn.server.resourcemanager.webapp.TestWebServiceUtil.restoreSchedulerConfigFileInTarget;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Shared Jersey fixture for scheduler configuration REST tests. */
abstract class AbstractRMWebServicesConfigurationTest extends JerseyTestBase {
  protected static final QueuePath ROOT = new QueuePath("root");
  protected static final QueuePath ROOT_A = new QueuePath("root", "a");
  protected static final QueuePath ROOT_A_A1 = QueuePath.createFromQueues(
      "root", "a", "a1");
  protected static final QueuePath ROOT_A_A2 = QueuePath.createFromQueues(
      "root", "a", "a2");
  protected static final QueuePath ROOT_B = new QueuePath("root", "b");
  protected static final QueuePath ROOT_C = new QueuePath("root", "c");
  protected static final QueuePath ROOT_C_C1 = QueuePath.createFromQueues(
      "root", "c", "c1");
  protected static final QueuePath ROOT_D = new QueuePath("root", "d");
  protected static MockRM rm;
  protected static String userName;
  protected static CapacitySchedulerConfiguration csConf;
  protected static YarnConfiguration conf;
  private HttpServletRequest request;

  @Override
  protected Application configure() {
    ResourceConfig config = new ResourceConfig();
    config.register(RMWebServices.class);
    config.register(new JerseyBinder());
    config.register(GenericExceptionHandler.class);
    config.register(TestRMWebServicesAppsModification.TestRMCustomAuthFilter.class);
    config.register(JsonProviderFeature.class);
    config.register(JAXBContextResolver.class);
    return config;
  }

  private class JerseyBinder extends AbstractBinder {
    @Override
    protected void configure() {
      try {
        userName = UserGroupInformation.getCurrentUser().getShortUserName();
      } catch (IOException ioe) {
        throw new RuntimeException("Unable to get current user name "
            + ioe.getMessage(), ioe);
      }
      csConf = new CapacitySchedulerConfiguration(new Configuration(false),
          false);
      setupQueueConfiguration(csConf);
      conf = new YarnConfiguration();
      conf.setClass(YarnConfiguration.RM_SCHEDULER, CapacityScheduler.class,
          ResourceScheduler.class);
      conf.set(YarnConfiguration.SCHEDULER_CONFIGURATION_STORE_CLASS,
          YarnConfiguration.MEMORY_CONFIGURATION_STORE);
      conf.set(YarnConfiguration.YARN_ADMIN_ACL, userName);
      try {
        FileOutputStream out = new FileOutputStream(
            getCapacitySchedulerConfigFileInTarget());
        csConf.writeXml(out);
        out.close();
      } catch (IOException e) {
        throw new RuntimeException("Failed to write XML file", e);
      }
      rm = new MockRM(conf);

      request = mock(HttpServletRequest.class);
      when(request.getScheme()).thenReturn("http");
      final HttpServletResponse response = mock(HttpServletResponse.class);
      bind(rm).to(ResourceManager.class).named("rm");
      bind(csConf).to(Configuration.class).named("conf");
      Principal principal = () -> userName;
      bind(request).to(HttpServletRequest.class);
      when(request.getUserPrincipal()).thenReturn(principal);
      bind(response).to(HttpServletResponse.class);
    }
  }

  @BeforeAll
  public static void beforeClass() {
    backupSchedulerConfigFileInTarget();
  }

  @AfterAll
  public static void afterClass() {
    restoreSchedulerConfigFileInTarget();
  }

  @Override
  @BeforeEach
  public void setUp() throws Exception {
    super.setUp();
  }

  protected static void setupQueueConfiguration(
      CapacitySchedulerConfiguration config) {
    config.setQueues(ROOT, new String[]{"a", "b", "c", "mappedqueue"});

    config.setCapacity(ROOT_A, 25f);
    config.setMaximumCapacity(ROOT_A, 50f);

    config.setQueues(ROOT_A, new String[]{"a1", "a2"});
    config.setCapacity(ROOT_A_A1, 100f);
    config.setCapacity(ROOT_A_A2, 0f);

    config.setCapacity(ROOT_B, 75f);

    config.setCapacity(ROOT_C, 0f);

    config.setQueues(ROOT_C, new String[] {"c1"});
    config.setCapacity(ROOT_C_C1, 0f);

    config.setCapacity(ROOT_D, 0f);
    config.set(CapacitySchedulerConfiguration.QUEUE_MAPPING,
        "g:hadoop:mappedqueue");
  }

  @Override
  @AfterEach
  public void tearDown() throws Exception {
    if (rm != null) {
      rm.stop();
    }
    super.tearDown();
  }
}
