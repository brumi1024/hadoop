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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity;

import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.QueueACL;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.security.AccessType;
import org.apache.hadoop.yarn.security.YarnAuthorizationProvider;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration.NONE_ACL;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration.PREFIX;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Verifies that auto queue creation templates do not contribute ACLs to
 * statically configured queues.
 *
 * A template describes the queues a parent creates. If its {@code acl_*}
 * entries reached a static queue that omits its own ACL, that queue would be
 * granted access instead of falling back to the NONE default - and because
 * {@code ConfiguredYarnAuthorizer} only widens access as it walks up the
 * hierarchy, an ancestor can never narrow it again.
 */
public class TestCapacitySchedulerTemplateAclIsolation {

  private static final QueuePath ROOT = new QueuePath("root");
  private static final QueuePath A = new QueuePath("root.a");
  private static final QueuePath A1 = new QueuePath("root.a.a1");

  private static final UserGroupInformation ALICE =
      UserGroupInformation.createRemoteUser("alice");
  private static final UserGroupInformation BOB =
      UserGroupInformation.createRemoteUser("bob");

  private MockRM rm;

  @AfterEach
  public void tearDown() {
    if (rm != null) {
      rm.stop();
      rm = null;
    }
    // The authorizer is a memoized singleton; leaving it in place would leak
    // permissions into any test that runs after this one.
    YarnAuthorizationProvider.destroy();
  }

  @Test
  public void testStaticQueueDoesNotInheritTemplateSubmitAcl()
      throws Exception {
    CapacitySchedulerConfiguration csConf =
        new CapacitySchedulerConfiguration();
    csConf.setQueues(ROOT, new String[]{"a"});
    csConf.setQueues(A, new String[]{"a1"});
    csConf.setCapacity(A, 100f);
    csConf.setCapacity(A1, 100f);

    // A restricted ancestor chain: nobody may submit at root, and only alice
    // may submit under root.a. Root has to be restricted explicitly because
    // its ACL defaults to "*".
    csConf.set(PREFIX + "root.acl_submit_applications", NONE_ACL);
    csConf.set(PREFIX + "root.a.acl_submit_applications", "alice");

    // root.a auto-creates children and opens submission up for them.
    csConf.setBoolean(PREFIX + "root.a.auto-queue-creation-v2.enabled", true);
    csConf.set(PREFIX
        + "root.a.auto-queue-creation-v2.template.acl_submit_applications",
        "*");

    // root.a.a1 is declared by the administrator and sets no ACL of its own.

    YarnConfiguration conf = new YarnConfiguration(csConf);
    conf.set(YarnConfiguration.RM_SCHEDULER, CapacityScheduler.class.getName());
    conf.setBoolean(YarnConfiguration.YARN_ACL_ENABLE, true);

    rm = new MockRM(conf);
    rm.start();
    CapacityScheduler cs = (CapacityScheduler) rm.getResourceScheduler();

    AbstractCSQueue staticChild = (AbstractCSQueue) cs.getQueue("root.a.a1");
    assertNotNull(staticChild, "root.a.a1 should be a static queue");

    // The ACL map that feeds the authorizer must not carry the template value.
    assertFalse(staticChild.getACLs().get(AccessType.SUBMIT_APP)
            .isUserAllowed(BOB),
        "static queue root.a.a1 must not grant submit access from its "
            + "parent's auto-queue-creation template");

    // And the same through the authorizer, which is what app submission uses.
    assertFalse(staticChild.hasAccess(QueueACL.SUBMIT_APPLICATIONS, BOB),
        "bob must not be able to submit to root.a.a1");
    assertTrue(staticChild.hasAccess(QueueACL.SUBMIT_APPLICATIONS, ALICE),
        "alice must still inherit submit access from root.a");
  }
}
