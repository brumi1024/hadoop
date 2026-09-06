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


package org.apache.hadoop.security;

import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/** Reading a compatibility descriptor must never initialize a plugin. */
public class TestGroupsProviderDescriptor {
  @AfterEach
  public void reset() {
    Groups.reset();
  }

  @Test
  public void testUninitializedDescriptorDoesNotCreateGroups() {
    Groups.reset();
    assertNull(Groups.getInitializedProviderClassName());
    assertNull(Groups.getInitializedProviderClassName());
  }

  @Test
  public void testDescriptorDoesNotInvokeInstalledProvider() {
    Groups.reset();
    Configuration conf = new Configuration(false);
    conf.setClass(CommonConfigurationKeys.HADOOP_SECURITY_GROUP_MAPPING,
        UnusableProvider.class, GroupMappingServiceProvider.class);
    Groups groups = Groups.getUserToGroupsMappingService(conf);
    assertEquals(UnusableProvider.class.getName(),
        Groups.getInitializedProviderClassName());
    assertEquals(UnusableProvider.class.getName(), groups.getProviderClassName());
  }

  public static class UnusableProvider implements GroupMappingServiceProvider {
    @Override
    public List<String> getGroups(String user) {
      throw new AssertionError("Descriptor must not resolve groups");
    }

    @Override
    public void cacheGroupsRefresh() {
      throw new AssertionError("Descriptor must not refresh groups");
    }

    @Override
    public void cacheGroupsAdd(List<String> groups) {
      throw new AssertionError("Descriptor must not cache groups");
    }
  }
}
