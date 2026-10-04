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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.HttpHeaders;

import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;

/**
 * Tests the scheduler configuration entity tag and If-Match evaluation.
 */
public class TestSchedulerConfigurationETag {

  private static Configuration conf(String... keyValues) {
    Configuration conf = new Configuration(false);
    for (int i = 0; i < keyValues.length; i += 2) {
      conf.set(keyValues[i], keyValues[i + 1]);
    }
    return conf;
  }

  private static String etag(String... keyValues) {
    return SchedulerConfigurationETag.compute(conf(keyValues));
  }

  @Test
  public void testIndependentOfInsertionOrder() {
    assertEquals(etag("a", "1", "b", "2", "c", "3"),
        etag("c", "3", "a", "1", "b", "2"));
  }

  @Test
  public void testStableForEqualContent() {
    Configuration conf = conf("yarn.scheduler.capacity.root.queues", "a,b");
    assertEquals(SchedulerConfigurationETag.compute(conf),
        SchedulerConfigurationETag.compute(new Configuration(conf)));
  }

  @Test
  public void testSensitiveToEveryChange() {
    String base = etag("a", "1", "b", "2");
    assertNotEquals(base, etag("a", "1", "b", "3"), "value change");
    assertNotEquals(base, etag("a", "1", "c", "2"), "key change");
    assertNotEquals(base, etag("a", "1", "b", "2", "c", "3"), "added key");
    assertNotEquals(base, etag("a", "1"), "removed key");
    assertNotEquals(base, etag("a", "1", "b", "2 "), "trailing whitespace");
    assertNotEquals(etag("a", "1", "b", ""), etag("a", "1"), "empty value");
  }

  @Test
  public void testUnambiguousEncoding() {
    assertNotEquals(etag("a", "b=c"), etag("a=b", "c"));
    assertNotEquals(etag("a", "b\nc", "d", "e"), etag("a", "b", "c\nd", "e"));
    assertNotEquals(etag("ab", "c"), etag("a", "bc"));
  }

  @Test
  public void testQuotedStrongTag() {
    String tag = etag("a", "1");
    assertTrue(tag.length() > 2);
    assertEquals('"', tag.charAt(0));
    assertEquals('"', tag.charAt(tag.length() - 1));
    String opaque = tag.substring(1, tag.length() - 1);
    assertTrue(opaque.matches("[A-Za-z0-9_-]+"), opaque);
    assertTrue(SchedulerConfigurationETag.matches(
        Collections.singletonList(tag), tag));
  }

  @Test
  public void testUnconditional() {
    assertFalse(SchedulerConfigurationETag.isConditional(null));
    assertFalse(SchedulerConfigurationETag.isConditional(
        Collections.emptyList()));
    assertFalse(SchedulerConfigurationETag.isConditional(ifMatch("*")));
    assertFalse(SchedulerConfigurationETag.isConditional(ifMatch(" * ")));
    assertFalse(SchedulerConfigurationETag.isConditional(ifMatch("*", "")));
    assertTrue(SchedulerConfigurationETag.matches(ifMatch("*"), "\"x\""));
    assertTrue(SchedulerConfigurationETag.matches(
        Collections.emptyList(), "\"x\""));
  }

  @Test
  public void testStrongComparison() {
    String current = "\"abc\"";
    assertTrue(SchedulerConfigurationETag.matches(ifMatch("\"abc\""), current));
    assertTrue(SchedulerConfigurationETag.matches(
        ifMatch("\"old\", \"abc\""), current));
    assertTrue(SchedulerConfigurationETag.matches(
        ifMatch("\"old\"", " \"abc\" "), current));
    assertTrue(SchedulerConfigurationETag.matches(
        ifMatch(",\t\"abc\" ,"), current));
    assertFalse(SchedulerConfigurationETag.matches(ifMatch("\"old\""), current));
    assertFalse(SchedulerConfigurationETag.matches(ifMatch("\"ABC\""), current));
  }

  @Test
  public void testWeakOrMalformedNeverMatches() {
    String current = "\"abc\"";
    for (String value : new String[] {"W/\"abc\"", "abc", "\"abc", "\"abc\" \"abc\"",
        "", " , "}) {
      List<String> ifMatch = ifMatch(value);
      assertTrue(SchedulerConfigurationETag.isConditional(ifMatch), value);
      assertFalse(SchedulerConfigurationETag.matches(ifMatch, current), value);
    }
  }

  @Test
  public void testReadsEveryIfMatchField() {
    HttpServletRequest request = mock(HttpServletRequest.class);
    assertTrue(SchedulerConfigurationETag.getIfMatch(request).isEmpty());
    when(request.getHeaders(HttpHeaders.IF_MATCH)).thenReturn(
        Collections.enumeration(Arrays.asList("\"a\"", "\"b\"")));
    assertEquals(Arrays.asList("\"a\"", "\"b\""),
        SchedulerConfigurationETag.getIfMatch(request));
  }

  private static List<String> ifMatch(String... values) {
    return Arrays.asList(values);
  }
}
