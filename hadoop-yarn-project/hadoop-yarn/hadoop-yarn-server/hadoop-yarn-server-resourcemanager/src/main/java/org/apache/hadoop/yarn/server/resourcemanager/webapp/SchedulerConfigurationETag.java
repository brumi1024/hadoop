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

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.HttpHeaders;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;

/**
 * Computes the strong entity tag of the scheduler configuration served by
 * {@code GET /scheduler-conf} and evaluates {@code If-Match} preconditions
 * against it (RFC 7232, strong comparison).
 * <p>
 * The tag is a SHA-256 digest over the sorted properties of the configuration,
 * so it depends only on content: it survives store format, RM restart and
 * failover as long as the content is unchanged, and it changes whenever the
 * content changes. Every key and value is length-prefixed, so no two
 * different property sets produce the same digest input.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public final class SchedulerConfigurationETag {

  private static final String ANY = "*";

  private SchedulerConfigurationETag() {
  }

  /**
   * Computes the quoted strong entity tag of a configuration.
   *
   * @param conf the configuration the response body is built from
   * @return the entity tag including the surrounding double quotes
   */
  public static String compute(Configuration conf) {
    // Iterating the configuration yields the same raw values ConfInfo returns.
    Map<String, String> sorted = new TreeMap<>();
    for (Map.Entry<String, String> entry : conf) {
      sorted.put(entry.getKey(), entry.getValue());
    }
    MessageDigest digest;
    try {
      digest = MessageDigest.getInstance("SHA-256");
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException("SHA-256 is not available", e);
    }
    for (Map.Entry<String, String> entry : sorted.entrySet()) {
      update(digest, entry.getKey());
      update(digest, entry.getValue());
    }
    return '"' + Base64.getUrlEncoder().withoutPadding()
        .encodeToString(digest.digest()) + '"';
  }

  private static void update(MessageDigest digest, String value) {
    byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
    int length = bytes.length;
    digest.update(new byte[] {(byte) (length >>> 24), (byte) (length >>> 16),
        (byte) (length >>> 8), (byte) length});
    digest.update(bytes);
  }

  /**
   * Returns every {@code If-Match} field value of a request, in order.
   *
   * @param hsr the servlet request
   * @return the field values, empty when the header is absent
   */
  public static List<String> getIfMatch(HttpServletRequest hsr) {
    Enumeration<String> values = hsr.getHeaders(HttpHeaders.IF_MATCH);
    if (values == null) {
      return Collections.emptyList();
    }
    return Collections.list(values);
  }

  /**
   * Tells whether an {@code If-Match} precondition has to be evaluated.
   *
   * @param ifMatch the field values of every If-Match header of the request
   * @return false when the header is absent or is exactly {@code *}
   */
  public static boolean isConditional(List<String> ifMatch) {
    if (ifMatch == null || ifMatch.isEmpty()) {
      return false;
    }
    List<String> elements = elements(ifMatch);
    return !(elements.size() == 1 && ANY.equals(elements.get(0)));
  }

  /**
   * Evaluates an {@code If-Match} precondition with the strong comparison
   * function: an element matches only when it equals the quoted current tag.
   * A weak or malformed element never equals the strong tag the server
   * issues, so an unusable precondition fails closed.
   *
   * @param ifMatch the field values of every If-Match header of the request
   * @param currentETag the quoted strong entity tag of the current state
   * @return true when the precondition holds
   */
  public static boolean matches(List<String> ifMatch, String currentETag) {
    if (!isConditional(ifMatch)) {
      return true;
    }
    return elements(ifMatch).contains(currentETag);
  }

  /**
   * Splits the field values on commas and trims the elements, dropping the
   * empty ones. The tags the server issues never contain a comma.
   */
  private static List<String> elements(List<String> fieldValues) {
    List<String> elements = new ArrayList<>();
    for (String value : fieldValues) {
      if (value == null) {
        continue;
      }
      for (String element : value.split(",")) {
        String trimmed = element.trim();
        if (!trimmed.isEmpty()) {
          elements.add(trimmed);
        }
      }
    }
    return elements;
  }
}
