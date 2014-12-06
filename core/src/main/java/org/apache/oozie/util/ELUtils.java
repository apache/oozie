/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.oozie.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.oozie.service.ELService;
import org.apache.oozie.service.Services;

import java.util.HashMap;
import java.util.Map;

/**
 * EL utilities
 */
public class ELUtils {

    /**
     * Resolves the application name using the configuration properties to resolve any EL variable.
     *
     * @param name name to EL resolve.
     * @param conf configuration to use for resolving any EL variable in the name.
     *
     * @return the resolved application name.
     *
     * @throws Exception thrown if the name could not be EL resolved.
     */
    public static String resolveAppName(String name, Configuration conf) throws Exception {
        ELService elService = Services.get().get(ELService.class);
        ELEvaluator elEvaluator = elService.createEvaluator("job-submit");
        Map<String, Object> map = new HashMap<String, Object>();
        for (Map.Entry<String, String> entry : conf) {
            map.put(entry.getKey(), entry.getValue());
        }
        elEvaluator.getContext().setVariables(map);
        return elEvaluator.evaluate(name, String.class);
    }

}
