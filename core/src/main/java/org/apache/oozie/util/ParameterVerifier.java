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

import java.util.Iterator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.oozie.ErrorCode;
import org.jdom.Element;
import org.jdom.Namespace;

import org.apache.hadoop.conf.Configuration;

/**
 * Utility class to parse and verify the <parameters> section in a workflow or coordinator job
 */
public abstract class ParameterVerifier 
{
    private static final Pattern nsVersionPattern = Pattern.compile("uri:oozie:(workflow|coordinator|bundle):(\\d+.\\d+)");
    
    private static final double workflowMinVersion = 0.4;
    private static final double coordinatorMinVersion = 0.4;
    private static final double bundleMinVersion = 0.2;
    
    /**
     * Verify the parameters section (if supported in the schema)
     *
     * @param conf The job configuration
     * @param rootElement The root element of the workflow, coordinator, or bundle definition
     * @throws ParameterVerifierException If required parameters are not defined and have no default values, or if a name is empty
     */
    public static void verifyParameters(Configuration conf, Element rootElement) throws ParameterVerifierException {
        ParamChecker.notNull(conf, "conf");
        if (rootElement == null) {
            return;
        }
        
        if (supportsParameters(rootElement.getNamespaceURI())) {
            Element params = rootElement.getChild("parameters", rootElement.getNamespace());
            if (params != null) {
                int numMissing = 0;
                StringBuilder missingParameters = new StringBuilder();
                Namespace paramsNs = params.getNamespace();
                Iterator<Element> it = params.getChildren("property", paramsNs).iterator();
                while (it.hasNext()) {
                    Element prop = it.next();
                    String name = prop.getChildTextTrim("name", paramsNs);
                    if (name != null) {
                        if (name.isEmpty()) {
                            throw new ParameterVerifierException(ErrorCode.E0739);
                        }
                        if (conf.get(name) == null) {
                            String defaultValue = prop.getChildTextTrim("value", paramsNs);
                            if (defaultValue != null) {
                                conf.set(name, defaultValue);
                            } else {
                                missingParameters.append(name);
                                missingParameters.append(", ");
                                numMissing++;
                            }
                        }
                    }
                }
                if (numMissing > 0) {
                    missingParameters.setLength(missingParameters.length() - 2);    //remove the trailing ", "
                    throw new ParameterVerifierException(ErrorCode.E0738, numMissing, missingParameters.toString());
                }
            } else {
                // Log a warning when the <parameters> section is missing
                XLog.getLog(ParameterVerifier.class).warn("The application does not define formal parameters in its XML "
                        + "definition");
            }
        }
    }
    
    static boolean supportsParameters(String namespaceURI) {
        boolean supports = false;
        Matcher m = nsVersionPattern.matcher(namespaceURI);
        if (m.matches() && m.groupCount() == 2) {
            String type = m.group(1);
            double ver = Double.parseDouble(m.group(2));
            supports = ((type.equals("workflow") && ver >= workflowMinVersion) || 
                    (type.equals("coordinator") && ver >= coordinatorMinVersion) || 
                    (type.equals("bundle") && ver >= bundleMinVersion));
        }
        return supports;
    }
}
