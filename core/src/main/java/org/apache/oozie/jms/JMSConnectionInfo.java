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

package org.apache.oozie.jms;

import java.util.Properties;

import org.apache.oozie.util.XLog;

public class JMSConnectionInfo {

    private String jndiPropertiesString;
    private Properties props;

    /**
     * Create a JMSConnectionInfo
     * @param jndiPropertiesString JNDI properties with # as key value delimiter and ; as properties delimiter
     */
    public JMSConnectionInfo(String jndiPropertiesString) {
        this.jndiPropertiesString = jndiPropertiesString;
        initializeProps();
    }

    private void initializeProps() {
        this.props = new Properties();
        String[] propArr = jndiPropertiesString.split(";");
        for (String pair : propArr) {
            String[] kV = pair.split("#");
            if (kV.length > 1) {
                props.put(kV[0].trim(), kV[1].trim());
            }
            else {
                XLog.getLog(getClass()).warn("Unformatted properties. Expected key#value : " + pair);
                props = null;
            }
        }
        if (props.isEmpty()) {
            props = null;
        }
    }

    /**
     * Get JNDI properties to establish a JMS connection
     * @return JNDI properties
     */
    public Properties getJNDIProperties() {
        return props;
    }

    /**
     * Return JNDI properties string
     * @return JNDI properties string
     */
    public String getJNDIPropertiesString() {
        return jndiPropertiesString;
    }

    @Override
    public int hashCode() {
        return 31 + ((jndiPropertiesString == null) ? 0 : jndiPropertiesString.hashCode());
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        JMSConnectionInfo other = (JMSConnectionInfo) obj;
        if (jndiPropertiesString == null) {
            if (other.jndiPropertiesString != null)
                return false;
        }
        else if (!jndiPropertiesString.equals(other.jndiPropertiesString))
            return false;
        return true;
    }

    @Override
    public String toString() {
        return "JMSConnectionInfo [jndiProperties=" + jndiPropertiesString + "]";
    }

}
