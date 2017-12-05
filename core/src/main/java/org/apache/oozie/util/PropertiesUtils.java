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

import java.util.Properties;
import java.util.Set;
import java.io.StringWriter;
import java.io.IOException;
import java.io.StringReader;
import java.io.Reader;

import org.apache.hadoop.conf.Configuration;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.command.CommandException;

public class PropertiesUtils {

    public static final String HADOOP_USER = "user.name";
    public static final String YEAR = "YEAR";
    public static final String MONTH = "MONTH";
    public static final String DAY = "DAY";
    public static final String HOUR = "HOUR";
    public static final String MINUTE = "MINUTE";
    public static final String DAYS = "DAYS";
    public static final String HOURS = "HOURS";
    public static final String MINUTES = "MINUTES";
    public static final String KB = "KB";
    public static final String MB = "MB";
    public static final String GB = "GB";
    public static final String TB = "TB";
    public static final String PB = "PB";
    public static final String RECORDS = "RECORDS";
    public static final String MAP_IN = "MAP_IN";
    public static final String MAP_OUT = "MAP_OUT";
    public static final String REDUCE_IN = "REDUCE_IN";
    public static final String REDUCE_OUT = "REDUCE_OUT";
    public static final String GROUPS = "GROUPS";

    public static String propertiesToString(Properties props) {
        ParamChecker.notNull(props, "props");
        try {
            StringWriter sw = new StringWriter();
            props.store(sw, "");
            sw.close();
            return sw.toString();
        }
        catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    public static Properties stringToProperties(String str) {
        ParamChecker.notNull(str, "str");
        try {
            StringReader sr = new StringReader(str);
            Properties props = new Properties();
            props.load(sr);
            sr.close();
            return props;
        }
        catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    public static Properties readProperties(Reader reader, int maxDataLen) throws IOException {
        String data = IOUtils.getReaderAsString(reader, maxDataLen);
        return stringToProperties(data);
    }

    /**
     * Create a set from an array
     *
     * @param properties String array
     * @param set String set
     */
    public static void createPropertySet(String[] properties, Set<String> set) {
        ParamChecker.notNull(set, "set");
        for (String p : properties) {
            set.add(p);
        }
    }

    /**
     * Validate against DISALLOWED Properties.
     *
     * @param conf : configuration to check.
     * @param set the set containing the disallowed properties
     * @throws CommandException if a property in the set is not null in the conf
     */
    public static void checkDisallowedProperties(Configuration conf, Set<String> set) throws CommandException {
        ParamChecker.notNull(conf, "conf");
        for (String prop : set) {
            if (conf.get(prop) != null) {
                throw new CommandException(ErrorCode.E0808, prop);
            }
        }
    }

}
