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
package org.apache.oozie.util;

import java.util.Properties;
import java.io.StringWriter;
import java.io.IOException;
import java.io.StringReader;
import java.io.Reader;

public class PropertiesUtils {

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

}
