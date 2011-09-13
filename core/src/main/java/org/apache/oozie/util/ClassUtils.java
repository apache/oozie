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

import java.util.Enumeration;
import java.net.URL;
import java.net.URLDecoder;
import java.io.IOException;

/**
 * Class utilities.
 */
public class ClassUtils {

    /**
     * Return the path to the JAR file in the classpath containing the specified class. <p/> This method has been
     * canibalized from Hadoop's JobConf class.
     *
     * @param clazz class to find its JAR file.
     * @return the JAR file of the class.
     */
    public static String findContainingJar(Class clazz) {
        ClassLoader loader = clazz.getClassLoader();
        String class_file = clazz.getName().replaceAll("\\.", "/") + ".class";
        try {
            for (Enumeration itr = loader.getResources(class_file); itr.hasMoreElements();) {
                URL url = (URL) itr.nextElement();
                if ("jar".equals(url.getProtocol())) {
                    String toReturn = url.getPath();
                    if (toReturn.startsWith("file:")) {
                        toReturn = toReturn.substring("file:".length());
                    }
                    toReturn = URLDecoder.decode(toReturn, "UTF-8");
                    return toReturn.replaceAll("!.*$", "");
                }
            }
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
        return null;
    }

}
