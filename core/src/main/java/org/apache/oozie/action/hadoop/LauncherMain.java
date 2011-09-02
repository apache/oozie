/**
 * Copyright (c) 2010 Yahoo! Inc. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License. See accompanying LICENSE file.
 */
package org.apache.oozie.action.hadoop;

import java.io.IOException;
import java.io.StringWriter;
import java.util.Collection;
import java.util.Map;

public abstract class LauncherMain {

    protected static void run(Class<? extends LauncherMain> klass, String[] args) throws Exception {
        LauncherMain main = klass.newInstance();
        main.run(args);
    }

    protected abstract void run(String[] args) throws Exception;

    /**
     * Write to STDOUT (the task log) the Configuration/Properties values. All properties that contain
     * any of the strings in the maskSet will be masked when writting it to STDOUT.
     *
     * @param header message for the beginning of the Configuration/Properties dump.
     * @param maskSet set with substrings of property names to mask.
     * @param conf Configuration/Properties object to dump to STDOUT
     * @throws IOException thrown if an IO error ocurred.
     */
    @SuppressWarnings("unchecked")
    protected static void logMasking(String header, Collection<String> maskSet, Iterable conf) throws IOException {
        StringWriter writer = new StringWriter();
        writer.write(header + "\n");
        writer.write("--------------------\n");
        for (Map.Entry entry : (Iterable<Map.Entry>) conf){
            String name = (String) entry.getKey();
            String value = (String) entry.getValue();
            for (String mask : maskSet) {
                if (name.contains(mask)) {
                    value = "*MASKED*";
                }
            }
            writer.write(" " + name + " : " + value + "\n");
        }
        writer.write("--------------------\n");
        writer.close();
        System.out.println(writer.toString());
        System.out.flush();
    }

}

class LauncherMainException extends Exception {
    private int errorCode;
    
    public LauncherMainException(int code) {
        errorCode = code;
    }
    
    int getErrorCode() {
        return errorCode;
    }
}