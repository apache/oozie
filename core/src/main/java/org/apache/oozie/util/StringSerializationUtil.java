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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * This class provides a workaround for the 64k limit for string in DataOutput.
 */
public final class StringSerializationUtil {
    // Using unique string to indicate version. This is to make sure that it
    // doesn't match with user data.
    private static final String DATA_VERSION = "V==1";
    private static final int CONVERSION_TRESHOLD = 60000;

    private StringSerializationUtil() {
    }

    /**
     * Writes out value to dOut. Converts it to byte array if the length of the UTF-8 byte array representation of the
     * string is longer than 60k bytes.
     *
     * @param dOut the targed output stream
     * @param value the string to write
     * @throws IOException in case of error during serialization
     */
    public static void writeString(DataOutput dOut, String value) throws IOException {
        if (value == null) {
            dOut.writeUTF(value);
            return;
        }

        byte[] data = value.getBytes(StandardCharsets.UTF_8.name());
        if (data.length > CONVERSION_TRESHOLD) {
            dOut.writeUTF(DATA_VERSION);
            dOut.writeInt(data.length);
            dOut.write(data);
        } else {
            dOut.writeUTF(value);
        }
    }

    public static String readString(DataInput dIn) throws IOException {
        String value = dIn.readUTF();
        if (DATA_VERSION.equals(value)) {
            int length = dIn.readInt();
            byte[] data = new byte[length];
            dIn.readFully(data);
            value = new String(data, StandardCharsets.UTF_8.name());
        }
        return value;
    }
}
