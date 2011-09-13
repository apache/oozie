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

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.DataOutput;
import java.io.DataInput;

/**
 * Utility class to write/read Hadoop writables to/from a byte array.
 */
public class WritableUtils {

    /**
     * Write a writable to a byte array.
     *
     * @param writable writable to write.
     * @return array containing the serialized writable.
     */
    public static byte[] toByteArray(Writable writable) {
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            DataOutputStream daos = new DataOutputStream(baos);
            writable.write(daos);
            daos.close();
            return baos.toByteArray();
        }
        catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    /**
     * Read a writable from a byte array.
     *
     * @param array byte array with the serialized writable.
     * @param clazz writable class.
     * @return writable deserialized from the byte array.
     */
    @SuppressWarnings("unchecked")
    public static <T extends Writable> T fromByteArray(byte[] array, Class<T> clazz) {
        try {
            T o = (T) ReflectionUtils.newInstance(clazz, null);
            o.readFields(new DataInputStream(new ByteArrayInputStream(array)));
            return o;
        }
        catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    private static final String NULL = "||";

    /**
     * Write a string to a data output supporting <code>null</code> values. <p/> It uses the '||' token to represent
     * <code>null</code>.
     *
     * @param dataOutput data output.
     * @param str string to write.
     * @throws IOException thrown if the string could not be written.
     */
    public static void writeStr(DataOutput dataOutput, String str) throws IOException {
        str = (str != null) ? str : NULL;
        dataOutput.writeUTF(str);
    }

    /**
     * Read a string from a data input supporting <code>null</code> values. <p/> It uses the '||' token to represent
     * <code>null</code>.
     *
     * @param dataInput data input.
     * @return read string, <code>null</code> if the '||' token was read.
     * @throws IOException thrown if the string could not be read.
     */
    public static String readStr(DataInput dataInput) throws IOException {
        String str = dataInput.readUTF();
        return (str.equals(NULL)) ? null : str;
    }
}
