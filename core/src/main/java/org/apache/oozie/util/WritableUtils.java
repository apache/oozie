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
import org.apache.oozie.compression.CodecFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.DataOutput;
import java.io.DataInput;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 * Utility class to write/read Hadoop writables to/from a byte array.
 */
public class WritableUtils {

    public static XLog LOG = XLog.getLog(WritableUtils.class);

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
     * @param <T> the object type
     * @param array byte array with the serialized writable.
     * @param clazz writable class.
     * @return writable deserialized from the byte array.
     */
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
     * Write a string to a data output supporting <code>null</code> values. <p> It uses the '||' token to represent
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
     * Read a string from a data input supporting <code>null</code> values. <p> It uses the '||' token to represent
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

    /**
     * Read list.
     *
     * @param <T> the generic type
     * @param dataInput the data input
     * @param clazz the clazz
     * @return the list
     * @throws IOException Signals that an I/O exception has occurred.
     */
    public static <T extends Writable> List<T> readList(DataInput dataInput, Class<T> clazz) throws IOException {
        List<T> a = new ArrayList<T>();
        int count = dataInput.readInt();
        for (int i = 0; i < count; i++) {
            T o = (T) ReflectionUtils.newInstance(clazz, null);
            o.readFields(dataInput);
            a.add(o);
        }
        return a;
    }

    public static List<String> readStringList(DataInput dataInput) throws IOException {
        List<String> a = new ArrayList<String>();
        int count = dataInput.readInt();
        for (int i = 0; i < count; i++) {
            a.add(readBytesAsString(dataInput));
        }
        return a;
    }

    /**
     * Write list.
     *
     * @param <T> the generic type
     * @param dataOutput the data output
     * @param list the list
     * @throws IOException Signals that an I/O exception has occurred.
     */
    public static <T extends Writable> void writeList(DataOutput dataOutput, List<T> list) throws IOException {
        dataOutput.writeInt(list.size());
        for (T t : list) {
            t.write(dataOutput);
        }
    }

    /**
     * Write string list.
     *
     * @param dataOutput the data output
     * @param list the list
     * @throws IOException Signals that an I/O exception has occurred.
     */
    public static void writeStringList(DataOutput dataOutput, List<String> list) throws IOException {
        dataOutput.writeInt(list.size());
        for (String str : list) {
            writeStringAsBytes(dataOutput, str);
        }
    }

    /**
     * Write map.
     *
     * @param <T> the generic type
     * @param dataOutput the data output
     * @param map the map
     * @throws IOException Signals that an I/O exception has occurred.
     */
    public static <T extends Writable> void writeMap(DataOutput dataOutput, Map<String, T> map) throws IOException {
        dataOutput.writeInt(map.size());
        for (Entry<String, T> t : map.entrySet()) {
            writeStringAsBytes(dataOutput, t.getKey());
            t.getValue().write(dataOutput);
        }
    }

    /**
     * Write map with list.
     *
     * @param <T> the generic type
     * @param dataOutput the data output
     * @param map the map
     * @throws IOException Signals that an I/O exception has occurred.
     */
    public static <T extends Writable> void writeMapWithList(DataOutput dataOutput, Map<String, List<T>> map)
            throws IOException {
        dataOutput.writeInt(map.size());
        for (Entry<String, List<T>> t : map.entrySet()) {
            writeStringAsBytes(dataOutput, t.getKey());
            writeList(dataOutput, t.getValue());
        }
    }

    /**
     * Read map.
     *
     * @param <T> the generic type
     * @param dataInput the data input
     * @param clazz the clazz
     * @return the map
     * @throws IOException Signals that an I/O exception has occurred.
     */
    public static <T extends Writable> Map<String, T> readMap(DataInput dataInput, Class<T> clazz) throws IOException {
        Map<String, T> map = new HashMap<String, T>();
        int count = dataInput.readInt();
        for (int i = 0; i < count; i++) {
            String key = readBytesAsString(dataInput);
            T value = (T) ReflectionUtils.newInstance(clazz, null);
            value.readFields(dataInput);
            map.put(key, value);
        }
        return map;
    }

    /**
     * Read map with list.
     *
     * @param <T> the generic type
     * @param dataInput the data input
     * @param clazz the clazz
     * @return the map
     * @throws IOException Signals that an I/O exception has occurred.
     */
    public static <T extends Writable> Map<String, List<T>> readMapWithList(DataInput dataInput, Class<T> clazz)
            throws IOException {
        Map<String, List<T>> map = new HashMap<String, List<T>>();
        int count = dataInput.readInt();
        for (int i = 0; i < count; i++) {
            String key = readBytesAsString(dataInput);
            map.put(key, readList(dataInput, clazz));
        }
        return map;
    }

    public static void writeStringAsBytes(DataOutput dOut, String value) throws IOException {
        byte[] data = value.getBytes(CodecFactory.UTF_8_ENCODING);
        dOut.writeInt(data.length);
        dOut.write(data);
    }

    public static String readBytesAsString(DataInput dIn) throws IOException {
        int length = dIn.readInt();
        byte[] data = new byte[length];
        dIn.readFully(data);
        return new String(data, CodecFactory.UTF_8_ENCODING);
    }

}
