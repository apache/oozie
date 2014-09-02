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

package org.apache.oozie.compression;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.oozie.util.XLog;

/**
 *  Utility class for maintaining list of codecs and providing facility
 *  for compressing and decompressing.
 *
 */
public class CodecFactory {
    private static final Map<String, CompressionCodec> REGISTERED = new HashMap<String, CompressionCodec>();
    public static final String COMPRESSION_CODECS = "oozie.compression.codecs";
    public static final String COMPRESSION_OUTPUT_CODEC = "oozie.output.compression.codec";
    private static CompressionCodec outputCompressionCodec;
    public static final String COMPRESSION_MAGIC_DATA = "OBJ";
    public static final String COMPRESSION_KEY_HEADER = "codec";
    public static final String UTF_8_ENCODING = "UTF-8";
    private static boolean isEnabled;
    private static XLog LOG = XLog.getLog(CodecFactory.class);;
    private static byte[] headerBytes;

    /**
     * Initialize the codec factory to maintain list of codecs
     * @param conf
     * @throws Exception
     */
    public static void initialize(Configuration conf) throws Exception {
        String outputCompressionStr = conf.get(COMPRESSION_OUTPUT_CODEC);
        if (outputCompressionStr == null || outputCompressionStr.trim().equalsIgnoreCase("NONE") ||
                outputCompressionStr.trim().equalsIgnoreCase("")) {
            isEnabled = false;
        }
        else {
            outputCompressionStr = outputCompressionStr.trim();
            isEnabled = true;
        }
        String[] outputCompressionCodecs = conf.getStrings(COMPRESSION_CODECS);
        for (String comp : outputCompressionCodecs) {
            parseCompressionConfig(comp);
        }
        if (isEnabled) {
            if (REGISTERED.get(GzipCompressionCodec.CODEC_NAME) == null) {
                REGISTERED.put(GzipCompressionCodec.CODEC_NAME, new GzipCompressionCodec());
            }
            outputCompressionCodec = REGISTERED.get(outputCompressionStr);
            if (outputCompressionCodec == null) {
                throw new RuntimeException("No codec class found for codec " + outputCompressionStr);
            }
        }
        LOG.info("Using " + outputCompressionStr + " as output compression codec");

        // Initialize header bytes
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream daos = new DataOutputStream(baos);
        // magic data
        daos.write(COMPRESSION_MAGIC_DATA.getBytes(UTF_8_ENCODING));
        // version
        daos.writeInt(1);
        // no of key value pairs
        daos.writeInt(1);
        daos.writeUTF(COMPRESSION_KEY_HEADER);
        daos.writeUTF(outputCompressionStr);
        daos.close();
        headerBytes = baos.toByteArray();

    }

    private static void parseCompressionConfig(String comp) throws Exception {
        String[] compression = comp.split("=", 2);
        if (compression.length == 2) {
            String key = compression[0];
            String value = compression[1];
            REGISTERED.put(key, (CompressionCodec) Class.forName(value).newInstance());
            LOG.info("Adding [{0}] to list of output compression codecs", key);
        }
        else {
            throw new IllegalArgumentException("Property " + comp + " not in key=value format"
                    + "; output compression cannot be enabled");
        }
    }

    private static CompressionCodec getCodec(String key) {
        CompressionCodec codec = REGISTERED.get(key);
        if (codec != null) {
            return codec;
        }
        else {
            throw new RuntimeException("No compression algo found corresponding to " + key);
        }
    }

    /**
     * Check whether compression is enabled or not
     * @return true if compression is enabled
     */
    public static boolean isCompressionEnabled() {
        return isEnabled;
    }

    /**
     * Get decompression codec after reading from stream
     * @param dais the input stream
     * @return the decompression codec
     * @throws IOException
     */
    public static CompressionCodec getDeCompressionCodec(DataInputStream dais) throws IOException {
        byte[] buffer = new byte[COMPRESSION_MAGIC_DATA.length()];
        dais.read(buffer, 0, buffer.length);
        Map<String, String> compressionProps = new HashMap<String, String>();
        try {
            if (new String(buffer, UTF_8_ENCODING).equals(COMPRESSION_MAGIC_DATA)) {
                // read Version; need to handle if multiple versions are
                // supported
                dais.readInt();
                // read no of key value pairs; need to handle if more than one
                dais.readInt();
                compressionProps.put(dais.readUTF(), dais.readUTF());
            }
            else {
                dais.reset();
                return null;
            }
        }
        catch (UnsupportedEncodingException ex) {
            throw new RuntimeException(ex);
        }
        return getCodec(compressionProps.get(COMPRESSION_KEY_HEADER));
    }

    /**
     * Get output compression codec
     * @return the compression codec
     */
    public static CompressionCodec getCompressionCodec() {
        return outputCompressionCodec;
    }

    /**
     * Get header bytes
     * @return the header bytes
     */
    public static byte[] getHeaderBytes() {
        return headerBytes;
    }

}
