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

package org.apache.oozie;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;

import org.apache.oozie.compression.CodecFactory;
import org.apache.oozie.compression.CompressionCodec;

/**
 * StringBlob to maintain compress and uncompressed data
 */
public class StringBlob {

    private byte[] rawBlob;
    private String string;

    /**
     * Construct string blob from compressed byte array
     *
     * @param byteArray
     */
    public StringBlob(byte[] byteArray) {
        this.rawBlob = byteArray;
    }

    /**
     * Construct StringBlob with uncompressed string
     *
     * @param string
     */
    public StringBlob(String inputString) {
        this.string = inputString;
        this.rawBlob = null;
    }

    /**
     * Set string
     *
     * @param str
     */
    public void setString(String str) {
        this.string = str;
        this.rawBlob = null;
    }

    /**
     * Get uncompressed string
     *
     * @return uncompressed string
     */
    public String getString() {
        if (string != null) {
            return string;
        }
        if (rawBlob == null) {
            return null;
        }
        try {
            DataInputStream dais = new DataInputStream(new ByteArrayInputStream(rawBlob));
            CompressionCodec codec = CodecFactory.getDeCompressionCodec(dais);
            if (codec != null) {
                string = codec.decompressToString(dais);
            }
            else {
                string = new String(rawBlob, CodecFactory.UTF_8_ENCODING);
            }
            dais.close();

        }
        catch (IOException ex) {
            throw new RuntimeException(ex);
        }
        rawBlob = null;
        return string;
    }

    /**
     * Get raw blob
     *
     * @return raw blob
     */
    public byte[] getRawBlob() {
        if (rawBlob != null) {
            return rawBlob;
        }
        if (string == null) {
            return null;
        }
        if (CodecFactory.isCompressionEnabled()) {
            byte[] bytes = CodecFactory.getHeaderBytes();
            try {
                rawBlob = CodecFactory.getCompressionCodec().compressString(bytes, string);
            }
            catch (IOException ex) {
                throw new RuntimeException(ex);
            }
        }
        else {
            rawBlob = string.getBytes();
        }
        return rawBlob;
    }

}
